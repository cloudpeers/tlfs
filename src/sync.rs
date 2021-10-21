use anyhow::{bail, Result};
use async_trait::async_trait;
use bytecheck::CheckBytes;
use fnv::FnvHashMap;
use futures::io::{AsyncRead, AsyncWrite};
use futures::prelude::*;
use libp2p::request_response::{
    self, ProtocolName, ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec,
    RequestResponseConfig,
};
use libp2p::swarm::NetworkBehaviourEventProcess;
use libp2p::{Multiaddr, NetworkBehaviour};
use libp2p_broadcast::{Broadcast, BroadcastConfig, BroadcastEvent, Topic};
use rkyv::{Archive, Deserialize, Serialize};
use std::convert::TryInto;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tlfs_crdt::{Backend, Causal, CausalContext, DocId, Hash, Keypair, PeerId, Ref};

macro_rules! unwrap {
    ($r:expr) => {
        match $r {
            Ok(r) => r,
            Err(err) => {
                tracing::error!("{}", err);
                return;
            }
        }
    };
}

#[derive(Clone)]
pub struct SyncProtocol;

impl ProtocolName for SyncProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/tlfs/sync/1.0.0".as_bytes()
    }
}

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub enum SyncRequest {
    Lenses([u8; 32]),
    Unjoin(DocId, CausalContext),
}

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub enum SyncResponse {
    Lenses(Vec<u8>),
    Unjoin([u8; 32], Causal),
}

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct Delta {
    schema: [u8; 32],
    causal: Causal,
}

#[derive(Clone, Default)]
pub struct SyncCodec {
    buffer: Vec<u8>,
}

#[async_trait]
impl RequestResponseCodec for SyncCodec {
    type Protocol = SyncProtocol;
    type Request = Ref<SyncRequest>;
    type Response = Ref<SyncResponse>;

    async fn read_request<T>(&mut self, _: &SyncProtocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.buffer.clear();
        io.read_to_end(&mut self.buffer).await?;
        Ref::checked(&self.buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{}", err)))
    }

    async fn read_response<T>(&mut self, _: &SyncProtocol, io: &mut T) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.buffer.clear();
        io.read_to_end(&mut self.buffer).await?;
        Ref::checked(&self.buffer)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{}", err)))
    }

    async fn write_request<T>(
        &mut self,
        _: &SyncProtocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(req.as_bytes()).await?;
        io.close().await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &SyncProtocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        io.write_all(res.as_bytes()).await?;
        io.close().await?;
        Ok(())
    }
}

type RequestResponseEvent =
    request_response::RequestResponseEvent<Ref<SyncRequest>, Ref<SyncResponse>>;

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    #[behaviour(ignore)]
    backend: Backend,
    req: RequestResponse<SyncCodec>,
    #[behaviour(ignore)]
    unjoin_req: FnvHashMap<RequestId, DocId>,
    #[behaviour(ignore)]
    buffer: Vec<(Hash, DocId, PeerId, Causal)>,
    broadcast: Broadcast,
}

impl Behaviour {
    pub fn new(backend: Backend) -> Result<Self> {
        let mut me = Self {
            backend,
            req: RequestResponse::new(
                SyncCodec::default(),
                vec![(SyncProtocol, ProtocolSupport::Full)],
                RequestResponseConfig::default(),
            ),
            unjoin_req: Default::default(),
            buffer: Default::default(),
            broadcast: Broadcast::new(BroadcastConfig::default()),
        };
        for res in me.backend.frontend().docs() {
            let doc = res?;
            me.subscribe(&doc);
        }
        Ok(me)
    }

    pub fn poll_backend(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.backend).poll(cx)
    }

    pub fn add_address(&mut self, peer: &PeerId, addr: Multiaddr) {
        self.req.add_address(&peer.to_libp2p().into_peer_id(), addr);
    }

    pub fn remove_address(&mut self, peer: &PeerId, addr: &Multiaddr) {
        self.req
            .remove_address(&peer.to_libp2p().into_peer_id(), addr);
    }

    pub fn request_lenses(&mut self, peer_id: &PeerId, hash: Hash) -> RequestId {
        tracing::debug!("request_lenses {} {}", peer_id, hash);
        let peer_id = peer_id.to_libp2p().into_peer_id();
        let req = SyncRequest::Lenses(hash.into());
        self.req.send_request(&peer_id, Ref::archive(&req))
    }

    pub fn request_unjoin(&mut self, peer_id: &PeerId, doc: DocId) -> Result<RequestId> {
        tracing::debug!("request_unjoin {} {}", peer_id, doc);
        let peer_id = peer_id.to_libp2p().into_peer_id();
        let ctx = self.backend.frontend().ctx(&doc)?;
        let req = SyncRequest::Unjoin(doc, ctx);
        let id = self.req.send_request(&peer_id, Ref::archive(&req));
        self.unjoin_req.insert(id, doc);
        Ok(id)
    }

    pub fn subscribe(&mut self, doc: &DocId) {
        let topic = Topic::new(doc.as_ref());
        self.broadcast.subscribe(topic);
    }

    pub fn broadcast(&mut self, doc: &DocId, causal: Causal) -> Result<()> {
        let topic = Topic::new(doc.as_ref());
        let hash = self.backend.frontend().schema_id(doc)?;
        let delta = Delta {
            schema: hash.into(),
            causal,
        };
        let delta = Ref::archive(&delta);
        self.broadcast.broadcast(&topic, delta.as_bytes().into());
        Ok(())
    }

    fn inject_causal(
        &mut self,
        peer: PeerId,
        doc: DocId,
        schema: Hash,
        causal: Causal,
    ) -> Result<()> {
        if self.backend.registry().contains(&schema)? {
            self.backend.join(&peer, &doc, &schema, causal)?;
        } else {
            self.buffer.push((schema, doc, peer, causal));
            self.request_lenses(&peer, schema);
        }
        Ok(())
    }
}

impl NetworkBehaviourEventProcess<BroadcastEvent> for Behaviour {
    fn inject_event(&mut self, ev: BroadcastEvent) {
        use BroadcastEvent::*;
        match ev {
            Subscribed(peer, topic) => {
                let peer = unwrap!(libp2p_peer_id(&peer));
                let doc = DocId::new(topic.as_ref().try_into().unwrap());
                unwrap!(self.request_unjoin(&peer, doc));
            }
            Received(peer, topic, msg) => {
                let peer = unwrap!(libp2p_peer_id(&peer));
                let doc = DocId::new(topic.as_ref().try_into().unwrap());
                let delta = unwrap!(unwrap!(Ref::<Delta>::checked(&msg)).to_owned());
                unwrap!(self.inject_causal(peer, doc, delta.schema.into(), delta.causal));
            }
            Unsubscribed(_peer, _topic) => {}
        }
    }
}

impl NetworkBehaviourEventProcess<RequestResponseEvent> for Behaviour {
    fn inject_event(&mut self, ev: RequestResponseEvent) {
        use request_response::{RequestResponseEvent::*, RequestResponseMessage::*};
        match ev {
            Message { peer, message } => match message {
                Request {
                    request_id: _,
                    request,
                    channel,
                } => {
                    tracing::debug!("{:?}", request.as_ref());
                    use ArchivedSyncRequest::*;
                    match request.as_ref() {
                        Lenses(hash) => {
                            let hash = Hash::from(*hash);
                            if let Some(lenses) = unwrap!(self.backend.registry().lenses(&hash)) {
                                let resp = SyncResponse::Lenses(lenses.into());
                                let resp = Ref::archive(&resp);
                                self.req.send_response(channel, resp).ok();
                            }
                        }
                        Unjoin(doc, ctx) => {
                            let peer = unwrap!(libp2p_peer_id(&peer));
                            let causal = unwrap!(self.backend.unjoin(&peer, doc, ctx));
                            let schema = unwrap!(self.backend.frontend().schema_id(doc));
                            let resp = SyncResponse::Unjoin(schema.into(), causal);
                            let resp = Ref::archive(&resp);
                            self.req.send_response(channel, resp).ok();
                        }
                    }
                }
                Response {
                    request_id,
                    response,
                } => {
                    tracing::debug!("{:?}", response.as_ref());
                    use ArchivedSyncResponse::*;
                    match response.as_ref() {
                        Lenses(lenses) => {
                            let schema = unwrap!(self.backend.registry().register(lenses));
                            for i in 0..self.buffer.len() {
                                if self.buffer[i].0 == schema {
                                    let (schema, doc, peer, causal) = self.buffer.remove(i);
                                    unwrap!(self.backend.join(&peer, &doc, &schema, causal));
                                }
                            }
                        }
                        Unjoin(schema, causal) => {
                            let schema = Hash::from(*schema);
                            let peer = unwrap!(libp2p_peer_id(&peer));
                            let causal = unwrap!(causal.deserialize(&mut rkyv::Infallible));
                            let doc = self.unjoin_req.remove(&request_id).unwrap();
                            unwrap!(self.inject_causal(peer, doc, schema, causal));
                        }
                    }
                }
            },
            OutboundFailure {
                peer: _,
                request_id,
                error,
            } => {
                self.unjoin_req.remove(&request_id);
                tracing::error!("{}", error);
            }
            InboundFailure {
                peer: _,
                request_id: _,
                error,
            } => {
                tracing::error!("{}", error);
            }
            ResponseSent {
                peer: _,
                request_id: _,
            } => {}
        }
    }
}

pub trait ToLibp2pKeypair {
    fn to_libp2p(self) -> libp2p::identity::Keypair;
}

impl ToLibp2pKeypair for Keypair {
    fn to_libp2p(self) -> libp2p::identity::Keypair {
        let mut secret_key: [u8; 32] = self.into();
        let secret_key = libp2p::identity::ed25519::SecretKey::from_bytes(&mut secret_key).unwrap();
        libp2p::identity::Keypair::Ed25519(secret_key.into())
    }
}

pub trait ToLibp2pPublic {
    fn to_libp2p(self) -> libp2p::identity::PublicKey;
}

impl ToLibp2pPublic for PeerId {
    fn to_libp2p(self) -> libp2p::identity::PublicKey {
        let public_key: [u8; 32] = self.into();
        let public_key = libp2p::identity::ed25519::PublicKey::decode(&public_key[..]).unwrap();
        libp2p::identity::PublicKey::Ed25519(public_key)
    }
}

fn libp2p_peer_id(peer_id: &libp2p::PeerId) -> Result<PeerId> {
    match libp2p::multihash::Multihash::from_bytes(&peer_id.to_bytes()) {
        Ok(multihash) => {
            if multihash.code() == u64::from(libp2p::multihash::Code::Identity) {
                let bytes = multihash.digest();
                let libp2p_pubkey =
                    libp2p::core::identity::PublicKey::from_protobuf_encoding(bytes)?;
                match libp2p_pubkey {
                    libp2p::core::identity::PublicKey::Ed25519(ed25519_pub) => {
                        let bytes = ed25519_pub.encode();
                        Ok(PeerId::new(bytes))
                    }
                    _ => bail!("Expected ed25519_dalek::PublicKey!"),
                }
            } else {
                bail!("Only PeerIds encoded with identity hash can be decoded")
            }
        }

        Err(err) => bail!(err),
    }
}

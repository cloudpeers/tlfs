use crate::{Metadata, Secrets};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytecheck::CheckBytes;
use fnv::FnvHashMap;
use futures::io::{AsyncRead, AsyncWrite};
use futures::prelude::*;
use libp2p::gossipsub::{
    Gossipsub, GossipsubConfigBuilder, GossipsubEvent, IdentTopic, MessageAuthenticity,
    ValidationMode,
};
use libp2p::request_response::{
    self, ProtocolName, ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec,
    RequestResponseConfig,
};
use libp2p::swarm::NetworkBehaviourEventProcess;
use libp2p::{Multiaddr, NetworkBehaviour};
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tlfs_crdt::{
    Backend, Causal, CausalContext, DocId, Encrypted, Hash, Key, Keypair, PeerId, Permission, Ref,
    Signed,
};

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
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub enum SyncRequest {
    Lenses([u8; 32]),
    Key(DocId),
    Unjoin(CausalContext),
}

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub enum SyncResponse {
    Lenses(Vec<u8>),
    Key(Key),
    Unjoin(Causal),
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
        // TODO: doesn't work
        //Ref::checked(&self.buffer)
        //    .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{}", err)))
        Ok(Ref::new(self.buffer.clone().into()))
    }

    async fn read_response<T>(&mut self, _: &SyncProtocol, io: &mut T) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.buffer.clear();
        io.read_to_end(&mut self.buffer).await?;
        // TODO: doesn't work
        //Ref::checked(&self.buffer)
        //    .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("{}", err)))
        Ok(Ref::new(self.buffer.clone().into()))
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

pub enum Event {
    Gossip(GossipsubEvent),
    ReqResp(RequestResponseEvent),
}

impl From<GossipsubEvent> for Event {
    fn from(ev: GossipsubEvent) -> Self {
        Self::Gossip(ev)
    }
}

impl From<RequestResponseEvent> for Event {
    fn from(ev: RequestResponseEvent) -> Self {
        Self::ReqResp(ev)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event")]
pub struct Behaviour {
    #[behaviour(ignore)]
    backend: Backend,
    #[behaviour(ignore)]
    secrets: Secrets,
    req: RequestResponse<SyncCodec>,
    gossip: Gossipsub,
    #[behaviour(ignore)]
    peer_id: PeerId,
    #[behaviour(ignore)]
    key_req: FnvHashMap<RequestId, DocId>,
    #[behaviour(ignore)]
    encrypted: Vec<(DocId, PeerId, Ref<Encrypted>)>,
    #[behaviour(ignore)]
    buffer: Vec<(PeerId, Causal)>,
}

impl Behaviour {
    pub fn new(backend: Backend, secrets: Secrets) -> Result<Self> {
        let peer_id = secrets.keypair(Metadata::new())?.unwrap().peer_id();
        Ok(Self {
            backend,
            secrets,
            req: RequestResponse::new(
                SyncCodec::default(),
                vec![(SyncProtocol, ProtocolSupport::Full)],
                RequestResponseConfig::default(),
            ),
            gossip: Gossipsub::new(
                MessageAuthenticity::Author(peer_id.to_libp2p().into_peer_id()),
                GossipsubConfigBuilder::default()
                    .validation_mode(ValidationMode::None)
                    .build()
                    .unwrap(),
            )
            .unwrap(),
            peer_id,
            key_req: Default::default(),
            encrypted: Default::default(),
            buffer: Default::default(),
        })
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

    pub fn request_key(&mut self, peer_id: &PeerId, doc: DocId) -> RequestId {
        tracing::debug!("request_key {} {}", peer_id, doc);
        let peer_id = peer_id.to_libp2p().into_peer_id();
        let req = SyncRequest::Key(doc);
        let id = self.req.send_request(&peer_id, Ref::archive(&req));
        self.key_req.insert(id, doc);
        id
    }

    pub fn request_unjoin(&mut self, peer_id: &PeerId, doc: DocId) -> Result<RequestId> {
        tracing::debug!("request_unjoin {} {}", peer_id, doc);
        let peer_id = peer_id.to_libp2p().into_peer_id();
        let ctx = self.backend.doc(doc)?.ctx()?;
        let req = SyncRequest::Unjoin(ctx);
        Ok(self.req.send_request(&peer_id, Ref::archive(&req)))
    }

    pub fn subscribe_doc(&mut self, id: DocId) -> Result<()> {
        tracing::debug!("subscribe_doc {}", id);
        let doc = self.backend.doc(id)?;
        let metadata = Metadata::new().doc(id).peer(*doc.peer_id());
        if self.secrets.key(metadata)?.is_none() {
            self.rotate_key(id)?;
        }
        let topic = IdentTopic::new(id.to_string());
        self.gossip
            .subscribe(&topic)
            .map_err(|err| anyhow!("{:?}", err))?;
        Ok(())
    }

    pub fn send_delta(&mut self, causal: &Causal) -> Result<()> {
        let doc = self.backend.doc(*causal.ctx().doc())?;
        let signed = self.secrets.keypair(Metadata::new())?.unwrap().sign(causal);
        let metadata = Metadata::new().doc(*doc.id()).peer(*doc.peer_id());
        let encrypted = self.secrets.key_nonce(metadata)?.unwrap().encrypt(&signed);
        let msg = Ref::archive(&encrypted);
        let topic = IdentTopic::new(doc.id().to_string());
        self.gossip.publish(topic, msg).ok();
        Ok(())
    }

    pub fn rotate_key(&mut self, doc: DocId) -> Result<()> {
        tracing::debug!("rotate_key {}", doc);
        let peer_id = *self.backend.doc(doc)?.peer_id();
        self.secrets
            .generate_key(Metadata::new().doc(doc).peer(peer_id))?;
        Ok(())
    }

    fn inject_signed(&mut self, signed: &Archived<Signed>) {
        let (peer, causal) = unwrap!(signed.verify::<Causal>());
        let causal: Causal = unwrap!(causal.deserialize(&mut rkyv::Infallible));
        let lenses = causal.ctx().schema();
        if unwrap!(self.backend.registry().contains(&lenses)) {
            self.backend.join(&peer, causal).ok();
            // TODO: detect missing updates and request unjoin
        } else {
            self.buffer.push((peer, causal));
            self.request_lenses(&peer, lenses);
        }
    }
}

impl NetworkBehaviourEventProcess<GossipsubEvent> for Behaviour {
    fn inject_event(&mut self, ev: GossipsubEvent) {
        use GossipsubEvent::*;
        match ev {
            Message {
                propagation_source: _,
                message_id: _,
                mut message,
            } => {
                let doc = unwrap!(message.topic.as_str().parse());
                let peer = unwrap!(libp2p_peer_id(&message.source.unwrap()));
                let meta = Metadata::new().doc(doc).peer(peer);
                let encrypted = unwrap!(Ref::checked(&message.data));
                let signed = match unwrap!(self.secrets.key(meta)) {
                    Some(key) => key.decrypt::<Signed>(&mut message.data).ok(),
                    None => None,
                };
                if let Some(signed) = signed {
                    self.inject_signed(signed);
                } else {
                    self.encrypted.push((doc, peer, encrypted));
                    self.request_key(&peer, doc);
                }
            }
            Subscribed {
                peer_id: _,
                topic: _,
            } => {}
            Unsubscribed {
                peer_id: _,
                topic: _,
            } => {}
        }
    }
}

impl NetworkBehaviourEventProcess<RequestResponseEvent> for Behaviour {
    fn inject_event(&mut self, ev: RequestResponseEvent) {
        use request_response::{RequestResponseEvent::*, RequestResponseMessage::*};
        match ev {
            Message { peer, message } => {
                match message {
                    Request {
                        request_id: _,
                        request,
                        channel,
                    } => {
                        tracing::debug!("{:?}", request);
                        use ArchivedSyncRequest::*;
                        match request.as_ref() {
                            Lenses(hash) => {
                                let hash = Hash::from(*hash);
                                if let Some(lenses) = unwrap!(self.backend.registry().lenses(&hash))
                                {
                                    let resp = SyncResponse::Lenses(lenses.into());
                                    let resp = Ref::archive(&resp);
                                    self.req.send_response(channel, resp).ok();
                                }
                            }
                            Key(doc) => {
                                // TODO: fine grained keys
                                let doc = unwrap!(self.backend.doc(*doc));
                                let peer = unwrap!(libp2p_peer_id(&peer));
                                if !unwrap!(doc.cursor().can(&peer, Permission::Read)) {
                                    return;
                                }
                                let meta = Metadata::new().doc(*doc.id()).peer(self.peer_id);
                                if let Some(key) = unwrap!(self.secrets.key(meta)) {
                                    let resp = SyncResponse::Key(key);
                                    let resp = Ref::archive(&resp);
                                    self.req.send_response(channel, resp).ok();
                                }
                            }
                            Unjoin(ctx) => {
                                let peer = unwrap!(libp2p_peer_id(&peer));
                                let causal = unwrap!(self.backend.unjoin(&peer, ctx));
                                let resp = SyncResponse::Unjoin(causal);
                                let resp = Ref::archive(&resp);
                                self.req.send_response(channel, resp).ok();
                            }
                        }
                    }
                    Response {
                        request_id,
                        response,
                    } => {
                        tracing::debug!("{:?}", response);
                        use ArchivedSyncResponse::*;
                        match response.as_ref() {
                            Lenses(lenses) => {
                                let schema = unwrap!(self.backend.registry().register(lenses));
                                for i in 0..self.buffer.len() {
                                    if self.buffer[i].1.ctx().schema() == schema {
                                        let (peer, causal) = self.buffer.remove(i);
                                        unwrap!(self.backend.join(&peer, causal));
                                    }
                                }
                            }
                            Key(key) => {
                                let doc = if let Some(doc) = self.key_req.remove(&request_id) {
                                    doc
                                } else {
                                    return;
                                };
                                let peer = unwrap!(libp2p_peer_id(&peer));
                                unwrap!(self
                                    .secrets
                                    .add_key(Metadata::new().doc(doc).peer(peer), *key));
                                for i in 0..self.encrypted.len() {
                                    if self.encrypted[i].0 == doc && self.encrypted[i].1 == peer {
                                        let mut encrypted: Vec<u8> =
                                            self.encrypted.remove(i).2.into();
                                        let signed = unwrap!(key.decrypt::<Signed>(&mut encrypted));
                                        self.inject_signed(signed);
                                    }
                                }
                            }
                            Unjoin(causal) => {
                                let peer = unwrap!(libp2p_peer_id(&peer));
                                let lenses = causal.ctx().schema();
                                // TODO: don't deserialize
                                let causal = unwrap!(causal.deserialize(&mut rkyv::Infallible));
                                if unwrap!(self.backend.registry().contains(&lenses)) {
                                    unwrap!(self.backend.join(&peer, causal));
                                } else {
                                    self.buffer.push((peer, causal));
                                    self.request_lenses(&peer, lenses);
                                }
                            }
                        }
                    }
                }
            }
            OutboundFailure {
                peer: _,
                request_id,
                error,
            } => {
                self.key_req.remove(&request_id);
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

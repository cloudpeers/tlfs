use crate::{Metadata, Secrets};
use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytecheck::CheckBytes;
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
use rkyv::{Archive, Deserialize, Serialize};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tlfs_crdt::{
    Backend, Causal, CausalContext, DocId, Hash, Key, Keypair, PeerId, Permission, Ref, Signed,
};

#[derive(Clone)]
pub struct SyncProtocol;

impl ProtocolName for SyncProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/tlfs/sync/1.0.0".as_bytes()
    }
}

#[derive(Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub enum SyncRequest {
    Lenses([u8; 32]),
    Key(DocId, PeerId),
    Unjoin(CausalContext),
}

#[derive(Archive, Deserialize, Serialize)]
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
}

impl Behaviour {
    pub fn new(backend: Backend, secrets: Secrets) -> Result<Self> {
        let peer_id = secrets
            .keypair(Metadata::new())?
            .unwrap()
            .peer_id()
            .to_libp2p()
            .into_peer_id();
        Ok(Self {
            backend,
            secrets,
            req: RequestResponse::new(
                SyncCodec::default(),
                vec![(SyncProtocol, ProtocolSupport::Full)],
                RequestResponseConfig::default(),
            ),
            gossip: Gossipsub::new(
                MessageAuthenticity::Author(peer_id),
                GossipsubConfigBuilder::default()
                    .validation_mode(ValidationMode::None)
                    .build()
                    .unwrap(),
            )
            .unwrap(),
        })
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    pub fn poll_backend(&mut self, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.backend).poll(cx)
    }

    pub fn secrets(&self) -> &Secrets {
        &self.secrets
    }

    pub fn add_address(&mut self, peer: &PeerId, addr: Multiaddr) {
        self.req.add_address(&peer.to_libp2p().into_peer_id(), addr);
    }

    pub fn remove_address(&mut self, peer: &PeerId, addr: &Multiaddr) {
        self.req
            .remove_address(&peer.to_libp2p().into_peer_id(), addr);
    }

    pub fn request_lenses(&mut self, peer_id: &libp2p::PeerId, hash: Hash) -> RequestId {
        let req = SyncRequest::Lenses(hash.into());
        self.req.send_request(peer_id, Ref::archive(&req))
    }

    pub fn request_key(&mut self, peer_id: &libp2p::PeerId, doc: DocId, peer: PeerId) -> RequestId {
        let req = SyncRequest::Key(doc, peer);
        self.req.send_request(peer_id, Ref::archive(&req))
    }

    pub fn request_unjoin(&mut self, peer_id: &libp2p::PeerId, doc: DocId) -> Result<RequestId> {
        let ctx = self.backend.doc(doc)?.ctx()?;
        let req = SyncRequest::Unjoin(ctx);
        Ok(self.req.send_request(peer_id, Ref::archive(&req)))
    }

    pub fn subscribe_doc(&mut self, id: DocId) -> Result<()> {
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
        let peer_id = *self.backend.doc(doc)?.peer_id();
        self.secrets
            .generate_key(Metadata::new().doc(doc).peer(peer_id))?;
        Ok(())
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
                let doc = message.topic.as_str().parse().unwrap();
                let peer = if let Some(Ok(peer)) = message.source.as_ref().map(libp2p_peer_id) {
                    peer
                } else {
                    return;
                };
                let meta = Metadata::new().doc(doc).peer(peer);
                if let Ok(Some(key)) = self.secrets.key(meta) {
                    if let Ok(signed) = key.decrypt::<Signed>(&mut message.data) {
                        if let Ok((peer_id, causal)) = signed.verify::<Causal>() {
                            let causal: Causal = causal.deserialize(&mut rkyv::Infallible).unwrap();
                            if let Ok(true) =
                                self.backend.registry().contains(&causal.ctx().schema())
                            {
                                self.backend.join(&peer_id, causal).ok();
                                // TODO: detect missing updates and request unjoin
                            } else {
                                // TODO: request lenses
                            }
                        }
                    } else {
                        // TODO: request key
                    }
                } else {
                    // TODO: request key
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
                        use ArchivedSyncRequest::*;
                        match request.as_ref() {
                            Lenses(hash) => {
                                if let Ok(Some(lenses)) =
                                    self.backend.registry().lenses(&(*hash).into())
                                {
                                    let resp = SyncResponse::Lenses(lenses.into());
                                    let resp = Ref::archive(&resp);
                                    self.req.send_response(channel, resp).ok();
                                }
                            }
                            Key(doc, peer) => {
                                // TODO: fine grained keys
                                if let Ok(doc) = self.backend.doc(*doc) {
                                    if let Ok(true) = doc.cursor().can(peer, Permission::Read) {
                                        if let Ok(Some(key)) = self
                                            .secrets
                                            .key(Metadata::new().doc(*doc.id()).peer(*peer))
                                        {
                                            let resp = SyncResponse::Key(key);
                                            let resp = Ref::archive(&resp);
                                            self.req.send_response(channel, resp).ok();
                                        }
                                    }
                                }
                            }
                            Unjoin(ctx) => {
                                if let Ok(peer) = libp2p_peer_id(&peer) {
                                    if let Ok(causal) = self.backend.unjoin(&peer, ctx) {
                                        let resp = SyncResponse::Unjoin(causal);
                                        let resp = Ref::archive(&resp);
                                        self.req.send_response(channel, resp).ok();
                                    }
                                }
                            }
                        }
                    }
                    Response {
                        request_id: _,
                        response,
                    } => {
                        use ArchivedSyncResponse::*;
                        match response.as_ref() {
                            Lenses(lenses) => {
                                self.backend.registry().register(lenses).ok();
                            }
                            Key(key) => {
                                // TODO: store request
                                let doc = DocId::new([0; 32]);
                                let peer = PeerId::new([0; 32]);
                                self.secrets
                                    .add_key(Metadata::new().doc(doc).peer(peer), *key)
                                    .ok();
                            }
                            Unjoin(causal) => {
                                if let Ok(peer) = libp2p_peer_id(&peer) {
                                    if let Ok(true) =
                                        self.backend.registry().contains(&causal.ctx().schema())
                                    {
                                        // TODO: don't deserialize
                                        if let Ok(causal) =
                                            causal.deserialize(&mut rkyv::Infallible)
                                        {
                                            self.backend.join(&peer, causal).ok();
                                        }
                                    } else {
                                        // TODO: request lenses
                                    }
                                }
                            }
                        }
                    }
                }
            }
            OutboundFailure {
                peer: _,
                request_id: _,
                error: _,
            } => {}
            InboundFailure {
                peer: _,
                request_id: _,
                error: _,
            } => {}
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

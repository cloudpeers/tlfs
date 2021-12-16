use anyhow::{bail, Result};
use async_trait::async_trait;
use bytecheck::CheckBytes;
use fnv::FnvHashMap;
use futures::{
    channel::mpsc,
    io::{AsyncRead, AsyncWrite},
    prelude::*,
};
use libp2p::{
    mdns, ping,
    request_response::{
        self, ProtocolName, ProtocolSupport, RequestId, RequestResponse, RequestResponseCodec,
        RequestResponseConfig,
    },
    swarm::{
        dial_opts::{DialOpts, PeerCondition},
        NetworkBehaviour, NetworkBehaviourAction, NetworkBehaviourEventProcess, PollParameters,
    },
    Multiaddr, NetworkBehaviour,
};
use libp2p_broadcast::{Broadcast, BroadcastConfig, BroadcastEvent, Topic};
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    convert::TryInto,
    io,
    pin::Pin,
    task::{Context, Poll},
};
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
    Invite(DocId, String),
    Lenses([u8; 32]),
    Unjoin(DocId, CausalContext),
}

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub enum SyncResponse {
    Invite,
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

/// Invitation to collaborate on a document.
#[derive(Clone, Debug)]
#[repr(C)]
pub struct Invite {
    /// Document identifier.
    pub doc: DocId,
    /// Schema of the document.
    pub schema: String,
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
        Ref::checked(&self.buffer).map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("read_request: {} {:?}", err, &self.buffer))
        })
    }

    async fn read_response<T>(&mut self, _: &SyncProtocol, io: &mut T) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.buffer.clear();
        io.read_to_end(&mut self.buffer).await?;
        Ref::checked(&self.buffer).map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("read_response: {} {:?}", err, &self.buffer))
        })
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

pub(crate) fn notify(subs: &mut Vec<mpsc::Sender<()>>) {
    subs.retain(|tx| match tx.clone().try_send(()) {
        Ok(()) => true,
        Err(err) if err.is_full() => true,
        Err(_) => false,
    });
}

type RequestResponseEvent =
    request_response::RequestResponseEvent<Ref<SyncRequest>, Ref<SyncResponse>>;

#[derive(NetworkBehaviour)]
#[behaviour(event_process = true, poll_method = "poll_dial")]
pub struct Behaviour {
    req: RequestResponse<SyncCodec>,
    broadcast: Broadcast,
    ping: ping::Behaviour,
    mdns: mdns::Mdns,
    #[behaviour(ignore)]
    unjoin_req: FnvHashMap<RequestId, DocId>,
    #[behaviour(ignore)]
    buffer: Vec<(Hash, DocId, PeerId, Causal)>,
    #[behaviour(ignore)]
    backend: Backend,
    #[behaviour(ignore)]
    sub_local_peers: Vec<mpsc::Sender<()>>,
    #[behaviour(ignore)]
    sub_invites: Vec<mpsc::Sender<()>>,
    #[behaviour(ignore)]
    invites: Vec<Invite>,
    #[behaviour(ignore)]
    dial: VecDeque<PeerId>,
}

impl Behaviour {
    pub async fn new(backend: Backend) -> Result<Self> {
        let mut me = Self {
            backend,
            req: RequestResponse::new(
                SyncCodec::default(),
                vec![(SyncProtocol, ProtocolSupport::Full)],
                RequestResponseConfig::default(),
            ),
            mdns: mdns::Mdns::new(mdns::MdnsConfig::default()).await?,
            ping: ping::Behaviour::new(ping::Config::new().with_keep_alive(true)),
            unjoin_req: Default::default(),
            buffer: Default::default(),
            broadcast: Broadcast::new(BroadcastConfig::default()),
            sub_local_peers: Default::default(),
            sub_invites: Default::default(),
            invites: Default::default(),
            dial: Default::default(),
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
        self.req.add_address(&peer.to_libp2p().to_peer_id(), addr);
    }

    pub fn remove_address(&mut self, peer: &PeerId, addr: &Multiaddr) {
        self.req
            .remove_address(&peer.to_libp2p().to_peer_id(), addr);
    }

    pub fn local_peers(&self) -> BTreeSet<PeerId> {
        self.mdns
            .discovered_nodes()
            .filter_map(|peer| libp2p_peer_id(peer).ok())
            .collect()
    }

    pub fn subscribe_local_peers(&mut self, ch: mpsc::Sender<()>) {
        self.sub_local_peers.push(ch);
    }

    pub fn subscribe_invites(&mut self, ch: mpsc::Sender<()>) {
        self.sub_invites.push(ch);
    }

    pub fn request_lenses(&mut self, peer_id: &PeerId, hash: Hash) -> RequestId {
        tracing::debug!("request_lenses {} {}", peer_id, hash);
        let peer_id = peer_id.to_libp2p().to_peer_id();
        let req = SyncRequest::Lenses(hash.into());
        self.req.send_request(&peer_id, Ref::archive(&req))
    }

    pub fn request_unjoin(&mut self, peer_id: &PeerId, doc: DocId) -> Result<RequestId> {
        tracing::debug!("request_unjoin {} {}", peer_id, doc);
        let peer_id = peer_id.to_libp2p().to_peer_id();
        let ctx = self.backend.frontend().ctx(&doc)?;
        let req = SyncRequest::Unjoin(doc, ctx);
        let id = self.req.send_request(&peer_id, Ref::archive(&req));
        self.unjoin_req.insert(id, doc);
        Ok(id)
    }

    pub fn subscribe(&mut self, doc: &DocId) {
        let topic = Topic::new(doc.as_ref());
        self.broadcast.subscribe(topic);
        let mut peers = vec![];
        if let Some(iter) = self.broadcast.peers(&topic) {
            for peer in iter {
                if let Ok(peer) = libp2p_peer_id(&peer) {
                    peers.push(peer);
                }
            }
        }
        for peer in peers {
            unwrap!(self.request_unjoin(&peer, *doc));
        }
    }

    pub fn invite(&mut self, peer_id: &PeerId, doc: DocId, schema: String) -> RequestId {
        tracing::debug!("invite {} {}", peer_id, doc);
        let peer_id = peer_id.to_libp2p().to_peer_id();
        let req = SyncRequest::Invite(doc, schema);
        self.req.send_request(&peer_id, Ref::archive(&req))
    }

    pub fn clear_invites(&mut self) -> Vec<Invite> {
        std::mem::take(&mut self.invites)
    }

    pub fn broadcast(&mut self, doc: &DocId, causal: Causal) -> Result<()> {
        let topic = Topic::new(doc.as_ref());
        let hash = self.backend.frontend().schema(doc)?.as_ref().hash();
        let delta = Delta {
            schema: hash.into(),
            causal,
        };
        let delta = Ref::archive(&delta);
        tracing::debug!("sending broadcast");
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
        if self.backend.registry().contains(&schema) {
            self.backend.join(&peer, &doc, &schema, causal)?;
        } else {
            self.buffer.push((schema, doc, peer, causal));
            self.request_lenses(&peer, schema);
        }
        Ok(())
    }

    fn poll_dial(
        &mut self,
        _cx: &mut Context,
        _params: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self as NetworkBehaviour>::OutEvent,
            <Self as NetworkBehaviour>::ProtocolsHandler,
        >,
    > {
        if let Some(peer) = self.dial.pop_front() {
            Poll::Ready(NetworkBehaviourAction::Dial {
                opts: DialOpts::peer_id(peer.to_libp2p().to_peer_id())
                    .condition(PeerCondition::Disconnected)
                    .build(),
                handler: self.new_handler(),
            })
        } else {
            Poll::Pending
        }
    }
}

impl NetworkBehaviourEventProcess<BroadcastEvent> for Behaviour {
    fn inject_event(&mut self, ev: BroadcastEvent) {
        use BroadcastEvent::*;
        match ev {
            Subscribed(peer, topic) => {
                let peer = unwrap!(libp2p_peer_id(&peer));
                let doc = DocId::new(topic.as_ref().try_into().unwrap());
                tracing::debug!("{} subscribed to {}", peer, doc);
                if unwrap!(self.backend.contains(&doc)) {
                    unwrap!(self.request_unjoin(&peer, doc));
                }
            }
            Received(peer, topic, msg) => {
                tracing::debug!("received broadcast");
                let peer = unwrap!(libp2p_peer_id(&peer));
                let doc = DocId::new(topic.as_ref().try_into().unwrap());
                let delta = unwrap!(unwrap!(Ref::<Delta>::checked(&msg)).to_owned());
                unwrap!(self.inject_causal(peer, doc, delta.schema.into(), delta.causal));
            }
            Unsubscribed(peer, topic) => {
                let peer = unwrap!(libp2p_peer_id(&peer));
                let doc = DocId::new(topic.as_ref().try_into().unwrap());
                tracing::debug!("{} unsubscribed from {}", peer, doc);
            }
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
                    tracing::debug!("req {:?}", request.as_ref());
                    use ArchivedSyncRequest as SyncRequest;
                    match request.as_ref() {
                        SyncRequest::Invite(doc, schema) => {
                            self.invites.push(Invite {
                                doc: *doc,
                                schema: schema.to_string(),
                            });
                            notify(&mut self.sub_invites);
                            let resp = SyncResponse::Invite;
                            let resp = Ref::archive(&resp);
                            self.req.send_response(channel, resp).ok();
                        }
                        SyncRequest::Lenses(hash) => {
                            let hash = Hash::from(*hash);
                            if let Some(lenses) = self.backend.registry().get(&hash) {
                                let resp = SyncResponse::Lenses(lenses.as_ref().as_ref().to_vec());
                                let resp = Ref::archive(&resp);
                                self.req.send_response(channel, resp).ok();
                            }
                        }
                        SyncRequest::Unjoin(doc, ctx) => {
                            let peer = unwrap!(libp2p_peer_id(&peer));
                            let schema =
                                unwrap!(self.backend.frontend().schema(doc)).as_ref().hash();
                            let causal = unwrap!(self.backend.unjoin(&peer, doc, ctx));
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
                    tracing::debug!("resp {:?}", response.as_ref());
                    use ArchivedSyncResponse::*;
                    match response.as_ref() {
                        Invite => {}
                        Lenses(lenses) => {
                            let schema2 = unwrap!(self.backend.registry().register(lenses));
                            self.buffer.retain(|(schema, doc, peer, causal)| {
                                if *schema == schema2 {
                                    if let Err(err) =
                                        self.backend.join(&peer, &doc, &schema, causal.clone())
                                    {
                                        tracing::error!("{}", err);
                                    }
                                    false
                                } else {
                                    true
                                }
                            });
                        }
                        Unjoin(schema, causal) => {
                            let schema = Hash::from(*schema);
                            let peer = unwrap!(libp2p_peer_id(&peer));
                            let causal = unwrap!(causal.deserialize(&mut rkyv::Infallible));
                            let res = self.unjoin_req.remove(&request_id).ok_or_else(|| {
                                anyhow::anyhow!("received response without request")
                            });
                            let doc = unwrap!(res);
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

impl NetworkBehaviourEventProcess<ping::Event> for Behaviour {
    fn inject_event(&mut self, _event: ping::Event) {}
}

impl NetworkBehaviourEventProcess<mdns::MdnsEvent> for Behaviour {
    fn inject_event(&mut self, event: mdns::MdnsEvent) {
        if let mdns::MdnsEvent::Discovered(iter) = event {
            for (peer, _) in iter {
                if let Ok(peer) = libp2p_peer_id(&peer) {
                    // TODO: handle becomes active after discovery
                    if self.backend.active_peer(&peer) {
                        tracing::info!("dialing active peer {}", peer);
                        self.dial.push_back(peer);
                    }
                }
            }
        }
        notify(&mut self.sub_local_peers);
    }
}

/// Conversion to libp2p
pub trait ToLibp2pKeypair {
    /// Converts the [`Keypair`] into a libp2p identity
    fn to_libp2p(self) -> libp2p::identity::Keypair;
}

impl ToLibp2pKeypair for Keypair {
    fn to_libp2p(self) -> libp2p::identity::Keypair {
        let mut secret_key: [u8; 32] = self.into();
        let secret_key = libp2p::identity::ed25519::SecretKey::from_bytes(&mut secret_key).unwrap();
        libp2p::identity::Keypair::Ed25519(secret_key.into())
    }
}

/// Conversion to libp2p
pub trait ToLibp2pPublic {
    /// Converts the [`PeerId`] into a libp2p PeerId
    fn to_libp2p(self) -> libp2p::identity::PublicKey;
}

impl ToLibp2pPublic for PeerId {
    fn to_libp2p(self) -> libp2p::identity::PublicKey {
        let public_key: [u8; 32] = self.into();
        let public_key = libp2p::identity::ed25519::PublicKey::decode(&public_key[..]).unwrap();
        libp2p::identity::PublicKey::Ed25519(public_key)
    }
}

/// Convert a [`libp2p::PeerId`] into a [`tlfs::PeerId`], if possible.
pub fn libp2p_peer_id(peer_id: &libp2p::PeerId) -> Result<PeerId> {
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

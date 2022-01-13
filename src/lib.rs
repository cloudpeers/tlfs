//! The Local First SDK.
//!
//! See the `tlfs_crdt` docs for details of how it works.
#![deny(missing_docs)]
mod sync;
mod transport;

pub use crate::sync::{libp2p_peer_id, Invite, ToLibp2pKeypair, ToLibp2pPublic};
pub use libp2p::Multiaddr;
pub use tlfs_crdt::{
    Actor, ArchivedSchema, Backend, Can, Causal, Cursor, DocId, Event, Frontend, Keypair, Kind,
    Lens, Lenses, Package, PathBuf, PeerId, Permission, PrimitiveKind, Ref, Schema, Subscriber,
};

use crate::sync::{notify, Behaviour};
use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::poll_fn,
    Future, Stream, StreamExt,
};
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    swarm::{AddressScore, SwarmEvent},
    Swarm,
};
use std::collections::BTreeSet;
use std::task::Poll;

/// Main entry point for `tlfs`.
pub struct Sdk {
    frontend: Frontend,
    peer: PeerId,
    swarm: mpsc::UnboundedSender<Command>,
    #[cfg(not(target_family = "wasm"))]
    _task: async_global_executor::Task<()>,
}

impl Sdk {
    /// Creates a new persistent [`Sdk`] instance.
    pub async fn persistent(db: &std::path::Path, package: &[u8]) -> Result<Self> {
        Self::new(
            std::sync::Arc::new(tlfs_crdt::FileStorage::new(db)),
            package,
        )
        .await
    }

    /// Create a new in-memory [`Sdk`] instance.
    pub async fn memory(package: &[u8]) -> Result<Self> {
        Self::new(
            std::sync::Arc::new(tlfs_crdt::MemStorage::default()),
            package,
        )
        .await
    }

    #[allow(clippy::if_same_then_else)]
    async fn new(storage: std::sync::Arc<dyn tlfs_crdt::Storage>, package: &[u8]) -> Result<Self> {
        use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
        // FIXME: replace by tracing feature
        tracing_log::LogTracer::init().ok();
        let env = std::env::var(EnvFilter::DEFAULT_ENV)
            .unwrap_or_else(|_| "tlfs,info,libp2p_swarm".to_owned());
        let subscriber = {
            let b = tracing_subscriber::FmtSubscriber::builder()
                .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
                .with_env_filter(EnvFilter::new(env))
                .with_writer(std::io::stderr);

            #[cfg(target_family = "wasm")]
            // TODO
            let b = b.without_time();
            b.finish()
        };
        if cfg!(target_os = "android") {
            #[cfg(target_os = "android")]
            use tracing_subscriber::layer::SubscriberExt;
            #[cfg(target_os = "android")]
            let subscriber = subscriber.with(tracing_android::layer("com.cloudpeer")?);
            tracing::subscriber::set_global_default(subscriber).ok();
            std::env::set_var("RUST_BACKTRACE", "1");
        } else if cfg!(target_family = "wasm") {
            #[cfg(target_family = "wasm")]
            let subscriber = {
                use tracing_subscriber::layer::SubscriberExt;
                subscriber.with(tracing_wasm::WASMLayer::default())
            };
            tracing::subscriber::set_global_default(subscriber).ok();
        } else {
            tracing::subscriber::set_global_default(subscriber).ok();
        };
        log_panics::init();

        let backend = Backend::new(storage, package)?;
        let frontend = backend.frontend();

        let keypair = frontend.default_keypair()?;
        let peer = keypair.peer_id();
        tracing::info!("our peer id is: {}", peer);

        let transport = transport::transport(keypair.to_libp2p())?;
        let mut listen_on = vec!["/dns4/local1st.net/tcp/443/wss/p2p-webrtc-star"
            .parse()
            .unwrap()];
        //if !cfg!(target_family = "wasm") {
        //    listen_on.push("/ip4/0.0.0.0/tcp/0".parse().unwrap());
        //}

            //TODO
            //        slf.add_external_address(
            //            signaling_server
            //                .with(Protocol::P2pWebRtcStar)
            //                .with(Protocol::P2p(libp2p_peer.into())),
            //            // TODO
            //            AddressScore::Infinite,
            //        )
        Self::new_with_transport(backend, frontend, peer, transport, listen_on.into_iter()).await
    }

    /// Creates a new [`Sdk`] instance from the given [`Backend`], [`Frontend`] and libp2p
    /// transport.
    pub async fn new_with_transport(
        backend: Backend,
        frontend: Frontend,
        peer: PeerId,
        transport: Boxed<(libp2p::PeerId, StreamMuxerBox)>,
        listen_on: impl Iterator<Item = Multiaddr>,
    ) -> Result<Self> {
        let behaviour = Behaviour::new(backend).await?;
        let mut swarm = Swarm::new(transport, behaviour, peer.to_libp2p().to_peer_id());
        for i in listen_on {
            swarm.listen_on(i)?;
        }

        let (tx, mut rx) = mpsc::unbounded();
        let driver = poll_fn::<(), _>(move |cx| {
            let mut sub_addresses = vec![];
            let mut sub_connected_peers = vec![];
            while let Poll::Ready(Some(cmd)) = rx.poll_next_unpin(cx) {
                match cmd {
                    Command::AddAddress(peer, addr) => {
                        swarm.behaviour_mut().add_address(&peer, addr);
                        if let Err(err) = swarm.dial(peer.to_libp2p().to_peer_id()) {
                            tracing::error!("{}", err);
                        }
                    }
                    Command::AddExternalAddress(addr, score) => {
                        swarm.add_external_address(addr, score);
                    }
                    Command::RemoveAddress(peer, addr) => {
                        swarm.behaviour_mut().remove_address(&peer, &addr)
                    }
                    Command::Addresses(ch) => {
                        let addrs = swarm.listeners().cloned().collect::<Vec<_>>();
                        ch.send(addrs).ok();
                    }
                    Command::SubscribeAddresses(ch) => {
                        sub_addresses.push(ch);
                    }
                    Command::LocalPeers(ch) => {
                        let peers = swarm.behaviour_mut().local_peers();
                        ch.send(peers).ok();
                    }
                    Command::SubscribeLocalPeers(ch) => {
                        swarm.behaviour_mut().subscribe_local_peers(ch);
                    }
                    Command::ConnectedPeers(ch) => {
                        let peers = swarm
                            .connected_peers()
                            .filter_map(|peer| libp2p_peer_id(peer).ok())
                            .collect();
                        ch.send(peers).ok();
                    }
                    Command::SubscribeConnectedPeers(ch) => {
                        sub_connected_peers.push(ch);
                    }
                    Command::Subscribe(doc) => {
                        swarm.behaviour_mut().subscribe(&doc);
                    }
                    Command::Broadcast(doc, causal) => {
                        swarm.behaviour_mut().broadcast(&doc, causal).ok();
                    }
                    Command::Invite(peer, doc, schema) => {
                        swarm.behaviour_mut().invite(&peer, doc, schema);
                    }
                    Command::Invites(tx) => {
                        let invites = swarm.behaviour_mut().clear_invites();
                        tx.send(invites).ok();
                    }
                    Command::SubscribeInvites(ch) => {
                        swarm.behaviour_mut().subscribe_invites(ch);
                    }
                };
            }
            while swarm.behaviour_mut().poll_backend(cx).is_ready() {}
            while let Poll::Ready(Some(ev)) = swarm.poll_next_unpin(cx) {
                match ev {
                    SwarmEvent::NewListenAddr { .. } => notify(&mut sub_addresses),
                    SwarmEvent::ExpiredListenAddr { .. } => notify(&mut sub_addresses),
                    SwarmEvent::ConnectionEstablished { .. } => notify(&mut sub_connected_peers),
                    SwarmEvent::ConnectionClosed { .. } => notify(&mut sub_connected_peers),
                    _ => {}
                }
            }
            Poll::Pending
        });

        #[cfg(not(target_family = "wasm"))]
        let _task = async_global_executor::spawn(driver);
        #[cfg(target_family = "wasm")]
        wasm_bindgen_futures::spawn_local(async move {
            driver.await;
        });

        Ok(Self {
            frontend,
            peer,
            swarm: tx,
            #[cfg(not(target_family = "wasm"))]
            _task,
        })
    }

    /// Returns the [`PeerId`] of this [`Sdk`].
    pub fn peer_id(&self) -> &PeerId {
        &self.peer
    }

    /// Adds a new [`Multiaddr`] for a [`PeerId`].
    pub fn add_address(&self, peer: PeerId, addr: Multiaddr) {
        self.swarm
            .unbounded_send(Command::AddAddress(peer, addr))
            .ok();
    }

    /// Adds an external [`Multiaddr`] record for the local node.
    pub fn add_external_address(&self, addr: Multiaddr, score: AddressScore) {
        self.swarm
            .unbounded_send(Command::AddExternalAddress(addr, score))
            .ok();
    }

    /// Removes a [`Multiaddr`] of a [`PeerId`].
    pub fn remove_address(&self, peer: PeerId, addr: Multiaddr) {
        self.swarm
            .unbounded_send(Command::RemoveAddress(peer, addr))
            .ok();
    }

    /// Returns the list of [`Multiaddr`] the [`Sdk`] is listening on.
    pub fn addresses(&self) -> impl Future<Output = Vec<Multiaddr>> {
        let (tx, rx) = oneshot::channel();
        self.swarm.unbounded_send(Command::Addresses(tx)).unwrap();
        async move { rx.await.unwrap() }
    }

    /// Subscribe to address changes.
    pub fn subscribe_addresses(&self) -> impl Stream<Item = ()> {
        let (tx, rx) = mpsc::channel(1);
        self.swarm
            .unbounded_send(Command::SubscribeAddresses(tx))
            .unwrap();
        rx
    }

    /// Returns the local peers.
    pub fn local_peers(&self) -> impl Future<Output = BTreeSet<PeerId>> {
        let (tx, rx) = oneshot::channel();
        self.swarm.unbounded_send(Command::LocalPeers(tx)).unwrap();
        async move { rx.await.unwrap() }
    }

    /// Subscribes to local peer changes.
    pub fn subscribe_local_peers(&self) -> impl Stream<Item = ()> {
        let (tx, rx) = mpsc::channel(1);
        self.swarm
            .unbounded_send(Command::SubscribeLocalPeers(tx))
            .unwrap();
        rx
    }

    /// Returns the connected peers.
    pub fn connected_peers(&self) -> impl Future<Output = Vec<PeerId>> {
        let (tx, rx) = oneshot::channel();
        self.swarm
            .unbounded_send(Command::ConnectedPeers(tx))
            .unwrap();
        async move { rx.await.unwrap() }
    }

    /// Subscribes to local peer changes.
    pub fn subscribe_connected_peers(&self) -> impl Stream<Item = ()> {
        let (tx, rx) = mpsc::channel(1);
        self.swarm
            .unbounded_send(Command::SubscribeConnectedPeers(tx))
            .unwrap();
        rx
    }

    /// Clears and returns pending invitations.
    pub fn invites(&self) -> impl Future<Output = Vec<Invite>> {
        let (tx, rx) = oneshot::channel();
        self.swarm.unbounded_send(Command::Invites(tx)).unwrap();
        async move { rx.await.unwrap() }
    }

    /// Subscribe to invitations.
    pub fn subscribe_invites(&self) -> impl Stream<Item = ()> {
        let (tx, rx) = mpsc::channel(1);
        self.swarm
            .unbounded_send(Command::SubscribeInvites(tx))
            .unwrap();
        rx
    }

    /// Returns an iterator of [`DocId`].
    pub fn docs(&self, schema: String) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.frontend.docs_by_schema(schema)
    }

    /// Subscribes to document changes.
    pub fn subscribe_docs(&self) -> impl Stream<Item = ()> {
        self.frontend.subscribe()
    }

    /// Creates a new document with an initial [`Schema`].
    pub async fn create_doc(&self, schema: &str) -> Result<Doc> {
        let peer_id = self.peer_id();
        let doc = self
            .frontend
            .create_doc(*peer_id, schema, Keypair::generate())?
            .await;
        self.swarm
            .unbounded_send(Command::Subscribe(*doc.id()))
            .ok();
        Ok(Doc::new(doc, self.swarm.clone()))
    }

    /// Adds a document with a [`Schema`].
    pub fn add_doc(&self, id: DocId, schema: &str) -> Result<Doc> {
        let peer_id = self.peer_id();
        let doc = self.frontend.add_doc(id, peer_id, schema)?;
        self.swarm
            .unbounded_send(Command::Subscribe(*doc.id()))
            .ok();
        Ok(Doc::new(doc, self.swarm.clone()))
    }

    /// Returns a document handle.
    pub fn doc(&self, id: DocId) -> Result<Doc> {
        let doc = self.frontend.doc(id)?;
        Ok(Doc::new(doc, self.swarm.clone()))
    }

    /// Removes a document.
    pub fn remove_doc(&self, id: &DocId) -> Result<()> {
        self.frontend.remove_doc(id)
    }
}

/// Document handle.
#[derive(Clone)]
pub struct Doc {
    doc: tlfs_crdt::Doc,
    swarm: mpsc::UnboundedSender<Command>,
}

impl Doc {
    fn new(doc: tlfs_crdt::Doc, swarm: mpsc::UnboundedSender<Command>) -> Self {
        Self { doc, swarm }
    }

    /// Returns the document identifier.
    pub fn id(&self) -> &DocId {
        self.doc.id()
    }

    /// Returns a cursor for the document.
    pub fn cursor(&self) -> Cursor<'_> {
        self.doc.cursor()
    }

    /// Applies a transaction to the document.
    pub fn apply(&self, causal: Causal) -> Result<()> {
        self.doc.apply(&causal)?;
        self.swarm
            .unbounded_send(Command::Broadcast(*self.id(), causal))
            .ok();
        Ok(())
    }

    /// Invite peer. Make sure the peer has at least read permission before
    /// doing this.
    pub fn invite(&self, peer: PeerId) -> Result<()> {
        let schema = self.doc.schema()?;
        self.swarm
            .unbounded_send(Command::Invite(
                peer,
                *self.id(),
                schema.as_ref().name.to_string(),
            ))
            .unwrap();
        Ok(())
    }
}

enum Command {
    AddAddress(PeerId, Multiaddr),
    AddExternalAddress(Multiaddr, AddressScore),
    RemoveAddress(PeerId, Multiaddr),
    Addresses(oneshot::Sender<Vec<Multiaddr>>),
    SubscribeAddresses(mpsc::Sender<()>),
    LocalPeers(oneshot::Sender<BTreeSet<PeerId>>),
    SubscribeLocalPeers(mpsc::Sender<()>),
    ConnectedPeers(oneshot::Sender<Vec<PeerId>>),
    SubscribeConnectedPeers(mpsc::Sender<()>),
    Subscribe(DocId),
    Broadcast(DocId, Causal),
    Invite(PeerId, DocId, String),
    Invites(oneshot::Sender<Vec<Invite>>),
    SubscribeInvites(mpsc::Sender<()>),
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::pin::Pin;
    use std::time::Duration;

    #[async_std::test]
    async fn test_api() -> Result<()> {
        let mut lenses = vec![
            Lens::Make(Kind::Struct),
            Lens::AddProperty("todos".into()),
            Lens::Make(Kind::Table(PrimitiveKind::U64)).lens_in("todos"),
            Lens::Make(Kind::Struct).lens_map_value().lens_in("todos"),
            Lens::AddProperty("title".into())
                .lens_map_value()
                .lens_in("todos"),
            Lens::Make(Kind::Reg(PrimitiveKind::Str))
                .lens_in("title")
                .lens_map_value()
                .lens_in("todos"),
            Lens::AddProperty("complete".into())
                .lens_map_value()
                .lens_in("todos"),
            Lens::Make(Kind::Flag)
                .lens_in("complete")
                .lens_map_value()
                .lens_in("todos"),
        ];
        let packages = vec![Package::new(
            "todoapp".into(),
            8,
            &Lenses::new(lenses.clone()),
        )];
        let sdk = Sdk::memory(Ref::archive(&packages).as_bytes()).await?;
        let doc = sdk.create_doc("todoapp").await?;

        async_std::task::sleep(Duration::from_millis(100)).await;
        assert!(doc.cursor().can(sdk.peer_id(), Permission::Write)?);

        let docs = sdk.docs("todoapp".into()).collect::<Result<Vec<_>>>()?;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0], *doc.id());

        let title = "something that needs to be done";
        let op = doc
            .cursor()
            .field("todos")?
            .key_u64(0)?
            .field("title")?
            .assign_str(title)?;
        doc.apply(op)?;

        let value = doc
            .cursor()
            .field("todos")?
            .key_u64(0)?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);

        let mut local_peers = sdk.subscribe_local_peers();

        lenses.push(Lens::RenameProperty("todos".into(), "tasks".into()));
        let packages = vec![Package::new("todoapp".into(), 9, &Lenses::new(lenses))];
        let sdk2 = Sdk::memory(Ref::archive(&packages).as_bytes()).await?;
        let mut invites = sdk2.subscribe_invites();

        local_peers.next().await;
        let peer_id = sdk.local_peers().await.into_iter().next().unwrap();
        assert_eq!(peer_id, *sdk2.peer_id());
        tracing::info!("found local peer");

        let op = doc.cursor().say_can(Some(peer_id), Permission::Write)?;
        doc.apply(op)?;
        doc.invite(peer_id)?;

        invites.next().await;
        let invite = &sdk2.invites().await[0];
        assert_eq!(&invite.doc, doc.id());
        assert_eq!(&invite.schema, "todoapp");
        tracing::info!("received invite");
        let doc2 = sdk2.add_doc(invite.doc, &invite.schema)?;
        let mut sub = doc2.cursor().field("tasks")?.subscribe();
        let mut exit = false;
        while !exit {
            if let Some(iter) = Pin::new(&mut sub).next().await {
                for ev in iter.into_iter() {
                    if let Event::Insert(_) = ev {
                        exit = true;
                        break;
                    }
                }
            }
        }

        let value = doc2
            .cursor()
            .field("tasks")?
            .key_u64(0)?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);

        Ok(())
    }
}

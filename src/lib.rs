//! The Local First SDK.
//!
//! See the `tlfs_crdt` docs for details of how it works.
#![deny(missing_docs)]
mod sync;

pub use crate::sync::ToLibp2pKeypair;
pub use libp2p::Multiaddr;
pub use tlfs_crdt::{
    ArchivedSchema, Backend, Causal, Cursor, DocId, Event, Frontend, Keypair, Kind, Lens, Lenses,
    Package, PathBuf, PeerId, Permission, Primitive, PrimitiveKind, Ref, Schema, Subscriber,
};

use crate::sync::{Behaviour, ToLibp2pPublic};
use anyhow::Result;
use futures::{
    channel::{mpsc, oneshot},
    future::poll_fn,
    stream::Stream,
    Future,
};
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    Swarm,
};
use std::{pin::Pin, task::Poll};

/// Main entry point for `tlfs`.
pub struct Sdk {
    frontend: Frontend,
    peer: PeerId,
    swarm: mpsc::UnboundedSender<Command>,
}

impl Sdk {
    /// Creates a new persistent [`Sdk`] instance.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn persistent(db: &std::path::Path, package: &[u8]) -> Result<Self> {
        let (sdk, driver) = Self::new(
            std::sync::Arc::new(tlfs_crdt::FileStorage::new(db)),
            package,
        )
        .await?;
        async_global_executor::spawn::<_, ()>(driver).detach();

        Ok(sdk)
    }

    /// Create a new in-memory [`Sdk`] instance.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn memory(package: &[u8]) -> Result<Self> {
        let (sdk, driver) = Self::new(
            std::sync::Arc::new(tlfs_crdt::MemStorage::default()),
            package,
        )
        .await?;
        async_global_executor::spawn::<_, ()>(driver).detach();

        Ok(sdk)
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn new(
        storage: std::sync::Arc<dyn tlfs_crdt::Storage>,
        package: &[u8],
    ) -> Result<(Self, impl Future<Output = ()>)> {
        use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};
        tracing_log::LogTracer::init().ok();
        let env = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "info".to_owned());
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
            .with_env_filter(EnvFilter::new(env))
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).ok();
        log_panics::init();

        let backend = Backend::new(storage, package)?;
        let frontend = backend.frontend();

        let keypair = frontend.default_keypair()?;
        let peer = keypair.peer_id();
        tracing::info!("our peer id is: {}", peer);

        let transport = libp2p::development_transport(keypair.to_libp2p()).await?;

        Self::new_with_transport(
            backend,
            frontend,
            peer,
            transport,
            std::iter::once("/ip4/0.0.0.0/tcp/0".parse().unwrap()),
        )
        .await
    }

    /// Creates a new [`Sdk`] instance from the given [`Backend`], [`Frontend`] and libp2p
    /// transport.
    pub async fn new_with_transport(
        backend: Backend,
        frontend: Frontend,
        peer: PeerId,
        transport: Boxed<(libp2p::PeerId, StreamMuxerBox)>,
        listen_on: impl Iterator<Item = Multiaddr>,
    ) -> Result<(Self, impl Future<Output = ()>)> {
        let behaviour = Behaviour::new(backend)?;
        let mut swarm = Swarm::new(transport, behaviour, peer.to_libp2p().to_peer_id());
        for i in listen_on {
            swarm.listen_on(i)?;
        }

        let (tx, mut rx) = mpsc::unbounded();
        let driver = poll_fn(move |cx| {
            while let Poll::Ready(Some(cmd)) = Pin::new(&mut rx).poll_next(cx) {
                match cmd {
                    Command::AddAddress(peer, addr) => {
                        swarm.behaviour_mut().add_address(&peer, addr);
                        if let Err(err) = swarm.dial(peer.to_libp2p().to_peer_id()) {
                            tracing::error!("{}", err);
                        }
                    }
                    Command::RemoveAddress(peer, addr) => {
                        swarm.behaviour_mut().remove_address(&peer, &addr)
                    }
                    Command::Addresses(ch) => {
                        let addrs = swarm.listeners().cloned().collect::<Vec<_>>();
                        ch.send(addrs).ok();
                    }
                    Command::Subscribe(doc) => {
                        swarm.behaviour_mut().subscribe(&doc);
                    }
                    Command::Broadcast(doc, causal) => {
                        swarm.behaviour_mut().broadcast(&doc, causal).ok();
                    }
                };
            }
            while swarm.behaviour_mut().poll_backend(cx).is_ready() {}
            while Pin::new(&mut swarm).poll_next(cx).is_ready() {}
            Poll::Pending
        });

        Ok((
            Self {
                frontend,
                peer,
                swarm: tx,
            },
            driver,
        ))
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

    /// Removes a [`Multiaddr`] of a [`PeerId`].
    pub fn remove_address(&self, peer: PeerId, addr: Multiaddr) {
        self.swarm
            .unbounded_send(Command::RemoveAddress(peer, addr))
            .ok();
    }

    /// Returns the list of [`Multiaddr`] the [`Sdk`] is listening on.
    pub async fn addresses(&self) -> Vec<Multiaddr> {
        let (tx, rx) = oneshot::channel();
        if let Ok(()) = self.swarm.unbounded_send(Command::Addresses(tx)) {
            if let Ok(addrs) = rx.await {
                return addrs;
            }
        }
        vec![]
    }

    /// Returns an iterator of [`DocId`].
    pub fn docs(&self, schema: String) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.frontend.docs_by_schema(schema)
    }

    /// Creates a new document with an initial [`Schema`].
    pub fn create_doc(&self, schema: &str) -> Result<Doc> {
        let peer_id = self.peer_id();
        let doc = self
            .frontend
            .create_doc(*peer_id, schema, Keypair::generate())?;
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
}

enum Command {
    AddAddress(PeerId, Multiaddr),
    RemoveAddress(PeerId, Multiaddr),
    Addresses(oneshot::Sender<Vec<Multiaddr>>),
    Subscribe(DocId),
    Broadcast(DocId, Causal),
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use futures::StreamExt;
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
        let doc = sdk.create_doc("todoapp")?;

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

        lenses.push(Lens::RenameProperty("todos".into(), "tasks".into()));
        let packages = vec![Package::new("todoapp".into(), 9, &Lenses::new(lenses))];
        let sdk2 = Sdk::memory(Ref::archive(&packages).as_bytes()).await?;

        let op = doc
            .cursor()
            .say_can(Some(*sdk2.peer_id()), Permission::Write)?;
        doc.apply(op)?;

        for addr in sdk.addresses().await {
            sdk2.add_address(*sdk.peer_id(), addr);
        }
        let doc2 = sdk2.add_doc(*doc.id(), "todoapp")?;
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

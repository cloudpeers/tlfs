//! The Local First SDK.
//!
//! See the `tlfs_crdt` docs for details of how it works.
#![deny(missing_docs)]
mod sync;

pub use libp2p::Multiaddr;
pub use tlfs_crdt::{
    Causal, Cursor, DocId, Event, Keypair, Kind, Lens, Package, PeerId, Permission, PrimitiveKind,
    Schema, Subscriber,
};

use crate::sync::{Behaviour, ToLibp2pKeypair, ToLibp2pPublic};
use anyhow::Result;
use futures::channel::{mpsc, oneshot};
use futures::future::poll_fn;
use futures::stream::Stream;
use libp2p::Swarm;
use std::path::Path;
use std::pin::Pin;
use std::task::Poll;
use tlfs_crdt::{Backend, Frontend};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

/// Main entry point for `tlfs`.
pub struct Sdk {
    frontend: Frontend,
    peer: PeerId,
    swarm: mpsc::UnboundedSender<Command>,
}

impl Sdk {
    /// Creates a new persistent [`Sdk`] instance.
    pub async fn persistent(db: &Path, package: &Path) -> Result<Self> {
        Self::new(
            sled::Config::new().path(db).open()?,
            &std::fs::read(package)?,
        )
        .await
    }

    /// Create a new in-memory [`Sdk`] instance.
    pub async fn memory(package: &[u8]) -> Result<Self> {
        Self::new(sled::Config::new().temporary(true).open()?, package).await
    }

    async fn new(db: sled::Db, package: &[u8]) -> Result<Self> {
        tracing_log::LogTracer::init().ok();
        let env = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "info".to_owned());
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
            .with_env_filter(EnvFilter::new(env))
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).ok();
        log_panics::init();

        let backend = Backend::new(db, package)?;
        let frontend = backend.frontend();

        let keypair = frontend.default_keypair()?;
        let peer = keypair.peer_id();
        tracing::info!("our peer id is: {}", peer);

        let transport = libp2p::development_transport(keypair.to_libp2p()).await?;
        let behaviour = Behaviour::new(backend)?;
        let mut swarm = Swarm::new(transport, behaviour, peer.to_libp2p().to_peer_id());
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())?;

        let (tx, mut rx) = mpsc::unbounded();
        async_global_executor::spawn::<_, ()>(poll_fn(move |cx| {
            while let Poll::Ready(Some(cmd)) = Pin::new(&mut rx).poll_next(cx) {
                match cmd {
                    Command::AddAddress(peer, addr) => {
                        swarm.behaviour_mut().add_address(&peer, addr);
                        if let Err(err) = swarm.dial(&peer.to_libp2p().to_peer_id()) {
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
        }))
        .detach();

        Ok(Self {
            frontend,
            peer,
            swarm: tx,
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
    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> {
        self.frontend.docs()
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
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::time::Duration;
    use tlfs_crdt::Ref;

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
        let package = Package::new(
            "todoapp".into(),
            vec![("0.1.0".into(), 8)],
            Ref::archive(&lenses).into(),
        );
        let sdk = Sdk::memory(Ref::<Vec<Package>>::archive(&vec![package]).as_bytes()).await?;
        let doc = sdk.create_doc("todoapp")?;

        async_std::task::sleep(Duration::from_millis(100)).await;
        assert!(doc.cursor().can(sdk.peer_id(), Permission::Write)?);

        let docs = sdk.docs().collect::<Result<Vec<_>>>()?;
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
        let package = Package::new(
            "todoapp".into(),
            vec![("0.1.0".into(), 9)],
            Ref::archive(&lenses).into(),
        );
        let sdk2 = Sdk::memory(Ref::archive(&vec![package]).as_bytes()).await?;

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

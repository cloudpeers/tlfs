mod sync;

pub use libp2p::Multiaddr;
pub use tlfs_crdt::{Causal, DocId, Hash, Keypair, Kind, Lens, PeerId, Permission, PrimitiveKind};

use crate::sync::{Behaviour, ToLibp2pKeypair, ToLibp2pPublic};
use anyhow::Result;
use futures::channel::{mpsc, oneshot};
use futures::future::poll_fn;
use futures::stream::Stream;
use libp2p::Swarm;
use std::pin::Pin;
use std::task::Poll;
use tlfs_crdt::{Backend, Doc, Frontend};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

pub struct Migrate {
    backend: Backend,
}

impl Migrate {
    pub fn new(db: sled::Db) -> Result<Self> {
        tracing_log::LogTracer::init().ok();
        let env = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "info".to_owned());
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
            .with_env_filter(EnvFilter::new(env))
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).ok();
        log_panics::init();

        let backend = Backend::new(db)?;
        Ok(Self { backend })
    }

    pub fn memory() -> Result<Self> {
        Self::new(sled::Config::new().temporary(true).open()?)
    }

    pub fn register(&self, lenses: Vec<Lens>) -> Result<Hash> {
        self.backend.register(lenses)
    }

    pub fn migrate(&mut self, doc: &DocId, hash: &Hash) -> Result<()> {
        self.backend.transform(doc, hash)
    }

    pub async fn finish(self) -> Result<Sdk> {
        Sdk::new(self.backend).await
    }
}

pub struct Sdk {
    frontend: Frontend,
    peer: PeerId,
    swarm: mpsc::UnboundedSender<Command>,
}

impl Sdk {
    async fn new(backend: Backend) -> Result<Self> {
        let frontend = backend.frontend();

        let peer = frontend.generate_keypair()?;
        let keypair = frontend.keypair(&peer)?;

        let transport = libp2p::development_transport(keypair.to_libp2p()).await?;
        let behaviour = Behaviour::new(backend);
        let mut swarm = Swarm::new(transport, behaviour, peer.to_libp2p().into_peer_id());
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse().unwrap())?;

        let (tx, mut rx) = mpsc::unbounded();
        async_global_executor::spawn::<_, ()>(poll_fn(move |cx| {
            while let Poll::Ready(Some(cmd)) = Pin::new(&mut rx).poll_next(cx) {
                match cmd {
                    Command::AddAddress(peer, addr) => {
                        swarm.behaviour_mut().add_address(&peer, addr)
                    }
                    Command::RemoveAddress(peer, addr) => {
                        swarm.behaviour_mut().remove_address(&peer, &addr)
                    }
                    Command::Addresses(ch) => {
                        let addrs = swarm.listeners().cloned().collect::<Vec<_>>();
                        ch.send(addrs).ok();
                    }
                    Command::Unjoin(doc, peer) => {
                        swarm.behaviour_mut().request_unjoin(&peer, doc).ok();
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

    pub fn peer_id(&self) -> &PeerId {
        &self.peer
    }

    pub fn add_address(&self, peer: PeerId, addr: Multiaddr) {
        self.swarm
            .unbounded_send(Command::AddAddress(peer, addr))
            .ok();
    }

    pub fn remove_address(&self, peer: PeerId, addr: Multiaddr) {
        self.swarm
            .unbounded_send(Command::RemoveAddress(peer, addr))
            .ok();
    }

    pub async fn addresses(&self) -> Vec<Multiaddr> {
        let (tx, rx) = oneshot::channel();
        if let Ok(()) = self.swarm.unbounded_send(Command::Addresses(tx)) {
            if let Ok(addrs) = rx.await {
                return addrs;
            }
        }
        vec![]
    }

    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.frontend.docs()
    }

    pub fn create_doc(&mut self, schema: &Hash) -> Result<Doc> {
        let peer_id = self.peer_id();
        self.frontend
            .create_doc(*peer_id, schema, Keypair::generate())
    }

    pub fn add_doc(&self, id: DocId, schema: &Hash) -> Result<Doc> {
        let peer_id = self.peer_id();
        self.frontend.add_doc(id, peer_id, schema)
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        self.frontend.doc(id)
    }

    pub fn remove_doc(&self, id: DocId) -> Result<()> {
        self.frontend.remove_doc(id)
    }

    pub fn unjoin(&self, doc: DocId, peer: PeerId) {
        self.swarm.unbounded_send(Command::Unjoin(doc, peer)).ok();
    }
}

enum Command {
    AddAddress(PeerId, Multiaddr),
    RemoveAddress(PeerId, Multiaddr),
    Addresses(oneshot::Sender<Vec<Multiaddr>>),
    Unjoin(DocId, PeerId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[async_std::test]
    async fn test_api() -> Result<()> {
        let migrate = Migrate::memory()?;
        let lenses = vec![
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
        let hash = migrate.register(lenses.clone())?;

        let mut sdk = migrate.finish().await?;
        let doc = sdk.create_doc(&hash)?;

        // TODO: subscription api
        async_std::task::sleep(Duration::from_millis(100)).await;
        assert!(doc.cursor().can(sdk.peer_id(), Permission::Write)?);

        let title = "something that needs to be done";
        let op = doc
            .cursor()
            .field("todos")?
            .key_u64(0)?
            .field("title")?
            .assign_str(title)?;
        doc.apply(&op)?;

        let value = doc
            .cursor()
            .field("todos")?
            .key_u64(0)?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);

        let sdk2 = Migrate::memory()?;
        sdk2.register(lenses)?;
        let sdk2 = sdk2.finish().await?;
        let op = doc
            .cursor()
            .say_can(Some(*sdk2.peer_id()), Permission::Write)?;
        doc.apply(&op)?;

        for addr in sdk.addresses().await {
            sdk2.add_address(*sdk.peer_id(), addr);
        }
        let doc2 = sdk2.add_doc(*doc.id(), &hash)?;

        async_std::task::sleep(Duration::from_millis(100)).await;
        sdk2.unjoin(*doc.id(), *sdk.peer_id());

        // TODO: subscription api
        async_std::task::sleep(Duration::from_millis(1000)).await;

        let value = doc2
            .cursor()
            .field("todos")?
            .key_u64(0)?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);

        Ok(())
    }
}

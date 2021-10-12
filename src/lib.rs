mod secrets;
mod sync;

pub use crate::secrets::{Metadata, Secrets};
pub use crate::sync::{Behaviour, ToLibp2pKeypair, ToLibp2pPublic};

use anyhow::Result;
use futures::channel::mpsc;
use futures::future::poll_fn;
use futures::stream::Stream;
use libp2p::Swarm;
use std::pin::Pin;
use std::task::Poll;
use tlfs_crdt::{Backend, Causal, Doc, DocId, Frontend, Hash, Lens, PeerId};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

pub struct Sdk {
    frontend: Frontend,
    secrets: Secrets,
    swarm: mpsc::UnboundedSender<Command>,
}

impl Sdk {
    pub async fn new(db: sled::Db) -> Result<Self> {
        tracing_log::LogTracer::init().ok();
        let env = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "info".to_owned());
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
            .with_env_filter(EnvFilter::new(env))
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).ok();
        log_panics::init();

        let backend = Backend::new(db.clone())?;
        let frontend = backend.frontend();
        let secrets = Secrets::new(db.open_tree("secrets")?);

        if secrets.keypair(Metadata::new())?.is_none() {
            secrets.generate_keypair(Metadata::new())?;
        }
        let keypair = secrets.keypair(Metadata::new())?.unwrap();

        let transport = libp2p::development_transport(keypair.to_libp2p()).await?;
        let behaviour = Behaviour::new(backend, secrets.clone())?;
        let mut swarm = Swarm::new(
            transport,
            behaviour,
            keypair.peer_id().to_libp2p().into_peer_id(),
        );

        let (tx, mut rx) = mpsc::unbounded();
        async_global_executor::spawn::<_, ()>(poll_fn(move |cx| {
            while let Poll::Ready(Some(cmd)) = Pin::new(&mut rx).poll_next(cx) {
                match cmd {
                    Command::Publish(causal) => swarm.behaviour_mut().send_delta(&causal).ok(),
                    Command::Subscribe(id) => swarm.behaviour_mut().subscribe_doc(&id).ok(),
                };
            }
            while let Poll::Ready(_) = swarm.behaviour_mut().poll_backend(cx) {}
            while let Poll::Ready(_) = Pin::new(&mut swarm).poll_next(cx) {}
            Poll::Pending
        }))
        .detach();

        Ok(Self {
            frontend,
            secrets,
            swarm: tx,
        })
    }

    pub async fn memory() -> Result<Self> {
        Self::new(sled::Config::new().temporary(true).open()?).await
    }

    pub fn peer_id(&self) -> Result<PeerId> {
        Ok(self.secrets.keypair(Metadata::new())?.unwrap().peer_id())
    }

    pub fn register(&self, lenses: Vec<Lens>) -> Result<Hash> {
        self.frontend.register(lenses)
    }

    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.frontend.docs()
    }

    pub fn create_doc(&self) -> Result<Doc> {
        let peer_id = self.peer_id()?;
        let doc = self.frontend.create_doc(peer_id)?;
        self.swarm
            .unbounded_send(Command::Subscribe(*doc.id()))
            .ok();
        Ok(doc)
    }

    pub fn add_doc(&self, id: DocId) -> Result<Doc> {
        let peer_id = self.peer_id()?;
        let doc = self.frontend.add_doc(id, peer_id)?;
        self.swarm
            .unbounded_send(Command::Subscribe(*doc.id()))
            .ok();
        Ok(doc)
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        let doc = self.frontend.doc(id)?;
        self.swarm
            .unbounded_send(Command::Subscribe(*doc.id()))
            .ok();
        Ok(doc)
    }

    pub fn remove_doc(&self, id: DocId) -> Result<()> {
        self.frontend.remove_doc(id)
    }

    pub fn apply(&self, causal: Causal) -> Result<()> {
        self.frontend.apply(&causal)?;
        self.swarm.unbounded_send(Command::Publish(causal)).ok();
        Ok(())
    }
}

enum Command {
    Publish(Causal),
    Subscribe(DocId),
}

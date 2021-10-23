use crate::acl::{Acl, Engine, Permission};
use crate::crdt::{Causal, CausalContext, Crdt};
use crate::crypto::Keypair;
use crate::cursor::Cursor;
use crate::id::{DocId, PeerId};
use crate::lens::{Lens, Lenses};
use crate::path::Path;
use crate::registry::{Hash, Registry, EMPTY_HASH, EMPTY_LENSES, EMPTY_SCHEMA};
use crate::schema::Schema;
use crate::util::Ref;
use anyhow::{anyhow, Result};
use futures::channel::mpsc;
use futures::prelude::*;
use rkyv::Archived;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone)]
struct Docs(sled::Tree);

impl Docs {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.0.iter().keys().filter_map(|r| match r {
            Ok(k) if k[32] == 1 => Some(Ok(DocId::new((&k[..32]).try_into().unwrap()))),
            Ok(_) => None,
            Err(err) => Some(Err(err.into())),
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = Result<PeerId>> + '_ {
        self.0.iter().keys().filter_map(|r| match r {
            Ok(k) if k[32] == 2 => Some(Ok(PeerId::new((&k[..32]).try_into().unwrap()))),
            Ok(_) => None,
            Err(err) => Some(Err(err.into())),
        })
    }

    pub fn create(&self, id: &DocId, schema: &Hash) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert(key, schema.as_bytes())?;
        key[32] = 1;
        self.0.insert(key, id.as_ref())?;
        Ok(())
    }

    pub fn remove(&self, id: &DocId) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.remove(key)?;
        key[32] = 1;
        self.0.remove(key)?;
        Ok(())
    }

    pub fn schema_id(&self, id: &DocId) -> Result<Hash> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        Ok(self
            .0
            .get(key)?
            .map(|b| {
                let b: [u8; 32] = b.as_ref().try_into().unwrap();
                b.into()
            })
            .unwrap_or_else(|| EMPTY_HASH.into()))
    }

    pub fn set_schema_id(&self, id: &DocId, hash: &Hash) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert(key, hash.as_bytes())?;
        Ok(())
    }

    pub fn peer_id(&self, id: &DocId) -> Result<PeerId> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 1;
        let v = self.0.get(key)?.unwrap();
        Ok(PeerId::new(v.as_ref().try_into().unwrap()))
    }

    pub fn set_peer_id(&self, id: &DocId, peer: &PeerId) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 1;
        self.0.insert(key, peer.as_ref())?;
        Ok(())
    }

    pub fn add_keypair(&self, keypair: Keypair) -> Result<PeerId> {
        let peer = keypair.peer_id();
        let mut key = [0; 33];
        key[..32].copy_from_slice(peer.as_ref());
        key[32] = 2;
        self.0.insert(key, keypair.as_ref())?;
        Ok(peer)
    }

    pub fn keypair(&self, peer: &PeerId) -> Result<Keypair> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(peer.as_ref());
        key[32] = 2;
        let keypair = self.0.get(key)?.unwrap();
        Ok(Keypair::new(keypair.as_ref().try_into().unwrap()))
    }

    pub fn remove_keypair(&self, peer: &PeerId) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(peer.as_ref());
        key[32] = 2;
        self.0.remove(key)?;
        Ok(())
    }
}

struct DebugDoc<'a>(&'a Docs, DocId);

impl<'a> std::fmt::Debug for DebugDoc<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = f.debug_struct("Doc");
        s.field("peer_id", &self.0.peer_id(&self.1).unwrap());
        s.field("schema", &self.0.schema_id(&self.1).unwrap());
        s.finish()
    }
}

struct DebugDocs<'a>(&'a Docs);

impl<'a> std::fmt::Debug for DebugDocs<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut docs = f.debug_map();
        for e in self.0.docs() {
            let doc = e.unwrap();
            docs.entry(&doc, &DebugDoc(self.0, doc));
        }
        docs.finish()
    }
}

struct DebugKeys<'a>(&'a Docs);

impl<'a> std::fmt::Debug for DebugKeys<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut keys = f.debug_set();
        for e in self.0.keys() {
            let peer = e.unwrap();
            keys.entry(&peer);
        }
        keys.finish()
    }
}

impl std::fmt::Debug for Docs {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Docs")
            .field("docs", &DebugDocs(self))
            .field("keys", &DebugKeys(self))
            .finish()
    }
}

/// The crdt [`Backend`] is the main entry point to interact with this crate.
pub struct Backend {
    registry: Registry,
    crdt: Crdt,
    docs: Docs,
    engine: Engine,
    tx: mpsc::Sender<()>,
    rx: mpsc::Receiver<()>,
}

impl Backend {
    /// Creates a new [`Backend`] from a [`sled::Db`].
    pub fn new(db: sled::Db) -> Result<Self> {
        let registry = Registry::new(db.open_tree("lenses")?);
        let docs = Docs::new(db.open_tree("docs")?);
        let acl = Acl::new(db.open_tree("acl")?);
        let crdt = Crdt::new(
            db.open_tree("store")?,
            db.open_tree("expired")?,
            acl.clone(),
        );
        let engine = Engine::new(acl)?;
        let (tx, rx) = mpsc::channel(1);
        let mut me = Self {
            registry,
            crdt,
            docs,
            engine,
            tx,
            rx,
        };
        me.update_acl()?;
        Ok(me)
    }

    /// Creates a new in-memory backend for testing purposes.
    #[cfg(test)]
    pub fn memory() -> Result<Self> {
        use tracing_subscriber::fmt::format::FmtSpan;
        use tracing_subscriber::EnvFilter;
        tracing_log::LogTracer::init().ok();
        let env = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "info".to_owned());
        let subscriber = tracing_subscriber::FmtSubscriber::builder()
            .with_span_events(FmtSpan::ACTIVE | FmtSpan::CLOSE)
            .with_env_filter(EnvFilter::new(env))
            .with_writer(std::io::stderr)
            .finish();
        tracing::subscriber::set_global_default(subscriber).ok();
        log_panics::init();
        Self::new(sled::Config::new().temporary(true).open()?)
    }

    /// Registers lenses in the lens registry.
    pub fn register(&self, lenses: Vec<Lens>) -> Result<Hash> {
        self.registry
            .register(Ref::archive(&Lenses::new(lenses)).as_bytes())
    }

    /// Returns a reference to the lens registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    fn update_acl(&mut self) -> Result<()> {
        for res in self.crdt.iter() {
            let key = res?;
            let path = Path::new(&key[..]);
            self.engine.add_policy(path);
        }
        self.engine.update_acl()
    }

    /// Applies a remote change received from a peer.
    pub fn join(
        &mut self,
        peer_id: &PeerId,
        doc: &DocId,
        causal_schema: &Hash,
        mut causal: Causal,
    ) -> Result<()> {
        let doc_schema_id = self.docs.schema_id(doc)?;
        let doc_lenses = self
            .registry
            .lenses(&doc_schema_id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into()));
        let schema = self.registry.schema(causal_schema)?;
        let lenses = self.registry.lenses(causal_schema)?;
        let (schema, lenses) = match (schema, lenses) {
            (Some(schema), Some(lenses)) => (schema, lenses),
            _ => {
                if *causal_schema.as_bytes() == EMPTY_HASH {
                    (
                        Ref::new(EMPTY_SCHEMA.to_vec().into()),
                        Ref::new(EMPTY_LENSES.to_vec().into()),
                    )
                } else {
                    return Err(anyhow!("missing lenses with hash {}", causal_schema));
                }
            }
        };
        if !schema.as_ref().validate(&causal) {
            return Err(anyhow!("crdt failed schema validation"));
        }
        causal.transform(lenses.as_ref(), doc_lenses.as_ref());
        self.crdt.join_policy(&causal)?;
        self.update_acl()?;
        self.crdt.join(peer_id, &causal)?;
        Ok(())
    }

    /// Returns the changes required to bring a peer up to speed.
    pub fn unjoin(
        &self,
        peer_id: &PeerId,
        doc: &DocId,
        ctx: &Archived<CausalContext>,
    ) -> Result<Causal> {
        self.crdt.unjoin(peer_id, doc, ctx)
    }

    /// Transforms a document into a the [`Schema`] identified by [`struct@Hash`].
    pub fn transform(&mut self, id: &DocId, schema_id: &Hash) -> Result<()> {
        let doc_schema_id = self.docs.schema_id(id)?;
        let doc_lenses = self
            .registry
            .lenses(&doc_schema_id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into()));
        let lenses = self
            .registry
            .lenses(schema_id)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", &schema_id))?;
        self.crdt
            .transform(id, doc_lenses.as_ref(), lenses.as_ref())?;
        self.docs.set_schema_id(id, schema_id)?;
        Ok(())
    }

    /// Returns a clonable [`Frontend`].
    pub fn frontend(&self) -> Frontend {
        Frontend::new(
            self.crdt.clone(),
            self.docs.clone(),
            self.registry.clone(),
            self.tx.clone(),
        )
    }
}

impl Future for Backend {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if Pin::new(&mut self.rx).poll_next(cx).is_ready() {
            Poll::Ready(self.update_acl())
        } else {
            Poll::Pending
        }
    }
}

/// Clonable [`Frontend`].
#[derive(Clone)]
pub struct Frontend {
    crdt: Crdt,
    docs: Docs,
    registry: Registry,
    tx: mpsc::Sender<()>,
}

impl Frontend {
    fn new(crdt: Crdt, docs: Docs, registry: Registry, tx: mpsc::Sender<()>) -> Self {
        Self {
            crdt,
            docs,
            registry,
            tx,
        }
    }

    /// Adds a [`Keypair`].
    pub fn add_keypair(&self, key: Keypair) -> Result<PeerId> {
        self.docs.add_keypair(key)
    }

    /// Generates a new [`Keypair`].
    pub fn generate_keypair(&self) -> Result<PeerId> {
        self.add_keypair(Keypair::generate())
    }

    /// Returns the [`Keypair`] matching [`PeerId`].
    pub fn keypair(&self, peer: &PeerId) -> Result<Keypair> {
        self.docs.keypair(peer)
    }

    /// Removes the [`Keypair`] matching [`PeerId`].
    pub fn remove_keypair(&self, peer: &PeerId) -> Result<()> {
        self.docs.remove_keypair(peer)
    }

    /// Returns an iterator of [`DocId`].
    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.docs.docs()
    }

    /// Creates a new document using [`Keypair`] with initial schema and owner.
    pub fn create_doc(&self, owner: PeerId, schema: &Hash, la: Keypair) -> Result<Doc> {
        let id = DocId::new(la.peer_id().into());
        self.docs.create(&id, schema)?;
        let schema = self.schema(schema)?;
        let doc = Doc::new(id, self.clone(), la, schema);
        let delta = doc.cursor().say_can(Some(owner), Permission::Own)?;
        self.apply(&id, &delta)?;
        self.set_peer_id(&id, &owner)?;
        self.doc(id)
    }

    /// Adds an existing document identified by [`DocId`] with schema and associates the local
    /// keypair identified by [`PeerId`].
    pub fn add_doc(&self, id: DocId, peer: &PeerId, schema: &Hash) -> Result<Doc> {
        self.docs.set_peer_id(&id, peer)?;
        self.docs.set_schema_id(&id, schema)?;
        self.doc(id)
    }

    /// Removes a document identified by [`DocId`].
    pub fn remove_doc(&self, id: &DocId) -> Result<()> {
        self.crdt.remove(id)?;
        self.docs.remove(id)?;
        Ok(())
    }

    /// Returns the local [`PeerId`] associated with a document.
    pub fn peer_id(&self, id: &DocId) -> Result<PeerId> {
        self.docs.peer_id(id)
    }

    /// Changes the associated [`Keypair`] for a document.
    pub fn set_peer_id(&self, id: &DocId, peer: &PeerId) -> Result<()> {
        self.docs.set_peer_id(id, peer)
    }

    /// Returns the current schema identifier of a document.
    pub fn schema_id(&self, id: &DocId) -> Result<Hash> {
        self.docs.schema_id(id)
    }

    /// Returns the current lenses of a document.
    pub fn lenses(&self, id: &Hash) -> Result<Ref<Lenses>> {
        Ok(self
            .registry
            .lenses(id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into())))
    }

    /// Returns the current schema of a document.
    pub fn schema(&self, id: &Hash) -> Result<Ref<Schema>> {
        Ok(self
            .registry
            .schema(id)?
            .unwrap_or_else(|| Ref::new(EMPTY_SCHEMA.as_ref().into())))
    }

    /// Computes the [`CausalContext`] to sync with a remote peer.
    pub fn ctx(&self, id: &DocId) -> Result<CausalContext> {
        self.crdt.ctx(id)
    }

    /// Opens a document.
    pub fn doc(&self, id: DocId) -> Result<Doc> {
        let peer_id = self.peer_id(&id)?;
        self.doc_as(id, &peer_id)
    }

    /// Opens a document with a local keypair identified by [`PeerId`].
    pub fn doc_as(&self, id: DocId, peer_id: &PeerId) -> Result<Doc> {
        let hash = self.schema_id(&id)?;
        let schema = self.schema(&hash)?;
        let key = self.keypair(peer_id)?;
        Ok(Doc::new(id, self.clone(), key, schema))
    }

    /// Applies a local change to a document.
    pub fn apply(&self, doc: &DocId, causal: &Causal) -> Result<()> {
        let peer = self.peer_id(doc)?;
        self.crdt.join(&peer, causal)?;
        self.tx.clone().send(()).now_or_never();
        Ok(())
    }
}

impl std::fmt::Debug for Frontend {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Frontend")
            .field("crdt", &self.crdt)
            .field("docs", &self.docs)
            .finish_non_exhaustive()
    }
}

/// A clonable document handle.
#[derive(Debug, Clone)]
pub struct Doc {
    id: DocId,
    frontend: Frontend,
    key: Keypair,
    schema: Ref<Schema>,
}

impl Doc {
    fn new(id: DocId, frontend: Frontend, key: Keypair, schema: Ref<Schema>) -> Self {
        Self {
            id,
            frontend,
            key,
            schema,
        }
    }

    /// Returns the [`DocId`].
    pub fn id(&self) -> &DocId {
        &self.id
    }

    /// Computes the [`CausalContext`] to sync with a remote peer.
    pub fn ctx(&self) -> Result<CausalContext> {
        self.frontend.ctx(&self.id)
    }

    /// Returns a cursor for the document.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor::new(self.key, self.id, self.schema.as_ref(), &self.frontend.crdt)
    }

    /// Applies a local change to the document.
    pub fn apply(&self, causal: &Causal) -> Result<()> {
        self.frontend.apply(&self.id, causal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Kind, Lens, Permission, PrimitiveKind};

    #[async_std::test]
    async fn test_api() -> Result<()> {
        let mut sdk = Backend::memory()?;
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
        let hash = sdk.register(lenses.clone())?;

        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, &hash, Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&peer, Permission::Write)?);

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

        let mut sdk2 = Backend::memory()?;
        sdk2.register(lenses)?;
        let peer2 = sdk2.frontend().generate_keypair()?;
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;

        let doc2 = sdk2.frontend().add_doc(*doc.id(), &peer2, &hash)?;
        let ctx = Ref::archive(&doc2.ctx()?);
        let delta = sdk.unjoin(&peer2, doc2.id(), ctx.as_ref())?;
        sdk2.join(&peer, doc.id(), &hash, delta)?;

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

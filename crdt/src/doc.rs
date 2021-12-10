use crate::acl::{Acl, Engine, Permission};
use crate::crdt::{Causal, CausalContext, Crdt};
use crate::crypto::Keypair;
use crate::cursor::Cursor;
use crate::id::{DocId, PeerId};
use crate::lens::LensesRef;
use crate::path::Path;
use crate::radixdb::{BlobMap, BlobSet, Storage};
use crate::registry::{Expanded, Hash, Registry};
use crate::util::Ref;
use crate::MemStorage;
use anyhow::{anyhow, Result};
use futures::channel::{mpsc, oneshot};
use futures::prelude::*;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug))]
pub struct SchemaInfo {
    name: String,
    version: u32,
    hash: [u8; 32],
}

impl SchemaInfo {
    pub fn new(name: String, version: u32, hash: Hash) -> Self {
        Self {
            name,
            version,
            hash: hash.into(),
        }
    }
}

impl ArchivedSchemaInfo {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn hash(&self) -> Hash {
        self.hash.into()
    }
}

#[derive(Clone)]
struct Docs(BlobMap);

impl Docs {
    pub fn new(tree: BlobMap) -> Self {
        Self(tree)
    }

    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.0.iter().filter_map(|(k, _)| {
            if k[32] == 1 {
                Some(Ok(DocId::new((&k[..32]).try_into().unwrap())))
            } else {
                None
            }
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = Result<PeerId>> + '_ {
        self.0.iter().filter_map(|(k, _)| {
            if k[32] == 2 {
                Some(Ok(PeerId::new((&k[..32]).try_into().unwrap())))
            } else {
                None
            }
        })
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

    pub fn docs_by_schema(&self, schema: String) -> impl Iterator<Item = Result<DocId>> + '_ {
        let docs = self.clone();
        self.docs()
            .map(move |res| {
                let id = res?;
                let info = docs.schema(&id)?;
                Ok((id, info))
            })
            .filter_map(move |res| match res {
                Ok((id, info)) if info.as_ref().name() == schema => Some(Ok(id)),
                Ok(_) => None,
                Err(err) => Some(Err(err)),
            })
    }

    pub fn schema(&self, id: &DocId) -> Result<Ref<SchemaInfo>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        let schema = self.0.get(key)?.unwrap();
        Ok(Ref::new(schema))
    }

    pub fn set_schema(&self, id: &DocId, schema: &SchemaInfo) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert_archived(key, schema)?;
        Ok(())
    }

    pub fn peer_id(&self, id: &DocId) -> Result<PeerId> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 1;
        let v = self
            .0
            .get(key)?
            .ok_or_else(|| anyhow!("{:?} doesn't have an associated peer id", id))?;
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

    /// Returns the default [`Keypair`]. If no default [`Keypair`] is set it will
    /// generate one.
    pub fn default_keypair(&self) -> Result<Keypair> {
        let mut key = [0; 33];
        key[32] = 3;
        if let Some(value) = self.0.get(&key)? {
            return self.keypair(&PeerId::new(value.as_ref().try_into().unwrap()));
        }
        let keypair = Keypair::generate();
        self.add_keypair(keypair)?;
        self.set_default_keypair(&keypair.peer_id())?;
        Ok(keypair)
    }

    pub fn set_default_keypair(&self, keypair: &PeerId) -> Result<()> {
        let mut key = [0; 33];
        key[32] = 3;
        self.0.insert(&key, keypair.as_ref())?;
        Ok(())
    }

    pub fn subscribe(&self) -> impl Stream<Item = ()> {
        self.0.watch_prefix(&[]).map(|_| ())
    }
}

struct DebugDoc<'a>(&'a Docs, DocId);

impl<'a> std::fmt::Debug for DebugDoc<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = f.debug_struct("Doc");
        s.field("peer_id", &self.0.peer_id(&self.1).unwrap());
        s.field("schema", &self.0.schema(&self.1).unwrap());
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
    tx: mpsc::Sender<oneshot::Sender<()>>,
    rx: mpsc::Receiver<oneshot::Sender<()>>,
}

impl Backend {
    /// Creates a new [`Backend`] from a radixdb storage.
    pub fn new(storage: Arc<dyn Storage>, package: &[u8]) -> Result<Self> {
        let registry = Registry::new(package)?;
        let docs = Docs::new(BlobMap::load(storage.clone(), "docs")?);
        let acl = Acl::new(BlobMap::load(storage.clone(), "acl")?);
        let crdt = Crdt::new(
            BlobSet::load(storage.clone(), "store")?,
            BlobSet::load(storage, "expired")?,
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

        // migrate docs
        for res in me.docs.docs() {
            let id = res?;
            let info = me.docs.schema(&id)?;
            let (version, hash) = me.registry.lookup(&info.as_ref().name).unwrap();
            if version > info.as_ref().version {
                let lenses = me.registry.get(&hash).unwrap();
                let end = info.as_ref().version as usize;
                let curr_lenses = LensesRef::new(&lenses.lenses().lenses()[..end]);
                me.crdt
                    .transform(&id, curr_lenses, lenses.lenses().to_ref())?;
                let info = SchemaInfo::new(info.as_ref().name.to_string(), version, hash);
                me.docs.set_schema(&id, &info)?;
            }
        }
        Ok(me)
    }

    /// Creates a new in memory [`Backend`].
    pub fn memory(package: &[u8]) -> Result<Self> {
        Self::new(Arc::new(MemStorage::default()), package)
    }

    /// Creates a new in-memory backend for testing purposes.
    #[cfg(test)]
    #[allow(clippy::ptr_arg)]
    pub fn test(packages: &str) -> Result<Self> {
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
        let packages = tlfsc::compile_lenses(packages)?;
        let packages = Ref::archive(&packages);
        Self::memory(packages.as_bytes())
    }

    /// Returns a reference to the lens registry.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    fn update_acl(&mut self) -> Result<()> {
        for key in self.crdt.iter() {
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
        let doc_schema = self.docs.schema(doc)?;
        let doc_lenses = self.registry.get(&doc_schema.as_ref().hash.into()).unwrap();
        let lenses = self
            .registry
            .get(causal_schema)
            .ok_or_else(|| anyhow!("missing lenses with hash {}", causal_schema))?;
        if !lenses.schema().validate(&causal) {
            return Err(anyhow!("crdt failed schema validation"));
        }
        causal.transform(lenses.lenses().to_ref(), doc_lenses.lenses().to_ref());
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
        if let Poll::Ready(Some(tx)) = Pin::new(&mut self.rx).poll_next(cx) {
            let res = self.update_acl();
            tx.send(()).ok();
            Poll::Ready(res)
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
    tx: mpsc::Sender<oneshot::Sender<()>>,
}

impl Frontend {
    fn new(
        crdt: Crdt,
        docs: Docs,
        registry: Registry,
        tx: mpsc::Sender<oneshot::Sender<()>>,
    ) -> Self {
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

    /// Returns the default [`Keypair`]. If no default [`Keypair`] is set it will
    /// generate one.
    pub fn default_keypair(&self) -> Result<Keypair> {
        self.docs.default_keypair()
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

    /// Returns an iterator of [`DocId`].
    pub fn docs_by_schema(&self, schema: String) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.docs.docs_by_schema(schema)
    }

    /// Creates a new document using [`Keypair`] with initial schema and owner.
    pub fn create_doc(
        &self,
        owner: PeerId,
        schema: &str,
        la: Keypair,
    ) -> Result<impl Future<Output = Doc>> {
        let id = DocId::new(la.peer_id().into());
        let (version, hash) = self
            .registry
            .lookup(schema)
            .ok_or_else(|| anyhow!("missing schema {}", schema))?;
        let info = SchemaInfo::new(schema.into(), version, hash);
        let schema = self.registry.get(&hash).unwrap();
        self.docs.set_peer_id(&id, &id.into())?;
        self.docs.set_schema(&id, &info)?;
        let doc = Doc::new(id, self.clone(), la, schema);
        let delta = doc.cursor().say_can(Some(owner), Permission::Own)?;
        self.apply(&id, &delta)?;
        self.docs.set_peer_id(&id, &owner)?;
        let (tx, rx) = oneshot::channel();
        self.tx.clone().send(tx).now_or_never();
        let doc = self.doc(id)?;
        Ok(async move {
            rx.await.ok();
            doc
        })
    }

    /// Adds an existing document identified by [`DocId`] with schema and associates the local
    /// keypair identified by [`PeerId`].
    pub fn add_doc(&self, id: DocId, peer: &PeerId, schema: &str) -> Result<Doc> {
        let (version, hash) = self
            .registry
            .lookup(schema)
            .ok_or_else(|| anyhow!("missing schema {}", schema))?;
        let info = SchemaInfo::new(schema.into(), version, hash);
        self.docs.set_schema(&id, &info)?;
        self.docs.set_peer_id(&id, peer)?;
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

    /// Returns the current schema identifier of a document.
    pub fn schema(&self, id: &DocId) -> Result<Ref<SchemaInfo>> {
        self.docs.schema(id)
    }

    /// Returns the current lenses of a document.
    pub fn lenses(&self, id: &Hash) -> Result<Arc<Expanded>> {
        self.registry
            .get(id)
            .ok_or_else(|| anyhow!("missing schema for {}", id))
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
        let info = self.schema(&id)?;
        let schema = self.lenses(&info.as_ref().hash.into())?;
        let key = self.keypair(peer_id)?;
        Ok(Doc::new(id, self.clone(), key, schema))
    }

    /// Applies a local change to a document.
    pub fn apply(&self, doc: &DocId, causal: &Causal) -> Result<()> {
        let peer = self.peer_id(doc)?;
        self.crdt.join(&peer, causal)?;
        let (tx, rx) = oneshot::channel();
        self.tx.clone().send(tx).now_or_never();
        drop(rx);
        Ok(())
    }

    /// Subscribes to document changes.
    pub fn subscribe(&self) -> impl Stream<Item = ()> {
        self.docs.subscribe()
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
    schema: Arc<Expanded>,
}

impl Doc {
    fn new(id: DocId, frontend: Frontend, key: Keypair, schema: Arc<Expanded>) -> Self {
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
        Cursor::new(self.key, self.id, self.schema.schema(), &self.frontend.crdt)
    }

    /// Applies a local change to the document.
    pub fn apply(&self, causal: &Causal) -> Result<()> {
        self.frontend.apply(&self.id, causal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Permission;

    #[async_std::test]
    async fn test_api() -> Result<()> {
        let packages = r#"
            todoapp {
                0.1.0 {
                    .: Struct
                    .todos: Table<u64>
                    .todos.{}: Struct
                    .todos.{}.title: MVReg<String>
                    .todos.{}.complete: EWFlag
                }
            }
        "#;
        let mut sdk = Backend::test(packages)?;
        let peer = sdk.frontend().default_keypair()?.peer_id();
        let doc = sdk
            .frontend()
            .create_doc(peer, "todoapp", Keypair::generate())?;
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

        let mut sdk2 = Backend::test(packages)?;
        let peer2 = sdk2.frontend().default_keypair()?.peer_id();
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;

        let doc2 = sdk2.frontend().add_doc(*doc.id(), &peer2, "todoapp")?;
        let ctx = Ref::archive(&doc2.ctx()?);
        let delta = sdk.unjoin(&peer2, doc2.id(), ctx.as_ref())?;
        let hash = sdk2.frontend().registry.lookup("todoapp").unwrap().1;
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

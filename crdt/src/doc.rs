use crate::{
    Acl, Actor, Causal, CausalContext, Crdt, Cursor, DocId, Dot, Engine, Hash, Keypair, Lens,
    Lenses, PathBuf, PeerId, Permission, Policy, Ref, Registry, Schema, EMPTY_HASH, EMPTY_LENSES,
    EMPTY_SCHEMA,
};
use anyhow::{anyhow, Result};
use rkyv::Archived;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct Backend {
    registry: Registry,
    crdt: Crdt,
    docs: Docs,
    engine: Engine,
}

impl Backend {
    pub fn new(db: sled::Db) -> Result<Self> {
        let registry = Registry::new(db.open_tree("lenses")?);
        let docs = Docs::new(db.open_tree("docs")?);
        let acl = Acl::new(db.open_tree("acl")?);
        let crdt = Crdt::new(
            db.open_tree("crdt")?,
            db.open_tree("expired")?,
            acl.clone(),
            docs.clone(),
        );
        let engine = Engine::new(crdt.clone(), acl)?;
        Ok(Self {
            registry,
            crdt,
            docs,
            engine,
        })
    }

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

    pub fn register(&self, lenses: Vec<Lens>) -> Result<Hash> {
        self.registry
            .register(Ref::archive(&Lenses::new(lenses)).as_bytes())
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        Doc::new(
            id,
            self.crdt.clone(),
            self.docs.clone(),
            self.registry.clone(),
        )
    }

    pub fn join(&self, peer_id: &PeerId, mut causal: Causal) -> Result<()> {
        let doc_schema_id = self.docs.schema_id(&causal.doc)?;
        let doc_lenses = self
            .registry
            .lenses(&doc_schema_id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into()));
        let schema = self.registry.schema(&causal.schema())?;
        let lenses = self.registry.lenses(&causal.schema())?;
        let (schema, lenses) = match (schema, lenses) {
            (Some(schema), Some(lenses)) => (schema, lenses),
            _ => {
                if causal.schema == EMPTY_HASH {
                    (
                        Ref::new(EMPTY_SCHEMA.to_vec().into()),
                        Ref::new(EMPTY_LENSES.to_vec().into()),
                    )
                } else {
                    return Err(anyhow!("missing lenses with hash {}", causal.schema()));
                }
            }
        };
        if !schema.as_ref().validate(causal.store()) {
            return Err(anyhow!("crdt failed schema validation"));
        }
        causal.transform(doc_lenses.as_ref(), lenses.as_ref());
        self.crdt.join(peer_id, &causal)?;
        Ok(())
    }

    pub fn unjoin(&self, peer_id: &PeerId, ctx: &Archived<CausalContext>) -> Result<Causal> {
        self.crdt.unjoin(peer_id, ctx)
    }

    pub fn transform(&mut self, id: DocId, schema_id: &Hash) -> Result<()> {
        let doc_schema_id = self.docs.schema_id(&id)?;
        let doc_lenses = self
            .registry
            .lenses(&doc_schema_id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into()));
        let lenses = self
            .registry
            .lenses(schema_id)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", &schema_id))?;
        self.crdt
            .transform(&id, schema_id, doc_lenses.as_ref(), lenses.as_ref())?;
        Ok(())
    }

    pub fn frontend(&self) -> Frontend {
        Frontend::new(self.crdt.clone(), self.docs.clone(), self.registry.clone())
    }
}

impl Future for Backend {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        Pin::new(&mut self.engine).poll(cx)
    }
}

#[derive(Clone)]
pub struct Frontend {
    crdt: Crdt,
    docs: Docs,
    registry: Registry,
}

impl Frontend {
    pub fn new(crdt: Crdt, docs: Docs, registry: Registry) -> Self {
        Self {
            crdt,
            docs,
            registry,
        }
    }

    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.docs.docs()
    }

    pub fn create_doc(&self, owner: PeerId, schema: &Hash) -> Result<Doc> {
        self.create_doc_deterministic(owner, schema, Keypair::generate())
    }

    pub fn create_doc_deterministic(
        &self,
        owner: PeerId,
        schema: &Hash,
        la: Keypair,
    ) -> Result<Doc> {
        let id = DocId::new(la.peer_id().into());
        self.docs.create(&id, &owner, schema)?;
        let delta = self.crdt.say(
            PathBuf::new(id).as_path(),
            &Writer::new(id.into(), 0),
            Policy::Can(Actor::Peer(owner), Permission::Own),
        )?;
        self.crdt.join(&id.into(), &delta)?;
        self.doc(id)
    }

    pub fn add_doc(&self, id: DocId, peer: &PeerId, schema: &Hash) -> Result<Doc> {
        self.docs.set_peer_id(&id, peer)?;
        self.docs.set_schema_id(&id, schema)?;
        self.doc(id)
    }

    pub fn remove_doc(&self, _id: DocId) -> Result<()> {
        todo!()
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        Doc::new(
            id,
            self.crdt.clone(),
            self.docs.clone(),
            self.registry.clone(),
        )
    }

    pub fn apply(&self, causal: &Causal) -> Result<()> {
        let peer_id = self.docs.peer_id(&causal.doc)?.unwrap();
        self.crdt.join(&peer_id, causal)?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct Docs(sled::Tree);

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

    pub fn create(&self, id: &DocId, owner: &PeerId, schema: &Hash) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert(key, schema.as_bytes())?;
        key[32] = 1;
        self.0.insert(key, owner.as_ref())?;
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

    pub fn peer_id(&self, id: &DocId) -> Result<Option<PeerId>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 1;
        let peer = self
            .0
            .get(key)?
            .map(|v| PeerId::new(v.as_ref().try_into().unwrap()));
        Ok(peer)
    }

    pub fn set_peer_id(&self, id: &DocId, peer: &PeerId) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 1;
        self.0.insert(key, peer.as_ref())?;
        Ok(())
    }

    pub fn counter(&self, id: &DocId, peer_id: &PeerId) -> Result<u64> {
        let mut key = [0; 65];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 2;
        key[33..].copy_from_slice(peer_id.as_ref());
        let v = self
            .0
            .get(key)?
            .map(|b| u64::from_le_bytes(b.as_ref().try_into().unwrap()))
            .unwrap_or_default();
        Ok(v)
    }

    pub fn contains(&self, id: &DocId, dot: &Dot) -> Result<bool> {
        Ok(self.counter(id, &dot.id)? >= dot.counter)
    }
}

#[derive(Clone)]
pub struct Writer {
    peer_id: PeerId,
    counter: Arc<AtomicU64>,
}

impl Writer {
    pub fn new(peer_id: PeerId, counter: u64) -> Self {
        Self {
            peer_id,
            counter: Arc::new(AtomicU64::new(counter + 1)),
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn dot(&self) -> Dot {
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        Dot::new(self.peer_id, counter)
    }
}

#[derive(Clone)]
pub struct Doc {
    id: DocId,
    schema_id: Hash,
    writer: Writer,
    lenses: Ref<Lenses>,
    schema: Ref<Schema>,
    crdt: Crdt,
    registry: Registry,
}

impl std::fmt::Debug for Doc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Doc")
            .field("id", &self.id)
            .field("schema_id", &self.schema_id)
            .field("lenses", &self.lenses)
            .field("schema", &self.schema)
            .field("crdt", &self.crdt)
            .finish_non_exhaustive()
    }
}

impl Doc {
    fn new(id: DocId, crdt: Crdt, docs: Docs, registry: Registry) -> Result<Self> {
        let peer_id = docs.peer_id(&id)?.unwrap();
        let schema_id = docs.schema_id(&id)?;
        let lenses = registry
            .lenses(&schema_id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into()));
        let schema = registry
            .schema(&schema_id)?
            .unwrap_or_else(|| Ref::new(EMPTY_SCHEMA.as_ref().into()));
        let counter = docs.counter(&id, &peer_id)?;
        let writer = Writer::new(peer_id, counter);
        Ok(Self {
            id,
            schema_id,
            writer,
            lenses,
            schema,
            crdt,
            registry,
        })
    }

    pub fn id(&self) -> &DocId {
        &self.id
    }

    pub fn schema_id(&self) -> &Hash {
        &self.schema_id
    }

    pub fn lenses(&self) -> &Archived<Lenses> {
        self.lenses.as_ref()
    }

    pub fn schema(&self) -> &Archived<Schema> {
        self.schema.as_ref()
    }

    pub fn peer_id(&self) -> &PeerId {
        self.writer.peer_id()
    }

    pub fn ctx(&self) -> Result<CausalContext> {
        self.crdt.ctx(*self.id())
    }

    /// Returns a cursor for the document.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor::new(
            self.id,
            self.schema_id,
            &self.writer,
            self.schema(),
            &self.crdt,
        )
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

        let peer = PeerId::new([0; 32]);
        let doc = sdk.frontend().create_doc(peer, &hash)?;
        Pin::new(&mut sdk).await?;
        assert!(doc.cursor().can(&peer, Permission::Write)?);

        let title = "something that needs to be done";
        let delta = doc
            .cursor()
            .field("todos")?
            .key(&0u64.into())?
            .field("title")?
            .assign(title)?;
        sdk.join(&peer, delta)?;

        let value = doc
            .cursor()
            .field("todos")?
            .key(&0u64.into())?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);

        let sdk2 = Backend::memory()?;
        sdk2.register(lenses)?;
        let peer2 = PeerId::new([1; 32]);
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        sdk.join(&peer, op)?;
        Pin::new(&mut sdk).await?;

        let doc2 = sdk2.frontend().add_doc(*doc.id(), &peer2, &hash)?;
        let ctx = Ref::archive(&doc2.ctx()?);
        let delta = sdk.unjoin(&peer2, ctx.as_ref())?;
        sdk2.join(&peer, delta)?;

        let value = doc2
            .cursor()
            .field("todos")?
            .key(&0u64.into())?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);

        Ok(())
    }
}

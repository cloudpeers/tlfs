use crate::{
    Acl, Causal, CausalContext, Crdt, Cursor, DocId, Engine, Hash, Keypair, Lens, Lenses, Path,
    PeerId, Permission, Ref, Registry, Schema, EMPTY_HASH, EMPTY_LENSES, EMPTY_SCHEMA,
};
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

    pub fn create(&self, id: &DocId, schema: &Hash) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert(key, schema.as_bytes())?;
        key[32] = 1;
        self.0.insert(key, id.as_ref())?;
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
}

pub struct Backend {
    registry: Registry,
    crdt: Crdt,
    docs: Docs,
    engine: Engine,
    tx: mpsc::Sender<()>,
    rx: mpsc::Receiver<()>,
}

impl Backend {
    pub fn new(db: sled::Db) -> Result<Self> {
        let registry = Registry::new(db.open_tree("lenses")?);
        let docs = Docs::new(db.open_tree("docs")?);
        let acl = Acl::new(db.open_tree("acl")?);
        let crdt = Crdt::new(db.open_tree("crdt")?, db.open_tree("expired")?, acl.clone());
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

    fn update_acl(&mut self) -> Result<()> {
        for res in self.crdt.iter() {
            let key = res?;
            let path = Path::new(&key[..]);
            self.engine.add_policy(path);
        }
        self.engine.update_acl()
    }

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
        let schema = self.registry.schema(&causal_schema)?;
        let lenses = self.registry.lenses(&causal_schema)?;
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
        if !schema.as_ref().validate(causal.store()) {
            return Err(anyhow!("crdt failed schema validation"));
        }
        causal.transform(doc_lenses.as_ref(), lenses.as_ref());
        self.crdt.join_policy(&causal)?;
        self.update_acl()?;
        self.crdt.join(peer_id, doc, &causal)?;
        Ok(())
    }

    pub fn unjoin(
        &self,
        peer_id: &PeerId,
        doc: &DocId,
        ctx: &Archived<CausalContext>,
    ) -> Result<Causal> {
        self.crdt.unjoin(peer_id, doc, ctx)
    }

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

    pub fn docs(&self) -> impl Iterator<Item = Result<DocId>> + '_ {
        self.docs.docs()
    }

    pub fn create_doc(&self, owner: PeerId, schema: &Hash, la: Keypair) -> Result<Doc> {
        let id = DocId::new(la.peer_id().into());
        self.docs.create(&id, schema)?;
        let doc = self.doc(id)?;
        let delta = doc.cursor().say_can(Some(owner), Permission::Own)?;
        self.set_peer_id(&id, &owner)?;
        self.crdt.join(&id.into(), &id, &delta)?;
        self.tx.clone().send(()).now_or_never();
        Ok(doc)
    }

    pub fn add_doc(&self, id: DocId, peer: &PeerId, schema: &Hash) -> Result<Doc> {
        self.docs.set_peer_id(&id, peer)?;
        self.docs.set_schema_id(&id, schema)?;
        self.doc(id)
    }

    pub fn remove_doc(&self, _id: DocId) -> Result<()> {
        todo!()
    }

    pub fn peer_id(&self, id: &DocId) -> Result<PeerId> {
        self.docs.peer_id(&id)
    }

    pub fn set_peer_id(&self, id: &DocId, peer: &PeerId) -> Result<()> {
        self.docs.set_peer_id(id, peer)
    }

    pub fn schema_id(&self, id: &DocId) -> Result<Hash> {
        self.docs.schema_id(id)
    }

    pub fn lenses(&self, id: &Hash) -> Result<Ref<Lenses>> {
        Ok(self
            .registry
            .lenses(id)?
            .unwrap_or_else(|| Ref::new(EMPTY_LENSES.as_ref().into())))
    }

    pub fn schema(&self, id: &Hash) -> Result<Ref<Schema>> {
        Ok(self
            .registry
            .schema(id)?
            .unwrap_or_else(|| Ref::new(EMPTY_SCHEMA.as_ref().into())))
    }

    pub fn ctx(&self, id: &DocId) -> Result<CausalContext> {
        self.crdt.ctx(id)
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        let hash = self.schema_id(&id)?;
        let schema = self.schema(&hash)?;
        let peer_id = self.peer_id(&id)?;
        Ok(Doc::new(id, self.clone(), peer_id, schema))
    }

    pub fn apply(&self, doc: &DocId, causal: &Causal) -> Result<()> {
        let peer_id = self.docs.peer_id(doc)?;
        self.crdt.join(&peer_id, doc, causal)?;
        self.tx.clone().send(()).now_or_never();
        Ok(())
    }
}

#[derive(Clone)]
pub struct Doc {
    id: DocId,
    frontend: Frontend,
    peer_id: PeerId,
    schema: Ref<Schema>,
}

impl Doc {
    fn new(id: DocId, frontend: Frontend, peer_id: PeerId, schema: Ref<Schema>) -> Self {
        Self {
            id,
            frontend,
            peer_id,
            schema,
        }
    }

    pub fn id(&self) -> &DocId {
        &self.id
    }

    pub fn ctx(&self) -> Result<CausalContext> {
        self.frontend.ctx(&self.id)
    }

    /// Returns a cursor for the document.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor::new(
            self.id,
            self.peer_id,
            self.schema.as_ref(),
            &self.frontend.crdt,
        )
    }

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

        let peer = PeerId::new([0; 32]);
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
        let peer2 = PeerId::new([1; 32]);
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        doc.apply(&op)?;

        let doc2 = sdk2.frontend().add_doc(*doc.id(), &peer2, &hash)?;
        let ctx = Ref::archive(&doc2.ctx()?);
        let delta = sdk.unjoin(&peer2, doc2.id(), ctx.as_ref())?;
        println!("{:?}", delta);
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

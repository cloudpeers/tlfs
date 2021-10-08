use crate::{
    empty_hash, Acl, Actor, Causal, CausalContext, Crdt, Cursor, DocId, Dot, Engine, Hash, Keypair,
    Lenses, PathBuf, PeerId, Permission, Policy, Ref, Registry, Schema,
};
use anyhow::{anyhow, Result};
use rkyv::Archived;
use std::convert::TryInto;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct Backend {
    registry: Registry,
    crdt: Crdt,
    docs: Docs,
    acl: Acl,
    engine: Engine,
}

impl Backend {
    pub fn new(config: sled::Config) -> Result<Self> {
        let db = config.open()?;
        let registry = Registry::new(db.open_tree("lenses")?);
        let crdt = Crdt::new(db.open_tree("crdt")?);
        let docs = Docs::new(db.open_tree("docs")?);
        let acl = Acl::new(db.open_tree("acl")?);
        let engine = Engine::new(crdt.clone(), acl.clone())?;
        Ok(Self {
            registry,
            crdt,
            docs,
            acl,
            engine,
        })
    }

    pub fn memory() -> Result<Self> {
        Self::new(sled::Config::new().temporary(true))
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub fn create_doc(&self, owner: PeerId) -> Result<Doc> {
        let la = Keypair::generate();
        let id = DocId::new(la.peer_id().into());
        let hash = empty_hash();
        let mut ctx = CausalContext::new(id, hash);
        let delta = self.crdt.say(
            PathBuf::new(id).as_path(),
            Ref::archive(&ctx).as_ref(),
            Dot::new(id.into(), 1),
            Policy::Can(Actor::Peer(owner), Permission::Own),
        )?;
        self.crdt.join(&mut ctx, &id.into(), &delta)?;
        self.docs.create(id, owner, &ctx)?;
        self.doc(id)
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        Doc::new(
            id,
            self.crdt.clone(),
            self.docs.clone(),
            self.registry.clone(),
            self.acl.clone(),
        )
    }
}

impl Future for Backend {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if let Poll::Ready(Err(err)) = Pin::new(&mut self.engine).poll(cx) {
            tracing::error!("{}", err);
        }
        Poll::Pending
    }
}

#[derive(Clone)]
pub struct Docs(sled::Tree);

impl Docs {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    pub fn create(&self, id: DocId, owner: PeerId, ctx: &CausalContext) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert(key, Ref::archive(ctx).as_bytes())?;
        key[32] = 1;
        self.0.insert(key, owner.as_ref())?;
        Ok(())
    }

    pub fn ctx(&self, id: &DocId) -> Result<Option<Ref<CausalContext>>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        Ok(self.0.get(key)?.map(Ref::new))
    }

    pub fn peer_id(&self, id: &DocId) -> Result<Option<PeerId>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 1;
        Ok(self
            .0
            .get(key)?
            .map(|v| PeerId::new(v.as_ref().try_into().unwrap())))
    }

    pub fn counter(&self, id: &DocId) -> Result<u64> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 2;
        let v = self
            .0
            .fetch_and_update(key, |v| {
                let v = v
                    .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
                    .unwrap_or_default()
                    + 1;
                Some(v.to_le_bytes().to_vec())
            })?
            .unwrap();
        Ok(u64::from_le_bytes(v.as_ref().try_into().unwrap()))
    }
}

#[derive(Clone)]
pub struct Doc {
    crdt: Crdt,
    docs: Docs,
    registry: Registry,
    acl: Acl,
    ctx: Ref<CausalContext>,
    lenses: Ref<Lenses>,
    schema: Ref<Schema>,
    peer_id: PeerId,
}

impl Doc {
    fn new(id: DocId, crdt: Crdt, docs: Docs, registry: Registry, acl: Acl) -> Result<Self> {
        let ctx = docs.ctx(&id)?.unwrap();
        let peer_id = docs.peer_id(&id)?.unwrap();
        let lenses = registry.lenses(&ctx.as_ref().schema.into())?.unwrap();
        let schema = registry.schema(&ctx.as_ref().schema.into())?.unwrap();
        Ok(Self {
            crdt,
            docs,
            registry,
            acl,
            ctx,
            lenses,
            schema,
            peer_id,
        })
    }

    pub fn ctx(&self) -> &Archived<CausalContext> {
        self.ctx.as_ref()
    }

    pub fn lenses(&self) -> &Archived<Lenses> {
        self.lenses.as_ref()
    }

    pub fn schema(&self) -> &Archived<Schema> {
        self.schema.as_ref()
    }

    /// Returns a cursor for the document.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor::new(
            self.ctx(),
            &self.crdt,
            &self.docs,
            &self.acl,
            self.schema(),
            self.peer_id,
        )
    }

    pub fn join(&self, _peer_id: &PeerId, mut causal: Causal) -> Result<()> {
        let schema_id = causal.ctx().schema();
        let schema = self
            .registry
            .schema(&schema_id)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", schema_id))?;
        let lenses = self.registry.lenses(&schema_id)?.unwrap();

        if !schema.as_ref().validate(causal.store()) {
            return Err(anyhow!("crdt failed schema validation"));
        }
        causal.transform(self.lenses(), lenses.as_ref());
        todo!();
        //self.crdt.join(self.ctx(), peer_id, &causal)?;
        //Ok(())
    }

    pub fn unjoin(&self, peer_id: &PeerId, ctx: &Archived<CausalContext>) -> Result<Causal> {
        self.crdt.unjoin(self.ctx(), peer_id, &ctx)
    }

    pub fn transform(&mut self, schema_id: &Hash) -> Result<()> {
        let lenses = self
            .registry
            .lenses(schema_id)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", schema_id))?;
        let _schema = self.registry.schema(schema_id)?.unwrap();
        self.crdt.transform(self.lenses(), lenses.as_ref());
        todo!();
        //self.ctx().schema = (*schema_id).into();
        //self.schema = schema;
        //Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Kind, Lens, Lenses, Permission, PrimitiveKind, Ref, EMPTY_LENSES};

    #[test]
    #[ignore]
    fn test_api() -> Result<()> {
        let sdk = Backend::memory()?;
        sdk.registry().register(EMPTY_LENSES.to_vec())?;
        let peer_id = PeerId::new([42; 32]);

        let lenses = Lenses::new(vec![
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
        ]);
        let hash = sdk
            .registry()
            .register(Ref::archive(&lenses).as_bytes().to_vec())?;

        let mut doc = sdk.create_doc(peer_id)?;
        doc.transform(&hash)?;
        assert!(doc.cursor().can(peer_id, Permission::Write)?);

        let title = "something that needs to be done";

        let delta = doc
            .cursor()
            .field("todos")?
            .key(&0u64.into())?
            .field("title")?
            .assign(title)?;
        doc.join(&peer_id, delta)?;

        let value = doc
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

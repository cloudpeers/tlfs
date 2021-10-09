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
    engine: Engine,
}

impl Backend {
    pub fn new(config: sled::Config) -> Result<Self> {
        let db = config.open()?;
        let registry = Registry::new(db.open_tree("lenses")?);
        let docs = Docs::new(db.open_tree("docs")?);
        let acl = Acl::new(db.open_tree("acl")?);
        let crdt = Crdt::new(db.open_tree("crdt")?, acl.clone(), docs.clone());
        let engine = Engine::new(crdt.clone(), acl)?;
        Ok(Self {
            registry,
            crdt,
            docs,
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
        self.docs.create(id, owner)?;
        let delta = self.crdt.say(
            PathBuf::new(id).as_path(),
            &id.into(),
            Policy::Can(Actor::Peer(owner), Permission::Own),
        )?;
        self.crdt.join(&id.into(), &delta)?;
        self.doc(id)
    }

    pub fn doc(&self, id: DocId) -> Result<Doc> {
        Doc::new(
            id,
            self.crdt.clone(),
            self.docs.clone(),
            self.registry.clone(),
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

    pub fn create(&self, id: DocId, owner: PeerId) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        self.0.insert(key, empty_hash().as_bytes())?;
        key[32] = 1;
        self.0.insert(key, owner.as_ref())?;
        Ok(())
    }

    pub fn schema_id(&self, id: &DocId) -> Result<Option<Hash>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        Ok(self.0.get(key)?.map(|b| {
            let b: [u8; 32] = b.as_ref().try_into().unwrap();
            b.into()
        }))
    }

    pub fn set_schema_id(&self, id: &DocId, hash: Hash) -> Result<()> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 0;
        let hash: [u8; 32] = hash.into();
        self.0.insert(key, &hash)?;
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

    pub fn set_peer_id(&self, id: &DocId, peer: PeerId) -> Result<()> {
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

    pub fn present(&self, id: &DocId) -> impl Iterator<Item = Result<(PeerId, u64)>> + '_ {
        let mut prefix = [0; 33];
        prefix[..32].copy_from_slice(id.as_ref());
        prefix[32] = 2;
        self.0.scan_prefix(prefix).map(|r| {
            let (k, v) = r?;
            Ok((
                PeerId::new(k.as_ref().try_into().unwrap()),
                u64::from_le_bytes(v.as_ref().try_into().unwrap()),
            ))
        })
    }

    pub fn increment(&self, id: &DocId, peer_id: &PeerId) -> Result<u64> {
        let mut key = [0; 65];
        key[..32].copy_from_slice(id.as_ref());
        key[32] = 2;
        key[33..].copy_from_slice(peer_id.as_ref());
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

    pub fn dot(&self, id: &DocId, peer_id: &PeerId) -> Result<Dot> {
        let counter = self.increment(id, peer_id)?;
        Ok(Dot::new(*peer_id, counter))
    }
}

#[derive(Clone)]
pub struct Doc {
    id: DocId,
    schema_id: Hash,
    peer_id: PeerId,
    lenses: Ref<Lenses>,
    schema: Ref<Schema>,
    crdt: Crdt,
    registry: Registry,
}

impl Doc {
    fn new(id: DocId, crdt: Crdt, docs: Docs, registry: Registry) -> Result<Self> {
        let peer_id = docs.peer_id(&id)?.unwrap();
        let schema_id = docs.schema_id(&id)?.unwrap();
        let lenses = registry.lenses(&schema_id)?.unwrap();
        let schema = registry.schema(&schema_id)?.unwrap();
        Ok(Self {
            id,
            schema_id,
            peer_id,
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
        &self.peer_id
    }

    /// Returns a cursor for the document.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor::new(
            self.id,
            self.schema_id,
            self.peer_id,
            self.schema(),
            &self.crdt,
        )
    }

    pub fn join(&self, peer_id: &PeerId, mut causal: Causal) -> Result<()> {
        let schema = self
            .registry
            .schema(&self.schema_id)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", &self.schema_id))?;
        let lenses = self.registry.lenses(&self.schema_id)?.unwrap();

        if !schema.as_ref().validate(causal.store()) {
            return Err(anyhow!("crdt failed schema validation"));
        }
        causal.transform(self.lenses(), lenses.as_ref());
        self.crdt.join(peer_id, &causal)?;
        Ok(())
    }

    pub fn unjoin(&self, peer_id: &PeerId, ctx: &Archived<CausalContext>) -> Result<Causal> {
        self.crdt.unjoin(peer_id, ctx)
    }

    pub fn transform(&mut self, schema_id: Hash) -> Result<()> {
        let lenses = self
            .registry
            .lenses(&schema_id)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", &schema_id))?;
        let schema = self.registry.schema(&schema_id)?.unwrap();
        self.crdt
            .transform(&self.id, schema_id, self.lenses(), lenses.as_ref())?;
        self.schema = schema;
        Ok(())
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
        doc.transform(hash)?;
        assert!(doc.cursor().can(&peer_id, Permission::Write)?);

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

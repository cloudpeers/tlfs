use crate::{
    AbstractDotSet, Acl, ArchivedLenses, DocId, Docs, Dot, DotSet, Hash, PeerId, Permission,
    Policy, Ref, Writer, Primitive, Path, PathBuf,
};
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::{ser::serializers::AllocSerializer, Archive, Archived, Deserialize, Serialize};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
};

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize, Default)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(Debug, CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub struct DotStore(BTreeSet<PathBuf>);

impl DotStore {
    pub fn policy(args: impl IntoIterator<Item = (Dot, Policy)>) -> Self {
        Self(
            args.into_iter()
                .map(|(dot, policy)| {
                    let mut path = Path::empty().to_owned();
                    path.policy(&dot);
                    (path, archive(&policy))
                })
                .collect(),
        )
    }

    pub fn dotfun(args: impl IntoIterator<Item = (Dot, Primitive)>) -> Self {
        Self(
            args.into_iter()
                .map(|(dot, primitive)| {
                    let mut path = Path::empty().to_owned();
                    path.dotfun(&dot);
                    (path, archive(&primitive))
                })
                .collect(),
        )
    }

    pub fn dotset(args: impl IntoIterator<Item = Dot>) -> Self {
        Self(
            args.into_iter()
                .map(|dot| {
                    let mut path = Path::empty().to_owned();
                    path.dotset(&dot);
                    (path, Vec::new())
                })
                .collect(),
        )
    }

    pub fn dotmap(args: impl IntoIterator<Item = (Primitive, Self)>) -> Self {
        let entries = args.into_iter().flat_map(move |(key, store)| {
            store.0.into_iter().map(move |(k, v)| (key.clone(), k, v))
        });
        Self(
            entries
                .map(|(key, k, v)| {
                    let mut path = Path::empty().to_owned();
                    path.key(&key);
                    path.0.extend(k.0.into_iter());
                    (path, v)
                })
                .collect(),
        )
    }

    pub fn r#struct(args: impl IntoIterator<Item = (String, Self)>) -> Self {
        let entries = args.into_iter().flat_map(move |(field, store)| {
            store.0.into_iter().map(move |(k, v)| (field.clone(), k, v))
        });
        Self(
            entries
                .map(|(field, k, v)| {
                    let mut path = Path::empty().to_owned();
                    path.field(&field);
                    path.0.extend(k.0.into_iter());
                    (path, v)
                })
                .collect(),
        )
    }

    /// prefix the entire dot store with a path
    pub fn prefix(&self, path: Path) -> Self {
        Self(
            self.0
                .iter()
                .map(|(k, v)| {
                    let mut k = k.clone();
                    k.0.splice(0..0, path.0.iter().cloned());
                    (k, v.clone())
                })
                .collect(),
        )
    }

    fn assert_invariants(&self) {
        debug_assert!(self.0.keys().all(|x| !x.as_path().is_empty()));
        debug_assert!(self.0.keys().all(|x| {
            let t = x.as_path().ty().unwrap();
            t == DotStoreType::Policy || t == DotStoreType::Set || t == DotStoreType::Fun
        }));
    }

    pub fn get(&self, key: &Path) -> Option<&[u8]> {
        self.0.get(key.as_ref()).map(|x| x.as_ref())
    }

    pub fn dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.0.keys().map(|x| x.as_path().dot())
    }

    pub fn join(&mut self, that: &Self, expired: &impl AbstractDotSet) {
        self.0.outer_join_with(
            &that.0,
            |k, v, w| {
                let dot = k.as_path().dot();
                if let Some(w) = w {
                    assert_eq!(v, w);
                    assert!(!expired.contains(&dot));
                    true
                } else {
                    !expired.contains(&dot)
                }
            },
            |k, w| {
                let dot = k.as_path().dot();
                if !expired.contains(&dot) {
                    Some(w.clone())
                } else {
                    None
                }
            },
        );
        self.assert_invariants();
    }

    pub fn unjoin(&self, diff: &DotSet) -> Self {
        Self(
            self.0
                .iter()
                .filter_map(|(k, v)| {
                    if diff.contains(&k.as_path().dot()) {
                        Some((k.clone(), v.clone()))
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Clone, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct CausalContext {
    pub(crate) doc: DocId,
    pub(crate) schema: [u8; 32],
    /// the dots to be considered. These are the dots in the store.
    pub(crate) dots: DotSet,
    /// the expired dots. The intersection of this and dots must be empty.
    pub(crate) expired: DotSet,
}

impl CausalContext {
    pub fn new(doc: DocId, schema: Hash) -> Self {
        Self {
            doc,
            schema: schema.into(),
            dots: Default::default(),
            expired: Default::default(),
        }
    }

    pub fn doc(&self) -> &DocId {
        &self.doc
    }

    pub fn schema(&self) -> Hash {
        self.schema.into()
    }

    pub fn dots(&self) -> &DotSet {
        &self.dots
    }

    pub fn expired(&self) -> &DotSet {
        &self.expired
    }
}

impl ArchivedCausalContext {
    pub fn doc(&self) -> &DocId {
        &self.doc
    }

    pub fn schema(&self) -> Hash {
        self.schema.into()
    }

    pub fn dots(&self) -> &Archived<DotSet> {
        &self.dots
    }

    pub fn expired(&self) -> &Archived<DotSet> {
        &self.expired
    }
}

impl std::fmt::Debug for CausalContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CausalContext")
            .field("doc", &self.doc)
            .field("schema", &self.schema[0])
            .field("dots", &self.dots)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct Causal {
    pub(crate) doc: DocId,
    pub(crate) schema: [u8; 32],
    /// the expired dots. The intersection of this and dots must be empty.
    pub(crate) expired: DotSet,
    pub(crate) store: DotStore,
}

impl Causal {
    pub fn store(&self) -> &DotStore {
        &self.store
    }

    pub fn dots(&self) -> DotSet {
        self.store.dots().collect()
    }

    pub fn expired(&self) -> &DotSet {
        &self.expired
    }

    pub fn doc(&self) -> &DocId {
        &self.doc
    }

    pub fn schema(&self) -> Hash {
        self.schema.into()
    }

    pub fn join(&mut self, that: &Causal) {
        assert_eq!(self.doc, that.doc);
        assert_eq!(&self.schema, &that.schema);

        self.store.join(&that.store, that.expired());
        self.expired.union(&that.expired);
    }

    pub fn unjoin(&self, ctx: &CausalContext) -> Self {
        let diff = self.dots().difference(ctx.dots());
        let expired = self.expired.difference(&ctx.expired);
        let store = self.store.unjoin(&diff);
        Self {
            doc: self.doc,
            schema: self.schema,
            expired,
            store,
        }
    }

    pub fn ctx(&self) -> CausalContext {
        CausalContext {
            doc: self.doc,
            dots: self.dots(),
            expired: self.expired.clone(),
            schema: self.schema,
        }
    }

    pub fn transform(&mut self, from: &ArchivedLenses, to: &ArchivedLenses) {
        from.transform_dotstore(&mut self.store, to);
    }
}

impl ArchivedCausal {
    pub fn ctx(&self) -> CausalContext {
        let dots = self
            .store
            .0
            .keys()
            .map(|path| Path::new(&path.0).dot())
            .collect::<DotSet>();
        CausalContext {
            doc: self.doc,
            expired: self.expired.to_dotset(),
            schema: self.schema,
            dots,
        }
    }
}

#[derive(Clone)]
pub struct Crdt {
    state: sled::Tree,
    expired: sled::Tree,
    acl: Acl,
    docs: Docs,
}

impl std::fmt::Debug for Crdt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crdt")
            .field("state", &StateDebug(&self.state))
            .finish_non_exhaustive()
    }
}

struct StateDebug<'a>(&'a sled::Tree);

impl<'a> std::fmt::Debug for StateDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for e in self.0.iter() {
            let (k, v) = e.map_err(|_| std::fmt::Error::default())?;
            let path = Path::new(&k);
            m.entry(&path.to_string(), &HexDebug(&v));
        }
        m.finish()
    }
}

impl Crdt {
    pub fn new(state: sled::Tree, expired: sled::Tree, acl: Acl, docs: Docs) -> Self {
        Self {
            state,
            expired,
            acl,
            docs,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = sled::Result<(sled::IVec, sled::IVec)>> {
        self.state.iter()
    }

    pub fn dotset(&self, path: Path) -> impl Iterator<Item = Result<Dot>> {
        self.state
            .scan_prefix(path)
            .keys()
            .filter_map(|res| match res {
                Ok(key) => {
                    let key = Path::new(&key[..]);
                    if key.ty() == Some(DotStoreType::Set) {
                        Some(Ok(key.dot()))
                    } else {
                        None
                    }
                }
                Err(err) => Some(Err(err.into())),
            })
    }

    pub fn watch_path(&self, path: Path<'_>) -> sled::Subscriber {
        self.state.watch_prefix(path)
    }

    pub fn primitive(&self, path: Path) -> Result<Option<Ref<Primitive>>> {
        if path.ty() != Some(DotStoreType::Fun) {
            return Err(anyhow!("is not a primitive path"));
        }
        if let Some(bytes) = self.state.get(path.as_ref())? {
            Ok(Some(Ref::new(bytes)))
        } else {
            Ok(None)
        }
    }

    pub fn primitives(&self, path: Path) -> impl Iterator<Item = Result<Ref<Primitive>>> + '_ {
        self.state
            .scan_prefix(path)
            .filter(|r| {
                r.as_ref()
                    .map(|(k, _)| Path::new(&k[..]).ty() == Some(DotStoreType::Fun))
                    .unwrap_or(true)
            })
            .map(|r| r.map(|(_, v)| Ref::new(v)).map_err(Into::into))
    }

    pub fn policy(&self, path: Path<'_>) -> Result<Option<Ref<BTreeSet<Policy>>>> {
        if path.ty() != Some(DotStoreType::Policy) {
            return Err(anyhow!("is not a policy path"));
        }
        if let Some(bytes) = self.state.get(path.as_ref())? {
            Ok(Some(Ref::new(bytes)))
        } else {
            Ok(None)
        }
    }

    pub fn can(&self, peer: &PeerId, perm: Permission, path: Path) -> Result<bool> {
        self.acl.can(*peer, perm, path)
    }

    fn join_store(
        &self,
        doc: DocId,
        peer: &PeerId,
        that: &DotStore,
        expired: &DotSet,
    ) -> Result<()> {
        let path = PathBuf::new(doc);
        let mut common = BTreeSet::new();
        for item in self.state.scan_prefix(&path) {
            let (k, v) = item?;
            let k = Path::new(&k);
            let dot = k.dot();
            match that.get(&k) {
                Some(w) => {
                    common.insert(k.to_owned());
                    // different value for the same dot would be a bug
                    assert_eq!(v, w);
                    // new value should not be in the expired set
                    assert!(!expired.contains(&dot));
                }
                None => {
                    if !self.can(peer, Permission::Write, k)? {
                        tracing::info!("00: skipping {} due to lack of permissions", k);
                        continue;
                    }
                    // The type does not even matter.
                    // If it is in the expired set, it needs go to.
                    if expired.contains(&dot) {
                        self.state.remove(&k)?;
                    }
                }
            }
        }
        for (k, w) in &that.0 {
            if !common.contains(k) {
                let dot = k.as_path().dot();
                // new value should not be in the expired set
                assert!(!expired.contains(&dot));
                if self.can(peer, Permission::Write, k.as_path())? {
                    self.state.insert(&k, w.clone())?;
                } else {
                    tracing::info!("11: skipping {} due to lack of permissions", k.as_path());
                }
            }
        }
        Ok(())
    }

    pub fn join_policy(&self, causal: &Causal) -> Result<()> {
        for (k, w) in &causal.store.0 {
            if !self.state.contains_key(k.as_path())?
                && k.as_path().ty() == Some(DotStoreType::Policy)
            {
                self.state.insert(&k, w.clone())?;
            }
        }
        Ok(())
    }

    pub fn join(&self, peer_id: &PeerId, causal: &Causal) -> Result<()> {
        let doc = causal.doc;
        self.join_store(causal.doc, peer_id, &causal.store, causal.expired())?;
        for dot in causal.expired().iter() {
            let mut path = PathBuf::new(doc);
            path.dotset(&dot);
            self.expired.insert(&path, &[])?;
        }
        Ok(())
    }

    pub fn unjoin(&self, peer_id: &PeerId, other: &Archived<CausalContext>) -> Result<Causal> {
        let prefix = PathBuf::new(other.doc);
        let ctx = self.ctx(other.doc)?;
        let dots = ctx.dots.difference(&other.dots);
        let expired = ctx.expired.difference(&other.expired);
        let mut store = DotStore::default();
        for r in self.state.scan_prefix(prefix) {
            let (k, v) = r?;
            let path = Path::new(&k[..]);
            let dot = path.dot();
            if !dots.contains(&dot) {
                continue;
            }
            if !self.can(peer_id, Permission::Read, path)? {
                tracing::info!("unjoin: peer is unauthorized to read");
                continue;
            }
            store.0.insert(path.to_owned(), v.to_vec());
        }
        Ok(Causal {
            doc: ctx.doc,
            schema: ctx.schema,
            expired,
            store,
        })
    }

    fn empty_ctx(&self, doc: DocId) -> Result<CausalContext> {
        let schema = self.docs.schema_id(&doc)?;
        Ok(CausalContext {
            doc,
            schema: schema.into(),
            dots: Default::default(),
            expired: Default::default(),
        })
    }

    // reads all dots for a docid.
    fn dots(&self, doc: DocId) -> impl Iterator<Item = sled::Result<Dot>> {
        let path = PathBuf::new(doc);
        self.state
            .scan_prefix(path.as_path())
            .keys()
            .map(move |i| i.map(|key| Path::new(&key).dot()))
    }

    // reads all expired for a docid.
    fn expired(&self, doc: DocId) -> impl Iterator<Item = sled::Result<Dot>> {
        let path = PathBuf::new(doc);
        self.expired
            .scan_prefix(path.as_path())
            .keys()
            .map(move |i| i.map(|key| Path::new(&key).dot()))
    }

    pub fn ctx(&self, doc: DocId) -> Result<CausalContext> {
        let mut ctx = self.empty_ctx(doc)?;
        for dot in self.dots(doc) {
            ctx.dots.insert(dot?);
        }
        for dot in self.expired(doc) {
            ctx.expired.insert(dot?);
        }
        Ok(ctx)
    }

    pub fn enable(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let dot = writer.dot();
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        Ok(Causal {
            store: DotStore::dotset(std::iter::once(dot)).prefix(path),
            doc,
            schema: schema.into(),
            expired: Default::default(),
        })
    }

    pub fn disable(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let mut expired = DotSet::default();
        // add all dots to be tombstoned into the context
        for i in self.state.scan_prefix(&path).keys() {
            let i = i?;
            let path = Path::new(&i);
            let dot = path.dot();
            let ty = path.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            expired.insert(dot);
        }
        Ok(Causal {
            store: DotStore::dotset([]).prefix(path),
            doc,
            expired,
            schema: schema.into(),
        })
    }

    pub fn is_enabled(&self, path: Path<'_>) -> bool {
        self.state.scan_prefix(path).next().is_some()
    }

    pub fn assign(&self, path: Path, writer: &Writer, v: Primitive) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let mut expired = DotSet::default();
        // add all dots to be tombstoned into the context
        for i in self.state.scan_prefix(&path).keys() {
            let i = i?;
            let path = Path::new(&i);
            let dot = path.dot();
            expired.insert(dot);
        }
        // add the new value into the context with a new dot
        let dot = writer.dot();
        Ok(Causal {
            store: DotStore::dotfun(std::iter::once((dot, v))).prefix(path),
            doc,
            schema: schema.into(),
            expired,
        })
    }

    pub fn values(&self, path: Path<'_>) -> impl Iterator<Item = sled::Result<Ref<Primitive>>> {
        self.state
            .scan_prefix(path)
            .values()
            .map(|res| res.map(Ref::new))
    }

    pub fn remove(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let mut expired = DotSet::default();
        let dot = writer.dot();
        expired.insert(dot);
        for res in self.state.scan_prefix(path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            let ty = key.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            expired.insert(dot);
        }
        Ok(Causal {
            store: DotStore::default(),
            doc,
            expired,
            schema: schema.into(),
        })
    }

    pub fn say(&self, path: Path, writer: &Writer, policy: Policy) -> Result<Causal> {
        if !match &policy {
            Policy::Can(_, perm) | Policy::CanIf(_, perm, _) => {
                if perm.controllable() {
                    self.can(writer.peer_id(), Permission::Control, path)?
                } else {
                    self.can(writer.peer_id(), Permission::Own, path)?
                }
            }
            Policy::Revokes(_) => self.can(writer.peer_id(), Permission::Control, path)?,
        } {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let dot = writer.dot();
        Ok(Causal {
            store: DotStore::policy(std::iter::once((dot, policy))).prefix(path),
            doc,
            schema: schema.into(),
            expired: DotSet::default(),
        })
    }

    pub fn transform(
        &self,
        doc: &DocId,
        schema_id: &Hash,
        from: &ArchivedLenses,
        to: &ArchivedLenses,
    ) -> Result<()> {
        from.transform_crdt(doc, self, to)?;
        self.docs.set_schema_id(doc, schema_id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{props::*, Keypair};
    use crate::{Backend, Kind, Lens, PrimitiveKind};
    use proptest::prelude::*;
    use std::pin::Pin;

    #[async_std::test]
    async fn test_ewflag() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let peer = PeerId::new([0; 32]);
        let hash = sdk.register(vec![Lens::Make(Kind::Flag)])?;
        let doc = sdk.frontend().create_doc(peer, &hash)?;
        Pin::new(&mut sdk).await?;
        let op = doc.cursor().enable()?;
        assert!(!doc.cursor().enabled()?);
        sdk.join(&peer, op)?;
        assert!(doc.cursor().enabled()?);
        let op = doc.cursor().disable()?;
        sdk.join(&peer, op)?;
        assert!(!doc.cursor().enabled()?);
        Ok(())
    }

    #[async_std::test]
    async fn test_ewflag_unjoin() -> Result<()> {
        let peer1 = PeerId::new([0; 32]);
        let la = Keypair::generate();

        let mut sdk1 = Backend::memory()?;
        let hash1 = sdk1.register(vec![Lens::Make(Kind::Flag)])?;
        let doc1 = sdk1
            .frontend()
            .create_doc_deterministic(peer1, &hash1, la)?;
        Pin::new(&mut sdk1).await?;

        let mut sdk2 = Backend::memory()?;
        let hash2 = sdk2.register(vec![Lens::Make(Kind::Flag)])?;
        let doc2 = sdk2
            .frontend()
            .create_doc_deterministic(peer1, &hash2, la)?;
        Pin::new(&mut sdk2).await?;
        assert_eq!(hash1, hash2);

        let mut op = doc1.cursor().enable()?;
        println!("{:?}", op);
        sdk1.join(&peer1, op.clone())?;
        op.doc = *doc2.id();
        sdk2.join(&peer1, op)?;

        println!("{:#?}", doc1);
        println!("{:#?}", doc2);
        assert!(doc1.cursor().enabled()?);
        assert!(doc2.cursor().enabled()?);

        let ctx_after_enable = doc1.ctx()?;
        let mut op = doc1.cursor().disable()?;
        sdk1.join(&peer1, op.clone())?;
        let ctx_after_disable = doc1.ctx()?;
        if false {
            // apply the op
            op.doc = *doc2.id();
            sdk2.join(&peer1, op)?;
        } else {
            // compute the delta using unjoin, and apply that
            let mut delta = sdk1.unjoin(&peer1, Ref::archive(&ctx_after_enable).as_ref())?;
            let diff = ctx_after_disable
                .expired
                .difference(&ctx_after_enable.expired);
            println!("op {:?}", op);
            println!("expired after enable {:?}", ctx_after_enable.expired);
            println!("expired after disable {:?}", ctx_after_disable.expired);
            println!("difference {:?}", diff);
            println!("delta {:?}", delta);
            delta.doc = *doc2.id();
            println!("{:?}", delta.store());
            sdk2.join(&peer1, delta)?;
        }
        assert!(!doc1.cursor().enabled()?);
        assert!(!doc2.cursor().enabled()?);

        Ok(())
    }

    #[async_std::test]
    async fn test_mvreg() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let hash = sdk.register(vec![Lens::Make(Kind::Reg(PrimitiveKind::U64))])?;
        let peer1 = PeerId::new([1; 32]);
        let doc = sdk.frontend().create_doc(peer1, &hash)?;
        Pin::new(&mut sdk).await?;

        let peer2 = PeerId::new([2; 32]);
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        sdk.join(&peer1, op)?;

        let op1 = doc.cursor().assign(Primitive::U64(42))?;
        sdk.join(&peer1, op1)?;

        //TODO
        //let op2 = crdt.assign(path.as_path(), &peer2, Primitive::U64(43))?;
        //crdt.join(&peer2, &op2)?;

        let values = doc.cursor().u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        //assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        //assert!(values.contains(&43));

        let op = doc.cursor().assign(Primitive::U64(99))?;
        sdk.join(&peer1, op)?;

        let values = doc.cursor().u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 1);
        assert!(values.contains(&99));

        Ok(())
    }

    #[async_std::test]
    async fn test_ormap() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let peer = PeerId::new([1; 32]);
        let hash = sdk.register(vec![
            Lens::Make(Kind::Table(PrimitiveKind::Str)),
            Lens::LensMapValue(Box::new(Lens::Make(Kind::Table(PrimitiveKind::Str)))),
            Lens::LensMapValue(Box::new(Lens::LensMapValue(Box::new(Lens::Make(
                Kind::Reg(PrimitiveKind::U64),
            ))))),
        ])?;
        let doc = sdk.frontend().create_doc(peer, &hash)?;
        Pin::new(&mut sdk).await?;

        let a = Primitive::Str("a".into());
        let b = Primitive::Str("b".into());
        let cur = doc.cursor().key(&a)?.key(&b)?;
        let op = cur.assign(Primitive::U64(42))?;
        sdk.join(&peer, op)?;

        let values = cur.u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 1);
        assert!(values.contains(&42));

        let cur = doc.cursor().key(&a)?;
        let op = cur.remove(b.clone())?;
        sdk.join(&peer, op)?;

        let values = cur.key(&b)?.u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert!(values.is_empty());

        Ok(())
    }

    proptest! {
        #[test]
        fn causal_unjoin(a in arb_causal(arb_dotstore()), b in arb_causal_ctx()) {
            let b = a.unjoin(&b);
            prop_assert_eq!(join(&a, &b), a);
        }

        #[test]
        fn causal_join_idempotent(a in arb_causal(arb_dotstore())) {
            prop_assert_eq!(join(&a, &a), a);
        }

        #[test]
        fn causal_join_commutative(dots in arb_causal(arb_dotstore()), a in arb_causal_ctx(), b in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let ab = join(&a, &b);
            let ba = join(&b, &a);
            if ab != ba {
                println!("ab {:?}\nba {:?}", ab.dots(), ba.dots());
                println!("ab {:?}\nba {:?}", ab.expired(), ba.expired());
                println!("ab {:?}\nba {:?}", ab.store(), ba.store());
                println!();
                prop_assert_eq!(ab, ba);
            }
        }

        #[test]
        fn causal_join_associative(dots in arb_causal(arb_dotstore()), a in arb_causal_ctx(), b in arb_causal_ctx(), c in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let c = dots.unjoin(&c);
            prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
        }

        #[test]
        fn crdt_join(dots in arb_causal(arb_dotstore()), a in arb_causal_ctx(), b in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let crdt = causal_to_crdt(&a);
            let c = join(&a, &b);
            crdt.join(&dots.doc.into(), &b).unwrap();
            let c2 = crdt_to_causal(&crdt, &dots.ctx());
            // TODO: crdt doesn't causally join
            assert_eq!(c.store, c2.store);
        }

        #[test]
        fn crdt_unjoin(causal in arb_causal(arb_dotstore()), ctx in arb_causal_ctx()) {
            let crdt = causal_to_crdt(&causal);
            let c = causal.unjoin(&ctx);
            let actx = Ref::archive(&ctx);
            let mut c2 = crdt.unjoin(&ctx.doc.into(), actx.as_ref()).unwrap();
            c2.schema = [0; 32];
            // TODO: crdt doesn't causally join
            assert_eq!(c.store, c2.store);
        }
    }
}

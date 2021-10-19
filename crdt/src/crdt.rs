use crate::{
    AbstractDotSet, Acl, ArchivedLenses, DocId, Dot, DotSet, Path, PathBuf, PeerId, Permission,
    Subscriber,
};
use anyhow::Result;
use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(Debug, CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub struct DotStore(BTreeSet<PathBuf>);

impl DotStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// prefix the entire dot store with a path
    pub fn prefix(&self, path: Path) -> Self {
        Self(
            self.0
                .iter()
                .map(|p| {
                    let mut path = path.to_owned();
                    path.extend(p.as_path());
                    path
                })
                .collect(),
        )
    }

    pub fn contains(&self, path: &Path) -> bool {
        self.0.contains(path.as_ref())
    }

    pub fn insert(&mut self, path: PathBuf) {
        self.0.insert(path);
    }

    pub fn iter(&self) -> impl Iterator<Item = Path<'_>> + '_ {
        self.0.iter().map(|path| path.as_path())
    }

    pub fn dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.0.iter().map(|path| path.as_path().dot())
    }

    pub fn union(&mut self, other: &Self) {
        for path in other.iter() {
            self.insert(path.to_owned());
        }
    }

    pub fn join(&mut self, other: &Self, expired: &impl AbstractDotSet) {
        for path in other.0.iter() {
            self.0.insert(path.clone());
        }
        self.0
            .retain(|path| !expired.contains(&path.as_path().dot()));
    }

    pub fn unjoin(&self, diff: &DotSet) -> Self {
        Self(
            self.0
                .iter()
                .filter_map(|path| {
                    if diff.contains(&path.as_path().dot()) {
                        Some(path.clone())
                    } else {
                        None
                    }
                })
                .collect(),
        )
    }
}

impl ArchivedDotStore {
    pub fn dots(&self) -> impl Iterator<Item = Dot> + '_ {
        self.0.iter().map(|path| path.as_path().dot())
    }
}

#[derive(Clone, Default, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct CausalContext {
    /// the dots to be considered. These are the dots in the store.
    pub(crate) dots: DotSet,
    /// the expired dots. The intersection of this and dots must be empty.
    pub(crate) expired: DotSet,
}

impl CausalContext {
    pub fn new() -> Self {
        Self {
            dots: Default::default(),
            expired: Default::default(),
        }
    }

    pub fn dots(&self) -> &DotSet {
        &self.dots
    }

    pub fn expired(&self) -> &DotSet {
        &self.expired
    }
}

impl ArchivedCausalContext {
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
            .field("dots", &self.dots)
            .field("expired", &self.expired)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct Causal {
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

    pub fn join(&mut self, that: &Causal) {
        self.expired.union(&that.expired);
        self.store.join(&that.store, &self.expired);
    }

    pub fn unjoin(&self, ctx: &CausalContext) -> Self {
        let diff = self.dots().difference(ctx.dots());
        let expired = self.expired.difference(&ctx.expired);
        let store = self.store.unjoin(&diff);
        Self { expired, store }
    }

    pub fn ctx(&self) -> CausalContext {
        CausalContext {
            dots: self.dots(),
            expired: self.expired.clone(),
        }
    }

    pub fn transform(&mut self, from: &ArchivedLenses, to: &ArchivedLenses) {
        from.transform_dotstore(&mut self.store, to);
    }
}

impl ArchivedCausal {
    pub fn ctx(&self) -> CausalContext {
        CausalContext {
            expired: self.expired.to_dotset(),
            dots: self.store.dots().collect(),
        }
    }
}

#[derive(Clone)]
pub struct Crdt {
    state: sled::Tree,
    expired: sled::Tree,
    acl: Acl,
}

impl std::fmt::Debug for Crdt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crdt")
            .field("state", &StateDebug(&self.state))
            .field("expired", &ExpiredDebug(&self.expired))
            .field("acl", &self.acl)
            .finish()
    }
}

struct StateDebug<'a>(&'a sled::Tree);

impl<'a> std::fmt::Debug for StateDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for e in self.0.iter().keys() {
            let k = e.map_err(|_| std::fmt::Error::default())?;
            let path = Path::new(&k);
            m.entry(&path.dot(), &path);
        }
        m.finish()
    }
}

struct ExpiredDebug<'a>(&'a sled::Tree);

impl<'a> std::fmt::Debug for ExpiredDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_set();
        for e in self.0.iter().keys() {
            let k = e.map_err(|_| std::fmt::Error::default())?;
            let path = Path::new(&k);
            m.entry(&path);
        }
        m.finish()
    }
}

impl Crdt {
    pub fn new(state: sled::Tree, expired: sled::Tree, acl: Acl) -> Self {
        Self {
            state,
            expired,
            acl,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = sled::Result<sled::IVec>> {
        self.state.iter().keys()
    }

    pub fn scan_path(&self, path: Path) -> impl Iterator<Item = Result<sled::IVec>> + '_ {
        self.state
            .scan_prefix(path)
            .keys()
            .map(|k| k.map_err(Into::into))
    }

    pub fn watch_path(&self, path: Path) -> Subscriber {
        Subscriber::new(
            self.state.watch_prefix(path),
            self.acl.subscribe(&path.first().unwrap().doc().unwrap()),
        )
    }

    pub fn can(&self, peer: &PeerId, perm: Permission, path: Path) -> Result<bool> {
        self.acl.can(*peer, perm, path)
    }

    /// reads all dots for a docid.
    pub fn dots(&self, doc: &DocId) -> impl Iterator<Item = sled::Result<Dot>> {
        let mut path = PathBuf::new();
        path.doc(doc);
        self.state
            .scan_prefix(path.as_path())
            .keys()
            .map(move |i| i.map(|key| Path::new(&key).dot()))
    }

    /// reads all expired for a docid.
    pub fn expired(&self, doc: &DocId) -> impl Iterator<Item = sled::Result<Dot>> {
        let mut path = PathBuf::new();
        path.doc(doc);
        self.expired
            .scan_prefix(path.as_path())
            .keys()
            .map(move |i| i.map(|key| Path::new(&key).last().unwrap().dot().unwrap()))
    }

    pub fn ctx(&self, doc: &DocId) -> Result<CausalContext> {
        let mut ctx = CausalContext::new();
        for dot in self.dots(doc) {
            ctx.dots.insert(dot?);
        }
        for dot in self.expired(doc) {
            ctx.expired.insert(dot?);
        }
        Ok(ctx)
    }

    pub fn join_policy(&self, causal: &Causal) -> Result<()> {
        for path in causal.store.iter() {
            if path.last().unwrap().policy().is_some() {
                self.state.insert(path.to_owned(), &[])?;
            }
        }
        Ok(())
    }

    pub fn join(&self, peer: &PeerId, doc: &DocId, causal: &Causal) -> Result<()> {
        let mut path = PathBuf::new();
        path.doc(doc);
        let mut common = BTreeSet::new();
        for r in self.scan_path(path.as_path()) {
            let k = r?;
            let path = Path::new(&k[..]);
            let dot = path.dot();
            if causal.store.contains(&path) && !causal.expired.contains(&dot) {
                common.insert(path.to_owned());
            } else if causal.expired.contains(&dot) {
                if !self.can(peer, Permission::Write, path)? {
                    tracing::info!("join: peer is unauthorized to remove {}", path);
                    continue;
                }
                self.state.remove(path)?;
            }
        }
        for path in causal.store.iter() {
            let dot = path.dot();
            if !common.contains(path.as_ref()) && !causal.expired.contains(&dot) {
                if !self.can(peer, Permission::Write, path)? {
                    tracing::info!("join: peer is unauthorized to insert {}", path);
                    continue;
                }
                self.state.insert(path.to_owned(), &[])?;
            }
        }
        for dot in causal.expired().iter() {
            // TODO: who can add to the expired set?
            let mut path = PathBuf::new();
            path.doc(doc);
            path.dot(dot);
            self.expired.insert(&path, &[])?;
        }
        Ok(())
    }

    pub fn unjoin(
        &self,
        peer_id: &PeerId,
        doc: &DocId,
        other: &Archived<CausalContext>,
    ) -> Result<Causal> {
        let mut path = PathBuf::new();
        path.doc(doc);

        let ctx = self.ctx(doc)?;
        let dots = ctx.dots.difference(&other.dots);
        let expired = ctx.expired.difference(&other.expired);

        let mut store = DotStore::default();
        for r in self.state.scan_prefix(path).keys() {
            let k = r?;
            let path = Path::new(&k[..]);
            let dot = path.dot();
            if !dots.contains(&dot) {
                continue;
            }
            if !self.can(peer_id, Permission::Read, path)? {
                tracing::info!("unjoin: peer is unauthorized to read {}", path);
                continue;
            }
            store.insert(path.to_owned());
        }
        Ok(Causal { expired, store })
    }

    pub fn remove(&self, doc: &DocId) -> Result<()> {
        let mut path = PathBuf::new();
        path.doc(doc);
        for r in self.state.scan_prefix(&path).keys() {
            let k = r?;
            self.state.remove(k)?;
        }
        for r in self.expired.scan_prefix(&path).keys() {
            let k = r?;
            self.expired.remove(k)?;
        }
        Ok(())
    }

    pub fn transform(&self, doc: &DocId, from: &ArchivedLenses, to: &ArchivedLenses) -> Result<()> {
        from.transform_crdt(doc, self, to)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{props::*, Keypair};
    use crate::{Backend, Kind, Lens, PrimitiveKind, Ref};
    use proptest::prelude::*;
    use std::pin::Pin;

    #[async_std::test]
    async fn test_ewflag() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let peer = sdk.frontend().generate_keypair()?;
        let hash = sdk.register(vec![Lens::Make(Kind::Flag)])?;
        let doc = sdk
            .frontend()
            .create_doc(peer, &hash, Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        assert!(!doc.cursor().enabled()?);

        let op = doc.cursor().enable()?;
        doc.apply(&op)?;
        assert!(doc.cursor().enabled()?);

        let op = doc.cursor().disable()?;
        doc.apply(&op)?;
        assert!(!doc.cursor().enabled()?);

        Ok(())
    }

    #[async_std::test]
    async fn test_ewflag_unjoin() -> Result<()> {
        let la = Keypair::generate();
        let key = Keypair::generate();
        let peer = key.peer_id();

        let mut sdk1 = Backend::memory()?;
        let hash1 = sdk1.register(vec![Lens::Make(Kind::Flag)])?;
        sdk1.frontend().add_keypair(key)?;
        let doc1 = sdk1.frontend().create_doc(peer, &hash1, la)?;
        Pin::new(&mut sdk1).await?;

        let mut sdk2 = Backend::memory()?;
        let hash2 = sdk2.register(vec![Lens::Make(Kind::Flag)])?;
        sdk2.frontend().add_keypair(key)?;
        let doc2 = sdk2.frontend().create_doc(peer, &hash2, la)?;
        Pin::new(&mut sdk2).await?;
        assert_eq!(hash1, hash2);

        let op = doc1.cursor().enable()?;
        doc1.apply(&op)?;
        doc2.apply(&op)?;

        assert!(doc1.cursor().enabled()?);
        assert!(doc2.cursor().enabled()?);

        let op = doc1.cursor().disable()?;
        doc1.apply(&op)?;

        let delta = sdk1.unjoin(&peer, doc1.id(), Ref::archive(&doc2.ctx()?).as_ref())?;
        sdk2.join(&peer, doc1.id(), &hash1, delta)?;

        assert!(!doc1.cursor().enabled()?);
        assert!(!doc2.cursor().enabled()?);

        Ok(())
    }

    #[async_std::test]
    async fn test_mvreg() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let hash = sdk.register(vec![Lens::Make(Kind::Reg(PrimitiveKind::U64))])?;
        let peer1 = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer1, &hash, Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let peer2 = sdk.frontend().generate_keypair()?;
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        doc.apply(&op)?;
        Pin::new(&mut sdk).await?;
        let doc2 = sdk.frontend().doc_as(*doc.id(), &peer2)?;

        let op1 = doc.cursor().assign_u64(42)?;
        let op2 = doc2.cursor().assign_u64(43)?;
        doc.apply(&op1)?;
        doc.apply(&op2)?;

        let values = doc.cursor().u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        assert!(values.contains(&43));

        let op = doc.cursor().assign_u64(99)?;
        doc.apply(&op)?;

        let values = doc.cursor().u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 1);
        assert!(values.contains(&99));

        Ok(())
    }

    #[async_std::test]
    async fn test_ormap() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let peer = sdk.frontend().generate_keypair()?;
        let hash = sdk.register(vec![
            Lens::Make(Kind::Table(PrimitiveKind::Str)),
            Lens::LensMapValue(Box::new(Lens::Make(Kind::Table(PrimitiveKind::Str)))),
            Lens::LensMapValue(Box::new(Lens::LensMapValue(Box::new(Lens::Make(
                Kind::Reg(PrimitiveKind::U64),
            ))))),
        ])?;
        let doc = sdk
            .frontend()
            .create_doc(peer, &hash, Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let cur = doc.cursor().key_str("a")?.key_str("b")?;
        let op = cur.assign_u64(42)?;
        doc.apply(&op)?;

        let values = cur.u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 1);
        assert!(values.contains(&42));

        let op = doc.cursor().key_str("a")?.key_str("b")?.remove()?;
        doc.apply(&op)?;

        let values = doc
            .cursor()
            .key_str("a")?
            .key_str("b")?
            .u64s()?
            .collect::<Result<BTreeSet<u64>>>()?;
        assert!(values.is_empty());

        Ok(())
    }

    proptest! {
        #[test]
        fn causal_unjoin(a in arb_causal(), b in arb_causal()) {
            let b = a.unjoin(&b.ctx());
            prop_assert_eq!(join(&a, &b), a);
        }

        #[test]
        fn causal_join_idempotent(a in arb_causal()) {
            prop_assert_eq!(join(&a, &a), a);
        }

        #[test]
        fn causal_join_commutative(a in arb_causal(), b in arb_causal()) {
            let ab = join(&a, &b);
            let ba = join(&b, &a);
            prop_assert_eq!(ab, ba);
        }

        #[test]
        fn causal_join_associative(a in arb_causal(), b in arb_causal(), c in arb_causal()) {
            prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
        }

        #[test]
        fn crdt_join(a in arb_causal(), b in arb_causal()) {
            let doc = DocId::new([0; 32]);
            let crdt = causal_to_crdt(&doc, &a);
            let c = join(&a, &b);
            crdt.join(&doc.into(), &doc, &b).unwrap();
            let c2 = crdt_to_causal(&doc, &crdt);
            assert_eq!(c, c2);
        }

        #[test]
        fn crdt_unjoin(a in arb_causal(), b in arb_causal()) {
            let doc = DocId::new([0; 32]);
            let crdt = causal_to_crdt(&doc, &a);
            let c = a.unjoin(&b.ctx());
            let actx = Ref::archive(&b.ctx());
            let c2 = crdt.unjoin(&doc.into(), &doc, actx.as_ref()).unwrap();
            assert_eq!(c, c2);
        }
    }
}

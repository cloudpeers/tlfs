use crate::acl::{Acl, Permission};
use crate::dotset::DotSet;
use crate::id::{DocId, PeerId};
use crate::lens::LensesRef;
use crate::path::{Path, PathBuf};
use crate::radixdb::BlobSet;
use crate::subscriber::Subscriber;
use anyhow::Result;
use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::iter::FromIterator;
use vec_collections::radix_tree::{AbstractRadixTree, AbstractRadixTreeMut, IterKey, RadixTree};

#[derive(Clone, Default, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(Debug, CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub struct DotStore(RadixTree<u8, ()>);

impl DotStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// prefix the entire dot store with a path
    pub fn prefix(&self, path: Path) -> Self {
        let mut res = self.0.clone();
        res.prepend(path.as_ref());
        Self(res)
    }

    pub fn contains(&self, path: Path) -> bool {
        self.0.contains_key(path.as_ref())
    }

    pub fn contains_prefix(&self, prefix: Path) -> bool {
        self.0.scan_prefix(prefix.as_ref()).next().is_some()
    }

    pub fn insert(&mut self, path: PathBuf) {
        self.0.union_with(&RadixTree::single(path.as_ref(), ()));
    }

    pub fn iter(&self) -> impl Iterator<Item = PathBuf> + '_ {
        self.0.iter().map(|path| Path::new(&path.0).to_owned())
    }

    pub fn union(&mut self, other: &Self) {
        self.0.union_with(&other.0)
    }

    pub fn extend(&mut self, other: Self) {
        self.0.union_with(&other.0)
    }
}

impl FromIterator<PathBuf> for DotStore {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = PathBuf>,
    {
        let mut store = DotStore::new();
        for path in iter.into_iter() {
            store.insert(path);
        }
        store
    }
}

/// Represents the state of a crdt.
#[derive(Clone, Default, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct CausalContext {
    /// Store dots. These are the dots in the store.
    pub(crate) store: DotSet,
    /// Expired dots. The intersection of this and dots must be empty.
    pub(crate) expired: DotSet,
}

impl CausalContext {
    /// Creates an empty `CausalContext`.
    pub fn new() -> Self {
        Self {
            store: Default::default(),
            expired: Default::default(),
        }
    }

    /// Returns the store dots. These are the active dots in the store.
    pub fn store(&self) -> &DotSet {
        &self.store
    }

    /// Returns the expired dots. These are the tombstoned dots in the store.
    pub fn expired(&self) -> &DotSet {
        &self.expired
    }
}

impl ArchivedCausalContext {
    /// Returns the store dots. These are the active dots in the store.
    pub fn store(&self) -> &Archived<DotSet> {
        &self.store
    }

    /// Returns the expired dots. These are the tombstoned dots in the store.
    pub fn expired(&self) -> &Archived<DotSet> {
        &self.expired
    }
}

impl std::fmt::Debug for CausalContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CausalContext")
            .field("store", &self.store)
            .field("expired", &self.expired)
            .finish()
    }
}

/// Represents a state transition of a crdt. Multiple state transitions can be combined
/// together into an atomic transaction.
#[derive(Clone, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, CheckBytes))]
#[repr(C)]
pub struct Causal {
    pub(crate) store: DotStore,
    pub(crate) expired: DotStore,
}

impl Causal {
    /// Returns the store. These are the new paths created by the transaction.
    pub fn store(&self) -> &DotStore {
        &self.store
    }

    /// Returns the expired. These are the paths tombstoned by the transaction.
    pub fn expired(&self) -> &DotStore {
        &self.expired
    }

    /// Computes the [`CausalContext`] of this transaction.
    pub fn ctx(&self) -> CausalContext {
        let mut ctx = CausalContext::new();
        for buf in self.store.iter() {
            let path = buf.as_path();
            ctx.store.insert(path.dot());
        }
        for buf in self.expired.iter() {
            let path = buf.as_path();
            let dot = path.parent().unwrap().parent().unwrap().dot();
            ctx.expired.insert(dot);
        }
        ctx
    }

    /// Combines two transactions into a larger transaction.
    pub fn join(&mut self, that: &Causal) {
        self.store.union(&that.store);
        self.expired.union(&that.expired);
        self.store.0.difference_with(&self.expired.0);
    }

    /// Returns the difference of a transaction and a [`CausalContext`].
    pub fn unjoin(&self, ctx: &CausalContext) -> Self {
        let mut expired = DotStore::new();
        for buf in self.expired.iter() {
            let path = buf.as_path();
            let dot = path.parent().unwrap().parent().unwrap().dot();
            if !ctx.expired.contains(&dot) {
                expired.insert(buf);
            }
        }
        let mut store = DotStore::new();
        for buf in self.store.iter() {
            let path = buf.as_path();
            let dot = path.dot();
            if !ctx.store.contains(&dot) && !ctx.expired.contains(&dot) && !expired.contains(path) {
                store.insert(buf);
            }
        }
        Self { expired, store }
    }

    /// Transforms a transaction so that it can be applied to a target document.
    pub fn transform(&mut self, from: LensesRef, to: LensesRef) {
        let mut store = DotStore::new();
        for buf in self.store.iter() {
            let path = buf.as_path();
            if let Some(path) = from.transform_path(path, to) {
                store.insert(path);
            }
        }
        self.store = store;
        let mut expired = DotStore::new();
        for buf in self.expired.iter() {
            let path = buf.as_path();
            if let Some(path) = from.transform_path(path, to) {
                expired.insert(path);
            }
        }
        self.expired = expired;
    }
}

impl std::fmt::Debug for Causal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Causal")
            .field("store", &self.store)
            .field("expired", &self.expired)
            .finish()
    }
}

impl std::fmt::Debug for DotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for p in self.iter() {
            let path = p.as_path();
            m.entry(&path.dot(), &path);
        }
        m.finish()
    }
}

#[derive(Clone)]
pub struct Crdt {
    store: BlobSet,
    expired: BlobSet,
    acl: Acl,
}

impl std::fmt::Debug for Crdt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crdt")
            .field("store", &StoreDebug(&self.store))
            .field("expired", &ExpiredDebug(&self.expired))
            .field("acl", &self.acl)
            .finish()
    }
}

struct StoreDebug<'a>(&'a BlobSet);

impl<'a> std::fmt::Debug for StoreDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for k in self.0.keys() {
            let path = Path::new(&k);
            m.entry(&path.dot(), &path);
        }
        m.finish()
    }
}

struct ExpiredDebug<'a>(&'a BlobSet);

impl<'a> std::fmt::Debug for ExpiredDebug<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut m = f.debug_map();
        for k in self.0.keys() {
            let path = Path::new(&k);
            m.entry(&path.parent().unwrap().parent().unwrap().dot(), &path);
        }
        m.finish()
    }
}

impl Crdt {
    pub fn new(store: BlobSet, expired: BlobSet, acl: Acl) -> Self {
        Self {
            store,
            expired,
            acl,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = IterKey<u8>> {
        self.store.keys()
    }

    pub fn scan_path(&self, path: Path) -> impl Iterator<Item = IterKey<u8>> {
        self.store.scan_prefix(path.as_ref().to_vec())
    }

    pub fn watch_path(&self, path: Path) -> Subscriber {
        Subscriber::new(
            self.store.watch_prefix(path),
            self.acl.subscribe(&path.first().unwrap().doc().unwrap()),
        )
    }

    pub fn can(&self, peer: &PeerId, perm: Permission, path: Path) -> Result<bool> {
        self.acl.can(*peer, perm, path)
    }

    pub fn ctx(&self, doc: &DocId) -> Result<CausalContext> {
        let mut ctx = CausalContext::new();
        let mut path = PathBuf::new();
        path.doc(doc);
        for k in self.store.scan_prefix(&path) {
            let dot = Path::new(&k).dot();
            ctx.store.insert(dot);
        }
        for k in self.expired.scan_prefix(&path) {
            let dot = Path::new(&k).parent().unwrap().parent().unwrap().dot();
            ctx.expired.insert(dot);
        }
        Ok(ctx)
    }

    pub fn join_policy(&self, causal: &Causal) -> Result<()> {
        for buf in causal.store.iter() {
            let path = buf.as_path();
            if path
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .last()
                .unwrap()
                .policy()
                .is_some()
            {
                self.store.insert(path);
            }
        }
        self.store.flush()?;
        Ok(())
    }

    /// Applies a transaction. Uses the peer that sent the transaction for acl and not
    /// the peer that created the transaction. The reason for this is that the logic
    /// would be a little bit more complicated to ensure convergence in the presence of
    /// revocations.
    pub fn join(&self, peer: &PeerId, causal: &Causal) -> Result<()> {
        for buf in causal.store.iter() {
            let path = buf.as_path();
            let is_expired = self.expired.scan_prefix(path.as_ref()).next().is_some();
            if !is_expired && !causal.expired.contains_prefix(path) {
                if !self.can(peer, Permission::Write, path)? {
                    tracing::info!("join: peer is unauthorized to insert {}", path);
                    continue;
                }
                self.store.insert(&path);
            }
        }
        for buf in causal.expired.iter() {
            let path = buf.as_path();
            let store_path = path.parent().unwrap().parent().unwrap();
            if !self.can(peer, Permission::Write, store_path)? {
                tracing::info!("join: peer is unauthorized to remove {}", store_path);
                continue;
            }
            if self.store.contains(store_path) {
                self.store.remove(store_path);
            }
            self.expired.insert(&path);
        }
        self.expired.flush()?;
        self.store.flush()?;
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
        let expired_dots = ctx.expired.difference(&other.expired);
        let store_dots = ctx
            .store
            .difference(&other.store)
            .difference(&other.expired);

        let mut store = DotStore::new();
        for k in self.store.scan_prefix(&path) {
            let path = Path::new(&k[..]);
            let dot = path.dot();
            if !store_dots.contains(&dot) {
                continue;
            }
            if !self.can(peer_id, Permission::Read, path)? {
                tracing::info!("unjoin: peer is unauthorized to read");
                continue;
            }
            if store_dots.contains(&dot) {
                store.insert(path.to_owned());
            }
        }
        let mut expired = DotStore::new();
        for k in self.expired.scan_prefix(&path) {
            let path = Path::new(&k);
            let dot = path.parent().unwrap().parent().unwrap().dot();
            if !expired_dots.contains(&dot) {
                continue;
            }
            if !self.can(peer_id, Permission::Read, path)? {
                tracing::info!("unjoin: peer is unauthorized to read {}", path);
                continue;
            }
            if expired_dots.contains(&dot) {
                expired.insert(path.to_owned());
            }
        }
        Ok(Causal { expired, store })
    }

    pub fn remove(&self, doc: &DocId) -> Result<()> {
        let mut path = PathBuf::new();
        path.doc(doc);
        for k in self.store.scan_prefix(&path) {
            self.store.remove(k);
        }
        for k in self.expired.scan_prefix(&path) {
            self.expired.remove(k);
        }
        self.expired.flush()?;
        self.store.flush()?;
        Ok(())
    }

    pub fn transform(&self, doc: &DocId, from: LensesRef, to: LensesRef) -> Result<()> {
        let mut path = PathBuf::new();
        path.doc(doc);
        for k in self.scan_path(path.as_path()) {
            let path = Path::new(&k);
            if let Some(path) = from.transform_path(path, to) {
                self.store.insert(path);
            }
            self.store.remove(k);
        }
        for k in self.scan_path(path.as_path()) {
            let path = Path::new(&k);
            if let Some(path) = from.transform_path(path, to) {
                self.expired.insert(path);
            }
            self.expired.remove(k);
        }
        self.expired.flush()?;
        self.store.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doc::Backend;
    use crate::lens::{Kind, Lens, Lenses};
    use crate::registry::Package;
    use crate::schema::PrimitiveKind;
    use crate::util::Ref;
    use crate::{props::*, Keypair};
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use std::pin::Pin;

    #[async_std::test]
    async fn test_ewflag() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![Lens::Make(Kind::Flag)]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, "crdt", Keypair::generate())?;
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
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![Lens::Make(Kind::Flag)]),
        )];
        let la = Keypair::generate();
        let key = Keypair::generate();
        let peer = key.peer_id();

        let mut sdk1 = Backend::memory(&packages)?;
        sdk1.frontend().add_keypair(key)?;
        let doc1 = sdk1.frontend().create_doc(peer, "crdt", la)?;
        Pin::new(&mut sdk1).await?;

        let mut sdk2 = Backend::memory(&packages)?;
        sdk2.frontend().add_keypair(key)?;
        let doc2 = sdk2.frontend().create_doc(peer, "crdt", la)?;
        Pin::new(&mut sdk2).await?;

        let op = doc1.cursor().enable()?;
        doc1.apply(&op)?;
        doc2.apply(&op)?;

        assert!(doc1.cursor().enabled()?);
        assert!(doc2.cursor().enabled()?);

        let op = doc1.cursor().disable()?;
        doc1.apply(&op)?;

        let delta = sdk1.unjoin(&peer, doc1.id(), Ref::archive(&doc2.ctx()?).as_ref())?;
        let hash = sdk1.frontend().schema(doc1.id())?.as_ref().hash();
        sdk2.join(&peer, doc1.id(), &hash, delta)?;

        assert!(!doc1.cursor().enabled()?);
        assert!(!doc2.cursor().enabled()?);

        Ok(())
    }

    #[async_std::test]
    async fn test_mvreg() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![Lens::Make(Kind::Reg(PrimitiveKind::U64))]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer1 = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer1, "crdt", Keypair::generate())?;
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
    async fn test_orarray_smoke() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![
                Lens::Make(Kind::Array),
                Lens::LensMap(Box::new(Lens::Make(Kind::Reg(PrimitiveKind::U64)))),
            ]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, "crdt", Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        let mut cur = doc.cursor();
        cur.index(0)?;
        let op = cur.assign_u64(42)?;

        let op1 = cur.assign_u64(43)?;
        doc.apply(&op)?;
        doc.apply(&op1)?;

        let r = doc
            .cursor()
            .index(0)?
            .u64s()?
            .collect::<Result<BTreeSet<_>>>()?;
        assert_eq!(r.len(), 2);
        assert!(r.contains(&42));
        assert!(r.contains(&43));

        let op2 = cur.assign_u64(44)?;
        doc.apply(&op2)?;

        let r = doc.cursor().index(0)?.u64s()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(r, vec![44]);

        let op_delete = doc.cursor().index(0)?.delete()?;
        doc.apply(&op_delete)?;
        assert!(doc.cursor().index(0)?.u64s()?.next().is_none());

        Ok(())
    }

    #[async_std::test]
    async fn test_orarray_nested_crdt() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![
                Lens::Make(Kind::Array),
                Lens::LensMap(Box::new(Lens::Make(Kind::Table(PrimitiveKind::Str)))),
                Lens::LensMap(Box::new(Lens::LensMapValue(Box::new(Lens::Make(
                    Kind::Reg(PrimitiveKind::U64),
                ))))),
            ]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, "crdt", Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        let mut cur = doc.cursor();
        cur.index(0)?.key_str("a")?;
        let op = cur.assign_u64(42)?;

        let op1 = cur.assign_u64(43)?;
        doc.apply(&op)?;
        doc.apply(&op1)?;

        let r = doc
            .cursor()
            .index(0)?
            .key_str("a")?
            .u64s()?
            .collect::<Result<BTreeSet<_>>>()?;
        assert_eq!(r.len(), 2);
        assert!(r.contains(&42));
        assert!(r.contains(&43));

        let op2 = cur.assign_u64(44)?;
        doc.apply(&op2)?;

        let r = doc
            .cursor()
            .index(0)?
            .key_str("a")?
            .u64s()?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(r, vec![44]);

        let op_delete = doc.cursor().index(0)?.key_str("a")?.delete()?;
        doc.apply(&op_delete)?;
        assert!(doc
            .cursor()
            .index(0)?
            .key_str("a")?
            .u64s()?
            .next()
            .is_none());

        Ok(())
    }

    #[async_std::test]
    async fn test_orarray_nested_orarray() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![
                Lens::Make(Kind::Array),
                Lens::LensMap(Box::new(Lens::Make(Kind::Table(PrimitiveKind::Str)))),
                Lens::LensMap(Box::new(Lens::LensMapValue(Box::new(Lens::Make(
                    Kind::Array,
                ))))),
                Lens::LensMap(Box::new(Lens::LensMapValue(Box::new(Lens::LensMap(
                    Box::new(Lens::Make(Kind::Reg(PrimitiveKind::U64))),
                ))))),
            ]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, "crdt", Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        let mut cur = doc.cursor();
        cur.index(0)?.key_str("a")?.index(0)?;
        let op = cur.assign_u64(42)?;

        let op1 = cur.assign_u64(43)?;
        doc.apply(&op)?;
        doc.apply(&op1)?;

        let r = doc
            .cursor()
            .index(0)?
            .key_str("a")?
            .index(0)?
            .u64s()?
            .collect::<Result<BTreeSet<_>>>()?;
        assert_eq!(r.len(), 2);
        assert!(r.contains(&42));
        assert!(r.contains(&43));

        let op2 = cur.assign_u64(44)?;
        doc.apply(&op2)?;

        let r = doc
            .cursor()
            .index(0)?
            .key_str("a")?
            .index(0)?
            .u64s()?
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(r, vec![44]);

        let op_delete = doc.cursor().index(0)?.key_str("a")?.delete()?;
        doc.apply(&op_delete)?;
        assert!(doc
            .cursor()
            .index(0)?
            .key_str("a")?
            .index(0)?
            .u64s()?
            .next()
            .is_none());

        Ok(())
    }

    // FIXME Fraction Ordering
    #[ignore]
    #[async_std::test]
    async fn test_orarray_move() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![
                Lens::Make(Kind::Array),
                Lens::LensMap(Box::new(Lens::Make(Kind::Reg(PrimitiveKind::U64)))),
            ]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, "crdt", Keypair::generate())?;
        Pin::new(&mut sdk).await?;
        for i in 0..10 {
            let op = doc.cursor().index(i)?.assign_u64(i as u64)?;
            doc.apply(&op)?;
        }
        let mut r = vec![];
        for i in 0..10 {
            r.extend(doc.cursor().index(i)?.u64s()?.collect::<Result<Vec<_>>>()?);
        }
        assert_eq!(r, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

        println!("{:#?}", doc);
        let move_op = doc.cursor().index(5)?.r#move(2)?;
        println!("move_op {:#?}", move_op);
        doc.apply(&move_op)?;

        let mut r = vec![];
        for i in 0..10 {
            r.extend(doc.cursor().index(i)?.u64s()?.collect::<Result<Vec<_>>>()?);
        }
        assert_eq!(r, vec![0, 1, 5, 2, 3, 4, 6, 7, 8, 9]);
        Ok(())
    }

    #[async_std::test]
    async fn test_ormap() -> Result<()> {
        let packages = vec![Package::new(
            "crdt".into(),
            1,
            &Lenses::new(vec![
                Lens::Make(Kind::Table(PrimitiveKind::Str)),
                Lens::LensMapValue(Box::new(Lens::Make(Kind::Table(PrimitiveKind::Str)))),
                Lens::LensMapValue(Box::new(Lens::LensMapValue(Box::new(Lens::Make(
                    Kind::Reg(PrimitiveKind::U64),
                ))))),
            ]),
        )];
        let mut sdk = Backend::memory(&packages)?;
        let peer = sdk.frontend().generate_keypair()?;
        let doc = sdk
            .frontend()
            .create_doc(peer, "crdt", Keypair::generate())?;
        Pin::new(&mut sdk).await?;

        let mut cur = doc.cursor();
        cur.key_str("a")?.key_str("b")?;
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
            crdt.join(&doc.into(), &b).unwrap();
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

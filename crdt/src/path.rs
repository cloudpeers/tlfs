use crate::{CausalContext, DocId, Dot, PeerId, Policy};
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{archived_root, Archive, Archived, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;

fn archive<T>(t: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(t).unwrap();
    ser.into_serializer().into_inner().to_vec()
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd, CheckBytes))]
#[repr(C)]
pub enum Primitive {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
}

impl From<bool> for Primitive {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<u64> for Primitive {
    fn from(u: u64) -> Self {
        Self::U64(u)
    }
}

impl From<i64> for Primitive {
    fn from(i: i64) -> Self {
        Self::I64(i)
    }
}

impl From<String> for Primitive {
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

impl From<&str> for Primitive {
    fn from(s: &str) -> Self {
        Self::Str(s.to_string())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Ref<T> {
    marker: PhantomData<T>,
    bytes: sled::IVec,
}

impl<T> Ref<T>
where
    T: Archive,
    Archived<T>: Deserialize<T, rkyv::Infallible>,
{
    pub fn new(bytes: sled::IVec) -> Self {
        Self {
            marker: PhantomData,
            bytes,
        }
    }

    pub fn to_owned(&self) -> Result<T> {
        Ok(self.as_ref().deserialize(&mut rkyv::Infallible)?)
    }
}

impl<T: Archive> AsRef<Archived<T>> for Ref<T> {
    fn as_ref(&self) -> &Archived<T> {
        unsafe { archived_root::<T>(&self.bytes[..]) }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub enum DotStore {
    Null,
    DotSet(BTreeSet<Dot>),
    DotFun(BTreeMap<Dot, Primitive>),
    DotMap(#[omit_bounds] BTreeMap<Primitive, DotStore>),
    Struct(#[omit_bounds] BTreeMap<String, DotStore>),
    Policy(BTreeMap<Dot, Policy>),
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive(as = "DotStoreType")]
#[repr(u8)]
pub enum DotStoreType {
    Root,
    Set,
    Fun,
    Map,
    Struct,
    Policy,
}

impl DotStoreType {
    fn from(u: u8) -> Option<Self> {
        use DotStoreType::*;
        match u {
            u if u == Root as u8 => Some(Root),
            u if u == Set as u8 => Some(Set),
            u if u == Fun as u8 => Some(Fun),
            u if u == Map as u8 => Some(Map),
            u if u == Struct as u8 => Some(Struct),
            _ => None,
        }
    }

    fn default(&self) -> Option<DotStore> {
        use DotStoreType::*;
        match self {
            Root => None,
            Set => Some(DotStore::DotSet(Default::default())),
            Fun => Some(DotStore::DotFun(Default::default())),
            Map => Some(DotStore::DotMap(Default::default())),
            Struct => Some(DotStore::Struct(Default::default())),
            Policy => Some(DotStore::Policy(Default::default())),
        }
    }
}

impl DotStore {
    pub fn dots(&self, ctx: &mut CausalContext) {
        match self {
            Self::Null => {}
            Self::DotSet(set) => {
                for dot in set {
                    ctx.insert(*dot);
                }
            }
            Self::DotFun(fun) => {
                for dot in fun.keys() {
                    ctx.insert(*dot);
                }
            }
            Self::DotMap(map) => {
                for store in map.values() {
                    store.dots(ctx);
                }
            }
            Self::Struct(fields) => {
                for store in fields.values() {
                    store.dots(ctx);
                }
            }
            Self::Policy(policy) => {
                for dot in policy.keys() {
                    ctx.insert(*dot);
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct Causal {
    pub store: DotStore,
    pub ctx: CausalContext,
}

#[derive(
    Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
#[repr(C)]
pub struct PathBuf(Vec<u8>);

impl PathBuf {
    pub fn new(id: DocId) -> Self {
        let mut path = Self::default();
        path.extend(DotStoreType::Root, id.as_ref());
        path
    }

    fn extend_len(&mut self, len: usize) {
        assert!(len <= u16::MAX as usize);
        self.0.extend((len as u16).to_be_bytes());
    }

    fn extend(&mut self, ty: DotStoreType, bytes: &[u8]) {
        self.0.extend(&[ty as u8]);
        self.extend_len(bytes.len());
        self.0.extend(bytes);
        self.extend_len(bytes.len());
        self.0.extend(&[ty as u8]);
    }

    pub fn key(&mut self, key: &Primitive) {
        self.extend(DotStoreType::Map, &archive(key));
    }

    pub fn field(&mut self, field: &str) {
        self.extend(DotStoreType::Struct, field.as_bytes());
    }

    pub fn dotset(&mut self, dot: &Dot) {
        self.extend(DotStoreType::Set, &archive(dot));
    }

    pub fn dotfun(&mut self, dot: &Dot) {
        self.extend(DotStoreType::Fun, &archive(dot));
    }

    pub fn policy(&mut self, dot: &Dot) {
        self.extend(DotStoreType::Policy, &archive(dot));
    }

    pub fn pop(&mut self) {
        if let Some(path) = self.as_path().parent() {
            let len = path.0.len();
            self.0.truncate(len);
        }
    }

    pub fn as_path(&self) -> Path<'_> {
        Path(&self.0)
    }
}

impl AsRef<[u8]> for PathBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Path<'a>(&'a [u8]);

impl<'a> Path<'a> {
    pub fn new(p: &'a [u8]) -> Self {
        Self(p)
    }

    pub fn parent(&self) -> Option<Path<'a>> {
        if self.0.is_empty() {
            return None;
        }
        let pos = self.0.len() - 3;
        let mut len = [0; 2];
        len.copy_from_slice(&self.0[pos..(pos + 2)]);
        let len = u16::from_be_bytes(len) as usize;
        let ppos = pos - len - 3;
        Some(Path(&self.0[..ppos]))
    }

    fn target(&self) -> &[u8] {
        let startpos = self.parent().unwrap().0.len() + 3;
        let endpos = self.0.len() - 3;
        &self.0[startpos..endpos]
    }

    fn first(&self) -> Path<'_> {
        let mut len = [0; 2];
        len.copy_from_slice(&self.0[1..3]);
        let len = u16::from_be_bytes(len) as usize;
        Path::new(&self.0[..(len + 6)])
    }

    pub fn ty(&self) -> Option<DotStoreType> {
        DotStoreType::from(*self.0.last()?)
    }

    pub fn doc(&self) -> DocId {
        use std::convert::TryInto;
        debug_assert_eq!(self.ty(), Some(DotStoreType::Root));
        let doc = self.target();
        DocId::new(doc.try_into().unwrap())
    }

    pub fn key(&self) -> Ref<Primitive> {
        debug_assert_eq!(self.ty(), Some(DotStoreType::Map));
        let key = self.target();
        Ref::new(key.into())
    }

    pub fn field(&self) -> &str {
        debug_assert_eq!(self.ty(), Some(DotStoreType::Struct));
        let field = self.target();
        unsafe { std::str::from_utf8_unchecked(field) }
    }

    pub fn dot(&self) -> Dot {
        debug_assert!(
            self.ty() == Some(DotStoreType::Set)
                || self.ty() == Some(DotStoreType::Fun)
                || self.ty() == Some(DotStoreType::Policy)
        );
        let bytes = self.target();
        let mut dot = Dot::new(PeerId::new([0; 32]), 1);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const Dot, &mut dot as *mut _, 1)
        };
        dot
    }

    fn wrap(&self, mut causal: Causal) -> Result<Causal> {
        use DotStoreType::*;
        match self.ty() {
            Some(Map) => {
                let mut map = BTreeMap::new();
                let key = self.key().to_owned()?;
                map.insert(key, causal.store);
                causal.store = DotStore::DotMap(map);
                self.parent().unwrap().wrap(causal)
            }
            Some(Struct) => {
                let mut map = BTreeMap::new();
                map.insert(self.field().to_string(), causal.store);
                causal.store = DotStore::Struct(map);
                self.parent().unwrap().wrap(causal)
            }
            Some(Root) => Ok(causal),
            ty => Err(anyhow!("invalid path {:?}", ty)),
        }
    }

    pub fn root(&self) -> Option<DocId> {
        let first = self.first();
        if let Some(DotStoreType::Root) = first.ty() {
            Some(first.doc())
        } else {
            None
        }
    }

    pub fn is_ancestor(&self, other: Path) -> bool {
        other.as_ref().starts_with(self.as_ref())
    }

    pub fn to_owned(&self) -> PathBuf {
        PathBuf(self.0.to_vec())
    }
}

impl<'a> std::fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use DotStoreType::*;
        if let Some(ty) = self.ty() {
            if ty != Root {
                write!(f, "{}.", self.parent().unwrap())?;
            }
            match ty {
                Root => write!(f, "{}", self.doc())?,
                Set => write!(f, "{}", self.dot())?,
                Fun => write!(f, "{}", self.dot())?,
                Map => write!(f, "{:?}", self.key().as_ref())?,
                Struct => write!(f, "{}", self.field())?,
                Policy => write!(f, "{}", self.dot())?,
            }
        }
        Ok(())
    }
}

impl<'a> AsRef<[u8]> for Path<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

#[derive(Clone)]
pub struct Crdt(sled::Tree);

impl Crdt {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    pub fn memory(name: &str) -> Result<Self> {
        let db = sled::Config::new().temporary(true).open()?;
        let tree = db.open_tree(name)?;
        Ok(Self(tree))
    }

    pub fn iter(&self) -> impl Iterator<Item = sled::Result<(sled::IVec, sled::IVec)>> {
        self.0.iter()
    }

    pub fn contains(&self, path: Path) -> bool {
        self.0.scan_prefix(path).next().is_some()
    }

    pub fn primitive(&self, path: Path) -> Result<Option<Ref<Primitive>>> {
        if path.ty() != Some(DotStoreType::Fun) {
            return Err(anyhow!("is not a primitive path"));
        }
        if let Some(bytes) = self.0.get(path.as_ref())? {
            Ok(Some(Ref::new(bytes)))
        } else {
            Ok(None)
        }
    }

    pub fn primitives(&self, path: Path) -> impl Iterator<Item = Result<Ref<Primitive>>> + '_ {
        self.0
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
        if let Some(bytes) = self.0.get(path.as_ref())? {
            Ok(Some(Ref::new(bytes)))
        } else {
            Ok(None)
        }
    }

    pub fn watch_path(&self, path: Path<'_>) -> sled::Subscriber {
        self.0.watch_prefix(path)
    }

    fn join_dotset(
        &self,
        path: &mut PathBuf,
        ctx: &CausalContext,
        other: &BTreeSet<Dot>,
        other_ctx: &CausalContext,
    ) -> Result<()> {
        for res in self.0.scan_prefix(&path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            if key.ty() != Some(DotStoreType::Set) {
                continue;
            }
            let dot = key.dot();
            if !other.contains(&dot) && other_ctx.contains(&dot) {
                self.0.remove(key)?;
            }
        }
        for dot in other {
            if !ctx.contains(dot) {
                path.dotset(dot);
                self.0.insert(&path, &[])?;
                path.pop();
            }
        }
        Ok(())
    }

    fn join_dotfun(
        &self,
        path: &mut PathBuf,
        ctx: &CausalContext,
        other: &BTreeMap<Dot, Primitive>,
        other_ctx: &CausalContext,
    ) -> Result<()> {
        for res in self.0.scan_prefix(&path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            if key.ty() != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            if !other.contains_key(&dot) && other_ctx.contains(&dot) {
                self.0.remove(key)?;
            }
        }
        for (dot, v) in other {
            if ctx.contains(dot) {
                continue;
            }
            path.dotfun(dot);
            if self.0.contains_key(&path)? {
                continue;
            }
            self.0.insert(&path, archive(v))?;
            path.pop();
        }
        Ok(())
    }

    fn join_dotmap(
        &self,
        path: &mut PathBuf,
        ctx: &CausalContext,
        other: &BTreeMap<Primitive, DotStore>,
        other_ctx: &CausalContext,
    ) -> Result<()> {
        for res in self.0.scan_prefix(&path).keys() {
            let leaf = res?;
            let key = Path::new(&leaf[path.as_ref().len()..])
                .first()
                .key()
                .to_owned()?;
            path.key(&key);
            let default = Path::new(&leaf[path.as_ref().len()..])
                .first()
                .ty()
                .unwrap()
                .default()
                .unwrap();
            let store = other.get(&key).unwrap_or(&default);
            self.join_store(path, ctx, store, other_ctx)?;
            path.pop();
        }
        for (k, store) in other {
            path.key(k);
            self.join_store(path, ctx, store, other_ctx)?;
            path.pop();
        }
        Ok(())
    }

    fn join_struct(
        &self,
        path: &mut PathBuf,
        ctx: &CausalContext,
        other: &BTreeMap<String, DotStore>,
        other_ctx: &CausalContext,
    ) -> Result<()> {
        for (k, v) in other {
            path.field(k);
            match v {
                DotStore::Null => {}
                DotStore::DotSet(set) => self.join_dotset(path, ctx, set, other_ctx)?,
                DotStore::DotFun(fun) => self.join_dotfun(path, ctx, fun, other_ctx)?,
                DotStore::DotMap(map) => self.join_dotmap(path, ctx, map, other_ctx)?,
                DotStore::Struct(fields) => self.join_struct(path, ctx, fields, other_ctx)?,
                DotStore::Policy(policy) => self.join_policy(path, ctx, policy, other_ctx)?,
            }
            path.pop();
        }
        Ok(())
    }

    fn join_policy(
        &self,
        path: &mut PathBuf,
        _: &CausalContext,
        other: &BTreeMap<Dot, Policy>,
        _: &CausalContext,
    ) -> Result<()> {
        for (dot, policy) in other {
            path.policy(dot);
            self.0.transaction::<_, _, std::io::Error>(|tree| {
                let mut policies = if let Some(bytes) = tree.get(path.as_ref())? {
                    Ref::<BTreeSet<Policy>>::new(bytes).to_owned().unwrap()
                } else {
                    Default::default()
                };
                policies.insert(policy.clone());
                tree.insert(path.as_ref(), archive(&policies))?;
                Ok(())
            })?;
            path.pop();
        }
        Ok(())
    }

    fn join_store(
        &self,
        path: &mut PathBuf,
        ctx: &CausalContext,
        other: &DotStore,
        other_ctx: &CausalContext,
    ) -> Result<()> {
        match other {
            DotStore::Null => {}
            DotStore::DotSet(set) => self.join_dotset(path, ctx, set, other_ctx)?,
            DotStore::DotFun(fun) => self.join_dotfun(path, ctx, fun, other_ctx)?,
            DotStore::DotMap(map) => self.join_dotmap(path, ctx, map, other_ctx)?,
            DotStore::Struct(fields) => self.join_struct(path, ctx, fields, other_ctx)?,
            DotStore::Policy(policy) => self.join_policy(path, ctx, policy, other_ctx)?,
        }
        Ok(())
    }

    pub fn join(&self, doc: DocId, ctx: &mut CausalContext, causal: &Causal) -> Result<()> {
        let mut path = PathBuf::new(doc);
        self.join_store(&mut path, ctx, &causal.store, &causal.ctx)?;
        ctx.union(&causal.ctx);
        Ok(())
    }

    pub fn enable(&self, path: Path<'_>, ctx: &CausalContext, dot: Dot) -> Result<Causal> {
        let mut store = BTreeSet::new();
        store.insert(dot);
        let mut causal = Causal {
            store: DotStore::DotSet(store),
            ctx: ctx.clone(),
        };
        causal.ctx.insert(dot);
        path.wrap(causal)
    }

    pub fn disable(&self, path: Path<'_>, ctx: &CausalContext, dot: Dot) -> Result<Causal> {
        let mut causal = Causal {
            store: DotStore::DotSet(Default::default()),
            ctx: ctx.clone(),
        };
        causal.ctx.insert(dot);
        path.wrap(causal)
    }

    pub fn is_enabled(&self, path: Path<'_>) -> bool {
        self.0.scan_prefix(path).next().is_some()
    }

    pub fn assign(
        &self,
        path: Path<'_>,
        ctx: &CausalContext,
        dot: Dot,
        v: Primitive,
    ) -> Result<Causal> {
        let mut store = BTreeMap::new();
        store.insert(dot, v);
        let mut causal = Causal {
            store: DotStore::DotFun(store),
            ctx: ctx.clone(),
        };
        causal.ctx.insert(dot);
        path.wrap(causal)
    }

    pub fn values(&self, path: Path<'_>) -> impl Iterator<Item = sled::Result<Ref<Primitive>>> {
        self.0
            .scan_prefix(path)
            .values()
            .map(|res| res.map(Ref::new))
    }

    pub fn remove(&self, path: Path<'_>, dot: Dot) -> Result<Causal> {
        let mut ctx = CausalContext::default();
        ctx.insert(dot);
        for res in self.0.scan_prefix(path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            let ty = key.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            ctx.insert(dot);
        }
        let causal = Causal {
            store: DotStore::DotMap(Default::default()),
            ctx,
        };
        path.wrap(causal)
    }

    pub fn say(&self, path: Path<'_>, dot: Dot, policy: Policy) -> Result<Causal> {
        let mut ctx = CausalContext::default();
        ctx.insert(dot);
        let mut store = BTreeMap::new();
        store.insert(dot, policy);
        let causal = Causal {
            store: DotStore::Policy(store),
            ctx,
        };
        path.wrap(causal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::props::*;
    //use proptest::prelude::*;
    use sled::Config;

    #[test]
    fn test_ewflag() -> Result<()> {
        let crdt = Crdt::memory("test")?;
        let doc = DocId::new([0; 32]);
        let mut dot = Dot::new(PeerId::new([0; 32]), 1);
        let mut ctx = CausalContext::default();
        let mut path = PathBuf::new(doc);
        path.field("a");
        path.field("b");
        let op = crdt.enable(path.as_path(), &ctx, dot.inc())?;
        assert!(!crdt.is_enabled(path.as_path()));
        crdt.join(doc, &mut ctx, &op)?;
        assert!(crdt.is_enabled(path.as_path()));
        let op = crdt.disable(path.as_path(), &ctx, dot.inc())?;
        crdt.join(doc, &mut ctx, &op)?;
        assert!(!crdt.is_enabled(path.as_path()));
        Ok(())
    }

    #[test]
    fn test_mvreg() -> Result<()> {
        let crdt = Crdt::memory("test")?;
        let doc = DocId::new([0; 32]);
        let mut dot1 = Dot::new(PeerId::new([0; 32]), 1);
        let mut dot2 = Dot::new(PeerId::new([1; 32]), 1);
        let mut ctx = CausalContext::default();
        let mut path = PathBuf::new(doc);
        path.field("a");
        path.field("b");
        let op1 = crdt.assign(path.as_path(), &ctx, dot1.inc(), Primitive::U64(42))?;
        let op2 = crdt.assign(path.as_path(), &ctx, dot2.inc(), Primitive::U64(43))?;
        crdt.join(doc, &mut ctx, &op1)?;
        crdt.join(doc, &mut ctx, &op2)?;

        let mut values = BTreeSet::new();
        for value in crdt.values(path.as_path()) {
            if let Primitive::U64(value) = value?.to_owned()? {
                values.insert(value);
            } else {
                unreachable!();
            }
        }
        assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        assert!(values.contains(&43));

        let op = crdt.assign(path.as_path(), &ctx, dot1.inc(), Primitive::U64(99))?;
        crdt.join(doc, &mut ctx, &op)?;

        let mut values = BTreeSet::new();
        for value in crdt.values(path.as_path()) {
            if let Primitive::U64(value) = value?.to_owned()? {
                values.insert(value);
            } else {
                unreachable!();
            }
        }
        assert_eq!(values.len(), 1);
        assert!(values.contains(&99));

        Ok(())
    }

    #[test]
    fn test_ormap() -> Result<()> {
        let crdt = Crdt::memory("test")?;
        let doc = DocId::new([0; 32]);
        let mut dot1 = Dot::new(PeerId::new([0; 32]), 1);
        let mut ctx = CausalContext::default();
        let mut path = PathBuf::new(doc);
        path.key(&"a".into());
        path.key(&"b".into());
        let op = crdt.assign(path.as_path(), &ctx, dot1.inc(), Primitive::U64(42))?;
        crdt.join(doc, &mut ctx, &op)?;

        let mut values = BTreeSet::new();
        for value in crdt.values(path.as_path()) {
            if let Primitive::U64(value) = value?.to_owned()? {
                values.insert(value);
            } else {
                unreachable!();
            }
        }
        assert_eq!(values.len(), 1);
        assert!(values.contains(&42));

        let mut path2 = PathBuf::new(doc);
        path2.key(&"a".into());
        let op = crdt.remove(path2.as_path(), dot1.inc())?;
        crdt.join(doc, &mut ctx, &op)?;

        let mut values = BTreeSet::new();
        for value in crdt.values(path.as_path()) {
            if let Primitive::U64(value) = value?.to_owned()? {
                values.insert(value);
            } else {
                unreachable!();
            }
        }
        assert!(values.is_empty());

        Ok(())
    }

    /*proptest! {
        #[test]
        fn idempotent(a in arb_causal()) {
            prop_assert_eq!(join(&a, &a), a);
        }

        #[test]
        fn commutative(a in arb_causal(), b in arb_causal()) {
            prop_assert_eq!(join(&a, &b), join(&b, &a));
        }

        #[test]
        fn unjoin(a in arb_causal(), b in arb_ctx()) {
            let b = a.unjoin(&b);
            prop_assert_eq!(join(&a, &b), a);
        }

        #[test]
        fn associative(dots in arb_causal(), a in arb_ctx(), b in arb_ctx(), c in arb_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let c = dots.unjoin(&c);
            prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
        }
    }*/
}

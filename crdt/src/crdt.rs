use crate::{ArchivedLenses, DocId, Dot, DotSet, Hash, PeerId, Policy, Ref};
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub enum DotStore {
    Null,
    DotSet(DotSet),
    DotFun(BTreeMap<Dot, Primitive>),
    DotMap(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        BTreeMap<Primitive, DotStore>,
    ),
    Struct(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        BTreeMap<String, DotStore>,
    ),
    Policy(BTreeMap<Dot, BTreeSet<Policy>>),
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
            u if u == Policy as u8 => Some(Policy),
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
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Null => true,
            Self::DotSet(set) => set.is_empty(),
            Self::DotFun(fun) => fun.is_empty(),
            Self::DotMap(map) => map.is_empty(),
            Self::Struct(fields) => fields.is_empty(),
            Self::Policy(policy) => policy.is_empty(),
        }
    }

    pub fn dots(&self, ctx: &mut DotSet) {
        match self {
            Self::Null => {}
            Self::DotSet(set) => {
                for dot in set.iter() {
                    ctx.insert(dot);
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

    pub fn join(&mut self, ctx: &DotSet, other: &Self, other_ctx: &DotSet) {
        match (self, other) {
            (me @ Self::Null, other) => *me = other.clone(),
            (_, Self::Null) => {}
            (Self::DotSet(set), Self::DotSet(other)) => {
                // from the paper
                // (s, c) ∐ (s', c') = ((s ∩ s') ∪ (s \ c') (s' \ c), c ∪ c')
                // (s \ c')
                let a = set.difference(other_ctx);
                // (s' \ c)
                let b = other.difference(ctx);
                // ((s ∩ s')
                *set = set.intersection(other);
                // (s ∩ s') ∪ (s \ c') (s' \ c)
                set.union(&a);
                set.union(&b);
            }
            (Self::DotFun(fun), Self::DotFun(other)) => {
                // from the paper
                // (m, c) ∐ (m', c') = ({ k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' } ∪
                //                      {(d, v) ∊ m | d ∉ c'} ∪ {(d, v) ∊ m' | d ∉ c}, c ∪ c')
                fun.retain(|dot, _v| {
                    if let Some(_v2) = other.get(dot) {
                        // join all elements that are in both funs
                        // { k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' }
                        // this can only occur if a dot was reused
                        // v.join(v2);
                        true
                    } else {
                        // keep all elements unmodified that are not in the other causal context
                        // { (d, v) ∊ m | d ∉ c' }
                        !other_ctx.contains(dot)
                    }
                });
                // copy all elements from the other fun, that are neither in our fun nor in our
                // causal context
                // { (d, v) ∊ m' | d ∉ c }
                for (d, v) in other {
                    if !fun.contains_key(d) && !ctx.contains(d) {
                        fun.insert(*d, v.clone());
                    }
                }
            }
            (Self::DotMap(map), Self::DotMap(other)) => {
                // from the paper
                // (m, c) ∐ (m', c') = ({ k -> v(k), k ∈ dom m ∪ dom m' ∧ v(k) ≠ ⊥ }, c ∪ c')
                //                     where v(k) = fst ((m(k), c) ∐ (m'(k), c'))
                let mut all = map.keys().cloned().collect::<Vec<_>>();
                all.extend(other.keys().cloned());
                for key in all {
                    let v1 = map.entry(key.clone()).or_insert(DotStore::Null);
                    let v2 = other.get(&key).unwrap_or(&DotStore::Null);
                    v1.join(ctx, v2, other_ctx);
                    if v1.is_empty() {
                        map.remove(&key);
                    }
                }
            }
            (Self::Struct(fields), Self::Struct(other)) => {
                for (field, value2) in other {
                    if let Some(value) = fields.get_mut(field) {
                        value.join(ctx, value2, other_ctx);
                    } else {
                        fields.insert(field.clone(), value2.clone());
                    }
                }
            }
            (Self::Policy(policy), Self::Policy(other)) => {
                policy.extend(other.iter().map(|(k, v)| (*k, v.clone())));
            }
            (x, y) => panic!("invalid data\n l: {:?}\n r: {:?}", x, y),
        }
    }

    pub fn unjoin(&self, diff: &DotSet) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::DotSet(set) => Self::DotSet(set.intersection(diff)),
            Self::DotFun(fun) => {
                let mut delta = BTreeMap::new();
                for (dot, v) in fun {
                    if diff.contains(dot) {
                        delta.insert(*dot, v.clone());
                    }
                }
                Self::DotFun(delta)
            }
            Self::DotMap(map) => {
                let mut delta = BTreeMap::new();
                for (k, v) in map {
                    let v = v.unjoin(diff);
                    if !v.is_empty() {
                        delta.insert(k.clone(), v);
                    }
                }
                Self::DotMap(delta)
            }
            Self::Struct(fields) => {
                let mut delta = BTreeMap::new();
                for (k, v) in fields {
                    let v = v.unjoin(diff);
                    if !v.is_empty() {
                        delta.insert(k.clone(), v);
                    }
                }
                Self::Struct(delta)
            }
            Self::Policy(policy) => {
                let delta = policy
                    .iter()
                    .filter(|(dot, _)| diff.contains(dot))
                    .map(|(k, v)| (*k, v.clone()))
                    .collect();
                Self::Policy(delta)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct CausalContext {
    pub(crate) doc: DocId,
    pub(crate) schema: [u8; 32],
    pub(crate) dots: DotSet,
}

impl CausalContext {
    pub fn new(doc: DocId, schema: Hash) -> Self {
        Self {
            doc,
            schema: schema.into(),
            dots: Default::default(),
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
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct Causal {
    pub(crate) ctx: CausalContext,
    pub(crate) store: DotStore,
}

impl Causal {
    pub fn ctx(&self) -> &CausalContext {
        &self.ctx
    }

    pub fn store(&self) -> &DotStore {
        &self.store
    }

    pub fn join(&mut self, other: &Archived<Causal>) {
        assert_eq!(self.ctx().doc(), &other.ctx.doc);
        assert_eq!(&self.ctx().schema, &other.ctx.schema);
        // TODO
        let other: Causal = other.deserialize(&mut rkyv::Infallible).unwrap();
        self.store
            .join(&self.ctx.dots, &other.store, &other.ctx.dots);
        self.ctx.dots.union(&other.ctx.dots);
    }

    pub fn unjoin(&self, ctx: &Archived<CausalContext>) -> Self {
        // TODO
        let other = ctx.dots.deserialize(&mut rkyv::Infallible).unwrap();
        let dots = self.ctx.dots.difference(&other);
        let store = self.store.unjoin(&dots);
        Self {
            ctx: CausalContext {
                doc: self.ctx.doc,
                schema: self.ctx.schema,
                dots,
            },
            store,
        }
    }

    pub fn transform(&mut self, from: &ArchivedLenses, to: &ArchivedLenses) {
        from.transform_dotstore(&mut self.store, to);
    }
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
        self.extend(DotStoreType::Map, Ref::archive(key).as_bytes());
    }

    pub fn field(&mut self, field: &str) {
        self.extend(DotStoreType::Struct, field.as_bytes());
    }

    pub fn dotset(&mut self, dot: &Dot) {
        self.extend(DotStoreType::Set, Ref::archive(dot).as_bytes());
    }

    pub fn dotfun(&mut self, dot: &Dot) {
        self.extend(DotStoreType::Fun, Ref::archive(dot).as_bytes());
    }

    pub fn policy(&mut self, dot: &Dot) {
        self.extend(DotStoreType::Policy, Ref::archive(dot).as_bytes());
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
        ctx: &DotSet,
        other: &DotSet,
        other_ctx: &DotSet,
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
        for dot in other.iter() {
            if !ctx.contains(&dot) {
                path.dotset(&dot);
                self.0.insert(&path, &[])?;
                path.pop();
            }
        }
        Ok(())
    }

    fn join_dotfun(
        &self,
        path: &mut PathBuf,
        ctx: &DotSet,
        other: &BTreeMap<Dot, Primitive>,
        other_ctx: &DotSet,
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
            self.0.insert(&path, Ref::archive(v).as_bytes())?;
            path.pop();
        }
        Ok(())
    }

    fn join_dotmap(
        &self,
        path: &mut PathBuf,
        ctx: &DotSet,
        other: &BTreeMap<Primitive, DotStore>,
        other_ctx: &DotSet,
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
        ctx: &DotSet,
        other: &BTreeMap<String, DotStore>,
        other_ctx: &DotSet,
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
        _: &DotSet,
        other: &BTreeMap<Dot, BTreeSet<Policy>>,
        _: &DotSet,
    ) -> Result<()> {
        for (dot, ps) in other {
            path.policy(dot);
            self.0.transaction::<_, _, std::io::Error>(|tree| {
                let mut policies = if let Some(bytes) = tree.get(path.as_ref())? {
                    Ref::<BTreeSet<Policy>>::new(bytes).to_owned().unwrap()
                } else {
                    Default::default()
                };
                for policy in ps {
                    policies.insert(policy.clone());
                }
                tree.insert(path.as_ref(), Ref::archive(&policies).as_bytes())?;
                Ok(())
            })?;
            path.pop();
        }
        Ok(())
    }

    fn join_store(
        &self,
        path: &mut PathBuf,
        ctx: &DotSet,
        other: &DotStore,
        other_ctx: &DotSet,
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

    pub fn join(&self, ctx: &mut CausalContext, _peer_id: &PeerId, causal: &Causal) -> Result<()> {
        assert_eq!(ctx.doc(), causal.ctx().doc());
        // TODO: check write permission
        let mut path = PathBuf::new(ctx.doc);
        self.join_store(&mut path, &mut ctx.dots, &causal.store, &causal.ctx.dots)?;
        ctx.dots.union(&causal.ctx.dots);
        Ok(())
    }

    pub fn unjoin(
        &self,
        ctx: &Archived<CausalContext>,
        _peer_id: &PeerId,
        other: &Archived<CausalContext>,
    ) -> Result<Causal> {
        // TODO: check read permission
        assert_eq!(ctx.doc, other.doc);
        // TODO: no deserialize
        let dots: DotSet = ctx.dots.deserialize(&mut rkyv::Infallible)?;
        // TODO: no deserialize
        let other: DotSet = other.dots.deserialize(&mut rkyv::Infallible)?;
        let diff = dots.difference(&other);
        let mut store = DotStore::Null;
        for r in self.0.scan_prefix(PathBuf::new(ctx.doc)) {
            let (k, v) = r?;
            let path = Path::new(&k[..]);
            let dot = path.dot();
            if !diff.contains(&dot) {
                continue;
            }
            let delta = match path.ty() {
                Some(DotStoreType::Set) => {
                    let mut dotset = DotSet::new();
                    dotset.insert(dot);
                    DotStore::DotSet(dotset)
                }
                Some(DotStoreType::Fun) => {
                    let mut dotfun = BTreeMap::new();
                    dotfun.insert(dot, Ref::<Primitive>::new(v).to_owned()?);
                    DotStore::DotFun(dotfun)
                }
                Some(DotStoreType::Policy) => {
                    let mut policy = BTreeMap::new();
                    policy.insert(dot, Ref::<BTreeSet<Policy>>::new(v).to_owned()?);
                    DotStore::Policy(policy)
                }
                _ => continue,
            };
            store.join(&DotSet::new(), &delta, &DotSet::new());
        }
        Ok(Causal {
            ctx: CausalContext {
                doc: ctx.doc,
                schema: ctx.schema,
                dots: diff,
            },
            store: DotStore::Null,
        })
    }

    pub fn enable(
        &self,
        path: Path<'_>,
        ctx: &Archived<CausalContext>,
        dot: Dot,
    ) -> Result<Causal> {
        assert_eq!(path.root().unwrap(), ctx.doc);
        let mut store = DotSet::new();
        store.insert(dot);
        let mut causal = Causal {
            store: DotStore::DotSet(store),
            ctx: ctx.deserialize(&mut rkyv::Infallible)?,
        };
        causal.ctx.dots.insert(dot);
        path.wrap(causal)
    }

    pub fn disable(
        &self,
        path: Path<'_>,
        ctx: &Archived<CausalContext>,
        dot: Dot,
    ) -> Result<Causal> {
        assert_eq!(path.root().unwrap(), ctx.doc);
        let mut causal = Causal {
            store: DotStore::DotSet(Default::default()),
            ctx: ctx.deserialize(&mut rkyv::Infallible)?,
        };
        causal.ctx.dots.insert(dot);
        path.wrap(causal)
    }

    pub fn is_enabled(&self, path: Path<'_>) -> bool {
        self.0.scan_prefix(path).next().is_some()
    }

    pub fn assign(
        &self,
        path: Path<'_>,
        ctx: &Archived<CausalContext>,
        dot: Dot,
        v: Primitive,
    ) -> Result<Causal> {
        assert_eq!(path.root().unwrap(), ctx.doc);
        let mut store = BTreeMap::new();
        store.insert(dot, v);
        let mut causal = Causal {
            store: DotStore::DotFun(store),
            ctx: ctx.deserialize(&mut rkyv::Infallible)?,
        };
        causal.ctx.dots.insert(dot);
        path.wrap(causal)
    }

    pub fn values(&self, path: Path<'_>) -> impl Iterator<Item = sled::Result<Ref<Primitive>>> {
        self.0
            .scan_prefix(path)
            .values()
            .map(|res| res.map(Ref::new))
    }

    pub fn remove(
        &self,
        path: Path<'_>,
        ctx: &Archived<CausalContext>,
        dot: Dot,
    ) -> Result<Causal> {
        assert_eq!(path.root().unwrap(), ctx.doc);
        let mut ctx = CausalContext {
            doc: ctx.doc,
            schema: ctx.schema,
            dots: Default::default(),
        };
        ctx.dots.insert(dot);
        for res in self.0.scan_prefix(path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            let ty = key.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            ctx.dots.insert(dot);
        }
        let causal = Causal {
            store: DotStore::DotMap(Default::default()),
            ctx,
        };
        path.wrap(causal)
    }

    pub fn say(
        &self,
        path: Path<'_>,
        ctx: &Archived<CausalContext>,
        dot: Dot,
        policy: Policy,
    ) -> Result<Causal> {
        assert_eq!(path.root().unwrap(), ctx.doc);
        let mut ctx = CausalContext {
            doc: ctx.doc,
            schema: ctx.schema,
            dots: Default::default(),
        };
        ctx.dots.insert(dot);
        let mut set = BTreeSet::new();
        set.insert(policy);
        let mut store = BTreeMap::new();
        store.insert(dot, set);
        let causal = Causal {
            store: DotStore::Policy(store),
            ctx,
        };
        path.wrap(causal)
    }

    pub fn transform(&self, from: &ArchivedLenses, to: &ArchivedLenses) {
        from.transform_crdt(self, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::props::*;
    use proptest::prelude::*;

    /*#[test]
    fn test_ewflag() -> Result<()> {
        let crdt = Crdt::memory("test")?;
        let doc = DocId::new([0; 32]);
        let peer_id = PeerId::new([0; 32]);
        let mut dot = Dot::new(PeerId::new([0; 32]), 1);
        let mut ctx = CausalContext {
            doc,
            schema: [0; 32],
            dots: Default::default(),
        };
        let mut path = PathBuf::new(doc);
        path.field("a");
        path.field("b");
        let op = crdt.enable(path.as_path(), &ctx, dot.inc())?;
        assert!(!crdt.is_enabled(path.as_path()));
        crdt.join(&mut ctx, &peer_id, &op)?;
        assert!(crdt.is_enabled(path.as_path()));
        let op = crdt.disable(path.as_path(), &ctx, dot.inc())?;
        crdt.join(&mut ctx, &peer_id, &op)?;
        assert!(!crdt.is_enabled(path.as_path()));
        Ok(())
    }

    #[test]
    fn test_mvreg() -> Result<()> {
        let crdt = Crdt::memory("test")?;
        let doc = DocId::new([0; 32]);
        let peer_id = PeerId::new([0; 32]);
        let mut dot1 = Dot::new(PeerId::new([0; 32]), 1);
        let mut dot2 = Dot::new(PeerId::new([1; 32]), 1);
        let mut ctx = CausalContext {
            doc,
            schema: [0; 32],
            dots: Default::default(),
        };
        let mut path = PathBuf::new(doc);
        path.field("a");
        path.field("b");
        let op1 = crdt.assign(path.as_path(), &ctx, dot1.inc(), Primitive::U64(42))?;
        let op2 = crdt.assign(path.as_path(), &ctx, dot2.inc(), Primitive::U64(43))?;
        crdt.join(&mut ctx, &peer_id, &op1)?;
        crdt.join(&mut ctx, &peer_id, &op2)?;

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
        crdt.join(&mut ctx, &op)?;

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
        let peer_id = PeerId::new([0; 32]);
        let mut dot1 = Dot::new(PeerId::new([0; 32]), 1);
        let mut ctx = CausalContext {
            doc,
            schema: [0; 32],
            dots: Default::default(),
        };
        let mut path = PathBuf::new(doc);
        path.key(&"a".into());
        path.key(&"b".into());
        let op = crdt.assign(path.as_path(), &ctx, dot1.inc(), Primitive::U64(42))?;
        crdt.join(&mut ctx, &peer_id, &op)?;

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
        let op = crdt.remove(path2.as_path(), &ctx, dot1.inc())?;
        crdt.join(&mut ctx, &peer_id, &op)?;

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
            let a = Ref::archive(&a);
            let a = dots.unjoin(a.as_ref());
            let b = Ref::archive(&b);
            let b = dots.unjoin(b.as_ref());
            prop_assert_eq!(join(&a, &b), join(&b, &a));
        }

        #[test]
        fn causal_join_associative(dots in arb_causal(arb_dotstore()), a in arb_causal_ctx(), b in arb_causal_ctx(), c in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let c = dots.unjoin(&c);
            prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
        }

        #[test]
        #[ignore]
        // TODO: crdt can infer defaults from path, causal just sets it to null
        fn crdt_join(dots in arb_causal(arb_dotstore()), a in arb_causal_ctx(), b in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let (mut ctx, crdt) = causal_to_crdt(&a);
            let c = join(&a, &b);
            crdt.join(&mut ctx, &b).unwrap();
            let c2 = crdt_to_causal(&crdt, &ctx);
            assert_eq!(c, c2);
        }

        #[test]
        #[ignore]
        fn crdt_unjoin(causal in arb_causal(arb_dotstore()), ctx in arb_causal_ctx()) {
            let peer_id = PeerId::new([0; 32]);
            let (crdt_ctx, crdt) = causal_to_crdt(&causal);
            let crdt_ctx = Ref::archive(&crdt_ctx);
            let ctx = Ref::archive(&ctx);
            let c = causal.unjoin(ctx.as_ref());
            let c2 = crdt.unjoin(crdt_ctx.as_ref(), &peer_id, ctx.as_ref()).unwrap();
            assert_eq!(c, c2);
        }
    }*/
}

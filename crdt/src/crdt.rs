use crate::{
    AbstractDotSet, Acl, ArchivedLenses, DocId, Docs, Dot, DotSet, Engine, Hash, PeerId,
    Permission, Policy, Ref, Writer,
};
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::{
    archived_root, ser::serializers::AllocSerializer, Archive, Archived, Deserialize, Serialize,
};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
};

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
#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize, Default)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub struct FlatDotStore(BTreeMap<PathBuf, Vec<u8>>);

fn archive<T>(value: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    Ref::archive(value).as_bytes().to_owned()
}

fn unarchive<T>(value: &[u8]) -> anyhow::Result<T>
where
    T: Archive,
    Archived<T>: Deserialize<T, rkyv::Infallible>,
{
    let archived = unsafe { archived_root::<T>(&value) };
    Ok(archived.deserialize(&mut rkyv::Infallible)?)
}

pub trait InPlaceRelationalOps<K, V> {
    fn outer_join_with<W, L, R>(&mut self, that: &BTreeMap<K, W>, l: L, r: R)
    where
        K: Ord + Clone,
        L: Fn(&K, &mut V, Option<&W>) -> bool,
        R: Fn(&K, &W) -> Option<V>;
}

impl<K, V> InPlaceRelationalOps<K, V> for BTreeMap<K, V> {
    fn outer_join_with<W, L, R>(&mut self, that: &BTreeMap<K, W>, l: L, r: R)
    where
        K: Ord + Clone,
        L: Fn(&K, &mut V, Option<&W>) -> bool,
        R: Fn(&K, &W) -> Option<V>,
    {
        // k in that
        for (k, w) in that.iter() {
            match self.get_mut(k) {
                Some(v) => {
                    if !l(k, v, Some(w)) {
                        self.remove(k);
                    }
                }
                None => {
                    if let Some(v) = r(k, w) {
                        self.insert(k.clone(), v);
                    }
                }
            }
        }
        // k not in that
        self.retain(|k, v| that.get(k).is_some() || l(k, v, None));
    }
}

fn join_policy(a: &mut Vec<u8>, r: &[u8]) {
    let mut l: BTreeSet<Policy> = unarchive(a.as_ref()).unwrap();
    let r: BTreeSet<Policy> = unarchive(r).unwrap();
    l.extend(r.into_iter());
    *a = archive(&l);
}

impl FlatDotStore {
    pub fn policy(path: Path, args: BTreeMap<Dot, Policy>) -> Self {
        Self(
            args.iter()
                .map(|(dot, policy)| {
                    let mut path = path.to_owned();
                    path.policy(dot);
                    (path, archive(policy))
                })
                .collect(),
        )
    }

    pub fn dotfun(path: Path, args: BTreeMap<Dot, Primitive>) -> Self {
        Self(
            args.iter()
                .map(|(dot, primitive)| {
                    let mut path = path.to_owned();
                    path.dotfun(&dot);
                    (path, archive(primitive))
                })
                .collect(),
        )
    }

    pub fn dotset(path: Path, args: &DotSet) -> Self {
        Self(
            args.iter()
                .map(|dot| {
                    let mut path = path.to_owned();
                    path.dotset(&dot);
                    (path, Vec::new())
                })
                .collect(),
        )
    }

    pub fn dotmap(path: Path, args: BTreeMap<Primitive, Self>) -> Self {
        let entries = args.into_iter().flat_map(move |(key, store)| {
            store.0.into_iter().map(move |(k, v)| (key.clone(), k, v))
        });
        Self(
            entries
                .map(|(key, k, v)| {
                    let mut path = path.to_owned();
                    path.key(&key);
                    path.0.extend(k.0.into_iter());
                    (path, v)
                })
                .collect(),
        )
    }

    pub fn strct(path: Path, args: BTreeMap<String, Self>) -> Self {
        let entries = args.into_iter().flat_map(move |(field, store)| {
            store.0.into_iter().map(move |(k, v)| (field.clone(), k, v))
        });
        Self(
            entries
                .map(|(field, k, v)| {
                    let mut path = path.to_owned();
                    path.field(&field);
                    path.0.extend(k.0.into_iter());
                    (path, v)
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

    pub fn join(
        &mut self,
        ctx: &impl AbstractDotSet<PeerId>,
        that: &Self,
        that_ctx: &impl AbstractDotSet<PeerId>,
    ) {
        self.0.outer_join_with(
            &that.0,
            |k, v, w| {
                let ty = k.as_path().ty().unwrap();
                let dot = k.as_path().dot();
                if let Some(w) = w {
                    match ty {
                        DotStoreType::Policy => {
                            // this is a grow only set, so we just merge them without looking at the contexts at all
                            join_policy(v, w);
                            true
                        }
                        DotStoreType::Set => {
                            if !v.is_empty() {
                                println!("{:?}", v);
                            }
                            assert!(v.is_empty());
                            assert!(w.is_empty());
                            // if we get here, the dot exists on both sides
                            // (s ∩ s')
                            true
                        }
                        DotStoreType::Fun => {
                            // { k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' }
                            // different value for the same dot would be a bug
                            assert_eq!(v, w);
                            true
                        }
                        _ => {
                            panic!()
                        }
                    }
                } else {
                    match ty {
                        DotStoreType::Policy => {
                            // keep the policy unchanged
                            true
                        }
                        DotStoreType::Set => {
                            // only keep the dot from v if it is not in the other context
                            // (s \ c')
                            !that_ctx.contains(&dot)
                        }
                        DotStoreType::Fun => {
                            // keep all elements unmodified that are not in the other causal context
                            // { (d, v) ∊ m | d ∉ c' }
                            !that_ctx.contains(&dot)
                        }
                        _ => {
                            panic!()
                        }
                    }
                }
            },
            |k, w| {
                let ty = k.as_path().ty().unwrap();
                let dot = k.as_path().dot();
                match ty {
                    DotStoreType::Policy => {
                        // take the policy from the right
                        Some(w.clone())
                    }
                    DotStoreType::Set => {
                        // only keep the dot from w if it is not in our context
                        // (s' \ c)
                        if !ctx.contains(&dot) {
                            Some(w.clone())
                        } else {
                            None
                        }
                    }
                    DotStoreType::Fun => {
                        // copy all elements from the other fun, that are neither in our fun nor in our
                        // causal context
                        // { (d, v) ∊ m' | d ∉ c }
                        if !ctx.contains(&dot) {
                            Some(w.clone())
                        } else {
                            None
                        }
                    }
                    _ => {
                        panic!()
                    }
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

    pub fn from_dot_store(value: &DotStore, prefix: PathBuf) -> Self {
        Self(iter(value, prefix).collect())
    }

    pub fn to_dot_store(&self) -> anyhow::Result<DotStore> {
        let atoms: Vec<DotStore> = self
            .0
            .iter()
            .map(|(path, value)| pair_to_dot_store(path.as_path(), value))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(atoms.into_iter().fold(DotStore::Null, |mut agg, elem| {
            agg.merge(&elem);
            agg
        }))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

fn pair_to_dot_store(mut path: Path<'_>, value: &[u8]) -> anyhow::Result<DotStore> {
    let mut store = match path.ty() {
        Some(DotStoreType::Policy) => {
            let key = path.dot();
            let value = unarchive(value)?;
            let mut res = BTreeMap::new();
            res.insert(key, value);
            DotStore::Policy(res)
        }
        Some(DotStoreType::Fun) => {
            let key = path.dot();
            let value = unarchive(value)?;
            let mut res = BTreeMap::new();
            res.insert(key, value);
            DotStore::DotFun(res)
        }
        Some(DotStoreType::Set) => {
            let key = path.dot();
            let mut res = DotSet::new();
            res.insert(key);
            DotStore::DotSet(res)
        }
        _ => DotStore::Null,
    };
    while let Some(parent) = path.parent() {
        match parent.ty() {
            Some(DotStoreType::Map) => {
                let key = parent.key().to_owned()?;
                let mut res = BTreeMap::new();
                res.insert(key, store);
                store = DotStore::DotMap(res);
            }
            Some(DotStoreType::Struct) => {
                let key = parent.field();
                let mut res = BTreeMap::new();
                res.insert(key.to_owned(), store);
                store = DotStore::Struct(res);
            }
            Some(DotStoreType::Root) => {
                let doc = parent.doc();
            }
            None => {}
            x => {
                panic!("unexpected parent type {:?} {:?}", x, parent)
            }
        }
        path = parent;
    }
    Ok(store)
}

fn iter<'a>(
    value: &'a DotStore,
    prefix: PathBuf,
) -> Box<dyn Iterator<Item = (PathBuf, Vec<u8>)> + 'a> {
    match value {
        DotStore::DotSet(s) => Box::new(s.iter().map(move |dot| {
            let mut path = prefix.clone();
            path.dotset(&dot);
            (path, Vec::new())
        })),
        DotStore::DotFun(s) => Box::new(s.iter().map(move |(dot, value)| {
            let mut path = prefix.clone();
            path.dotfun(&dot);
            (path, archive(value))
        })),
        DotStore::DotMap(s) => Box::new(s.iter().flat_map(move |(k, v)| {
            let mut path = prefix.clone();
            path.key(&k);
            iter(v, path)
        })),
        DotStore::Struct(s) => Box::new(s.iter().flat_map(move |(k, v)| {
            let mut path = prefix.clone();
            path.field(&k);
            iter(v, path)
        })),
        DotStore::Policy(s) => Box::new(s.iter().flat_map(move |(dot, policies)| {
            let mut path = prefix.clone();
            path.policy(dot);
            std::iter::once((path, archive(policies)))
        })),
        DotStore::Null => Box::new(std::iter::empty()),
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

    pub fn join(
        &mut self,
        ctx: &impl AbstractDotSet<PeerId>,
        other: &Self,
        other_ctx: &impl AbstractDotSet<PeerId>,
    ) {
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
                // (s ∩ s') ∪ (s \ c') ∪ (s' \ c)
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

    pub fn merge(&mut self, that: &Self) {
        match (self, that) {
            (me @ Self::Null, that) => *me = that.clone(),
            (_, Self::Null) => {}
            (Self::DotSet(this), Self::DotSet(that)) => this.union(that),
            (Self::Policy(this), Self::Policy(that)) => {
                for (k, w) in that {
                    this.insert(k.clone(), w.clone());
                }
            }
            (Self::DotFun(this), Self::DotFun(that)) => {
                for (k, w) in that {
                    this.insert(k.clone(), w.clone());
                }
            }
            (Self::DotMap(this), Self::DotMap(that)) => {
                for (k, w) in that {
                    let v = this.entry(k.clone()).or_insert(DotStore::Null);
                    v.merge(w)
                }
            }
            (Self::Struct(this), Self::Struct(that)) => {
                for (k, w) in that {
                    let v = this.entry(k.clone()).or_insert(DotStore::Null);
                    v.merge(w)
                }
            }
            (x, y) => panic!("invalid data\n l: {:?}\n r: {:?}", x, y),
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
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct Causal {
    pub(crate) ctx: CausalContext,
    pub(crate) store: FlatDotStore,
}

impl Causal {
    pub fn ctx(&self) -> &CausalContext {
        &self.ctx
    }

    pub fn store(&self) -> &FlatDotStore {
        &self.store
    }

    pub fn join(&mut self, other: &Causal) {
        assert_eq!(self.ctx().doc(), &other.ctx.doc);
        assert_eq!(&self.ctx().schema, &other.ctx.schema);

        self.store
            .join(&self.ctx.dots, &other.store, &other.ctx.dots);
        self.ctx.dots.union(&other.ctx.dots);
    }

    pub fn unjoin(&self, ctx: &CausalContext) -> Self {
        let dots = self.ctx.dots.difference(&ctx.dots);
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

impl Borrow<[u8]> for PathBuf {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn empty() -> Self {
        Self(&[])
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
pub struct Crdt {
    state: sled::Tree,
    acl: Acl,
    docs: Docs,
}

impl Crdt {
    pub fn new(state: sled::Tree, acl: Acl, docs: Docs) -> Self {
        Self { state, acl, docs }
    }

    pub fn memory() -> Result<(Self, Engine)> {
        let db = sled::Config::new().temporary(true).open()?;
        let state = db.open_tree("state")?;
        let acl = Acl::new(db.open_tree("acl")?);
        let docs = Docs::new(db.open_tree("docs")?);
        let me = Self::new(state, acl.clone(), docs);
        let engine = Engine::new(me.clone(), acl)?;
        Ok((me, engine))
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

    fn join_dotset(
        &self,
        path: &mut PathBuf,
        peer: &PeerId,
        other: &DotSet,
        other_ctx: &DotSet,
    ) -> Result<()> {
        if !self.can(peer, Permission::Write, path.as_path())? {
            tracing::info!("join_dotset denied");
            return Ok(());
        }
        for res in self.dotset(path.as_path()) {
            let dot = res?;
            if !other.contains(&dot) && other_ctx.contains(&dot) {
                path.dotset(&dot);
                self.state.remove(&path)?;
                path.pop();
            }
        }
        for dot in other.iter() {
            if !self.docs.contains(&path.as_path().root().unwrap(), &dot)? {
                path.dotset(&dot);
                self.state.insert(&path, &[])?;
                path.pop();
            }
        }
        Ok(())
    }

    fn join_dotfun(
        &self,
        path: &mut PathBuf,
        peer: &PeerId,
        other: &BTreeMap<Dot, Primitive>,
        other_ctx: &DotSet,
    ) -> Result<()> {
        if !self.can(peer, Permission::Write, path.as_path())? {
            tracing::info!("join_dotfun denied");
            return Ok(());
        }
        for res in self.state.scan_prefix(&path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            if key.ty() != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            if !other.contains_key(&dot) && other_ctx.contains(&dot) {
                self.state.remove(key)?;
            }
        }
        for (dot, v) in other {
            if self.docs.contains(&path.as_path().root().unwrap(), dot)? {
                continue;
            }
            path.dotfun(dot);
            if self.state.contains_key(&path)? {
                continue;
            }
            self.state.insert(&path, Ref::archive(v).as_bytes())?;
            path.pop();
        }
        Ok(())
    }

    fn join_dotmap(
        &self,
        path: &mut PathBuf,
        peer_id: &PeerId,
        other: &BTreeMap<Primitive, DotStore>,
        other_ctx: &DotSet,
    ) -> Result<()> {
        for res in self.state.scan_prefix(&path).keys() {
            let leaf = res?;
            let key = Path::new(&leaf[path.as_ref().len()..]);
            if key.first().ty() != Some(DotStoreType::Map) {
                continue;
            }
            let key = key.first().key().to_owned()?;
            path.key(&key);
            let default = Path::new(&leaf[path.as_ref().len()..])
                .first()
                .ty()
                .unwrap()
                .default()
                .unwrap();
            let store = other.get(&key).unwrap_or(&default);
            self.join_store(path, peer_id, store, other_ctx)?;
            path.pop();
        }
        for (key, store) in other {
            path.key(key);
            self.join_store(path, peer_id, store, other_ctx)?;
            path.pop();
        }
        Ok(())
    }

    fn join_struct(
        &self,
        path: &mut PathBuf,
        peer_id: &PeerId,
        other: &BTreeMap<String, DotStore>,
        other_ctx: &DotSet,
    ) -> Result<()> {
        use DotStore::*;
        for (k, v) in other {
            path.field(k);
            match v {
                Null => {}
                DotSet(set) => self.join_dotset(path, peer_id, set, other_ctx)?,
                DotFun(fun) => self.join_dotfun(path, peer_id, fun, other_ctx)?,
                DotMap(map) => self.join_dotmap(path, peer_id, map, other_ctx)?,
                Struct(fields) => self.join_struct(path, peer_id, fields, other_ctx)?,
                Policy(policy) => self.join_policy(path, peer_id, policy, other_ctx)?,
            }
            path.pop();
        }
        Ok(())
    }

    fn join_policy(
        &self,
        path: &mut PathBuf,
        peer: &PeerId,
        other: &BTreeMap<Dot, Policy>,
        _: &DotSet,
    ) -> Result<()> {
        if !self.can(peer, Permission::Control, path.as_path())? {
            tracing::info!("join_policy denied");
            return Ok(());
        }
        for (dot, policy) in other {
            path.policy(dot);
            self.state.transaction::<_, _, std::io::Error>(|tree| {
                if let Some(bytes) = tree.get(path.as_ref())? {
                    let current = Ref::<Policy>::new(bytes).to_owned().unwrap();
                    assert!(policy == &current);
                } else {
                    tree.insert(path.as_ref(), Ref::archive(policy).as_bytes())?;
                };
                Ok(())
            })?;
            path.pop();
        }
        Ok(())
    }

    fn join_store(
        &self,
        path: &mut PathBuf,
        peer_id: &PeerId,
        other: &DotStore,
        other_ctx: &DotSet,
    ) -> Result<()> {
        use DotStore::*;
        match other {
            Null => {
                for key in self.state.scan_prefix(path).keys() {
                    let key = key?;
                    let key = Path::new(&key);
                    let dot = key.dot();
                    if other_ctx.contains(&dot) {
                        self.state.remove(key)?;
                    }
                }
            }
            DotSet(set) => self.join_dotset(path, peer_id, set, other_ctx)?,
            DotFun(fun) => self.join_dotfun(path, peer_id, fun, other_ctx)?,
            DotMap(map) => self.join_dotmap(path, peer_id, map, other_ctx)?,
            Struct(fields) => self.join_struct(path, peer_id, fields, other_ctx)?,
            Policy(policy) => self.join_policy(path, peer_id, policy, other_ctx)?,
        }
        Ok(())
    }

    fn join_flat_store(
        &self,
        doc: DocId,
        peer_id: PeerId,
        that: &FlatDotStore,
        that_ctx: &DotSet,
    ) -> Result<()> {
        // todo: permissions!
        let path = PathBuf::new(doc);
        let mut common = BTreeSet::new();
        for item in self.state.scan_prefix(path) {
            let (k, v) = item?;
            let k = Path::new(&k);
            let dot = k.dot();
            match that.get(&k) {
                Some(w) => {
                    common.insert(k.to_owned());
                    // different value for the same dot would be a bug
                    assert_eq!(v, w);
                }
                None => {
                    // The type does not even matter.
                    // If it is in that_ctx but not in that, it needs to go
                    if that_ctx.contains(&dot) {
                        self.state.remove(&k)?;
                    }
                }
            }
        }
        for (k, w) in &that.0 {
            if !common.contains(k) {
                self.state.insert(&k, w.clone())?;
            }
        }
        Ok(())
    }

    pub fn join(&self, peer_id: &PeerId, causal: &Causal) -> Result<()> {
        let mut path = PathBuf::new(causal.ctx.doc);
        let causal_store = causal.store.to_dot_store().unwrap();
        self.join_store(&mut path, peer_id, &causal_store, &causal.ctx.dots)?;
        for peer_id in causal.ctx.dots.peers() {
            self.docs
                .extend_present(&causal.ctx.doc, peer_id, causal.ctx.dots.max(peer_id))?;
        }
        Ok(())
    }

    pub fn unjoin(&self, peer_id: &PeerId, other: &Archived<CausalContext>) -> Result<Causal> {
        let prefix = PathBuf::new(other.doc);
        let ctx = self.ctx(other.doc)?;
        let diff = ctx.dots.difference(&other.dots);
        let mut store = DotStore::Null;
        for r in self.state.scan_prefix(prefix) {
            let (k, v) = r?;
            let path = Path::new(&k[..]);
            let dot = path.dot();
            if !diff.contains(&dot) {
                continue;
            }
            if !self.can(peer_id, Permission::Read, path)? {
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
                    policy.insert(dot, Ref::<Policy>::new(v).to_owned()?);
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
            store: FlatDotStore::default(),
        })
    }

    fn empty_ctx(&self, doc: DocId) -> Result<CausalContext> {
        let schema = self.docs.schema_id(&doc)?;
        Ok(CausalContext {
            doc,
            schema: schema.into(),
            dots: Default::default(),
        })
    }

    pub fn ctx(&self, doc: DocId) -> Result<CausalContext> {
        let mut ctx = self.empty_ctx(doc)?;
        ctx.dots = DotSet::from_map(self.docs.present(&doc).collect::<Result<_>>()?);
        Ok(ctx)
    }

    pub fn enable(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut ctx = self.empty_ctx(path.root().unwrap())?;
        let dot = writer.dot();
        ctx.dots.insert(dot);
        Ok(Causal {
            store: FlatDotStore::dotset(path, &ctx.dots),
            ctx,
        })
    }

    pub fn disable(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut ctx = self.empty_ctx(path.root().unwrap())?;
        // add all dots to be tombstoned into the context
        for i in self.state.scan_prefix(&path).keys() {
            let i = i?;
            let path = Path::new(&i);
            let dot = path.dot();
            ctx.dots.insert(dot);
        }
        Ok(Causal {
            store: FlatDotStore::dotset(path, &Default::default()),
            ctx,
        })
    }

    pub fn is_enabled(&self, path: Path<'_>) -> bool {
        self.state.scan_prefix(path).next().is_some()
    }

    pub fn assign(&self, path: Path, writer: &Writer, v: Primitive) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut ctx = self.ctx(path.root().unwrap())?;
        // add all dots to be tombstoned into the context
        for i in self.state.scan_prefix(&path).keys() {
            let i = i?;
            let path = Path::new(&i);
            let dot = path.dot();
            ctx.dots.insert(dot);
        }
        // add the new value into the context with a new dot
        let dot = writer.dot();
        ctx.dots.insert(dot);
        let mut store = BTreeMap::new();
        store.insert(dot, v);
        Ok(Causal {
            store: FlatDotStore::dotfun(path, store),
            ctx,
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
        let mut ctx = self.empty_ctx(path.root().unwrap())?;
        let dot = writer.dot();
        ctx.dots.insert(dot);
        for res in self.state.scan_prefix(path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            let ty = key.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            ctx.dots.insert(dot);
        }
        Ok(Causal {
            store: FlatDotStore::default(),
            ctx,
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
        let mut ctx = self.empty_ctx(path.root().unwrap())?;
        let dot = writer.dot();
        ctx.dots.insert(dot);
        let mut store = BTreeMap::new();
        store.insert(dot, policy);
        Ok(Causal {
            store: FlatDotStore::policy(path, store),
            ctx,
        })
    }

    pub fn transform(
        &self,
        doc: &DocId,
        schema_id: Hash,
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
    use crate::props::*;
    use crate::{Backend, Kind, Lens, PrimitiveKind};
    use proptest::prelude::*;
    use std::pin::Pin;

    #[async_std::test]
    async fn test_ewflag() -> Result<()> {
        let sdk = Backend::memory()?;
        let peer = PeerId::new([0; 32]);
        let mut doc = sdk.create_doc(peer)?;
        let hash = sdk.register(vec![Lens::Make(Kind::Flag)])?;
        doc.transform(hash)?;
        sdk.await?;
        let op = doc.cursor().enable()?;
        assert!(!doc.cursor().enabled()?);
        doc.join(&peer, op)?;
        assert!(doc.cursor().enabled()?);
        let op = doc.cursor().disable()?;
        println!("created op {:?}", op);
        doc.join(&peer, op)?;
        println!("joined op");
        assert!(!doc.cursor().enabled()?);
        Ok(())
    }

    #[async_std::test]
    #[ignore]
    async fn test_ewflag_unjoin() -> Result<()> {
        let peer1 = PeerId::new([0; 32]);

        let sdk1 = Backend::memory()?;
        let hash1 = sdk1.register(vec![Lens::Make(Kind::Flag)])?;
        let mut doc1 = sdk1.create_doc(peer1)?;
        doc1.transform(hash1)?;
        sdk1.await?;

        let sdk2 = Backend::memory()?;
        let mut doc2 = sdk2.create_doc(peer1)?;
        let hash2 = sdk2.register(vec![Lens::Make(Kind::Flag)])?;
        doc2.transform(hash2)?;
        sdk2.await?;
        assert!(hash1 == hash2);

        let mut op = doc1.cursor().enable()?;
        doc1.join(&peer1, op.clone())?;
        op.ctx.doc = *doc2.id();
        doc2.join(&peer1, op)?;

        assert!(doc1.cursor().enabled()?);
        assert!(doc2.cursor().enabled()?);

        let ctx_after_enable = doc1.ctx()?;
        let mut op = doc1.cursor().disable()?;
        doc1.join(&peer1, op.clone())?;
        let ctx_after_disable = doc1.ctx()?;
        if false {
            // apply the op
            op.ctx.doc = *doc2.id();
            doc2.join(&peer1, op)?;
        } else {
            // compute the delta using unjoin, and apply that
            let mut delta = doc1.unjoin(&peer1, ctx_after_enable.as_ref())?;
            let diff = ctx_after_disable
                .to_owned()?
                .dots
                .difference(&ctx_after_enable.to_owned()?.dots);
            println!("op {:?}", op);
            println!("ctx after enable {:?}", ctx_after_enable.to_owned()?.dots);
            println!("ctx after disable {:?}", ctx_after_disable.to_owned()?.dots);
            println!("difference {:?}", diff);
            println!("delta {:?}", delta);
            delta.ctx.doc = *doc2.id();
            println!("{:?}", delta.store());
            doc2.join(&peer1, delta)?;
        }
        assert!(!doc1.cursor().enabled()?);
        assert!(!doc2.cursor().enabled()?);

        Ok(())
    }

    #[async_std::test]
    async fn test_mvreg() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let peer1 = PeerId::new([1; 32]);
        let mut doc = sdk.create_doc(peer1)?;
        let hash = sdk.register(vec![Lens::Make(Kind::Reg(PrimitiveKind::U64))])?;
        doc.transform(hash)?;
        Pin::new(&mut sdk).await?;

        let peer2 = PeerId::new([2; 32]);
        let op = doc.cursor().say_can(Some(peer2), Permission::Write)?;
        doc.join(&peer1, op)?;
        Pin::new(&mut sdk).await?;

        let op1 = doc.cursor().assign(Primitive::U64(42))?;
        doc.join(&peer1, op1)?;

        //TODO
        //let op2 = crdt.assign(path.as_path(), &peer2, Primitive::U64(43))?;
        //crdt.join(&peer2, &op2)?;

        let values = doc.cursor().u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        //assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        //assert!(values.contains(&43));

        let op = doc.cursor().assign(Primitive::U64(99))?;
        doc.join(&peer1, op)?;

        let values = doc.cursor().u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 1);
        assert!(values.contains(&99));

        Ok(())
    }

    #[async_std::test]
    async fn test_ormap() -> Result<()> {
        let mut sdk = Backend::memory()?;
        let peer = PeerId::new([1; 32]);
        let mut doc = sdk.create_doc(peer)?;

        let hash = sdk.register(vec![
            Lens::Make(Kind::Table(PrimitiveKind::Str)),
            Lens::LensMapValue(Box::new(Lens::Make(Kind::Table(PrimitiveKind::Str)))),
            Lens::LensMapValue(Box::new(Lens::LensMapValue(Box::new(Lens::Make(
                Kind::Reg(PrimitiveKind::U64),
            ))))),
        ])?;
        doc.transform(hash)?;
        Pin::new(&mut sdk).await?;

        let a = Primitive::Str("a".into());
        let b = Primitive::Str("b".into());
        let cur = doc.cursor().key(&a)?.key(&b)?;
        let op = cur.assign(Primitive::U64(42))?;
        doc.join(&peer, op)?;

        let values = cur.u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert_eq!(values.len(), 1);
        assert!(values.contains(&42));

        let cur = doc.cursor().key(&a)?;
        let op = cur.remove(b.clone())?;
        doc.join(&peer, op)?;

        let values = cur.key(&b)?.u64s()?.collect::<Result<BTreeSet<u64>>>()?;
        assert!(values.is_empty());

        Ok(())
    }

    proptest! {
        #[test]
        fn causal_unjoin(a in arb_causal(arb_flatdotstore()), b in arb_causal_ctx()) {
            let b = a.unjoin(&b);
            prop_assert_eq!(join(&a, &b), a);
        }

        #[test]
        fn causal_join_idempotent(a in arb_causal(arb_flatdotstore())) {
            prop_assert_eq!(join(&a, &a), a);
        }

        #[test]
        fn causal_join_commutative(dots in arb_causal(arb_flatdotstore()), a in arb_causal_ctx(), b in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            prop_assert_eq!(join(&a, &b), join(&b, &a));
        }

        #[test]
        fn causal_join_associative(dots in arb_causal(arb_flatdotstore()), a in arb_causal_ctx(), b in arb_causal_ctx(), c in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let c = dots.unjoin(&c);
            prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
        }

        #[test]
        #[ignore]
        // TODO: crdt can infer defaults from path, causal just sets it to null
        fn crdt_join(dots in arb_causal(arb_flatdotstore()), a in arb_causal_ctx(), b in arb_causal_ctx()) {
            let a = dots.unjoin(&a);
            let b = dots.unjoin(&b);
            let crdt = causal_to_crdt(&a);
            let c = join(&a, &b);
            crdt.join(&dots.ctx.doc.into(), &b).unwrap();
            let c2 = crdt_to_causal(&crdt, &dots.ctx);
            assert_eq!(c, c2);
        }

        #[test]
        #[ignore]
        fn crdt_unjoin(causal in arb_causal(arb_flatdotstore()), ctx in arb_causal_ctx()) {
            let peer_id = PeerId::new([0; 32]);
            let crdt = causal_to_crdt(&causal);
            let c = causal.unjoin(&ctx);
            let actx = Ref::archive(&ctx);
            let c2 = crdt.unjoin(&peer_id, actx.as_ref()).unwrap();
            assert_eq!(c, c2);
        }
    }
}

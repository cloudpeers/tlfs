use crate::engine::Policy;
use crate::id::{DocId, PeerId};
use crate::Causal;
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tlfs_crdt::{DotStore, EWFlag, Lattice, MVReg, ORMap};

pub type CausalContext = tlfs_crdt::CausalContext<PeerId>;
pub type Dot = tlfs_crdt::Dot<PeerId>;
pub type Prop = String;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd, CheckBytes))]
#[repr(C)]
pub enum Primitive {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
}

impl Lattice for Primitive {
    fn join(&mut self, _other: &Self) {
        panic!("should never happen");
    }
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
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub enum Data {
    Null,
    Flag(EWFlag<PeerId>),
    Reg(MVReg<PeerId, Primitive>),
    Table(ORMap<Primitive, Crdt>),
    Struct(BTreeMap<Prop, Crdt>),
}

impl Default for Data {
    fn default() -> Self {
        Self::Null
    }
}

impl DotStore<PeerId> for Data {
    fn is_empty(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Flag(f) => f.is_empty(),
            Self::Reg(r) => r.is_empty(),
            Self::Table(t) => t.is_empty(),
            Self::Struct(s) => s.iter().all(|(_, v)| v.is_empty()),
        }
    }

    fn dots(&self, dots: &mut BTreeSet<Dot>) {
        match self {
            Self::Null => {}
            Self::Flag(f) => f.dots(dots),
            Self::Reg(r) => r.dots(dots),
            Self::Table(t) => t.dots(dots),
            Self::Struct(s) => {
                for v in s.values() {
                    v.dots(dots);
                }
            }
        }
    }

    fn join(&mut self, ctx: &CausalContext, other: &Self, other_ctx: &CausalContext) {
        match (self, other) {
            (Self::Flag(f1), Self::Flag(f2)) => f1.join(ctx, f2, other_ctx),
            (Self::Reg(r1), Self::Reg(r2)) => r1.join(ctx, r2, other_ctx),
            (Self::Table(t1), Self::Table(t2)) => t1.join(ctx, t2, other_ctx),
            (Self::Struct(s1), Self::Struct(s2)) => {
                for (k, v2) in s2 {
                    if let Some(v1) = s1.get_mut(k) {
                        v1.join(ctx, v2, other_ctx);
                    } else {
                        s1.insert(k.clone(), v2.clone());
                    }
                }
            }
            (_, _) => panic!("invalid data"),
        }
    }

    fn unjoin(&self, diff: &CausalContext) -> Self {
        match self {
            Self::Null => Self::Null,
            Self::Flag(f) => Self::Flag(f.unjoin(diff)),
            Self::Reg(r) => Self::Reg(r.unjoin(diff)),
            Self::Table(t) => Self::Table(t.unjoin(diff)),
            Self::Struct(s) => {
                let mut delta = BTreeMap::new();
                for (k, v) in s {
                    let v = v.unjoin(diff);
                    if !v.is_empty() {
                        delta.insert(k.clone(), v);
                    }
                }
                Self::Struct(delta)
            }
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(derive(CheckBytes))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub struct Crdt {
    #[omit_bounds]
    #[archive_attr(omit_bounds)]
    pub data: Data,
    pub policy: BTreeMap<Dot, Policy>,
}

impl DotStore<PeerId> for Crdt {
    fn is_empty(&self) -> bool {
        self.data.is_empty() && self.policy.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot>) {
        self.data.dots(dots);
        dots.extend(self.policy.keys().copied());
    }

    fn join(&mut self, ctx: &CausalContext, other: &Self, other_ctx: &CausalContext) {
        self.data.join(ctx, &other.data, other_ctx);
        self.policy
            .extend(other.policy.iter().map(|(k, v)| (*k, v.clone())));
    }

    fn unjoin(&self, diff: &CausalContext) -> Self {
        Self {
            data: self.data.unjoin(diff),
            policy: self
                .policy
                .iter()
                .filter(|(dot, _)| diff.contains(dot))
                .map(|(k, v)| (*k, v.clone()))
                .collect(),
        }
    }
}

impl Crdt {
    pub fn new(data: Data) -> Self {
        Self {
            data,
            policy: Default::default(),
        }
    }

    pub fn say(dot: Dot, policy: Policy) -> Causal {
        let mut causal = Causal::new();
        causal.store.policy.insert(dot, policy);
        causal
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq, CheckBytes))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[archive_attr(check_bytes(
    bound = "__C: rkyv::validation::ArchiveContext, <__C as rkyv::Fallible>::Error: std::error::Error"
))]
#[repr(C)]
pub enum Label {
    Root(DocId),
    Field(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Label>,
        String,
    ),
    Key(
        #[omit_bounds]
        #[archive_attr(omit_bounds)]
        Box<Label>,
        Primitive,
    ),
}

impl Label {
    fn root(&self) -> DocId {
        match self {
            Self::Root(id) => *id,
            Self::Field(l, _) => l.root(),
            Self::Key(l, _) => l.root(),
        }
    }
}

impl std::fmt::Display for Label {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Root(id) => write!(f, "{}", id),
            Self::Field(l, s) => {
                write!(f, "{}", l)?;
                write!(f, ".{}", s)
            }
            Self::Key(l, s) => {
                write!(f, "{}", l)?;
                write!(f, ".{:?}", s)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LabelRef<'a> {
    Root(DocId),
    Field(&'a LabelRef<'a>, &'a str),
    Key(&'a LabelRef<'a>, &'a Primitive),
}

impl<'a> LabelRef<'a> {
    fn root(&self) -> DocId {
        match self {
            Self::Root(id) => *id,
            Self::Field(l, _) => l.root(),
            Self::Key(l, _) => l.root(),
        }
    }

    pub fn to_label(self) -> Label {
        match self {
            Self::Root(id) => Label::Root(id),
            Self::Field(l, s) => Label::Field(Box::new(l.to_label()), s.to_string()),
            Self::Key(l, s) => Label::Key(Box::new(l.to_label()), s.clone()),
        }
    }
}

impl<'a> std::fmt::Display for LabelRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Root(id) => write!(f, "{}", id),
            Self::Field(l, s) => {
                write!(f, "{}", l)?;
                write!(f, ".{}", s)
            }
            Self::Key(l, s) => {
                write!(f, "{}", l)?;
                write!(f, ".{:?}", s)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LabelCow<'a> {
    Label(&'a Label),
    LabelRef(LabelRef<'a>),
}

impl<'a> LabelCow<'a> {
    pub fn root(self) -> DocId {
        match self {
            Self::Label(l) => l.root(),
            Self::LabelRef(l) => l.root(),
        }
    }

    pub fn is_ancestor(self, other: LabelCow<'a>) -> bool {
        let s = self.to_string();
        let s2 = other.to_string();
        s2.starts_with(&s)
    }
}

impl<'a> std::fmt::Display for LabelCow<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Label(l) => write!(f, "{}", l),
            Self::LabelRef(l) => write!(f, "{}", l),
        }
    }
}

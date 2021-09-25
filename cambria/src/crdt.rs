use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tlfs_crdt::{Clock, DotStore, EWFlag, Lattice, MVReg, ORMap};

pub use tlfs_crdt::{Actor, Dot};

pub type Prop = String;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize)]
#[cfg_attr(any(test, feature = "arb"), derive(proptest_derive::Arbitrary))]
#[repr(C)]
pub enum Primitive {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
}

impl Lattice for Primitive {
    fn join(&mut self, _other: &Self) {
        // Not needed for a mvreg
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub enum Crdt<A: Actor> {
    Null,
    Flag(EWFlag<A>),
    Reg(MVReg<A, Primitive>),
    Table(ORMap<Primitive, Crdt<A>>),
    Struct(BTreeMap<Prop, Crdt<A>>),
}

impl<A: Actor> Default for Crdt<A> {
    fn default() -> Self {
        Self::Null
    }
}

impl<A: Actor> DotStore<A> for Crdt<A> {
    fn is_empty(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Flag(f) => f.is_empty(),
            Self::Reg(r) => r.is_empty(),
            Self::Table(t) => t.is_empty(),
            Self::Struct(s) => s.iter().all(|(_, v)| v.is_empty()),
        }
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
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

    fn join(&mut self, clock: &Clock<A>, other: &Self, other_clock: &Clock<A>) {
        match (self, other) {
            (Self::Flag(f1), Self::Flag(f2)) => f1.join(clock, f2, other_clock),
            (Self::Reg(r1), Self::Reg(r2)) => r1.join(clock, r2, other_clock),
            (Self::Table(t1), Self::Table(t2)) => t1.join(clock, t2, other_clock),
            (Self::Struct(s1), Self::Struct(s2)) => {
                for (k, v2) in s2 {
                    if let Some(v1) = s1.get_mut(k) {
                        v1.join(clock, v2, other_clock);
                    } else {
                        s1.insert(k.clone(), v2.clone());
                    }
                }
            }
            (me, _) => *me = Self::Null,
        }
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
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

#[cfg(any(test, feature = "proptest"))]
pub mod arb {
    use super::*;
    use proptest::prelude::*;
    use tlfs_crdt::props::*;

    pub fn arb_crdt() -> impl Strategy<Value = Crdt<u8>> {
        let leaf = prop_oneof![
            Just(Crdt::Null),
            arb_ewflag().prop_map(Crdt::Flag),
            arb_mvreg().prop_map(Crdt::Reg),
        ];
        leaf.prop_recursive(8, 256, 10, |inner| {
            prop_oneof![
                arb_ormap(inner.clone()).prop_map(Crdt::Table),
                prop::collection::btree_map("[^/ ][^/ ]*", inner, 0..10).prop_map(Crdt::Struct),
            ]
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    tlfs_crdt::lattice!(crdt, arb::arb_crdt);
}

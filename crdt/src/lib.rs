//! Delta crdts

mod clock;
mod crdts;
#[cfg(any(feature = "proptest", test))]
pub mod props;
mod store;

pub use crate::clock::{Actor, Clock, Dot};
pub use crate::crdts::{EWFlag, MVReg, ORMap};
pub use crate::store::{DotFun, DotMap, DotSet, DotStore};

use rkyv::{Archive, Deserialize, Serialize};

/// Join semilattice.
pub trait Lattice {
    /// Joins are required to be idempotent, associative and commutative.
    fn join(&mut self, other: &Self);
}

impl Lattice for u64 {
    fn join(&mut self, other: &Self) {
        if other > self {
            *self = *other;
        }
    }
}

pub struct CausalRef<'a, A: Actor, S> {
    pub store: &'a S,
    pub clock: &'a Clock<A>,
}

impl<'a, A: Actor, S> Clone for CausalRef<'a, A, S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            clock: self.clock,
        }
    }
}

impl<'a, A: Actor, S> Copy for CausalRef<'a, A, S> {}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct Causal<A: Actor, S> {
    pub store: S,
    pub clock: Clock<A>,
}

impl<A: Actor, S: Default> Default for Causal<A, S> {
    fn default() -> Self {
        Self {
            store: Default::default(),
            clock: Default::default(),
        }
    }
}

impl<A: Actor, S> Causal<A, S> {
    pub fn new() -> Self
    where
        S: Default,
    {
        Self::default()
    }

    pub fn as_ref(&self) -> CausalRef<'_, A, S> {
        CausalRef {
            store: &self.store,
            clock: &self.clock,
        }
    }

    pub fn join(&mut self, other: &Self)
    where
        A: Clone,
        S: DotStore<A>,
    {
        self.store.join(&self.clock, &other.store, &other.clock);
        self.clock.union(&other.clock);
    }

    pub fn unjoin(&self, other: &Clock<A>) -> Self
    where
        A: Clone,
        S: DotStore<A>,
    {
        let diff = self.clock.difference(other);
        Self {
            store: self.store.unjoin(&diff),
            clock: diff,
        }
    }
}

impl<A: Actor, S: DotStore<A>> Lattice for Causal<A, S> {
    fn join(&mut self, other: &Self) {
        self.join(other);
    }
}

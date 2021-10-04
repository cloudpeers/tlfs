//! Delta crdts

mod crdts;
mod dotset;
mod dotset2;
#[cfg(any(feature = "proptest", test))]
pub mod props;
mod store;

pub use crate::crdts::{EWFlag, MVReg, ORMap};
pub use crate::dotset::{Dot, DotSet, ReplicaId};
pub use crate::store::{CheckBottom, DotFun, DotMap, DotStore, Key};

/// A causal context is a grow only set of dots that describes the causal history of a replica.
/// Every dot corresponds to an event. We use a DotSet for this.
///
/// CausalContext = 𝑃(𝕀 ✕ ℕ)
pub type CausalContext<I> = DotSet<I>;
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

/// Join semilattice.
pub trait Lattice: Clone + Archive {
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

pub struct CausalRef<'a, I: ReplicaId, S> {
    pub store: &'a S,
    pub ctx: &'a CausalContext<I>,
}

impl<'a, I: ReplicaId, S> Clone for CausalRef<'a, I, S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            ctx: self.ctx,
        }
    }
}

impl<'a, I: ReplicaId, S> Copy for CausalRef<'a, I, S> {}

impl<'a, I: ReplicaId, S> CausalRef<'a, I, S> {
    pub fn new(store: &'a S, ctx: &'a CausalContext<I>) -> Self {
        Self { store, ctx }
    }

    pub fn map<S2>(self, store: &'a S2) -> CausalRef<'a, I, S2> {
        CausalRef::new(store, self.ctx)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, CheckBytes, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct Causal<I: ReplicaId, S> {
    pub store: S,
    pub ctx: CausalContext<I>,
}

impl<I: ReplicaId, S: Default> Default for Causal<I, S> {
    fn default() -> Self {
        Self {
            store: Default::default(),
            ctx: Default::default(),
        }
    }
}

impl<I: ReplicaId, S> Causal<I, S> {
    pub fn new() -> Self
    where
        S: Default,
    {
        Self::default()
    }

    pub fn map<S2, F: Fn(S) -> S2>(self, f: F) -> Causal<I, S2> {
        Causal {
            store: f(self.store),
            ctx: self.ctx,
        }
    }

    pub fn as_ref(&self) -> CausalRef<'_, I, S> {
        CausalRef::new(&self.store, &self.ctx)
    }

    pub fn join(&mut self, other: &Self)
    where
        S: DotStore<I>,
    {
        self.store.join(&self.ctx, &other.store, &other.ctx);
        self.ctx.union(&other.ctx);
    }

    pub fn unjoin(&self, other: &DotSet<I>) -> Self
    where
        S: DotStore<I>,
    {
        let diff = self.ctx.difference(other);
        Self {
            store: self.store.unjoin(&diff),
            ctx: diff,
        }
    }
}

impl<I: ReplicaId, S: DotStore<I>> Lattice for Causal<I, S> {
    fn join(&mut self, other: &Self) {
        self.join(other);
    }
}

use crate::{CausalContext, Dot, DotSet, Lattice, ReplicaId};
use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

pub trait Key: Clone + Ord + Archive {}

impl<T: Clone + Ord + Archive> Key for T {}

pub trait CheckBottom {
    fn is_bottom(&self) -> bool;
}

pub trait DotStore<I: ReplicaId>: Archive + Clone + Default + CheckBottom {
    /// Returns true if there are no dots in the store.
    fn is_empty(&self) -> bool;
    /// Returns the set of dots in the store.
    fn dots(&self, dots: &mut BTreeSet<Dot<I>>);
    /// Joins are required to be idempotent, associative and commutative.
    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>);
    /// Unjoin a state based on a diff (context \ other_context).
    fn unjoin(&self, diff: &DotSet<I>) -> Self;
}

impl<I: ReplicaId> CheckBottom for DotSet<I> {
    fn is_bottom(&self) -> bool {
        self.is_empty()
    }
}

impl<I: ReplicaId> DotStore<I> for DotSet<I> {
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        for dot in self.iter() {
            dots.insert(dot);
        }
    }

    /// from the paper
    /// (s, c) ∐ (s', c') = ((s ∩ s') ∪ (s \ c') (s' \ c), c ∪ c')
    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        // (s \ c')
        let a = self.difference(&other_ctx);
        // (s' \ c)
        let b = other.difference(ctx);
        // ((s ∩ s')
        *self = self.intersection(other);
        // (s ∩ s') ∪ (s \ c') (s' \ c)
        self.union(&a);
        self.union(&b);
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        self.intersection(diff)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, CheckBytes, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct DotFun<I: ReplicaId, T> {
    fun: BTreeMap<Dot<I>, T>,
}

impl<I: ReplicaId, T> DotFun<I, T> {
    pub fn new(fun: BTreeMap<Dot<I>, T>) -> Self {
        Self { fun }
    }
}

impl<I: ReplicaId, T> Default for DotFun<I, T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<I: ReplicaId, T> Deref for DotFun<I, T> {
    type Target = BTreeMap<Dot<I>, T>;

    fn deref(&self) -> &Self::Target {
        &self.fun
    }
}

impl<I: ReplicaId, T> DerefMut for DotFun<I, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fun
    }
}

impl<I: ReplicaId, T> CheckBottom for DotFun<I, T> {
    fn is_bottom(&self) -> bool {
        self.fun.is_empty()
    }
}

impl<I: ReplicaId, T: Lattice + Clone> DotStore<I> for DotFun<I, T> {
    fn is_empty(&self) -> bool {
        self.fun.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        for dot in self.fun.keys() {
            dots.insert(*dot);
        }
    }

    /// from the paper
    /// (m, c) ∐ (m', c') = ({ k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' } ∪
    ///                      {(d, v) ∊ m | d ∉ c'} ∪ {(d, v) ∊ m' | d ∉ c}, c ∪ c')
    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        self.fun.retain(|dot, v| {
            if let Some(v2) = other.fun.get(dot) {
                // join all elements that are in both funs
                // { k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' }
                v.join(v2);
                true
            } else {
                // keep all elements unmodified that are not in the other causal context
                // { (d, v) ∊ m | d ∉ c' }
                !other_ctx.contains(dot)
            }
        });
        // copy all elements from the other fun, that are neither in our fun nor in our causal context
        // { (d, v) ∊ m' | d ∉ c }
        for (d, v) in &other.fun {
            if !self.fun.contains_key(d) && !ctx.contains(d) {
                self.fun.insert(*d, v.clone());
            }
        }
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        let mut fun = BTreeMap::new();
        for (dot, v) in &self.fun {
            if diff.contains(dot) {
                fun.insert(*dot, v.clone());
            }
        }
        Self { fun }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, CheckBytes, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct DotMap<K: Ord, V> {
    map: BTreeMap<K, V>,
}

impl<K: Ord, V: CheckBottom> DotMap<K, V> {
    pub fn new(mut map: BTreeMap<K, V>) -> Self {
        // make sure empty values are not explicitly stored
        map.retain(|_, v| !v.is_bottom());
        Self { map }
    }

    pub fn insert(&mut self, key: K, value: V) {
        if !value.is_bottom() {
            self.map.insert(key, value);
        } else {
            self.map.remove(&key);
        }
    }
}

impl<K: Ord, V> Default for DotMap<K, V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

impl<K: Ord, V> Deref for DotMap<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

// TODO: get rid of this, since it allows the outside to break the invariants
impl<K: Ord, V> DerefMut for DotMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl<K: Key, V> CheckBottom for DotMap<K, V> {
    fn is_bottom(&self) -> bool {
        self.map.is_empty()
    }
}

impl<I: ReplicaId, K: Key, V: DotStore<I>> DotStore<I> for DotMap<K, V>
where
    Archived<K>: Ord,
{
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        for store in self.map.values() {
            store.dots(dots);
        }
    }

    /// from the paper
    /// (m, c) ∐ (m', c') = ({ k -> v(k), k ∈ dom m ∪ dom m' ∧ v(k) ≠ ⊥ }, c ∪ c')
    ///                     where v(k) = fst ((m(k), c) ∐ (m'(k), c'))
    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        let t = V::default();
        let mut all = self.map.keys().cloned().collect::<Vec<_>>();
        all.extend(other.map.keys().cloned());
        for key in all {
            let v1 = self.map.entry(key.clone()).or_default();
            let v2 = other.map.get(&key).unwrap_or(&t);
            v1.join(ctx, v2, other_ctx);
            if v1.is_bottom() {
                self.map.remove(&key);
            }
        }
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        let mut map = BTreeMap::new();
        for (k, v) in &self.map {
            let v = v.unjoin(diff);
            if !v.is_empty() {
                map.insert(k.clone(), v);
            }
        }
        Self { map }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    crate::lattice!(dotset, arb_dotset);
    crate::lattice!(dotfun, || arb_dotfun(any::<u64>()));
    crate::lattice!(dotmap, || arb_dotmap(arb_dotset()));
}

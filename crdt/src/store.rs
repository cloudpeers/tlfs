use crate::{Actor, Clock, Dot, Lattice};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

pub trait DotStore<A: Actor>: Clone + Default {
    /// Returns true if there are no dots in the store.
    fn is_empty(&self) -> bool;
    /// Returns the set of dots in the store.
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>);
    /// Joins are required to be idempotent, associative and commutative.
    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>);
    /// Unjoin a state based on a clock (clock \ other_clock).
    fn unjoin(&self, diff: &Clock<A>) -> Self;
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct DotSet<A: Actor> {
    set: BTreeSet<Dot<A>>,
}

impl<A: Actor> DotSet<A> {
    pub fn new(set: BTreeSet<Dot<A>>) -> Self {
        Self { set }
    }
}

impl<A: Actor> Default for DotSet<A> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<A: Actor> Deref for DotSet<A> {
    type Target = BTreeSet<Dot<A>>;

    fn deref(&self) -> &Self::Target {
        &self.set
    }
}

impl<A: Actor> DerefMut for DotSet<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.set
    }
}

impl<A: Actor> DotStore<A> for DotSet<A> {
    fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for dot in &self.set {
            dots.insert(*dot);
        }
    }

    /// from the paper
    /// (s, c) ∐ (s', c') = ((s ∩ s') ∪ (s \ c') (s' \ c), c ∪ c')
    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        // intersection of the two sets, and keep elements that are not in the other clock
        self.set.retain(|dot|
                // ((s ∩ s')
                other.set.contains(dot) ||
                // (s \ c')
                !clock_other.contains(dot));
        // add all elements of the other set which are not in our clock
        // (s' \ c)
        for dot in &other.set {
            if !clock.contains(dot) {
                self.set.insert(*dot);
            }
        }
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
        let mut set = BTreeSet::new();
        for dot in &self.set {
            if diff.contains(dot) {
                set.insert(*dot);
            }
        }
        Self { set }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct DotFun<A: Actor, T> {
    fun: BTreeMap<Dot<A>, T>,
}

impl<A: Actor, T> DotFun<A, T> {
    pub fn new(fun: BTreeMap<Dot<A>, T>) -> Self {
        Self { fun }
    }
}

impl<A: Actor, T> Default for DotFun<A, T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<A: Actor, T> Deref for DotFun<A, T> {
    type Target = BTreeMap<Dot<A>, T>;

    fn deref(&self) -> &Self::Target {
        &self.fun
    }
}

impl<A: Actor, T> DerefMut for DotFun<A, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.fun
    }
}

impl<A: Actor, T: Lattice + Clone> DotStore<A> for DotFun<A, T> {
    fn is_empty(&self) -> bool {
        self.fun.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for dot in self.fun.keys() {
            dots.insert(*dot);
        }
    }

    /// from the paper
    /// (m, c) ∐ (m', c') = ({ k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' } ∪
    ///                      {(d, v) ∊ m | d ∉ c'} ∪ {(d, v) ∊ m' | d ∉ c}, c ∪ c')
    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        self.fun.retain(|dot, v| {
            if let Some(v2) = other.fun.get(dot) {
                // join all elements that are in both funs
                // { k -> m(k) ∐ m'(k), k ∈ dom m ∩ dom m' }
                v.join(v2);
                true
            } else {
                // keep all elements unmodified that are not in the other clock
                // { (d, v) ∊ m | d ∉ c' }
                !clock_other.contains(dot)
            }
        });
        // copy all elements from the other fun, that are neither in our fun nor in our clock
        // { (d, v) ∊ m' | d ∉ c }
        for (d, v) in &other.fun {
            if !self.fun.contains_key(d) && !clock.contains(d) {
                self.fun.insert(*d, v.clone());
            }
        }
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
        let mut fun = BTreeMap::new();
        for (dot, v) in &self.fun {
            if diff.contains(dot) {
                fun.insert(*dot, v.clone());
            }
        }
        Self { fun }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct DotMap<K: Ord, V> {
    map: BTreeMap<K, V>,
}

impl<K: Ord, V> DotMap<K, V> {
    pub fn new(map: BTreeMap<K, V>) -> Self {
        Self { map }
    }
}

impl<K: Ord, V> Default for DotMap<K, V> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<K: Ord, V> Deref for DotMap<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K: Ord, V> DerefMut for DotMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl<A: Actor, K: Clone + Ord, V: DotStore<A>> DotStore<A> for DotMap<K, V> {
    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for store in self.map.values() {
            store.dots(dots);
        }
    }

    /// from the paper
    /// (m, c) ∐ (m', c') = ({ k -> v(k), k ∈ dom m ∪ dom m' ∧ v(k) ≠ ⊥ }, c ∪ c')
    ///                     where v(k) = fst ((m(k), c) ∐ (m'(k), c'))
    fn join(&mut self, clock: &Clock<A>, other: &Self, other_clock: &Clock<A>) {
        for (k, v2) in &other.map {
            if let Some(v) = self.map.get_mut(k) {
                // we got a value in both maps, so we need to do the join
                v.join(clock, v2, other_clock);
            } else {
                // we don't have a value yet, just copy over the other one
                self.map.insert(k.clone(), v2.clone());
            }
        }
        // all other values will remain unchanged
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
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

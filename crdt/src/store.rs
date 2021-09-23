use crate::clock::Clock;
use crate::dot::Dot;
use std::collections::{BTreeMap, BTreeSet};

/// Join semilattice.
pub trait Lattice {
    /// Joins are required to be idempotent, associative and commutative.
    fn join(&mut self, other: &Self);
}

pub trait DotStore<A: Ord> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>);
    fn clock(&self, clock: &mut Clock<A>);
}

#[derive(Clone, Debug)]
pub struct DotSet<A: Ord> {
    pub set: BTreeSet<Dot<A>>,
}

impl<A: Ord> Default for DotSet<A> {
    fn default() -> Self {
        Self {
            set: Default::default(),
        }
    }
}

impl<A: Clone + Ord> DotStore<A> for DotSet<A> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for dot in &self.set {
            dots.insert(dot.clone());
        }
    }

    fn clock(&self, clock: &mut Clock<A>) {
        for dot in &self.set {
            clock.apply(dot.clone());
        }
    }
}

#[derive(Clone, Debug)]
pub struct DotFun<A: Ord, T> {
    pub fun: BTreeMap<Dot<A>, T>,
}

impl<A: Ord, T> Default for DotFun<A, T> {
    fn default() -> Self {
        Self {
            fun: Default::default(),
        }
    }
}

impl<A: Clone + Ord, T: Lattice> DotStore<A> for DotFun<A, T> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for dot in self.fun.keys() {
            dots.insert(dot.clone());
        }
    }

    fn clock(&self, clock: &mut Clock<A>) {
        for dot in self.fun.keys() {
            clock.apply(dot.clone());
        }
    }
}

#[derive(Clone, Debug)]
pub struct DotMap<K: Ord, V> {
    pub map: BTreeMap<K, V>,
}

impl<K: Ord, V> Default for DotMap<K, V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
        }
    }
}

impl<A: Clone + Ord, K: Clone + Ord, V: Clone + DotStore<A>> DotStore<A> for DotMap<K, V> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for store in self.map.values() {
            store.dots(dots);
        }
    }

    fn clock(&self, clock: &mut Clock<A>) {
        for store in self.map.values() {
            store.clock(clock);
        }
    }
}

#[derive(Clone, Debug)]
pub struct Causal<A: Ord, S> {
    pub store: S,
    pub clock: Clock<A>,
}

pub struct CausalRef<'a, A: Ord, S> {
    pub store: &'a S,
    pub clock: &'a Clock<A>,
}

impl<'a, A: Ord, S> Clone for CausalRef<'a, A, S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store,
            clock: self.clock,
        }
    }
}

impl<'a, A: Ord, S> Copy for CausalRef<'a, A, S> {}

impl<A: Ord, S: Default> Default for Causal<A, S> {
    fn default() -> Self {
        Self {
            store: Default::default(),
            clock: Default::default(),
        }
    }
}

impl<A: Ord, S> Causal<A, S> {
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
}

/// Causal join semilattice.
pub trait CausalLattice<A: Ord> {
    /// Joins are required to be idempotent, associative and commutative.
    fn causal_join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>);
}

impl<A: Clone + Ord, T: CausalLattice<A>> Lattice for Causal<A, T> {
    fn join(&mut self, other: &Self) {
        self.store
            .causal_join(&self.clock, &other.store, &other.clock);
        self.clock.union(&other.clock);
    }
}

impl<A: Ord, T: Lattice> CausalLattice<A> for T {
    fn causal_join(&mut self, _: &Clock<A>, other: &Self, _: &Clock<A>) {
        self.join(other);
    }
}

impl<A: Clone + Ord> CausalLattice<A> for DotSet<A> {
    fn causal_join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        for dot in &other.set {
            if clock_other.get(&dot.actor) > clock.get(&dot.actor) {
                self.set.insert(dot.clone());
            }
        }
        self.set.retain(|dot| {
            other.set.contains(dot) || clock.get(&dot.actor) > clock_other.get(&dot.actor)
        });
    }
}

impl<A: Clone + Ord, T: Clone + Lattice> CausalLattice<A> for DotFun<A, T> {
    fn causal_join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        for (dot, v) in &other.fun {
            if let Some(v2) = self.fun.get_mut(dot) {
                v2.join(v);
            } else if clock_other.get(&dot.actor) > clock.get(&dot.actor) {
                self.fun.insert(dot.clone(), v.clone());
            }
        }
        self.fun.retain(|dot, _| {
            other.fun.contains_key(dot) || clock.get(&dot.actor) > clock_other.get(&dot.actor)
        });
    }
}

impl<A: Clone + Ord, K: Clone + Ord, V> CausalLattice<A> for DotMap<K, V>
where
    V: CausalLattice<A> + Clone,
{
    fn causal_join(&mut self, clock: &Clock<A>, other: &Self, other_clock: &Clock<A>) {
        for (k, v) in &other.map {
            if let Some(v2) = self.map.get_mut(k) {
                v2.causal_join(clock, v, other_clock);
            } else {
                self.map.insert(k.clone(), v.clone());
            }
        }
    }
}

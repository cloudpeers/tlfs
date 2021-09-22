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
}

pub struct DotSet<A: Ord> {
    set: BTreeSet<Dot<A>>,
}

impl<A: Clone + Ord> DotStore<A> for DotSet<A> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for dot in &self.set {
            dots.insert(dot.clone());
        }
    }
}

pub struct DotFun<A: Ord, T> {
    fun: BTreeMap<Dot<A>, T>,
}

impl<A: Clone + Ord, T: Lattice> DotStore<A> for DotFun<A, T> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for dot in self.fun.keys() {
            dots.insert(dot.clone());
        }
    }
}

pub struct DotMap<K: Ord, V> {
    map: BTreeMap<K, V>,
}

impl<A: Clone + Ord, K: Clone + Ord, V: Clone + DotStore<A>> DotStore<A> for DotMap<K, V> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        for store in self.map.values() {
            store.dots(dots);
        }
    }
}

pub struct Causal<A: Ord, S> {
    store: S,
    clock: Clock<A>,
}

/// Causal join semilattice.
pub trait CausalLattice<A: Ord> {
    /// Joins are required to be idempotent, associative and commutative.
    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>);
}

impl<A: Clone + Ord, T: CausalLattice<A>> Lattice for Causal<A, T> {
    fn join(&mut self, other: &Self) {
        self.store.join(&self.clock, &other.store, &other.clock);
        self.clock.union(&other.clock);
    }
}

impl<A: Ord, T: Lattice> CausalLattice<A> for T {
    fn join(&mut self, _: &Clock<A>, other: &Self, _: &Clock<A>) {
        self.join(other);
    }
}

impl<'a, A: Clone + Ord> CausalLattice<A> for DotSet<A> {
    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
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

impl<'a, A: Clone + Ord, T: Clone + Lattice> CausalLattice<A> for DotFun<A, T> {
    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
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
    fn join(&mut self, clock: &Clock<A>, other: &Self, other_clock: &Clock<A>) {
        for (k, v) in &other.map {
            if let Some(v2) = self.map.get_mut(k) {
                v2.join(clock, v, other_clock);
            } else {
                self.map.insert(k.clone(), v.clone());
            }
        }
    }
}

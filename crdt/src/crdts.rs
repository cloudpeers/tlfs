use crate::{Causal, CausalRef, Clock, Dot, DotMap, DotSet, DotStore};
use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

#[derive(Clone)]
pub struct EWFlag<A: Clone + Ord>(DotSet<A>);

impl<A: Clone + Ord> Default for EWFlag<A> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<A: Clone + Ord> Deref for EWFlag<A> {
    type Target = DotSet<A>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Clone + Ord> DerefMut for EWFlag<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Clone + Ord> DotStore<A> for EWFlag<A> {
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        self.0.dots(dots)
    }

    fn clock(&self, clock: &mut Clock<A>) {
        self.0.clock(clock)
    }

    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        self.0.join(clock, other, clock_other);
    }
}

impl<'a, A: Clone + Ord> CausalRef<'a, A, EWFlag<A>> {
    pub fn enable(self, dot: Dot<A>) -> Causal<A, EWFlag<A>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.store.set.insert(dot.clone());
        delta.clock = self.clock.clone();
        delta.clock.apply(dot);
        delta
    }

    pub fn disable(self, dot: Dot<A>) -> Causal<A, EWFlag<A>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.clock = self.clock.clone();
        delta.clock.apply(dot);
        delta
    }

    pub fn value(self) -> bool {
        !self.store.set.is_empty()
    }
}

pub struct ORMap<K: Ord, V>(DotMap<K, V>);

impl<K: Ord, V> Default for ORMap<K, V> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K: Ord, V> Deref for ORMap<K, V> {
    type Target = DotMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Ord, V> DerefMut for ORMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Clone + Ord, K: Clone + Ord, V> DotStore<A> for ORMap<K, V>
where
    V: DotStore<A> + Clone,
{
    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        self.0.dots(dots)
    }

    fn clock(&self, clock: &mut Clock<A>) {
        self.0.clock(clock)
    }

    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        self.0.join(clock, other, clock_other);
    }
}

impl<'a, A: Clone + Ord, K: Ord, V> CausalRef<'a, A, ORMap<K, V>>
where
    V: DotStore<A> + Default,
{
    pub fn apply<F>(self, k: K, f: F) -> Causal<A, ORMap<K, V>>
    where
        F: Fn(CausalRef<'_, A, V>) -> Causal<A, V>,
    {
        let inner_delta = if let Some(v) = self.get(&k) {
            f(v)
        } else {
            let v = V::default();
            let vref = CausalRef {
                store: &v,
                clock: &self.clock,
            };
            f(vref)
        };
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        delta.store.map.insert(k, inner_delta.store);
        delta.clock = inner_delta.clock;
        delta
    }

    pub fn remove(self, k: &K) -> Causal<A, ORMap<K, V>> {
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        if let Some(v) = self.store.map.get(k) {
            v.clock(&mut delta.clock);
        }
        delta
    }

    pub fn get(self, k: &'a K) -> Option<CausalRef<'a, A, V>> {
        self.store.map.get(k).map(|v| CausalRef {
            store: v,
            clock: &self.clock,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Lattice;

    #[test]
    fn test_ew_flag() {
        let mut flag: Causal<_, EWFlag<_>> = Causal::new();
        let op1 = flag.as_ref().enable(Dot::new(0, 1));
        flag.join(&op1);
        assert!(flag.as_ref().value());
        let op2 = flag.as_ref().disable(Dot::new(0, 2));
        flag.join(&op2);
        assert!(!flag.as_ref().value());
    }

    #[test]
    fn test_or_map() {
        let mut map: Causal<_, ORMap<_, EWFlag<_>>> = Causal::new();
        let op1 = map
            .as_ref()
            .apply("flag", |flag| flag.enable(Dot::new(0, 1)));
        map.join(&op1);
        assert!(map.as_ref().get(&"flag").unwrap().value());
        let op2 = map
            .as_ref()
            .apply("flag", |flag| flag.disable(Dot::new(1, 1)));
        map.join(&op2);
        assert!(!map.as_ref().get(&"flag").unwrap().value());
    }
}

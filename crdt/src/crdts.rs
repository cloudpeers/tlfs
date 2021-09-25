use crate::{Actor, Causal, CausalRef, Clock, Dot, DotFun, DotMap, DotSet, DotStore, Lattice};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct EWFlag<A: Actor>(DotSet<A>);

impl<A: Actor> Default for EWFlag<A> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<A: Actor> Deref for EWFlag<A> {
    type Target = DotSet<A>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Actor> DerefMut for EWFlag<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Actor> DotStore<A> for EWFlag<A> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        self.0.dots(dots)
    }

    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        self.0.join(clock, other, clock_other);
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
        Self(self.0.unjoin(diff))
    }
}

impl<'a, A: Actor> CausalRef<'a, A, EWFlag<A>> {
    pub fn enable(self, dot: Dot<A>) -> Causal<A, EWFlag<A>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.store.insert(dot);
        delta.clock = self.clock.clone();
        delta.clock.insert(dot);
        delta
    }

    pub fn disable(self, dot: Dot<A>) -> Causal<A, EWFlag<A>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.clock = self.clock.clone();
        delta.clock.insert(dot);
        delta
    }

    pub fn value(self) -> bool {
        !self.store.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct MVReg<A: Actor, L>(DotFun<A, L>);

impl<A: Actor, L> Default for MVReg<A, L> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<A: Actor, L> Deref for MVReg<A, L> {
    type Target = DotFun<A, L>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<A: Actor, L> DerefMut for MVReg<A, L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Actor, L: Lattice + Clone> DotStore<A> for MVReg<A, L> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        self.0.dots(dots)
    }

    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        self.0.join(clock, other, clock_other);
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
        Self(self.0.unjoin(diff))
    }
}

impl<'a, A: Actor, L: Lattice> CausalRef<'a, A, MVReg<A, L>> {
    pub fn write(self, dot: Dot<A>, v: L) -> Causal<A, MVReg<A, L>> {
        let mut delta = Causal::<_, MVReg<_, _>>::new();
        delta.store.insert(dot, v);
        delta.clock = self.clock.clone();
        delta.clock.insert(dot);
        delta
    }

    pub fn read(self) -> impl Iterator<Item = &'a L> {
        self.store.values()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
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

impl<A: Actor, K: Clone + Ord, V> DotStore<A> for ORMap<K, V>
where
    V: DotStore<A> + Clone,
{
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<A>>) {
        self.0.dots(dots)
    }

    fn join(&mut self, clock: &Clock<A>, other: &Self, clock_other: &Clock<A>) {
        self.0.join(clock, other, clock_other);
    }

    fn unjoin(&self, diff: &Clock<A>) -> Self {
        Self(self.0.unjoin(diff))
    }
}

impl<'a, A: Actor, K: Ord, V: DotStore<A>> CausalRef<'a, A, ORMap<K, V>> {
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
                clock: self.clock,
            };
            f(vref)
        };
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        delta.store.insert(k, inner_delta.store);
        delta.clock = inner_delta.clock;
        delta
    }

    pub fn remove(self, dot: Dot<A>, k: &K) -> Causal<A, ORMap<K, V>> {
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        if let Some(v) = self.store.get(k) {
            let mut dots = BTreeSet::new();
            v.dots(&mut dots);
            delta.clock = dots.into_iter().collect();
            delta.clock.insert(dot);
        }
        delta
    }

    pub fn get(self, k: &'a K) -> Option<CausalRef<'a, A, V>> {
        self.store.get(k).map(|v| CausalRef {
            store: v,
            clock: self.clock,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_mv_reg() {
        let mut reg: Causal<_, MVReg<_, _>> = Causal::new();
        let op1 = reg.as_ref().write(Dot::new(0, 1), 42);
        let op2 = reg.as_ref().write(Dot::new(1, 1), 43);
        reg.join(&op1);
        reg.join(&op2);
        let values = reg.as_ref().read().collect::<BTreeSet<_>>();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        assert!(values.contains(&43));
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
        let op3 = map.as_ref().remove(Dot::new(0, 2), &"flag");
        map.join(&op2);
        map.join(&op3);
        assert!(!map.as_ref().get(&"flag").unwrap().value());
    }
}

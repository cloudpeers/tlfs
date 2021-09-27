use crate::{Actor, Causal, CausalRef, Clock, Dot, DotFun, DotMap, DotSet, DotStore, Key, Lattice};
use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
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
}

impl<A: Actor> EWFlag<A> {
    pub fn value(&self) -> bool {
        !self.0.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
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
}

impl<A: Actor, L: Lattice> MVReg<A, L> {
    pub fn read(&self) -> impl Iterator<Item = &L> {
        self.0.values()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct ORMap<K: Key, V>(DotMap<K, V>);

impl<K: Key, V> Default for ORMap<K, V> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<K: Key, V> Deref for ORMap<K, V> {
    type Target = DotMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K: Key, V> DerefMut for ORMap<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<A: Actor, K: Key, V: DotStore<A>> DotStore<A> for ORMap<K, V>
where
    Archived<K>: Ord,
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

impl<'a, A: Actor, K: Key, V: DotStore<A>> CausalRef<'a, A, ORMap<K, V>> {
    pub fn apply<F>(self, k: K, mut f: F) -> Causal<A, ORMap<K, V>>
    where
        F: FnMut(CausalRef<'_, A, V>) -> Causal<A, V>,
    {
        let inner_delta = if let Some(v) = self.store.get(&k) {
            f(self.map(v))
        } else {
            let v = V::default();
            let vref = self.map(&v);
            f(vref)
        };
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        delta.store.insert(k, inner_delta.store);
        delta.clock = inner_delta.clock;
        delta
    }

    pub fn remove<Q: ?Sized>(self, dot: Dot<A>, k: &Q) -> Causal<A, ORMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        if let Some(v) = self.store.get(k) {
            let mut dots = BTreeSet::new();
            v.dots(&mut dots);
            delta.clock = dots.into_iter().collect();
            delta.clock.insert(dot);
        }
        delta
    }
}

impl<K: Key, V> ORMap<K, V> {
    pub fn get<Q: ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        self.0.get(k)
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
        assert!(flag.store.value());
        let op2 = flag.as_ref().disable(Dot::new(0, 2));
        flag.join(&op2);
        assert!(!flag.store.value());
    }

    #[test]
    fn test_mv_reg() {
        let mut reg: Causal<_, MVReg<_, _>> = Causal::new();
        let op1 = reg.as_ref().write(Dot::new(0, 1), 42);
        let op2 = reg.as_ref().write(Dot::new(1, 1), 43);
        reg.join(&op1);
        reg.join(&op2);
        let values = reg.store.read().collect::<BTreeSet<_>>();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&42));
        assert!(values.contains(&43));
    }

    #[test]
    fn test_or_map() {
        let mut map: Causal<_, ORMap<_, EWFlag<_>>> = Causal::new();
        let op1 = map
            .as_ref()
            .apply("flag".to_string(), |flag| flag.enable(Dot::new(0, 1)));
        map.join(&op1);
        assert!(map.store.get("flag").unwrap().value());
        let op2 = map
            .as_ref()
            .apply("flag".to_string(), |flag| flag.disable(Dot::new(1, 1)));
        let op3 = map.as_ref().remove(Dot::new(0, 2), "flag");
        map.join(&op2);
        map.join(&op3);
        assert!(!map.store.get("flag").unwrap().value());
    }
}

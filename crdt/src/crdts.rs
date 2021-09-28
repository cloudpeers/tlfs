use crate::{Causal, CausalContext, CausalRef, Dot, DotFun, DotMap, DotSet, DotStore, Lattice, ReplicaId};
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct EWFlag<I: ReplicaId>(DotSet<I>);

impl<I: ReplicaId> Default for EWFlag<I> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<I: ReplicaId> Deref for EWFlag<I> {
    type Target = DotSet<I>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I: ReplicaId> DerefMut for EWFlag<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<I: ReplicaId> DotStore<I> for EWFlag<I> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        self.0.dots(dots)
    }

    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        self.0.join(ctx, other, other_ctx);
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        Self(self.0.unjoin(diff))
    }
}

impl<'a, I: ReplicaId> CausalRef<'a, I, EWFlag<I>> {
    pub fn enable(self, dot: Dot<I>) -> Causal<I, EWFlag<I>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.store.insert(dot);
        delta.ctx = self.ctx.clone();
        delta.ctx.insert(dot);
        delta
    }

    pub fn disable(self, dot: Dot<I>) -> Causal<I, EWFlag<I>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.ctx = self.ctx.clone();
        delta.ctx.insert(dot);
        delta
    }

    pub fn value(self) -> bool {
        !self.store.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct MVReg<I: ReplicaId, L>(DotFun<I, L>);

impl<I: ReplicaId, L> Default for MVReg<I, L> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<I: ReplicaId, L> Deref for MVReg<I, L> {
    type Target = DotFun<I, L>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I: ReplicaId, L> DerefMut for MVReg<I, L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<I: ReplicaId, L: Lattice + Clone> DotStore<I> for MVReg<I, L> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        self.0.dots(dots)
    }

    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        self.0.join(ctx, other, other_ctx);
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        Self(self.0.unjoin(diff))
    }
}

impl<'a, I: ReplicaId, L: Lattice> CausalRef<'a, I, MVReg<I, L>> {
    pub fn write(self, dot: Dot<I>, v: L) -> Causal<I, MVReg<I, L>> {
        let mut delta = Causal::<_, MVReg<_, _>>::new();
        delta.store.insert(dot, v);
        delta.ctx = self.ctx.clone();
        delta.ctx.insert(dot);
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

impl<I: ReplicaId, K: Clone + Ord, V> DotStore<I> for ORMap<K, V>
where
    V: DotStore<I> + Clone,
{
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        self.0.dots(dots)
    }

    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        self.0.join(ctx, other, other_ctx);
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        Self(self.0.unjoin(diff))
    }
}

impl<'a, I: ReplicaId, K: Ord, V: DotStore<I>> CausalRef<'a, I, ORMap<K, V>> {
    pub fn apply<F>(self, k: K, f: F) -> Causal<I, ORMap<K, V>>
    where
        F: Fn(CausalRef<'_, I, V>) -> Causal<I, V>,
    {
        let inner_delta = if let Some(v) = self.get(&k) {
            f(v)
        } else {
            let v = V::default();
            let vref = CausalRef {
                store: &v,
                ctx: self.ctx,
            };
            f(vref)
        };
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        delta.store.insert(k, inner_delta.store);
        delta.ctx = inner_delta.ctx;
        delta
    }

    pub fn remove(self, dot: Dot<I>, k: &K) -> Causal<I, ORMap<K, V>> {
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        if let Some(v) = self.store.get(k) {
            let mut dots = BTreeSet::new();
            v.dots(&mut dots);
            delta.ctx = dots.into_iter().collect();
            delta.ctx.insert(dot);
        }
        delta
    }

    pub fn get(self, k: &'a K) -> Option<CausalRef<'a, I, V>> {
        self.store.get(k).map(|v| CausalRef {
            store: v,
            ctx: self.ctx,
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

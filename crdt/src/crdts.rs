use crate::pos_identifier::PositionalIdentifier;
use crate::store::CheckBottom;
use crate::{
    Causal, CausalContext, CausalRef, Dot, DotFun, DotMap, DotSet, DotStore, Key, Lattice,
    ReplicaId,
};
use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
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

impl<I: ReplicaId> CheckBottom for EWFlag<I> {
    fn is_bottom(&self) -> bool {
        self.0.is_empty()
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
    // enable δi ((s, c)) = (d, d ∪ s) where d = {next i (c)}
    pub fn enable(self, dot: Dot<I>) -> Causal<I, EWFlag<I>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.store.insert(dot);
        delta.ctx = self.ctx.clone();
        delta.ctx.insert(dot);
        delta
    }
    // disable δi ((s, c)) = ({}, s)
    pub fn disable(self, dot: Dot<I>) -> Causal<I, EWFlag<I>> {
        let mut delta = Causal::<_, EWFlag<_>>::new();
        delta.ctx = self.ctx.clone();
        delta.ctx.insert(dot);
        delta
    }
}

impl<I: ReplicaId> EWFlag<I> {
    // read((s, c)) = s 6 = {}
    pub fn value(&self) -> bool {
        !self.0.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
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

impl<I: ReplicaId, L> CheckBottom for MVReg<I, L> {
    fn is_bottom(&self) -> bool {
        self.0.is_empty()
    }
}

impl<I: ReplicaId, L: Lattice + Clone + std::fmt::Debug> DotStore<I> for MVReg<I, L> {
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
    // write δi (v, (m, c)) = ({d → v}, {d} ∪ dom m) where d = next i (c)
    pub fn write(self, dot: Dot<I>, v: L) -> Causal<I, MVReg<I, L>> {
        let mut delta = Causal::<_, MVReg<_, _>>::new();
        delta.store.insert(dot, v);
        delta.ctx = self.ctx.clone();
        delta.ctx.insert(dot);
        delta
    }
}

impl<I: ReplicaId, L: Lattice> MVReg<I, L> {
    // read((m, c)) = ran m
    pub fn read(&self) -> impl Iterator<Item = &L> {
        self.0.values()
    }
}

/// Dot for initial insertion delta used as an uid
type Uid<I> = Dot<I>;
type Metadata<I> = DotMap<Uid<I>, DotMap<Dot<I>, DotFun<I, PositionalIdentifier<I>>>>;

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct ORArray<I: ReplicaId, V> {
    meta: Metadata<I>,
    content: BTreeMap<PositionalIdentifier<I>, (Uid<I>, V)>,
}
impl<I: ReplicaId, V> Default for ORArray<I, V> {
    fn default() -> Self {
        Self {
            meta: Default::default(),
            content: Default::default(),
        }
    }
}

impl<I: ReplicaId, V> CheckBottom for ORArray<I, V> {
    fn is_bottom(&self) -> bool {
        self.meta.is_bottom()
    }
}

impl<I: ReplicaId, V> ORArray<I, V> {
    pub fn iter(&self) -> impl Iterator<Item = &V> {
        self.content.values().map(|(_, v)| v)
    }
    pub fn position(&self, ix: usize) -> Option<&V> {
        self.iter().nth(ix)
    }
}

impl<I: ReplicaId, V: Archive + Clone + std::fmt::Debug> DotStore<I> for ORArray<I, V> {
    fn is_empty(&self) -> bool {
        self.meta.is_empty()
    }

    fn dots(&self, dots: &mut BTreeSet<Dot<I>>) {
        self.meta.dots(dots)
    }

    fn join(&mut self, ctx: &CausalContext<I>, other: &Self, other_ctx: &CausalContext<I>) {
        self.meta.join(ctx, &other.meta, other_ctx);
        // TODO: Optimize book-keeping. This is brutally wasteful.
        let all_pos = self
            .meta
            .values()
            .flat_map(|x| x.values())
            .flat_map(|x| x.values())
            .collect::<BTreeSet<_>>();
        self.content.extend(other.content.clone());
        self.content.retain(|p, _| all_pos.contains(p));
    }

    fn unjoin(&self, diff: &DotSet<I>) -> Self {
        let meta = self.meta.unjoin(diff);
        let content = meta
            .values()
            .flat_map(|x| x.values())
            .flat_map(|x| x.values())
            .map(|p| (*p, self.content.get(p).cloned().expect("Positions in sync")))
            .collect();

        Self { meta, content }
    }
}

impl<'a, I: ReplicaId, V: Clone> CausalRef<'a, I, ORArray<I, V>> {
    pub fn insert(self, mut ix: usize, dot: Dot<I>, v: V) -> Causal<I, ORArray<I, V>> {
        ix = ix.min(self.store.content.len());
        let (left, right) = match ix.checked_sub(1) {
            Some(s) => {
                let mut iter = self.store.content.keys().skip(s);
                (iter.next(), iter.next())
            }
            None => {
                let mut iter = self.store.content.keys();
                (None, iter.next())
            }
        };
        let pos = PositionalIdentifier::between(left, right, dot.id);
        println!("pos: {:?}, left: {:?}, right: {:?}", pos, left, right);

        let mut delta = Causal::<_, ORArray<_, _>>::new();
        let mut inner: DotMap<Dot<I>, DotFun<I, PositionalIdentifier<I>>> = Default::default();
        inner.entry(dot).or_default().insert(dot, pos);
        delta.store.meta.insert(dot, inner);
        delta.store.content.insert(pos, (dot, v));
        delta.ctx.insert(dot);
        delta
    }

    pub fn delete(self, ix: usize, dot: Dot<I>) -> Causal<I, ORArray<I, V>> {
        let mut delta = Causal::<_, ORArray<_, _>>::new();
        if let Some(pos) = self.store.content.keys().nth(ix) {
            let (uid, _) = self.store.content.get(pos).expect("Positions in sync");
            let v = self.store.meta.get(uid).expect("Positions in sync");
            let mut dots = BTreeSet::new();
            v.dots(&mut dots);
            delta.ctx = dots.into_iter().chain(std::iter::once(dot)).collect();
        }
        delta
    }

    pub fn update(
        self,
        ix: usize,
        dot: Dot<I>,
        mut f: impl FnMut(&mut V),
    ) -> Causal<I, ORArray<I, V>> {
        let mut delta = Causal::<_, ORArray<_, _>>::new();
        if let Some((pos, (uid, v))) = self.store.content.iter().nth(ix) {
            let mut v = v.clone();
            f(&mut v);

            let mut dots = Default::default();
            self.store.meta.get(uid).unwrap().dots(&mut dots);
            delta.ctx = dots.into_iter().collect();

            let mut inner: DotMap<Dot<I>, DotFun<I, PositionalIdentifier<I>>> = Default::default();
            inner.entry(dot).or_default().insert(dot, *pos);
            delta.store.meta.insert(*uid, inner);
            delta.store.content.insert(*pos, (*uid, v));
        }
        delta.ctx.insert(dot);
        delta
    }
    pub fn r#move(self, ix: usize, mut to: usize, dot: Dot<I>) -> Causal<I, ORArray<I, V>> {
        let mut delta = Causal::<_, ORArray<_, _>>::new();
        if let Some((_, (uid, v))) = self.store.content.iter().nth(ix) {
            let new_pos = {
                to = to.min(self.store.content.len());
                let (left, right) = match to.checked_sub(1) {
                    Some(s) => {
                        let mut iter = self.store.content.keys().skip(s);
                        (iter.next(), iter.next())
                    }
                    None => {
                        let mut iter = self.store.content.keys();
                        (None, iter.next())
                    }
                };
                // TODO: maybe put the whole dot into the identifier and simplify the mapping?
                PositionalIdentifier::between(left, right, dot.id)
            };
            let mut dots = Default::default();
            let mut inner = self.store.meta.get(uid).unwrap().clone();
            inner.dots(&mut dots);
            delta.ctx = dots.into_iter().collect();
            let mut path: DotFun<I, PositionalIdentifier<I>> = Default::default();
            path.insert(dot, new_pos);
            for v in inner.values_mut() {
                *v = path.clone();
            }

            delta.store.meta.insert(*uid, inner);
            delta.store.content.insert(new_pos, (*uid, v.clone()));
        }
        delta.ctx.insert(dot);
        delta
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
/// Map providing observed-remove semantics. This is achieved by using a common causal context for
/// the whole map, to be used for all nested components, which is never reset.
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

impl<K: Key, V> CheckBottom for ORMap<K, V> {
    fn is_bottom(&self) -> bool {
        self.0.is_empty()
    }
}

impl<I: ReplicaId, K: Key, V: DotStore<I>> DotStore<I> for ORMap<K, V>
where
    Archived<K>: Ord,
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

impl<'a, I: ReplicaId, K: Key, V: DotStore<I>> CausalRef<'a, I, ORMap<K, V>> {
    pub fn apply<F, D>(self, k: K, mut f: F, mut default: D) -> Causal<I, ORMap<K, V>>
    where
        F: FnMut(CausalRef<'_, I, V>) -> Causal<I, V>,
        D: FnMut() -> V,
    {
        let inner_delta = if let Some(v) = self.store.get(&k) {
            f(self.map(v))
        } else {
            let v = default();
            let vref = self.map(&v);
            f(vref)
        };
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        delta.store.0.insert(k, inner_delta.store);
        delta.ctx = inner_delta.ctx;
        delta
    }

    pub fn remove<Q: ?Sized>(self, dot: Dot<I>, k: &Q) -> Causal<I, ORMap<K, V>>
    where
        K: Borrow<Q>,
        Q: Ord,
    {
        let mut delta = Causal::<_, ORMap<_, _>>::new();
        if let Some(v) = self.store.get(k) {
            let mut dots = BTreeSet::new();
            v.dots(&mut dots);
            delta.ctx = dots.into_iter().chain(std::iter::once(dot)).collect();
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
    use std::convert::TryInto;

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
        let mut reg: Causal<_, MVReg<_, u64>> = Causal::new();
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
        // set the flag to true
        let op1 = map.as_ref().apply(
            "flag".to_string(),
            |flag| flag.enable(Dot::new(0, 1)),
            Default::default,
        );
        map.join(&op1);
        assert!(map.store.get("flag").unwrap().value());
        // set it to false
        let op2 = map.as_ref().apply(
            "flag".to_string(),
            |flag| flag.disable(Dot::new(1, 1)),
            Default::default,
        );
        map.join(&op2);
        // flag will be gone
        assert!(map.store.get("flag").is_none());
        // set the flag to true again
        let op3 = map.as_ref().apply(
            "flag".to_string(),
            |flag| flag.enable(Dot::new(0, 3)),
            Default::default,
        );
        map.join(&op3);
        assert!(map.store.get("flag").unwrap().value());
        // remove the flag field entirely
        let op4 = map.as_ref().remove(Dot::new(0, 3), "flag");
        map.join(&op4);
        // flag will be gone
        assert!(map.store.get("flag").is_none());
    }

    #[test]
    fn test_or_map_string() {
        let mut map: Causal<_, ORMap<_, MVReg<_, u64>>> = Causal::new();
        let op1 = map.as_ref().apply(
            "flag".to_string(),
            |s| {
                s.write(
                    Dot::new(0, 1),
                    s.store.read().next().cloned().unwrap_or_default() + 1,
                )
            },
            Default::default,
        );
        map.join(&op1);
        assert!(map.store.get("flag").unwrap().read().next().unwrap() == &1);
        // set it to false
    }

    #[test]
    fn test_or_array_smoke() {
        let mut array: Causal<_, ORArray<_, usize>> = Default::default();
        let input = 0usize..100;
        let mut seq = 1;
        let mut deletions = vec![];
        for i in input.clone() {
            let dot = Dot::new(0, seq);
            seq += 1;
            let op = array.as_ref().insert(i, dot, i);
            array.join(&op);

            if i % 2 != 0 {
                let dot = Dot::new(0, seq);
                seq += 1;
                let op = array.as_ref().delete(i, dot);
                deletions.push(op);
            }
        }
        for o in deletions {
            array.join(&o);
        }
        let values = array.store.iter().cloned().collect::<Vec<_>>();
        assert_eq!(
            values,
            input.into_iter().filter(|x| x % 2 == 0).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_or_array_insert_between() {
        let mut array: Causal<_, ORArray<_, usize>> = Default::default();
        for i in 0usize..10 {
            let dot = Dot::new(0, (i + 1).try_into().unwrap());
            let op = array.as_ref().insert(i, dot, i);
            array.join(&op);
        }

        let dot = Dot::new(0, 11);
        let op = array.as_ref().insert(3, dot, 42);
        array.join(&op);

        let values = array.store.iter().cloned().collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 42, 3, 4, 5, 6, 7, 8, 9]);

        let dot = Dot::new(1, 0);
        let op = array.as_ref().insert(3, dot, 43);
        array.join(&op);

        let values = array.store.iter().cloned().collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 43, 42, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_or_array_update() {
        let mut array: Causal<_, ORArray<_, usize>> = Default::default();
        for i in 0usize..10 {
            let dot = Dot::new(0, (i + 1).try_into().unwrap());
            let op = array.as_ref().insert(i, dot, i);
            array.join(&op);
        }

        let dot = Dot::new(0, 11);
        let op = array.as_ref().update(3, dot, |i| *i += 1);
        array.join(&op);

        let values = array.store.iter().cloned().collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 4, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_or_array_move() {
        let mut array: Causal<_, ORArray<_, usize>> = Default::default();
        for i in 0usize..10 {
            let dot = Dot::new(0, (i + 1).try_into().unwrap());
            let op = array.as_ref().insert(i, dot, i);
            array.join(&op);
        }

        let dot = Dot::new(0, 11);
        let op = array.as_ref().r#move(3, 8, dot);
        array.join(&op);

        let values = array.store.iter().cloned().collect::<Vec<_>>();
        assert_eq!(values, vec![0, 1, 2, 4, 5, 6, 7, 3, 8, 9]);
    }
}

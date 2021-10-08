//! This module contains an efficient set of dots for use as both a dot store and a causal context
use bytecheck::CheckBytes;
use itertools::Itertools;
use range_collections::{range_set::ArchivedRangeSet, AbstractRangeSet, RangeSet, RangeSet2};
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::{btree_map, BTreeMap, BTreeSet},
    iter::FromIterator,
    ops::{Bound, Range},
};

/// A replica id 𝕀 is an opaque identifier for a replica
pub trait ReplicaId:
    Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self> + 'static
{
}

impl<T: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self> + 'static> ReplicaId for T {}

/// Dot is a version marker for a single replica.
#[derive(
    Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, CheckBytes, Deserialize, Serialize,
)]
#[archive(as = "Dot<I>")]
#[repr(C)]
pub struct Dot<I: ReplicaId> {
    /// The replica identifier.
    pub id: I,
    /// The current version of this replica.
    pub counter: u64,
}

impl<I: ReplicaId> Dot<I> {
    /// Build a Dot from an replica id and counter.
    pub fn new(id: I, counter: u64) -> Self {
        Self { id, counter }
    }

    /// Generate the successor of this dot
    pub fn inc(&mut self) -> Self {
        let res = *self;
        self.counter += 1;
        res
    }

    pub fn counter(&self) -> u64 {
        self.counter
    }
}

impl<I: ReplicaId + std::fmt::Display> std::fmt::Display for Dot<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}", self.id, self.counter)
    }
}

impl<I: ReplicaId + std::fmt::Debug> std::fmt::Debug for Dot<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({:?},{})", self.id, self.counter)
    }
}

impl<I: ReplicaId> From<(I, u64)> for Dot<I> {
    fn from(dot: (I, u64)) -> Self {
        Self {
            id: dot.0,
            counter: dot.1,
        }
    }
}

type EntryIter<'a, I, R> = Box<dyn Iterator<Item = (&'a I, &'a R)> + 'a>;

pub trait AbstractDotSet<I: ReplicaId> {
    fn entry(&self, id: &I) -> Option<&Self::RangeSet>;
    fn entries<'a>(&'a self) -> EntryIter<'a, I, Self::RangeSet>;
    fn to_dotset(&self) -> DotSet<I>;
    type RangeSet: AbstractRangeSet<u64>;

    fn contains(&self, dot: &Dot<I>) -> bool {
        self.entry(&dot.id)
            .map(|range| range.contains(&dot.counter))
            .unwrap_or_default()
    }

    fn iter(&self) -> Box<dyn Iterator<Item = Dot<I>> + '_> {
        Box::new(self.entries().flat_map(|(id, ranges)| {
            ranges.iter().flat_map(move |(from, to)| {
                elems(from, to)
                    .filter(|counter| *counter != 0)
                    .map(move |counter| Dot::new(*id, counter))
            })
        }))
    }

    /// Return the associated counter for this replica.
    ///
    /// The associated counter is the maximum counter value for the replica id.
    /// All replicas not in the set have an implied count of 0.
    ///
    /// maxᵢ(c) = max({ n | (i, n) ∈ c} ∪ { 0 })
    fn max(&self, id: &I) -> u64 {
        if let Some(r) = self.entry(id) {
            r.boundaries()
                .last()
                .map(|x| *x - 1)
                .expect("must not have explicit empty ranges")
        } else {
            0
        }
    }

    /// Returns the associated dot for this replica.
    fn dot(&self, id: I) -> Dot<I> {
        Dot::new(id, self.max(&id))
    }

    /// Returns the incremented dot for this replica.
    ///
    /// nextᵢ(c) = (i, maxᵢ(c) + 1)
    fn next(&self, id: I) -> Dot<I> {
        self.dot(id).inc()
    }

    fn is_empty(&self) -> bool {
        self.entries().next().is_none()
    }

    fn is_causal(&self) -> bool {
        self.entries().all(|(_, r)| {
            let b = r.boundaries();
            b.len() <= 2 && b[0] == 1
        })
    }

    /// Returns the intersection of two dot sets.
    fn intersection(&self, other: &impl AbstractDotSet<I>) -> DotSet<I> {
        DotSet(
            self.entries()
                .filter_map(|(k, vl)| {
                    other.entry(k).and_then(|vr| {
                        let r = vl.intersection(vr);
                        if !r.is_empty() {
                            Some((*k, r))
                        } else {
                            None
                        }
                    })
                })
                .collect(),
        )
    }

    /// Returns the difference of two dot sets.
    fn difference(&self, other: &impl AbstractDotSet<I>) -> DotSet<I> {
        DotSet(
            self.entries()
                .filter_map(|(k, vl)| {
                    if let Some(vr) = other.entry(k) {
                        let r: RangeSet2<u64> = vl.difference(vr);
                        if !r.is_empty() {
                            Some((*k, r))
                        } else {
                            None
                        }
                    } else {
                        Some((*k, vl.to_range_set()))
                    }
                })
                .collect(),
        )
    }
}

impl<I: ReplicaId> AbstractDotSet<I> for DotSet<I> {
    fn entry(&self, id: &I) -> Option<&RangeSet2<u64>> {
        self.0.get(&id)
    }

    type RangeSet = RangeSet2<u64>;

    fn to_dotset(&self) -> DotSet<I> {
        self.clone()
    }

    fn entries(&self) -> EntryIter<'_, I, Self::RangeSet> {
        Box::new(self.0.iter())
    }
}

impl<I: ReplicaId> AbstractDotSet<I> for ArchivedDotSet<I> {
    fn entry(&self, id: &I) -> Option<&ArchivedRangeSet<u64>> {
        self.0.get(&id)
    }

    type RangeSet = ArchivedRangeSet<u64>;

    fn to_dotset(&self) -> DotSet<I> {
        DotSet(
            self.entries()
                .map(|(i, r)| (*i, r.to_range_set()))
                .collect(),
        )
    }

    fn entries(&self) -> EntryIter<'_, I, Self::RangeSet> {
        Box::new(self.0.iter())
    }
}

/// An opaque set of dots.
///
/// Supports membership tests as well as the typical set operations union, intersection, difference.
/// For the purpose of testing, also supports enumerating all elements.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct DotSet<I>(BTreeMap<I, RangeSet2<u64>>);

impl<I: ReplicaId> FromIterator<Dot<I>> for DotSet<I> {
    fn from_iter<T: IntoIterator<Item = Dot<I>>>(iter: T) -> Self {
        let elems = iter
            .into_iter()
            .filter(|dot| dot.counter != 0)
            .group_by(|x| x.id)
            .into_iter()
            .map(|(id, elems)| {
                let entry: RangeSet2<u64> = elems.fold(RangeSet::empty(), |mut set, dot| {
                    let c = dot.counter();
                    set |= RangeSet::from(c..c + 1);
                    set
                });
                (id, entry)
            })
            .collect();
        Self(elems)
    }
}

impl<I: ReplicaId> Default for DotSet<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: ReplicaId> DotSet<I> {
    pub fn new() -> Self {
        Self(Default::default())
    }

    pub fn from_set(elems: BTreeSet<Dot<I>>) -> Self {
        elems.into_iter().collect()
    }

    /// creates a causal dot set from a map that contains the maximum dot for each replica (inclusive!)
    ///
    /// a maximum of 0 will be ignored
    pub fn from_map(x: BTreeMap<I, u64>) -> Self {
        Self(
            x.into_iter()
                .filter(|(_, max)| *max > 0)
                .map(|(i, max)| (i, RangeSet::from(1..max + 1)))
                .collect(),
        )
    }

    pub fn insert(&mut self, item: Dot<I>) {
        if item.counter == 0 {
            return;
        }
        let counter = item.counter();
        let range = RangeSet::from(counter..counter + 1);
        match self.0.get_mut(&item.id) {
            Some(existing) => {
                *existing |= range;
            }
            None => {
                self.0.insert(item.id, range);
            }
        }
    }

    /// Merges with the other dot set.
    pub fn union(&mut self, other: &impl AbstractDotSet<I>) {
        for (k, vr) in other.entries() {
            match self.0.entry(*k) {
                btree_map::Entry::Occupied(e) => {
                    e.into_mut().union_with(vr);
                }
                btree_map::Entry::Vacant(e) => {
                    e.insert(vr.to_range_set());
                }
            }
        }
    }
}

fn elems(lower: Bound<&u64>, upper: Bound<&u64>) -> Range<u64> {
    match (lower, upper) {
        (Bound::Included(lower), Bound::Excluded(upper)) => *lower..*upper,
        (Bound::Unbounded, Bound::Excluded(upper)) => 0..*upper,
        _ => panic!(),
    }
}

#[cfg(test)]
mod tests {
    use crate::dotset::AbstractDotSet;
    use crate::props::*;
    use crate::{Dot, DotSet, PeerId};
    use proptest::prelude::*;
    use std::collections::BTreeSet;

    /// convert a dotset into a std set for reference ops
    fn std_set(x: &DotSet) -> BTreeSet<Dot> {
        x.iter().collect()
    }

    /// convert an iterator into a dotset
    fn dot_set<'a>(x: impl IntoIterator<Item = &'a Dot>) -> DotSet {
        x.into_iter().cloned().collect()
    }

    fn from_tuples(x: impl IntoIterator<Item = (u8, u64)>) -> DotSet {
        x.into_iter()
            .map(|(i, c)| Dot::new(PeerId::new([i; 32]), c))
            .collect()
    }

    fn union(a: &DotSet, b: &DotSet) -> DotSet {
        let mut a = a.clone();
        a.union(b);
        a
    }

    fn intersect(a: &DotSet, b: &DotSet) -> DotSet {
        a.intersection(b)
    }

    fn difference(a: &DotSet, b: &DotSet) -> DotSet {
        a.difference(b)
    }

    #[test]
    fn is_causal() {
        let a = from_tuples([(1, 1), (1, 2), (1, 3)]);
        println!("{:?}", a);
        assert!(from_tuples([(1, 1), (1, 2), (1, 3)]).is_causal());
        assert!(!from_tuples([(1, 1), (1, 2), (1, 4)]).is_causal());
        assert!(!from_tuples([(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 4)]).is_causal());
    }

    proptest! {
        #[test]
        fn union_elements(s1 in arb_ctx(), s2 in arb_ctx()) {
            let reference = dot_set(std_set(&s1).union(&std_set(&s2)));
            let result = union(&s1, &s2);
            prop_assert_eq!(result, reference);
        }

        #[test]
        fn intersection_elements(s1 in arb_ctx(), s2 in arb_ctx()) {
            let reference = dot_set(std_set(&s1).intersection(&std_set(&s2)));
            let result = intersect(&s1, &s2);
            prop_assert_eq!(result, reference);
        }

        #[test]
        fn difference_elements(s1 in arb_ctx(), s2 in arb_ctx()) {
            let reference = dot_set(std_set(&s1).difference(&std_set(&s2)));
            let result = difference(&s1, &s2);
            prop_assert_eq!(result, reference);
        }

        #[test]
        fn insert_reference(s in arb_ctx(), e in arb_dot()) {
            let mut reference = std_set(&s);
            reference.insert(e);
            let reference = dot_set(reference.iter());
            let mut result = s;
            result.insert(e);
            prop_assert_eq!(result, reference);
        }

        #[test]
        fn union_idempotence(s1 in arb_ctx()) {
            prop_assert_eq!(union(&s1, &s1), s1);
        }

        #[test]
        fn union_commutativity(s1 in arb_ctx(), s2 in arb_ctx()) {
            prop_assert_eq!(union(&s1, &s2), union(&s2, &s1));
        }

        #[test]
        fn union_associativity(s1 in arb_ctx(), s2 in arb_ctx(), s3 in arb_ctx()) {
            prop_assert_eq!(union(&union(&s1, &s2), &s3), union(&s1, &union(&s2, &s3)));
        }

        #[test]
        fn intersect_idempotence(s1 in arb_ctx()) {
            prop_assert_eq!(intersect(&s1, &s1), s1);
        }

        #[test]
        fn intersect_commutativity(s1 in arb_ctx(), s2 in arb_ctx()) {
            prop_assert_eq!(intersect(&s1, &s2), intersect(&s2, &s1));
        }

        #[test]
        fn intersect_associativity(s1 in arb_ctx(), s2 in arb_ctx(), s3 in arb_ctx()) {
            prop_assert_eq!(intersect(&intersect(&s1, &s2), &s3), intersect(&s1, &intersect(&s2, &s3)));
        }

        #[test]
        fn union_intersect_dist(s1 in arb_ctx(), s2 in arb_ctx(), s3 in arb_ctx()) {
            prop_assert_eq!(union(&s1, &intersect(&s2, &s3)), intersect(&union(&s1, &s2), &union(&s1, &s3)));
        }

        #[test]
        fn intersect_union_dist(s1 in arb_ctx(), s2 in arb_ctx(), s3 in arb_ctx()) {
            prop_assert_eq!(intersect(&s1, &union(&s2, &s3)), union(&intersect(&s1, &s2), &intersect(&s1, &s3)));
        }

        #[test]
        fn union_difference_and_intersect(s1 in arb_ctx(), s2 in arb_ctx()) {
            prop_assert_eq!(union(&difference(&s1, &s2), &intersect(&s1, &s2)), s1);
        }
    }
}

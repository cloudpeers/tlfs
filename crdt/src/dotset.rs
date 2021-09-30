//! This module contains an efficient set of dots for use as both a dot store and a causal context
use bytecheck::CheckBytes;
use itertools::Itertools;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A replica id 𝕀 is an opaque identifier for a replica
pub trait ReplicaId: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self> {}

impl<T: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self>> ReplicaId for T {}

/// Dot is a version marker for a single replica.
#[derive(
    Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, CheckBytes, Deserialize, Serialize,
)]
#[archive(as = "Dot<I>")]
#[repr(C)]
pub struct Dot<I: ReplicaId> {
    /// The replica identifier.
    pub id: I,
    /// The current version of this replica.
    counter: u64,
}

impl<I: ReplicaId> Dot<I> {
    /// Build a Dot from an replica id and counter.
    pub fn new(id: I, counter: u64) -> Self {
        assert!(counter > 0);
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

/// An opaque set of dots.
///
/// Supports membership tests as well as the typical set operations union, intersection, difference.
/// For the purpose of testing, also supports enumerating all elements.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct DotSet<I: ReplicaId> {
    pub(crate) set: BTreeSet<Dot<I>>,
}

impl<I: ReplicaId> Default for DotSet<I> {
    fn default() -> Self {
        Self {
            set: Default::default(),
        }
    }
}

impl<I: ReplicaId> DotSet<I> {
    /// Returns a new instance.
    pub fn new(set: BTreeSet<Dot<I>>) -> Self {
        Self { set }
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// creates a causal dot set from a map that contains the maximum dot for each replica (inclusive!)
    ///
    /// a maximum of 0 will be ignored
    pub fn from_map(x: BTreeMap<I, u64>) -> Self {
        let mut cloud = BTreeSet::new();
        for (id, max) in x {
            for i in 1..=max {
                cloud.insert(Dot::new(id, i));
            }
        }
        Self { set: cloud }
    }

    /// Checks if the set is causal.
    ///
    /// a dot set is considered causal when there are only contiguous sequences of
    /// counters for each replica, starting with 1
    pub fn is_causal(&self) -> bool {
        self.set
            .iter()
            .group_by(|x| x.id)
            .into_iter()
            .all(|(_, iter)| is_causal_for_replica(iter))
    }

    /// Checks if the dot is contained in the set.
    pub fn contains(&self, dot: &Dot<I>) -> bool {
        self.set.contains(dot)
    }

    /// Adds a dot to the set.
    pub fn insert(&mut self, dot: Dot<I>) {
        self.set.insert(dot);
    }

    /// Return the associated counter for this replica.
    ///
    /// The associated counter is the maximum counter value for the replica id.
    /// All replicas not in the set have an implied count of 0.
    ///
    /// maxᵢ(c) = max({ n | (i, n) ∈ c} ∪ { 0 })
    pub fn max(&self, id: &I) -> u64 {
        // using last() relies on set being sorted, which is the case for a BTreeSet
        self.set
            .iter()
            .filter(|x| &x.id == id)
            .map(|x| x.counter)
            .last()
            .unwrap_or_default()
    }

    /// Returns the associated dot for this replica.
    pub fn dot(&self, id: I) -> Dot<I> {
        Dot::new(id, self.max(&id))
    }

    /// Returns the incremented dot for this replica.
    ///
    /// nextᵢ(c) = (i, maxᵢ(c) + 1)
    pub fn next(&self, id: I) -> Dot<I> {
        self.dot(id).inc()
    }

    /// Returns the intersection of two dot sets.
    pub fn intersection(&self, other: &Self) -> Self {
        Self {
            set: self.set.intersection(&other.set).cloned().collect(),
        }
    }

    /// Returns the difference of two dot sets.
    pub fn difference(&self, other: &DotSet<I>) -> DotSet<I> {
        let mut res = DotSet::default();
        for dot in &self.set {
            if !other.contains(dot) {
                res.set.insert(*dot);
            }
        }
        res
    }

    /// Merges with the other dot set.
    pub fn union(&mut self, other: &DotSet<I>) {
        for dot in &other.set {
            self.insert(*dot);
        }
    }

    /// Iterator over all dots in this dot set
    ///
    /// Note that this is mostly useful for testing, since iterating over all
    /// dots in a large dotset can be expensive.
    pub fn iter(&self) -> impl Iterator<Item = &Dot<I>> {
        self.set.iter()
    }
}

impl<I: ReplicaId> std::iter::FromIterator<Dot<I>> for DotSet<I> {
    fn from_iter<II: IntoIterator<Item = Dot<I>>>(iter: II) -> Self {
        let mut res = DotSet::default();
        for dot in iter {
            res.insert(dot);
        }
        res
    }
}

fn is_causal_for_replica<'a, I: ReplicaId + 'a>(
    mut iter: impl Iterator<Item = &'a Dot<I>>,
) -> bool {
    let mut prev = 0;
    iter.all(|e| {
        prev += 1;
        e.counter == prev
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::{props::*, Dot, DotSet};
    use proptest::prelude::*;

    /// convert a dotset into a std set for reference ops
    fn std_set(x: &DotSet<u8>) -> BTreeSet<Dot<u8>> {
        x.iter().cloned().collect()
    }

    /// convert an iterator into a dotset
    fn dot_set<'a>(x: impl IntoIterator<Item = &'a Dot<u8>>) -> DotSet<u8> {
        x.into_iter().cloned().collect()
    }

    fn from_tuples(x: impl IntoIterator<Item = (u8, u64)>) -> DotSet<u8> {
        x.into_iter().map(|(i, c)| Dot::new(i, c)).collect()
    }

    #[test]
    fn is_causal() {
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

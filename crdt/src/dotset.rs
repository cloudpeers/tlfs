//! This module contains an efficient set of dots for use as both a dot store and a causal context
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// A replica id 𝕀 is an opaque identifier for a replica
pub trait ReplicaId: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self> {}

impl<T: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self>> ReplicaId for T {}

/// Dot is a version marker for a single replica.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize)]
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
    pub fn inc(mut self) -> Self {
        self.counter += 1;
        self
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
    pub fn new(cloud: BTreeSet<Dot<I>>) -> Self {
        Self { set: cloud }
    }

    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    pub fn from_map(x: BTreeMap<I, u64>) -> Self {
        let mut cloud = BTreeSet::new();
        for (id, max) in x {
            for i in 1..=max {
                cloud.insert(Dot::new(id, i));
            }
        }
        Self { set: cloud }
    }

    /// Checks if the set is causally consistent.
    pub fn is_causal(&self) -> bool {
        // TODO!
        self.set.is_empty()
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
    /// All replicas not in the set have an implied count of 0.
    pub fn get(&self, id: &I) -> u64 {
        let dots = self.set.iter().filter(|x| &x.id == id).collect::<Vec<_>>();
        let mut prev = 0;
        for dot in dots {
            if dot.counter != prev + 1 {
                return prev;
            }
            prev = dot.counter;
        }
        prev
    }

    /// Returns the associated dot for this replica.
    pub fn dot(&self, id: I) -> Dot<I> {
        let counter = self.get(&id);
        Dot::new(id, counter)
    }

    /// Returns the incremented dot for this replica.
    pub fn inc(&self, id: I) -> Dot<I> {
        self.dot(id).inc()
    }

    /// Returns the intersection of two dot sets.
    pub fn intersect(&self, other: &Self) -> Self {
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

#[cfg(test)]
mod tests {
    use crate::props::*;
    use proptest::prelude::*;

    proptest! {
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

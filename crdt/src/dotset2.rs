use bytecheck::CheckBytes;
use itertools::Itertools;
use range_collections::RangeSet;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    iter::FromIterator,
    ops::{BitOrAssign, Bound, Range},
    ptr::NonNull,
};

use crate::{Dot, ReplicaId};

/// An opaque set of dots.
///
/// Supports membership tests as well as the typical set operations union, intersection, difference.
/// For the purpose of testing, also supports enumerating all elements.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct DotSet<I>(BTreeMap<I, RangeSet<u64>>);

impl<I: ReplicaId> FromIterator<Dot<I>> for DotSet<I> {
    fn from_iter<T: IntoIterator<Item = Dot<I>>>(iter: T) -> Self {
        let elems = iter
            .into_iter()
            .group_by(|x| x.id)
            .into_iter()
            .map(|(id, elems)| {
                let entry: RangeSet<u64> = elems.fold(RangeSet::empty(), |mut set, dot| {
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

impl<I: ReplicaId> DotSet<I> {
    /// creates a causal dot set from a map that contains the maximum dot for each replica (inclusive!)
    ///
    /// a maximum of 0 will be ignored
    pub fn from_map(x: BTreeMap<I, u64>) -> Self {
        Self(
            x.into_iter()
                .filter(|(_, max)| *max > 0)
                .map(|(i, max)| (i, RangeSet::from(1..max)))
                .collect(),
        )
    }

    pub fn contains(&self, dot: &Dot<I>) -> bool {
        let counter = dot.counter();
        self.0
            .get(&dot.id)
            .map(|range| range.contains(&counter))
            .unwrap_or_default()
    }

    pub fn iter(&self) -> impl Iterator<Item = Dot<I>> + '_ {
        self.0.iter().flat_map(|(id, ranges)| {
            ranges.iter().flat_map(move |(from, to)| {
                elems(from, to).map(move |counter| Dot::new(*id, counter))
            })
        })
    }

    pub fn insert(&mut self, item: Dot<I>) {
        let counter = item.counter();
        let range = RangeSet::from(counter..counter + 1);
        // todo: add entry API for VecMap?
        match self.0.get_mut(&item.id) {
            Some(existing) => {
                *existing |= range;
            }
            None => {
                self.0.insert(item.id, range);
            }
        }
    }

    /// Return the associated counter for this replica.
    ///
    /// The associated counter is the maximum counter value for the replica id.
    /// All replicas not in the set have an implied count of 0.
    ///
    /// maxᵢ(c) = max({ n | (i, n) ∈ c} ∪ { 0 })
    pub fn max(&self, id: &I) -> u64 {
        if let Some(r) = self.0.get(id) {
            r.boundaries()
                .last()
                .cloned()
                .expect("must not have explicit empty ranges")
        } else {
            0
        }
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the intersection of two dot sets.
    pub fn intersection(&self, other: &Self) -> Self {
        Self(
            self.0
                .iter()
                .filter_map(|(k, vl)| {
                    other.0.get(k).and_then(|vr| {
                        let r = vl & vr;
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
    pub fn difference(&self, other: &Self) -> Self {
        Self(
            self.0
                .iter()
                .filter_map(|(k, vl)| {
                    if let Some(vr) = other.0.get(k) {
                        let r = vl - vr;
                        if !r.is_empty() {
                            Some((*k, r))
                        } else {
                            None
                        }
                    } else {
                        Some((*k, vl.clone()))
                    }
                })
                .collect(),
        )
    }

    /// Merges with the other dot set.
    pub fn union(&mut self, other: &Self) {
        for (k, vr) in other.0.iter() {
            match self.0.entry(*k) {
                Entry::Occupied(e) => {
                    e.into_mut().bitor_assign(vr.clone());
                }
                Entry::Vacant(e) => {
                    e.insert(vr.clone());
                }
            }
        }
    }

    pub fn is_causal(&self) -> bool {
        self.0.iter().all(|(_, r)| {
            let b = r.boundaries();
            b.len() == 2 && b[0] == 1 && !r.contains(&0)
        })
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
    use std::{collections::BTreeSet, iter::FromIterator, ops::Range};

    use super::DotSet;
    use crate::{props::*, Dot};
    use proptest::prelude::*;

    pub fn arb_dot_in(counter: Range<u64>) -> impl Strategy<Value = Dot<u8>> {
        (0u8..5, counter).prop_map(|(a, c)| Dot::new(a, c))
    }

    pub fn arb_dot() -> impl Strategy<Value = Dot<u8>> {
        arb_dot_in(1u64..25)
    }

    pub fn arb_ctx() -> impl Strategy<Value = DotSet<u8>> {
        prop::collection::btree_set(arb_dot_in(1u64..5), 0..50)
            .prop_map(|dots| dots.into_iter().collect())
    }

    /// convert a dotset into a std set for reference ops
    fn std_set(x: &DotSet<u8>) -> BTreeSet<Dot<u8>> {
        x.iter().collect()
    }

    /// convert an iterator into a dotset
    fn dot_set<'a>(x: impl IntoIterator<Item = &'a Dot<u8>>) -> DotSet<u8> {
        x.into_iter().cloned().collect()
    }

    fn from_tuples(x: impl IntoIterator<Item = (u8, u64)>) -> DotSet<u8> {
        x.into_iter().map(|(i, c)| Dot::new(i, c)).collect()
    }

    pub fn union(a: &DotSet<u8>, b: &DotSet<u8>) -> DotSet<u8> {
        let mut a = a.clone();
        a.union(b);
        a
    }

    pub fn intersect(a: &DotSet<u8>, b: &DotSet<u8>) -> DotSet<u8> {
        a.intersection(b)
    }

    pub fn difference(a: &DotSet<u8>, b: &DotSet<u8>) -> DotSet<u8> {
        a.difference(b)
    }

    #[test]
    fn is_causal() {
        assert!(from_tuples([(1, 1), (1, 2), (1, 3)]).is_causal());
        assert!(!from_tuples([(1, 1), (1, 2), (1, 4)]).is_causal());
        assert!(!from_tuples([(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 4)]).is_causal());
    }

    #[test]
    fn difference_elements_1() {
        let l = DotSet::<u8>::from_iter([Dot::new(2, 1), Dot::new(2, 2)]);
        let r = DotSet::<u8>::from_iter([]);
        let res = difference(&l, &r);
        println!("{:?} {:?} {:?}", l, r, res);
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

            if reference != result {
                println!("XXXX {:?} {:?} {:?} {:?}", s1, s2, reference, result);
            }
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

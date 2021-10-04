
use std::{collections::BTreeMap, iter::FromIterator, ops::{Bound, Range}};

use itertools::Itertools;
use vec_collections::VecMap1;
use range_collections::RangeSet;

use crate::{Dot, ReplicaId};

struct DotSet1<I>(VecMap1<I, RangeSet<u64>>);

impl<I: ReplicaId> FromIterator<Dot<I>> for DotSet1<I> {
    fn from_iter<T: IntoIterator<Item = Dot<I>>>(iter: T) -> Self {
        let elems = iter.into_iter().group_by(|x| x.id)
            .into_iter().map(|(id, elems)| {
                let entry: RangeSet<u64> = elems.fold(RangeSet::empty(), |mut set, dot| {
                    let c =  dot.counter();
                    set |= RangeSet::from(c..c+1);
                    set
                });
                (id, entry)
            }).collect();
        Self(elems)
    }
}

impl<I: ReplicaId> DotSet1<I> {

    /// creates a causal dot set from a map that contains the maximum dot for each replica (inclusive!)
    ///
    /// a maximum of 0 will be ignored
    pub fn from_map(x: BTreeMap<I, u64>) -> Self {
        Self(x.into_iter().filter(|(_, max)| *max > 0).map(|(i, max)| (i, RangeSet::from(1..max))).collect())
    }

    pub fn contains(&self, dot: &Dot<I>) -> bool {
        let counter = dot.counter();
        self.0.get(&dot.id).map(|range| {
            range.contains(&counter)
        }).unwrap_or_default()
    }

    pub fn iter(&self) -> impl Iterator<Item = Dot<I>> + '_ {
        self.0.iter().flat_map(|(id, ranges)|
        ranges.iter().flat_map(move |(from, to)| {
                elems(from, to).map(move |counter| Dot::new(*id, counter))
            })
        )
    }

    pub fn insert(&mut self, item: Dot<I>) {
        let counter = item.counter();
        let range = RangeSet::from(counter .. counter + 1);
        // todo: add entry API for VecMap?
        match self.0.get_mut(&item.id) {
            Some(existing) => {
                *existing |= range;
            },
            None => {
                self.0.insert(item.id, range);
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the intersection of two dot sets.
    pub fn intersection(&self, other: &Self) -> Self {
        Self(self.0.inner_join(&other.0, |id, l,r| {
            let result = l & r;
            if result.is_empty() {
                None
            } else {
                Some(result)
            }
         }))
    }

    /// Returns the difference of two dot sets.
    pub fn difference(&self, other: &Self) -> Self {
        Self(self.0.left_join::<_, _, _, [_; 2]>(&other.0, |_, l, r| {
            match r {
                Some(r) => {
                    let mut l = l.clone();
                    l -= r.clone();
                    if l.is_empty() {
                        None
                    } else {
                        Some(l)
                    }                    
                }
                None => Some(l.clone()),
            }
        }))
    }

    /// Merges with the other dot set.
    pub fn union(&mut self, other: &Self) {
        self.0.combine_with(other.0.clone(), |mut l, r| {
            l |= r;
            l
        })
    }
}

fn elems(lower: Bound<&u64>, upper: Bound<&u64>) -> Range<u64> {
    match (lower, upper) {
        (Bound::Included(lower), Bound::Excluded(upper)) => *lower..*upper,
        (Bound::Unbounded, Bound::Excluded(upper)) => 0..*upper,
        _ => panic!()
    }
}
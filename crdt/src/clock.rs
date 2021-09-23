//! This module contains a generic vector clock implementation.
use crate::Dot;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Clock<A: Ord> {
    pub(crate) clock: BTreeMap<A, u64>,
    pub(crate) cloud: BTreeSet<Dot<A>>,
}

impl<A: Ord> Default for Clock<A> {
    fn default() -> Self {
        Self {
            clock: Default::default(),
            cloud: Default::default(),
        }
    }
}

impl<A: Ord> Clock<A> {
    /// Returns a new instance.
    pub fn new() -> Self {
        Self::default()
    }

    /// Checks if the clock is causally consistent.
    pub fn is_causal(&self) -> bool {
        self.cloud.is_empty()
    }

    /// Checks if the dot is contained in the clock.
    pub fn contains(&self, dot: &Dot<A>) -> bool {
        if self.get(&dot.actor) >= dot.counter {
            return true;
        }
        self.cloud.contains(dot)
    }

    /// Adds a dot to the clock.
    pub fn insert(&mut self, dot: Dot<A>) {
        let current = self.get(&dot.actor);
        let next = current + 1;
        if dot.counter == next {
            self.clock.insert(dot.actor, dot.counter);
            self.compact();
        } else if dot.counter > current {
            self.cloud.insert(dot);
        }
    }

    /// Return the associated counter for this actor.
    /// All actors not in the clock have an implied count of 0.
    pub fn get(&self, actor: &A) -> u64 {
        self.clock.get(actor).copied().unwrap_or_default()
    }

    /// Returns the associated dot for this actor.
    pub fn dot(&self, actor: A) -> Dot<A> {
        let counter = self.get(&actor);
        Dot::new(actor, counter)
    }

    /// Returns the incremented dot for this actor.
    pub fn inc(&self, actor: A) -> Dot<A> {
        self.dot(actor).inc()
    }

    /// Returns the intersection of two clocks.
    pub fn intersect(&self, other: &Clock<A>) -> Clock<A>
    where
        A: Clone,
    {
        let mut clock = Clock::new();
        for (actor, counter) in &self.clock {
            let counter = std::cmp::min(*counter, other.get(actor));
            if counter > 0 {
                clock.clock.insert(actor.clone(), counter);
            }
        }
        clock.cloud = self.cloud.intersection(&other.cloud).cloned().collect();
        clock
    }

    /// Returns the difference of two clocks.
    pub fn difference(&self, other: &Clock<A>) -> Clock<A>
    where
        A: Clone,
    {
        let mut clock = Clock::new();
        for (actor, counter) in &self.clock {
            if *counter > other.get(actor) {
                clock.clock.insert(actor.clone(), *counter);
            }
        }
        for dot in &self.cloud {
            if !other.contains(dot) {
                clock.cloud.insert(dot.clone());
            }
        }
        clock
    }

    /// Merges with the other clock.
    pub fn union(&mut self, other: &Clock<A>)
    where
        A: Clone,
    {
        for (actor, counter) in &other.clock {
            if *counter > self.get(actor) {
                self.clock.insert(actor.clone(), *counter);
            }
        }
        self.compact();
        for dot in &other.cloud {
            self.insert(dot.clone());
        }
    }

    fn compact(&mut self) {
        let clock = &mut self.clock;
        loop {
            let mut progress = false;
            self.cloud.retain(|dot| {
                if let Some(counter) = clock.get_mut(&dot.actor) {
                    let ncounter = *counter + 1;
                    if dot.counter == ncounter {
                        *counter = ncounter;
                        progress = true;
                    }
                    dot.counter > ncounter
                } else {
                    true
                }
            });
            if !progress {
                break;
            }
        }
    }
}

impl<A: Ord> std::iter::FromIterator<Dot<A>> for Clock<A> {
    fn from_iter<I: IntoIterator<Item = Dot<A>>>(iter: I) -> Self {
        let mut clock = Clock::new();
        for dot in iter {
            clock.insert(dot);
        }
        clock
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn arb_clock() -> impl Strategy<Value = Clock<u8>> {
        (
            prop::collection::btree_map(0u8..5, 1u64..5, 0..5),
            prop::collection::btree_set((0u8..5, 6u64..10).prop_map(|(a, c)| Dot::new(a, c)), 0..5),
        )
            .prop_map(|(clock, cloud)| Clock { clock, cloud })
    }

    fn union(a: &Clock<u8>, b: &Clock<u8>) -> Clock<u8> {
        let mut a = a.clone();
        a.union(b);
        a
    }

    fn intersect(a: &Clock<u8>, b: &Clock<u8>) -> Clock<u8> {
        a.intersect(b)
    }

    fn difference(a: &Clock<u8>, b: &Clock<u8>) -> Clock<u8> {
        a.difference(b)
    }

    proptest! {
        #[test]
        fn union_idempotence(s1 in arb_clock()) {
            prop_assert_eq!(union(&s1, &s1), s1);
        }

        #[test]
        fn union_commutativity(s1 in arb_clock(), s2 in arb_clock()) {
            prop_assert_eq!(union(&s1, &s2), union(&s2, &s1));
        }

        #[test]
        fn union_associativity(s1 in arb_clock(), s2 in arb_clock(), s3 in arb_clock()) {
            prop_assert_eq!(union(&union(&s1, &s2), &s3), union(&s1, &union(&s2, &s3)));
        }

        #[test]
        fn intersect_idempotence(s1 in arb_clock()) {
            prop_assert_eq!(intersect(&s1, &s1), s1);
        }

        #[test]
        fn intersect_commutativity(s1 in arb_clock(), s2 in arb_clock()) {
            prop_assert_eq!(intersect(&s1, &s2), intersect(&s2, &s1));
        }

        #[test]
        fn intersect_associativity(s1 in arb_clock(), s2 in arb_clock(), s3 in arb_clock()) {
            prop_assert_eq!(intersect(&intersect(&s1, &s2), &s3), intersect(&s1, &intersect(&s2, &s3)));
        }

        #[test]
        fn union_intersect_dist(s1 in arb_clock(), s2 in arb_clock(), s3 in arb_clock()) {
            prop_assert_eq!(union(&s1, &intersect(&s2, &s3)), intersect(&union(&s1, &s2), &union(&s1, &s3)));
        }

        #[test]
        fn intersect_union_dist(s1 in arb_clock(), s2 in arb_clock(), s3 in arb_clock()) {
            prop_assert_eq!(intersect(&s1, &union(&s2, &s3)), union(&intersect(&s1, &s2), &intersect(&s1, &s3)));
        }

        #[test]
        fn union_difference_and_intersect(s1 in arb_clock(), s2 in arb_clock()) {
            prop_assert_eq!(union(&difference(&s1, &s2), &intersect(&s1, &s2)), s1);
        }
    }
}

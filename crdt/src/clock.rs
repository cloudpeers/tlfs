//! This module contains a generic vector clock implementation.
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub trait Actor: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self> {}

impl<T: Copy + std::fmt::Debug + Ord + rkyv::Archive<Archived = Self>> Actor for T {}

/// Dot is a version marker for a single actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive(as = "Dot<A>")]
#[repr(C)]
pub struct Dot<A: Actor> {
    /// The actor identifier.
    pub actor: A,
    /// The current version of this actor.
    pub counter: u64,
}

impl<A: Actor> Dot<A> {
    /// Build a Dot from an actor and counter.
    pub fn new(actor: A, counter: u64) -> Self {
        Self { actor, counter }
    }

    /// Generate the successor of this dot
    pub fn inc(mut self) -> Self {
        self.counter += 1;
        self
    }
}

impl<A: Actor + std::fmt::Display> std::fmt::Display for Dot<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}", self.actor, self.counter)
    }
}

impl<A: Actor> From<(A, u64)> for Dot<A> {
    fn from(dot: (A, u64)) -> Self {
        Self {
            actor: dot.0,
            counter: dot.1,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[repr(C)]
pub struct Clock<A: Actor> {
    pub(crate) clock: BTreeMap<A, u64>,
    pub(crate) cloud: BTreeSet<Dot<A>>,
}

impl<A: Actor> Default for Clock<A> {
    fn default() -> Self {
        Self {
            clock: Default::default(),
            cloud: Default::default(),
        }
    }
}

impl<A: Actor> Clock<A> {
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
                clock.clock.insert(*actor, counter);
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
                clock.clock.insert(*actor, *counter);
            }
        }
        for dot in &self.cloud {
            if !other.contains(dot) {
                clock.cloud.insert(*dot);
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
                self.clock.insert(*actor, *counter);
            }
        }
        self.compact();
        for dot in &other.cloud {
            self.insert(*dot);
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

impl<A: Actor> std::iter::FromIterator<Dot<A>> for Clock<A> {
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
    use crate::props::*;
    use proptest::prelude::*;

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

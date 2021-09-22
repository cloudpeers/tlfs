//! This module contains a generic Vector Clock implementation.
//!
//! # Examples
//!
//! ```
//! use crdt::{Dot, Clock};
//!
//! let mut a = Clock::new();
//! let mut b = Clock::new();
//! a.apply(Dot::new("A", 2));
//! b.apply(Dot::new("A", 1));
//! assert!(a > b);
//! ```

use core::cmp::{self, Ordering};
use core::fmt::{self, Debug, Display};
use std::collections::{btree_map, BTreeMap};

use crate::dot::Dot;

/// A `Clock` is a standard vector clock.
/// It contains a set of "actors" and associated counters.
/// When a particular actor witnesses a mutation, their associated
/// counter in a `Clock` is incremented. `Clock` is typically used
/// as metadata for associated application data, rather than as the
/// container for application data. `Clock` just tracks causality.
/// It can tell you if something causally descends something else,
/// or if different replicas are "concurrent" (were mutated in
/// isolation, and need to be resolved externally).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Clock<A: Ord> {
    /// dots is the mapping from actors to their associated counters
    pub dots: BTreeMap<A, u64>,
}

impl<A: Ord> Default for Clock<A> {
    fn default() -> Self {
        Self {
            dots: BTreeMap::new(),
        }
    }
}

impl<A: Ord> PartialOrd for Clock<A> {
    fn partial_cmp(&self, other: &Clock<A>) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else if other.dots.iter().all(|(w, c)| self.get(w) >= *c) {
            Some(Ordering::Greater)
        } else if self.dots.iter().all(|(w, c)| other.get(w) >= *c) {
            Some(Ordering::Less)
        } else {
            None
        }
    }
}

impl<A: Ord + Display> Display for Clock<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<")?;
        for (i, (actor, count)) in self.dots.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}:{}", actor, count)?;
        }
        write!(f, ">")
    }
}

impl<A: Ord> Clock<A> {
    /// Returns a new `Clock` instance.
    pub fn new() -> Self {
        Default::default()
    }

    /// Generate Op to increment an actor's counter.
    ///
    /// # Examples
    /// ```
    /// use crdt::Clock;
    /// let mut a = Clock::new();
    ///
    /// // `a.inc()` does not mutate the vclock!
    /// let op = a.inc("A");
    /// assert_eq!(a, Clock::new());
    ///
    /// // we must apply the op to the Clock to have
    /// // its edit take effect.
    /// a.apply(op.clone());
    /// assert_eq!(a.get(&"A"), 1);
    ///
    /// // Op's can be replicated to another node and
    /// // applied to the local state there.
    /// let mut other_node = Clock::new();
    /// other_node.apply(op);
    /// assert_eq!(other_node.get(&"A"), 1);
    /// ```
    pub fn inc(&self, actor: A) -> Dot<A>
    where
        A: Clone,
    {
        self.dot(actor).inc()
    }

    /// Return the associated counter for this actor.
    /// All actors not in the vclock have an implied count of 0
    pub fn get(&self, actor: &A) -> u64 {
        self.dots.get(actor).copied().unwrap_or(0)
    }

    /// Return the Dot for a given actor
    pub fn dot(&self, actor: A) -> Dot<A> {
        let counter = self.get(&actor);
        Dot::new(actor, counter)
    }

    /// True if two vector clocks have diverged.
    ///
    /// # Examples
    /// ```
    /// use crdt::Clock;
    /// let (mut a, mut b) = (Clock::new(), Clock::new());
    /// a.apply(a.inc("A"));
    /// b.apply(b.inc("B"));
    /// assert!(a.concurrent(&b));
    /// ```
    pub fn concurrent(&self, other: &Clock<A>) -> bool {
        self.partial_cmp(other).is_none()
    }

    /// Returns `true` if this vector clock contains nothing.
    pub fn is_empty(&self) -> bool {
        self.dots.is_empty()
    }

    /// Returns the common dots for two `Clock` instances.
    pub fn intersect(&self, other: &Clock<A>) -> Clock<A>
    where
        A: Clone,
    {
        let mut dots = BTreeMap::new();
        for (actor, counter) in self.dots.iter() {
            if let Some(other_counter) = other.dots.get(actor) {
                dots.insert(actor.clone(), cmp::min(*counter, *other_counter));
            }
        }
        Self { dots }
    }

    /// Returns the difference for the two `Clock` instances.
    pub fn difference(&self, other: &Clock<A>) -> Clock<A>
    where
        A: Clone,
    {
        let mut dots = BTreeMap::new();
        for (actor, counter) in &self.dots {
            if *counter > other.get(actor) {
                dots.insert(actor.clone(), *counter);
            }
        }
        Self { dots }
    }

    /// Returns the union for two `Clock` instances.
    pub fn union(&mut self, other: &Clock<A>)
    where
        A: Clone,
    {
        for dot in other.iter() {
            self.apply(dot);
        }
    }

    /// Returns an iterator over the dots in this vclock
    pub fn iter(&self) -> impl Iterator<Item = Dot<A>> + '_
    where
        A: Clone,
    {
        self.dots.iter().map(|(a, c)| Dot {
            actor: a.clone(),
            counter: *c,
        })
    }

    /// Monotonically adds the given actor version to
    /// this VClock.
    ///
    /// # Examples
    /// ```
    /// use crdt::{Clock, Dot};
    /// let mut v = Clock::new();
    ///
    /// v.apply(Dot::new("A", 2));
    ///
    /// // now all dots applied to `v` from actor `A` where
    /// // the counter is not bigger than 2 are nops.
    /// v.apply(Dot::new("A", 0));
    /// assert_eq!(v.get(&"A"), 2);
    /// ```
    pub fn apply(&mut self, dot: Dot<A>) {
        if self.get(&dot.actor) < dot.counter {
            self.dots.insert(dot.actor, dot.counter);
        }
    }
}

/// Generated from calls to Clock::into_iter()
pub struct IntoIter<A: Ord> {
    btree_iter: btree_map::IntoIter<A, u64>,
}

impl<A: Ord> std::iter::Iterator for IntoIter<A> {
    type Item = Dot<A>;

    fn next(&mut self) -> Option<Dot<A>> {
        self.btree_iter
            .next()
            .map(|(actor, counter)| Dot::new(actor, counter))
    }
}

impl<A: Ord> std::iter::IntoIterator for Clock<A> {
    type Item = Dot<A>;
    type IntoIter = IntoIter<A>;

    /// Consumes the vclock and returns an iterator over dots in the clock
    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            btree_iter: self.dots.into_iter(),
        }
    }
}

impl<A: Ord + Clone + Debug> std::iter::FromIterator<Dot<A>> for Clock<A> {
    fn from_iter<I: IntoIterator<Item = Dot<A>>>(iter: I) -> Self {
        let mut clock = Clock::default();

        for dot in iter {
            clock.apply(dot);
        }

        clock
    }
}

impl<A: Ord + Clone + Debug> From<Dot<A>> for Clock<A> {
    fn from(dot: Dot<A>) -> Self {
        let mut clock = Clock::default();
        clock.apply(dot);
        clock
    }
}

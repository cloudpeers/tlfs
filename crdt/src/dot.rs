///! Dot
use std::cmp::{Ordering, PartialOrd};
use std::fmt;
use std::hash::{Hash, Hasher};

/// Dot is a version marker for a single actor.
pub struct Dot<A> {
    /// The actor identifier.
    pub actor: A,
    /// The current version of this actor.
    pub counter: u64,
}

impl<A> Dot<A> {
    /// Build a Dot from an actor and counter.
    pub fn new(actor: A, counter: u64) -> Self {
        Self { actor, counter }
    }
}

impl<A: Clone> Dot<A> {
    /// Generate the successor of this dot
    pub fn inc(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            counter: self.counter + 1,
        }
    }
}

impl<A: Clone> Clone for Dot<A> {
    fn clone(&self) -> Self {
        Self {
            actor: self.actor.clone(),
            counter: self.counter,
        }
    }
}

impl<A: Copy> Copy for Dot<A> {}

impl<A: PartialEq> PartialEq for Dot<A> {
    fn eq(&self, other: &Self) -> bool {
        self.actor == other.actor && self.counter == other.counter
    }
}

impl<A: Eq> Eq for Dot<A> {}

impl<A: Hash> Hash for Dot<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.actor.hash(state);
        self.counter.hash(state);
    }
}

impl<A: Ord> PartialOrd for Dot<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<A: Ord> Ord for Dot<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        let cmp = self.actor.cmp(&other.actor);
        if cmp == Ordering::Equal {
            self.counter.cmp(&other.counter)
        } else {
            cmp
        }
    }
}

impl<A: fmt::Debug> fmt::Debug for Dot<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}.{:?}", self.actor, self.counter)
    }
}

impl<A: fmt::Display> fmt::Display for Dot<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.actor, self.counter)
    }
}

impl<A> From<(A, u64)> for Dot<A> {
    fn from(dot: (A, u64)) -> Self {
        Self {
            actor: dot.0,
            counter: dot.1,
        }
    }
}

//! Identifiers in totally ordered set. It's always possible to construct a new identifier between
//! two existing identifiers:
//!
//! Given Some identifiers `x`, and `z`, where `x != z`, then it's possible to construct an
//! identifier y with `x < y < z` or `x > y > z`.
use std::cmp::Ordering;

use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use crate::{Dot, Lattice, ReplicaId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes, Ord, PartialEq, Eq, PartialOrd))]
#[repr(C)]
pub struct Position {
    mantissa: u128,
    exponent: u8,
}

impl Position {
    fn new(mantissa: u128, exponent: u8) -> Self {
        Self { mantissa, exponent }
    }
    fn zero() -> Self {
        Self {
            mantissa: 0,
            exponent: 0,
        }
    }

    fn value(&self, exp: u8) -> u128 {
        if exp < self.exponent {
            self.mantissa / 2u128.pow((self.exponent - exp) as u32)
        } else {
            self.mantissa * 2u128.pow((exp - self.exponent) as u32)
        }
    }
}

impl PartialOrd for Position {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Position {
    fn cmp(&self, other: &Self) -> Ordering {
        let exp = self.exponent.max(other.exponent);
        self.value(exp).cmp(&other.value(exp))
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Archive, CheckBytes, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes, Ord, PartialEq, Eq, PartialOrd))]
#[repr(C)]
pub struct PositionalIdentifier<I: ReplicaId>
where
    Archived<Dot<I>>: Ord,
{
    val: Position,
    id: Dot<I>,
}

impl<I: ReplicaId> Lattice for PositionalIdentifier<I> {
    // TODO: The Lattice bound should probably be removed form the DotFun
    fn join(&mut self, other: &Self) {
        unreachable!("{:?} {:?}", self, other)
    }
}

impl<I: ReplicaId + Ord> PartialOrd for PositionalIdentifier<I> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl<I: ReplicaId + Ord> Ord for PositionalIdentifier<I> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.val.cmp(&other.val) {
            Ordering::Equal => self.id.cmp(&other.id),
            ord => ord,
        }
    }
}

impl<I: ReplicaId> PositionalIdentifier<I> {
    pub fn id(&self) -> Dot<I> {
        self.id
    }
    pub fn between(left: Option<&Self>, right: Option<&Self>, id: Dot<I>) -> Self {
        let val = between(left.map(|x| x.val).as_ref(), right.map(|x| x.val).as_ref());
        Self { val, id }
    }
}

fn between(left: Option<&Position>, right: Option<&Position>) -> Position {
    match (left, right) {
        (None, None) => Position::zero(),
        (None, Some(right)) => {
            let mut r = *right;
            r.mantissa -= 1;
            r
        }
        (Some(left), None) => {
            let mut r = *left;
            r.mantissa += 1;
            r
        }

        (Some(left), Some(right)) => {
            let mut r = if left.mantissa == 0 {
                *right
            } else if right.mantissa == 0 {
                *left
            } else {
                match left.exponent.cmp(&right.exponent) {
                    Ordering::Greater => Position::new(
                        left.mantissa
                            + right.mantissa * 2u128.pow((left.exponent - right.exponent).into()),
                        left.exponent,
                    ),
                    Ordering::Less => Position::new(
                        right.mantissa
                            + left.mantissa * 2u128.pow((right.exponent - left.exponent).into()),
                        right.exponent,
                    ),
                    Ordering::Equal => Position::new(left.mantissa + right.mantissa, left.exponent),
                }
            };
            r.exponent += 1;
            r
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn value() {
        assert!(Position::new(5, 1) > Position::new(2, 0));
        assert!(Position::new(5, 1) == Position::new(5, 1));
        assert!(Position::new(5, 1) < Position::new(9, 0));

        assert_eq!(
            between(Some(&Position::new(2, 0)), Some(&Position::new(5, 1))),
            Position::new(9, 2)
        );
    }
}

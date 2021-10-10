use std::cmp::Ordering;

use bytecheck::CheckBytes;
use rkyv::{Archive, Archived, Deserialize, Serialize};

use crate::{Dot, Lattice, ReplicaId};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(CheckBytes, Ord, PartialEq, Eq, PartialOrd))]
#[repr(C)]
pub struct Number {
    mantissa: u128,
    exponent: u8,
}

impl Number {
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

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Number {
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
    val: Number,
    id: Dot<I>,
}

impl<I: ReplicaId> Lattice for PositionalIdentifier<I> {
    fn join(&mut self, other: &Self) {
        panic!("{:?} {:?}", self, other)
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

fn between(left: Option<&Number>, right: Option<&Number>) -> Number {
    match (left, right) {
        (None, None) => Number::zero(),
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
                    Ordering::Greater => Number::new(
                        left.mantissa
                            + right.mantissa * 2u128.pow((left.exponent - right.exponent).into()),
                        left.exponent,
                    ),
                    Ordering::Less => Number::new(
                        right.mantissa
                            + left.mantissa * 2u128.pow((right.exponent - left.exponent).into()),
                        right.exponent,
                    ),
                    Ordering::Equal => Number::new(left.mantissa + right.mantissa, left.exponent),
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
        assert!(Number::new(5, 1) > Number::new(2, 0));
        assert!(Number::new(5, 1) == Number::new(5, 1));
        assert!(Number::new(5, 1) < Number::new(9, 0));

        assert_eq!(
            between(Some(&Number::new(2, 0)), Some(&Number::new(5, 1))),
            Number::new(9, 2)
        );
    }
}

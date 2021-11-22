use core::{fmt, ops::Index};
use smallvec::{smallvec, SmallVec};

/// A binary fraction type. Can encode any value in the interval [0..1) with arbitary precision.
///
/// trailing zeros are not stored to make it canonical.
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct Fraction(SmallVec<[u8; 8]>);

const DIGIT_BITS: u32 = 7;
const DIGIT_MASK: usize = (1 << DIGIT_BITS) - 1;
const DIGIT_MASK_U8: u8 = (1 << DIGIT_BITS) - 1;

impl AsRef<[u8]> for Fraction {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl Index<usize> for Fraction {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        self.0.get(index).unwrap_or(&0u8)
    }
}

impl fmt::Debug for Fraction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fraction({})", self)
    }
}

impl fmt::Display for Fraction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.0.is_empty() {
            write!(f, "0x0.{}", hex::encode(&self.0))
        } else {
            write!(f, "0x0")
        }
    }
}

impl Fraction {
    pub fn new(data: SmallVec<[u8; 8]>) -> Self {
        Self(data)
    }

    /// shifts the digits and adds the continue bits
    pub fn from_digits(mut digits: SmallVec<[u8; 8]>) -> Self {
        assert!(digits.iter().all(|x| *x < 0x80));
        // canonicalize
        if digits.is_empty() {
            // make sure length is at least 1
            digits.push(0);
        } else {
            // remove trailing 0 digits, except one
            while digits.len() > 1 && digits.last() == Some(&0u8) {
                digits.pop();
            }
        }
        for i in 0..digits.len() {
            digits[i] <<= 1;
            if i < digits.len() - 1 {
                digits[i] |= 1;
            }
        }
        Self(digits)
    }

    /// Compute the midpoint between two numbers
    pub fn mid(&self, that: &Self) -> Self {
        let n = self.0.len().max(that.0.len());
        let mut digits = SmallVec::with_capacity(n);
        // calculate self + that. Highest bit will be in carry.
        let mut carry = 0usize;
        for i in (0..n).rev() {
            carry += self.digit(i);
            carry += that.digit(i);
            digits.push((carry & DIGIT_MASK) as u8);
            carry >>= DIGIT_BITS;
        }
        digits.reverse();
        // divide by 2, including carry. Lowest bit will be in carry.
        for i in 0..n {
            let r = digits[i];
            digits[i] = (r >> 1) + ((carry as u8) << (DIGIT_BITS - 1));
            carry = (r & 1) as usize;
        }
        // if we have a carry, we need to extend the result
        if carry != 0 {
            digits.push(0x40);
        }
        Self::from_digits(digits)
    }

    /// Compute a number that is larger than the current number by some small amount
    pub fn succ(&self) -> Self {
        let mut digits = self.digits();
        // if we would wraparound when adding, add some more fractional digits
        if digits.iter().all(|x| *x == DIGIT_MASK_U8) {
            let n = digits.len().max(1);
            digits.extend((0..n).map(|_| 0u8));
        }
        // add 1 to the lowest current fractional digit
        for byte in digits.iter_mut().rev() {
            // add 1
            *byte = (*byte + 1) & DIGIT_MASK_U8;
            if *byte != 0 {
                break;
            }
        }
        Self::from_digits(digits)
    }
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
    pub fn zero() -> Fraction {
        Self::new(smallvec![0u8])
    }
    pub fn half() -> Fraction {
        Self::from_digits(smallvec![1 << (DIGIT_BITS - 1)])
    }
    fn digits(&self) -> SmallVec<[u8; 8]> {
        let mut res = self.0.clone();
        res.iter_mut().for_each(|x| *x >>= 1);
        res
    }
    /// return the number part of a digit
    fn digit(&self, i: usize) -> usize {
        self.0.get(i).map(|x| *x >> 1).unwrap_or_default() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn fraction_smoke() {
        let t = Fraction::zero();
        let u = t.succ();
        let v = t.mid(&u);
        assert!(t < u);
        assert!(t < v && v < u);
        println!("{:?} < {:?} < {:?}", t, v, u);
    }

    fn arb_fraction() -> impl Strategy<Value = Fraction> {
        any::<Vec<u8>>().prop_map(|v| {
            let mut digits: SmallVec<[u8; 8]> = v.into();
            digits.iter_mut().for_each(|x| *x &= DIGIT_MASK_U8);
            Fraction::from_digits(digits)
        })
    }

    impl Arbitrary for Fraction {
        type Parameters = ();
        type Strategy = BoxedStrategy<Fraction>;

        fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
            arb_fraction().boxed()
        }
    }

    fn to_bytes(fs: &[Fraction]) -> Vec<u8> {
        let mut res = Vec::new();
        for f in fs {
            res.extend_from_slice(&f.0);
        }
        res
    }

    proptest! {
        #[test]
        fn mid(
            mut a in arb_fraction(),
            mut b in arb_fraction(),
        ) {
            if a != b {
                if a > b {
                    std::mem::swap(&mut a, &mut b);
                }
                let m = a.mid(&b);
                println!("a={:?}, b={:?}, m={:?}", a, b, m);
                prop_assert!(a < m);
                prop_assert!(m < b);
            } else {
                let m = a.mid(&b);
                prop_assert!(m == a);
                prop_assert!(m == b);
            }
        }

        #[test]
        fn succ(
            a in arb_fraction(),
        ) {
            prop_assert!(a.succ() > a);
        }

        #[test]
        fn ord(
            a in any::<Vec<Fraction>>(),
            b in any::<Vec<Fraction>>(),
        ) {
            let ab = to_bytes(&a);
            let bb = to_bytes(&b);
            let byte_order = ab.cmp(&bb);
            let val_order = a.cmp(&b);
            println!("a={:?} b={:?}", a, b);
            println!("ab={} bb={}", hex::encode(ab), hex::encode(bb));
            prop_assert_eq!(byte_order, val_order);
        }
    }
}

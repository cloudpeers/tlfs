use core::{fmt, ops::Index};
use smallvec::{smallvec, SmallVec};

/// A binary fraction type. Can encode any value in the interval [0..1) with arbitary precision.
///
/// trailing zeros are not stored.
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone)]
pub struct Fraction(SmallVec<[u8; 8]>);

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
            write!(f, "0x0.{}", base64::encode(&self.0))
        } else {
            write!(f, "0x0")
        }
    }
}

impl Fraction {
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_ref()
    }
    pub fn zero() -> Fraction {
        Self::new(SmallVec::new())
    }

    pub fn half() -> Fraction {
        Self::new(smallvec![0x80u8])
    }

    pub fn new(mut data: SmallVec<[u8; 8]>) -> Self {
        // canonicalize
        while data.last() == Some(&0u8) {
            data.pop();
        }
        Self(data)
    }

    /// Compute the midpoint between two numbers
    pub fn mid(&self, that: &Self) -> Self {
        let n = self.0.len().max(that.0.len());
        let mut res = SmallVec::with_capacity(n);
        // calculate self + that. Highest bit will be in carry.
        let mut carry = 0usize;
        for i in (0..n).rev() {
            carry += self[i] as usize;
            carry += that[i] as usize;
            res.push(carry as u8);
            carry >>= 8;
        }
        res.reverse();
        // divide by 2, including carry. Lowest bit will be in carry.
        for i in 0..n {
            let r = res[i];
            res[i] = (r >> 1) + ((carry as u8) << 7);
            carry = (r & 1) as usize;
        }
        // if we have a carry, we need to extend the result
        if carry != 0 {
            res.push(0x80);
        }
        Self::new(res)
    }

    /// Compute a number that is larger than the current number by some small amount
    pub fn succ(&self) -> Self {
        let mut res = self.0.clone();
        // if we would wraparound when adding, add some more fractional digits
        if res.iter().all(|x| *x == 0xff) {
            let n = res.len().max(1);
            res.extend((0..n).map(|_| 0u8));
        }
        // add 1 to the lowest current fractional digit
        for byte in res.iter_mut().rev() {
            *byte += 1;
            if *byte != 0 {
                break;
            }
        }
        Self::new(res)
    }
}

#[test]
fn fraction_smoke() {
    let t = Fraction::zero();
    let u = t.succ();
    let v = t.mid(&u);
    assert!(t < u);
    assert!(t < v && v < u);
    println!("{:?} < {:?} < {:?}", t, v, u);
}

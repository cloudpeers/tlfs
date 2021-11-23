use crate::acl::Policy;
use crate::dotset::Dot;
use crate::fraction::Fraction;
use crate::id::{DocId, PeerId};
use crate::util::Ref;
use anyhow::{Context, Result};
use bytecheck::CheckBytes;
use ed25519_dalek::Signature;
use rkyv::{Archive, Deserialize, Serialize};
use std::borrow::Borrow;
use std::convert::TryInto;
use std::iter::FromIterator;

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive(as = "SegmentType")]
#[repr(u8)]
pub enum SegmentType {
    Doc,
    Peer,
    Nonce,
    Bool,
    U64,
    I64,
    Str,
    Policy,
    Dot,
    Position,
    Sig,
}

impl SegmentType {
    fn new(u: u8) -> Option<Self> {
        use SegmentType::*;
        match u {
            u if u == Doc as u8 => Some(Doc),
            u if u == Peer as u8 => Some(Peer),
            u if u == Nonce as u8 => Some(Nonce),
            u if u == Bool as u8 => Some(Bool),
            u if u == U64 as u8 => Some(U64),
            u if u == I64 as u8 => Some(I64),
            u if u == Str as u8 => Some(Str),
            u if u == Policy as u8 => Some(Policy),
            u if u == Dot as u8 => Some(Dot),
            u if u == Position as u8 => Some(Position),
            u if u == Sig as u8 => Some(Sig),
            _ => unreachable!("Unexpected SegmentType: {}", u),
        }
    }

    fn last_element(data: &[u8]) -> Option<(SegmentType, usize, &[u8])> {
        use std::mem::size_of;
        let last = data.last()?;
        let ty = SegmentType::new(*last).unwrap();
        let len = 1 + match ty {
            SegmentType::Doc => size_of::<DocId>(),
            SegmentType::Peer => size_of::<PeerId>(),
            SegmentType::Nonce => size_of::<u64>(),
            SegmentType::Bool => size_of::<bool>(),
            SegmentType::U64 => size_of::<u64>(),
            SegmentType::I64 => size_of::<i64>(),
            SegmentType::Dot => size_of::<Dot>(),
            SegmentType::Sig => size_of::<Signature>(),
            SegmentType::Str | SegmentType::Position | SegmentType::Policy => {
                if data.len() < 3 {
                    return None;
                }
                let size =
                    u16::from_be_bytes(data[data.len() - 3..data.len() - 1].try_into().unwrap());
                2 + (size as usize)
            }
        };
        if data.len() < len {
            return None;
        }
        let content = if ty.is_variable_length() {
            &data[data.len() - len..data.len() - 3]
        } else {
            &data[data.len() - len..data.len() - 1]
        };
        Some((ty, len, content))
    }

    fn first_element(mut data: &[u8]) -> Option<(SegmentType, usize, &[u8])> {
        while let Some((ty, len, content)) = Self::last_element(data) {
            if len == data.len() {
                return Some((ty, len, content));
            } else {
                data = &data[..data.len() - len];
            }
        }
        None
    }

    fn is_variable_length(&self) -> bool {
        matches!(
            self,
            SegmentType::Position | SegmentType::Str | SegmentType::Policy
        )
    }
}

/// A segment of a path.
#[derive(Clone, Eq, PartialEq)]
pub enum Segment {
    /// Document identifier.
    Doc(DocId),
    /// Peer identifier.
    Peer(PeerId),
    /// Randomness used to ensure path uniqueness.
    Nonce(u64),
    /// Boolean primitive.
    Bool(bool),
    /// Unsigned integer primitive.
    U64(u64),
    /// Signed integer primitive.
    I64(i64),
    /// Utf8 string primitive.
    Str(String),
    /// Policy statement.
    Policy(Policy),
    /// Path identifier.
    Dot(Dot),
    /// Positional identifier.
    Position(Fraction),
    /// Signature primitive.
    Sig(Signature),
}

impl Segment {
    fn new(ty: SegmentType, data: &[u8]) -> Self {
        match ty {
            SegmentType::Doc => Self::Doc(DocId::new(data.try_into().unwrap())),
            SegmentType::Peer => Self::Peer(PeerId::new(data.try_into().unwrap())),
            SegmentType::Nonce => Self::Nonce(u64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::Bool => Self::Bool(data[0] > 0),
            SegmentType::U64 => Self::U64(u64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::I64 => Self::I64(i64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::Str => {
                Self::Str(unsafe { std::str::from_utf8_unchecked(data) }.to_string())
            }
            SegmentType::Policy => {
                let policy = Ref::<Policy>::new(data.into());
                Self::Policy(policy.to_owned().unwrap())
            }
            SegmentType::Dot => Self::Dot(Dot::new(data.try_into().unwrap())),
            SegmentType::Position => Self::Position(Fraction::new(data.into())),
            SegmentType::Sig => Self::Sig(Signature::new(data.try_into().unwrap())),
        }
    }

    /// Returns the `DocId`.
    pub fn doc(self) -> Option<DocId> {
        if let Segment::Doc(doc) = self {
            Some(doc)
        } else {
            None
        }
    }

    /// Returns the `PeerId`.
    pub fn peer(self) -> Option<PeerId> {
        if let Segment::Peer(peer) = self {
            Some(peer)
        } else {
            None
        }
    }

    /// Returns the nonce.
    pub fn nonce(self) -> Option<u64> {
        if let Segment::Nonce(nonce) = self {
            Some(nonce)
        } else {
            None
        }
    }

    /// Returns the `bool`.
    pub fn prim_bool(self) -> Option<bool> {
        if let Segment::Bool(b) = self {
            Some(b)
        } else {
            None
        }
    }

    /// Returns the `u64`.
    pub fn prim_u64(self) -> Option<u64> {
        if let Segment::U64(u) = self {
            Some(u)
        } else {
            None
        }
    }

    /// Returns the `i64`.
    pub fn prim_i64(self) -> Option<i64> {
        if let Segment::I64(u) = self {
            Some(u)
        } else {
            None
        }
    }

    /// Returns the `&str`.
    pub fn prim_str(&self) -> Option<&str> {
        if let Segment::Str(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    /// Returns the `String`.
    pub fn prim_string(self) -> Option<String> {
        if let Segment::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    /// Returns the `Policy`.
    pub fn policy(self) -> Option<Policy> {
        if let Segment::Policy(policy) = self {
            Some(policy)
        } else {
            None
        }
    }

    /// Returns the `Dot`.
    pub fn dot(self) -> Option<Dot> {
        if let Segment::Dot(dot) = self {
            Some(dot)
        } else {
            None
        }
    }

    /// Returns the position.
    pub fn position(self) -> Option<Fraction> {
        if let Segment::Position(frac) = self {
            Some(frac)
        } else {
            None
        }
    }
    /// Returns the `Signature`.
    pub fn sig(self) -> Option<Signature> {
        if let Segment::Sig(sig) = self {
            Some(sig)
        } else {
            None
        }
    }
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Doc(s) => write!(f, "{:?}", s),
            Self::Peer(s) => write!(f, "{:?}", s),
            Self::Nonce(s) => write!(f, "Nonce({})", s),
            Self::Bool(s) => write!(f, "{}", s),
            Self::U64(s) => write!(f, "{}", s),
            Self::I64(s) => write!(f, "{}", s),
            Self::Str(s) => write!(f, "{:?}", s),
            Self::Policy(s) => write!(f, "{:?}", s),
            Self::Dot(s) => write!(f, "{:?}", s),
            Self::Position(s) => write!(f, "Position({})", base64::encode(s)),
            Self::Sig(_) => write!(f, "Sig"),
        }
    }
}

/// An owned concatentation of binary encoded segments.
#[derive(Clone, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, PartialEq, Ord, PartialOrd, CheckBytes))]
#[repr(C)]
pub struct PathBuf(Vec<u8>);

impl Borrow<[u8]> for PathBuf {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PathBuf {
    /// Creates an empty path buffer.
    pub fn new() -> Self {
        Self::default()
    }

    fn push_len(&mut self, len: usize) {
        assert!(len <= u16::MAX as usize);
        self.0.extend((len as u16).to_be_bytes());
    }

    fn push(&mut self, ty: SegmentType, bytes: &[u8]) {
        self.0.extend(bytes);
        if ty.is_variable_length() {
            self.push_len(bytes.len());
        }
        self.0.extend(&[ty as u8]);
    }

    /// Appends a [`Segment`].
    pub fn push_segment(&mut self, segment: Segment) {
        match segment {
            Segment::Doc(d) => self.doc(&d),
            Segment::Peer(d) => self.peer(&d),
            Segment::Nonce(d) => self.nonce(d),
            Segment::Bool(d) => self.prim_bool(d),
            Segment::U64(d) => self.prim_u64(d),
            Segment::I64(d) => self.prim_i64(d),
            Segment::Str(d) => self.prim_str(&*d),
            Segment::Policy(d) => self.policy(&d),
            Segment::Dot(d) => self.dot(&d),
            Segment::Position(d) => self.position(&d),
            Segment::Sig(d) => self.sig(d),
        }
    }

    /// Appends a doc segment.
    pub fn doc(&mut self, doc: &DocId) {
        self.push(SegmentType::Doc, doc.as_ref());
    }

    /// Appends a peer segment.
    pub fn peer(&mut self, peer: &PeerId) {
        self.push(SegmentType::Peer, peer.as_ref());
    }

    /// Appends a nonce segment.
    pub fn nonce(&mut self, nonce: u64) {
        self.push(SegmentType::Nonce, nonce.to_be_bytes().as_ref());
    }

    /// Appends a bool segment.
    pub fn prim_bool(&mut self, b: bool) {
        let b = if b { 1 } else { 0 };
        self.push(SegmentType::Bool, &[b]);
    }

    /// Appends a u64 segment.
    pub fn prim_u64(&mut self, u: u64) {
        self.push(SegmentType::U64, u.to_be_bytes().as_ref());
    }

    /// Appends an i64 segment.
    pub fn prim_i64(&mut self, i: i64) {
        self.push(SegmentType::I64, i.to_be_bytes().as_ref());
    }

    /// Appends a utf8 segment.
    pub fn prim_str(&mut self, s: &str) {
        self.push(SegmentType::Str, s.as_bytes());
    }

    /// Appends a policy segment.
    pub fn policy(&mut self, policy: &Policy) {
        self.push(SegmentType::Policy, Ref::archive(policy).as_bytes());
    }

    /// Appends a dot segment.
    pub fn dot(&mut self, dot: &Dot) {
        self.push(SegmentType::Dot, dot.as_ref());
    }

    /// Apends a position segment.
    pub fn position(&mut self, data: &Fraction) {
        self.push(SegmentType::Position, data.as_ref());
    }

    /// Appends a sig segment.
    pub fn sig(&mut self, sig: Signature) {
        self.push(SegmentType::Sig, sig.as_ref());
    }

    /// Pops the last segment.
    pub fn pop(&mut self) {
        if let Some(path) = self.as_path().parent() {
            let len = path.0.len();
            self.0.truncate(len);
        }
    }

    /// Returns a borrowed path.
    pub fn as_path(&self) -> Path<'_> {
        Path(&self.0)
    }

    /// Extends the buffer with the segments of a path.
    pub fn extend(&mut self, path: Path) {
        self.0.extend_from_slice(path.as_ref());
    }
}

impl std::fmt::Debug for PathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_path().fmt(f)
    }
}

impl std::fmt::Display for PathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.as_path().fmt(f)
    }
}

impl AsRef<[u8]> for PathBuf {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl ArchivedPathBuf {
    pub fn as_path(&self) -> Path<'_> {
        Path(&self.0)
    }
}

impl FromIterator<Segment> for PathBuf {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Segment>,
    {
        let mut path = PathBuf::new();
        for seg in iter.into_iter() {
            path.push_segment(seg);
        }
        path
    }
}

/// A borrowed binary encoded path.
#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Path<'a>(&'a [u8]);

impl<'a> Path<'a> {
    /// Creates a new path from an encoded byte slice.
    pub fn new(p: &'a [u8]) -> Self {
        Self(p)
    }

    /// Retrns true if the path contains no segments.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns true if the path is an ancestor of `other`.
    pub fn is_ancestor(&self, other: Path) -> bool {
        other.as_ref().starts_with(self.as_ref())
    }

    /// Returns an owned `PathBuf`.
    pub fn to_owned(&self) -> PathBuf {
        PathBuf(self.0.to_vec())
    }

    fn first_len(&self) -> Option<usize> {
        SegmentType::first_element(self.0).map(|(_, l, _)| l)
    }

    fn last_len(&self) -> Option<usize> {
        SegmentType::last_element(self.0).map(|(_, l, _)| l)
    }

    /// Returns the last segment.
    pub fn last(&self) -> Option<Segment> {
        let (ty, _, data) = SegmentType::last_element(self.0)?;
        Some(Segment::new(ty, data))
    }

    /// Returns the first segment.
    pub fn first(&self) -> Option<Segment> {
        let (ty, _, data) = SegmentType::first_element(self.0)?;
        Some(Segment::new(ty, data))
    }

    /// Returns the path without the first segment.
    pub fn child(&self) -> Option<Path<'a>> {
        let len = self.first_len()?;
        Some(Path(&self.0[len..]))
    }

    /// Returns the path without the last segment.
    pub fn parent(&self) -> Option<Path<'a>> {
        let len = self.last_len()?;
        let end = self.0.len();
        Some(Path(&self.0[..(end - len)]))
    }

    /// Returns an identifier for the path.
    pub fn dot(&self) -> Dot {
        Dot::new(blake3::hash(self.as_ref()).into())
    }

    /// Returns a path that, when joined onto `base`, yields `self`.
    pub fn strip_prefix(&self, base: Self) -> Result<PathBuf> {
        Ok(iter_after((*self).into_iter(), base.into_iter())
            .context("StripPrefixError")?
            .collect())
    }

    /// Returns the first segment and the path without the first segment.
    pub fn split_first(&self) -> Option<(Segment, Path<'a>)> {
        let first = self.first()?;
        let child = self.child()?;
        Some((first, child))
    }

    /// Returns the last segment and the path without the last segment.
    pub fn split_last(&self) -> Option<(Path<'a>, Segment)> {
        let parent = self.parent()?;
        let last = self.last()?;
        Some((parent, last))
    }
}

fn iter_after<I, J>(mut iter: I, mut prefix: J) -> Option<I>
where
    I: Iterator<Item = Segment> + Clone,
    J: Iterator<Item = Segment>,
{
    loop {
        let mut iter_next = iter.clone();
        match (iter_next.next(), prefix.next()) {
            (Some(ref x), Some(ref y)) if x == y => (),
            (Some(_), Some(_)) => return None,
            (Some(_), None) => return Some(iter),
            (None, None) => return Some(iter),
            (None, Some(_)) => return None,
        }
        iter = iter_next;
    }
}

#[derive(Clone)]
/// Iterator over path segments.
pub struct PathIter<'a>(Path<'a>);

impl<'a> Iterator for PathIter<'a> {
    type Item = Segment;

    fn next(&mut self) -> Option<Segment> {
        if let Some((seg, path)) = self.0.split_first() {
            self.0 = path;
            Some(seg)
        } else {
            None
        }
    }
}

impl<'a> IntoIterator for Path<'a> {
    type IntoIter = PathIter<'a>;
    type Item = Segment;

    fn into_iter(self) -> Self::IntoIter {
        PathIter(self)
    }
}

impl<'a> std::fmt::Debug for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(parent) = self.parent() {
            if !parent.is_empty() {
                write!(f, "{:?}.", parent)?;
            }
        }
        if let Some(last) = self.last() {
            write!(f, "{:?}", last)?;
        }
        Ok(())
    }
}

impl<'a> std::fmt::Display for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<'a> AsRef<[u8]> for Path<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iter() {
        let mut p = PathBuf::new();
        p.doc(&DocId::new([0; 32]));
        println!("{:?}", p);
        p.prim_str("a");
        println!("{:?}", p);
        p.prim_i64(42);
        println!("{:?}", p);
        p.prim_str("b");
        println!("{:?}", p);
        p.prim_i64(43);
        println!("{:?}", p);
        p.prim_str("c");
        println!("{:?}", p);

        let mut path = p.as_path().into_iter();
        for i in [
            Segment::Doc(DocId::new([0; 32])),
            Segment::Str("a".to_string()),
            Segment::I64(42),
            Segment::Str("b".to_string()),
            Segment::I64(43),
            Segment::Str("c".to_string()),
        ] {
            assert_eq!(path.next().unwrap(), i);
        }
        assert!(path.next().is_none());
    }

    #[test]
    fn strip_prefix() {
        let mut p = PathBuf::new();
        p.doc(&DocId::new([0; 32]));
        p.prim_str("a");
        p.prim_i64(42);
        p.prim_str("b");
        p.prim_i64(43);
        p.prim_str("c");

        let mut base = PathBuf::new();
        base.doc(&DocId::new([0; 32]));
        base.prim_str("a");
        base.prim_i64(42);

        let relative = p.as_path().strip_prefix(base.as_path()).unwrap();
        let mut iter = relative.as_path().into_iter();
        for i in [
            Segment::Str("b".to_string()),
            Segment::I64(43),
            Segment::Str("c".to_string()),
        ] {
            assert_eq!(iter.next().unwrap(), i);
        }
        assert!(iter.next().is_none());
    }
}

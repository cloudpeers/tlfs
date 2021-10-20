use crate::{DocId, Dot, PeerId, Policy, Ref};
use anyhow::Context;
use blake3::Hash;
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::borrow::Borrow;
use std::convert::{TryFrom, TryInto};

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Archive, Deserialize, Serialize,
)]
#[archive(as = "SegmentType")]
#[repr(u8)]
pub enum SegmentType {
    Schema,
    Doc,
    Peer,
    Nonce,
    Bool,
    U64,
    I64,
    Str,
    Policy,
    Dot,
    Slice,
}

impl SegmentType {
    fn new(u: u8) -> Option<Self> {
        use SegmentType::*;
        match u {
            u if u == Schema as u8 => Some(Schema),
            u if u == Doc as u8 => Some(Doc),
            u if u == Peer as u8 => Some(Peer),
            u if u == Nonce as u8 => Some(Nonce),
            u if u == Bool as u8 => Some(Bool),
            u if u == U64 as u8 => Some(U64),
            u if u == I64 as u8 => Some(I64),
            u if u == Str as u8 => Some(Str),
            u if u == Policy as u8 => Some(Policy),
            u if u == Dot as u8 => Some(Dot),
            u if u == Slice as u8 => Some(Slice),
            _ => unreachable!("Unexpected SegmentType: {}", u),
        }
    }
}

#[derive(Eq, PartialEq)]
pub enum Segment<'a> {
    Schema(Hash),
    Doc(DocId),
    Peer(PeerId),
    Nonce(u64),
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(&'a str),
    Policy(Policy),
    Dot(Dot),
    Slice(&'a [u8]),
}

impl<'a> Segment<'a> {
    fn new(ty: SegmentType, data: &'a [u8]) -> Self {
        match ty {
            SegmentType::Schema => Self::Schema(Hash::from(<[u8; 32]>::try_from(data).unwrap())),
            SegmentType::Doc => Self::Doc(DocId::new(data.try_into().unwrap())),
            SegmentType::Peer => Self::Peer(PeerId::new(data.try_into().unwrap())),
            SegmentType::Nonce => Self::Nonce(u64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::Bool => Self::Bool(data[0] > 0),
            SegmentType::U64 => Self::U64(u64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::I64 => Self::I64(i64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::Str => Self::Str(unsafe { std::str::from_utf8_unchecked(data) }),
            SegmentType::Policy => {
                let policy = Ref::<Policy>::new(data.into());
                Self::Policy(policy.to_owned().unwrap())
            }
            SegmentType::Dot => Self::Dot(Dot::new(data.try_into().unwrap())),
            SegmentType::Slice => Self::Slice(data),
        }
    }

    pub fn schema(self) -> Option<Hash> {
        if let Segment::Schema(schema) = self {
            Some(schema)
        } else {
            None
        }
    }

    pub fn doc(self) -> Option<DocId> {
        if let Segment::Doc(doc) = self {
            Some(doc)
        } else {
            None
        }
    }

    pub fn peer(self) -> Option<PeerId> {
        if let Segment::Peer(peer) = self {
            Some(peer)
        } else {
            None
        }
    }

    pub fn nonce(self) -> Option<u64> {
        if let Segment::Nonce(nonce) = self {
            Some(nonce)
        } else {
            None
        }
    }

    pub fn prim_bool(self) -> Option<bool> {
        if let Segment::Bool(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub fn prim_u64(self) -> Option<u64> {
        if let Segment::U64(u) = self {
            Some(u)
        } else {
            None
        }
    }

    pub fn prim_i64(self) -> Option<i64> {
        if let Segment::I64(u) = self {
            Some(u)
        } else {
            None
        }
    }

    pub fn prim_str(self) -> Option<&'a str> {
        if let Segment::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn policy(self) -> Option<Policy> {
        if let Segment::Policy(policy) = self {
            Some(policy)
        } else {
            None
        }
    }

    pub fn dot(self) -> Option<Dot> {
        if let Segment::Dot(dot) = self {
            Some(dot)
        } else {
            None
        }
    }

    pub fn slice(self) -> Option<&'a [u8]> {
        if let Segment::Slice(slice) = self {
            Some(slice)
        } else {
            None
        }
    }
}

impl<'a> std::fmt::Debug for Segment<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Schema(s) => write!(f, "Schema({})", hex::encode(&s.as_bytes()[..2])),
            Self::Doc(s) => write!(f, "{:?}", s),
            Self::Peer(s) => write!(f, "{:?}", s),
            Self::Nonce(s) => write!(f, "Nonce({})", s),
            Self::Bool(s) => write!(f, "{}", s),
            Self::U64(s) => write!(f, "{}", s),
            Self::I64(s) => write!(f, "{}", s),
            Self::Str(s) => write!(f, "{:?}", s),
            Self::Policy(s) => write!(f, "{:?}", s),
            Self::Dot(s) => write!(f, "{:?}", s),
            Self::Slice(s) => write!(f, "Slice({})", hex::encode(s)),
        }
    }
}

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
    pub fn new() -> Self {
        Self::default()
    }

    fn push_len(&mut self, len: usize) {
        assert!(len <= u16::MAX as usize);
        self.0.extend((len as u16).to_be_bytes());
    }

    fn push(&mut self, ty: SegmentType, bytes: &[u8]) {
        self.0.extend(&[ty as u8]);
        self.push_len(bytes.len());
        self.0.extend(bytes);
        self.push_len(bytes.len());
        self.0.extend(&[ty as u8]);
    }

    pub fn push_segment(&mut self, segment: &Segment) {
        match segment {
            Segment::Schema(d) => self.schema(d),
            Segment::Doc(d) => self.doc(d),
            Segment::Peer(d) => self.peer(d),
            Segment::Nonce(d) => self.nonce(*d),
            Segment::Bool(d) => self.prim_bool(*d),
            Segment::U64(d) => self.prim_u64(*d),
            Segment::I64(d) => self.prim_i64(*d),
            Segment::Str(d) => self.prim_str(d),
            Segment::Policy(d) => self.policy(d),
            Segment::Dot(d) => self.dot(d),
            Segment::Slice(d) => self.slice(d),
        }
    }

    pub fn schema(&mut self, schema: &Hash) {
        self.push(SegmentType::Schema, schema.as_bytes());
    }

    pub fn doc(&mut self, doc: &DocId) {
        self.push(SegmentType::Doc, doc.as_ref());
    }

    pub fn peer(&mut self, peer: &PeerId) {
        self.push(SegmentType::Peer, peer.as_ref());
    }

    pub fn nonce(&mut self, nonce: u64) {
        self.push(SegmentType::Nonce, nonce.to_be_bytes().as_ref());
    }

    pub fn prim_bool(&mut self, b: bool) {
        let b = if b { 1 } else { 0 };
        self.push(SegmentType::Bool, &[b]);
    }

    pub fn prim_u64(&mut self, u: u64) {
        self.push(SegmentType::U64, u.to_be_bytes().as_ref());
    }

    pub fn prim_i64(&mut self, i: i64) {
        self.push(SegmentType::I64, i.to_be_bytes().as_ref());
    }

    pub fn prim_str(&mut self, s: &str) {
        self.push(SegmentType::Str, s.as_bytes());
    }

    pub fn policy(&mut self, policy: &Policy) {
        self.push(SegmentType::Policy, Ref::archive(policy).as_bytes());
    }

    pub fn dot(&mut self, dot: &Dot) {
        self.push(SegmentType::Dot, dot.as_ref());
    }

    pub fn slice(&mut self, data: &[u8]) {
        self.push(SegmentType::Slice, data);
    }

    pub fn pop(&mut self) {
        if let Some(path) = self.as_path().parent() {
            let len = path.0.len();
            self.0.truncate(len);
        }
    }

    pub fn as_path(&self) -> Path<'_> {
        Path(&self.0)
    }

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

#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Path<'a>(&'a [u8]);

impl<'a> Path<'a> {
    pub fn new(p: &'a [u8]) -> Self {
        Self(p)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_ancestor(&self, other: Path) -> bool {
        other.as_ref().starts_with(self.as_ref())
    }

    pub fn to_owned(&self) -> PathBuf {
        PathBuf(self.0.to_vec())
    }

    fn first_len(&self) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let mut len = [0; 2];
        len.copy_from_slice(&self.0[1..3]);
        Some(u16::from_be_bytes(len) as usize)
    }

    fn last_len(&self) -> Option<usize> {
        if self.is_empty() {
            return None;
        }
        let end = self.0.len();
        let mut len = [0; 2];
        len.copy_from_slice(&self.0[(end - 3)..(end - 1)]);
        Some(u16::from_be_bytes(len) as usize)
    }

    pub fn last(&self) -> Option<Segment> {
        let len = self.last_len()?;
        let end = self.0.len();
        let ty = SegmentType::new(self.0[end - 1])?;
        Some(Segment::new(ty, &self.0[(end - 3 - len)..(end - 3)]))
    }

    pub fn first(&self) -> Option<Segment<'a>> {
        let len = self.first_len()?;
        let ty = SegmentType::new(self.0[0])?;
        Some(Segment::new(ty, &self.0[3..(len + 3)]))
    }

    pub fn child(&self) -> Option<Path<'a>> {
        let len = self.first_len()?;
        Some(Path(&self.0[(len + 6)..]))
    }

    pub fn parent(&self) -> Option<Path> {
        let len = self.last_len()?;
        let end = self.0.len();
        Some(Path(&self.0[..(end - len - 6)]))
    }

    pub fn dot(&self) -> Dot {
        Dot::new(blake3::hash(self.as_ref()).into())
    }

    /// Returns a path that, when joined onto `base`, yields `self`.
    pub fn strip_prefix(&self, base: Self) -> anyhow::Result<Self> {
        iter_after(*self, base).context("StripPrefixError")
    }
}

impl<'a> Iterator for Path<'a> {
    type Item = Segment<'a>;

    fn next(&mut self) -> Option<Segment<'a>> {
        let r = self.first();
        if let Some(c) = self.child() {
            *self = c;
        }

        r
    }
}

fn iter_after<'a, 'b, I, J>(mut iter: I, mut prefix: J) -> Option<I>
where
    I: Iterator<Item = Segment<'a>> + Clone,
    J: Iterator<Item = Segment<'b>>,
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
        p.prim_str("a");
        p.prim_i64(42);
        p.prim_str("b");
        p.prim_i64(43);
        p.prim_str("c");

        let mut path = p.as_path();
        for i in [
            Segment::Doc(DocId::new([0; 32])),
            Segment::Str("a"),
            Segment::I64(42),
            Segment::Str("b"),
            Segment::I64(43),
            Segment::Str("c"),
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

        let mut relative = p.as_path().strip_prefix(base.as_path()).unwrap();
        for i in [Segment::Str("b"), Segment::I64(43), Segment::Str("c")] {
            assert_eq!(relative.next().unwrap(), i);
        }
        assert!(relative.next().is_none());
    }
}

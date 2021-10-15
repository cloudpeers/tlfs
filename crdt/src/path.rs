use crate::{DocId, PeerId, Policy, Primitive, Ref};
use blake3::Hash;
use bytecheck::CheckBytes;
use rkyv::{archived_root, Archive, Archived, Deserialize, Serialize};
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
    Primitive,
    Str,
    Policy,
}

impl SegmentType {
    fn new(u: u8) -> Option<Self> {
        use SegmentType::*;
        match u {
            u if u == Schema as u8 => Some(Schema),
            u if u == Doc as u8 => Some(Doc),
            u if u == Peer as u8 => Some(Peer),
            u if u == Nonce as u8 => Some(Nonce),
            u if u == Primitive as u8 => Some(Primitive),
            u if u == Str as u8 => Some(Str),
            u if u == Policy as u8 => Some(Policy),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum Segment<'a> {
    Schema(Hash),
    Doc(DocId),
    Peer(PeerId),
    Nonce(u64),
    Primitive(&'a Archived<Primitive>),
    Str(&'a str),
    Policy(&'a Archived<Policy>),
}

impl<'a> Segment<'a> {
    fn new(ty: SegmentType, data: &'a [u8]) -> Self {
        match ty {
            SegmentType::Schema => Self::Schema(Hash::from(<[u8; 32]>::try_from(data).unwrap())),
            SegmentType::Doc => Self::Doc(DocId::new(data.try_into().unwrap())),
            SegmentType::Peer => Self::Peer(PeerId::new(data.try_into().unwrap())),
            SegmentType::Nonce => Self::Nonce(u64::from_be_bytes(data.try_into().unwrap())),
            SegmentType::Primitive => Self::Primitive(unsafe { archived_root::<Primitive>(data) }),
            SegmentType::Str => Self::Str(unsafe { std::str::from_utf8_unchecked(data) }),
            SegmentType::Policy => Self::Policy(unsafe { archived_root::<Policy>(data) }),
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

    pub fn primitive(self) -> Option<&'a Archived<Primitive>> {
        if let Segment::Primitive(primitive) = self {
            Some(primitive)
        } else {
            None
        }
    }

    pub fn str(self) -> Option<&'a str> {
        if let Segment::Str(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn policy(self) -> Option<&'a Archived<Policy>> {
        if let Segment::Policy(policy) = self {
            Some(policy)
        } else {
            None
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

    fn extend_len(&mut self, len: usize) {
        assert!(len <= u16::MAX as usize);
        self.0.extend((len as u16).to_be_bytes());
    }

    fn extend(&mut self, ty: SegmentType, bytes: &[u8]) {
        self.0.extend(&[ty as u8]);
        self.extend_len(bytes.len());
        self.0.extend(bytes);
        self.extend_len(bytes.len());
        self.0.extend(&[ty as u8]);
    }

    pub fn schema(&mut self, schema: &Hash) {
        self.extend(SegmentType::Schema, schema.as_bytes());
    }

    pub fn doc(&mut self, doc: &DocId) {
        self.extend(SegmentType::Doc, doc.as_ref());
    }

    pub fn peer(&mut self, peer: &PeerId) {
        self.extend(SegmentType::Peer, peer.as_ref());
    }

    pub fn nonce(&mut self, nonce: u64) {
        self.extend(SegmentType::Nonce, nonce.to_be_bytes().as_ref());
    }

    pub fn primitive(&mut self, primitive: &Primitive) {
        self.extend(SegmentType::Primitive, Ref::archive(primitive).as_bytes());
    }

    pub fn str(&mut self, s: &str) {
        self.extend(SegmentType::Str, s.as_bytes());
    }

    pub fn policy(&mut self, policy: &Policy) {
        self.extend(SegmentType::Policy, Ref::archive(policy).as_bytes());
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

    pub fn first(&self) -> Option<Segment> {
        let len = self.first_len()?;
        let ty = SegmentType::new(self.0[0])?;
        Some(Segment::new(ty, &self.0[3..(len + 3)]))
    }

    pub fn child(&self) -> Option<Path> {
        let len = self.first_len()?;
        Some(Path(&self.0[(len + 6)..]))
    }

    pub fn parent(&self) -> Option<Path> {
        let len = self.last_len()?;
        let end = self.0.len();
        Some(Path(&self.0[..(end - len - 6)]))
    }
}

impl<'a> std::fmt::Debug for Path<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(parent) = self.parent() {
            write!(f, "{:?}.", parent)?;
        }
        write!(f, "{:?}", self.last())
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

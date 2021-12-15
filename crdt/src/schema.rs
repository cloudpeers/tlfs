use crate::crdt::Causal;
use crate::path::{Path, Segment};
use crate::PathBuf;
use bytecheck::CheckBytes;
use ed25519_dalek::{PublicKey, Verifier};
use rkyv::{Archive, Serialize};
use std::collections::BTreeMap;

/// Kind of a primitive value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, CheckBytes, Serialize)]
#[archive(as = "PrimitiveKind")]
#[repr(u8)]
pub enum PrimitiveKind {
    /// Kind of [`bool`].
    Bool,
    /// Kind of [`u64`].
    U64,
    /// Kind of [`i64`].
    I64,
    /// Kind of [`String`].
    Str,
}

impl PrimitiveKind {
    fn validate(self, seg: Segment) -> bool {
        matches!(
            (self, seg),
            (Self::Bool, Segment::Bool(_))
                | (Self::U64, Segment::U64(_))
                | (Self::I64, Segment::I64(_))
                | (Self::Str, Segment::Str(_))
        )
    }
}

/// Schema defines the set of allowable paths.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(allow(missing_docs))]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Schema {
    /// Identity schema that contains only the empty [`Path`].
    Null,
    /// Flag schema contains paths with a single nonce segment.
    Flag,
    /// Reg schema contains paths with a nonce and a primitive of kind [`PrimitiveKind`].
    Reg(PrimitiveKind),
    /// Table schema contains paths with a primitive of kind [`PrimitiveKind`] and a sequence
    /// of segments matching [`Schema`].
    Table(PrimitiveKind, #[omit_bounds] Box<Schema>),
    /// Array schema contains a sequence of segments matching [`Schema`].
    Array(#[omit_bounds] Box<Schema>),
    /// Struct schema contains paths with a primitive of kind [`PrimitiveKind::Str`] and a
    /// sequence of segments matching [`Schema`].
    Struct(#[omit_bounds] BTreeMap<String, Schema>),
}

impl Default for Schema {
    fn default() -> Self {
        Self::Null
    }
}

impl ArchivedSchema {
    /// Returns if [`Causal`] matches [`ArchivedSchema`].
    pub fn validate(&self, causal: &Causal) -> bool {
        self._validate(causal) == Some(true)
    }

    fn _validate(&self, causal: &Causal) -> Option<bool> {
        for buf in causal.store.iter() {
            let path = buf.as_path();
            let path = verify_sig(path)?;
            let (doc, path) = path.split_first()?;
            doc.doc()?;
            if self.validate_path(path) != Some(true) {
                tracing::error!("invalid path {}", path);
                return Some(false);
            }
        }
        for path in causal.expired.iter() {
            let path = path.as_path();
            let path = verify_sig(path)?;
            let path = verify_sig(path)?;
            let (doc, path) = path.split_first()?;
            doc.doc()?;
            if path.last()?.policy().is_some() {
                tracing::error!("policy cannot be expired");
                return Some(false);
            }
            if self.validate_path(path) != Some(true) {
                tracing::error!("invalid expired path {}", path);
                return Some(false);
            }
        }
        Some(true)
    }

    fn validate_path(&self, path: Path) -> Option<bool> {
        if validate_policy(path) == Some(true) {
            return Some(true);
        }
        match self {
            Self::Null => Some(path.is_empty()),
            Self::Flag => {
                let (nonce, path) = path.split_first()?;
                nonce.nonce()?;
                Some(path.is_empty())
            }
            Self::Reg(kind) => {
                let (nonce, path) = path.split_first()?;
                nonce.nonce()?;
                let (prim, path) = path.split_first()?;
                Some(kind.validate(prim) && path.is_empty())
            }
            Self::Table(kind, schema) => {
                let (key, path) = path.split_first()?;
                Some(kind.validate(key) && schema.validate_path(path)?)
            }
            Self::Struct(fields) => {
                let (field, path) = path.split_first()?;
                let field = field.prim_str()?;
                let schema = fields.get(field)?;
                Some(schema.validate_path(path)?)
            }
            Self::Array(schema) => {
                let (prim, path) = path.split_first()?;
                match prim {
                    Segment::Str(x) => match x.as_str() {
                        "VALUES" => {
                            // <path_to_array>.VALUES.<pos>.<uid>.<value>
                            let mut path = path.into_iter();
                            path.next()?.position()?;
                            path.next()?.prim_u64()?;
                            schema.validate_path(path.collect::<PathBuf>().as_path())
                        }
                        "META" => {
                            // <path_to_array>.META.<uid>.<nonce>.<nonce>.<pos>.<nonce>.<peer>.<sig>
                            let mut path = path.into_iter();
                            path.next()?.prim_u64()?;
                            path.next()?.prim_u64()?;
                            path.next()?.prim_u64()?;
                            path.next()?.position()?;
                            path.next()?.prim_u64()?;
                            Some(path.next().is_none())
                        }
                        _ => Some(false),
                    },
                    _ => Some(false),
                }
            }
        }
    }
}

fn validate_policy(path: Path) -> Option<bool> {
    let (policy, path) = path.split_first()?;
    policy.policy()?;
    Some(path.is_empty())
}

fn verify_sig(path: Path) -> Option<Path> {
    let (path, sig) = path.split_last()?;
    let (path, peer) = path.split_last()?;
    let sig = sig.sig()?;
    let peer = peer.peer()?;
    let pubkey = PublicKey::from_bytes(peer.as_ref()).unwrap();
    if pubkey.verify(path.as_ref(), &sig).is_err() {
        tracing::error!("invalid signature of {:?} for {}", peer, path);
        return None;
    }
    Some(path)
}

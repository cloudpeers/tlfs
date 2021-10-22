use crate::crdt::Causal;
use crate::path::{Path, Segment};
use bytecheck::CheckBytes;
use ed25519_dalek::{PublicKey, Verifier};
use rkyv::{Archive, Serialize};
use std::collections::BTreeMap;

pub type Prop = String;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, CheckBytes, Serialize)]
#[archive(as = "PrimitiveKind")]
#[repr(u8)]
pub enum PrimitiveKind {
    Bool,
    U64,
    I64,
    Str,
}

impl PrimitiveKind {
    pub fn validate(self, seg: Segment) -> bool {
        matches!(
            (self, seg),
            (Self::Bool, Segment::Bool(_))
                | (Self::U64, Segment::U64(_))
                | (Self::I64, Segment::I64(_))
                | (Self::Str, Segment::Str(_))
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub enum Schema {
    Null,
    Flag,
    Reg(PrimitiveKind),
    Table(PrimitiveKind, #[omit_bounds] Box<Schema>),
    Struct(#[omit_bounds] BTreeMap<Prop, Schema>),
    Array(#[omit_bounds] Box<Schema>),
}

impl ArchivedSchema {
    pub fn validate(&self, causal: &Causal) -> bool {
        self._validate(causal) == Some(true)
    }

    fn _validate(&self, causal: &Causal) -> Option<bool> {
        for path in causal.store.iter() {
            let path = verify_sig(path)?;
            let (doc, path) = path.split_first()?;
            doc.doc()?;
            if self.validate_path(path) != Some(true) {
                tracing::error!("invalid path {}", path);
                return Some(false);
            }
        }
        for path in causal.expired.iter() {
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
                todo!("validate Array");
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

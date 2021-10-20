use crate::crdt::Causal;
use crate::path::{Path, Segment};
use bytecheck::CheckBytes;
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
}

impl ArchivedSchema {
    pub fn validate(&self, causal: &Causal) -> bool {
        for path in causal.store.iter() {
            if let Some((seg, child)) = path.split_first() {
                if seg.doc().is_none() {
                    return false;
                }
                if self.validate_path(child) != Some(true) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    fn validate_path(&self, path: Path) -> Option<bool> {
        if self.validate_policy(path) == Some(true) {
            return Some(true);
        }
        match self {
            Self::Null => {
                let eof = path.is_empty();
                Some(eof)
            }
            Self::Flag => {
                let peer = path.first()?.peer().is_some();
                let nonce = path.child()?.first()?.nonce().is_some();
                let eof = path.child()?.child()?.is_empty();
                Some(peer && nonce && eof)
            }
            Self::Reg(kind) => {
                let peer = path.first()?.peer().is_some();
                let nonce = path.child()?.first()?.nonce().is_some();
                let prim = path.child()?.child()?.first()?;
                let eof = path.child()?.child()?.child()?.is_empty();
                Some(peer && nonce && kind.validate(prim) && eof)
            }
            Self::Table(kind, schema) => {
                let key = path.first()?;
                let value = path.child()?;
                Some(kind.validate(key) && schema.validate_path(value)?)
            }
            Self::Struct(fields) => {
                let field = path.first()?.prim_string()?;
                let value = path.child()?;
                let schema = fields.get(field.as_str())?;
                Some(schema.validate_path(value)?)
            }
        }
    }

    fn validate_policy(&self, path: Path) -> Option<bool> {
        let peer = path.first()?.peer().is_some();
        let policy = path.child()?.first()?.policy().is_some();
        let eof = path.child()?.child()?.is_empty();
        Some(peer && policy && eof)
    }
}

use crate::{Causal, Path, Segment};
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;

pub type Prop = String;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, Hash, Ord, PartialEq, PartialOrd, CheckBytes))]
#[repr(C)]
pub enum Primitive {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
}

impl From<bool> for Primitive {
    fn from(b: bool) -> Self {
        Self::Bool(b)
    }
}

impl From<u64> for Primitive {
    fn from(u: u64) -> Self {
        Self::U64(u)
    }
}

impl From<i64> for Primitive {
    fn from(i: i64) -> Self {
        Self::I64(i)
    }
}

impl From<String> for Primitive {
    fn from(s: String) -> Self {
        Self::Str(s)
    }
}

impl From<&str> for Primitive {
    fn from(s: &str) -> Self {
        Self::Str(s.to_string())
    }
}

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

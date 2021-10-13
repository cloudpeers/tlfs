use crate::{crdt::DotStore, Primitive};
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
    pub fn validate(self, v: &Primitive) -> bool {
        matches!(
            (self, v),
            (Self::Bool, Primitive::Bool(_))
                | (Self::U64, Primitive::U64(_))
                | (Self::I64, Primitive::I64(_))
                | (Self::Str, Primitive::Str(_))
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
    pub fn validate(&self, _v: &DotStore) -> bool {
        // TODO
        true
    }
}

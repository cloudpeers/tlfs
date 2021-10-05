use crate::{DotStore, Primitive};
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
    Flag,
    Reg(PrimitiveKind),
    Table(PrimitiveKind, #[omit_bounds] Box<Schema>),
    Struct(#[omit_bounds] BTreeMap<Prop, Schema>),
}

impl ArchivedSchema {
    pub fn validate(&self, v: &DotStore) -> bool {
        match (self, v) {
            (Self::Flag, DotStore::DotSet(_)) => true,
            (Self::Reg(kind), DotStore::DotFun(fun)) => {
                for v in fun.values() {
                    if !kind.validate(v) {
                        return false;
                    }
                }
                true
            }
            (Self::Table(kind, schema), DotStore::DotMap(map)) => {
                for (key, crdt) in map.iter() {
                    if !kind.validate(key) {
                        return false;
                    }
                    if !schema.validate(crdt) {
                        return false;
                    }
                }
                true
            }
            (Self::Struct(schema), DotStore::Struct(fields)) => {
                /*for prop in schema.keys() {
                    if !map.contains_key(prop.as_str()) {
                        return false;
                    }
                }*/
                for (prop, crdt) in fields {
                    if let Some(schema) = schema.get(prop.as_str()) {
                        if !schema.validate(&crdt) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            _ => false,
        }
    }

    pub fn default(&self) -> DotStore {
        match self {
            Self::Flag => DotStore::DotSet(Default::default()),
            Self::Reg(_) => DotStore::DotFun(Default::default()),
            Self::Table(_, _) => DotStore::DotMap(Default::default()),
            Self::Struct(_) => DotStore::Struct(Default::default()),
        }
    }
}

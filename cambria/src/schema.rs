use crate::crdt::{Actor, Crdt, Primitive, Prop};
use rkyv::{Archive, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Archive, Serialize)]
#[archive(as = "PrimitiveKind")]
#[repr(C)]
pub enum PrimitiveKind {
    Bool,
    U64,
    I64,
    Str,
}

impl PrimitiveKind {
    pub fn validate(self, v: &Primitive) -> bool {
        match (self, v) {
            (Self::Bool, Primitive::Bool(_)) => true,
            (Self::U64, Primitive::U64(_)) => true,
            (Self::I64, Primitive::I64(_)) => true,
            (Self::Str, Primitive::Str(_)) => true,
            _ => false,
        }
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
    pub fn validate<A: Actor>(&self, v: &Crdt<A>) -> bool {
        match (self, v) {
            (Self::Null, Crdt::Null) => true,
            (Self::Flag, Crdt::Flag(_)) => true,
            (Self::Reg(kind), Crdt::Reg(reg)) => {
                for v in reg.values() {
                    if !kind.validate(v) {
                        return false;
                    }
                }
                true
            }
            (Self::Table(kind, schema), Crdt::Table(map)) => {
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
            (Self::Struct(schema), Crdt::Struct(map)) => {
                for prop in schema.keys() {
                    if !map.contains_key(prop.as_str()) {
                        return false;
                    }
                }
                for (prop, crdt) in map {
                    if let Some(schema) = schema.get(prop.as_str()) {
                        if !schema.validate(crdt) {
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
}

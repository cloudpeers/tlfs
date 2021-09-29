use crate::data::{Crdt, Data, Primitive, Prop};
use bytecheck::CheckBytes;
use rkyv::{Archive, Serialize};
use std::collections::BTreeMap;

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
    pub fn validate(&self, v: &Crdt) -> bool {
        match (self, &v.data) {
            (Self::Null, Data::Null) => true,
            (Self::Flag, Data::Flag(_)) => true,
            (Self::Reg(kind), Data::Reg(reg)) => {
                for v in reg.values() {
                    if !kind.validate(v) {
                        return false;
                    }
                }
                true
            }
            (Self::Table(kind, schema), Data::Table(map)) => {
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
            (Self::Struct(schema), Data::Struct(map)) => {
                /*for prop in schema.keys() {
                    if !map.contains_key(prop.as_str()) {
                        return false;
                    }
                }*/
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

    pub fn default(&self) -> Crdt {
        Crdt::new(match self {
            Self::Null => Data::Null,
            Self::Flag => Data::Flag(Default::default()),
            Self::Reg(_) => Data::Reg(Default::default()),
            Self::Table(_, _) => Data::Table(Default::default()),
            Self::Struct(_) => Data::Struct(Default::default()),
        })
    }
}

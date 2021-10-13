use crate::{crdt::DotStore, HDotStore, Primitive};
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
    pub fn validate(&self, v: &DotStore) -> bool {
        let store = v.to_dot_store().unwrap();
        self.validate0(&store)
    }

    fn validate0(&self, v: &HDotStore) -> bool {
        match (self, v) {
            (Self::Null, _) => true,
            (_, HDotStore::Null) => true,
            (Self::Flag, HDotStore::DotSet(_)) => true,
            (Self::Reg(kind), HDotStore::DotFun(fun)) => {
                for v in fun.values() {
                    if !kind.validate(v) {
                        return false;
                    }
                }
                true
            }
            (Self::Table(kind, schema), HDotStore::DotMap(map)) => {
                for (key, crdt) in map.iter() {
                    if !kind.validate(key) {
                        return false;
                    }
                    if !schema.validate0(crdt) {
                        return false;
                    }
                }
                true
            }
            (Self::Struct(schema), HDotStore::Struct(fields)) => {
                /*for prop in schema.keys() {
                    if !map.contains_key(prop.as_str()) {
                        return false;
                    }
                }*/
                for (prop, crdt) in fields {
                    if let Some(schema) = schema.get(prop.as_str()) {
                        if !schema.validate0(crdt) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                true
            }
            (_, HDotStore::Policy(_)) => true,
            _ => false,
        }
    }

    pub fn default(&self) -> HDotStore {
        match self {
            Self::Null => HDotStore::Null,
            Self::Flag => HDotStore::DotSet(Default::default()),
            Self::Reg(_) => HDotStore::DotFun(Default::default()),
            Self::Table(_, _) => HDotStore::DotMap(Default::default()),
            Self::Struct(_) => HDotStore::Struct(Default::default()),
        }
    }
}

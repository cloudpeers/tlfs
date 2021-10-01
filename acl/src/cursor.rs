use crate::data::{Crdt, Data, Dot, LabelRef, Primitive};
use crate::engine::{Actor, Can, Engine, Permission, Policy};
use crate::id::{DocId, PeerId};
use crate::schema::ArchivedSchema;
use crate::{Causal, CausalRef};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

type CausalData = tlfs_crdt::Causal<PeerId, Data>;
type CausalRefData<'a> = tlfs_crdt::CausalRef<'a, PeerId, Data>;

#[derive(Clone)]
pub struct W<'a> {
    schema: &'a ArchivedSchema,
    peer_id: PeerId,
    counter: Rc<RefCell<u64>>,
}

#[derive(Clone)]
pub struct Cursor<'a, T> {
    label: LabelRef<'a>,
    crdt: CausalRef<'a>,
    engine: &'a Engine,
    w: T,
}

impl<'a> Cursor<'a, ()> {
    pub fn new(doc: DocId, crdt: &'a Causal, engine: &'a Engine) -> Self {
        Self {
            label: LabelRef::Root(doc),
            crdt: crdt.as_ref(),
            engine,
            w: (),
        }
    }
}

impl<'a, T> Cursor<'a, T> {
    /// Checks permissions.
    pub fn can(&self, peer: PeerId, perm: Permission) -> bool {
        self.engine.can(peer, perm, self.label.as_ref())
    }

    /// Returns if a flag is enabled.
    pub fn enabled(&self) -> Option<bool> {
        if let Data::Flag(f) = &self.crdt.store.data {
            Some(f.value())
        } else {
            None
        }
    }

    fn values(&'a self) -> Option<impl Iterator<Item = &'a Primitive> + 'a> {
        if let Data::Reg(r) = &self.crdt.store.data {
            Some(r.read())
        } else {
            None
        }
    }

    /// Returns an iterator of bools.
    pub fn bools(&'a self) -> Option<impl Iterator<Item = bool> + 'a> {
        Some(self.values()?.filter_map(|p| {
            if let Primitive::Bool(v) = p {
                Some(*v)
            } else {
                None
            }
        }))
    }

    /// Returns an iterator of u64s.
    pub fn u64s(&'a self) -> Option<impl Iterator<Item = u64> + 'a> {
        Some(self.values()?.filter_map(|p| {
            if let Primitive::U64(v) = p {
                Some(*v)
            } else {
                None
            }
        }))
    }

    /// Returns an iterator of i64s.
    pub fn i64s(&'a self) -> Option<impl Iterator<Item = i64> + 'a> {
        Some(self.values()?.filter_map(|p| {
            if let Primitive::I64(v) = p {
                Some(*v)
            } else {
                None
            }
        }))
    }

    /// Returns an iterator of strs.
    pub fn strs(&'a self) -> Option<impl Iterator<Item = &'a str> + 'a> {
        Some(self.values()?.filter_map(|p| {
            if let Primitive::Str(v) = p {
                Some(v.as_str())
            } else {
                None
            }
        }))
    }

    /// Returns a cursor to a value in a table.
    pub fn key(&'a self, key: &'a Primitive) -> Option<Cursor<'a, ()>> {
        if let Data::Table(t) = &self.crdt.store.data {
            if let Some(crdt) = t.get(key) {
                return Some(Cursor {
                    crdt: self.crdt.map(crdt),
                    label: LabelRef::Key(&self.label, key),
                    engine: self.engine,
                    w: (),
                });
            }
        }
        None
    }

    /// Returns a cursor to a field in a struct.
    pub fn field(&'a self, key: &'a str) -> Option<Cursor<'a, ()>> {
        if let Data::Struct(fields) = &self.crdt.store.data {
            if let Some(crdt) = fields.get(key) {
                return Some(Cursor {
                    crdt: self.crdt.map(crdt),
                    label: LabelRef::Field(&self.label, key),
                    engine: self.engine,
                    w: (),
                });
            }
        }
        None
    }
}

impl<'a> Cursor<'a, W<'a>> {
    pub fn new(
        doc: DocId,
        crdt: &'a Causal,
        engine: &'a Engine,
        peer_id: PeerId,
        counter: u64,
        schema: &'a ArchivedSchema,
    ) -> Self {
        Self {
            label: LabelRef::Root(doc),
            crdt: crdt.as_ref(),
            engine,
            w: W {
                peer_id,
                counter: Rc::new(RefCell::new(counter)),
                schema,
            },
        }
    }

    fn mutate<F: FnOnce(CausalRefData) -> Option<CausalData>>(&self, f: F) -> Option<Causal> {
        if !self.can(self.w.peer_id, Permission::Write) {
            return None;
        }
        Some(f(self.crdt.map(&self.crdt.store.data))?.map(Crdt::new))
    }

    fn dot(&self) -> Dot {
        let mut counter = self.w.counter.borrow_mut();
        *counter += 1;
        Dot::new(self.w.peer_id, *counter)
    }

    /// Enables a flag.
    pub fn enable(&self) -> Option<Causal> {
        self.mutate(|data| {
            if let Data::Flag(f) = &data.store {
                Some(data.map(f).enable(self.dot()).map(Data::Flag))
            } else {
                None
            }
        })
    }

    /// Disables a flag.
    pub fn disable(&self) -> Option<Causal> {
        self.mutate(|data| {
            if let Data::Flag(f) = &data.store {
                Some(data.map(f).disable(self.dot()).map(Data::Flag))
            } else {
                None
            }
        })
    }

    /// Assigns a value to a register.
    pub fn assign(&self, value: impl Into<Primitive>) -> Option<Causal> {
        let value = value.into();
        match self.w.schema {
            ArchivedSchema::Reg(kind) if kind.validate(&value) => {}
            _ => return None,
        }
        self.mutate(|data| {
            if let Data::Reg(r) = &data.store {
                Some(data.map(r).write(self.dot(), value).map(Data::Reg))
            } else {
                None
            }
        })
    }

    /// Mutates the value for key in the table.
    pub fn key_mut<F>(&self, key: impl Into<Primitive>, mut f: F) -> Option<Causal>
    where
        F: FnMut(Cursor<'_, W>) -> Causal,
    {
        let key = key.into();
        let schema = match self.w.schema {
            ArchivedSchema::Table(kind, schema) if kind.validate(&key) => schema,
            _ => return None,
        };
        self.mutate(|data| {
            if let Data::Table(t) = &data.store {
                Some(
                    data.map(t)
                        .apply(
                            key.clone(),
                            |crdt| {
                                f(Cursor {
                                    crdt: self.crdt.map(crdt.store),
                                    label: LabelRef::Key(&self.label, &key),
                                    engine: self.engine,
                                    w: W {
                                        peer_id: self.w.peer_id,
                                        counter: self.w.counter.clone(),
                                        schema,
                                    },
                                })
                            },
                            || schema.default(),
                        )
                        .map(Data::Table),
                )
            } else {
                None
            }
        })
    }

    /// Mutates the field of a struct.
    pub fn field_mut<F>(&self, k: &'a str, mut f: F) -> Option<Causal>
    where
        F: FnMut(Cursor<'_, W>) -> Causal,
    {
        let schema = match self.w.schema {
            ArchivedSchema::Struct(fields) if fields.contains_key(k) => fields.get(k).unwrap(),
            _ => return None,
        };
        self.mutate(|data| {
            if let Data::Struct(fields) = &data.store {
                let default = schema.default();
                let crdt = if let Some(crdt) = fields.get(k) {
                    crdt
                } else {
                    &default
                };
                let cursor = Cursor {
                    crdt: self.crdt.map(crdt),
                    label: LabelRef::Field(&self.label, k),
                    engine: self.engine,
                    w: W {
                        peer_id: self.w.peer_id,
                        counter: self.w.counter.clone(),
                        schema,
                    },
                };
                Some(f(cursor).map(|field| {
                    let mut fields = BTreeMap::new();
                    fields.insert(k.to_string(), field);
                    Data::Struct(fields)
                }))
            } else {
                None
            }
        })
    }

    /// Gives permission to a peer.
    pub fn say_can(&self, actor: Option<PeerId>, perm: Permission) -> Option<Causal> {
        if !self.can(self.w.peer_id, Permission::Control) {
            return None;
        }
        if !perm.controllable() && !self.can(self.w.peer_id, Permission::Own) {
            return None;
        }
        Some(Crdt::say(self.dot(), Policy::Can(actor.into(), perm)))
    }

    /// Gives conditional permission to a peer.
    pub fn say_can_if(&self, actor: Actor, perm: Permission, cond: Can) -> Option<Causal> {
        if !self.can(self.w.peer_id, Permission::Control) {
            return None;
        }
        if !perm.controllable() && !self.can(self.w.peer_id, Permission::Own) {
            return None;
        }
        Some(Crdt::say(
            self.dot(),
            Policy::CanIf(actor.into(), perm, cond),
        ))
    }

    /// Revokes a policy.
    pub fn revoke(&self, claim: Dot) -> Option<Causal> {
        // TODO: check permission to revoke
        Some(Crdt::say(self.dot(), Policy::Revokes(claim)))
    }
}

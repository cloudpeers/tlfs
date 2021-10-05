use crate::{
    Actor, ArchivedSchema, Can, Causal, CausalContext, Crdt, DocId, Dot, Engine, PathBuf, PeerId,
    Permission, Policy, Primitive, PrimitiveKind,
};
use anyhow::{anyhow, Result};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone)]
pub struct W {
    peer_id: PeerId,
    counter: Rc<RefCell<u64>>,
    ctx: CausalContext,
}

//#[derive(Clone)]
pub struct Cursor<'a, T> {
    path: PathBuf,
    crdt: Crdt,
    engine: Engine,
    schema: &'a ArchivedSchema,
    w: T,
}

impl<'a> Cursor<'a, ()> {
    pub fn new(doc: DocId, crdt: Crdt, engine: Engine, schema: &'a ArchivedSchema) -> Self {
        Self {
            path: PathBuf::new(doc),
            crdt,
            engine,
            schema,
            w: (),
        }
    }
}

impl<'a, T> Cursor<'a, T> {
    /// Checks permissions.
    pub fn can(&self, peer: PeerId, perm: Permission) -> bool {
        self.engine.can(peer, perm, self.path.as_path())
    }

    /// Returns if a flag is enabled.
    pub fn enabled(&self) -> Result<bool> {
        if let ArchivedSchema::Flag = &self.schema {
            Ok(self.crdt.contains(self.path.as_path()))
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Returns an iterator of bools.
    pub fn bools(&self) -> Result<impl Iterator<Item = Result<bool>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::Bool) = &self.schema {
            Ok(self.crdt.primitives(self.path.as_path()).map(|prim| {
                if let Primitive::Bool(v) = prim?.to_owned()? {
                    Ok(v)
                } else {
                    Err(anyhow!("Reg<bool> contains invalid values"))
                }
            }))
        } else {
            Err(anyhow!("not a Reg<bool>"))
        }
    }

    /// Returns an iterator of u64s.
    pub fn u64s(&self) -> Result<impl Iterator<Item = Result<u64>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::U64) = &self.schema {
            Ok(self.crdt.primitives(self.path.as_path()).map(|prim| {
                if let Primitive::U64(v) = prim?.to_owned()? {
                    Ok(v)
                } else {
                    Err(anyhow!("Reg<u64> contains invalid values"))
                }
            }))
        } else {
            Err(anyhow!("not a Reg<u64>"))
        }
    }

    /// Returns an iterator of i64s.
    pub fn i64s(&self) -> Result<impl Iterator<Item = Result<i64>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::I64) = &self.schema {
            Ok(self.crdt.primitives(self.path.as_path()).map(|prim| {
                if let Primitive::I64(v) = prim?.to_owned()? {
                    Ok(v)
                } else {
                    Err(anyhow!("Reg<i64> contains invalid values"))
                }
            }))
        } else {
            Err(anyhow!("not a Reg<i64>"))
        }
    }

    /// Returns an iterator of strs.
    pub fn strs(&self) -> Result<impl Iterator<Item = Result<String>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::Str) = &self.schema {
            Ok(self.crdt.primitives(self.path.as_path()).map(|prim| {
                if let Primitive::Str(v) = prim?.to_owned()? {
                    Ok(v)
                } else {
                    Err(anyhow!("Reg<String> contains invalid values"))
                }
            }))
        } else {
            Err(anyhow!("not a Reg<String>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key(&mut self, key: &Primitive) -> Result<()> {
        if let ArchivedSchema::Table(kind, schema) = &self.schema {
            if kind.validate(key) {
                self.path.key(key);
                self.schema = schema;
                Ok(())
            } else {
                Err(anyhow!("invalid key"))
            }
        } else {
            Err(anyhow!("not a table"))
        }
    }

    /// Returns a cursor to a field in a struct.
    pub fn field(&mut self, key: &str) -> Result<()> {
        if let ArchivedSchema::Struct(fields) = &self.schema {
            if let Some(schema) = fields.get(key) {
                self.path.field(key);
                self.schema = schema;
                Ok(())
            } else {
                Err(anyhow!("field doesn't exist"))
            }
        } else {
            Err(anyhow!("not a struct"))
        }
    }
}

impl<'a> Cursor<'a, W> {
    pub fn new(
        doc: DocId,
        crdt: Crdt,
        ctx: CausalContext,
        engine: Engine,
        peer_id: PeerId,
        counter: u64,
        schema: &'a ArchivedSchema,
    ) -> Self {
        Self {
            path: PathBuf::new(doc),
            crdt,
            engine,
            schema,
            w: W {
                peer_id,
                counter: Rc::new(RefCell::new(counter)),
                ctx,
            },
        }
    }

    fn dot(&self) -> Dot {
        let mut counter = self.w.counter.borrow_mut();
        *counter += 1;
        Dot::new(self.w.peer_id, *counter)
    }

    /// Enables a flag.
    pub fn enable(&self) -> Result<Causal> {
        if let ArchivedSchema::Flag = &self.schema {
            if self.can(self.w.peer_id, Permission::Write) {
                self.crdt
                    .enable(self.path.as_path(), &self.w.ctx, self.dot())
            } else {
                Err(anyhow!("unauthorized"))
            }
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Disables a flag.
    pub fn disable(&self) -> Result<Causal> {
        if let ArchivedSchema::Flag = &self.schema {
            if self.can(self.w.peer_id, Permission::Write) {
                self.crdt
                    .disable(self.path.as_path(), &self.w.ctx, self.dot())
            } else {
                Err(anyhow!("unauthorized"))
            }
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Assigns a value to a register.
    pub fn assign(&self, value: impl Into<Primitive>) -> Result<Causal> {
        let value = value.into();
        if let ArchivedSchema::Reg(kind) = &self.schema {
            if kind.validate(&value) {
                if self.can(self.w.peer_id, Permission::Write) {
                    self.crdt
                        .assign(self.path.as_path(), &self.w.ctx, self.dot(), value)
                } else {
                    Err(anyhow!("unauthorized"))
                }
            } else {
                Err(anyhow!("invalid value"))
            }
        } else {
            Err(anyhow!("not a reg"))
        }
    }

    /// Gives permission to a peer.
    pub fn say_can(&self, actor: Option<PeerId>, perm: Permission) -> Result<Causal> {
        if !self.can(self.w.peer_id, Permission::Control) {
            return Err(anyhow!("unauthoried"));
        }
        if !perm.controllable() && !self.can(self.w.peer_id, Permission::Own) {
            return Err(anyhow!("unauthorized"));
        }
        self.crdt.say(
            self.path.as_path(),
            self.dot(),
            Policy::Can(actor.into(), perm),
        )
    }

    /// Gives conditional permission to a peer.
    pub fn say_can_if(&self, actor: Actor, perm: Permission, cond: Can) -> Result<Causal> {
        if !self.can(self.w.peer_id, Permission::Control) {
            return Err(anyhow!("unauthorized"));
        }
        if !perm.controllable() && !self.can(self.w.peer_id, Permission::Own) {
            return Err(anyhow!("unauthorized"));
        }
        self.crdt.say(
            self.path.as_path(),
            self.dot(),
            Policy::CanIf(actor, perm, cond),
        )
    }

    /// Revokes a policy.
    pub fn revoke(&self, claim: Dot) -> Result<Causal> {
        // TODO: check permission to revoke
        self.crdt
            .say(self.path.as_path(), self.dot(), Policy::Revokes(claim))
    }
}

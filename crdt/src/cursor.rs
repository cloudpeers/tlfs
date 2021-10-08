use crate::{
    Acl, Actor, ArchivedSchema, Can, Causal, Crdt, Docs, Dot, PathBuf, PeerId,
    Permission, Policy, Primitive, PrimitiveKind, Schema, Hash, DocId,
};
use anyhow::{anyhow, Result};
use rkyv::Archived;

#[derive(Clone)]
pub struct Cursor<'a> {
    id: DocId,
    schema_id: Hash,
    peer_id: PeerId,
    schema: &'a Archived<Schema>,
    path: PathBuf,
    crdt: &'a Crdt,
    docs: &'a Docs,
    acl: &'a Acl,
}

impl<'a> Cursor<'a> {
    pub fn new(
        id: DocId,
        schema_id: Hash,
        peer_id: PeerId,
        schema: &'a Archived<Schema>,
        crdt: &'a Crdt,
        docs: &'a Docs,
        acl: &'a Acl,
    ) -> Self {
        Self {
            id,
            schema_id,
            peer_id,
            schema,
            path: PathBuf::new(id),
            crdt,
            docs,
            acl,
        }
    }

    /// Checks permissions.
    pub fn can(&self, peer: PeerId, perm: Permission) -> Result<bool> {
        self.acl.can(peer, perm, self.path.as_path())
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
    pub fn key(mut self, key: &Primitive) -> Result<Self> {
        if let ArchivedSchema::Table(kind, schema) = &self.schema {
            if kind.validate(key) {
                self.path.key(key);
                self.schema = schema;
                Ok(self)
            } else {
                Err(anyhow!("invalid key"))
            }
        } else {
            Err(anyhow!("not a table"))
        }
    }

    /// Returns a cursor to a field in a struct.
    pub fn field(mut self, key: &str) -> Result<Self> {
        if let ArchivedSchema::Struct(fields) = &self.schema {
            if let Some(schema) = fields.get(key) {
                self.path.field(key);
                self.schema = schema;
                Ok(self)
            } else {
                Err(anyhow!("field doesn't exist"))
            }
        } else {
            Err(anyhow!("not a struct"))
        }
    }

    fn dot(&self) -> Result<Dot> {
        let counter = self.docs.increment(&self.id, &self.peer_id)?;
        Ok(Dot::new(self.peer_id, counter))
    }

    /// Enables a flag.
    pub fn enable(&self) -> Result<Causal> {
        if let ArchivedSchema::Flag = &self.schema {
            if self.can(self.peer_id, Permission::Write)? {
                self.crdt.enable(self.path.as_path(), self.ctx, self.dot()?)
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
            if self.can(self.peer_id, Permission::Write)? {
                self.crdt
                    .disable(self.path.as_path(), self.ctx, self.dot()?)
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
                if self.can(self.peer_id, Permission::Write)? {
                    self.crdt
                        .assign(self.path.as_path(), self.ctx, self.dot()?, value)
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

    /// Removes a value from a map.
    pub fn remove(&self, key: impl Into<Primitive>) -> Result<Causal> {
        let key = key.into();
        if let ArchivedSchema::Table(kind, _) = &self.schema {
            if kind.validate(&key) {
                if self.can(self.peer_id, Permission::Write)? {
                    self.crdt.remove(self.path.as_path(), self.ctx, self.dot()?)
                } else {
                    Err(anyhow!("unauthorized"))
                }
            } else {
                Err(anyhow!("invalid key"))
            }
        } else {
            Err(anyhow!("not a table"))
        }
    }

    /// Gives permission to a peer.
    pub fn say_can(&self, actor: Option<PeerId>, perm: Permission) -> Result<Causal> {
        if !self.can(self.peer_id, Permission::Control)? {
            return Err(anyhow!("unauthoried"));
        }
        if !perm.controllable() && !self.can(self.peer_id, Permission::Own)? {
            return Err(anyhow!("unauthorized"));
        }
        self.crdt.say(
            self.path.as_path(),
            self.ctx,
            self.dot()?,
            Policy::Can(actor.into(), perm),
        )
    }

    /// Gives conditional permission to a peer.
    pub fn say_can_if(&self, actor: Actor, perm: Permission, cond: Can) -> Result<Causal> {
        if !self.can(self.peer_id, Permission::Control)? {
            return Err(anyhow!("unauthorized"));
        }
        if !perm.controllable() && !self.can(self.peer_id, Permission::Own)? {
            return Err(anyhow!("unauthorized"));
        }
        self.crdt.say(
            self.path.as_path(),
            self.ctx,
            self.dot()?,
            Policy::CanIf(actor, perm, cond),
        )
    }

    /// Revokes a policy.
    pub fn revoke(&self, claim: Dot) -> Result<Causal> {
        // TODO: check permission to revoke
        self.crdt.say(
            self.path.as_path(),
            self.ctx,
            self.dot()?,
            Policy::Revokes(claim),
        )
    }
}

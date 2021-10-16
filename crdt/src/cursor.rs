use crate::{
    Actor, ArchivedSchema, Can, Causal, Crdt, DocId, Dot, Hash, PathBuf, PeerId, Permission,
    Policy, Primitive, PrimitiveKind, Schema, Writer,
};
use anyhow::{anyhow, Result};
use rkyv::Archived;

#[derive(Clone)]
pub struct Cursor<'a> {
    id: DocId,
    schema_id: Hash,
    writer: &'a Writer,
    schema: &'a Archived<Schema>,
    path: PathBuf,
    crdt: &'a Crdt,
}

impl<'a> Cursor<'a> {
    pub fn new(
        id: DocId,
        schema_id: Hash,
        writer: &'a Writer,
        schema: &'a Archived<Schema>,
        crdt: &'a Crdt,
    ) -> Self {
        Self {
            id,
            schema_id,
            writer,
            schema,
            path: PathBuf::new(id),
            crdt,
        }
    }

    /// Checks permissions.
    pub fn can(&self, peer: &PeerId, perm: Permission) -> Result<bool> {
        self.crdt.can(peer, perm, self.path.as_path())
    }

    /// Returns if a flag is enabled.
    pub fn enabled(&self) -> Result<bool> {
        if let ArchivedSchema::Flag = &self.schema {
            Ok(self
                .crdt
                .dotset(self.path.as_path())
                .next()
                .transpose()?
                .is_some())
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

    /// Enables a flag.
    pub fn enable(&self) -> Result<Causal> {
        if let ArchivedSchema::Flag = &self.schema {
            self.crdt.enable(self.path.as_path(), self.writer)
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Disables a flag.
    pub fn disable(&self) -> Result<Causal> {
        if let ArchivedSchema::Flag = &self.schema {
            self.crdt.disable(self.path.as_path(), self.writer)
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Assigns a value to a register.
    pub fn assign(&self, value: impl Into<Primitive>) -> Result<Causal> {
        let value = value.into();
        if let ArchivedSchema::Reg(kind) = &self.schema {
            if kind.validate(&value) {
                self.crdt.assign(self.path.as_path(), self.writer, value)
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
                self.crdt.remove(self.path.as_path(), self.writer)
            } else {
                Err(anyhow!("invalid key"))
            }
        } else {
            Err(anyhow!("not a table"))
        }
    }

    /// Gives permission to a peer.
    pub fn say_can(&self, actor: Option<PeerId>, perm: Permission) -> Result<Causal> {
        self.crdt.say(
            self.path.as_path(),
            self.writer,
            Policy::Can(actor.into(), perm),
        )
    }

    /// Constructs a new condition.
    pub fn cond(&self, actor: Actor, perm: Permission) -> Can {
        Can::new(actor, perm, self.path.clone())
    }

    /// Gives conditional permission to a peer.
    pub fn say_can_if(&self, actor: Actor, perm: Permission, cond: Can) -> Result<Causal> {
        self.crdt.say(
            self.path.as_path(),
            self.writer,
            Policy::CanIf(actor, perm, cond),
        )
    }

    /// Revokes a policy.
    pub fn revoke(&self, claim: Dot) -> Result<Causal> {
        self.crdt
            .say(self.path.as_path(), self.writer, Policy::Revokes(claim))
    }
}

    pub fn enable(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let dot = writer.dot();
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        Ok(Causal {
            store: DotStore::dotset(std::iter::once(dot)).prefix(path),
            doc,
            schema: schema.into(),
            expired: Default::default(),
        })
    }

    pub fn disable(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let mut expired = DotSet::default();
        // add all dots to be tombstoned into the context
        for i in self.state.scan_prefix(&path).keys() {
            let i = i?;
            let path = Path::new(&i);
            let dot = path.dot();
            let ty = path.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            expired.insert(dot);
        }
        Ok(Causal {
            store: DotStore::dotset([]).prefix(path),
            doc,
            expired,
            schema: schema.into(),
        })
    }

    pub fn is_enabled(&self, path: Path<'_>) -> bool {
        self.state.scan_prefix(path).next().is_some()
    }

    pub fn assign(&self, path: Path, writer: &Writer, v: Primitive) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let mut expired = DotSet::default();
        // add all dots to be tombstoned into the context
        for i in self.state.scan_prefix(&path).keys() {
            let i = i?;
            let path = Path::new(&i);
            let dot = path.dot();
            expired.insert(dot);
        }
        // add the new value into the context with a new dot
        let dot = writer.dot();
        Ok(Causal {
            store: DotStore::dotfun(std::iter::once((dot, v))).prefix(path),
            doc,
            schema: schema.into(),
            expired,
        })
    }

    pub fn values(&self, path: Path<'_>) -> impl Iterator<Item = sled::Result<Ref<Primitive>>> {
        self.state
            .scan_prefix(path)
            .values()
            .map(|res| res.map(Ref::new))
    }

    pub fn remove(&self, path: Path, writer: &Writer) -> Result<Causal> {
        if !self.can(writer.peer_id(), Permission::Write, path)? {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let mut expired = DotSet::default();
        let dot = writer.dot();
        expired.insert(dot);
        for res in self.state.scan_prefix(path).keys() {
            let key = res?;
            let key = Path::new(&key[..]);
            let ty = key.ty();
            if ty != Some(DotStoreType::Set) && ty != Some(DotStoreType::Fun) {
                continue;
            }
            let dot = key.dot();
            expired.insert(dot);
        }
        Ok(Causal {
            store: DotStore::default(),
            doc,
            expired,
            schema: schema.into(),
        })
    }

    pub fn say(&self, path: Path, writer: &Writer, policy: Policy) -> Result<Causal> {
        if !match &policy {
            Policy::Can(_, perm) | Policy::CanIf(_, perm, _) => {
                if perm.controllable() {
                    self.can(writer.peer_id(), Permission::Control, path)?
                } else {
                    self.can(writer.peer_id(), Permission::Own, path)?
                }
            }
            Policy::Revokes(_) => self.can(writer.peer_id(), Permission::Control, path)?,
        } {
            return Err(anyhow!("unauthorized"));
        }
        let doc = path.root().unwrap();
        let schema = self.docs.schema_id(&doc)?;
        let dot = writer.dot();
        Ok(Causal {
            store: DotStore::policy(std::iter::once((dot, policy))).prefix(path),
            doc,
            schema: schema.into(),
            expired: DotSet::default(),
        })
    }

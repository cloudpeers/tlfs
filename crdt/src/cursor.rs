use crate::{
    Actor, ArchivedSchema, Can, Causal, Crdt, DocId, Dot, DotSet, DotStore, Path, PathBuf, PeerId,
    Permission, Policy, PrimitiveKind, Schema,
};
use anyhow::{anyhow, Result};
use rkyv::Archived;

#[derive(Clone)]
pub struct Cursor<'a> {
    id: DocId,
    peer_id: PeerId,
    schema: &'a Archived<Schema>,
    crdt: &'a Crdt,
    path: PathBuf,
}

impl<'a> Cursor<'a> {
    pub fn new(id: DocId, peer_id: PeerId, schema: &'a Archived<Schema>, crdt: &'a Crdt) -> Self {
        let mut path = PathBuf::new();
        path.doc(&id);
        Self {
            id,
            schema,
            peer_id,
            path,
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
                .scan_prefix(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.nonce()?)),
                    Err(err) => Some(Err(err)),
                })
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
            Ok(self
                .crdt
                .scan_prefix(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_bool()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<bool>"))
        }
    }

    /// Returns an iterator of u64s.
    pub fn u64s(&self) -> Result<impl Iterator<Item = Result<u64>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::U64) = &self.schema {
            Ok(self
                .crdt
                .scan_prefix(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_u64()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<u64>"))
        }
    }

    /// Returns an iterator of i64s.
    pub fn i64s(&self) -> Result<impl Iterator<Item = Result<i64>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::I64) = &self.schema {
            Ok(self
                .crdt
                .scan_prefix(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_i64()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<i64>"))
        }
    }

    /// Returns an iterator of strs.
    pub fn strs(&self) -> Result<impl Iterator<Item = Result<String>> + '_> {
        if let ArchivedSchema::Reg(PrimitiveKind::Str) = &self.schema {
            Ok(self
                .crdt
                .scan_prefix(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_str()?.to_owned())),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<String>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_bool(mut self, key: bool) -> Result<Self> {
        if let ArchivedSchema::Table(PrimitiveKind::Bool, schema) = &self.schema {
            self.path.prim_bool(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<bool, _>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_u64(mut self, key: u64) -> Result<Self> {
        if let ArchivedSchema::Table(PrimitiveKind::U64, schema) = &self.schema {
            self.path.prim_u64(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<u64, _>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_i64(mut self, key: i64) -> Result<Self> {
        if let ArchivedSchema::Table(PrimitiveKind::I64, schema) = &self.schema {
            self.path.prim_i64(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<i64, _>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_str(mut self, key: &str) -> Result<Self> {
        if let ArchivedSchema::Table(PrimitiveKind::Str, schema) = &self.schema {
            self.path.prim_str(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<String, _>"))
        }
    }

    /// Returns a cursor to a field in a struct.
    pub fn field(mut self, key: &str) -> Result<Self> {
        if let ArchivedSchema::Struct(fields) = &self.schema {
            if let Some(schema) = fields.get(key) {
                self.path.prim_str(key);
                self.schema = schema;
                Ok(self)
            } else {
                Err(anyhow!("field doesn't exist"))
            }
        } else {
            Err(anyhow!("not a struct"))
        }
    }

    fn nonce(&self) -> Result<u64> {
        let mut nonce = [0; 8];
        getrandom::getrandom(&mut nonce)?;
        Ok(u64::from_le_bytes(nonce))
    }

    /// Enables a flag.
    pub fn enable(&self) -> Result<Causal> {
        if *self.schema != ArchivedSchema::Flag {
            return Err(anyhow!("not a flag"));
        }
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut path = self.path.to_owned();
        path.peer(&self.peer_id);
        path.nonce(self.nonce()?);
        let mut store = DotStore::new();
        store.insert(path);
        Ok(Causal {
            store,
            expired: Default::default(),
        })
    }

    /// Disables a flag.
    pub fn disable(&self) -> Result<Causal> {
        if *self.schema != ArchivedSchema::Flag {
            return Err(anyhow!("not a flag"));
        }
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut expired = DotSet::new();
        // add all dots to be tombstoned into the context
        for r in self.crdt.scan_prefix(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            if path.last().unwrap().nonce().is_some() {
                expired.insert(path.dot());
            }
        }
        Ok(Causal {
            store: DotStore::new(),
            expired,
        })
    }

    fn assign(&self, kind: PrimitiveKind) -> Result<(PathBuf, DotSet)> {
        if *self.schema != ArchivedSchema::Reg(kind) {
            return Err(anyhow!("not a Reg<{:?}>", kind));
        }
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut expired = DotSet::new();
        // add all dots to be tombstoned into the context
        for r in self.crdt.scan_prefix(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            if path.last().unwrap().is_prim() {
                expired.insert(path.dot());
            }
        }
        let mut path = self.path.to_owned();
        path.peer(&self.peer_id);
        path.nonce(self.nonce()?);
        Ok((path, expired))
    }

    /// Assigns a value to a register.
    pub fn assign_bool(&self, value: bool) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::Bool)?;
        let mut store = DotStore::new();
        path.prim_bool(value);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Assigns a value to a register.
    pub fn assign_u64(&self, value: u64) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::U64)?;
        let mut store = DotStore::new();
        path.prim_u64(value);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Assigns a value to a register.
    pub fn assign_i64(&self, value: i64) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::I64)?;
        let mut store = DotStore::new();
        path.prim_i64(value);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Assigns a value to a register.
    pub fn assign_str(&self, value: &str) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::Str)?;
        let mut store = DotStore::new();
        path.prim_str(value);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Removes a value from a map.
    pub fn remove(&self) -> Result<Causal> {
        if let ArchivedSchema::Table(PrimitiveKind::Bool, _) = &self.schema {
        } else {
            return Err(anyhow!("not a Table<bool, _>"));
        }
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut expired = DotSet::new();
        for r in self.crdt.scan_prefix(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            expired.insert(path.dot());
        }
        Ok(Causal {
            store: DotStore::new(),
            expired,
        })
    }

    fn say(&self, policy: &Policy) -> Result<Causal> {
        if !match &policy {
            Policy::Can(_, perm) | Policy::CanIf(_, perm, _) => {
                if perm.controllable() {
                    self.can(&self.peer_id, Permission::Control)?
                } else {
                    self.can(&self.peer_id, Permission::Own)?
                }
            }
            Policy::Revokes(_) => self.can(&self.peer_id, Permission::Control)?,
        } {
            return Err(anyhow!("unauthorized"));
        }
        let mut path = self.path.clone();
        path.peer(&self.peer_id);
        path.policy(policy);
        let mut store = DotStore::new();
        store.insert(path);
        Ok(Causal {
            store,
            expired: DotSet::new(),
        })
    }

    /// Gives permission to a peer.
    pub fn say_can(&self, actor: Option<PeerId>, perm: Permission) -> Result<Causal> {
        self.say(&Policy::Can(actor.into(), perm))
    }

    /// Constructs a new condition.
    pub fn cond(&self, actor: Actor, perm: Permission) -> Can {
        Can::new(actor, perm, self.path.clone())
    }

    /// Gives conditional permission to a peer.
    pub fn say_can_if(&self, actor: Actor, perm: Permission, cond: Can) -> Result<Causal> {
        self.say(&Policy::CanIf(actor, perm, cond))
    }

    /// Revokes a policy.
    pub fn revoke(&self, claim: Dot) -> Result<Causal> {
        self.say(&Policy::Revokes(claim))
    }
}

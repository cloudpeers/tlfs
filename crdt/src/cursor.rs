use crate::acl::{Actor, Can, Permission, Policy};
use crate::crdt::{Causal, Crdt, DotStore};
use crate::crypto::Keypair;
use crate::dotset::Dot;
use crate::id::{DocId, PeerId};
use crate::path::{Path, PathBuf};
use crate::schema::{ArchivedSchema, PrimitiveKind, Schema};
use crate::subscriber::Subscriber;
use anyhow::{anyhow, Result};
use rkyv::Archived;

#[derive(Clone)]
pub struct Cursor<'a> {
    key: Keypair,
    peer_id: PeerId,
    id: DocId,
    schema: &'a Archived<Schema>,
    crdt: &'a Crdt,
    path: PathBuf,
}

impl<'a> Cursor<'a> {
    pub fn new(key: Keypair, id: DocId, schema: &'a Archived<Schema>, crdt: &'a Crdt) -> Self {
        let mut path = PathBuf::new();
        path.doc(&id);
        Self {
            key,
            peer_id: key.peer_id(),
            id,
            schema,
            path,
            crdt,
        }
    }

    /// Subscribe to a path.
    pub fn subscribe(&self) -> Subscriber {
        self.crdt.watch_path(self.path.as_path())
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
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).parent()?.parent()?.last()?.nonce()?)),
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
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path)
                        .parent()?
                        .parent()?
                        .last()?
                        .prim_bool()?)),
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
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path)
                        .parent()?
                        .parent()?
                        .last()?
                        .prim_u64()?)),
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
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path)
                        .parent()?
                        .parent()?
                        .last()?
                        .prim_i64()?)),
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
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path)
                        .parent()?
                        .parent()?
                        .last()?
                        .prim_str()?
                        .to_owned())),
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

    fn nonce(&self, path: &mut PathBuf) {
        let mut nonce = [0; 8];
        getrandom::getrandom(&mut nonce).unwrap();
        let nonce = u64::from_le_bytes(nonce);
        path.nonce(nonce);
    }

    fn sign(&self, path: &mut PathBuf) {
        tracing::debug!("signing {} as {:?}", path.as_path(), self.peer_id);
        let sig = self.key.sign(path.as_ref());
        path.peer(&self.peer_id);
        path.sig(sig);
    }

    fn tombstone(&self) -> Result<DotStore> {
        let mut expired = DotStore::new();
        for r in self.crdt.scan_path(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            if path
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .last()
                .unwrap()
                .policy()
                .is_none()
            {
                let mut path = path.to_owned();
                self.sign(&mut path);
                expired.insert(path);
            }
        }
        Ok(expired)
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
        self.nonce(&mut path);
        self.sign(&mut path);
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
        Ok(Causal {
            store: DotStore::new(),
            expired: self.tombstone()?,
        })
    }

    fn assign(&self, kind: PrimitiveKind) -> Result<(PathBuf, DotStore)> {
        if *self.schema != ArchivedSchema::Reg(kind) {
            return Err(anyhow!("not a Reg<{:?}>", kind));
        }
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut path = self.path.to_owned();
        self.nonce(&mut path);
        Ok((path, self.tombstone()?))
    }

    /// Assigns a value to a register.
    pub fn assign_bool(&self, value: bool) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::Bool)?;
        let mut store = DotStore::new();
        path.prim_bool(value);
        self.sign(&mut path);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Assigns a value to a register.
    pub fn assign_u64(&self, value: u64) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::U64)?;
        let mut store = DotStore::new();
        path.prim_u64(value);
        self.sign(&mut path);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Assigns a value to a register.
    pub fn assign_i64(&self, value: i64) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::I64)?;
        let mut store = DotStore::new();
        path.prim_i64(value);
        self.sign(&mut path);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Assigns a value to a register.
    pub fn assign_str(&self, value: &str) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::Str)?;
        let mut store = DotStore::new();
        path.prim_str(value);
        self.sign(&mut path);
        store.insert(path);
        Ok(Causal { store, expired })
    }

    /// Removes a value from a map.
    pub fn remove(&self) -> Result<Causal> {
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        Ok(Causal {
            store: DotStore::new(),
            expired: self.tombstone()?,
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
        path.policy(policy);
        self.sign(&mut path);
        let mut store = DotStore::new();
        store.insert(path);
        Ok(Causal {
            store,
            expired: DotStore::new(),
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

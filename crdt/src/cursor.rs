use crate::acl::{Actor, Can, Permission, Policy};
use crate::crdt::{Causal, Crdt, DotStore};
use crate::crypto::Keypair;
use crate::dotset::Dot;
use crate::fraction::Fraction;
use crate::id::{DocId, PeerId};
use crate::path::{Path, PathBuf};
use crate::schema::{ArchivedSchema, PrimitiveKind, Schema};
use crate::subscriber::Subscriber;
use anyhow::{anyhow, Context, Result};
use rkyv::Archived;

#[derive(Clone, Debug)]
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
                .find_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.nonce()?)),
                    Err(err) => Some(Err(err)),
                })
                .is_some())
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Returns an iterator of bools.
    pub fn bools(&self) -> Result<impl Iterator<Item = Result<bool>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::Bool) = &self.schema {
            Ok(self
                .crdt
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_bool()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<bool>"))
        }
    }

    /// Returns an iterator of u64s.
    pub fn u64s(&self) -> Result<impl Iterator<Item = Result<u64>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::U64) = &self.schema {
            Ok(self
                .crdt
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_u64()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<u64>"))
        }
    }

    /// Returns an iterator of i64s.
    pub fn i64s(&self) -> Result<impl Iterator<Item = Result<i64>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::I64) = &self.schema {
            Ok(self
                .crdt
                .scan_path(self.path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_i64()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<i64>"))
        }
    }

    /// Returns an iterator of strs.
    pub fn strs(&self) -> Result<impl Iterator<Item = Result<String>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::Str) = &self.schema {
            Ok(self
                .crdt
                .scan_path(self.path.as_path())
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

    /// Returns a cursor to a value in an array.
    // [..].crdt.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    pub fn index(mut self, ix: usize) -> Result<ArrayCursor<'a>> {
        if let ArchivedSchema::Array(schema) = &self.schema {
            self.schema = schema;
            ArrayCursor::new(self, ix)
        } else {
            anyhow::bail!("not an Array<_>");
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

    fn nonce() -> Result<u64> {
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
        path.nonce(Self::nonce()?);
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
        let mut expired = DotStore::new();
        // add all dots to be tombstoned into the context
        for r in self.crdt.scan_path(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            if path.last().unwrap().nonce().is_some() {
                expired.insert(path.to_owned());
            }
        }
        Ok(Causal {
            store: DotStore::new(),
            expired,
        })
    }

    fn assign(&self, kind: PrimitiveKind) -> Result<(PathBuf, DotStore)> {
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        if *self.schema != ArchivedSchema::Reg(kind) {
            return Err(anyhow!("not a Reg<{:?}>", kind));
        }
        let mut expired = DotStore::new();
        // add all dots to be tombstoned into the context
        for r in self.crdt.scan_path(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            if path.last().unwrap().policy().is_none() {
                expired.insert(path.to_owned());
            }
        }
        let mut path = self.path.to_owned();
        path.peer(&self.peer_id);
        path.nonce(Self::nonce()?);
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
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let mut expired = DotStore::new();
        for r in self.crdt.scan_path(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            expired.insert(path.to_owned());
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

#[derive(Clone, Debug)]
// TODO: Can't yet nest Arrays
pub struct ArrayCursor<'a> {
    cursor: Cursor<'a>,
    array_path: PathBuf,
    pos: Fraction,
    uid: u64,
}

// FIXME: Nested cursors
impl<'a> ArrayCursor<'a> {
    fn meta_path(&self) -> PathBuf {
        let mut p = self.array_path.clone();
        p.prim_str(array::ARRAY_META);
        p
    }

    fn value_path(&self) -> PathBuf {
        let mut p = self.array_path.clone();
        p.prim_str(array::ARRAY_VALUES);
        p
    }

    fn value(&self) -> PathBuf {
        let mut p = self.value_path();
        p.position(&self.pos);
        p.prim_u64(self.uid);
        p
    }

    fn meta(&self) -> PathBuf {
        let mut p = self.meta_path();
        p.prim_u64(self.uid);
        p
    }

    fn new(cursor: Cursor<'a>, mut ix: usize) -> Result<ArrayCursor<'a>> {
        let mut p = cursor.path.clone();
        p.prim_str(array::ARRAY_VALUES);
        // TODO: use sled's size hint
        let len = cursor.crdt.scan_path(p.as_path()).count();
        let mut iter = cursor.crdt.scan_path(p.as_path());

        ix = ix.min(len);
        let (pos, uid) = if let Some(entry) = iter.nth(ix) {
            let entry = entry?;
            let p_c = cursor.path.clone();
            let data = array::ArrayValue::from_path(
                Path::new(&entry).strip_prefix(p_c.as_path())?.as_path(),
            )
            .context("Reading array data")?;
            (data.pos, data.uid)
        } else {
            // No entry :-(
            let (left, right) = match ix.checked_sub(1) {
                Some(s) => {
                    let p_c = cursor.path.clone();
                    let mut iter = cursor.crdt.scan_path(p.as_path()).skip(s).map(move |p| {
                        p.and_then(|iv| {
                            let meta = array::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                            )?;
                            Ok(meta.pos)
                        })
                    });
                    (iter.next(), iter.next())
                }
                None => {
                    let p_c = cursor.path.clone();
                    let mut iter = cursor.crdt.scan_path(p.as_path()).map(move |p| {
                        p.and_then(|iv| {
                            let meta = array::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                            )?;
                            Ok(meta.pos)
                        })
                    });

                    (None, iter.next())
                }
            };

            let left = left.transpose()?.unwrap_or_else(Fraction::zero);
            let pos = if let Some(r) = right.transpose()? {
                left.mid(&r)
            } else {
                left.succ()
            };
            (pos, Cursor::nonce()?)
        };

        Ok(Self {
            array_path: cursor.path.clone(),
            cursor,
            pos,
            uid,
        })
    }
    // [..].crdt.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    pub fn r#move(self, mut to: usize) -> Result<Causal> {
        // On a Move, the replica deletes all children of all existing roots, and adds a single
        // child tree to all roots with the new position.

        println!("move: {}", self.cursor.path);
        let new_pos = {
            let value_path = self.value_path();
            // TODO: use sled's size hint
            let len = self.cursor.crdt.scan_path(value_path.as_path()).count();

            to = to.min(len);
            let (left, right) = match to.checked_sub(1) {
                Some(s) => {
                    let p_c = self.array_path.clone();
                    let mut iter = self
                        .cursor
                        .crdt
                        .scan_path(value_path.as_path())
                        .skip(s)
                        .map(move |p| {
                            p.and_then(|iv| {
                                let meta = array::ArrayValue::from_path(
                                    Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                                )?;
                                Ok(meta.pos)
                            })
                        });
                    (iter.next(), iter.next())
                }
                None => {
                    let p_c = self.array_path.clone();
                    let mut iter = self
                        .cursor
                        .crdt
                        .scan_path(value_path.as_path())
                        .map(move |p| {
                            p.and_then(|iv| {
                                let meta = array::ArrayValue::from_path(
                                    Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                                )?;
                                Ok(meta.pos)
                            })
                        });

                    (None, iter.next())
                }
            };

            println!("left {:?}, right {:?}", left, right);
            let left = left.transpose()?.unwrap_or_else(Fraction::zero);
            if let Some(r) = right.transpose()? {
                left.mid(&r)
            } else {
                left.succ()
            }
        };
        // --
        let meta_path_with_uid = {
            let mut p = self.meta_path();
            p.prim_u64(self.uid);
            p
        };
        let existing_meta = self
            .cursor
            .crdt
            .scan_path(meta_path_with_uid.as_path())
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(!existing_meta.is_empty(), "Value does not exist!");

        let mut store = DotStore::new();
        let mut expired = DotStore::new();
        let move_op = Cursor::nonce()?;
        for e in existing_meta {
            let p = Path::new(&e);
            expired.insert(p.to_owned());

            let meta = self.get_meta(p)?;
            let mut path = meta_path_with_uid.clone();
            path.prim_u64(meta.last_update);
            path.prim_u64(move_op);
            path.position(&meta.pos);
            path.peer(&self.cursor.peer_id);
            path.nonce(Cursor::nonce()?);

            store.insert(path);
        }
        let old_value_path = {
            let mut p = self.value_path();

            p.position(&self.pos);
            p
        };
        let mut new_value_path = self.value_path();
        // remove old pos
        let old = self
            .cursor
            .crdt
            .scan_path(old_value_path.as_path())
            .next()
            .expect("non empty")?;
        let p = Path::new(&old);
        expired.insert(p.to_owned());
        let v = self.get_value(p)?;

        // add new pos
        println!("new pos: {:?}", new_pos.as_ref());
        new_value_path.position(&new_pos);
        new_value_path.prim_u64(self.uid);
        new_value_path.peer(&self.cursor.peer_id);
        new_value_path.nonce(Cursor::nonce()?);
        new_value_path.push_segment(v.value);
        store.insert(new_value_path);

        Ok(Causal { store, expired })
    }

    fn get_meta(&self, path: Path) -> Result<array::ArrayMeta> {
        array::ArrayMeta::from_path(path.strip_prefix(self.array_path.as_path())?.as_path())
    }

    fn get_value(&self, path: Path<'_>) -> Result<array::ArrayValue> {
        array::ArrayValue::from_path(path.strip_prefix(self.array_path.as_path())?.as_path())
    }

    // [..].meta.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    fn insert(&self, mut inner: Causal) -> Result<Causal> {
        let mut p = self.meta();
        p.prim_u64(Cursor::nonce()?);
        p.prim_u64(Cursor::nonce()?);
        p.position(&self.pos);
        p.peer(&self.cursor.peer_id);
        p.nonce(Cursor::nonce()?);
        inner.store.insert(p);
        Ok(inner)
    }

    pub fn delete(&self) -> Result<Causal> {
        let mut expired = DotStore::default();

        for e in self
            .cursor
            .crdt
            .scan_path(self.value().as_path())
            .chain(self.cursor.crdt.scan_path(self.meta().as_path()))
        {
            let e = e?;
            expired.insert(Path::new(&e).to_owned());
        }
        Ok(Causal {
            expired,
            store: Default::default(),
        })
    }

    // [..].meta.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    fn update(&self, mut inner: Causal) -> Result<Causal> {
        // On an Update, besides updating the value of the top level pair, the replica also recommits the
        // current position of that element. This is done by deleting all observed roots from the forest
        // and adding a single tree of height 3 with the current position. This position is chosen
        // deterministically from the set of current possible positions.
        // update VALUES
        for e in self.cursor.crdt.scan_path(self.value().as_path()) {
            let e = e?;
            inner.expired.insert(Path::new(&e).to_owned());
        }
        // clean up meta
        for e in self.cursor.crdt.scan_path(self.meta().as_path()) {
            let e = e?;
            inner.expired.insert(Path::new(&e).to_owned());
        }

        // Commit current position
        let mut p = self.meta();
        p.prim_u64(Cursor::nonce()?);
        p.prim_u64(Cursor::nonce()?);
        p.position(&self.pos);
        p.peer(&self.cursor.peer_id);
        p.nonce(Cursor::nonce()?);
        inner.store.insert(p);
        Ok(inner)
    }

    fn augment_causal(&self, inner: Causal) -> Result<Causal> {
        if self
            .cursor
            .crdt
            .scan_path(self.value().as_path())
            .next()
            .is_some()
        {
            self.update(inner)
        } else {
            self.insert(inner)
        }
    }

    // Copied Cursor API
    /// Subscribe to a path.
    pub fn subscribe(&self) -> Subscriber {
        self.cursor.subscribe()
    }

    /// Checks permissions.
    pub fn can(&self, peer: &PeerId, perm: Permission) -> Result<bool> {
        self.cursor.can(peer, perm)
    }

    /// Returns if a flag is enabled.
    pub fn enabled(&self) -> Result<bool> {
        Cursor {
            path: self.value(),
            ..self.cursor
        }
        .enabled()
    }

    /// Returns an iterator of bools.
    pub fn bools(&self) -> Result<impl Iterator<Item = Result<bool>>> {
        Cursor {
            path: self.value(),
            ..self.cursor
        }
        .bools()
    }

    /// Returns an iterator of u64s.
    pub fn u64s(&self) -> Result<impl Iterator<Item = Result<u64>>> {
        Cursor {
            path: self.value(),
            ..self.cursor
        }
        .u64s()
    }

    /// Returns an iterator of i64s.
    pub fn i64s(&self) -> Result<impl Iterator<Item = Result<i64>>> {
        Cursor {
            path: self.value(),
            ..self.cursor
        }
        .i64s()
    }

    /// Returns an iterator of strs.
    pub fn strs(&self) -> Result<impl Iterator<Item = Result<String>>> {
        Cursor {
            path: self.value(),
            ..self.cursor
        }
        .strs()
    }

    /// Returns a cursor to a value in a table.
    pub fn key_bool(mut self, key: bool) -> Result<Self> {
        self.cursor = self.cursor.key_bool(key)?;
        Ok(self)
    }

    /// Returns a cursor to a value in a table.
    pub fn key_u64(mut self, key: u64) -> Result<Self> {
        self.cursor = self.cursor.key_u64(key)?;
        Ok(self)
    }

    /// Returns a cursor to a value in a table.
    pub fn key_i64(mut self, key: i64) -> Result<Self> {
        self.cursor = self.cursor.key_i64(key)?;
        Ok(self)
    }

    /// Returns a cursor to a value in a table.
    pub fn key_str(mut self, key: &str) -> Result<Self> {
        self.cursor = self.cursor.key_str(key)?;
        Ok(self)
    }

    /// Returns a cursor to a field in a struct.
    pub fn field(mut self, key: &str) -> Result<Self> {
        self.cursor = self.cursor.field(key)?;
        Ok(self)
    }

    /// Enables a flag.
    pub fn enable(&self) -> Result<Causal> {
        let inner = Cursor {
            path: self.value(),
            ..self.cursor
        }
        .enable()?;
        self.augment_causal(inner)
    }

    /// Disables a flag.
    pub fn disable(&self) -> Result<Causal> {
        let inner = Cursor {
            path: self.value(),
            ..self.cursor
        }
        .disable()?;
        self.augment_causal(inner)
    }

    fn assign(&self, kind: PrimitiveKind) -> Result<(PathBuf, DotStore)> {
        Cursor {
            path: self.value(),
            ..self.cursor
        }
        .assign(kind)
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
        let inner = Cursor {
            path: self.value(),
            ..self.cursor
        }
        .remove()?;
        self.augment_causal(inner)
    }

    fn say(&self, policy: &Policy) -> Result<Causal> {
        let inner = Cursor {
            path: self.value(),
            ..self.cursor
        }
        .say(policy)?;
        self.augment_causal(inner)
    }

    /// Gives permission to a peer.
    pub fn say_can(&self, actor: Option<PeerId>, perm: Permission) -> Result<Causal> {
        self.say(&Policy::Can(actor.into(), perm))
    }

    /// Constructs a new condition.
    pub fn cond(&self, actor: Actor, perm: Permission) -> Can {
        Can::new(actor, perm, self.cursor.path.clone())
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

mod array {
    use anyhow::Context;

    use crate::Segment;

    use super::*;
    // [..].crdt.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    pub(crate) struct ArrayValue {
        pub(crate) uid: u64,
        pub(crate) pos: Fraction,
        pub(crate) value: Segment,
    }
    impl ArrayValue {
        /// `path` needs to point into the array root dir
        pub(crate) fn from_path(path: Path<'_>) -> Result<ArrayValue> {
            println!("{}", path);
            let mut path = path.into_iter();
            anyhow::ensure!(
                path.next()
                    .context("Unexpected layout")?
                    .prim_str()
                    .context("Unexpected layout")?
                    == ARRAY_VALUES,
                "Unexpected layout"
            );

            let pos = path
                .next()
                .context("Unexpected layout")?
                .position()
                .context("Unexpected layout")?;

            let uid = path
                .next()
                .context("Unexpected layout")?
                .prim_u64()
                .context("Unexpected layout")?;

            // peer
            path.next();
            // nonce
            path.next();

            let value = path.next().context("Unexpected layout")?;
            Ok(Self { uid, pos, value })
        }
    }
    pub(crate) struct ArrayMeta {
        pub(crate) last_update: u64,
        pub(crate) last_move: u64,
        pub(crate) uid: u64,
        pub(crate) pos: Fraction,
    }
    pub(crate) const ARRAY_VALUES: &str = "VALUES";
    pub(crate) const ARRAY_META: &str = "META";
    impl ArrayMeta {
        /// `path` needs to point into the array root dir
        pub(crate) fn from_path(path: Path) -> Result<Self> {
            println!("ArrayMeta::from_path: {}", path);
            let mut path = path.into_iter();
            anyhow::ensure!(
                path.next()
                    .context("Unexpected layout")?
                    .prim_str()
                    .context("Unexpected layout")?
                    == ARRAY_META,
                "Unexpected layout"
            );
            let uid = path
                .next()
                .context("Unexpected layout")?
                .prim_u64()
                .context("Unexpected layout")?;
            let last_update = path
                .next()
                .context("Unexpected layout")?
                .prim_u64()
                .context("Unexpected layout")?;

            let last_move = path
                .next()
                .context("Unexpected layout")?
                .prim_u64()
                .context("Unexpected layout")?;

            let pos = path
                .next()
                .context("Unexpected layout")?
                .position()
                .context("Unexpected layout")?;
            Ok(Self {
                last_update,
                last_move,
                pos,
                uid,
            })
        }
    }
}

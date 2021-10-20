use crate::{
    fraction::Fraction, Actor, ArchivedSchema, Can, Causal, Crdt, DocId, Dot, DotSet, DotStore,
    Path, PathBuf, PeerId, Permission, Policy, PrimitiveKind, Schema,
};
use anyhow::{anyhow, Context, Result};
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
                .find_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.nonce()?)),
                    Err(err) => Some(Err(err)),
                })
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
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        if *self.schema != ArchivedSchema::Reg(kind) {
            return Err(anyhow!("not a Reg<{:?}>", kind));
        }
        let mut expired = DotSet::new();
        // add all dots to be tombstoned into the context
        for r in self.crdt.scan_prefix(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            if path.last().unwrap().policy().is_none() {
                expired.insert(path.dot());
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
        let mut expired = DotSet::new();
        for r in self.crdt.scan_prefix(self.path.as_path()) {
            let k = r?;
            let path = Path::new(&k);
            // TODO: policy?
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

#[derive(Clone)]
pub struct ArrayCursor<'a> {
    id: DocId,
    peer_id: PeerId,
    schema: &'a Archived<Schema>,
    crdt: &'a Crdt,
    base: PathBuf,
    pos: Fraction,
    uid: u64,
}

// FIXME: Nested cursors
impl<'a> ArrayCursor<'a> {
    fn meta_path(&self) -> PathBuf {
        let mut p = self.base.clone();
        p.prim_str(array::ARRAY_META);
        p
    }

    fn value_path(&self) -> PathBuf {
        let mut p = self.base.clone();
        p.prim_str(array::ARRAY_VALUES);
        p
    }

    fn value(&self) -> PathBuf {
        let mut p = self.value_path();
        p.slice(self.pos.as_bytes());
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
        let len = cursor.crdt.scan_prefix(p.as_path()).count();
        let mut iter = cursor.crdt.scan_prefix(p.as_path());

        ix = ix.min(len);
        let (pos, uid) = if let Some(entry) = iter.nth(ix) {
            let entry = entry?;
            let p_c = cursor.path.clone();
            let data = array::ArrayValue::from_path(Path::new(&entry).strip_prefix(p_c.as_path())?)
                .context("Reading array data")?;
            (data.pos, data.uid)
        } else {
            // No entry :-(
            let (left, right) = match ix.checked_sub(1) {
                Some(s) => {
                    let p_c = cursor.path.clone();
                    let mut iter = cursor.crdt.scan_prefix(p.as_path()).skip(s).map(move |p| {
                        p.and_then(|iv| {
                            let meta = array::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?,
                            )?;
                            Ok(meta.pos)
                        })
                    });
                    (iter.next(), iter.next())
                }
                None => {
                    let p_c = cursor.path.clone();
                    let mut iter = cursor.crdt.scan_prefix(p.as_path()).map(move |p| {
                        p.and_then(|iv| {
                            let meta = array::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?,
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
            id: cursor.id,
            peer_id: cursor.peer_id,
            schema: cursor.schema,
            crdt: cursor.crdt,
            base: cursor.path,
            pos,
            uid,
        })
    }
    // [..].crdt.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    pub fn r#move(self, mut to: usize) -> Result<Causal> {
        // On a Move, the replica deletes all children of all existing roots, and adds a single
        // child tree to all roots with the new position.

        println!("move: {}", self.base);
        let new_pos = {
            let value_path = self.value_path();
            // TODO: use sled's size hint
            let len = self.crdt.scan_prefix(value_path.as_path()).count();

            to = to.min(len);
            let (left, right) = match to.checked_sub(1) {
                Some(s) => {
                    let p_c = self.base.clone();
                    let mut iter =
                        self.crdt
                            .scan_prefix(value_path.as_path())
                            .skip(s)
                            .map(move |p| {
                                p.and_then(|iv| {
                                    let meta = array::ArrayValue::from_path(
                                        Path::new(&iv).strip_prefix(p_c.as_path())?,
                                    )?;
                                    Ok(meta.pos)
                                })
                            });
                    (iter.next(), iter.next())
                }
                None => {
                    let p_c = self.base.clone();
                    let mut iter = self.crdt.scan_prefix(value_path.as_path()).map(move |p| {
                        p.and_then(|iv| {
                            let meta = array::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?,
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
            .crdt
            .scan_prefix(meta_path_with_uid.as_path())
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(!existing_meta.is_empty(), "Value does not exist!");

        let mut store = DotStore::new();
        let mut expired = DotSet::new();
        let move_op = Cursor::nonce()?;
        for e in existing_meta {
            let p = Path::new(&e);
            expired.insert(p.dot());

            let meta = self.get_meta(p)?;
            let mut path = meta_path_with_uid.clone();
            path.prim_u64(meta.last_update);
            path.prim_u64(move_op);
            path.slice(meta.pos.as_ref());
            path.peer(&self.peer_id);
            path.nonce(Cursor::nonce()?);

            store.insert(path);
        }
        let old_value_path = {
            let mut p = self.value_path();

            p.slice(self.pos.as_ref());
            p
        };
        let mut new_value_path = self.value_path();
        // remove old pos
        let old = self
            .crdt
            .scan_prefix(old_value_path.as_path())
            .next()
            .expect("non empty")?;
        let p = Path::new(&old);
        expired.insert(p.dot());
        let v = self.get_value(p)?;

        // add new pos
        println!("new pos: {:?}", new_pos.as_ref());
        new_value_path.slice(new_pos.as_ref());
        new_value_path.prim_u64(self.uid);
        new_value_path.peer(&self.peer_id);
        new_value_path.nonce(Cursor::nonce()?);
        new_value_path.push_segment(&v.value);
        store.insert(new_value_path);

        Ok(Causal { store, expired })
    }

    fn get_meta(&self, path: Path) -> Result<array::ArrayMeta> {
        array::ArrayMeta::from_path(path.strip_prefix(self.base.as_path())?)
    }

    fn get_value(&'a self, path: Path<'a>) -> Result<array::ArrayValue<'a>> {
        array::ArrayValue::from_path(path.strip_prefix(self.base.as_path())?)
    }

    // [..].meta.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    fn insert(&self, mut inner: Causal) -> Result<Causal> {
        let mut p = self.meta();
        p.prim_u64(Cursor::nonce()?);
        p.prim_u64(Cursor::nonce()?);
        p.slice(self.pos.as_ref());
        p.peer(&self.peer_id);
        p.nonce(Cursor::nonce()?);
        inner.store.insert(p);
        Ok(inner)
    }

    pub fn delete(&self) -> Result<Causal> {
        let mut expired = DotSet::default();

        for e in self
            .crdt
            .scan_prefix(self.value().as_path())
            .chain(self.crdt.scan_prefix(self.meta().as_path()))
        {
            let e = e?;
            expired.insert(Path::new(&e).dot());
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
        for e in self.crdt.scan_prefix(self.value().as_path()) {
            let e = e?;
            inner.expired.insert(Path::new(&e).dot());
        }
        // clean up meta
        for e in self.crdt.scan_prefix(self.meta().as_path()) {
            let e = e?;
            inner.expired.insert(Path::new(&e).dot());
        }

        // Commit current position
        let mut p = self.meta();
        p.prim_u64(Cursor::nonce()?);
        p.prim_u64(Cursor::nonce()?);
        p.slice(self.pos.as_ref());
        p.peer(&self.peer_id);
        p.nonce(Cursor::nonce()?);
        inner.store.insert(p);
        Ok(inner)
    }

    /// Assigns a value to a register.
    pub fn assign_u64(&self, value: u64) -> Result<Causal> {
        let path = self.value();
        let inner = Cursor {
            id: self.id,
            peer_id: self.peer_id,
            schema: self.schema,
            crdt: self.crdt,
            path,
        }
        .assign_u64(value)?;
        if self
            .crdt
            .scan_prefix(self.value_path().as_path())
            .next()
            .is_some()
        {
            self.update(inner)
        } else {
            self.insert(inner)
        }
    }

    pub fn u64s(&self) -> Result<impl Iterator<Item = Result<u64>> + '_> {
        // TODO DRY with `Cursor`
        let path = self.value();
        println!("{} {:?}", self.pos, path);

        if let ArchivedSchema::Reg(PrimitiveKind::U64) = &self.schema {
            Ok(self
                .crdt
                .scan_prefix(path.as_path())
                .filter_map(|r| match r {
                    Ok(path) => Some(Ok(Path::new(&path).last()?.prim_u64()?)),
                    Err(err) => Some(Err(err)),
                }))
        } else {
            Err(anyhow!("not a Reg<u64>"))
        }
    }
}

mod array {
    use crate::Segment;

    use super::*;
    // [..].crdt.<uid>.<last_update>.<last_move>.<pos>.<peer>.<nonce>
    // [..].values.<pos>.<uid>.<value>.<peer>.<nonce>
    pub(crate) struct ArrayValue<'a> {
        pub(crate) uid: u64,
        pub(crate) pos: Fraction,
        pub(crate) value: Segment<'a>,
    }
    impl<'a> ArrayValue<'a> {
        /// `path` needs to point into the array root dir
        pub(crate) fn from_path(mut path: Path<'a>) -> Result<ArrayValue<'a>> {
            println!("{}", path);
            anyhow::ensure!(
                path.next()
                    .context("Unexpected layout")?
                    .prim_str()
                    .context("Unexpected layout")?
                    == ARRAY_VALUES,
                "Unexpected layout"
            );

            let pos = Fraction::new(
                path.next()
                    .context("Unexpected layout")?
                    .slice()
                    .context("Unexpected layout")?
                    .into(),
            );

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
        pub(crate) fn from_path(mut path: Path) -> Result<Self> {
            println!("ArrayMeta::from_path: {}", path);
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

            let pos = Fraction::new(
                path.next()
                    .context("Unexpected layout")?
                    .slice()
                    .context("Unexpected layout")?
                    .into(),
            );
            Ok(Self {
                last_update,
                last_move,
                pos,
                uid,
            })
        }
    }
}

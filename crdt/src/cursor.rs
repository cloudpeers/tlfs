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
use smallvec::SmallVec;

/// A cursor into a document used to construct transactions.
#[derive(Clone, Debug)]
pub struct Cursor<'a> {
    key: Keypair,
    peer_id: PeerId,
    /// The [`Schema`] this [`Cursor`] is pointing to.
    schema: &'a Archived<Schema>,
    crdt: &'a Crdt,
    /// The path this [`Cursor`] is pointing to.
    path: PathBuf,
    /// Helpers to work with nested ORArrays.
    array: SmallVec<[ArrayWrapper; 1]>,
}

impl<'a> Cursor<'a> {
    /// Creates a new [`Cursor`].
    pub fn new(key: Keypair, id: DocId, schema: &'a Archived<Schema>, crdt: &'a Crdt) -> Self {
        let mut path = PathBuf::new();
        path.doc(&id);
        Self {
            key,
            peer_id: key.peer_id(),
            schema,
            path,
            crdt,
            array: Default::default(),
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
                .find_map(|k| Path::new(&k).parent()?.parent()?.last()?.nonce())
                .is_some())
        } else {
            Err(anyhow!("not a flag"))
        }
    }

    /// Returns an iterator of bools.
    pub fn bools(&self) -> Result<impl Iterator<Item = Result<bool>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::Bool) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|path| {
                Some(Ok(Path::new(&path)
                    .parent()?
                    .parent()?
                    .last()?
                    .prim_bool()?))
            }))
        } else {
            Err(anyhow!("not a Reg<bool>"))
        }
    }

    /// Returns an iterator of u64s.
    pub fn u64s(&self) -> Result<impl Iterator<Item = Result<u64>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::U64) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|path| {
                Some(Ok(Path::new(&path)
                    .parent()?
                    .parent()?
                    .last()?
                    .prim_u64()?))
            }))
        } else {
            Err(anyhow!("not a Reg<u64>"))
        }
    }

    /// Returns an iterator of i64s.
    pub fn i64s(&self) -> Result<impl Iterator<Item = Result<i64>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::I64) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|path| {
                Some(Ok(Path::new(&path)
                    .parent()?
                    .parent()?
                    .last()?
                    .prim_i64()?))
            }))
        } else {
            Err(anyhow!("not a Reg<i64>"))
        }
    }

    /// Returns an iterator of strs.
    pub fn strs(&self) -> Result<impl Iterator<Item = Result<String>>> {
        if let ArchivedSchema::Reg(PrimitiveKind::Str) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|path| {
                Some(Ok(Path::new(&path)
                    .parent()?
                    .parent()?
                    .last()?
                    .prim_str()?
                    .to_owned()))
            }))
        } else {
            Err(anyhow!("not a Reg<String>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_bool(&mut self, key: bool) -> Result<&mut Self> {
        if let ArchivedSchema::Table(PrimitiveKind::Bool, schema) = &self.schema {
            self.path.prim_bool(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<bool, _>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_u64(&mut self, key: u64) -> Result<&mut Self> {
        if let ArchivedSchema::Table(PrimitiveKind::U64, schema) = &self.schema {
            self.path.prim_u64(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<u64, _>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_i64(&mut self, key: i64) -> Result<&mut Self> {
        if let ArchivedSchema::Table(PrimitiveKind::I64, schema) = &self.schema {
            self.path.prim_i64(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<i64, _>"))
        }
    }

    /// Returns a cursor to a value in a table.
    pub fn key_str(&mut self, key: &str) -> Result<&mut Self> {
        if let ArchivedSchema::Table(PrimitiveKind::Str, schema) = &self.schema {
            self.path.prim_str(key);
            self.schema = schema;
            Ok(self)
        } else {
            Err(anyhow!("not a Table<String, _>"))
        }
    }

    /// Returns a cursor to a value in an array.
    pub fn index(&mut self, ix: usize) -> Result<&mut Self> {
        if let ArchivedSchema::Array(schema) = &self.schema {
            self.schema = schema;
            let (array, path) = ArrayWrapper::new(self, ix)?;
            self.array.push(array);
            self.path = path;
            Ok(self)
        } else {
            anyhow::bail!("not an Array<_>");
        }
    }

    /// Returns the length of the array.
    pub fn len(&mut self) -> Result<u32> {
        if let ArchivedSchema::Array(_) = &self.schema {
            self.path.prim_str(array_util::ARRAY_VALUES);
            let res = self.count_path(self.path.as_path());
            self.path.pop();
            res
        } else {
            anyhow::bail!("not an Array<_>");
        }
    }

    /// Returns the schema this cursor is pointing at.
    pub fn schema(&self) -> &ArchivedSchema {
        self.schema
    }

    /// Returns if the array is empty.
    pub fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns a cursor to a field in a struct.
    pub fn field(&mut self, key: &str) -> Result<&mut Self> {
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

    fn count_path(&self, path: Path) -> Result<u32> {
        let mut i = 0;
        for _ in self.crdt.scan_path(path) {
            i += 1;
        }
        Ok(i)
    }

    fn nonce(&self, path: &mut PathBuf) {
        path.nonce(nonce());
    }

    fn sign(&self, path: &mut PathBuf) {
        tracing::debug!("signing {} as {:?}", path.as_path(), self.peer_id);
        let sig = self.key.sign(path.as_ref());
        path.peer(&self.peer_id);
        path.sig(sig);
    }

    fn tombstone(&self) -> Result<DotStore> {
        let mut expired = DotStore::new();
        for k in self.crdt.scan_path(self.path.as_path()) {
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
        let c = Causal {
            store,
            expired: Default::default(),
        };
        self.augment_array(c)
    }

    /// Disables a flag.
    pub fn disable(&self) -> Result<Causal> {
        if *self.schema != ArchivedSchema::Flag {
            return Err(anyhow!("not a flag"));
        }
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }

        let c = Causal {
            store: DotStore::new(),
            expired: self.tombstone()?,
        };
        self.augment_array(c)
    }

    fn assign(&self, kind: PrimitiveKind) -> Result<(PathBuf, DotStore)> {
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        if *self.schema != ArchivedSchema::Reg(kind) {
            return Err(anyhow!("not a Reg<{:?}>", kind));
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

        let c = Causal { store, expired };
        self.augment_array(c)
    }

    /// Assigns a value to a register.
    pub fn assign_u64(&self, value: u64) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::U64)?;
        let mut store = DotStore::new();
        path.prim_u64(value);
        self.sign(&mut path);
        store.insert(path);

        let c = Causal { store, expired };
        self.augment_array(c)
    }

    /// Assigns a value to a register.
    pub fn assign_i64(&self, value: i64) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::I64)?;
        let mut store = DotStore::new();
        path.prim_i64(value);
        self.sign(&mut path);
        store.insert(path);

        let c = Causal { store, expired };
        self.augment_array(c)
    }

    /// Assigns a value to a register.
    pub fn assign_str(&self, value: &str) -> Result<Causal> {
        let (mut path, expired) = self.assign(PrimitiveKind::Str)?;
        let mut store = DotStore::new();
        path.prim_str(value);
        self.sign(&mut path);
        store.insert(path);

        let c = Causal { store, expired };
        self.augment_array(c)
    }

    /// Removes a value from a map.
    pub fn remove(&self) -> Result<Causal> {
        if !self.can(&self.peer_id, Permission::Write)? {
            return Err(anyhow!("unauthorized"));
        }
        let c = Causal {
            store: DotStore::new(),
            expired: self.tombstone()?,
        };
        self.augment_array(c)
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

        let c = Causal {
            store,
            expired: DotStore::new(),
        };
        self.augment_array(c)
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

    /// Moves the entry inside an array.
    pub fn r#move(&mut self, to: usize) -> Result<Causal> {
        let array = self.array.pop().context("Not inside an ORArray")?;
        array.r#move(self, to)
    }

    /// Deletes the entry from an array.
    pub fn delete(&mut self) -> Result<Causal> {
        let array = self.array.pop().context("Not inside an ORArray")?;
        array.delete(self)
    }

    /// Augments a causal with array metadata if in an array, otherwise just returns the causal
    /// unchanged.
    fn augment_array(&self, mut inner: Causal) -> Result<Causal> {
        for a in &self.array {
            inner = a.augment_causal(self, inner)?;
        }
        Ok(inner)
    }
}

fn nonce() -> u64 {
    let mut nonce = [0; 8];
    getrandom::getrandom(&mut nonce).unwrap();
    u64::from_le_bytes(nonce)
}

#[derive(Clone, Debug)]
// The ORArray needs to store additional metadata additional to the actual value paths in order to
// support insert, move, update, and delete semantics.
// The paths are structured as follows:
// <path_to_array>.VALUES.<pos>.<uid>.<value>
// <path_to_array>.META.<uid>.<nonce>.<nonce>.<pos>.<nonce>.<peer>.<sig>
//                         ^     ^      ^       ^
//                         |     |      |       |
//          Stable identifier    |      |       |
//                  ID of last update   |       |
//                            ID of last move   |
//                                           Position
// The uid is stable for the element and created upon insertion. The positional identifiers are
// constructed as such that it's always possible to find one inbetween to existing ones. This way
// elements can be inserted at arbitrary places without needed to shift parts of the existing
// array.
// The main reason for the duality of this approach is to have fast access to the actual values, as
// the paths prefixed with `VALUES` are sorted according to `pos` and can thus be cheaply queried.
struct ArrayWrapper {
    /// Absolute path to the array root
    array_path: PathBuf,
    /// Position of the array element this struct is pointing to
    pos: Fraction,
    /// Uid of the array element this struct is pointing to
    uid: u64,
    /// value path
    value_path: PathBuf,
    /// meta path
    meta_path: PathBuf,
}

impl ArrayWrapper {
    /// Augments a `Causal` embedded into an `ORArray`.
    fn augment_causal(&self, cursor: &Cursor, inner: Causal) -> Result<Causal> {
        if cursor
            .crdt
            .scan_path(self.value_path.as_path())
            .next()
            .is_some()
        {
            self.update(cursor, inner)
        } else {
            self.insert(cursor, inner)
        }
    }

    fn new(cursor: &Cursor, mut ix: usize) -> Result<(Self, PathBuf)> {
        let array_path = cursor.path.clone();
        let array_value_root = {
            let mut p = array_path.clone();
            p.prim_str(array_util::ARRAY_VALUES);
            p
        };
        // TODO: use sled's size hint
        let len = cursor.crdt.scan_path(array_value_root.as_path()).count();
        let mut iter = cursor.crdt.scan_path(array_value_root.as_path());

        ix = ix.min(len);
        let (pos, uid) = if let Some(entry) = iter.nth(ix) {
            // Existing entry
            let p_c = cursor.path.clone();
            let data = array_util::ArrayValue::from_path(
                Path::new(&entry).strip_prefix(p_c.as_path())?.as_path(),
            )
            .context("Reading array data")?;
            (data.pos, data.uid)
        } else {
            // No entry, find position to insert
            let (left, right) = match ix.checked_sub(1) {
                Some(s) => {
                    let p_c = cursor.path.clone();
                    let mut iter = cursor
                        .crdt
                        .scan_path(array_value_root.as_path())
                        .skip(s)
                        .map(move |iv| -> anyhow::Result<_> {
                            let meta = array_util::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                            )?;
                            Ok(meta.pos)
                        });
                    (iter.next(), iter.next())
                }
                None => {
                    let p_c = cursor.path.clone();
                    let mut iter =
                        cursor
                            .crdt
                            .scan_path(array_value_root.as_path())
                            .map(move |iv| {
                                let meta = array_util::ArrayValue::from_path(
                                    Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                                )?;
                                Ok(meta.pos)
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
            (pos, nonce())
        };

        let value_path = {
            let mut p = array_path.clone();
            p.prim_str(array_util::ARRAY_VALUES);
            p.position(&pos);
            p.prim_u64(uid);
            p
        };

        let meta_path = {
            let mut p = array_path.clone();
            p.prim_str(array_util::ARRAY_META);
            p.prim_u64(uid);
            p
        };

        Ok((
            Self {
                array_path,
                pos,
                uid,
                value_path: value_path.clone(),
                meta_path,
            },
            value_path,
        ))
    }

    pub fn r#move(self, cursor: &Cursor, mut to: usize) -> Result<Causal> {
        // On a Move, the replica deletes all children of all existing roots, and adds a single
        // child tree to all roots with the new position.

        let new_pos = {
            // TODO: use sled's size hint
            let len = cursor.crdt.scan_path(self.value_path.as_path()).count();

            to = to.min(len);
            let (left, right) = match to.checked_sub(1) {
                Some(s) => {
                    let p_c = self.array_path.clone();
                    let mut iter = cursor
                        .crdt
                        .scan_path(self.value_path.as_path())
                        .skip(s)
                        .map(move |iv| -> anyhow::Result<_> {
                            let meta = array_util::ArrayValue::from_path(
                                Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                            )?;
                            Ok(meta.pos)
                        });
                    (iter.next(), iter.next())
                }
                None => {
                    let p_c = self.array_path.clone();
                    let mut iter =
                        cursor
                            .crdt
                            .scan_path(self.value_path.as_path())
                            .map(move |iv| {
                                let meta = array_util::ArrayValue::from_path(
                                    Path::new(&iv).strip_prefix(p_c.as_path())?.as_path(),
                                )?;
                                Ok(meta.pos)
                            });

                    (None, iter.next())
                }
            };

            let left = left.transpose()?.unwrap_or_else(Fraction::zero);
            if let Some(r) = right.transpose()? {
                left.mid(&r)
            } else {
                left.succ()
            }
        };
        // --
        let existing_meta = cursor
            .crdt
            .scan_path(self.meta_path.as_path())
            .collect::<Vec<_>>();
        anyhow::ensure!(!existing_meta.is_empty(), "Value does not exist!");

        let mut store = DotStore::new();
        let mut expired = DotStore::new();
        let move_op = nonce();
        for e in existing_meta {
            let p = Path::new(&e);
            expired.insert(p.to_owned());

            let meta = self.get_meta_data(p)?;
            let mut path = self.meta_path.clone();
            path.prim_u64(meta.last_update);
            path.prim_u64(move_op);
            path.position(&meta.pos);

            cursor.nonce(&mut path);
            cursor.sign(&mut path);

            store.insert(path);
        }
        let mut new_value_path = {
            let mut p = self.array_path.clone();
            p.prim_str(array_util::ARRAY_VALUES);
            p
        };
        // remove old pos
        let old = cursor
            .crdt
            .scan_path(self.value_path.as_path())
            .next()
            .context("Concurrent access")?;
        let p = Path::new(&old);
        expired.insert(p.to_owned());
        let v = self.get_value(p)?;

        // add new pos
        new_value_path.position(&new_pos);
        new_value_path.prim_u64(self.uid);
        new_value_path.nonce(nonce());
        for s in v.value {
            new_value_path.push_segment(s);
        }
        // overwrite existing peer and sig fields
        cursor.sign(&mut new_value_path);
        store.insert(new_value_path);

        Ok(Causal { store, expired })
    }

    /// Tombstones all value and meta paths
    fn tombstone(&self, cursor: &Cursor) -> Result<DotStore> {
        let mut expired = DotStore::new();
        for e in cursor
            .crdt
            .scan_path(self.value_path.as_path())
            .chain(cursor.crdt.scan_path(self.meta_path.as_path()))
        {
            let mut p = Path::new(&e).to_owned();
            cursor.sign(&mut p);
            expired.insert(p);
        }
        Ok(expired)
    }

    fn update(&self, cursor: &Cursor, mut inner: Causal) -> Result<Causal> {
        // On an Update, besides updating the value of the top level pair, the replica also recommits the
        // current position of that element. This is done by deleting all observed roots from the forest
        // and adding a single tree of height 3 with the current position. This position is chosen
        // deterministically from the set of current possible positions.

        inner.expired.extend(self.tombstone(cursor)?);

        // Commit current position
        let mut p = self.meta_path.clone();
        p.prim_u64(nonce());
        p.prim_u64(nonce());
        p.position(&self.pos);
        p.nonce(nonce());
        cursor.sign(&mut p);
        inner.store.insert(p);
        Ok(inner)
    }

    fn insert(&self, cursor: &Cursor, mut inner: Causal) -> Result<Causal> {
        let mut p = self.meta_path.clone();
        p.prim_u64(nonce());
        p.prim_u64(nonce());
        p.position(&self.pos);
        p.nonce(nonce());
        cursor.sign(&mut p);
        inner.store.insert(p);
        Ok(inner)
    }

    fn delete(&self, cursor: &Cursor) -> Result<Causal> {
        Ok(Causal {
            expired: self.tombstone(cursor)?,
            store: Default::default(),
        })
    }
    fn get_meta_data(&self, path: Path) -> Result<array_util::ArrayMeta> {
        array_util::ArrayMeta::from_path(path.strip_prefix(self.array_path.as_path())?.as_path())
    }

    fn get_value(&self, path: Path<'_>) -> Result<array_util::ArrayValue> {
        array_util::ArrayValue::from_path(path.strip_prefix(self.array_path.as_path())?.as_path())
    }
}

mod array_util {
    use super::*;
    use crate::Segment;
    use anyhow::Context;

    pub(crate) const ARRAY_VALUES: &str = "VALUES";
    pub(crate) const ARRAY_META: &str = "META";
    pub(crate) struct ArrayValue {
        pub(crate) uid: u64,
        pub(crate) pos: Fraction,
        pub(crate) value: Vec<Segment>,
    }
    impl ArrayValue {
        /// `path` needs to point into the array root dir
        pub(crate) fn from_path(path: Path<'_>) -> Result<ArrayValue> {
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

            // nonce
            path.next();

            let mut value = path.collect::<Vec<_>>();
            anyhow::ensure!(
                matches!(value.pop(), Some(Segment::Sig(_))),
                "Unexpected layout"
            );
            anyhow::ensure!(
                matches!(value.pop(), Some(Segment::Peer(_))),
                "Unexpected layout"
            );
            Ok(Self { uid, pos, value })
        }
    }

    #[allow(dead_code)]
    pub(crate) struct ArrayMeta {
        pub(crate) last_update: u64,
        pub(crate) last_move: u64,
        pub(crate) uid: u64,
        pub(crate) pos: Fraction,
    }
    impl ArrayMeta {
        /// `path` needs to point into the array root dir
        pub(crate) fn from_path(path: Path) -> Result<Self> {
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

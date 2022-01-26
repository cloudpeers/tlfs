use std::collections::{BTreeMap, BTreeSet};

use crate::acl::{Actor, Can, Permission, Policy};
use crate::crdt::{Causal, Crdt, DotStore};
use crate::crypto::Keypair;
use crate::cursor::array_util::ArrayMetaEntry;
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

#[allow(clippy::len_without_is_empty)]
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

    /// Return the current schema.
    pub fn schema(&self) -> &'a Archived<Schema> {
        self.schema
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

    /// If the cursor points to a Struct or a Table, returns an iterator of all existing keys.
    pub fn keys(&self) -> Result<Vec<String>> {
        match self.schema {
            ArchivedSchema::Array(_) => {
                let len = self.len().unwrap_or(0);
                Ok((0..len).map(|x| x.to_string()).collect())
            }
            ArchivedSchema::Table(_, _) => {
                let slf = self.path.clone();
                self.crdt
                    .scan_path(slf.as_path())
                    .map(move |p| {
                        let x = Path::new(&p).strip_prefix(slf.as_path())?;
                        x.first().context("Empty")
                    })
                    .filter_map(|segment| match segment {
                        Ok(crate::Segment::Bool(b)) => Some(Ok(b.to_string())),
                        Ok(crate::Segment::U64(n)) => Some(Ok(n.to_string())),
                        Ok(crate::Segment::I64(n)) => Some(Ok(n.to_string())),
                        Ok(crate::Segment::Str(s)) => Some(Ok(s)),
                        Ok(_) => None,
                        Err(e) => Some(Err(e)),
                    })
                    .collect::<Result<Vec<_>>>()
            }
            ArchivedSchema::Struct(s) => Ok(s.keys().map(|x| x.to_string()).collect()),
            _ => Ok(vec![]),
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

    /// Returns an iterator of table keys.
    pub fn keys_bool(&self) -> Result<impl Iterator<Item = bool> + '_> {
        if let ArchivedSchema::Table(PrimitiveKind::Bool, _) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|key| {
                Path::new(&key)
                    .strip_prefix(self.path.as_path())
                    .ok()?
                    .first()?
                    .prim_bool()
            }))
        } else {
            Err(anyhow!("not a Table<bool, _>"))
        }
    }

    /// Returns an iterator of table keys.
    pub fn keys_u64(&self) -> Result<impl Iterator<Item = u64> + '_> {
        if let ArchivedSchema::Table(PrimitiveKind::U64, _) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|key| {
                Path::new(&key)
                    .strip_prefix(self.path.as_path())
                    .ok()?
                    .first()?
                    .prim_u64()
            }))
        } else {
            Err(anyhow!("not a Table<u64, _>"))
        }
    }

    /// Returns an iterator of table keys.
    pub fn keys_i64(&self) -> Result<impl Iterator<Item = i64> + '_> {
        if let ArchivedSchema::Table(PrimitiveKind::I64, _) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|key| {
                Path::new(&key)
                    .strip_prefix(self.path.as_path())
                    .ok()?
                    .first()?
                    .prim_i64()
            }))
        } else {
            Err(anyhow!("not a Table<i64, _>"))
        }
    }

    /// Returns an iterator of table keys.
    pub fn keys_str(&self) -> Result<impl Iterator<Item = String> + '_> {
        if let ArchivedSchema::Table(PrimitiveKind::Str, _) = &self.schema {
            Ok(self.crdt.scan_path(self.path.as_path()).filter_map(|key| {
                Path::new(&key)
                    .strip_prefix(self.path.as_path())
                    .ok()?
                    .first()?
                    .prim_string()
            }))
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
    pub fn len(&self) -> Result<u32> {
        if let ArchivedSchema::Array(_) = &self.schema {
            let mut path = self.path.clone();
            path.prim_str(array_util::ARRAY_VALUES);

            let res = self.pos_iter(Some(path)).collect::<BTreeSet<_>>().len();
            Ok(res as u32)
        } else {
            anyhow::bail!("not an Array<_>");
        }
    }

    fn pos_iter(&self, path: Option<PathBuf>) -> impl Iterator<Item = Fraction> + '_ {
        let path = path.unwrap_or_else(|| self.path.clone());
        self.crdt.scan_path(path.as_path()).filter_map(move |e| {
            let p = Path::new(&e);
            p.strip_prefix(path.as_path())
                .ok()
                .and_then(|e| e.first())
                .and_then(|x| match x {
                    crate::Segment::Position(x) => Some(x),
                    _ => None,
                })
        })
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

    fn arr_items(
        cursor: &Cursor,
        array_root: PathBuf,
    ) -> impl Iterator<Item = Result<array_util::ArrayValueEntry>> {
        let array_value_root = {
            let mut p = array_root.clone();
            p.prim_str(array_util::ARRAY_VALUES);
            p
        };

        cursor
            .crdt
            .scan_path(array_value_root.as_path())
            .map(move |val| {
                array_util::ArrayValueEntry::from_path(
                    Path::new(&val).strip_prefix(array_root.as_path())?,
                )
                .context("Reading array data")
            })
    }

    fn distinct_arr_items(
        cursor: &Cursor,
        array_root: PathBuf,
    ) -> impl Iterator<Item = Result<(Fraction, u64)>> {
        let mut scan_state = BTreeMap::default();
        Self::arr_items(cursor, array_root).filter_map(move |val| match val {
            Ok(data) => match scan_state.insert(data.pos.clone(), data.uid) {
                Some(existing) if existing != data.uid => unreachable!(),
                Some(_) => None,
                None => Some(Ok((data.pos, data.uid))),
            },
            Err(e) => Some(Err(e)),
        })
    }

    fn new(cursor: &Cursor, mut ix: usize) -> Result<(Self, PathBuf)> {
        let array_path = cursor.path.clone();

        let len = Self::distinct_arr_items(cursor, cursor.path.clone()).count();
        let mut iter = Self::distinct_arr_items(cursor, cursor.path.clone());

        ix = ix.min(len);
        let (pos, uid) = if let Some(entry) = iter.nth(ix) {
            entry?
        } else {
            let p_c = cursor.path.clone();
            // No entry, find position to insert
            let (left, right) = match ix.checked_sub(1) {
                Some(s) => {
                    let mut iter = Self::distinct_arr_items(cursor, p_c)
                        .skip(s)
                        .map(|v| v.map(|(p, _)| p));
                    (iter.next(), iter.next())
                }
                None => {
                    let mut iter = Self::distinct_arr_items(cursor, p_c).map(|v| v.map(|(p, _)| p));

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
            let len = Self::distinct_arr_items(cursor, self.array_path.clone()).count();

            to = to.min(len);
            let p_c = self.array_path.clone();
            let (left, right) = match to.checked_sub(1) {
                Some(s) => {
                    let mut iter = Self::distinct_arr_items(cursor, p_c)
                        .skip(s)
                        .map(|v| v.map(|(p, _)| p));
                    (iter.next(), iter.next())
                }
                None => {
                    let mut iter = Self::distinct_arr_items(cursor, p_c).map(|v| v.map(|(p, _)| p));

                    (None, iter.next())
                }
            };

            let left = left.transpose()?.unwrap_or_else(Fraction::zero);
            if let Some(right) = right.transpose()? {
                left.mid(&right)
            } else {
                left.succ()
            }
        };

        let existing_meta = cursor
            .crdt
            .scan_path(self.meta_path.as_path())
            .collect::<Vec<_>>();
        anyhow::ensure!(!existing_meta.is_empty(), "Value does not exist!");

        let mut store = DotStore::new();
        let mut expired = DotStore::new();
        let move_op = nonce();
        for e in existing_meta {
            let mut p = Path::new(&e).to_owned();
            cursor.sign(&mut p);
            let mut meta = self.get_meta_data(p.as_path())?;
            expired.insert(p);

            meta.last_move = move_op;

            let mut path = meta.to_path(self.meta_path.clone());
            cursor.sign(&mut path);

            store.insert(path);
        }
        // remove old pos
        let old = cursor
            .crdt
            .scan_path(self.value_path.as_path())
            .next()
            .context("Concurrent access")?;
        let mut p = Path::new(&old).to_owned();
        let mut v = self.get_value(p.as_path())?;
        cursor.sign(&mut p);
        expired.insert(p);

        v.pos = new_pos;
        let mut new_value_path = v.to_path({
            let mut p = self.array_path;
            p.prim_str(array_util::ARRAY_VALUES);
            p
        });

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

        // tombstone old value
        for e in cursor.crdt.scan_path(cursor.path.as_path()) {
            let mut p = Path::new(&e).to_owned();
            cursor.sign(&mut p);
            inner.expired.insert(p);
        }

        let mut last_move = None;
        // and all meta entries
        for e in cursor.crdt.scan_path(self.meta_path.as_path()) {
            let mut p = Path::new(&e).to_owned();
            if last_move.is_none() {
                last_move.replace(self.get_meta_data(p.as_path())?.last_move);
            }
            cursor.sign(&mut p);
            inner.expired.insert(p);
        }
        // Commit current position
        let meta_entry = ArrayMetaEntry::new(
            self.uid,
            nonce(),
            last_move.context("No metadata for value entry found")?,
            self.pos.clone(),
        );
        let mut p = meta_entry.to_path(self.meta_path.clone());
        cursor.sign(&mut p);
        inner.store.insert(p);
        Ok(inner)
    }

    fn insert(&self, cursor: &Cursor, mut inner: Causal) -> Result<Causal> {
        let meta_entry = ArrayMetaEntry::new(self.uid, nonce(), nonce(), self.pos.clone());
        let mut p = meta_entry.to_path(self.meta_path.clone());
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
    fn get_meta_data(&self, path: Path) -> Result<array_util::ArrayMetaEntry> {
        array_util::ArrayMetaEntry::from_path(path.strip_prefix(self.array_path.as_path())?)
    }

    fn get_value(&self, path: Path<'_>) -> Result<array_util::ArrayValueEntry> {
        array_util::ArrayValueEntry::from_path(path.strip_prefix(self.array_path.as_path())?)
    }
}

mod array_util {
    use super::*;
    use crate::Segment;
    use anyhow::Context;

    pub(crate) const ARRAY_VALUES: &str = "VALUES";
    pub(crate) const ARRAY_META: &str = "META";

    // <path_to_array>.VALUES.<pos>.<uid>.<value>
    #[derive(Debug)]
    pub(crate) struct ArrayValueEntry {
        pub(crate) pos: Fraction,
        pub(crate) uid: u64,
        pub(crate) value: Vec<Segment>,
    }
    impl ArrayValueEntry {
        /// `path` needs to point into the array root dir
        pub(crate) fn from_path(path: Path<'_>) -> Result<ArrayValueEntry> {
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

        pub(crate) fn to_path(&self, mut base: PathBuf) -> PathBuf {
            base.position(&self.pos);
            base.prim_u64(self.uid);
            for s in &self.value {
                base.push_segment(s.clone());
            }
            base
        }
    }

    // <path_to_array>.META.<uid>.<nonce>.<nonce>.<pos>.<nonce>.<peer>.<sig>
    #[allow(dead_code)]
    pub(crate) struct ArrayMetaEntry {
        pub(crate) last_update: u64,
        pub(crate) last_move: u64,
        pub(crate) uid: u64,
        pub(crate) pos: Fraction,
    }
    impl ArrayMetaEntry {
        pub(crate) fn new(uid: u64, last_update: u64, last_move: u64, pos: Fraction) -> Self {
            Self {
                uid,
                last_update,
                last_move,
                pos,
            }
        }
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

        pub(crate) fn to_path(&self, mut base: PathBuf) -> PathBuf {
            // uid is expected to be already part of `base`
            base.prim_u64(self.last_update);
            base.prim_u64(self.last_move);
            base.position(&self.pos);
            base.prim_u64(nonce());
            base
        }
    }
}

use std::{
    collections::{hash_map, BTreeMap},
    fs, io,
    io::Write,
    path::PathBuf,
    sync::Arc,
};

use futures::{
    channel::mpsc::{UnboundedReceiver, UnboundedSender},
    future,
    stream::BoxStream,
    StreamExt,
};
use parking_lot::Mutex;
use rkyv::{
    archived_root,
    de::{
        deserializers::{SharedDeserializeMap, SharedDeserializeMapError},
        SharedDeserializeRegistry, SharedPointer,
    },
    ser::{
        serializers::{
            AlignedSerializer, AllocSerializer, CompositeSerializer, SharedSerializeMapError,
            WriteSerializer,
        },
        SharedSerializeRegistry,
    },
    ser::{
        serializers::{AllocScratch, FallbackScratch, HeapScratch},
        Serializer,
    },
    AlignedVec, Archive, Archived, Deserialize, Fallible, Serialize,
};
use vec_collections::{
    AbstractRadixTree, AbstractRadixTreeMut, ArcRadixTree, IterKey, TKey, TValue,
};

use crate::Ref;

pub struct Batch<K: TKey, V: TValue> {
    v0: ArcRadixTree<K, V>,
    v1: ArcRadixTree<K, V>,
}

impl<K: TKey, V: TValue> Batch<K, V> {
    pub fn prev(&self) -> &ArcRadixTree<K, V> {
        &self.v0
    }
    pub fn curr(&self) -> &ArcRadixTree<K, V> {
        &self.v1
    }
    pub fn added(&self) -> ArcRadixTree<K, V> {
        let mut res = self.v1.clone();
        res.difference_with(&self.v0);
        res
    }
    pub fn removed(&self) -> ArcRadixTree<K, V> {
        let mut res = self.v0.clone();
        res.difference_with(&self.v1);
        res
    }
    pub fn iter<'a>(&self) -> impl Iterator<Item = (IterKey<K>, Option<&'a V>)> + 'a {
        let added = self.added().into_iter().map(|(k, v)| (k, Some(v)));
        let removed = self.removed().into_iter().map(|(k, _)| (k, None));
        added.chain(removed)
    }
}

#[derive(Debug, Default)]
pub struct SharedSerializeMap2 {
    /// mapping from the rc/arc to the position in the buffer
    shared_resolvers: hash_map::HashMap<*const u8, usize>,
}

/// these are safe, because a *const u8 is safe to send and sync
///
/// see discussion in https://internals.rust-lang.org/t/shouldnt-pointers-be-send-sync-or/8818
unsafe impl Send for SharedSerializeMap2 {}
unsafe impl Sync for SharedSerializeMap2 {}

impl Fallible for SharedSerializeMap2 {
    type Error = SharedSerializeMapError;
}

impl SharedSerializeRegistry for SharedSerializeMap2 {
    fn get_shared_ptr(&mut self, value: *const u8) -> Option<usize> {
        self.shared_resolvers.get(&value).copied()
    }

    fn add_shared_ptr(&mut self, value: *const u8, pos: usize) -> Result<(), Self::Error> {
        match self.shared_resolvers.entry(value) {
            hash_map::Entry::Occupied(_) => {
                Err(SharedSerializeMapError::DuplicateSharedPointer(value))
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(pos);
                Ok(())
            }
        }
    }
}

#[derive(Default)]
pub struct SharedDeserializeMap2 {
    /// mapping from the position in the buffer to the rc/arc
    shared_pointers: hash_map::HashMap<*const u8, Box<dyn SharedPointer>>,
}

impl SharedDeserializeMap2 {
    pub fn to_shared_serializer_map(&self, base: *const u8) -> SharedSerializeMap2 {
        let shared_resolvers = self
            .shared_pointers
            .iter()
            .map(|(k, v)| {
                let offset: usize = (*k as usize) - (base as usize);
                let address = v.data_address() as *const u8;
                (address, offset)
            })
            .collect();
        SharedSerializeMap2 { shared_resolvers }
    }
}

impl Fallible for SharedDeserializeMap2 {
    type Error = SharedDeserializeMapError;
}

impl SharedDeserializeRegistry for SharedDeserializeMap2 {
    fn get_shared_ptr(&mut self, ptr: *const u8) -> Option<&dyn SharedPointer> {
        self.shared_pointers.get(&ptr).map(|p| p.as_ref())
    }

    fn add_shared_ptr(
        &mut self,
        ptr: *const u8,
        shared: Box<dyn SharedPointer>,
    ) -> Result<(), Self::Error> {
        match self.shared_pointers.entry(ptr) {
            hash_map::Entry::Occupied(_) => {
                Err(SharedDeserializeMapError::DuplicateSharedPointer(ptr))
            }
            hash_map::Entry::Vacant(e) => {
                e.insert(shared);
                Ok(())
            }
        }
    }
}

pub trait AbstractRadixDb<K: TKey, V: TValue> {
    fn tree(&self) -> &ArcRadixTree<K, V>;
    fn tree_mut(&mut self) -> &mut ArcRadixTree<K, V>;
    fn flush(&mut self) -> anyhow::Result<()>;
    fn vacuum(&mut self) -> anyhow::Result<()>;
    fn watch(&mut self) -> futures::channel::mpsc::UnboundedReceiver<ArcRadixTree<K, V>>;
    fn watch_prefix(&mut self, prefix: Vec<K>) -> BoxStream<'static, Batch<K, V>> {
        let tree = self.tree().clone();
        self.watch()
            .scan(tree, move |prev, curr| {
                let v0 = prev.filter_prefix(&prefix);
                let v1 = curr.filter_prefix(&prefix);
                future::ready(Some(Batch { v0, v1 }))
            })
            .boxed()
    }
}

pub trait Storage: Send + Sync + 'static {
    /// appends to a file. Should only return when the data is safely on disk (flushed)!
    /// appending will usually be done in large chunks.
    /// appending to a non existing file creates it.
    /// appending an empty chunk is a noop.
    fn append(&self, file: &str, chunk: &[u8]) -> io::Result<()>;

    /// load a file. The callback will get to look at the data and do something with it.
    /// loading a non-existing file is like loading an empty file. It will not create the file.
    fn load(&self, file: &str, f: Box<dyn FnMut(&[u8]) + '_>) -> io::Result<()>;

    /// atomically move a file. target will be atomically overwritten.
    /// if the source file does not exist, the target file will be deleted.
    fn mv(&self, from: &str, to: &str) -> io::Result<()>;
}

#[derive(Default, Clone)]
pub struct MemStorage {
    data: Arc<Mutex<BTreeMap<String, AlignedVec>>>,
}

impl Storage for MemStorage {
    fn append(&self, file: &str, chunk: &[u8]) -> std::io::Result<()> {
        if !chunk.is_empty() {
            let mut data = self.data.lock();
            let vec = if let Some(vec) = data.get_mut(file) {
                vec
            } else {
                data.entry(file.to_owned()).or_default()
            };
            vec.extend_from_slice(chunk);
        }
        Ok(())
    }

    fn load(&self, file: &str, mut f: Box<dyn FnMut(&[u8]) + '_>) -> std::io::Result<()> {
        let data = self.data.lock();
        if let Some(vec) = data.get(file) {
            f(&vec)
        } else {
            f(&[])
        };
        Ok(())
    }

    fn mv(&self, from: &str, to: &str) -> std::io::Result<()> {
        if from != to {
            let mut data = self.data.lock();
            if let Some(vec) = data.remove(from) {
                if !vec.is_empty() {
                    data.insert(to.to_owned(), vec);
                } else {
                    data.remove(to);
                }
            } else {
                data.remove(to);
            }
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct FileStorage {
    base: PathBuf,
}

impl FileStorage {
    pub fn new(base: impl AsRef<std::path::Path>) -> Self {
        Self {
            base: base.as_ref().to_path_buf(),
        }
    }
}

impl Storage for FileStorage {
    fn append(&self, file: &str, chunk: &[u8]) -> io::Result<()> {
        if !chunk.is_empty() {
            let mut file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.base.join(file))?;
            file.write_all(chunk)?;
        }
        Ok(())
    }

    fn load(&self, file: &str, mut f: Box<dyn FnMut(&[u8]) + '_>) -> io::Result<()> {
        match std::fs::read(self.base.join(file)) {
            Ok(data) => f(&data),
            Err(e) if e.kind() == io::ErrorKind::NotFound => f(&[]),
            Err(e) => return Err(e),
        };
        Ok(())
    }

    fn mv(&self, from: &str, to: &str) -> std::io::Result<()> {
        if from != to {
            let from = self.base.join(from);
            let to = self.base.join(to);
            match fs::rename(from, &to) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    fs::remove_file(to)?;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

pub struct RadixDb<K: TKey, V: TValue> {
    storage: Arc<dyn Storage>,
    name: String,
    serializers: Option<(
        SharedSerializeMap2,
        BTreeMap<usize, Arc<Vec<ArcRadixTree<K, V>>>>,
    )>,
    pos: usize,
    tree: ArcRadixTree<K, V>,
    watchers: Vec<UnboundedSender<ArcRadixTree<K, V>>>,
}

impl<K: TKey, V: TValue> RadixDb<K, V>
where
    Archived<K>: Deserialize<K, SharedDeserializeMap2>,
    Archived<V>: Deserialize<V, SharedDeserializeMap2>,
{
    fn memory(name: impl Into<String>) -> anyhow::Result<Self> {
        RadixDb::load(Arc::new(MemStorage::default()), name)
    }

    fn open(base: impl AsRef<std::path::Path>, name: impl Into<String>) -> anyhow::Result<Self> {
        RadixDb::load(Arc::new(FileStorage::new(base)), name)
    }
}

impl<K: TKey, V: TValue> RadixDb<K, V> {
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    pub fn load(storage: Arc<dyn Storage>, name: impl Into<String>) -> anyhow::Result<Self>
    where
        Archived<K>: Deserialize<K, SharedDeserializeMap2>,
        Archived<V>: Deserialize<V, SharedDeserializeMap2>,
    {
        let name = name.into();
        let mut tree: anyhow::Result<ArcRadixTree<K, V>> = Ok(Default::default());
        let mut map = Default::default();
        let mut pos = Default::default();
        storage.load(
            &name,
            Box::new(|data| {
                if !data.is_empty() {
                    let mut deserializer = SharedDeserializeMap2::default();
                    let archived: &Archived<ArcRadixTree<K, V>> =
                        unsafe { archived_root::<ArcRadixTree<K, V>>(data) };
                    tree = archived
                        .deserialize(&mut deserializer)
                        .map_err(|e| anyhow::anyhow!("Error while deserializing: {}", e));
                    map = deserializer.to_shared_serializer_map(&data[0] as *const u8);
                    pos = data.len();
                }
            }),
        )?;
        let tree = tree?;
        let mut arcs = Default::default();
        tree.all_arcs(&mut arcs);
        Ok(Self {
            tree,
            name,
            storage,
            pos,
            serializers: Some((map, arcs)),
            watchers: Default::default(),
        })
    }

    fn notify(&mut self) {
        let tree = self.tree.clone();
        self.watchers
            .retain(|sender| sender.unbounded_send(tree.clone()).is_ok())
    }
}

type MySerializer<'a> = CompositeSerializer<
    WriteSerializer<&'a mut AlignedVec>,
    FallbackScratch<HeapScratch<256>, AllocScratch>,
    SharedSerializeMap2,
>;

impl<K, V> AbstractRadixDb<K, V> for RadixDb<K, V>
where
    K: TKey + for<'x> Serialize<MySerializer<'x>>,
    V: TValue + for<'x> Serialize<MySerializer<'x>>,
{
    fn tree(&self) -> &ArcRadixTree<K, V> {
        &self.tree
    }

    fn tree_mut(&mut self) -> &mut ArcRadixTree<K, V> {
        &mut self.tree
    }

    fn vacuum(&mut self) -> anyhow::Result<()> {
        // write ourselves to a new file
        let mut file = AlignedVec::new();
        let mut serializer = CompositeSerializer::new(
            WriteSerializer::new(&mut file),
            Default::default(),
            Default::default(),
        );
        serializer
            .serialize_value(&self.tree)
            .map_err(|e| anyhow::anyhow!("Error while serializing: {}", e))?;
        let (_, _, map) = serializer.into_components();
        // compute just the current arcs of the current tree
        let mut arcs = BTreeMap::default();
        self.tree.all_arcs(&mut arcs);
        // store the new file and the new arcs
        let tmp = format!("{}.tmp", self.name);
        self.storage.append(&tmp, &file)?;
        self.storage.mv(&tmp, &self.name)?;
        self.pos = file.len();
        self.serializers = Some((map, arcs));
        self.notify();
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        let (map, mut arcs) = self.serializers.take().unwrap_or_default();
        let mut t = AlignedVec::new();
        let mut serializer = CompositeSerializer::new(
            WriteSerializer::with_pos(&mut t, self.pos),
            Default::default(),
            map,
        );
        serializer
            .serialize_value(&self.tree)
            .map_err(|e| anyhow::anyhow!("Error while serializing: {}", e))?;
        self.tree.all_arcs(&mut arcs);
        let (_, _, map) = serializer.into_components();
        self.storage.append(&self.name, &t)?;
        self.pos += t.len();
        self.serializers = Some((map, arcs));
        self.notify();
        Ok(())
    }

    fn watch(&mut self) -> UnboundedReceiver<ArcRadixTree<K, V>> {
        let (s, r) = futures::channel::mpsc::unbounded();
        self.watchers.push(s);
        r
    }
}

#[derive(Clone)]
pub struct BlobSet(Arc<Mutex<RadixDb<u8, ()>>>);

impl std::fmt::Debug for BlobSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut t = f.debug_set();
        for (k, _) in self.0.lock().tree().iter() {
            t.entry(&hex::encode(k));
        }
        t.finish()
    }
}

impl BlobSet {
    pub fn load(storage: Arc<dyn Storage>, name: &str) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(RadixDb::load(storage, name)?))))
    }

    pub fn insert(&self, key: impl AsRef<[u8]>) {
        let t: ArcRadixTree<u8, ()> = ArcRadixTree::single(key.as_ref(), ());
        // right biased union
        self.0.lock().tree_mut().union_with(&t);
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) {
        let t = ArcRadixTree::single(key.as_ref(), ());
        self.0.lock().tree_mut().difference_with(&t);
    }

    pub fn contains(&self, key: impl AsRef<[u8]>) -> bool {
        let lock = self.0.lock();
        lock.tree().contains_key(key.as_ref())
    }

    pub fn keys(&self) -> impl Iterator<Item = IterKey<u8>> {
        let tree = self.0.lock().tree().clone();
        tree.into_iter().map(|(k, _)| k)
    }

    pub fn scan_prefix(&self, prefix: impl AsRef<[u8]>) -> impl Iterator<Item = IterKey<u8>> {
        let tree = self.0.lock().tree().filter_prefix(prefix.as_ref());
        tree.into_iter().map(|(k, _)| k)
    }

    pub fn watch_prefix<'a>(
        &'a self,
        prefix: impl AsRef<[u8]>,
    ) -> BoxStream<'static, Batch<u8, ()>> {
        self.0.lock().watch_prefix(prefix.as_ref().into())
    }
}

#[derive(Clone)]
pub struct BlobMap(Arc<Mutex<RadixDb<u8, Arc<[u8]>>>>);

impl std::fmt::Debug for BlobMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut t = f.debug_map();
        for (k, v) in self.0.lock().tree().iter() {
            t.entry(&hex::encode(k), &hex::encode(v));
        }
        t.finish()
    }
}

impl BlobMap {
    pub fn load(storage: Arc<dyn Storage>, name: &str) -> anyhow::Result<Self> {
        Ok(Self(Arc::new(Mutex::new(RadixDb::load(storage, name)?))))
    }

    pub fn insert(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let t = ArcRadixTree::single(key.as_ref(), value.as_ref().into());
        // right biased union
        self.0.lock().tree_mut().outer_combine_with(&t, |a, b| {
            *a = b.clone();
            true
        });
        Ok(())
    }

    pub fn insert_archived<T: Archive + Serialize<AllocSerializer<256>>>(
        &self,
        key: impl AsRef<[u8]>,
        value: &T,
    ) -> anyhow::Result<()> {
        let value = Ref::archive(value);
        let t = ArcRadixTree::single(key.as_ref(), value.as_arc().clone());
        // right biased union
        self.0.lock().tree_mut().outer_combine_with(&t, |a, b| {
            *a = b.clone();
            true
        });
        Ok(())
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let t = ArcRadixTree::single(key.as_ref(), ());
        self.0.lock().tree_mut().difference_with(&t);
        Ok(())
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> anyhow::Result<Option<Arc<[u8]>>> {
        let lock = self.0.lock();
        Ok(lock.tree().get(key.as_ref()).cloned())
    }

    pub fn iter<'a>(&self) -> impl Iterator<Item = (IterKey<u8>, &'a Arc<[u8]>)> + 'a {
        let tree = self.0.lock().tree().clone();
        tree.into_iter()
    }

    pub fn scan_prefix<'a>(
        &self,
        prefix: impl AsRef<[u8]>,
    ) -> impl Iterator<Item = (IterKey<u8>, &'a Arc<[u8]>)> + 'a {
        let tree = self.0.lock().tree().filter_prefix(prefix.as_ref());
        tree.into_iter()
    }

    pub fn watch_prefix<'a>(
        &'a self,
        prefix: impl AsRef<[u8]>,
    ) -> BoxStream<'static, Batch<u8, Arc<[u8]>>> {
        self.0.lock().watch_prefix(prefix.as_ref().into())
    }
}

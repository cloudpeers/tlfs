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
    de::{deserializers::SharedDeserializeMapError, SharedDeserializeRegistry, SharedPointer},
    ser::{
        serializers::{
            AllocScratch, AllocSerializer, CompositeSerializer, FallbackScratch, HeapScratch,
            SharedSerializeMapError, WriteSerializer,
        },
        Serializer, SharedSerializeRegistry,
    },
    AlignedVec, Archive, Archived, Deserialize, Fallible, Serialize,
};
use vec_collections::radix_tree::{
    AbstractRadixTree, AbstractRadixTreeMut, ArcRadixTree, IterKey, TKey, TValue,
};

use crate::Ref;

/// The difference between a tree at one point in time `v0` and at a later point in time `v1`.
///
/// This can be used to compute all values that have been added and removed.
pub struct Diff<K: TKey, V: TValue> {
    v0: ArcRadixTree<K, V>,
    v1: ArcRadixTree<K, V>,
}

impl<K: TKey, V: TValue> Diff<K, V> {
    /// the previous state as a tree
    pub fn prev(&self) -> &ArcRadixTree<K, V> {
        &self.v0
    }
    /// the current state as a tree
    pub fn curr(&self) -> &ArcRadixTree<K, V> {
        &self.v1
    }
    /// All added items as a tree
    pub fn added(&self) -> ArcRadixTree<K, V> {
        let mut res = self.v1.clone();
        res.difference_with(&self.v0);
        res
    }
    /// All removed items as a tree
    pub fn removed(&self) -> ArcRadixTree<K, V> {
        let mut res = self.v0.clone();
        res.difference_with(&self.v1);
        res
    }
    /// Iterate over all changes from the previous to the current tree.
    ///
    /// The format is `(k, Some(v))` for added entries and `(k, None)` for removed entries.
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
    fn watch_prefix(&mut self, prefix: Vec<K>) -> BoxStream<'static, Diff<K, V>> {
        let tree = self.tree().clone();
        self.watch()
            .scan(tree, move |prev, curr| {
                let v0 = prev.filter_prefix(&prefix);
                let v1 = curr.filter_prefix(&prefix);
                future::ready(Some(Diff { v0, v1 }))
            })
            .boxed()
    }
}

/// Trait for radixdb storage
///
/// basically supports append and rename
pub trait Storage: Send + Sync + 'static {
    /// appends to a file. Should only return when the data is safely on disk (flushed)!
    /// appending will usually be done in large chunks.
    /// appending to a non existing file creates it.
    /// appending an empty chunk is a noop.
    fn append(&self, file: &str, chunk: &[u8]) -> io::Result<()>;

    /// atomically set the content of a file
    fn set(&self, file: &str, data: &[u8]) -> io::Result<()>;

    /// load a file. The callback will get to look at the data and do something with it.
    /// loading a non-existing file is like loading an empty file. It will not create the file.
    fn load(&self, file: &str, f: Box<dyn FnMut(&[u8]) + '_>) -> io::Result<()>;
}

/// A memory based storage implementation.
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

    fn set(&self, file: &str, content: &[u8]) -> std::io::Result<()> {
        let mut data = self.data.lock();
        let mut entry = AlignedVec::new();
        entry.extend_from_slice(content);
        data.insert(file.to_owned(), entry);
        Ok(())
    }

    fn load(&self, file: &str, mut f: Box<dyn FnMut(&[u8]) + '_>) -> std::io::Result<()> {
        let data = self.data.lock();
        if let Some(vec) = data.get(file) {
            f(vec)
        } else {
            f(&[])
        };
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
pub mod browser {
    use futures::{future::BoxFuture, FutureExt};
    use js_sys::{Array, ArrayBuffer, Uint8Array};
    use parking_lot::Mutex;
    use rkyv::AlignedVec;
    use std::{collections::BTreeMap, io, sync::Arc};
    use url::Url;
    use wasm_bindgen_futures::JsFuture;
    use web_sys::{Cache, CacheQueryOptions, DomException, Request, Response};

    use crate::Storage;

    #[derive(Debug)]
    pub struct CacheFileStore {
        cache: web_sys::Cache,
        data: Arc<Mutex<BTreeMap<String, AlignedVec>>>,
    }

    unsafe impl Send for CacheFileStore {}

    unsafe impl Sync for CacheFileStore {}

    async fn load(cache: &Cache, name: &str) -> Result<AlignedVec, DomException> {
        let responses = JsFuture::from(
            cache
                .match_all_with_str_and_options(name, CacheQueryOptions::new().ignore_search(true)),
        )
        .await?;
        let mut res = AlignedVec::new();
        for response in Array::from(&responses).iter() {
            let response = Response::from(response);
            let ab = ArrayBuffer::from(JsFuture::from(response.array_buffer()?).await?);
            let data = Uint8Array::new(&ab).to_vec();
            res.extend_from_slice(&data);
        }
        Ok(res)
    }

    /// fire writing to a cache, without waiting for completion
    fn fire_write(
        cache: &Cache,
        name: &str,
        content: &[u8],
    ) -> std::result::Result<(), DomException> {
        let mut content = content.to_vec();
        let req = Request::new_with_str(name)?;
        let res = Response::new_with_opt_u8_array(Some(&mut content))?;
        // this is a js promise, not a rust future. So it will fire immediately.
        // I assume that subsequent writes to the same file will be ordered.
        let _promise = cache.put_with_request(&req, &res);
        Ok(())
    }

    /// Convert a DomException to a std::io::Error
    fn dom_to_io(e: DomException) -> io::Error {
        io::Error::new(io::ErrorKind::Other, e.name())
    }

    impl CacheFileStore {
        pub fn new(
            name: String,
        ) -> BoxFuture<'static, std::result::Result<CacheFileStore, DomException>> {
            let res = Self::new0(name).boxed_local();
            unsafe { std::mem::transmute(res) }
        }

        async fn new0(name: String) -> std::result::Result<CacheFileStore, DomException> {
            let window = web_sys::window().expect("unable to get window");
            let caches = window.caches()?;
            let cache = web_sys::Cache::from(JsFuture::from(caches.open(&name)).await?);
            let keys = JsFuture::from(cache.keys()).await?;
            // set of distinct names
            let names = Array::from(&keys)
                .iter()
                .map(|req| Request::from(req).url())
                .filter_map(|url| {
                    Url::parse(&url)
                        .ok()
                        .and_then(|x| x.path().strip_prefix('/').map(|x| x.to_string()))
                })
                .collect::<Vec<String>>();
            let mut data = BTreeMap::new();
            for name in names {
                let content = load(&cache, &name).await?;
                data.insert(name, content);
            }
            Ok(Self {
                data: Arc::new(Mutex::new(data)),
                cache,
            })
        }
    }

    impl Storage for CacheFileStore {
        fn append(&self, file: &str, chunk: &[u8]) -> std::io::Result<()> {
            if !chunk.is_empty() {
                let mut data = self.data.lock();
                let vec = if let Some(vec) = data.get_mut(file) {
                    vec
                } else {
                    data.entry(file.to_owned()).or_default()
                };
                vec.extend_from_slice(chunk);
                fire_write(&self.cache, file, vec).map_err(dom_to_io)?;
            }
            Ok(())
        }

        fn set(&self, file: &str, content: &[u8]) -> std::io::Result<()> {
            let mut data = self.data.lock();
            let mut entry = AlignedVec::new();
            entry.extend_from_slice(content);
            data.insert(file.to_owned(), entry);
            fire_write(&self.cache, file, content).map_err(dom_to_io)?;
            Ok(())
        }

        fn load(&self, file: &str, mut f: Box<dyn FnMut(&[u8]) + '_>) -> std::io::Result<()> {
            let data = self.data.lock();
            if let Some(vec) = data.get(file) {
                f(vec)
            } else {
                f(&[])
            };
            Ok(())
        }
    }
}

/// Very basic file based storage
#[derive(Default, Clone)]
pub struct FileStorage {
    base: PathBuf,
}

impl FileStorage {
    /// creates a new file storage in the given base directory
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

    fn set(&self, file: &str, data: &[u8]) -> std::io::Result<()> {
        let tmp = format!("{}.tmp", file);
        let mut tmp_file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.base.join(&tmp))?;
        tmp_file.write_all(data)?;
        tmp_file.flush()?;
        let from = self.base.join(tmp);
        let to = self.base.join(file);
        match fs::rename(&from, &to) {
            Ok(()) => {}
            Err(e) => return Err(e),
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
}

#[allow(clippy::type_complexity)]
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

impl<K: TKey, V: TValue> RadixDb<K, V> {
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
        // this increases the strong count for all the arcs in the current tree and therefore
        // disables copy on write for these nodes.
        let mut arcs = BTreeMap::default();
        self.tree.all_arcs(&mut arcs);
        // store the new file and the new arcs
        self.storage.set(&self.name, &file)?;
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
        // this increases the strong count for all the arcs in the current tree and therefore
        // disables copy on write for these nodes.
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

/// A set of blobs, backed by a radix tree
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

    pub fn flush(&self) -> anyhow::Result<()> {
        self.0.lock().flush()
    }

    pub fn insert(&self, key: impl AsRef<[u8]>) {
        let t: ArcRadixTree<u8, ()> = ArcRadixTree::single(key.as_ref(), ());
        // right biased union
        let mut db = self.0.lock();
        db.tree_mut().union_with(&t);
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) {
        let t = ArcRadixTree::single(key.as_ref(), ());
        let mut db = self.0.lock();
        db.tree_mut().difference_with(&t);
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
    ) -> BoxStream<'static, Diff<u8, ()>> {
        self.0.lock().watch_prefix(prefix.as_ref().into())
    }
}

/// A map with blob keys and values, backed by a radix tree
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
        let mut db = self.0.lock();
        db.tree_mut().outer_combine_with(&t, |a, b| {
            *a = b.clone();
            true
        });
        db.flush()?;
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
        let mut db = self.0.lock();
        db.tree_mut().outer_combine_with(&t, |a, b| {
            *a = b.clone();
            true
        });
        db.flush()?;
        Ok(())
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) -> anyhow::Result<()> {
        let t = ArcRadixTree::single(key.as_ref(), ());
        let mut db = self.0.lock();
        db.tree_mut().difference_with(&t);
        db.flush()?;
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
    ) -> BoxStream<'static, Diff<u8, Arc<[u8]>>> {
        self.0.lock().watch_prefix(prefix.as_ref().into())
    }
}

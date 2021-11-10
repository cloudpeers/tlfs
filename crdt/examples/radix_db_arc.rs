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
            AlignedSerializer, CompositeSerializer, SharedSerializeMapError, WriteSerializer,
        },
        SharedSerializeRegistry,
    },
    ser::{
        serializers::{AllocScratch, FallbackScratch, HeapScratch},
        Serializer,
    },
    AlignedVec, Archived, Deserialize, Fallible, Serialize,
};
use vec_collections::{AbstractRadixTree, AbstractRadixTreeMut, ArcRadixTree, TKey, TValue};

struct Batch<K: TKey, V: TValue> {
    v0: ArcRadixTree<K, V>,
    v1: ArcRadixTree<K, V>,
}

impl<K: TKey, V: TValue> Batch<K, V> {
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
}
#[derive(Debug, Default)]
pub struct SharedSerializeMap2 {
    /// mapping from the rc/arc to the position in the buffer
    shared_resolvers: hash_map::HashMap<*const u8, usize>,
}

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

trait AbstractRadixDb<K: TKey, V: TValue> {
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

trait Storage {
    /// appends to a file. Should only return when the data is safely on disk (flushed)!
    /// appending will usually be done in large chunks.
    /// appending to a non existing file creates it.
    /// appending an empty chunk is a noop.
    fn append(&self, file: &str, chunk: &[u8]) -> io::Result<()>;

    /// load a file. The callback will get to look at the data and do something with it.
    /// loading a non-existing file is like loading an empty file. It will not create the file.
    fn load<T>(&self, file: &str, f: impl FnMut(&[u8]) -> T) -> io::Result<T>;

    /// atomically move a file. target will be atomically overwritten.
    /// if the source file does not exist, the target file will be deleted.
    fn mv(&self, from: &str, to: &str) -> io::Result<()>;
}

#[derive(Default, Clone)]
struct MemStorage {
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

    fn load<T>(&self, file: &str, mut f: impl FnMut(&[u8]) -> T) -> std::io::Result<T> {
        let data = self.data.lock();
        let res = if let Some(vec) = data.get(file) {
            f(&vec)
        } else {
            f(&[])
        };
        Ok(res)
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

    fn load<T>(&self, file: &str, mut f: impl FnMut(&[u8]) -> T) -> io::Result<T> {
        let res = match std::fs::read(self.base.join(file)) {
            Ok(data) => f(&data),
            Err(e) if e.kind() == io::ErrorKind::NotFound => f(&[]),
            Err(e) => return Err(e),
        };
        Ok(res)
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

struct RadixDb<K: TKey, V: TValue, S> {
    storage: S,
    name: String,
    serializers: Option<(
        SharedSerializeMap2,
        BTreeMap<usize, Arc<Vec<ArcRadixTree<K, V>>>>,
    )>,
    pos: usize,
    tree: ArcRadixTree<K, V>,
    watchers: Vec<UnboundedSender<ArcRadixTree<K, V>>>,
}

impl<K: TKey, V: TValue> RadixDb<K, V, MemStorage>
where
    Archived<K>: Deserialize<K, SharedDeserializeMap2>,
    Archived<V>: Deserialize<V, SharedDeserializeMap2>,
{
    fn memory(name: impl Into<String>) -> anyhow::Result<Self> {
        RadixDb::load(MemStorage::default(), name)
    }
}

impl<K: TKey, V: TValue> RadixDb<K, V, FileStorage>
where
    Archived<K>: Deserialize<K, SharedDeserializeMap2>,
    Archived<V>: Deserialize<V, SharedDeserializeMap2>,
{
    fn open(base: impl AsRef<std::path::Path>, name: impl Into<String>) -> anyhow::Result<Self> {
        RadixDb::load(FileStorage::new(base), name)
    }
}

impl<K: TKey, V: TValue, S: Storage> RadixDb<K, V, S> {
    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn load(storage: S, name: impl Into<String>) -> anyhow::Result<Self>
    where
        Archived<K>: Deserialize<K, SharedDeserializeMap2>,
        Archived<V>: Deserialize<V, SharedDeserializeMap2>,
    {
        let name = name.into();
        let (tree, map, arcs, pos) = storage.load(&name, |data| -> anyhow::Result<_> {
            Ok(if data.is_empty() {
                let pos = Default::default();
                let arcs = Default::default();
                let tree = Default::default();
                let map = Default::default();
                (tree, map, arcs, pos)
            } else {
                let mut deserializer = SharedDeserializeMap2::default();
                let tree: &Archived<ArcRadixTree<K, V>> =
                    unsafe { archived_root::<ArcRadixTree<K, V>>(data) };
                let tree: ArcRadixTree<K, V> = tree
                    .deserialize(&mut deserializer)
                    .map_err(|e| anyhow::anyhow!("Error while deserializing: {}", e))?;
                let map = deserializer.to_shared_serializer_map(&data[0] as *const u8);
                let mut arcs = BTreeMap::default();
                tree.all_arcs(&mut arcs);
                let pos = data.len();
                (tree, map, arcs, pos)
            })
        })??;
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

impl<K, V, S> AbstractRadixDb<K, V> for RadixDb<K, V, S>
where
    K: TKey + for<'x> Serialize<MySerializer<'x>>,
    V: TValue + for<'x> Serialize<MySerializer<'x>>,
    S: Storage,
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

#[async_std::main]
async fn main() -> anyhow::Result<()> {
    let mut db = RadixDb::open(std::env::current_dir()?, "test")?;
    let mut stream = db.watch_prefix("9".as_bytes().to_vec());
    async_std::task::spawn(async move {
        while let Some(x) = stream.next().await {
            for (added, _) in x.added().iter() {
                let text = std::str::from_utf8(&added).unwrap();
                println!("added {}", text);
            }
            for (removed, _) in x.removed().iter() {
                let text = std::str::from_utf8(&removed).unwrap();
                println!("removed {}", text);
            }
        }
    });
    for i in 0..100 {
        for j in 0..100 {
            let key = format!("{}-{}", i, j);
            db.tree_mut()
                .union_with(&ArcRadixTree::single(key.as_bytes(), ()));
        }
        if i % 10 == 0 {
            db.vacuum()?;
        } else {
            db.flush()?;
        }
        // db.flush()?;
        println!("{} {}", i, db.pos);
    }
    db.flush()?;
    println!("{}", db.pos);
    println!("db");
    for (k, v) in db.tree().iter() {
        println!("{}", std::str::from_utf8(&k)?);
    }
    let mut db2: RadixDb<u8, (), _> = RadixDb::load(db.storage().clone(), "test")?;
    db2.vacuum()?;
    println!("db2");
    for (k, v) in db2.tree().iter() {
        println!("{}", std::str::from_utf8(&k)?);
    }

    println!("{} {}", db.pos, db2.pos);
    Ok(())
}

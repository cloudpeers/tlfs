use crate::lens::Lenses;
use crate::schema::Schema;
use crate::util::Ref;
use anyhow::Result;
pub use blake3::Hash;
use parking_lot::RwLock;
use rkyv::string::ArchivedString;
use rkyv::{Archive, Archived, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A package of lenses.
#[derive(Clone, Debug, Eq, PartialEq, Archive, Deserialize, Serialize)]
#[archive_attr(derive(Debug, Eq, PartialEq))]
#[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
#[repr(C)]
pub struct Package {
    name: String,
    versions: Vec<(String, u32)>,
    lenses: Vec<u8>,
}

impl Package {
    /// Creates a new [`Lenses`] wrapper from a [`Vec<Lens>`].
    pub fn new(name: String, versions: Vec<(String, u32)>, lenses: Vec<u8>) -> Self {
        Self {
            name,
            versions,
            lenses,
        }
    }
}

impl ArchivedPackage {
    /// Returns the name of the lenses.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the versions of the lenses.
    pub fn versions(&self) -> &[(ArchivedString, u32)] {
        self.versions.as_ref()
    }

    /// Returns a reference to the [`ArchivedLenses`] bytes.
    pub fn lenses(&self) -> &[u8] {
        &self.lenses
    }
}

/// Expanded lenses.
pub struct Expanded {
    lenses: Ref<Lenses>,
    schema: Ref<Schema>,
}

impl Expanded {
    /// Expands lenses.
    pub fn new(lenses: Ref<Lenses>) -> Result<Self> {
        let schema = Ref::new(lenses.as_ref().to_schema()?.into());
        Ok(Self { lenses, schema })
    }

    /// Returns a reference to the [`ArchivedLenses`].
    pub fn lenses(&self) -> &Archived<Lenses> {
        self.lenses.as_ref()
    }

    /// Returns a reference to the [`ArchivedSchema`].
    pub fn schema(&self) -> &Archived<Schema> {
        self.schema.as_ref()
    }
}

impl AsRef<[u8]> for Expanded {
    fn as_ref(&self) -> &[u8] {
        self.lenses.as_bytes()
    }
}

impl std::fmt::Debug for Expanded {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Expanded")
            .field("lenses", self.lenses.as_ref())
            .field("schema", self.schema.as_ref())
            .finish()
    }
}

/// Lens registry.
#[derive(Clone)]
pub struct Registry {
    table: Arc<BTreeMap<String, Hash>>,
    expanded: Arc<RwLock<BTreeMap<[u8; 32], Arc<Expanded>>>>,
}

impl Registry {
    /// Creates a new lens registry.
    pub fn new(packages: &[u8]) -> Result<Self> {
        let packages = unsafe { rkyv::archived_root::<Vec<Package>>(packages) };
        let mut table = BTreeMap::new();
        let mut expanded = BTreeMap::new();
        for package in packages.as_ref() {
            let lenses = Ref::new(package.lenses().into());
            let hash = blake3::hash(lenses.as_bytes());
            let name = package.name().into();
            table.insert(name, hash);
            expanded.insert(hash.into(), Arc::new(Expanded::new(lenses)?));
        }
        Ok(Self {
            table: Arc::new(table),
            expanded: Arc::new(RwLock::new(expanded)),
        })
    }

    /// Registers archived [`Lenses`] and returns the [`struct@Hash`].
    pub fn register(&self, lenses: &[u8]) -> Result<Hash> {
        let lenses = Ref::<Lenses>::checked(lenses)?;
        let hash = blake3::hash(lenses.as_bytes());
        self.expanded
            .write()
            .insert(hash.into(), Arc::new(Expanded::new(lenses)?));
        Ok(hash)
    }

    /// Returns the schema.
    pub fn get(&self, hash: &Hash) -> Option<Arc<Expanded>> {
        self.expanded.read().get(hash.as_bytes()).cloned()
    }

    /// Returns the schema by name.
    pub fn lookup(&self, id: &str) -> Option<(Hash, u32)> {
        let hash = *self.table.get(id)?;
        let len = self
            .expanded
            .read()
            .get(hash.as_bytes())?
            .lenses()
            .lenses()
            .len();
        Some((hash, len as u32))
    }

    /// Returns true if the registry contains the [`Schema`] identified by [`struct@Hash`].
    pub fn contains(&self, hash: &Hash) -> bool {
        self.expanded.read().contains_key(hash.as_bytes())
    }
}

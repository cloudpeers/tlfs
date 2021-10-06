use crate::{Lenses, Ref, Schema};
use anyhow::{anyhow, Result};
pub use blake3::Hash;
use rkyv::validation::validators::check_archived_root;

pub const EMPTY_LENSES: [u8; 8] = [0; 8];

pub fn empty_hash() -> Hash {
    blake3::hash(&EMPTY_LENSES)
}

pub struct Registry(sled::Tree);

impl Registry {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    pub fn register(&self, lenses: Vec<u8>) -> Result<Hash> {
        let lenses_ref =
            check_archived_root::<Lenses>(&lenses[..]).map_err(|err| anyhow!("{}", err))?;
        let schema = lenses_ref.to_schema()?;
        let hash = blake3::hash(&lenses[..]);
        let mut key1 = [0; 33];
        key1[..32].copy_from_slice(hash.as_bytes());
        let mut key2 = key1;
        key2[32] = 1;
        self.0.transaction::<_, _, std::io::Error>(|tree| {
            tree.insert(&key1[..], &lenses[..])?;
            tree.insert(&key2[..], &schema[..])?;
            Ok(())
        })?;
        Ok(hash)
    }

    pub fn contains(&self, hash: &Hash) -> Result<bool> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(hash.as_bytes());
        Ok(self.0.contains_key(key)?)
    }

    pub fn lenses(&self, hash: &Hash) -> Result<Option<Ref<Lenses>>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(hash.as_bytes());
        Ok(self.0.get(key)?.map(Ref::new))
    }

    pub fn schema(&self, hash: &Hash) -> Result<Option<Ref<Schema>>> {
        let mut key = [1; 33];
        key[..32].copy_from_slice(hash.as_bytes());
        Ok(self.0.get(key)?.map(Ref::new))
    }

    pub fn remove(&self, hash: &Hash) -> Result<()> {
        let mut key1 = [0; 33];
        key1[..32].copy_from_slice(hash.as_bytes());
        let mut key2 = key1;
        key2[32] = 1;
        self.0.transaction::<_, _, std::io::Error>(|tree| {
            tree.remove(&key1[..])?;
            tree.remove(&key2[..])?;
            Ok(())
        })?;
        Ok(())
    }
}

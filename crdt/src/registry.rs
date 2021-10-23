use crate::lens::Lenses;
use crate::schema::Schema;
use crate::util::Ref;
use anyhow::{anyhow, Result};
pub use blake3::Hash;
use rkyv::validation::validators::check_archived_root;

/// Equivalent to `Ref::archive(&Lenses::new(vec![])).as_bytes()`.
pub const EMPTY_LENSES: [u8; 8] = [0; 8];
/// Equivalent to `Ref::archive(&Schema::Null).as_bytes()`.
pub const EMPTY_SCHEMA: [u8; 12] = [0; 12];
/// Equivalent to `blake3::hash(&EMPTY_LENSES)`.
pub const EMPTY_HASH: [u8; 32] = [
    113, 224, 169, 145, 115, 86, 73, 49, 192, 184, 172, 197, 45, 38, 133, 168, 227, 156, 100, 220,
    82, 227, 208, 35, 144, 253, 172, 42, 18, 177, 85, 203,
];

/// Lens registry.
#[derive(Clone)]
pub struct Registry(sled::Tree);

impl Registry {
    /// Creates a new lens registry.
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    /// Registers archived [`Lenses`] and returns the [`struct@Hash`].
    pub fn register(&self, lenses: &[u8]) -> Result<Hash> {
        let lenses_ref = check_archived_root::<Lenses>(lenses).map_err(|err| anyhow!("{}", err))?;
        let schema = lenses_ref.to_schema()?;
        let hash = blake3::hash(lenses);
        let mut key1 = [0; 33];
        key1[..32].copy_from_slice(hash.as_bytes());
        let mut key2 = key1;
        key2[32] = 1;
        self.0.transaction::<_, _, std::io::Error>(|tree| {
            tree.insert(&key1[..], lenses)?;
            tree.insert(&key2[..], &schema[..])?;
            Ok(())
        })?;
        Ok(hash)
    }

    /// Returns true if the registry contains the [`Schema`] identified by [`struct@Hash`].
    pub fn contains(&self, hash: &Hash) -> Result<bool> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(hash.as_bytes());
        Ok(self.0.contains_key(key)?)
    }

    /// Returns the archived [`Lenses`] identified by [`struct@Hash`].
    pub fn lenses(&self, hash: &Hash) -> Result<Option<Ref<Lenses>>> {
        let mut key = [0; 33];
        key[..32].copy_from_slice(hash.as_bytes());
        Ok(self.0.get(key)?.map(Ref::new))
    }

    /// Returns the archived [`Schema`] identified by [`struct@Hash`].
    pub fn schema(&self, hash: &Hash) -> Result<Option<Ref<Schema>>> {
        let mut key = [1; 33];
        key[..32].copy_from_slice(hash.as_bytes());
        Ok(self.0.get(key)?.map(Ref::new))
    }

    /// Removes the [`Schema`] identified by [`struct@Hash`] from the registry.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Lenses;

    #[test]
    fn test_empty_lenses() {
        assert_eq!(Ref::archive(&Lenses::new(vec![])).as_bytes(), EMPTY_LENSES);
        Ref::<Lenses>::new(EMPTY_LENSES.to_vec().into()).as_ref();
    }

    #[test]
    fn test_empty_schema() {
        assert_eq!(Ref::archive(&Schema::Null).as_bytes(), EMPTY_SCHEMA);
        Ref::<Schema>::new(EMPTY_SCHEMA.to_vec().into()).as_ref();
    }

    #[test]
    fn test_empty_hash() {
        assert_eq!(blake3::hash(&EMPTY_LENSES).as_bytes(), &EMPTY_HASH);
    }
}

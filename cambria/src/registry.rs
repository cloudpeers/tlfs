use crate::lens::{ArchivedLenses, Lenses};
use anyhow::{anyhow, Result};
pub use blake3::Hash;
use rkyv::archived_root;
use rkyv::validation::validators::check_archived_root;
use std::collections::BTreeMap;
use tlfs_acl::{ArchivedSchema, Schema};

const EMPTY_LENSES: [u8; 8] = [0; 8];

pub fn empty_hash() -> Hash {
    blake3::hash(&EMPTY_LENSES)
}

struct ExpandedLenses {
    lenses: Vec<u8>,
    schema: Vec<u8>,
}

pub struct Registry {
    lens: BTreeMap<[u8; 32], ExpandedLenses>,
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        let mut reg = Self {
            lens: Default::default(),
        };
        reg.register(EMPTY_LENSES.to_vec()).unwrap();
        reg
    }

    pub fn register(&mut self, lenses: Vec<u8>) -> Result<Hash> {
        let lenses_ref =
            check_archived_root::<Lenses>(&lenses[..]).map_err(|err| anyhow!("{}", err))?;
        let schema = lenses_ref.to_schema()?;
        let hash = blake3::hash(&lenses[..]);
        let expanded = ExpandedLenses { lenses, schema };
        self.lens.insert(hash.into(), expanded);
        Ok(hash)
    }

    pub fn lenses(&self, hash: &Hash) -> Option<&ArchivedLenses> {
        let lenses = &self.lens.get(hash.as_bytes())?.lenses[..];
        Some(unsafe { archived_root::<Lenses>(lenses) })
    }

    pub fn schema(&self, hash: &Hash) -> Option<&ArchivedSchema> {
        let schema = &self.lens.get(hash.as_bytes())?.schema[..];
        Some(unsafe { archived_root::<Schema>(schema) })
    }
}

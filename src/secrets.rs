use anyhow::Result;
use std::convert::TryInto;
use tlfs_crdt::{DocId, Key, KeyNonce, Keypair, PeerId, Ref};

#[repr(u8)]
enum KeyType {
    Keypair,
    Key,
    Nonce,
}

/// Information attached to a secret for queries.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Metadata([u8; 65]);

impl Metadata {
    /// Creates a top level query. This will return the defaults.
    pub fn new() -> Self {
        Self([0; 65])
    }

    /// Queries a secret based on a doc.
    pub fn doc(mut self, doc: DocId) -> Self {
        self.0[..32].copy_from_slice(doc.as_ref());
        self
    }

    /// Queries a secret based on a peer.
    pub fn peer(mut self, peer: PeerId) -> Self {
        self.0[32..64].copy_from_slice(peer.as_ref());
        self
    }

    fn ty(mut self, ty: KeyType) -> Self {
        self.0[64] = ty as u8;
        self
    }
}

impl Default for Metadata {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<[u8]> for Metadata {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub struct Secrets(sled::Tree);

impl Secrets {
    pub fn new(tree: sled::Tree) -> Self {
        Self(tree)
    }

    pub fn keypair(&self, metadata: Metadata) -> Result<Option<Keypair>> {
        Ok(self
            .0
            .get(metadata.ty(KeyType::Keypair))?
            .map(|v| *Ref::<Keypair>::new(v).as_ref()))
    }

    pub fn key(&self, metadata: Metadata) -> Result<Option<Key>> {
        Ok(self
            .0
            .get(metadata.ty(KeyType::Key))?
            .map(|v| *Ref::<Key>::new(v).as_ref()))
    }

    pub fn nonce(&self, metadata: Metadata) -> Result<u64> {
        let key = metadata.ty(KeyType::Nonce);
        let nonce = self.0.transaction::<_, _, std::io::Error>(|tree| {
            let nonce = tree
                .get(key.as_ref())?
                .map(|v| u64::from_le_bytes(v.as_ref().try_into().unwrap()))
                .unwrap_or_default();
            let next = nonce + 1;
            tree.insert(key.as_ref(), next.to_le_bytes().as_ref())?;
            Ok(nonce)
        })?;
        Ok(nonce)
    }

    pub fn key_nonce(&self, metadata: Metadata) -> Result<Option<KeyNonce>> {
        if let Some(key) = self.key(metadata)? {
            let nonce = self.nonce(metadata)?;
            Ok(Some(KeyNonce::new(key, nonce)))
        } else {
            Ok(None)
        }
    }

    pub fn generate_keypair(&self, metadata: Metadata) -> Result<()> {
        self.0.insert(
            metadata.ty(KeyType::Keypair),
            Ref::archive(&Keypair::generate()).as_bytes(),
        )?;
        Ok(())
    }

    pub fn generate_key(&self, metadata: Metadata) -> Result<()> {
        self.0.insert(
            metadata.ty(KeyType::Key),
            Ref::archive(&Key::generate()).as_bytes(),
        )?;
        self.0.insert(metadata.ty(KeyType::Key), [0; 8].as_ref())?;
        Ok(())
    }

    pub fn add_key(&self, metadata: Metadata, key: Key) -> Result<()> {
        self.0
            .insert(metadata.ty(KeyType::Key), Ref::archive(&key).as_bytes())?;
        Ok(())
    }
}

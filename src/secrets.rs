use std::collections::BTreeMap;
use tlfs_acl::{DocId, Key, KeyNonce, Keypair, PeerId};

#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct Metadata {
    label: Option<String>,
    doc: Option<DocId>,
    peer: Option<PeerId>,
}

impl Metadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn label(mut self, label: &str) -> Self {
        self.label = Some(label.into());
        self
    }

    pub fn doc(mut self, doc: DocId) -> Self {
        self.doc = Some(doc);
        self
    }

    pub fn peer(mut self, peer: PeerId) -> Self {
        self.peer = Some(peer);
        self
    }
}

#[derive(Default)]
struct Secret {
    keypair: Option<Keypair>,
    key: Option<Key>,
    key_nonce: Option<KeyNonce>,
    peer_id: Option<PeerId>,
}

impl Secret {
    fn merge(&mut self, other: Secret) {
        if other.keypair.is_some() {
            self.keypair = other.keypair
        }
        if other.key.is_some() {
            self.key = other.key
        }
        if other.key_nonce.is_some() {
            self.key_nonce = other.key_nonce
        }
        if other.peer_id.is_some() {
            self.peer_id = other.peer_id
        }
    }
}

impl From<Keypair> for Secret {
    fn from(key: Keypair) -> Self {
        Self {
            keypair: Some(key),
            key: None,
            key_nonce: None,
            peer_id: None,
        }
    }
}

impl From<Key> for Secret {
    fn from(key: Key) -> Self {
        Self {
            keypair: None,
            key: Some(key),
            key_nonce: None,
            peer_id: None,
        }
    }
}

impl From<KeyNonce> for Secret {
    fn from(key: KeyNonce) -> Self {
        Self {
            keypair: None,
            key: None,
            key_nonce: Some(key),
            peer_id: None,
        }
    }
}

impl From<PeerId> for Secret {
    fn from(peer: PeerId) -> Self {
        Self {
            keypair: None,
            key: None,
            key_nonce: None,
            peer_id: Some(peer),
        }
    }
}

#[derive(Default)]
pub struct Secrets {
    secrets: BTreeMap<Metadata, Secret>,
}

impl Secrets {
    pub fn keypair(&self, metadata: &Metadata) -> Option<&Keypair> {
        self.secrets.get(metadata)?.keypair.as_ref()
    }

    pub fn key(&self, metadata: &Metadata) -> Option<&Key> {
        self.secrets.get(metadata)?.key.as_ref()
    }

    pub fn key_nonce(&mut self, metadata: &Metadata) -> Option<&mut KeyNonce> {
        self.secrets.get_mut(metadata)?.key_nonce.as_mut()
    }

    #[allow(unused)]
    pub fn peer_id(&self, metadata: &Metadata) -> Option<&PeerId> {
        self.secrets.get(metadata)?.peer_id.as_ref()
    }

    pub fn generate_keypair(&mut self, metadata: Metadata) {
        self.secrets
            .entry(metadata)
            .or_default()
            .merge(Secret::from(Keypair::generate()));
    }

    pub fn generate_key(&mut self, metadata: Metadata) {
        self.secrets
            .entry(metadata)
            .or_default()
            .merge(Secret::from(KeyNonce::generate()));
    }

    pub fn add_key(&mut self, metadata: Metadata, key: Key) {
        self.secrets
            .entry(metadata)
            .or_default()
            .merge(Secret::from(key));
    }

    pub fn add_peer(&mut self, metadata: Metadata, peer: PeerId) {
        self.secrets
            .entry(metadata)
            .or_default()
            .merge(Secret::from(peer));
    }
}

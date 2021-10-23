use crate::id::PeerId;
use anyhow::Result;
use bytecheck::CheckBytes;
use ed25519_dalek::{PublicKey, SecretKey, Signature, Signer};
use rkyv::{Archive, Deserialize, Serialize};

/// ed25519 keypair.
#[derive(Clone, Copy, Archive, CheckBytes, Serialize, Deserialize)]
#[archive(as = "Keypair")]
#[repr(transparent)]
pub struct Keypair([u8; 32]);

impl Keypair {
    /// Creates a new ed25519 [`Keypair`] from a secret.
    pub fn new(secret: [u8; 32]) -> Self {
        Self(secret)
    }

    /// Generates a new ed25519 [`Keypair`].
    pub fn generate() -> Self {
        let mut secret = [0; 32];
        getrandom::getrandom(&mut secret).unwrap();
        Self(secret)
    }

    fn to_keypair(self) -> ed25519_dalek::Keypair {
        let secret = SecretKey::from_bytes(&self.0).unwrap();
        let public = PublicKey::from(&secret);
        ed25519_dalek::Keypair { secret, public }
    }

    /// Returns the [`PeerId`] identifying the [`Keypair`].
    pub fn peer_id(self) -> PeerId {
        PeerId::new(self.to_keypair().public.to_bytes())
    }

    /// Signs a message.
    pub fn sign(self, msg: &[u8]) -> Signature {
        self.to_keypair().sign(msg)
    }
}

impl std::fmt::Debug for Keypair {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Keypair({:?})", self.peer_id())
    }
}

impl From<Keypair> for [u8; 32] {
    fn from(keypair: Keypair) -> Self {
        keypair.0
    }
}

impl AsRef<[u8]> for Keypair {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

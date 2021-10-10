use crate::PeerId;
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use chacha20poly1305::aead::{AeadInPlace, NewAead};
use chacha20poly1305::ChaCha8Poly1305;
use ed25519_dalek::{PublicKey, SecretKey, Signature, Signer, Verifier};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::validation::validators::{check_archived_root, DefaultValidator};
use rkyv::{archived_root_mut, Archive, Archived, Deserialize, Serialize};
use std::pin::Pin;

#[derive(Clone, Copy, Archive, Serialize, Deserialize)]
#[archive(as = "Keypair")]
#[repr(transparent)]
pub struct Keypair([u8; 32]);

impl Keypair {
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

    pub fn peer_id(self) -> PeerId {
        PeerId::new(self.to_keypair().public.to_bytes())
    }

    pub fn sign<P>(self, payload: &P) -> Signed
    where
        P: Serialize<AllocSerializer<256>>,
    {
        let mut ser = AllocSerializer::<256>::default();
        ser.serialize_value(payload).unwrap();
        let payload = ser.into_serializer().into_inner().to_vec();
        let keypair = self.to_keypair();
        let sig = keypair.sign(&payload).to_bytes();
        let peer_id = PeerId::new(keypair.public.to_bytes());
        Signed {
            payload,
            peer_id,
            sig,
        }
    }
}

#[derive(Clone, Archive, CheckBytes, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct Signed {
    pub payload: Vec<u8>,
    pub peer_id: PeerId,
    pub sig: [u8; 64],
}

impl ArchivedSigned {
    pub fn verify<'a, P>(&'a self) -> Result<(PeerId, &'a Archived<P>)>
    where
        P: Archive,
        Archived<P>: CheckBytes<DefaultValidator<'a>>,
    {
        let public = PublicKey::from_bytes(self.peer_id.as_ref()).unwrap();
        let sig = Signature::from(self.sig);
        public.verify(&self.payload[..], &sig)?;
        // TODO: doesn't work
        //let payload =
        //    check_archived_root::<P>(&self.payload[..]).map_err(|err| anyhow!("{}", err))?;
        let payload = unsafe { rkyv::archived_root::<P>(&self.payload[..]) };
        Ok((self.peer_id, payload))
    }
}

#[derive(Clone, Copy, Archive, Serialize, Deserialize)]
#[archive(as = "Key")]
#[repr(C)]
pub struct Key([u8; 32]);

impl Key {
    pub fn generate() -> Self {
        let mut key = [0; 32];
        getrandom::getrandom(&mut key).unwrap();
        Self(key)
    }

    pub fn encrypt<P>(&self, payload: &P, nonce: u64) -> Encrypted
    where
        P: Serialize<AllocSerializer<256>>,
    {
        let mut ser = AllocSerializer::<256>::default();
        ser.serialize_value(payload).unwrap();
        let payload = ser.into_serializer().into_inner().to_vec();
        let mut payload = Encrypted {
            nonce: nonce.to_le_bytes(),
            payload,
            tag: [0; 16],
        };
        let mut nonce = [0; 12];
        nonce[0..8].copy_from_slice(&payload.nonce);
        let tag = ChaCha8Poly1305::new(&self.0.into())
            .encrypt_in_place_detached(&nonce.into(), &[], &mut payload.payload)
            .unwrap();
        payload.tag.copy_from_slice(&tag);
        payload
    }

    pub fn decrypt<'a, P>(&self, payload: &'a mut [u8]) -> Result<&'a Archived<P>>
    where
        P: Archive,
        Archived<P>: CheckBytes<DefaultValidator<'a>>,
    {
        check_archived_root::<Encrypted>(payload).map_err(|err| anyhow!("{}", err))?;
        let payload = unsafe { archived_root_mut::<Encrypted>(Pin::new(payload)) };
        let mut nonce = [0; 12];
        nonce[..8].copy_from_slice(&payload.nonce);
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                payload.payload.as_ptr() as *mut _,
                payload.payload.len(),
            )
        };
        ChaCha8Poly1305::new(&self.0.into())
            .decrypt_in_place_detached(&nonce.into(), &[], bytes, &payload.tag.into())
            .map_err(|err| anyhow!("{}", err))?;
        check_archived_root::<P>(bytes).map_err(|err| anyhow!("{}", err))
    }
}

#[derive(Clone, Archive, CheckBytes, Serialize, Deserialize)]
#[archive_attr(derive(CheckBytes))]
#[repr(C)]
pub struct Encrypted {
    pub nonce: [u8; 8],
    pub payload: Vec<u8>,
    pub tag: [u8; 16],
}

impl Encrypted {
    pub fn archive(&self) -> Vec<u8> {
        let mut ser = AllocSerializer::<256>::default();
        ser.serialize_value(self).unwrap();
        ser.into_serializer().into_inner().to_vec()
    }
}

#[derive(Archive, Serialize, Deserialize)]
#[archive(as = "KeyNonce")]
#[repr(C)]
pub struct KeyNonce {
    key: Key,
    nonce: u64,
}

impl KeyNonce {
    pub fn new(key: Key, nonce: u64) -> Self {
        Self { key, nonce }
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn encrypt<P>(self, payload: &P) -> Encrypted
    where
        P: Serialize<AllocSerializer<256>>,
    {
        self.key.encrypt(payload, self.nonce)
    }
}

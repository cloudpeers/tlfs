use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(
    Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, CheckBytes, Deserialize, Serialize,
)]
#[archive(as = "DocId")]
#[repr(transparent)]
pub struct DocId([u8; 32]);

impl DocId {
    pub fn new(id: [u8; 32]) -> Self {
        Self(id)
    }
}

impl From<DocId> for [u8; 32] {
    fn from(id: DocId) -> Self {
        id.0
    }
}

impl AsRef<[u8; 32]> for DocId {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for DocId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0[0..2]))
    }
}

impl std::fmt::Display for DocId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut peer_id = [0; 44];
        base64::encode_config_slice(&self.0, base64::URL_SAFE, &mut peer_id);
        write!(f, "{}", std::str::from_utf8(&peer_id).expect("wtf?"))
    }
}

impl std::str::FromStr for DocId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 44 {
            return Err(anyhow::anyhow!("invalid peer_id length {}", s.len()));
        }
        let mut peer_id = [0; 32];
        base64::decode_config_slice(s, base64::URL_SAFE, &mut peer_id)?;
        Ok(Self(peer_id))
    }
}

#[derive(
    Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Archive, CheckBytes, Deserialize, Serialize,
)]
#[archive(as = "PeerId")]
#[repr(transparent)]
pub struct PeerId([u8; 32]);

impl PeerId {
    pub fn new(id: [u8; 32]) -> Self {
        Self(id)
    }
}

impl From<PeerId> for [u8; 32] {
    fn from(id: PeerId) -> Self {
        id.0
    }
}

impl AsRef<[u8; 32]> for PeerId {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl std::fmt::Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", hex::encode(&self.0[0..2]))
    }
}

impl std::fmt::Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut peer_id = [0; 44];
        base64::encode_config_slice(&self.0, base64::URL_SAFE, &mut peer_id);
        write!(f, "{}", std::str::from_utf8(&peer_id).expect("wtf?"))
    }
}

impl std::str::FromStr for PeerId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 44 {
            return Err(anyhow::anyhow!("invalid peer_id length {}", s.len()));
        }
        let mut peer_id = [0; 32];
        base64::decode_config_slice(s, base64::URL_SAFE, &mut peer_id)?;
        Ok(Self(peer_id))
    }
}

impl From<DocId> for PeerId {
    fn from(id: DocId) -> Self {
        Self::new(id.into())
    }
}

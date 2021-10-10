use crate::secrets::{Metadata, Secrets};
use anyhow::{anyhow, Result};
use rkyv::Deserialize;
use tlfs_crdt::{Causal, CausalContext, Key, PeerId, Ref, Signed};

pub struct Doc {
    doc: tlfs_crdt::Doc,
    secrets: Secrets,
}

impl Doc {
    pub fn new(doc: tlfs_crdt::Doc, secrets: Secrets) -> Self {
        Self { doc, secrets }
    }

    /// Adds a decryption key for a peer.
    pub fn add_key(&self, peer_id: PeerId, key: Key) -> Result<()> {
        self.secrets
            .add_key(Metadata::new().doc(*self.doc.id()).peer(peer_id), key)?;
        Ok(())
    }

    /// Applies a local change to the current document and returns a signed and
    /// encrypted change to send to peers.
    pub fn apply(&self, causal: Causal) -> Result<Vec<u8>> {
        let signed = self
            .secrets
            .keypair(Metadata::new())?
            .unwrap()
            .sign(&causal);
        let metadata = Metadata::new()
            .doc(*self.doc.id())
            .peer(*self.doc.peer_id());
        let encrypted = self.secrets.key_nonce(metadata)?.unwrap().encrypt(&signed);
        self.doc.join(self.doc.peer_id(), causal)?;
        Ok(Ref::archive(&encrypted).into())
    }

    /// Joins a signed and encrypted delta sent by peer with `peer_id` in to
    /// the current state.
    pub fn join(&mut self, peer_id: &PeerId, payload: &mut [u8]) -> Result<()> {
        let signed = self
            .secrets
            .key(Metadata::new().doc(*self.doc.id()).peer(*peer_id))?
            .unwrap()
            .decrypt::<Signed>(payload)?;
        let (peer_id, causal) = signed.verify::<Causal>()?;
        let causal: Causal = causal.deserialize(&mut rkyv::Infallible)?;
        self.doc.join(&peer_id, causal)?;
        Ok(())
    }

    /// Takes a causal context and returns the delta to bring the peer up to speed. This
    /// is neither signed nor encrypted as it is assumed to be transmitted over a
    /// secure channel.
    pub fn unjoin(&self, peer_id: PeerId, ctx: &[u8]) -> Result<Vec<u8>> {
        let ctx =
            rkyv::check_archived_root::<CausalContext>(ctx).map_err(|err| anyhow!("{}", err))?;
        let causal = self.doc.unjoin(&peer_id, ctx)?;
        Ok(Ref::archive(&causal).into())
    }
}

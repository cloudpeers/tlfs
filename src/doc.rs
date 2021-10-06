use crate::crypto::{Key, Keypair, Signed};
use crate::secrets::Metadata;
use crate::Sdk;
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{Archive, Deserialize, Serialize};
use tlfs_crdt::{
    empty_hash, transform, Actor, Causal, CausalContext, Cursor, DocId, Dot, Hash, Lenses, PathBuf,
    PeerId, Permission, Policy, Ref, Schema, W,
};

fn archive<T>(t: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(t).unwrap();
    ser.into_serializer().into_inner().to_vec()
}

#[derive(Archive, Serialize)]
#[archive_attr(derive(CheckBytes))]
pub struct Delta {
    causal: Causal,
    hash: [u8; 32],
}

pub struct Doc<'a> {
    id: DocId,
    hash: Hash,
    peer_id: PeerId,
    counter: u64,
    ctx: CausalContext,
    sdk: &'a Sdk,
}

impl<'a> Doc<'a> {
    pub(crate) fn new(sdk: &'a Sdk) -> Result<Self> {
        let la = Keypair::generate();
        let id = DocId::new(la.peer_id().into());
        let hash = empty_hash();
        let peer_id = sdk.secrets().keypair(&Metadata::new()).unwrap().peer_id();
        sdk.secrets()
            .generate_key(Metadata::new().doc(id).peer(peer_id));
        let delta = sdk.crdt.say(
            PathBuf::new(id).as_path(),
            Dot::new(id.into(), 1),
            Policy::Can(Actor::Peer(peer_id), Permission::Own),
        )?;
        let mut ctx = CausalContext::default();
        sdk.crdt.join(id, &mut ctx, &delta)?;
        Ok(Self {
            id,
            hash,
            peer_id,
            counter: 0,
            ctx,
            sdk,
        })
    }

    /// Returns the document identifier.
    pub fn id(&self) -> DocId {
        self.id
    }

    /// Returns the schema identifier.
    pub fn schema_id(&self) -> Hash {
        self.hash
    }

    /// Returns the lenses.
    pub fn lenses(&self) -> Result<Ref<Lenses>> {
        Ok(self.sdk.registry.lenses(&self.hash)?.unwrap())
    }

    /// Returns the schema.
    pub fn schema(&self) -> Result<Ref<Schema>> {
        Ok(self.sdk.registry.schema(&self.hash)?.unwrap())
    }

    /// Adds a decryption key for a peer.
    pub fn add_key(&self, peer_id: PeerId, key: Key) {
        self.sdk
            .secrets
            .add_key(Metadata::new().doc(self.id).peer(peer_id), key);
    }

    /// Returns the current causal context.
    pub fn ctx(&self) -> Vec<u8> {
        archive(&self.ctx)
    }

    /// Performs a read only query on the document.
    pub fn cursor<T, F>(&self, f: F) -> Result<T>
    where
        F: FnMut(Cursor<'_, ()>) -> Result<T>,
    {
        let schema = self.schema()?;
        f(Cursor::<'_, ()>::new(
            self.id,
            &self.sdk.crdt,
            &self.sdk.engine,
            schema.as_ref(),
        ))
    }

    /// Performs a transaction on the document returning a signed and
    /// encrypted change set that can be applied to a document.
    pub fn transaction<F>(&self, mut f: F) -> Result<Vec<u8>>
    where
        F: FnMut(Cursor<'_, W>) -> Result<Causal>,
    {
        let causal = {
            let schema = self.schema()?;
            let cursor = Cursor::<'_, W>::new(
                self.id,
                &self.sdk.crdt,
                self.ctx.clone(),
                &self.sdk.engine,
                self.peer_id,
                self.counter,
                schema.as_ref(),
            );
            f(cursor)?
        };
        let counter = causal.ctx.max(&self.peer_id);
        if counter <= self.counter {
            return Err(anyhow!("invalid transaction"));
        }
        let delta = Delta {
            causal,
            hash: self.hash.into(),
        };
        let signed = self
            .sdk
            .secrets
            .keypair(&Metadata::new())
            .unwrap()
            .sign(&delta);
        let metadata = Metadata::new().doc(self.id).peer(self.peer_id);
        let encrypted = self
            .sdk
            .secrets
            .key_nonce(&metadata)
            .unwrap()
            .encrypt(&signed);
        self.counter = counter;
        Ok(encrypted.archive())
    }

    /// Joins a signed and encrypted delta sent by peer with `peer_id` in to
    /// the current state.
    pub fn join(&self, peer_id: &PeerId, payload: &mut [u8]) -> Result<()> {
        let signed = self
            .sdk
            .secrets
            .key(&Metadata::new().doc(self.id).peer(*peer_id))
            .unwrap()
            .decrypt::<Signed>(payload)?;
        let (peer_id, delta) = signed.verify::<Delta>()?;
        let mut causal: Causal = delta.causal.deserialize(&mut rkyv::Infallible)?;
        let hash = Hash::from(delta.hash);
        if !self
            .sdk
            .registry
            .schema(&hash)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", hash))?
            .as_ref()
            .validate(&causal.store)
        {
            return Err(anyhow!("crdt failed schema validation"));
        }
        let from_lenses = self.sdk.registry.lenses(&hash)?.expect("schema fetched");
        let to_lenses = self
            .sdk
            .registry
            .lenses(&self.hash)?
            .expect("current schema");
        transform(from_lenses.as_ref(), &mut causal.store, to_lenses.as_ref());
        causal.store = self.sdk.engine.filter(
            PathBuf::new(self.id),
            peer_id,
            Permission::Write,
            &causal.store,
        );
        self.sdk.crdt.join(self.id, &mut self.ctx, &causal);
        // TODO: poll engine
        Ok(())
    }

    /// Takes a causal context and returns the delta to bring the peer up to speed. This
    /// is neither signed nor encrypted as it is assumed to be transmitted over a
    /// secure channel.
    pub fn unjoin(&self, peer_id: PeerId, ctx: &[u8]) -> Result<Vec<u8>> {
        let ctx = rkyv::check_archived_root::<CausalContext>(ctx)
            .map_err(|err| anyhow!("{}", err))?
            .deserialize(&mut rkyv::Infallible)?;
        let mut causal = self.sdk.crdt.unjoin(&ctx);
        causal.store = self.sdk.engine.filter(
            PathBuf::new(self.id),
            peer_id,
            Permission::Read,
            &causal.store,
        );
        let delta = Delta {
            causal,
            hash: self.hash.into(),
        };
        Ok(archive(&delta))
    }

    /// Transforms the document a schema in the schema registry identified by it's hash.
    pub fn transform(&self, hash: Hash) -> Result<()> {
        let from_lenses = self
            .sdk
            .registry
            .lenses(&self.hash)?
            .expect("current lenses are registered");
        let to_lenses = self
            .sdk
            .registry
            .lenses(&hash)?
            .ok_or_else(|| anyhow!("missing lenses with hash {}", hash))?;
        transform(from_lenses.as_ref(), &mut self.sdk.crdt, to_lenses.as_ref());
        self.hash = hash;
        Ok(())
    }
}

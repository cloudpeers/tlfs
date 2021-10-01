use crate::secrets::Metadata;
use crate::State;
use anyhow::{anyhow, Result};
use bytecheck::CheckBytes;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{Archive, Deserialize, Serialize};
use std::cell::RefCell;
use std::rc::Rc;
use tlfs_acl::{
    Actor, Can, Causal, CausalContext, Crdt, Cursor, DocId, Dot, Id, Key, Keypair, Label, LabelRef,
    PeerId, Permission, Policy, Signed, W,
};
use tlfs_cambria::Hash;

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

pub struct Doc {
    id: DocId,
    hash: Hash,
    peer_id: PeerId,
    counter: u64,
    crdt: Causal,
    state: Rc<RefCell<State>>,
}

impl Doc {
    pub(crate) fn new(state: Rc<RefCell<State>>) -> Self {
        let la = Keypair::generate();
        let id = DocId::new(la.peer_id().into());
        let hash = tlfs_cambria::empty_hash();
        let peer_id = state
            .borrow()
            .secrets
            .keypair(&Metadata::new())
            .unwrap()
            .peer_id();
        state
            .borrow_mut()
            .secrets
            .generate_key(Metadata::new().doc(id).peer(peer_id));
        let acl = Policy::Can(Actor::Peer(peer_id), Permission::Own);
        let crdt = Crdt::say(Dot::new(PeerId::new(id.into()), 1), acl);
        state.borrow_mut().engine.says(
            Dot::new(Id::Doc(id), 1),
            Can::new(Actor::Peer(peer_id), Permission::Own, Label::Root(id)),
        );
        Self {
            id,
            hash,
            peer_id,
            counter: 0,
            crdt,
            state,
        }
    }

    /// Returns the document identifier.
    pub fn id(&self) -> DocId {
        self.id
    }

    /// Performs a read only query on the document.
    pub fn cursor<R, F: FnMut(Cursor<'_, ()>) -> R>(&self, mut f: F) -> R {
        let state = self.state.borrow();
        f(Cursor::<'_, ()>::new(self.id, &self.crdt, &state.engine))
    }

    /// Returns the schema identifier.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Adds a decryption key for a peer.
    pub fn add_key(&mut self, peer_id: PeerId, key: Key) {
        self.state
            .borrow_mut()
            .secrets
            .add_key(Metadata::new().doc(self.id).peer(peer_id), key);
    }

    /// Performs a transaction on the document returning a signed and
    /// encrypted change set that can be applied to a document.
    pub fn transaction<F>(&mut self, mut f: F) -> Result<Vec<u8>>
    where
        F: FnMut(Cursor<'_, W>) -> Result<Causal>,
    {
        let causal = {
            let state = self.state.borrow();
            let cursor = Cursor::<'_, W>::new(
                self.id,
                &self.crdt,
                &state.engine,
                self.peer_id,
                self.counter,
                state.registry.schema(&self.hash).unwrap(),
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
            .state
            .borrow()
            .secrets
            .keypair(&Metadata::new())
            .unwrap()
            .sign(&delta);
        let metadata = Metadata::new().doc(self.id).peer(self.peer_id);
        let encrypted = self
            .state
            .borrow_mut()
            .secrets
            .key_nonce(&metadata)
            .unwrap()
            .encrypt(&signed);
        self.counter = counter;
        Ok(encrypted.archive())
    }

    /// Joins a signed and encrypted delta sent by peer with `peer_id` in to
    /// the current state.
    pub fn join(&mut self, peer_id: &PeerId, payload: &mut [u8]) -> Result<()> {
        let signed = self
            .state
            .borrow()
            .secrets
            .key(&Metadata::new().doc(self.id).peer(*peer_id))
            .unwrap()
            .decrypt::<Signed>(payload)?;
        let (peer_id, delta) = signed.verify::<Delta>()?;
        let mut causal: Causal = delta.causal.deserialize(&mut rkyv::Infallible)?;
        let hash = Hash::from(delta.hash);
        {
            let state = self.state.borrow();
            if !state
                .registry
                .schema(&hash)
                .ok_or_else(|| anyhow!("missing lenses with hash {}", hash))?
                .validate(&causal.store)
            {
                return Err(anyhow!("crdt failed schema validation"));
            }
            let from_lenses = state.registry.lenses(&hash).expect("schema fetched");
            let to_lenses = state.registry.lenses(&self.hash).expect("current schema");
            tlfs_cambria::transform(from_lenses, &mut causal.store, to_lenses);
            causal.store = state.engine.filter(
                LabelRef::Root(self.id),
                peer_id,
                Permission::Write,
                &causal.store,
            );
        }
        self.crdt.join(&causal);
        let mut state = self.state.borrow_mut();
        self.crdt
            .store
            .policy(LabelRef::Root(self.id), &mut |dot, policy, label| {
                let id = if dot.id.as_ref() == self.id.as_ref() {
                    Id::Doc(DocId::new(dot.id.into()))
                } else {
                    Id::Peer(dot.id)
                };
                state
                    .engine
                    .apply_policy(Dot::new(id, dot.counter()), policy.clone(), label);
            });
        Ok(())
    }

    /// Returns the current causal context.
    pub fn ctx(&self) -> Vec<u8> {
        archive(&self.crdt.ctx)
    }

    /// Takes a causal context and returns the delta to bring the peer up to speed. This
    /// is neither signed nor encrypted as it is assumed to be transmitted over a
    /// secure channel.
    pub fn unjoin(&self, peer_id: PeerId, ctx: &[u8]) -> Result<Vec<u8>> {
        let ctx = rkyv::check_archived_root::<CausalContext>(ctx)
            .map_err(|err| anyhow!("{}", err))?
            .deserialize(&mut rkyv::Infallible)?;
        let mut causal = self.crdt.unjoin(&ctx);
        let state = self.state.borrow();
        causal.store = state.engine.filter(
            LabelRef::Root(self.id),
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
    pub fn transform(&mut self, hash: Hash) -> Result<()> {
        let state = self.state.borrow();
        let from_lenses = state
            .registry
            .lenses(&self.hash)
            .expect("current lenses are registered");
        let to_lenses = state
            .registry
            .lenses(&hash)
            .ok_or_else(|| anyhow!("missing lenses with hash {}", hash))?;
        tlfs_cambria::transform(from_lenses, &mut self.crdt.store, to_lenses);
        self.hash = hash;
        Ok(())
    }
}

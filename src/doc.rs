use crate::secrets::Metadata;
use crate::State;
use anyhow::{anyhow, Result};
use rkyv::Deserialize;
use std::cell::RefCell;
use std::rc::Rc;
use tlfs_acl::{
    Actor, Can, Causal, CausalContext, Crdt, Cursor, DocId, Dot, Id, Key, Keypair, Label, PeerId,
    Permission, Policy, Signed, W,
};
use tlfs_cambria::Hash;

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
            counter: 1,
            crdt,
            state,
        }
    }

    pub fn id(&self) -> DocId {
        self.id
    }

    pub fn cursor<R, F: FnMut(Cursor<'_, ()>) -> R>(&self, mut f: F) -> R {
        let state = self.state.borrow();
        f(Cursor::<'_, ()>::new(self.id, &self.crdt, &state.engine))
    }

    pub fn hash(&self) -> Hash {
        self.hash
    }

    pub fn add_key(&mut self, peer_id: PeerId, key: Key) {
        self.state
            .borrow_mut()
            .secrets
            .add_key(Metadata::new().doc(self.id).peer(peer_id), key);
    }

    pub fn transaction<F>(&mut self, mut f: F) -> Result<Vec<u8>>
    where
        F: FnMut(Cursor<'_, W>) -> Result<Causal>,
    {
        let state = self.state.borrow();
        let cursor = Cursor::<'_, W>::new(
            self.id,
            &self.crdt,
            &state.engine,
            self.peer_id,
            self.counter,
            state.registry.schema(&self.hash).unwrap(),
        );
        let delta = f(cursor)?;
        let counter = delta.ctx.max(&self.peer_id);
        if counter <= self.counter {
            return Err(anyhow!("invalid transaction"));
        }
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

    // TODO: encode hash in payload
    pub fn join(&mut self, peer_id: &PeerId, hash: &Hash, payload: &mut [u8]) -> Result<()> {
        let signed = self
            .state
            .borrow()
            .secrets
            .key(&Metadata::new().doc(self.id).peer(*peer_id))
            .unwrap()
            .decrypt::<Signed>(payload)?;
        // TODO: check peer_id permission
        let (_peer_id, delta) = signed.verify::<Causal>()?;
        let mut delta: Causal = delta.deserialize(&mut rkyv::Infallible)?;
        let state = self.state.borrow();
        if !state
            .registry
            .schema(hash)
            .ok_or_else(|| anyhow!("missing lenses with hash {}", hash))?
            .validate(&delta.store)
        {
            return Err(anyhow!("crdt failed schema validation"));
        }
        let from_lenses = state.registry.lenses(hash).expect("schema fetched");
        let to_lenses = state.registry.lenses(&self.hash).expect("current schema");
        tlfs_cambria::transform(from_lenses, &mut delta.store, to_lenses);
        self.crdt.join(&delta);
        Ok(())
    }

    pub fn clock(&mut self) -> Result<Vec<u8>> {
        let metadata = Metadata::new().doc(self.id).peer(self.peer_id);
        let encrypted = self
            .state
            .borrow_mut()
            .secrets
            .key_nonce(&metadata)
            .unwrap()
            .encrypt(&self.crdt.ctx);
        Ok(encrypted.archive())
    }

    pub fn unjoin(&mut self, peer_id: &PeerId, clock: &mut [u8]) -> Result<Vec<u8>> {
        let metadata = Metadata::new().doc(self.id).peer(*peer_id);
        let clock = self
            .state
            .borrow()
            .secrets
            .key(&metadata)
            .unwrap()
            .decrypt::<CausalContext>(clock)?;
        let clock = clock.deserialize(&mut rkyv::Infallible)?;
        let delta = self.crdt.unjoin(&clock);
        let metadata = Metadata::new().doc(self.id).peer(self.peer_id);
        let encrypted = self
            .state
            .borrow_mut()
            .secrets
            .key_nonce(&metadata)
            .unwrap()
            .encrypt(&delta);
        Ok(encrypted.archive())
    }

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

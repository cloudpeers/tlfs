mod crypto;
mod doc;
mod secrets;

use crate::doc::Doc;
use crate::secrets::{Metadata, Secrets};
use anyhow::Result;
use std::cell::RefCell;
use std::rc::Rc;
use tlfs_crdt::{Engine, Hash, PeerId, Registry};

pub use tlfs_crdt::{Actor, Crdt, Kind, Lens, Lenses, Permission, Primitive, PrimitiveKind};

struct State {
    secrets: Secrets,
    registry: Registry,
    engine: Engine,
    crdt: Crdt,
}

pub struct Sdk {
    state: Rc<RefCell<State>>,
}

impl Default for Sdk {
    fn default() -> Self {
        Self::new().unwrap()
    }
}

impl Sdk {
    /// Creates a new in memory sdk. A new keypair will be generated.
    pub fn new() -> Result<Self> {
        let mut secrets = Secrets::default();
        secrets.generate_keypair(Metadata::new());
        let registry = Registry::default();
        let crdt = Crdt::memory("memory")?;
        let engine = Engine::new(crdt.clone())?;
        let state = State {
            secrets,
            registry,
            crdt,
            engine,
        };
        Ok(Self {
            state: Rc::new(RefCell::new(state)),
        })
    }

    /// Returns the `PeerId` of this instance.
    pub fn peer_id(&self) -> PeerId {
        self.state
            .borrow()
            .secrets
            .keypair(&Metadata::new())
            .unwrap()
            .peer_id()
    }

    /// Adds a bootstrapped peer. This is a peer that was authenticated via
    /// physical proximity like NFC or a preshared secret.
    pub fn boostrap(&mut self, metadata: Metadata, peer_id: PeerId) {
        self.state.borrow_mut().secrets.add_peer(metadata, peer_id)
    }

    /// Registers a new schema and returns the hash.
    pub fn register_lenses(&mut self, lenses: Vec<u8>) -> Result<Hash> {
        self.state.borrow_mut().registry.register(lenses)
    }

    pub fn crdt(&self) -> Crdt {
        self.state.borrow().crdt.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api() -> Result<()> {
        let mut sdk = Sdk::new();
        let peer_id = sdk.peer_id();

        let lenses = Lenses::new(vec![
            Lens::Make(Kind::Struct),
            Lens::AddProperty("todos".into()),
            Lens::Make(Kind::Table(PrimitiveKind::U64)).lens_in("todos"),
            Lens::Make(Kind::Struct).lens_map_value().lens_in("todos"),
            Lens::AddProperty("title".into())
                .lens_map_value()
                .lens_in("todos"),
            Lens::Make(Kind::Reg(PrimitiveKind::Str))
                .lens_in("title")
                .lens_map_value()
                .lens_in("todos"),
            Lens::AddProperty("complete".into())
                .lens_map_value()
                .lens_in("todos"),
            Lens::Make(Kind::Flag)
                .lens_in("complete")
                .lens_map_value()
                .lens_in("todos"),
        ]);
        let hash = sdk.register_lenses(lenses.archive())?;

        let id = sdk.create_doc()?;
        sdk.doc_mut(id).unwrap().transform(hash)?;
        let doc = sdk.doc(id).unwrap();
        assert!(doc.cursor(|c| c.can(peer_id, Permission::Write)));

        let title = "something that needs to be done";

        let mut delta = sdk.doc_mut(id).unwrap().transaction(|cursor| {
            Ok(cursor
                .field_mut("todos", |cursor| {
                    cursor
                        .key_mut(0u64, |cursor| {
                            cursor
                                .field_mut("title", |cursor| cursor.assign(title).unwrap())
                                .unwrap()
                        })
                        .unwrap()
                })
                .unwrap())
        })?;
        sdk.doc_mut(id).unwrap().join(&peer_id, &mut delta)?;
        let value = sdk.doc(id).unwrap().cursor(|c| {
            c.field("todos")
                .unwrap()
                .key(&0u64.into())
                .unwrap()
                .field("title")
                .unwrap()
                .strs()
                .unwrap()
                .next()
                .unwrap()
                .to_string()
        });
        assert_eq!(value, title);
        Ok(())
    }
}

mod doc;
mod secrets;

use crate::doc::Doc;
use crate::secrets::{Metadata, Secrets};
use anyhow::Result;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use tlfs_acl::{DocId, Engine, PeerId};
use tlfs_cambria::{Hash, Registry};

pub use tlfs_acl::{Actor, Permission, Primitive, PrimitiveKind};
pub use tlfs_cambria::{Kind, Lens, Lenses};

#[derive(Default)]
struct State {
    secrets: Secrets,
    registry: Registry,
    engine: Engine,
}

pub struct Sdk {
    state: Rc<RefCell<State>>,
    docs: BTreeMap<DocId, Doc>,
}

impl Sdk {
    pub fn new() -> Self {
        let mut state = State::default();
        state.secrets.generate_keypair(Metadata::new());
        Self {
            state: Rc::new(RefCell::new(state)),
            docs: Default::default(),
        }
    }

    pub fn peer_id(&self) -> PeerId {
        self.state
            .borrow()
            .secrets
            .keypair(&Metadata::new())
            .unwrap()
            .peer_id()
    }

    pub fn boostrap(&mut self, metadata: Metadata, peer_id: PeerId) {
        self.state.borrow_mut().secrets.add_peer(metadata, peer_id)
    }

    pub fn register_lenses(&mut self, lenses: Vec<u8>) -> Result<Hash> {
        self.state.borrow_mut().registry.register(lenses)
    }

    pub fn create_doc(&mut self) -> Result<DocId> {
        let doc = Doc::new(self.state.clone());
        let id = doc.id();
        self.docs.insert(id, doc);
        Ok(id)
    }

    pub fn doc(&self, id: DocId) -> Option<&Doc> {
        self.docs.get(&id)
    }

    pub fn doc_mut(&mut self, id: DocId) -> Option<&mut Doc> {
        self.docs.get_mut(&id)
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
        assert!(doc.cursor(|c| c.can(Actor::Peer(peer_id), Permission::Write)));

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
        sdk.doc_mut(id).unwrap().join(&peer_id, &hash, &mut delta)?;
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

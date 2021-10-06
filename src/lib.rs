mod crypto;
mod doc;
mod secrets;

use crate::doc::Doc;
use crate::secrets::Secrets;
use anyhow::Result;
use tlfs_crdt::{Engine, Registry};

pub use crate::secrets::Metadata;
pub use tlfs_crdt::{Actor, Crdt, Kind, Lens, Lenses, Permission, Primitive, PrimitiveKind};

pub struct Sdk {
    secrets: Secrets,
    registry: Registry,
    engine: Engine,
    crdt: Crdt,
}

impl Sdk {
    /// Creates a new in memory sdk. A new keypair will be generated.
    pub fn new(config: sled::Config) -> Result<Self> {
        let db = config.open()?;
        let secrets = Secrets::new(db.open_tree("secrets")?);
        let registry = Registry::new(db.open_tree("lenses")?);
        let crdt = Crdt::new(db.open_tree("crdt")?);
        let engine = Engine::new(crdt.clone())?;
        Ok(Self {
            secrets,
            registry,
            crdt,
            engine,
        })
    }

    pub fn secrets(&self) -> &Secrets {
        &self.secrets
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    // TODO: docs aren't persisted yet
    pub fn doc(&self) -> Result<Doc> {
        Doc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api() -> Result<()> {
        let sdk = Sdk::new(sled::Config::new().temporary(true))?;
        sdk.secrets().generate_keypair(Metadata::new())?;
        sdk.registry().register(tlfs_crdt::EMPTY_LENSES.to_vec())?;

        let peer_id = sdk.secrets().keypair(Metadata::new())?.unwrap().peer_id();

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
        let hash = sdk.registry().register(lenses.archive())?;

        let mut doc = sdk.doc()?;
        doc.transform(hash)?;
        assert!(doc.cursor().can(peer_id, Permission::Write));

        let title = "something that needs to be done";

        let delta = doc
            .cursor()
            .field("todos")?
            .key(&0u64.into())?
            .field("title")?
            .assign(title)?;
        doc.apply(delta)?;

        let value = doc
            .cursor()
            .field("todos")?
            .key(&0u64.into())?
            .field("title")?
            .strs()?
            .next()
            .unwrap()?;
        assert_eq!(value, title);
        Ok(())
    }
}

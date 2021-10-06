mod acl;
mod cursor;
mod dotset;
mod id;
mod lens;
mod path;
#[cfg(any(test, proptest))]
pub mod props;
mod registry;
mod schema;
mod util;

pub use crate::acl::{Actor, Can, Engine, Permission, Policy};
pub use crate::cursor::{Cursor, W};
pub use crate::id::{DocId, PeerId};
pub use crate::lens::{ArchivedKind, ArchivedLens, ArchivedLenses, Kind, Lens, LensRef, Lenses};
pub use crate::path::{Causal, Crdt, DotStore, DotStoreType, Path, PathBuf, Primitive};
pub use crate::registry::{empty_hash, Hash, Registry, EMPTY_LENSES};
pub use crate::schema::{ArchivedSchema, PrimitiveKind, Prop, Schema};
pub use crate::util::{archive, Ref};

pub type Dot = crate::dotset::Dot<PeerId>;
pub type CausalContext = crate::dotset::DotSet<PeerId>;

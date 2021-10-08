mod acl;
mod crdt;
mod crypto;
mod cursor;
mod doc;
mod dotset;
mod id;
mod lens;
#[cfg(any(test, proptest))]
pub mod props;
mod registry;
mod schema;
mod util;

pub use crate::acl::{Acl, Actor, Can, Engine, Permission, Policy};
pub use crate::crdt::{
    Causal, CausalContext, Crdt, DotStore, DotStoreType, Path, PathBuf, Primitive,
};
pub use crate::crypto::{
    ArchivedEncrypted, ArchivedSigned, Encrypted, Key, KeyNonce, Keypair, Signed,
};
pub use crate::cursor::Cursor;
pub use crate::doc::{Backend, Doc, Docs};
pub use crate::id::{DocId, PeerId};
pub use crate::lens::{ArchivedKind, ArchivedLens, ArchivedLenses, Kind, Lens, LensRef, Lenses};
pub use crate::registry::{empty_hash, Hash, Registry, EMPTY_LENSES};
pub use crate::schema::{ArchivedSchema, PrimitiveKind, Prop, Schema};
pub use crate::util::Ref;

pub type Dot = crate::dotset::Dot<PeerId>;
pub type DotSet = crate::dotset::DotSet<PeerId>;
use crate::dotset::AbstractDotSet;

mod dotset;
mod id;
mod path;
#[cfg(any(test, proptest))]
pub mod props;

pub use crate::id::{DocId, PeerId};
pub use crate::path::{Causal, Crdt, Path, PathBuf, Primitive, PrimitiveRef};

pub type Dot = crate::dotset::Dot<PeerId>;
pub type CausalContext = crate::dotset::DotSet<PeerId>;

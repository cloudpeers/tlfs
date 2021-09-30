mod crypto;
mod cursor;
mod data;
mod engine;
mod id;
//#[cfg(any(test, feature = "proptest"))]
//mod props;
mod schema;

pub use crate::crypto::{
    ArchivedEncrypted, ArchivedSigned, Encrypted, Key, KeyNonce, Keypair, Signed,
};
pub use crate::cursor::{Cursor, W};
pub use crate::data::{Crdt, Data, Label, LabelRef, Primitive, Prop};
pub use crate::engine::{Actor, Can, Engine, Permission, Policy};
pub use crate::id::{DocId, Id, PeerId};
pub use crate::schema::{ArchivedSchema, PrimitiveKind, Schema};
pub use tlfs_crdt::Dot;

pub type Causal = tlfs_crdt::Causal<PeerId, Crdt>;
pub type CausalRef<'a> = tlfs_crdt::CausalRef<'a, PeerId, Crdt>;
pub type CausalContext = tlfs_crdt::CausalContext<PeerId>;

/*#[cfg(test)]
mod tests {
    use super::*;
    use crate::props::*;
    use proptest::prelude::*;
    use rkyv::archived_root;
    use tlfs_crdt::props::to_causal;

    tlfs_crdt::lattice!(crdt, arb_crdt);

    proptest! {
        #[test]
        fn join_preserves_validity((schema, crdt1, crdt2) in schema_and_crdt2()) {
            prop_assume!(validate(&schema, &crdt1));
            prop_assume!(validate(&schema, &crdt2));
            let mut crdt1 = to_causal(crdt1);
            let crdt2 = to_causal(crdt2);
            crdt1.join(&crdt2);
            prop_assert!(validate(&schema, &crdt1.store));
        }
    }
}*/

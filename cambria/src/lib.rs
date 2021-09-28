mod crdt;
mod lens;
mod precompile;
#[cfg(any(test, feature = "proptest"))]
pub mod props;
mod schema;

pub use crdt::{ArchivedCrdt, ArchivedPrimitive, Crdt, Primitive, Prop, ReplicaId};
pub use lens::{ArchivedLens, ArchivedLenses, Lens, LensRef, Lenses};
pub use precompile::{precompile, write_tokens};
pub use schema::{ArchivedSchema, PrimitiveKind, Schema};
pub use {aligned, anyhow, rkyv};

use anyhow::Result;
use rkyv::archived_root;

pub trait Cambria<I: ReplicaId> {
    fn lenses() -> &'static [u8];

    fn schema() -> &'static ArchivedSchema;

    fn transform(lenses: &[u8], crdt: &mut Crdt<I>) -> Result<()> {
        let a = unsafe { archived_root::<Lenses>(lenses) };
        let b = unsafe { archived_root::<Lenses>(Self::lenses()) };
        for lens in a.transform(b) {
            lens.transform_crdt(crdt);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::props::*;
    use proptest::prelude::*;
    use tlfs_crdt::props::to_causal;

    tlfs_crdt::lattice!(crdt, arb_crdt);

    proptest! {
        #[test]
        fn reversible((lens, schema) in lens_and_schema()) {
            let lens = archive(&lens);
            let lens = unsafe { archived_root::<Lens>(&lens) };
            let mut schema2 = schema.clone();
            prop_assume!(lens.to_ref().transform_schema(&mut schema2).is_ok());
            lens.to_ref().reverse().transform_schema(&mut schema2).unwrap();
            prop_assert_eq!(schema, schema2);
        }

        #[test]
        fn transform_preserves_validity((lens, mut schema, mut crdt) in lens_schema_and_crdt()) {
            let lens = archive(&lens);
            let lens = unsafe { archived_root::<Lens>(&lens) };
            prop_assume!(validate(&schema, &crdt));
            prop_assume!(lens.to_ref().transform_schema(&mut schema).is_ok());
            lens.to_ref().transform_crdt(&mut crdt);
            prop_assert!(validate(&schema, &crdt));
        }

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
}

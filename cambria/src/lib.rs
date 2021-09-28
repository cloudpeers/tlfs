mod crdt;
mod lens;
#[cfg(any(test, feature = "proptest"))]
pub mod props;
mod schema;

pub use crdt::{ReplicaId, ArchivedCrdt, ArchivedPrimitive, Crdt, Primitive, Prop};
pub use lens::{ArchivedLens, ArchivedLenses, Kind, Lens, LensRef, Lenses};
pub use schema::{ArchivedSchema, PrimitiveKind, Schema};

pub fn transform<I: ReplicaId>(
    from_lenses: &ArchivedLenses,
    crdt: &mut Crdt<I>,
    to_lenses: &ArchivedLenses,
) {
    for lens in from_lenses.transform(to_lenses) {
        lens.transform_crdt(crdt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::props::*;
    use proptest::prelude::*;
    use rkyv::archived_root;
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

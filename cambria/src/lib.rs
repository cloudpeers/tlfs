mod lens;
mod registry;
//#[cfg(any(test, feature = "proptest"))]
//pub mod props;

pub use lens::{ArchivedLens, ArchivedLenses, Kind, Lens, LensRef, Lenses};
pub use registry::{empty_hash, Hash, Registry};

use tlfs_acl::Crdt;

pub fn transform(from_lenses: &ArchivedLenses, crdt: &mut Crdt, to_lenses: &ArchivedLenses) {
    for lens in from_lenses.transform(to_lenses) {
        lens.transform_crdt(crdt);
    }
}

/*#[cfg(test)]
mod tests {
    use super::*;
    use crate::props::*;
    use proptest::prelude::*;
    use rkyv::archived_root;

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
    }
}*/

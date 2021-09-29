mod crdt;
mod lens;
#[cfg(any(test, feature = "proptest"))]
pub mod props;
mod schema;

pub use crdt::{ArchivedCrdt, ArchivedPrimitive, Crdt, Primitive, Prop, ReplicaId};
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

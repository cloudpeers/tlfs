use crate::path::DotStore;
use crate::{Causal, CausalContext, DocId, Dot, PeerId, Primitive, PrimitiveKind, Schema};
use proptest::prelude::*;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{archived_root, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

pub fn arb_peer_id() -> impl Strategy<Value = PeerId> {
    (0u8..5).prop_map(|i| PeerId::new([i; 32]))
}

pub fn arb_doc_id() -> impl Strategy<Value = DocId> {
    (0u8..5).prop_map(|i| DocId::new([i; 32]))
}

pub fn arb_dot_in(counter: Range<u64>) -> impl Strategy<Value = Dot> {
    (arb_peer_id(), counter).prop_map(|(a, c)| Dot::new(a, c))
}

pub fn arb_dot() -> impl Strategy<Value = Dot> {
    arb_dot_in(1u64..25)
}

pub fn arb_ctx() -> impl Strategy<Value = CausalContext> {
    prop::collection::btree_set(arb_dot_in(1u64..5), 0..50)
        .prop_map(|dots| dots.into_iter().collect())
}

pub fn arb_primitive() -> impl Strategy<Value = Primitive> {
    prop_oneof![
        any::<bool>().prop_map(Primitive::Bool),
        any::<u64>().prop_map(Primitive::U64),
        any::<i64>().prop_map(Primitive::I64),
        ".*".prop_map(Primitive::Str),
    ]
}

pub fn arb_primitive_kind() -> impl Strategy<Value = PrimitiveKind> {
    prop_oneof![
        Just(PrimitiveKind::Bool),
        Just(PrimitiveKind::U64),
        Just(PrimitiveKind::I64),
        Just(PrimitiveKind::Str),
    ]
}

pub fn arb_primitive_for_kind(kind: PrimitiveKind) -> BoxedStrategy<Primitive> {
    match kind {
        PrimitiveKind::Bool => any::<bool>().prop_map(Primitive::Bool).boxed(),
        PrimitiveKind::U64 => any::<u64>().prop_map(Primitive::U64).boxed(),
        PrimitiveKind::I64 => any::<i64>().prop_map(Primitive::I64).boxed(),
        PrimitiveKind::Str => ".*".prop_map(Primitive::Str).boxed(),
    }
}

fn arb_dotset() -> impl Strategy<Value = BTreeSet<Dot>> {
    prop::collection::btree_set(arb_dot(), 0..10)
}

fn arb_dotfun(kind: PrimitiveKind) -> impl Strategy<Value = BTreeMap<Dot, Primitive>> {
    prop::collection::btree_map(arb_dot(), arb_primitive_for_kind(kind), 0..10)
}

fn arb_dotmap(
    kind: PrimitiveKind,
    inner: impl Strategy<Value = DotStore>,
) -> impl Strategy<Value = BTreeMap<Primitive, DotStore>> {
    prop::collection::btree_map(arb_primitive_for_kind(kind), inner, 0..10)
}

fn arb_struct(
    inner: impl Strategy<Value = DotStore>,
) -> impl Strategy<Value = BTreeMap<String, DotStore>> {
    prop::collection::btree_map(".*", inner, 0..10)
}

pub fn arb_dotstore() -> impl Strategy<Value = DotStore> {
    let leaf = prop_oneof![
        arb_dotset().prop_map(DotStore::DotSet),
        arb_primitive_kind().prop_flat_map(|kind| arb_dotfun(kind).prop_map(DotStore::DotFun)),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        let inner2 = inner.clone();
        prop_oneof![
            arb_primitive_kind().prop_flat_map(
                move |kind| arb_dotmap(kind, inner2.clone()).prop_map(DotStore::DotMap)
            ),
            arb_struct(inner).prop_map(DotStore::Struct),
        ]
    })
}

pub fn arb_causal(store: impl Strategy<Value = DotStore>) -> impl Strategy<Value = Causal> {
    store.prop_map(|store| {
        let mut dots = CausalContext::default();
        store.dots(&mut dots);
        let mut present = BTreeMap::new();
        for dot in dots.iter() {
            let counter = dot.counter();
            let id = dot.id;
            if counter > 0 && counter > present.get(&id).copied().unwrap_or_default() {
                present.insert(id, counter);
            }
        }
        let ctx = CausalContext::from_map(present);
        Causal { store, ctx }
    })
}

pub fn union(a: &CausalContext, b: &CausalContext) -> CausalContext {
    let mut a = a.clone();
    a.union(b);
    a
}

pub fn intersect(a: &CausalContext, b: &CausalContext) -> CausalContext {
    a.intersection(b)
}

pub fn difference(a: &CausalContext, b: &CausalContext) -> CausalContext {
    a.difference(b)
}

pub fn arb_schema() -> impl Strategy<Value = Schema> {
    let leaf = prop_oneof![
        Just(Schema::Flag),
        arb_primitive_kind().prop_map(Schema::Reg),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            (arb_primitive_kind(), inner.clone())
                .prop_map(|(kind, schema)| Schema::Table(kind, Box::new(schema))),
            prop::collection::btree_map(".*", inner, 0..10).prop_map(Schema::Struct),
        ]
    })
}

pub fn arb_dotstore_for_schema(s: Schema) -> BoxedStrategy<DotStore> {
    match s {
        Schema::Flag => arb_dotset().prop_map(DotStore::DotSet).boxed(),
        Schema::Reg(kind) => arb_dotfun(kind).prop_map(DotStore::DotFun).boxed(),
        Schema::Table(kind, schema) => arb_dotmap(kind, arb_dotstore_for_schema(*schema))
            .prop_map(DotStore::DotMap)
            .boxed(),
        Schema::Struct(fields) => fields
            .into_iter()
            .map(|(k, s)| arb_dotstore_for_schema(s).prop_map(move |v| (k.clone(), v)))
            .collect::<Vec<_>>()
            .prop_map(|v| DotStore::Struct(v.into_iter().collect()))
            .boxed(),
    }
}

pub fn archive<T>(t: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(t).unwrap();
    ser.into_serializer().into_inner().to_vec()
}

pub fn validate(schema: &Schema, value: &Causal) -> bool {
    let schema = archive(schema);
    let schema = unsafe { archived_root::<Schema>(&schema) };
    schema.validate(&value.store)
}

prop_compose! {
    pub fn schema_and_causal()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt in arb_causal(arb_dotstore_for_schema(schema))) -> (Schema, Causal)
    {
        (schema, crdt)
    }
}

prop_compose! {
    pub fn schema_and_causal2()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt1 in arb_causal(arb_dotstore_for_schema(schema.clone())), crdt2 in arb_causal(arb_dotstore_for_schema(schema))) -> (Schema, Causal, Causal)
    {
        (schema, crdt1, crdt2)
    }
}

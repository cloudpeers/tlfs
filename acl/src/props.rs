use crate::data::{Crdt, Data, Primitive, Prop};
use crate::schema::{PrimitiveKind, Schema};
use proptest::prelude::*;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{archived_root, Serialize};
use tlfs_crdt::props::*;

pub fn arb_prop() -> impl Strategy<Value = Prop> {
    "[a-z]+"
}

pub fn arb_primitive() -> impl Strategy<Value = Primitive> {
    prop_oneof![
        any::<bool>().prop_map(Primitive::Bool),
        any::<u64>().prop_map(Primitive::U64),
        any::<i64>().prop_map(Primitive::I64),
        ".*".prop_map(Primitive::Str),
    ]
}

pub fn arb_data() -> impl Strategy<Value = Data> {
    let leaf = prop_oneof![
        Just(Data::Null),
        arb_ewflag().prop_map(Data::Flag),
        arb_mvreg(arb_primitive()).prop_map(Data::Reg),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            arb_ormap(arb_primitive(), inner.clone()).prop_map(Data::Table),
            prop::collection::btree_map(arb_prop(), inner, 0..10).prop_map(Data::Struct),
        ]
    })
}

pub fn arb_crdt() -> impl Strategy<Value = Crdt> {
    arb_data().prop_map(Crdt::new)
}

pub fn arb_primitive_kind() -> impl Strategy<Value = PrimitiveKind> {
    prop_oneof![
        Just(PrimitiveKind::Bool),
        Just(PrimitiveKind::U64),
        Just(PrimitiveKind::I64),
        Just(PrimitiveKind::Str),
    ]
}

pub fn arb_schema() -> impl Strategy<Value = Schema> {
    let leaf = prop_oneof![
        Just(Schema::Null),
        Just(Schema::Flag),
        arb_primitive_kind().prop_map(Schema::Reg),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            (arb_primitive_kind(), inner.clone())
                .prop_map(|(kind, schema)| Schema::Table(kind, Box::new(schema))),
            prop::collection::btree_map(arb_prop(), inner, 0..10).prop_map(Schema::Struct),
        ]
    })
}

pub fn arb_primitive_for_kind(kind: PrimitiveKind) -> BoxedStrategy<Primitive> {
    match kind {
        PrimitiveKind::Bool => any::<bool>().prop_map(Primitive::Bool).boxed(),
        PrimitiveKind::U64 => any::<u64>().prop_map(Primitive::U64).boxed(),
        PrimitiveKind::I64 => any::<i64>().prop_map(Primitive::I64).boxed(),
        PrimitiveKind::Str => ".*".prop_map(Primitive::Str).boxed(),
    }
}

pub fn arb_data_for_schema(s: Schema) -> BoxedStrategy<Data> {
    match s {
        Schema::Null => Just(Data::Null).boxed(),
        Schema::Flag => arb_ewflag().prop_map(Data::Flag).boxed(),
        Schema::Reg(kind) => arb_mvreg(arb_primitive_for_kind(kind))
            .prop_map(Data::Reg)
            .boxed(),
        Schema::Table(kind, schema) => {
            arb_ormap(arb_primitive_for_kind(kind), arb_crdt_for_schema(*schema))
                .prop_map(Data::Table)
                .boxed()
        }
        Schema::Struct(fields) => fields
            .into_iter()
            .map(|(k, s)| arb_crdt_for_schema(s).prop_map(move |v| (k.clone(), v)))
            .collect::<Vec<_>>()
            .prop_map(|v| Data::Struct(v.into_iter().collect()))
            .boxed(),
    }
}

pub fn arb_crdt_for_schema(s: Schema) -> impl Strategy<Value = Crdt> {
    arb_data_for_schema(s).prop_map(Crdt::new)
}

pub fn archive<T>(t: &T) -> Vec<u8>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(t).unwrap();
    ser.into_serializer().into_inner().to_vec()
}

pub fn validate(schema: &Schema, value: &Crdt) -> bool {
    let schema = archive(schema);
    let schema = unsafe { archived_root::<Schema>(&schema) };
    schema.validate(value)
}

prop_compose! {
    pub fn schema_and_crdt()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt in arb_crdt_for_schema(schema)) -> (Schema, Crdt)
    {
        (schema, crdt)
    }
}

prop_compose! {
    pub fn schema_and_crdt2()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt1 in arb_crdt_for_schema(schema.clone()), crdt2 in arb_crdt_for_schema(schema)) -> (Schema, Crdt, Crdt)
    {
        (schema, crdt1, crdt2)
    }
}

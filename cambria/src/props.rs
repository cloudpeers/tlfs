use crate::crdt::{Crdt, Primitive, Prop};
use crate::lens::{Kind, Lens, Lenses};
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

pub fn arb_crdt() -> impl Strategy<Value = Crdt<u8>> {
    let leaf = prop_oneof![
        Just(Crdt::Null),
        arb_ewflag().prop_map(Crdt::Flag),
        arb_mvreg(arb_primitive()).prop_map(Crdt::Reg),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            arb_ormap(arb_primitive(), inner.clone()).prop_map(Crdt::Table),
            prop::collection::btree_map(arb_prop(), inner, 0..10).prop_map(Crdt::Struct),
        ]
    })
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

pub fn arb_crdt_for_schema(s: Schema) -> BoxedStrategy<Crdt<u8>> {
    match s {
        Schema::Null => Just(Crdt::Null).boxed(),
        Schema::Flag => arb_ewflag().prop_map(Crdt::Flag).boxed(),
        Schema::Reg(kind) => arb_mvreg(arb_primitive_for_kind(kind))
            .prop_map(Crdt::Reg)
            .boxed(),
        Schema::Table(kind, schema) => {
            arb_ormap(arb_primitive_for_kind(kind), arb_crdt_for_schema(*schema))
                .prop_map(Crdt::Table)
                .boxed()
        }
        Schema::Struct(fields) => fields
            .into_iter()
            .map(|(k, s)| arb_crdt_for_schema(s).prop_map(move |v| (k.clone(), v)))
            .collect::<Vec<_>>()
            .prop_map(|v| Crdt::Struct(v.into_iter().collect()))
            .boxed(),
    }
}

pub fn arb_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Null),
        Just(Kind::Flag),
        arb_primitive_kind().prop_map(Kind::Reg),
        arb_primitive_kind().prop_map(Kind::Table),
        Just(Kind::Struct),
    ]
}

pub fn arb_lens_for_schema(s: &Schema) -> BoxedStrategy<Lens> {
    let mut strategy = vec![];
    match s {
        Schema::Null => {
            strategy.push(
                prop_oneof![
                    Just(Lens::Make(Kind::Flag)),
                    arb_primitive_kind().prop_map(|kind| Lens::Make(Kind::Reg(kind))),
                    arb_primitive_kind().prop_map(|kind| Lens::Make(Kind::Table(kind))),
                    Just(Lens::Make(Kind::Struct)),
                ]
                .boxed(),
            );
        }
        Schema::Flag => strategy.push(Just(Lens::Destroy(Kind::Flag)).boxed()),
        Schema::Reg(kind) => strategy.push(Just(Lens::Destroy(Kind::Reg(*kind))).boxed()),
        Schema::Table(kind, s) => {
            if **s == Schema::Null {
                strategy.push(Just(Lens::Destroy(Kind::Table(*kind))).boxed());
            }
            strategy.push(
                arb_lens_for_schema(s)
                    .prop_map(|l| Lens::LensMapValue(Box::new(l)))
                    .boxed(),
            );
        }
        Schema::Struct(fields) => {
            if fields.is_empty() {
                strategy.push(Just(Lens::Destroy(Kind::Struct)).boxed());
            }
            strategy.push(arb_prop().prop_map(Lens::AddProperty).boxed());
            for (k, s) in fields {
                if let Schema::Null = s {
                    strategy.push(Just(Lens::RemoveProperty(k.clone())).boxed());
                }
                let kk = k.clone();
                strategy.push(
                    arb_prop()
                        .prop_map(move |k2| Lens::RenameProperty(kk.clone(), k2))
                        .boxed(),
                );
                if let Schema::Struct(s2) = s {
                    for k2 in s2.keys() {
                        strategy.push(Just(Lens::HoistProperty(k.clone(), k2.clone())).boxed());
                    }
                    let kk = k.clone();
                    strategy.push(
                        arb_prop()
                            .prop_map(move |k2| Lens::PlungeProperty(kk.clone(), k2))
                            .boxed(),
                    );
                }
                let kk = k.clone();
                strategy.push(
                    arb_lens_for_schema(s)
                        .prop_map(move |l| Lens::LensIn(kk.clone(), Box::new(l)))
                        .boxed(),
                );
            }
        }
    }
    (0..strategy.len())
        .prop_flat_map(move |i| strategy[i].clone())
        .boxed()
}

fn arb_lenses_inner(
    lenses: Vec<Lens>,
    schema: Schema,
) -> impl Strategy<Value = (Vec<Lens>, Schema)> {
    arb_lens_for_schema(&schema).prop_flat_map(move |lens| {
        let mut lenses = lenses.clone();
        let mut schema = schema.clone();
        let bytes = archive(&lens);
        let archived = unsafe { archived_root::<Lens>(&bytes) }.to_ref();
        archived.transform_schema(&mut schema).unwrap();
        lenses.push(lens);
        (Just(lenses), Just(schema))
    })
}

fn arb_n_lenses(n: i32) -> BoxedStrategy<Lenses> {
    if n < 1 {
        return Just(Lenses::new(vec![Lens::Make(Kind::Struct)])).boxed();
    }
    let mut inner = arb_lenses_inner(Vec::with_capacity(n as usize), Schema::Null).boxed();
    for _ in 1..n {
        inner = inner
            .prop_flat_map(|(lenses, schema)| arb_lenses_inner(lenses, schema))
            .boxed();
    }
    inner.prop_map(|(lenses, _)| Lenses::new(lenses)).boxed()
}

prop_compose! {
    pub fn arb_lenses()(n in 0..25)(lenses in arb_n_lenses(n)) -> Lenses {
        lenses
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

pub fn validate(schema: &Schema, value: &Crdt<u8>) -> bool {
    let schema = archive(schema);
    let schema = unsafe { archived_root::<Schema>(&schema) };
    schema.validate(value)
}

fn lenses_to_schema(lenses: &Lenses) -> Schema {
    let bytes = archive(lenses);
    let lenses = unsafe { archived_root::<Lenses>(&bytes) };
    let mut schema = Schema::Null;
    for lens in lenses.lenses() {
        lens.to_ref().transform_schema(&mut schema).unwrap();
    }
    schema
}

prop_compose! {
    pub fn lens_and_schema()
        (schema in arb_schema())
        (lens in arb_lens_for_schema(&schema), schema in Just(schema)) -> (Lens, Schema)
    {
        (lens, schema)
    }
}

prop_compose! {
    pub fn schema_and_crdt()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt in arb_crdt_for_schema(schema)) -> (Schema, Crdt<u8>)
    {
        (schema, crdt)
    }
}

prop_compose! {
    pub fn schema_and_crdt2()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt1 in arb_crdt_for_schema(schema.clone()), crdt2 in arb_crdt_for_schema(schema)) -> (Schema, Crdt<u8>, Crdt<u8>)
    {
        (schema, crdt1, crdt2)
    }
}

prop_compose! {
    pub fn lens_schema_and_crdt()
        ((lens, schema) in lens_and_schema())
        (lens in Just(lens), schema in Just(schema.clone()), crdt in arb_crdt_for_schema(schema)) -> (Lens, Schema, Crdt<u8>)
    {
        (lens, schema, crdt)
    }
}

prop_compose! {
    pub fn lenses_and_crdt()
        (lenses in arb_lenses())
        (lenses in Just(lenses.clone()), crdt in arb_crdt_for_schema(lenses_to_schema(&lenses))) -> (Lenses, Crdt<u8>)
    {
        (lenses, crdt)
    }
}

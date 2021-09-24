use crate::lens::{Kind, Lens, Lenses, PrimitiveKind, PrimitiveValue, Prop, Schema, Value};
use crate::precompile::{precompile, write_tokens};
use proptest::prelude::*;
use quote::quote;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::{archived_root, Serialize};
use std::process::Command;

/*pub fn arb_lens() -> impl Strategy<Value = Lens> {
    let leaf = prop_oneof![
        arb_kind().prop_map(Lens::Make),
        arb_kind().prop_map(Lens::Destroy),
        ".*".prop_map(Lens::AddProperty),
        ".*".prop_map(Lens::RemoveProperty),
        (".*", ".*").prop_map(|(a, b)| Lens::RenameProperty(a, b)),
        (".*", ".*").prop_map(|(a, b)| Lens::HoistProperty(a, b)),
        (".*", ".*").prop_map(|(a, b)| Lens::PlungeProperty(a, b)),
        Just(Lens::Wrap),
        Just(Lens::Head),
        // TODO convert
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            (".*", inner.clone()).prop_map(|(p, l)| Lens::LensIn(p, Box::new(l))),
            inner.prop_map(|l| Lens::LensMap(Box::new(l))),
        ]
    })
}


pub fn arb_prim_kind() -> impl Strategy<Value = PrimitiveKind> {
    prop_oneof![
        Just(PrimitiveKind::Boolean),
        Just(PrimitiveKind::Number),
        Just(PrimitiveKind::Text),
    ]
}

pub fn arb_kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        Just(Kind::Null),
        arb_prim_kind().prop_map(Kind::Primitive),
        Just(Kind::Array),
        Just(Kind::Object),
    ]
}

pub fn arb_prim_value() -> impl Strategy<Value = PrimitiveValue> {
    prop_oneof![
        any::<bool>().prop_map(PrimitiveValue::Boolean),
        any::<i64>().prop_map(PrimitiveValue::Number),
        ".*".prop_map(PrimitiveValue::Text),
    ]
}

pub fn arb_value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        arb_prim_value().prop_map(Value::Primitive),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
            prop::collection::btree_map("[^/ ][^/ ]*", inner, 0..10).prop_map(Value::Object),
        ]
    })
}*/

pub fn arb_prop() -> impl Strategy<Value = Prop> {
    "[a-z]+"
}

pub fn arb_schema() -> impl Strategy<Value = Schema> {
    let leaf = prop_oneof![
        Just(Schema::Null),
        Just(Schema::Boolean),
        Just(Schema::Number),
        Just(Schema::Text),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            inner
                .clone()
                .prop_map(|s| Schema::Array(false, Box::new(s))),
            inner.clone().prop_map(|s| Schema::Array(true, Box::new(s))),
            prop::collection::btree_map(arb_prop(), inner, 0..10).prop_map(Schema::Object),
        ]
    })
}

pub fn arb_value_for_schema(s: Schema) -> BoxedStrategy<Value> {
    match s {
        Schema::Null => Just(Value::Null).boxed(),
        Schema::Boolean => any::<bool>()
            .prop_map(|b| Value::Primitive(PrimitiveValue::Boolean(b)))
            .boxed(),
        Schema::Number => any::<i64>()
            .prop_map(|n| Value::Primitive(PrimitiveValue::Number(n)))
            .boxed(),
        Schema::Text => ".*"
            .prop_map(|s| Value::Primitive(PrimitiveValue::Text(s)))
            .boxed(),
        Schema::Array(e, s) => {
            let range = if e { 0..10 } else { 1..10 };
            prop::collection::vec(arb_value_for_schema(*s), range)
                .prop_map(Value::Array)
                .boxed()
        }
        Schema::Object(s) => s
            .into_iter()
            .map(|(k, s)| arb_value_for_schema(s).prop_map(move |v| (k.clone(), v)))
            .collect::<Vec<_>>()
            .prop_map(|v| Value::Object(v.into_iter().collect()))
            .boxed(),
    }
}

pub fn arb_lens_for_schema(s: &Schema) -> impl Strategy<Value = Lens> {
    let mut strategy = vec![];
    let kind = match s {
        Schema::Null => Kind::Null,
        Schema::Boolean => Kind::Primitive(PrimitiveKind::Boolean),
        Schema::Number => Kind::Primitive(PrimitiveKind::Number),
        Schema::Text => Kind::Primitive(PrimitiveKind::Text),
        Schema::Array(_, _) => Kind::Array,
        Schema::Object(_) => Kind::Object,
    };
    match s {
        Schema::Null => {
            strategy.push(
                prop_oneof![
                    Just(Lens::Make(Kind::Primitive(PrimitiveKind::Boolean))),
                    Just(Lens::Make(Kind::Primitive(PrimitiveKind::Number))),
                    Just(Lens::Make(Kind::Primitive(PrimitiveKind::Text))),
                    Just(Lens::Make(Kind::Array)),
                    Just(Lens::Make(Kind::Object)),
                ]
                .boxed(),
            );
        }
        Schema::Boolean => strategy.push(Just(Lens::Destroy(kind)).boxed()),
        Schema::Number => strategy.push(Just(Lens::Destroy(kind)).boxed()),
        Schema::Text => strategy.push(Just(Lens::Destroy(kind)).boxed()),
        Schema::Array(true, s) if **s == Schema::Null => {
            strategy.push(Just(Lens::Destroy(kind)).boxed());
        }
        Schema::Object(m) if m.is_empty() => {
            strategy.push(Just(Lens::Destroy(kind)).boxed());
        }
        _ => {}
    }
    if let Schema::Object(s) = s {
        strategy.push(arb_prop().prop_map(Lens::AddProperty).boxed());
        for (k, s) in s {
            if let Schema::Null = s {
                strategy.push(Just(Lens::RemoveProperty(k.clone())).boxed());
            }
            let kk = k.clone();
            strategy.push(
                arb_prop()
                    .prop_map(move |k2| Lens::RenameProperty(kk.clone(), k2))
                    .boxed(),
            );
            if let Schema::Object(s2) = s {
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
    strategy.push(Just(Lens::Wrap).boxed());
    if let Schema::Array(e, s) = s {
        if !e {
            strategy.push(Just(Lens::Head).boxed());
        }
        strategy.push(
            arb_lens_for_schema(s)
                .prop_map(|l| Lens::LensMap(Box::new(l)))
                .boxed(),
        );
    }
    // TODO convert
    (0..strategy.len()).prop_flat_map(move |i| strategy[i].clone())
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
        return Just(Lenses::new(vec![Lens::Make(Kind::Object)])).boxed();
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

pub fn validate(schema: &Schema, value: &Value) -> bool {
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
    fn lens_and_schema()
        (schema in arb_schema())
        (lens in arb_lens_for_schema(&schema), schema in Just(schema)) -> (Lens, Schema)
    {
        (lens, schema)
    }
}

prop_compose! {
    fn schema_and_value()
        (schema in arb_schema())
        (schema in Just(schema.clone()), value in arb_value_for_schema(schema)) -> (Schema, Value)
    {
        (schema, value)
    }
}

prop_compose! {
    fn lens_schema_and_value()
        ((lens, schema) in lens_and_schema())
        (lens in Just(lens), schema in Just(schema.clone()), value in arb_value_for_schema(schema)) -> (Lens, Schema, Value)
    {
        (lens, schema, value)
    }
}

prop_compose! {
    fn lenses_and_value()
        (lenses in arb_lenses())
        (lenses in Just(lenses.clone()), value in arb_value_for_schema(lenses_to_schema(&lenses))) -> (Lenses, Value)
    {
        (lenses, value)
    }
}

proptest! {
    #[test]
    fn proptest_reversible((lens, schema) in lens_and_schema()) {
        let lens = archive(&lens);
        let lens = unsafe { archived_root::<Lens>(&lens) };
        let mut schema2 = schema.clone();
        prop_assume!(lens.to_ref().transform_schema(&mut schema2).is_ok());
        lens.to_ref().reverse().transform_schema(&mut schema2).unwrap();
        prop_assert_eq!(schema, schema2);
    }

    #[test]
    fn proptest_valid((lens, mut schema, mut value) in lens_schema_and_value()) {
        let lens = archive(&lens);
        let lens = unsafe { archived_root::<Lens>(&lens) };
        prop_assume!(validate(&schema, &value));
        prop_assume!(lens.to_ref().transform_schema(&mut schema).is_ok());
        lens.to_ref().transform_value(&mut value);
        prop_assert!(validate(&schema, &value));
    }

    #[test]
    #[ignore]
    fn proptest_to_value((lenses, value) in lenses_and_value()) {
        if let Value::Object(_) = &value {
        } else {
            prop_assume!(false);
        }
        let tokens = precompile("MySchema", lenses);
        let value = archive(&value);
        let value_len = value.len();
        let toml = r#"
[package]
name = "cambria_to_value"
version = "0.1.0"
edition = "2018"

[dependencies]
cambria = { path = "/home/dvc/ipld/cambria" }
rkyv = "0.7"
"#;
        let tokens = quote! {
            #tokens

            fn main() {
                use cambria::aligned::{Aligned, A8};
                use cambria::rkyv::ser::serializers::AllocSerializer;
                use cambria::rkyv::ser::Serializer;
                use cambria::rkyv::Deserialize;
                use cambria::{Cambria, FromValue};
                static VALUE: Aligned<A8, [u8; #value_len]> = Aligned([#(#value),*]);
                let value = unsafe { cambria::rkyv::archived_root::<cambria::Value>(&VALUE[..]) };
                let value = value.deserialize(&mut cambria::rkyv::Infallible).unwrap();
                let doc = MySchema::from_value(&value).unwrap();
                let mut ser = AllocSerializer::<256>::default();
                ser.serialize_value(&doc).unwrap();
                let bytes = ser.into_serializer().into_inner().to_vec();
                let value2 = cambria::Ptr::new(&bytes, MySchema::schema()).to_value();
                assert_eq!(value, value2);
            }
        };
        let tmp = std::env::temp_dir();
        let path = tmp.join("cambria_to_value");
        let src = path.join("src");
        std::fs::create_dir_all(&src).unwrap();
        std::fs::write(path.join("Cargo.toml"), &toml).unwrap();
        write_tokens(src.join("main.rs"), &tokens);
        Command::new("cargo")
            .current_dir(path)
            .arg("run")
            .status()
            .unwrap();
    }
}

use crate::{
    AbstractDotSet, Causal, CausalContext, Crdt, DocId, Dot, DotSet, DotStore, Kind, Lens, Lenses,
    PeerId, Primitive, PrimitiveKind, Prop, Ref, Schema,
};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::ops::Range;

pub fn arb_prop() -> impl Strategy<Value = Prop> {
    "[a-z]"
}

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

pub fn arb_ctx() -> impl Strategy<Value = DotSet> {
    prop::collection::btree_set(arb_dot_in(1u64..5), 0..50)
        .prop_map(|dots| dots.into_iter().collect())
}

pub fn arb_causal_ctx() -> impl Strategy<Value = CausalContext> {
    arb_ctx().prop_map(|dots| CausalContext {
        doc: DocId::new([0; 32]),
        schema: [0; 32],
        dots,
    })
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
        PrimitiveKind::Str => arb_prop().prop_map(Primitive::Str).boxed(),
    }
}

fn arb_dotset() -> impl Strategy<Value = DotSet> {
    prop::collection::btree_set(arb_dot(), 0..10).prop_map(DotSet::from_set)
}

fn arb_dotfun(kind: PrimitiveKind) -> impl Strategy<Value = BTreeMap<Dot, Primitive>> {
    prop::collection::btree_map(arb_dot(), arb_primitive_for_kind(kind), 0..10)
}

fn arb_dotmap(
    kind: PrimitiveKind,
    inner: impl Strategy<Value = DotStore>,
) -> impl Strategy<Value = BTreeMap<Primitive, DotStore>> {
    prop::collection::btree_map(arb_primitive_for_kind(kind), inner, 0..10)
        .prop_map(|map| map.into_iter().filter(|(_, v)| !v.is_empty()).collect())
}

fn arb_struct(
    inner: impl Strategy<Value = DotStore>,
) -> impl Strategy<Value = BTreeMap<String, DotStore>> {
    prop::collection::btree_map(arb_prop(), inner, 0..10)
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
        let mut dots = DotSet::new();
        store.dots(&mut dots);
        let mut present = BTreeMap::new();
        for dot in dots.iter() {
            let counter = dot.counter();
            let id = dot.id;
            if counter > 0 && counter > present.get(&id).copied().unwrap_or_default() {
                present.insert(id, counter);
            }
        }
        let dots = DotSet::from_map(present);
        Causal {
            ctx: CausalContext {
                doc: DocId::new([0; 32]),
                schema: [0; 32],
                dots,
            },
            store,
        }
    })
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
            prop::collection::btree_map(arb_prop(), inner, 0..10).prop_map(Schema::Struct),
        ]
    })
}

pub fn arb_dotstore_for_schema(s: Schema) -> BoxedStrategy<DotStore> {
    match s {
        Schema::Null => Just(DotStore::Null).boxed(),
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

pub fn validate(schema: &Schema, value: &Causal) -> bool {
    let schema = Ref::archive(schema);
    schema.as_ref().validate(value.store())
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
        let alens = Ref::archive(&lens);
        alens
            .as_ref()
            .to_ref()
            .transform_schema(&mut schema)
            .unwrap();
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

fn lenses_to_schema(lenses: &Lenses) -> Schema {
    let lenses = Ref::archive(lenses);
    let mut schema = Schema::Null;
    for lens in lenses.as_ref().lenses() {
        lens.to_ref().transform_schema(&mut schema).unwrap();
    }
    schema
}

prop_compose! {
    pub fn lens_and_schema()
        (schema in arb_schema())
        (schema in Just(schema.clone()), lens in arb_lens_for_schema(&schema)) -> (Lens, Schema)
    {
        (lens, schema)
    }
}

prop_compose! {
    pub fn lens_schema_and_causal()
        (schema in arb_schema())
        (lens in arb_lens_for_schema(&schema), schema in Just(schema.clone()), crdt in arb_causal(arb_dotstore_for_schema(schema))) -> (Lens, Schema, Causal)
    {
        (lens, schema, crdt)
    }
}

prop_compose! {
    pub fn lenses_and_causal()
        (lenses in arb_lenses())
        (lenses in Just(lenses.clone()), crdt in arb_causal(arb_dotstore_for_schema(lenses_to_schema(&lenses)))) -> (Lenses, Causal)
    {
        (lenses, crdt)
    }
}

pub fn join(c: &Causal, o: &Causal) -> Causal {
    let mut c = c.clone();
    c.join(o);
    c
}

pub fn causal_to_crdt(causal: &Causal) -> Crdt {
    let crdt = Crdt::memory().unwrap().0;
    crdt.join(&PeerId::new([0; 32]), causal).unwrap();
    crdt
}

pub fn crdt_to_causal(crdt: &Crdt, ctx: &CausalContext) -> Causal {
    let other = CausalContext::new(*ctx.doc(), ctx.schema());
    let peer_id = (*ctx.doc()).into();
    let other = Ref::archive(&other);
    crdt.unjoin(&peer_id, other.as_ref()).unwrap()
}

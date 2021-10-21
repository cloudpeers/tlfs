use crate::acl::Acl;
use crate::crdt::{Causal, CausalContext, Crdt, DotStore};
use crate::id::{DocId, PeerId};
use crate::lens::{Kind, Lens};
use crate::path::PathBuf;
use crate::schema::{PrimitiveKind, Prop, Schema};
use crate::util::Ref;
use proptest::collection::SizeRange;
use proptest::prelude::*;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Primitive {
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(String),
}

pub fn arb_prop() -> impl Strategy<Value = Prop> {
    "[a-z]"
}

pub fn arb_peer_id() -> impl Strategy<Value = PeerId> {
    (0u8..5).prop_map(|i| PeerId::new([i; 32]))
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

fn arb_dotset(elems: impl Into<SizeRange>) -> impl Strategy<Value = DotStore> {
    prop::collection::btree_set((arb_peer_id(), any::<u64>()), elems).prop_map(|set| {
        let mut store = DotStore::new();
        for (peer, nonce) in set {
            let mut path = PathBuf::new();
            path.peer(&peer);
            path.nonce(nonce);
            store.insert(path);
        }
        store
    })
}

fn arb_dotfun(kind: PrimitiveKind, elems: impl Into<SizeRange>) -> impl Strategy<Value = DotStore> {
    prop::collection::btree_set(
        (arb_peer_id(), any::<u64>(), arb_primitive_for_kind(kind)),
        elems,
    )
    .prop_map(|set| {
        let mut store = DotStore::new();
        for (peer, nonce, prim) in set {
            let mut path = PathBuf::new();
            path.peer(&peer);
            path.nonce(nonce);
            match prim {
                Primitive::Bool(value) => path.prim_bool(value),
                Primitive::U64(value) => path.prim_u64(value),
                Primitive::I64(value) => path.prim_i64(value),
                Primitive::Str(value) => path.prim_str(&value),
            }
            store.insert(path);
        }
        store
    })
}

fn arb_dotmap(
    kind: PrimitiveKind,
    inner: impl Strategy<Value = DotStore>,
    size: impl Into<SizeRange>,
) -> impl Strategy<Value = DotStore> {
    prop::collection::btree_map(arb_primitive_for_kind(kind), inner, size).prop_map(|set| {
        let mut dotmap = DotStore::new();
        for (prim, store) in set {
            let mut path = PathBuf::new();
            match prim {
                Primitive::Bool(value) => path.prim_bool(value),
                Primitive::U64(value) => path.prim_u64(value),
                Primitive::I64(value) => path.prim_i64(value),
                Primitive::Str(value) => path.prim_str(&value),
            }
            dotmap.union(&store.prefix(path.as_path()));
        }
        dotmap
    })
}

fn arb_struct(
    inner: impl Strategy<Value = DotStore>,
    size: impl Into<SizeRange>,
) -> impl Strategy<Value = DotStore> {
    prop::collection::btree_map(arb_prop(), inner, size).prop_map(|set| {
        let mut fields = DotStore::new();
        for (field, store) in set {
            let mut path = PathBuf::new();
            path.prim_str(&field);
            fields.union(&store.prefix(path.as_path()));
        }
        fields
    })
}

fn arb_dotstore() -> impl Strategy<Value = DotStore> {
    let leaf = prop_oneof![
        arb_dotset(0..10),
        arb_primitive_kind().prop_flat_map(|kind| arb_dotfun(kind, 0..10)),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        let inner2 = inner.clone();
        prop_oneof![
            arb_primitive_kind().prop_flat_map(move |kind| arb_dotmap(kind, inner2.clone(), 0..10)),
            arb_struct(inner, 0..10)
        ]
    })
}

fn arb_causal_for_dotstore(
    store: impl Strategy<Value = crate::crdt::DotStore>,
) -> impl Strategy<Value = Causal> {
    store.prop_map(|store| {
        let doc = DocId::new([0; 32]);
        let mut path = PathBuf::new();
        path.doc(&doc);
        Causal {
            // TODO: add some expired
            expired: Default::default(),
            store: store.prefix(path.as_path()),
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

fn arb_dotstore_for_schema(s: Schema) -> BoxedStrategy<DotStore> {
    match s {
        Schema::Null => Just(DotStore::new()).boxed(),
        Schema::Flag => arb_dotset(0..10).boxed(),
        Schema::Reg(kind) => arb_dotfun(kind, 0..10).boxed(),
        Schema::Table(kind, schema) => {
            arb_dotmap(kind, arb_dotstore_for_schema(*schema), 0..10).boxed()
        }
        Schema::Struct(fields) => fields
            .into_iter()
            .map(|(field, schema)| {
                arb_dotstore_for_schema(schema).prop_map(move |store| {
                    let mut path = PathBuf::new();
                    path.prim_str(&field);
                    store.prefix(path.as_path());
                    store
                })
            })
            .fold(Just(DotStore::new()).boxed(), |store, other| {
                (store, other)
                    .prop_map(move |(mut store, other)| {
                        store.union(&other);
                        store
                    })
                    .boxed()
            }),
        Schema::Array(schema) => {
            todo!()
        }
    }
}

pub fn arb_causal() -> impl Strategy<Value = Causal> {
    arb_causal_for_dotstore(arb_dotstore())
}

pub fn arb_causal_for_schema(schema: Schema) -> impl Strategy<Value = Causal> {
    arb_causal_for_dotstore(arb_dotstore_for_schema(schema))
}

pub fn validate(schema: &Schema, value: &Causal) -> bool {
    let schema = Ref::archive(schema);
    schema.as_ref().validate(value)
}

prop_compose! {
    pub fn schema_and_causal()
        (schema in arb_schema())
        (schema in Just(schema.clone()), crdt in arb_causal_for_schema(schema)) -> (Schema, Causal)
    {
        (schema, crdt)
    }
}

prop_compose! {
    pub fn schema_and_causal2()
        (schema in arb_schema())
        (
            schema in Just(schema.clone()),
            crdt1 in arb_causal_for_schema(schema.clone()),
            crdt2 in arb_causal_for_schema(schema),
        ) -> (Schema, Causal, Causal)
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
        Schema::Array(schema) => {
            todo!()
        }
    }
    (0..strategy.len())
        .prop_flat_map(move |i| strategy[i].clone())
        .boxed()
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
        (lens in arb_lens_for_schema(&schema), schema in Just(schema.clone()), crdt in arb_causal_for_schema(schema)) -> (Lens, Schema, Causal)
    {
        (lens, schema, crdt)
    }
}

pub fn join(c: &Causal, o: &Causal) -> Causal {
    let mut c = c.clone();
    c.join(o);
    c
}

pub fn causal_to_crdt(doc: &DocId, causal: &Causal) -> Crdt {
    let db = sled::Config::new().temporary(true).open().unwrap();
    let store = db.open_tree("store").unwrap();
    let expired = db.open_tree("expired").unwrap();
    let acl = Acl::new(db.open_tree("acl").unwrap());
    let crdt = Crdt::new(store, expired, acl);
    crdt.join(&(*doc).into(), causal).unwrap();
    crdt
}

pub fn crdt_to_causal(doc: &DocId, crdt: &Crdt) -> Causal {
    let other = CausalContext::new();
    let other = Ref::archive(&other);
    crdt.unjoin(&(*doc).into(), doc, other.as_ref()).unwrap()
}

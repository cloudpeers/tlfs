use crate::path::DotStore;
use crate::{Causal, CausalContext, DocId, Dot, PeerId, Primitive};
use proptest::prelude::*;
use std::collections::BTreeMap;
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

fn arb_dotstore() -> impl Strategy<Value = DotStore> {
    let leaf = prop_oneof![
        prop::collection::btree_set(arb_dot(), 0..10).prop_map(DotStore::DotSet),
        prop::collection::btree_map(arb_dot(), arb_primitive(), 0..10).prop_map(DotStore::DotFun),
    ];
    leaf.prop_recursive(8, 256, 10, |inner| {
        prop_oneof![
            prop::collection::btree_map(arb_primitive(), inner.clone(), 0..10)
                .prop_map(DotStore::DotMap),
            prop::collection::btree_map(".*", inner, 0..10).prop_map(DotStore::Struct),
        ]
    })
}

pub fn arb_causal() -> impl Strategy<Value = Causal> {
    arb_dotstore().prop_map(|store| {
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

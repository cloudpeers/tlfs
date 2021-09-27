use crate::{Causal, Clock, Dot, DotFun, DotMap, DotSet, DotStore, EWFlag, Lattice, MVReg, ORMap};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};
use std::iter::FromIterator;
use std::ops::Range;

pub fn arb_dot_in(counter: Range<u64>) -> impl Strategy<Value = Dot<u8>> {
    (0u8..5, counter).prop_map(|(a, c)| Dot::new(a, c))
}

pub fn arb_dot() -> impl Strategy<Value = Dot<u8>> {
    arb_dot_in(1u64..25)
}

pub fn arb_clock() -> impl Strategy<Value = Clock<u8>> {
    prop::collection::btree_set(arb_dot_in(1u64..5), 0..50)
        .prop_map(|cloud| Clock::from_iter(cloud.into_iter()))
}

pub fn to_causal<S: DotStore<u8>>(store: S) -> Causal<u8, S> {
    let mut dots = BTreeSet::new();
    store.dots(&mut dots);
    let mut present = BTreeMap::new();
    for dot in dots {
        let counter = dot.counter();
        let actor = dot.actor;
        if counter > 0 && counter > present.get(&actor).copied().unwrap_or_default() {
            present.insert(actor, counter);
        }
    }
    let clock = Clock::from_map(present);
    Causal { store, clock }
}

pub fn arb_causal<S, P, F>(s: F) -> impl Strategy<Value = Causal<u8, S>>
where
    S: DotStore<u8> + std::fmt::Debug,
    P: Strategy<Value = S>,
    F: Fn() -> P,
{
    s().prop_map(to_causal)
}

pub fn arb_dotset() -> impl Strategy<Value = DotSet<u8>> {
    prop::collection::btree_set(arb_dot(), 0..50).prop_map(DotSet::new)
}

pub fn arb_dotfun<L, P>(s: P) -> impl Strategy<Value = DotFun<u8, L>>
where
    L: Lattice + std::fmt::Debug,
    P: Strategy<Value = L>,
{
    prop::collection::btree_map(arb_dot(), s, 0..10).prop_map(DotFun::new)
}

pub fn arb_dotmap<S, P>(s: P) -> impl Strategy<Value = DotMap<u8, S>>
where
    S: DotStore<u8> + std::fmt::Debug,
    P: Strategy<Value = S>,
{
    prop::collection::btree_map(0u8..10, s, 0..5).prop_map(DotMap::new)
}

pub fn arb_ewflag() -> impl Strategy<Value = EWFlag<u8>> {
    (arb_dot(), any::<bool>()).prop_map(|(dot, b)| {
        let flag = Causal::<_, EWFlag<_>>::new();
        if b {
            flag.as_ref().enable(dot).store
        } else {
            flag.as_ref().disable(dot).store
        }
    })
}

pub fn arb_mvreg<L>(v: impl Strategy<Value = L>) -> impl Strategy<Value = MVReg<u8, L>>
where
    L: Lattice + std::fmt::Debug,
{
    (arb_dot(), v).prop_map(|(dot, v)| {
        let reg = Causal::<_, MVReg<_, _>>::new();
        reg.as_ref().write(dot, v).store
    })
}

pub fn arb_ormap<K, V>(
    k: impl Strategy<Value = K>,
    v: impl Strategy<Value = V>,
) -> impl Strategy<Value = ORMap<K, V>>
where
    K: Ord + std::fmt::Debug,
    V: DotStore<u8> + std::fmt::Debug,
{
    (k, v).prop_map(|(k, v)| {
        let map = Causal::<_, ORMap<_, _>>::new();
        map.as_ref()
            .apply(k, |_| Causal {
                store: v.clone(),
                clock: Default::default(),
            })
            .store
    })
}

pub fn union(a: &Clock<u8>, b: &Clock<u8>) -> Clock<u8> {
    let mut a = a.clone();
    a.union(b);
    a
}

pub fn intersect(a: &Clock<u8>, b: &Clock<u8>) -> Clock<u8> {
    a.intersect(b)
}

pub fn difference(a: &Clock<u8>, b: &Clock<u8>) -> Clock<u8> {
    a.difference(b)
}

pub fn join<L: Lattice + Clone>(a: &L, b: &L) -> L {
    let mut a = a.clone();
    a.join(b);
    a
}

#[macro_export]
macro_rules! lattice {
    ($module:ident, $arb:expr) => {
        mod $module {
            use super::*;
            use $crate::props::*;

            proptest! {
                #[test]
                fn idempotent(a in arb_causal($arb)) {
                    prop_assert_eq!(join(&a, &a), a);
                }

                #[test]
                fn commutative(a in arb_causal($arb), b in arb_causal($arb)) {
                    prop_assert_eq!(join(&a, &b), join(&b, &a));
                }

                #[test]
                fn unjoin(a in arb_causal($arb), b in arb_clock()) {
                    let b = a.unjoin(&b);
                    prop_assert_eq!(join(&a, &b), a);
                }

                #[test]
                fn associative(dots in arb_causal($arb), a in arb_clock(), b in arb_clock(), c in arb_clock()) {
                    let a = dots.unjoin(&a);
                    let b = dots.unjoin(&b);
                    let c = dots.unjoin(&c);
                    prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
                }
            }
        }
    };
}

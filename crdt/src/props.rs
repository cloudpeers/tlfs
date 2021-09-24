use crate::{Causal, Clock, Dot, DotStore, Lattice};
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet};

pub fn arb_dot() -> impl Strategy<Value = Dot<u8>> {
    (0u8..5, 1u64..25).prop_map(|(a, c)| Dot::new(a, c))
}

pub fn arb_clock() -> impl Strategy<Value = Clock<u8>> {
    (
        prop::collection::btree_map(0u8..5, 1u64..5, 0..5),
        prop::collection::btree_set((0u8..5, 6u64..10).prop_map(|(a, c)| Dot::new(a, c)), 0..5),
    )
        .prop_map(|(clock, cloud)| Clock { clock, cloud })
}

pub fn arb_causal<S, P, F>(s: F) -> impl Strategy<Value = Causal<u8, S>>
where
    S: DotStore<u8> + std::fmt::Debug,
    P: Strategy<Value = S>,
    F: Fn() -> P,
{
    s().prop_map(|store| {
        let mut dots = BTreeSet::new();
        store.dots(&mut dots);
        let mut present = BTreeMap::new();
        for Dot { actor, counter } in dots {
            if counter > 0 && counter > present.get(&actor).copied().unwrap_or_default() {
                present.insert(actor, counter);
            }
        }
        let mut clock = Clock::new();
        clock.clock = present;
        Causal { store, clock }
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

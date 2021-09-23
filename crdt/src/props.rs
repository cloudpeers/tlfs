use crate::{Causal, Clock, Dot, DotStore, Lattice};
use proptest::prelude::*;
use std::collections::BTreeSet;

pub fn arb_dot() -> impl Strategy<Value = Dot<u8>> {
    (0u8..5, 0u64..25).prop_map(|(a, c)| Dot::new(a, c))
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
        let mut clock = dots.into_iter().collect::<Clock<_>>();
        clock.cloud = Default::default();
        Causal { store, clock }
    })
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
                fn associative(a in arb_causal($arb), b in arb_causal($arb), c in arb_causal($arb)) {
                    prop_assert_eq!(join(&join(&a, &b), &c), join(&a, &join(&b, &c)));
                }
            }
        }
    };
}

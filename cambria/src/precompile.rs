use crate::lens::Lenses;
use heck::CamelCase;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use rkyv::archived_root;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use std::path::Path;
use std::process::Command;

pub fn write_tokens<P: AsRef<Path>>(path: P, tokens: &TokenStream) {
    std::fs::write(path.as_ref(), tokens.to_string()).unwrap();
    Command::new("rustfmt")
        .arg(path.as_ref())
        .arg("--emit")
        .arg("files")
        .status()
        .unwrap();
}

pub fn precompile(ident: &str, lenses: Lenses) -> TokenStream {
    let mut ser = AllocSerializer::<256>::default();
    ser.serialize_value(&lenses).unwrap();
    let lenses = ser.into_serializer().into_inner().to_vec();
    let lenses_ref = unsafe { archived_root::<Lenses>(&lenses) };

    let schema = lenses_ref.to_schema().unwrap();
    let ident = ident.to_camel_case();
    let ident = format_ident!("{}", ident);
    let lenses_len = lenses.len();
    let schema_len = schema.len();

    quote! {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub struct #ident;

        impl cambria::Cambria for #ident {
            fn lenses() -> &'static [u8] {
                use cambria::aligned::{Aligned, A8};
                static LENSES: Aligned<A8, [u8; #lenses_len]> = Aligned([#(#lenses),*]);
                &LENSES[..]
            }

            fn schema() -> &'static cambria::ArchivedSchema {
                use cambria::aligned::{Aligned, A8};
                static SCHEMA: Aligned<A8, [u8; #schema_len]> = Aligned([#(#schema),*]);
                unsafe { cambria::rkyv::archived_root::<cambria::Schema>(&SCHEMA[..]) }
            }
        }
    }
}

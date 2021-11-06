use cbindgen::{Builder, Language};

fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    Builder::new()
        .with_crate(crate_dir)
        .with_language(Language::C)
        .with_include_guard("TLFS_H")
        .with_no_includes()
        .with_include("stdbool.h")
        .with_include("stdint.h")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("tlfs.h");

    tlfsc::compile(
        "assets/capi/include/todoapp.tlfs",
        "assets/capi/include/todoapp.tlfs.rkyv",
    )
    .unwrap();
}

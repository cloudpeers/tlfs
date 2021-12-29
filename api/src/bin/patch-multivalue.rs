use anyhow::Context;
use ffi_gen::FfiGen;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let dir = PathBuf::from(
        std::env::var("CARGO_MANIFEST_DIR")
            .context("Run with `cargo run --bin patch-multivalue`")?,
    );
    let path = dir.join("tlfs.rsh");
    let ffigen = FfiGen::new(&path)?;

    let mut args = std::env::args();
    let me = args.next().unwrap();
    if let (Some(wasm), None) = (args.next(), args.next()) {
        ffigen.wasm_multi_value_shim(&wasm)?;
        println!("{}.multivalue.wasm", wasm);
        Ok(())
    } else {
        anyhow::bail!(
            r#"
-------------
Usage: {} my_module.wasm

This will output the file `my_module.wasm.multivalue.wasm` in the same directory as
`my_module.wasm`.
-------------
"#,
            me
        );
    }
}

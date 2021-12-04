use ffi_gen::FfiGen;
use std::path::PathBuf;

fn main() {
    let dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let path = dir.join("tlfs.rsh");
    println!(
        "cargo:rerun-if-changed={}",
        path.as_path().to_str().unwrap()
    );
    if std::env::var_os("FFIGEN").is_none() {
        return;
    }
    let ffigen = FfiGen::new(&path).unwrap();
    let dart = dir.join("dart").join("lib").join("tlfs.dart");
    ffigen.generate_dart(dart, "tlfs", "tlfs").unwrap();
    let js = dir.join("js").join("bindings.js");
    ffigen.generate_js(js).unwrap();
    //let ts = dir.join("js").join("bindings.d.ts");
    //ffigen.generate_ts(ts).unwrap();
}

#/bin/bash
set -e
WASMOPT=./wasm-opt
WASMOPT_VERSION=101
WASMBINDGEN_VERSION=0.2.77
OUT=./pkg

echo "Running cargo build"
# todo: release build
cargo build --target wasm32-unknown-unknown

if [ -d $OUT ]; then
  echo "Clearing output directory '$OUT'"
  rm -rf $OUT
fi

if ! [ -x "$(command -v wasm-bindgen)" ]; then
  echo "Installing wasm-bindgen-cli via cargo"
  cargo install wasm-bindgen-cli --version $WASMBINDGEN_VERSION
fi

echo "Generating wasm-bindings"

# add supports for Weak References, see [1].
# TLDR: Structs passed from Rust to JS will be deallocated
# automatically, no need to call `.free` in JS.
#
# [1]: https://rustwasm.github.io/docs/wasm-bindgen/reference/weak-references.html
wasm-bindgen ../target/wasm32-unknown-unknown/debug/tlfs_wasm_ffi.wasm \
  --out-dir $OUT \
  --out-name local_first \
  --target web \
  --typescript \
  --weak-refs
  #--reference-types # TODO: wasm-opt crashes with that flag

echo "Generating package.json"
cat <<EOF >> $OUT/package.json
{
  "name": "local-first",
  "version": "0.1.0",
  "files": [
    "local_first_bg.wasm",
    "local_first.js",
    "local_first.d.ts"
  ],
  "module": "local_first.js",
  "types": "local_first.d.ts",
  "sideEffects": false
}
EOF


if [ ! -f $WASMOPT ]; then
  echo "Downloading wasm-opt"
  wget -qO- \
  https://github.com/WebAssembly/binaryen/releases/download/version_$WASMOPT_VERSION/binaryen-version_$WASMOPT_VERSION-x86_64-linux.tar.gz \
  | tar xfz - binaryen-version_$WASMOPT_VERSION/bin/wasm-opt -O >> $WASMOPT
  chmod +x $WASMOPT
fi

if [ -z "${NO_OPTIMIZE}" ]; then
  echo "Optimizing wasm bindings with default optimization (this might take some time)"
  ./wasm-opt $OUT/local_first_bg.wasm -Oz -g --output $OUT/local_first_bg.opt.wasm
fi

echo "Find your wasm package in $OUT"

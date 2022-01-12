#/bin/bash
set -e
WASMOPT=./wasm-opt
WASMOPT_VERSION=104
WASMBINDGEN_VERSION=0.2.77
OUT=./pkg-wasm-bindgen

echo "Running cargo build"
if [ -z "${OPTIMIZE}" ]; then
  FFIGEN=1 cargo build --target wasm32-unknown-unknown
  MODE=debug
else
  FFIGEN=1 cargo build --target wasm32-unknown-unknown --release
  MODE=release
fi

if [ -d $OUT ]; then
  echo "Clearing output directory '$OUT'"
  rm -rf $OUT
fi

if ! [ -x "$(command -v wasm-bindgen)" ]; then
  echo "Installing wasm-bindgen-cli via cargo"
  cargo install wasm-bindgen-cli --version $WASMBINDGEN_VERSION
fi

echo "Patching multivalue returns"
LIB=../target/wasm32-unknown-unknown/$MODE/tlfs.wasm
cargo run --target x86_64-unknown-linux-gnu --bin patch-multivalue \
  -- $LIB

echo "Generating wasm-bindings"

# add supports for Weak References, see [1].
# TLDR: Structs passed from Rust to JS will be deallocated
# automatically, no need to call `.free` in JS.
#
# [1]: https://rustwasm.github.io/docs/wasm-bindgen/reference/weak-references.html
wasm-bindgen \
  $LIB.multivalue.wasm \
  --out-dir $OUT \
  --out-name local_first \
  --target web \
  --typescript \
  --weak-refs
#  --reference-types wasm-opt crashes with that flag?

if [ ! -f $WASMOPT ]; then
  echo "Downloading wasm-opt"
  wget -qO- \
  https://github.com/WebAssembly/binaryen/releases/download/version_$WASMOPT_VERSION/binaryen-version_$WASMOPT_VERSION-x86_64-linux.tar.gz \
  | tar xfz - binaryen-version_$WASMOPT_VERSION/bin/wasm-opt -O >> $WASMOPT
  chmod +x $WASMOPT
fi

if [ "${OPTIMIZE}" ]; then
  echo "Optimizing wasm bindings with default optimization (this might take some time)"
  ./wasm-opt \
    $OUT/local_first_bg.wasm \
    --enable-multivalue \
    -Oz \
    -g \
    --output $OUT/local_first_bg.opt.wasm
  cp $OUT/local_first_bg.wasm $OUT/local_first_bg_unoptimized.wasm
  cp $OUT/local_first_bg.opt.wasm $OUT/local_first_bg.wasm
fi


pushd js
npm i
PACKAGE=`npm pack --json | jq -r '.[].filename'`

echo "Find your npm package in ./js/$PACKAGE"

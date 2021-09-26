use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn my_async_test() {
    let promise = js_sys::Promise::resolve(&JsValue::from(42));

    let x = JsFuture::from(promise).await.unwrap();
    assert_eq!(x, 42);
}

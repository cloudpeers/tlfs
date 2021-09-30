use std::future::Future;

use js_sys::Promise;
use libp2p::{futures::channel::mpsc, identity, Multiaddr};
use log::*;
use serde::Serialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;

use crate::p2p::SwarmWrapper;

mod p2p;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen(start)]
pub fn start() {
    let _ = console_log::init_with_level(log::Level::Debug);
    ::console_error_panic_hook::set_once();
    debug!("Setup logging");
}

#[wasm_bindgen(js_name = "helloWorld")]
pub fn hello_world() -> String {
    "Hello World".into()
}

#[wasm_bindgen]
#[derive(Debug)]
pub struct LocalFirst {
    inner: SwarmWrapper,
}

#[wasm_bindgen]
impl LocalFirst {
    //    #[wasm_bindgen(js_name = "initWithKey")]
    //    pub async fn new_with_key(private_key: String) -> Result<LocalFirst, JsValue> {
    //        todo!();
    //    }

    #[wasm_bindgen(js_name = "init")]
    pub async fn new() -> Result<LocalFirst, JsValue> {
        let kp = identity::Keypair::generate_ed25519();
        let signaling_server: Multiaddr = "/dns4/local1st.net/tcp/443/wss/p2p-webrtc-star"
            .parse()
            .unwrap();
        Self::spawn(kp, signaling_server).map_err(map_err)
    }

    #[wasm_bindgen(js_name = "doSomething")]
    pub async fn do_something(self) -> Result<(), JsValue> {
        Ok(())
    }

    #[wasm_bindgen(js_name = "doSomethingElse")]
    pub fn do_something_else(&self) -> Promise {
        todo!()
    }

    pub fn dial(&self, addr: String) -> Promise {
        let mut inner = self.inner.clone();
        let fut = async move {
            inner.dial(addr.parse()?).await?;
            Result::<_, anyhow::Error>::Ok(())
        };
        to_promise(fut)
    }

    pub fn info(&self) -> Promise {
        let mut inner = self.inner.clone();
        to_promise(async move { inner.info().await })
    }

    fn spawn(identity: identity::Keypair, signaling_server: Multiaddr) -> anyhow::Result<Self> {
        let inner = SwarmWrapper::new(identity, signaling_server);
        Ok(Self { inner })
    }
}

fn to_promise(
    fut: impl Future<Output = std::result::Result<impl Serialize, impl std::fmt::Display>> + 'static,
) -> Promise {
    future_to_promise(async move {
        fut.await
            .map(|e| JsValue::from_serde(&e).unwrap())
            .map_err(map_err)
    })
}

fn map_err(err: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&format!("Error: {:#}", err)).into()
}

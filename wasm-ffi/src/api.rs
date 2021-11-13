use js_sys::{Array, Object, Proxy, Reflect};
use libp2p::Multiaddr;
use log::*;
use std::{cell::RefCell, rc::Rc};
use tlfs::{
    Backend, Causal, Doc, Kind, Lens, Lenses, Package, PrimitiveKind, Ref, Sdk, ToLibp2pKeypair,
};
use wasm_bindgen::prelude::*;

use crate::p2p::mk_transport;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[wasm_bindgen(start)]
pub fn start() {
    let _ = console_log::init_with_level(log::Level::Debug);
    ::console_error_panic_hook::set_once();
    debug!("Setup logging");
}

#[wasm_bindgen]
pub struct LocalFirst {
    inner: Sdk,
}

#[wasm_bindgen]
impl LocalFirst {
    #[wasm_bindgen(js_name = "init")]
    pub async fn new() -> Result<LocalFirst, JsValue> {
        let signaling_server: Multiaddr = "/dns4/local1st.net/tcp/443/wss/p2p-webrtc-star"
            .parse()
            .unwrap();
        let cloud_relay =
            vec!["/dns4/local1st.net/tcp/4002/wss/p2p/12D3KooWCL3666CJUm6euzw34jMure6rgkQmW21qK4m4DEd9iWGy".parse().unwrap()];
        let mut lenses = vec![
            Lens::Make(Kind::Struct),
            Lens::AddProperty("todos".into()),
            Lens::Make(Kind::Table(PrimitiveKind::U64)).lens_in("todos"),
            Lens::Make(Kind::Struct).lens_map_value().lens_in("todos"),
            Lens::AddProperty("title".into())
                .lens_map_value()
                .lens_in("todos"),
            Lens::Make(Kind::Reg(PrimitiveKind::Str))
                .lens_in("title")
                .lens_map_value()
                .lens_in("todos"),
            Lens::AddProperty("complete".into())
                .lens_map_value()
                .lens_in("todos"),
            Lens::Make(Kind::Flag)
                .lens_in("complete")
                .lens_map_value()
                .lens_in("todos"),
        ];
        let packages = vec![Package::new(
            "todoapp".into(),
            8,
            &Lenses::new(lenses.clone()),
        )];

        Self::spawn(
            signaling_server,
            cloud_relay,
            "demo".into(),
            Ref::archive(&packages).as_bytes(),
        )
        .await
        .map_err(map_err)
    }

    async fn spawn(
        signaling_server: Multiaddr,
        bootstrap: Vec<Multiaddr>,
        discovery_namespace: String,
        package: &[u8],
    ) -> anyhow::Result<Self> {
        // TODO: bootstrap and discovery!
        let backend = Backend::in_memory(package)?;
        let frontend = backend.frontend();
        let identity = frontend.default_keypair()?;
        let transport = mk_transport(identity.to_libp2p());
        let (inner, fut) = Sdk::new_with_transport(
            backend,
            frontend,
            identity.peer_id(),
            transport,
            std::iter::once(signaling_server.clone()),
        )
        .await?;
        wasm_bindgen_futures::spawn_local(fut);
        /*
                    swarm.listen_on(signaling_server.clone()).expect("FIXME");
                    swarm.add_external_address(
                        signaling_server
                            .with(Protocol::P2pWebRtcStar)
                            .with(Protocol::P2p(*peer_id.as_ref())),
                        // TODO
                        AddressScore::Infinite,
                    );
                    for b in &bootstrap {
                        if let Err(e) = swarm.dial_addr(b.clone()) {
                            error!("Error dialing bootstrap {}: {:#}", b, e);
                        }
                    }
        */
        Ok(Self { inner })
    }

    #[wasm_bindgen(js_name = "peerId")]
    pub fn peer_id(&self) -> String {
        self.inner.peer_id().to_string()
    }

    // TODO: type annotations
    pub fn docs(&self, schema: String) -> Result<js_sys::Array, JsValue> {
        wrap(move || {
            let docs = self
                .inner
                .docs(schema)
                .map(|x| x.map(|y| JsValue::from(y.to_string())))
                .collect::<anyhow::Result<_>>()?;
            Ok(docs)
        })
    }

    #[wasm_bindgen(js_name = "createDoc")]
    pub fn create_doc(&mut self, schema: &str) -> Result<DocWrapper, JsValue> {
        wrap(|| {
            let inner = self.inner.create_doc(schema)?;
            Ok(DocWrapper { inner })
        })
    }

    #[wasm_bindgen(js_name = "openDoc")]
    pub fn open_doc(&mut self, doc: String) -> Result<DocWrapper, JsValue> {
        wrap(|| {
            let doc = doc.parse()?;
            let inner = self.inner.doc(doc)?;
            Ok(DocWrapper { inner })
        })
    }

    #[wasm_bindgen(js_name = "addAddress")]
    pub fn add_address(&self, peer: String, addr: String) -> Result<(), JsValue> {
        wrap(|| {
            let peer = peer.parse()?;
            let addr = addr.parse()?;
            self.inner.add_address(peer, addr);
            Ok(())
        })
    }

    #[wasm_bindgen(js_name = "removeAddress")]
    pub fn remove_address(&self, peer: String, addr: String) -> Result<(), JsValue> {
        wrap(|| {
            let peer = peer.parse()?;
            let addr = addr.parse()?;
            self.inner.remove_address(peer, addr);
            Ok(())
        })
    }
}

#[wasm_bindgen]
pub struct DocWrapper {
    inner: Doc,
}

#[allow(dead_code)]
struct ProxyHandler {
    // Empty object acting as a dummy target
    target: Object,
    // The JS handler with references to `get` and `set`
    handler: Object,
    /// References to recursive proxies.
    // Kept to keep alive
    ref_stack: Rc<RefCell<Vec<Self>>>,
    // Kept to keep alive
    get: Closure<dyn Fn(JsValue, JsValue) -> Result<JsValue, JsValue>>,
    // Kept to keep alive
    set: Closure<dyn Fn(JsValue, JsValue, JsValue) -> Result<bool, JsValue>>,
}
impl ProxyHandler {
    fn proxy(&self) -> Proxy {
        Proxy::new(&self.target, &self.handler)
    }
}

impl ProxyHandler {
    fn new(doc: Rc<RefCell<Doc>>) -> Result<Self, JsValue> {
        let doc_c = doc.clone();
        let ref_stack = Rc::new(RefCell::new(vec![]));
        let ref_stack_c = ref_stack.clone();

        let proxy_get = Closure::wrap(Box::new(move |obj: JsValue, prop: JsValue| {
            info!("{:?} {:?} ", obj, prop);

            if prop.is_object() || Array::is_array(&prop) {
                // `prop` refers deeper into the object. Return a new proxy object for `prop`.
                let handler = Self::new(doc_c.clone())?;
                let proxy = handler.proxy();
                ref_stack_c.borrow_mut().push(handler);

                return Ok(proxy.into());
            }

            let doc = doc_c.borrow_mut();
            let mut c = doc.cursor();
            if let Some(s) = prop.as_string() {
                if let Err(e) = c.key_str(&s) {
                    // TODO: handle `e`
                    c.field(&s).map_err(map_err)?;
                }
            } else if let Some(u) = prop.as_f64() {
                if let Err(e) = c.key_u64(u as u64) {
                    // todo handle `e`
                    c.key_i64(u as i64).map_err(map_err)?;
                }
            } else {
                let b = prop.as_bool().unwrap();
                c.key_bool(b).map_err(map_err)?;
            };
            // FIXME: support other data types
            if let Some(r) = c.strs().map_err(map_err)?.next() {
                let r = r.map_err(map_err)?;
                Ok(r.into())
            } else {
                Ok(JsValue::undefined())
            }
        })
            as Box<dyn Fn(JsValue, JsValue) -> Result<JsValue, JsValue>>);
        //        let changes_c = changes.clone();
        let proxy_set = Closure::wrap(Box::new(
            move |obj: JsValue, prop: JsValue, value: JsValue| {
                info!("{:?} {:?} {:?}", obj, prop, value);
                let doc = doc.borrow_mut();
                let mut c = doc.cursor();
                if let Some(s) = prop.as_string() {
                    if let Err(e) = c.key_str(&s) {
                        // TODO; handle `e`
                        c.field(&s).map_err(map_err)?;
                    }
                } else if let Some(f) = prop.as_f64() {
                    if let Err(e) = c.key_u64(f as u64) {
                        // TODO; handle `e`
                        c.key_i64(f as i64).map_err(map_err)?;
                    }
                } else {
                    c.key_bool(prop.as_bool().unwrap()).map_err(map_err)?;
                }

                let causal = if let Some(s) = value.as_string() {
                    c.assign_str(&s).map_err(map_err)?
                } else if let Some(f) = value.as_f64() {
                    match c.assign_u64(f as u64) {
                        Err(e) =>
                        // TODO; handle `e`
                        {
                            c.assign_i64(f as i64).map_err(map_err)?
                        }
                        Ok(c) => c,
                    }
                } else {
                    c.assign_bool(value.as_bool().unwrap()).map_err(map_err)?
                };

                doc.apply(causal).map_err(map_err)?;

                Ok(true)
            },
        )
            as Box<dyn Fn(JsValue, JsValue, JsValue) -> Result<bool, JsValue>>);

        let handler = Object::new();
        Reflect::set(&handler, &"get".into(), proxy_get.as_ref())?;
        Reflect::set(&handler, &"set".into(), proxy_set.as_ref())?;

        Ok(Self {
            get: proxy_get,
            handler,
            set: proxy_set,
            target: Object::new(),
            ref_stack,
        })
    }
}

//#[wasm_bindgen]
impl DocWrapper {
    /// Returns the document identifier.
    pub fn id(&self) -> String {
        self.inner.id().to_string()
    }

    // TODO: add manual ts types for `f`
    pub fn change(&self, f: &js_sys::Function) -> Result<(), JsValue> {
        let proxy = ProxyHandler::new(Rc::new(RefCell::new(self.inner.clone())))?;

        f.call1(&JsValue::null(), &proxy.proxy())?;
        Ok(())
    }

    //    /// Applies a transaction to the document.
    //    pub fn apply(&self, causal: &JsValue) -> Result<(), JsValue> {
    //        let causal: Causal = causal.into_serde().map_err(map_err)?;
    //        self.inner.apply(causal).map_err(map_err)
    //    }
}

fn map_err(err: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&format!("Error: {:#}", err)).into()
}

fn wrap<T>(f: impl FnOnce() -> anyhow::Result<T>) -> Result<T, JsValue> {
    f().map_err(map_err)
}

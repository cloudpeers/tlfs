use js_sys::{Array, Object, Proxy, Reflect};
use libp2p::{futures::StreamExt, Multiaddr};
use log::*;
use std::{cell::RefCell, rc::Rc};
use tlfs::{
    ArchivedSchema, Backend, Causal, Cursor, Doc, Kind, Lens, Lenses, Package, Primitive,
    PrimitiveKind, Ref, Sdk, ToLibp2pKeypair,
};
use wasm_bindgen::prelude::*;

use crate::p2p::mk_transport;

//#[global_allocator]
//static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

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
        let lenses = vec![
            Lens::Make(Kind::Struct),
            Lens::AddProperty("todos".into()),
            Lens::Make(Kind::Table(PrimitiveKind::Str)).lens_in("todos"),
            Lens::Make(Kind::Reg(PrimitiveKind::Str))
                .lens_map_value()
                .lens_in("todos"),
            //            Lens::Make(Kind::Reg(PrimitiveKind::Str)).lens_in("todos"),
            //            Lens::Make(Kind::Table(PrimitiveKind::U64)).lens_in("todos"),
            //            Lens::Make(Kind::Struct).lens_map_value().lens_in("todos"),
            //            Lens::AddProperty("title".into())
            //                .lens_map_value()
            //                .lens_in("todos"),
            //            Lens::Make(Kind::Reg(PrimitiveKind::Str))
            //                .lens_in("title")
            //                .lens_map_value()
            //                .lens_in("todos"),
            //            Lens::AddProperty("complete".into())
            //                .lens_map_value()
            //                .lens_in("todos"),
            //            Lens::Make(Kind::Flag)
            //                .lens_in("complete")
            //                .lens_map_value()
            //                .lens_in("todos"),
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
    // Kept to keep alive
    delete: Closure<dyn Fn(JsValue, JsValue) -> Result<bool, JsValue>>,
}

impl ProxyHandler {
    fn proxy(&self) -> Proxy {
        Proxy::new(&self.target, &self.handler)
    }
}

struct JsonPointer {
    tokens: Vec<String>,
}

impl JsonPointer {
    fn new(str: &str) -> anyhow::Result<Self> {
        if str.is_empty() {
            return Ok(Self { tokens: vec![] });
        }
        anyhow::ensure!(str.starts_with('/'), "Invalid pointer");
        let tokens = str
            .split('/')
            .skip(1)
            .map(|x| x.replace("~1", "/").replace("~0", "~"))
            .collect();
        Ok(Self { tokens })
    }

    fn goto(&self, cursor: &mut Cursor) -> anyhow::Result<()> {
        for token in &self.tokens {
            match cursor.schema() {
                ArchivedSchema::Table(key_kind, _) => match key_kind {
                    PrimitiveKind::Bool => {
                        cursor.key_bool(token.parse()?)?;
                    }
                    PrimitiveKind::U64 => {
                        cursor.key_u64(token.parse()?)?;
                    }
                    PrimitiveKind::I64 => {
                        cursor.key_i64(token.parse()?)?;
                    }
                    PrimitiveKind::Str => {
                        cursor.key_str(token)?;
                    }
                },

                ArchivedSchema::Array(_) => {
                    let idx = if token == "-" {
                        cursor.len()? as usize
                    } else {
                        token.parse()?
                    };
                    cursor.index(idx)?;
                }
                ArchivedSchema::Struct(_) => {
                    cursor.field(token)?;
                }
                _ => anyhow::bail!("Hit a leaf"),
            }
        }
        Ok(())
    }
}

fn get_value(cursor: &mut Cursor) -> anyhow::Result<JsValue> {
    match cursor.schema() {
        ArchivedSchema::Null => Ok(JsValue::undefined()),
        ArchivedSchema::Flag => Ok(JsValue::from_bool(cursor.enabled()?)),
        ArchivedSchema::Reg(kind) => Ok(match kind {
            PrimitiveKind::Bool => cursor.bools()?.next().transpose()?.map(JsValue::from_bool),
            PrimitiveKind::U64 => cursor
                .u64s()?
                .next()
                .transpose()?
                .map(|x| JsValue::from_f64(x as f64)),
            PrimitiveKind::I64 => cursor
                .i64s()?
                .next()
                .transpose()?
                .map(|x| JsValue::from_f64(x as f64)),
            PrimitiveKind::Str => cursor.strs()?.next().transpose()?.map(Into::into),
        }
        .unwrap_or_else(JsValue::undefined)),
        ArchivedSchema::Struct(_) | ArchivedSchema::Table(_, _) => {
            let obj = Object::new();
            for key in cursor.keys() {
                let mut here = cursor.clone();
                let key = key?;
                let (k, v) = match key {
                    Primitive::Bool(b) => {
                        here.key_bool(b)?;
                        (JsValue::from_bool(b), get_value(&mut here)?)
                    }
                    Primitive::U64(n) => {
                        here.key_u64(n)?;
                        (JsValue::from_f64(n as f64), get_value(&mut here)?)
                    }
                    Primitive::I64(n) => {
                        here.key_i64(n)?;
                        (JsValue::from_f64(n as f64), get_value(&mut here)?)
                    }
                    Primitive::Str(s) => {
                        if matches!(cursor.schema(), ArchivedSchema::Struct(_)) {
                            here.field(&s)?;
                        } else {
                            here.key_str(&s)?;
                        }
                        (s.into(), get_value(&mut here)?)
                    }
                };
                Reflect::set(&obj, &k, &v).unwrap();
            }
            Ok(obj.into())
        }
        ArchivedSchema::Array(_) => {
            let len = cursor.len()?;
            let arr = Array::new_with_length(len);
            for i in 0..len {
                let mut here = cursor.clone();
                here.index(i as usize)?;
                let v = get_value(&mut here)?;
                arr.set(i, v);
            }
            Ok(arr.into())
        }
    }
}

#[derive(Debug)]
enum FromJs {
    Object(JsValue),
    Array(Array),
    Function(JsValue),
    String(String),
    Number(f64),
    Bool(bool),
}
impl From<JsValue> for FromJs {
    fn from(v: JsValue) -> Self {
        if v.is_object() {
            Self::Object(v)
        } else if Array::is_array(&v) {
            Self::Array(Array::from(&v))
        } else if v.is_function() {
            Self::Function(v)
        } else if let Some(s) = v.as_string() {
            Self::String(s)
        } else if let Some(u) = v.as_f64() {
            Self::Number(u)
        } else {
            Self::Bool(v.as_bool().unwrap())
        }
    }
}

impl FromJs {
    fn traverse(
        &self,
        c: &mut Cursor,
        mut path: Option<&mut Vec<CursorPath>>,
    ) -> anyhow::Result<()> {
        info!("{:?} {:?}", self, c.schema());
        match (self, c.schema()) {
            (FromJs::Object(_), _) | (FromJs::Array(_), _) => unreachable!("Handled above"),
            (FromJs::Function(_), _) => {
                // JS probably stringifies the fn?
                //return Ok(JsValue::undefined());
                anyhow::bail!("Passed a function");
            }
            (_, ArchivedSchema::Null) | (_, ArchivedSchema::Flag) | (_, ArchivedSchema::Reg(_)) => {
                todo!("Error or undefined?")
            }
            (FromJs::String(s), ArchivedSchema::Table(_, _)) => {
                c.key_str(s)?;
                if let Some(path) = path.as_mut() {
                    path.push(CursorPath::KeyStr(s.into()));
                }
            }
            (FromJs::String(_), ArchivedSchema::Array(_)) => {
                anyhow::bail!("Can't index into an array with a string");
            }
            (FromJs::String(s), ArchivedSchema::Struct(_)) => {
                info!("field {:?}", s);
                c.field(s)?;
                if let Some(path) = path.as_mut() {
                    path.push(CursorPath::Field(s.into()));
                }
            }
            (FromJs::Number(n), ArchivedSchema::Table(key_kind, _)) => match key_kind {
                PrimitiveKind::U64 => {
                    let n = *n as u64;
                    c.key_u64(n)?;
                    if let Some(path) = path.as_mut() {
                        path.push(CursorPath::KeyU64(n));
                    }
                }
                PrimitiveKind::I64 => {
                    let n = *n as i64;
                    c.key_i64(n)?;
                    if let Some(path) = path.as_mut() {
                        path.push(CursorPath::KeyI64(n));
                    }
                }
                kind => anyhow::bail!("Can't index into table (expected {:?})", kind),
            },
            (FromJs::Number(n), ArchivedSchema::Array(_)) => {
                let n = *n as usize;
                c.index(n)?;
                if let Some(path) = path.as_mut() {
                    path.push(CursorPath::Index(n));
                }
            }
            (FromJs::Number(_), ArchivedSchema::Struct(_)) => {
                anyhow::bail!("Can't index into a struct with a number")
            }
            (FromJs::Bool(_), ArchivedSchema::Table(_, _)) => todo!(),
            (FromJs::Bool(_), ArchivedSchema::Array(_)) => {
                anyhow::bail!("Can't index into an array with a bool");
            }
            (FromJs::Bool(_), ArchivedSchema::Struct(_)) => {
                anyhow::bail!("Can't index into a struct with a bool")
            }
        };
        Ok(())
    }

    fn get_causal(&self, cursor: &mut Cursor) -> anyhow::Result<Causal> {
        let mut causal = Causal::default();
        info!("get_causal: {:?}", self);
        match self {
            FromJs::Object(value) => {
                for kv in Object::entries(Object::try_from(value).unwrap()).iter() {
                    let arr = Array::from(&kv);
                    let mut a = arr.iter();
                    let key: FromJs = a.next().unwrap().into();
                    let mut here = cursor.clone();
                    key.traverse(&mut here, None)?;

                    let value: FromJs = a.next().unwrap().into();
                    causal.join(&value.get_causal(&mut here)?);
                }
            }
            FromJs::Array(arr) => {
                for (idx, value) in arr.iter().enumerate() {
                    let mut here = cursor.clone();
                    here.index(idx)?;
                    let value: FromJs = value.into();
                    causal.join(&value.get_causal(&mut here)?);
                }
            }
            FromJs::Function(_) => anyhow::bail!("Passed a function"),
            FromJs::String(str) => causal.join(&cursor.assign_str(str)?),
            FromJs::Number(f) => {
                if let ArchivedSchema::Reg(kind) = cursor.schema() {
                    match kind {
                        PrimitiveKind::U64 => causal.join(&cursor.assign_u64(*f as u64)?),
                        PrimitiveKind::I64 => causal.join(&cursor.assign_i64(*f as i64)?),
                        _ => anyhow::bail!("Not a Reg<u64|i64>"),
                    }
                } else {
                    anyhow::bail!("Not a Reg<_>");
                }
            }
            FromJs::Bool(b) => causal.join(&cursor.assign_bool(*b)?),
        };

        Ok(causal)
    }
}

#[derive(Clone, Debug)]
// TODO: unify with JsonPointer
enum CursorPath {
    KeyStr(String),
    Field(String),
    KeyU64(u64),
    KeyI64(i64),
    Index(usize),
}

impl CursorPath {
    fn traverse(&self, c: &mut Cursor) -> anyhow::Result<()> {
        match self {
            CursorPath::KeyStr(s) => c.key_str(s),
            CursorPath::Field(s) => c.field(s),
            CursorPath::KeyU64(n) => c.key_u64(*n),
            CursorPath::KeyI64(n) => c.key_i64(*n),
            CursorPath::Index(n) => c.index(*n),
        }?;
        Ok(())
    }
    fn traverse_vec(v: &[Self], c: &mut Cursor) -> anyhow::Result<()> {
        for p in v {
            p.traverse(c)?;
        }
        Ok(())
    }
}

impl ProxyHandler {
    fn new(doc: Rc<RefCell<Doc>>, path: Vec<CursorPath>) -> Result<Self, JsValue> {
        let doc_c = doc.clone();
        let ref_stack = Rc::new(RefCell::new(vec![]));
        let ref_stack_c = ref_stack.clone();

        let path_c = path.clone();
        let proxy_get = Closure::wrap(Box::new(move |obj: JsValue, prop: JsValue| {
            let prop: FromJs = prop.into();
            info!("proxy_get {:?} {:?} {:?}", obj, prop, path_c);
            if matches!(prop, FromJs::Array(_) | FromJs::Object(_)) {
                todo!("I think this should not happen, pending investigation");
            }

            let doc = doc_c.borrow_mut();
            let mut c = {
                let mut c = doc.cursor();
                CursorPath::traverse_vec(path_c.as_slice(), &mut c).map_err(map_err)?;
                c
            };
            let mut path = path_c.clone();
            prop.traverse(&mut c, Some(&mut path)).map_err(map_err)?;

            // Return the value or another proxy object when traversing
            match c.schema() {
                ArchivedSchema::Null => Ok(JsValue::undefined()),
                ArchivedSchema::Flag => Ok(JsValue::from(c.enabled().map_err(map_err)?)),
                ArchivedSchema::Reg(kind) => match kind {
                    PrimitiveKind::Bool => Ok(c
                        .bools()
                        .map_err(map_err)?
                        .next()
                        .transpose()
                        .map_err(map_err)?
                        .map(JsValue::from)
                        .unwrap_or_else(JsValue::undefined)),
                    PrimitiveKind::U64 => Ok(c
                        .u64s()
                        .map_err(map_err)?
                        .next()
                        .transpose()
                        .map_err(map_err)?
                        .map(JsValue::from)
                        .unwrap_or_else(JsValue::undefined)),

                    PrimitiveKind::I64 => Ok(c
                        .i64s()
                        .map_err(map_err)?
                        .next()
                        .transpose()
                        .map_err(map_err)?
                        .map(JsValue::from)
                        .unwrap_or_else(JsValue::undefined)),

                    PrimitiveKind::Str => Ok(c
                        .strs()
                        .map_err(map_err)?
                        .next()
                        .transpose()
                        .map_err(map_err)?
                        .map(JsValue::from)
                        .unwrap_or_else(JsValue::undefined)),
                },
                ArchivedSchema::Table(_, _)
                | ArchivedSchema::Array(_)
                | ArchivedSchema::Struct(_) => {
                    info!("returning new proxy!");
                    let handler = Self::new(doc_c.clone(), path)?;
                    let proxy = handler.proxy();
                    ref_stack_c.borrow_mut().push(handler);

                    Ok(proxy.into())
                }
            }
        })
            as Box<dyn Fn(JsValue, JsValue) -> Result<JsValue, JsValue>>);
        let doc_c = doc.clone();
        let path_c = path.clone();
        let proxy_set = Closure::wrap(Box::new(
            move |obj: JsValue, prop: JsValue, value: JsValue| {
                let prop: FromJs = prop.into();
                info!("proxy_set {:?} {:?} {:?} {:?}", obj, prop, value, path_c);
                let doc = doc_c.borrow_mut();

                let value: FromJs = value.into();
                let mut c = {
                    let mut c = doc.cursor();
                    CursorPath::traverse_vec(path_c.as_slice(), &mut c).map_err(map_err)?;
                    c
                };
                prop.traverse(&mut c, None).map_err(map_err)?;
                let causal = value.get_causal(&mut c).map_err(map_err)?;
                doc.apply(causal).map_err(map_err)?;
                Ok(true)
            },
        )
            as Box<dyn Fn(JsValue, JsValue, JsValue) -> Result<bool, JsValue>>);

        let proxy_delete = Closure::wrap(Box::new(move |obj: JsValue, prop: JsValue| {
            let prop: FromJs = prop.into();
            info!("proxy_delete {:?} {:?} {:?}", obj, prop, path);
            let doc = doc.borrow_mut();

            let mut c = {
                let mut c = doc.cursor();
                CursorPath::traverse_vec(path.as_slice(), &mut c).map_err(map_err)?;
                c
            };
            prop.traverse(&mut c, None).map_err(map_err)?;
            // TODO: Only do that if the prop existed
            let causal = c.remove().map_err(map_err)?;
            doc.apply(causal).map_err(map_err)?;
            Ok(true)
        })
            as Box<dyn Fn(JsValue, JsValue) -> Result<bool, JsValue>>);

        let handler = Object::new();
        Reflect::set(&handler, &"get".into(), proxy_get.as_ref())?;
        Reflect::set(&handler, &"set".into(), proxy_set.as_ref())?;
        Reflect::set(&handler, &"deleteProperty".into(), proxy_delete.as_ref())?;

        Ok(Self {
            get: proxy_get,
            handler,
            set: proxy_set,
            delete: proxy_delete,
            target: Object::new(),
            ref_stack,
        })
    }
}

#[wasm_bindgen]
impl DocWrapper {
    /// Returns the document identifier.
    pub fn id(&self) -> String {
        self.inner.id().to_string()
    }

    // TODO: add manual ts types for `f`
    pub fn change(&self, f: &js_sys::Function) -> Result<(), JsValue> {
        let proxy = ProxyHandler::new(Rc::new(RefCell::new(self.inner.clone())), vec![])?;

        f.call1(&JsValue::null(), &proxy.proxy())?;
        Ok(())
    }

    #[wasm_bindgen(js_name = "changePtr")]
    pub fn change_ptr(&self, ptr: &str, f: &js_sys::Function) -> Result<(), JsValue> {
        todo!()
    }

    pub fn subscribe(&self, ptr: &str, callback: js_sys::Function) {
        let ptr = JsonPointer::new(ptr).map_err(map_err).unwrap();
        let mut cursor = self.inner.cursor();
        ptr.goto(&mut cursor).unwrap();
        let mut sub = cursor.subscribe();
        let doc = self.inner.clone();
        wasm_bindgen_futures::spawn_local(async move {
            while let Some(_x) = sub.next().await {
                // TODO: get val from `_x`
                let mut c = doc.cursor();
                ptr.goto(&mut c).unwrap();
                let val = get_value(&mut c).expect("FIXME");
                if callback.call1(&JsValue::null(), &val).is_err() {
                    // TODO: Ownership?
                    break;
                }
            }
        });
    }

    #[wasm_bindgen(js_name = "getValue")]
    pub fn get_value(&self, ptr: &str) -> Result<JsValue, JsValue> {
        let ptr = JsonPointer::new(ptr).map_err(map_err)?;
        let mut cursor = self.inner.cursor();
        ptr.goto(&mut cursor).map_err(map_err)?;
        get_value(&mut cursor).map_err(map_err)
    }
}

fn map_err(err: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&format!("Error: {:#}", err)).into()
}

fn wrap<T>(f: impl FnOnce() -> anyhow::Result<T>) -> Result<T, JsValue> {
    f().map_err(map_err)
}

use libp2p::{multiaddr::Protocol, swarm::AddressScore, Multiaddr};
use log::*;
use tlfs::{
    libp2p_peer_id, Backend, Kind, Lens, Lenses, Package, PrimitiveKind, Ref, Sdk, ToLibp2pKeypair,
};
use wasm_bindgen::{prelude::*, JsCast};

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
    /// Creates a new SDK instance.
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
        _discovery_namespace: String,
        package: &[u8],
    ) -> anyhow::Result<Self> {
        // TODO: discovery!
        let backend = Backend::in_memory(package)?;
        let frontend = backend.frontend();
        let identity = frontend.default_keypair()?;
        let libp2p_identity = identity.to_libp2p();
        let libp2p_peer: libp2p::PeerId = libp2p_identity.public().into();
        let transport = mk_transport(libp2p_identity);
        let (inner, fut) = Sdk::new_with_transport(
            backend,
            frontend,
            identity.peer_id(),
            transport,
            std::iter::once(signaling_server.clone()),
        )
        .await?;
        wasm_bindgen_futures::spawn_local(fut);
        for mut b in bootstrap {
            if let Some(Protocol::P2p(peer)) = b.pop() {
                if let Ok(peer) = libp2p::PeerId::from_multihash(peer)
                    .map_err(|_| anyhow::anyhow!("Invalid peer id"))
                    .and_then(|x| libp2p_peer_id(&x))
                {
                    inner.add_address(peer, b);
                    continue;
                }
            }
            error!("Malformed bootstrap {:?}", b);
        }
        inner.add_external_address(
            signaling_server
                .with(Protocol::P2pWebRtcStar)
                .with(Protocol::P2p(libp2p_peer.into())),
            // TODO
            AddressScore::Infinite,
        );

        Ok(Self { inner })
    }

    /// Returns the Peer ID associated with this SDK.
    #[wasm_bindgen(js_name = "peerId")]
    pub fn peer_id(&self) -> JsPeerId {
        JsValue::from(self.inner.peer_id().to_string()).into()
    }

    /// Adds a multi`addr`ess for the given `peer`.
    #[wasm_bindgen(js_name = "addAddress")]
    pub fn add_address(&self, peer: String, addr: String) -> Result<(), JsValue> {
        wrap(|| {
            let peer = peer.parse()?;
            let addr = addr.parse()?;
            self.inner.add_address(peer, addr);
            Ok(())
        })
    }

    /// Removes a multi`addr`ess for the given `peer`.
    #[wasm_bindgen(js_name = "removeAddress")]
    pub fn remove_address(&self, peer: String, addr: String) -> Result<(), JsValue> {
        wrap(|| {
            let peer = peer.parse()?;
            let addr = addr.parse()?;
            self.inner.remove_address(peer, addr);
            Ok(())
        })
    }

    /// Returns all addresses the local node is listening one.
    #[wasm_bindgen]
    pub fn addresses(&self) -> PromiseStringArray {
        let f = self.inner.addresses();
        wasm_bindgen_futures::future_to_promise(async move {
            let array: js_sys::Array = f
                .await
                .into_iter()
                .map(|x| JsValue::from_str(&x.to_string()))
                .collect();
            Ok(array.unchecked_into())
        })
        .unchecked_into()
    }
}

#[wasm_bindgen(typescript_custom_section)]
const ADDITIONAL_TYLES: &'static str = r#"
 export type PeerId = string;
 "#;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(typescript_type = "PeerId")]
    pub type JsPeerId;
    #[wasm_bindgen(typescript_type = "Promise<Array<string>>")]
    pub type PromiseStringArray;
}

fn map_err(err: impl std::fmt::Display) -> JsValue {
    js_sys::Error::new(&format!("Error: {:#}", err)).into()
}

fn wrap<T>(f: impl FnOnce() -> anyhow::Result<T>) -> Result<T, JsValue> {
    f().map_err(map_err)
}

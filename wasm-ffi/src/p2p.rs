use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use futures_timer::Delay;
use instant::SystemTime;
use libp2p::{
    core::{
        self,
        either::EitherError,
        muxing::StreamMuxerBox,
        transport::{upgrade, Boxed},
    },
    futures::{
        channel::{mpsc, oneshot},
        pin_mut, select, stream, StreamExt,
    },
    gossipsub::{self, error::GossipsubHandlerError, GossipsubEvent},
    identify, identity, mplex,
    multiaddr::Protocol,
    noise,
    ping::{Ping, PingConfig, PingEvent, PingFailure},
    rendezvous,
    swarm::{AddressScore, SwarmBuilder, SwarmEvent},
    wasm_ext::{ffi, ExtTransport},
    yamux, Multiaddr, NetworkBehaviour, PeerId, Swarm, Transport,
};
use libp2p_webrtc::WebRtcTransport;
use log::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::{future_to_promise, spawn_local};

pub(crate) fn mk_transport(identity: identity::Keypair) -> Boxed<(PeerId, StreamMuxerBox)> {
    let peer_id = PeerId::from(identity.public());
    let webrtc = WebRtcTransport::new(peer_id, vec!["stun:stun.l.google.com:19302"]);
    let ws = ExtTransport::new(ffi::websocket_transport());
    let base = core::transport::OrTransport::new(webrtc, ws);
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&identity)
        .expect("Signing libp2p-noise static DH keypair failed.");

    base.upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(core::upgrade::SelectUpgrade::new(
            yamux::YamuxConfig::default(),
            mplex::MplexConfig::default(),
        ))
        .timeout(Duration::from_secs(20))
        .boxed()
}

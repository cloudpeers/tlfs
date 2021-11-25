use std::time::Duration;

use libp2p::{
    core::{
        self,
        muxing::StreamMuxerBox,
        transport::{upgrade, Boxed},
    },
    identity, mplex, noise,
    wasm_ext::{ffi, ExtTransport},
    yamux, PeerId, Transport,
};
use libp2p_webrtc::WebRtcTransport;

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

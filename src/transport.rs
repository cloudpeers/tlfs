use anyhow::Result;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    identity, PeerId,
};

pub fn transport(keypair: identity::Keypair) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {
    #[cfg(target_arch = "wasm32")]
    return wasm_transport(keypair);
    #[cfg(not(target_arch = "wasm32"))]
    return native_transport(keypair);
}

#[cfg(not(target_arch = "wasm32"))]
fn native_transport(keypair: identity::Keypair) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {
    use std::time::Duration;

    use libp2p::{
        core::upgrade::Version,
        noise::{self, NoiseConfig, X25519Spec},
        tcp::TcpConfig,
        yamux::YamuxConfig,
        Transport,
    };

    let tcp = TcpConfig::new().nodelay(true);
    let key = noise::Keypair::<X25519Spec>::new().into_authentic(&keypair)?;
    Ok(tcp
        .upgrade(Version::V1)
        .authenticate(NoiseConfig::xx(key).into_authenticated())
        .multiplex(YamuxConfig::default())
        .timeout(Duration::from_secs(20))
        .boxed())
}

#[cfg(target_arch = "wasm32")]
fn wasm_transport(identity: identity::Keypair) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {
    use std::time::Duration;

    use libp2p::{
        core::{self, transport::upgrade},
        noise,
        wasm_ext::{ffi, ExtTransport},
        yamux, Transport,
    };
    use libp2p_webrtc::WebRtcTransport;
    let peer_id = PeerId::from(identity.public());
    let webrtc = WebRtcTransport::new(peer_id, vec!["stun:stun.l.google.com:19302"]);
    let ws = ExtTransport::new(ffi::websocket_transport());
    let base = core::transport::OrTransport::new(webrtc, ws);
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&identity)?;

    Ok(base
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(yamux::YamuxConfig::default())
        .timeout(Duration::from_secs(20))
        .boxed())
}

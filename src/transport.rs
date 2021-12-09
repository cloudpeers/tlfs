use anyhow::Result;
use libp2p::core::identity;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::{Boxed, Transport};
use libp2p::core::upgrade::Version;
use libp2p::noise::{Keypair, NoiseConfig, X25519Spec};
use libp2p::tcp::TcpConfig;
use libp2p::yamux::YamuxConfig;
use libp2p::PeerId;
use std::time::Duration;

pub fn transport(keypair: identity::Keypair) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {
    if cfg!(target_family = "wasm") {
        panic!("unsupported");
    } else {
        native_transport(keypair)
    }
}

fn native_transport(keypair: identity::Keypair) -> Result<Boxed<(PeerId, StreamMuxerBox)>> {
    let tcp = TcpConfig::new().nodelay(true);
    let key = Keypair::<X25519Spec>::new().into_authentic(&keypair)?;
    Ok(tcp
        .upgrade(Version::V1)
        .authenticate(NoiseConfig::xx(key).into_authenticated())
        .multiplex(YamuxConfig::default())
        .timeout(Duration::from_secs(20))
        .boxed())
}

use std::{collections::BTreeSet, future::Future, sync::Arc, time::Duration};

use libp2p::{
    core::{self, upgrade::AuthenticationVersion},
    futures::{
        channel::{mpsc, oneshot},
        pin_mut, select, stream, StreamExt,
    },
    identity, mplex, noise,
    ping::{Ping, PingConfig, PingEvent},
    swarm::SwarmBuilder,
    yamux, Multiaddr, NetworkBehaviour, PeerId, Swarm, Transport,
};
use libp2p_webrtc::WebRtcTransport;
use log::{debug, info};
use parking_lot::Mutex;
use serde::Serialize;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::{future_to_promise, spawn_local};

#[derive(NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "Event")]
pub(crate) struct Behaviour {
    ping: Ping,
}
#[derive(Debug)]
pub(crate) enum Event {
    Ping(PingEvent),
}

impl From<PingEvent> for Event {
    fn from(event: PingEvent) -> Self {
        Event::Ping(event)
    }
}
enum SwarmCommand {
    Dial {
        addr: Multiaddr,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    GetInfo {
        tx: oneshot::Sender<anyhow::Result<SwarmInfo>>,
    },
    Shutdown,
}

#[derive(Debug, Serialize, Default, Clone)]
pub(crate) struct SwarmInfo {
    connected_peers: BTreeSet<Multiaddr>,
    own_addrs: BTreeSet<Multiaddr>,
}

#[derive(Clone)]
pub(crate) struct SwarmWrapper {
    tx: mpsc::Sender<SwarmCommand>,
}

impl std::fmt::Debug for SwarmWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SwarmWrapper")
    }
}

impl SwarmWrapper {
    pub(crate) async fn dial(&mut self, addr: Multiaddr) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.start_send(SwarmCommand::Dial { addr, tx })?;
        rx.await?
    }

    pub(crate) async fn info(&mut self) -> anyhow::Result<SwarmInfo> {
        let (tx, rx) = oneshot::channel();
        self.tx.start_send(SwarmCommand::GetInfo { tx })?;
        rx.await?
    }

    pub(crate) fn new(identity: identity::Keypair, signaling_server: Multiaddr) -> Self {
        let peer_id = PeerId::from(identity.public());
        let transport = {
            let base = WebRtcTransport::new(peer_id, vec!["stun:stun.l.google.com:19302"]);
            let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
                .into_authentic(&identity)
                .expect("Signing libp2p-noise static DH keypair failed.");

            base.upgrade()
                .authenticate_with_version(
                    noise::NoiseConfig::xx(noise_keys).into_authenticated(),
                    AuthenticationVersion::V1SimultaneousOpen,
                )
                .multiplex(core::upgrade::SelectUpgrade::new(
                    yamux::YamuxConfig::default(),
                    mplex::MplexConfig::default(),
                ))
                .timeout(Duration::from_secs(20))
                .boxed()
        };

        let mut swarm = SwarmBuilder::new(
            transport,
            Behaviour {
                ping: Ping::new(
                    PingConfig::new()
                        .with_interval(Duration::from_secs(1))
                        .with_keep_alive(true),
                ),
            },
            peer_id,
        )
        .executor(Box::new(|f| {
            spawn_local(f);
        }))
        .build();
        let (tx, mut rx) = mpsc::channel(256);
        let swarm_task = async move {
            // TODO: handle reconnects to signaling_server
            swarm.listen_on(signaling_server).expect("FIXME");
            let mut info = SwarmInfo::default();
            loop {
                select! {
                                command = rx.select_next_some() => {
                                    match command {
                                        SwarmCommand::Dial { addr, tx } => {
                                            let _ = tx.send(swarm.dial_addr(addr).map_err(Into::into));
                                        }
                                        SwarmCommand::GetInfo { tx } => {
                                            let _ = tx.send(Ok(info.clone()));
                                        }
                                        SwarmCommand::Shutdown => {
                                            info!("Shutting down swarm");
                                            break;
                                        }
                                    }
                                },
                                event = swarm.select_next_some()  => {
                                    match event {
                                        libp2p::swarm::SwarmEvent::NewListenAddr { address, ..} => {
                                            info.own_addrs.insert(address);
                                        },
                                        libp2p::swarm::SwarmEvent::ExpiredListenAddr { address, ..} => {
                                            info.own_addrs.remove(&address);
                                        },
                //                            libp2p::swarm::SwarmEvent::ConnectionClosed {} => {},
                                        ev => debug!("Unhandled swarm event: {:?}", ev)
                                    }
                                }
                            }
            }
            info!("Terminating swarm loop");
            Ok(JsValue::NULL)
        };
        // Leak the promise
        let _ = future_to_promise(swarm_task);
        SwarmWrapper { tx }
    }
}

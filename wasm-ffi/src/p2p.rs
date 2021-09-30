use std::{collections::BTreeSet, io, time::Duration};

use futures_timer::Delay;
use libp2p::{
    core::{self, either::EitherError, upgrade::AuthenticationVersion},
    futures::{
        channel::{mpsc, oneshot},
        pin_mut, select, stream, StreamExt,
    },
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
use serde::Serialize;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::{future_to_promise, spawn_local};

#[derive(NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "Event")]
pub(crate) struct Behaviour {
    ping: Ping,
    rendezvous: rendezvous::client::Behaviour,
    identify: identify::Identify,
}
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum Event {
    Ping(PingEvent),
    Rendezvous(rendezvous::client::Event),
    Identify(identify::IdentifyEvent),
}

impl From<PingEvent> for Event {
    fn from(event: PingEvent) -> Self {
        Event::Ping(event)
    }
}
impl From<rendezvous::client::Event> for Event {
    fn from(event: rendezvous::client::Event) -> Self {
        Event::Rendezvous(event)
    }
}
impl From<identify::IdentifyEvent> for Event {
    fn from(e: identify::IdentifyEvent) -> Self {
        Event::Identify(e)
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

impl Drop for SwarmWrapper {
    fn drop(&mut self) {
        let _ = self.tx.start_send(SwarmCommand::Shutdown);
    }
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

    pub(crate) fn new(
        identity: identity::Keypair,
        signaling_server: Multiaddr,
        bootstrap: Vec<Multiaddr>,
        discovery_namespace: String,
    ) -> Self {
        let peer_id = PeerId::from(identity.public());
        let transport = {
            let webrtc = WebRtcTransport::new(peer_id, vec!["stun:stun.l.google.com:19302"]);
            let ws = ExtTransport::new(ffi::websocket_transport());
            let base = core::transport::OrTransport::new(webrtc, ws);
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
                identify: identify::Identify::new(identify::IdentifyConfig::new(
                    "tlfs-rendezvous".into(),
                    identity.public(),
                )),
                rendezvous: rendezvous::client::Behaviour::new(identity),
            },
            peer_id,
        )
        .executor(Box::new(|f| {
            spawn_local(f);
        }))
        .build();
        let (tx, mut rx) = mpsc::channel(256);
        let swarm_task = async move {
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
            let ticker = stream::unfold(true, |is_first| async move {
                Delay::new(Duration::from_secs(if is_first { 5 } else { 60 })).await;
                Some(((), false))
            })
            .fuse();
            pin_mut!(ticker);
            let mut state = State {
                swarm,
                info: SwarmInfo::default(),
                discovery_token: None,
                bootstrap,
                discovery_namespace: rendezvous::Namespace::new(discovery_namespace).unwrap(),
            };
            loop {
                select! {
                    command = rx.select_next_some() => {
                        if !state.handle_command(command) {
                            break;
                        }
                    },
                    event = state.swarm.select_next_some()  => {
                        state.handle_swarm_event(event);
                    },
                    _ = ticker.next() => {
                        state.discover_peers();
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

struct State {
    info: SwarmInfo,
    discovery_token: Option<rendezvous::Cookie>,
    swarm: Swarm<Behaviour>,
    bootstrap: Vec<Multiaddr>,
    discovery_namespace: rendezvous::Namespace,
}
impl State {
    fn handle_command(&mut self, cmd: SwarmCommand) -> bool {
        match cmd {
            SwarmCommand::Dial { addr, tx } => {
                let _ = tx.send(self.swarm.dial_addr(addr).map_err(Into::into));
            }
            SwarmCommand::GetInfo { tx } => {
                let _ = tx.send(Ok(self.info.clone()));
            }
            SwarmCommand::Shutdown => {
                info!("Shutting down swarm");
                return false;
            }
        }
        true
    }
    fn handle_swarm_event(
        &mut self,
        event: SwarmEvent<Event, EitherError<EitherError<PingFailure, void::Void>, io::Error>>,
    ) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                self.info.own_addrs.insert(address);
            }
            SwarmEvent::ExpiredListenAddr { address, .. } => {
                self.info.own_addrs.remove(&address);
            }
            SwarmEvent::Behaviour(Event::Identify(identify::IdentifyEvent::Received {
                ..
            })) => {
                for p in self.get_bootstrap_peers() {
                    debug!("Register rendezvous with {}", p);
                    self.swarm.behaviour_mut().rendezvous.register(
                        self.discovery_namespace.clone(),
                        p,
                        None,
                    );
                }
            }
            SwarmEvent::Behaviour(Event::Rendezvous(rendezvous::client::Event::Discovered {
                registrations,
                cookie,
                ..
            })) => {
                self.discovery_token.replace(cookie);
                for r in registrations {
                    let peer = r.record.peer_id();
                    info!("Discovered new peer {} ({:?})", peer, r.record.addresses());
                    for a in r.record.addresses() {
                        let p2p_suffix = Protocol::P2p(*peer.as_ref());
                        let a_with_p2p =
                            if !a.ends_with(&Multiaddr::empty().with(p2p_suffix.clone())) {
                                a.clone().with(p2p_suffix)
                            } else {
                                a.clone()
                            };
                        let _ = self.swarm.dial_addr(a_with_p2p);
                    }
                }
            }

            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                info!("Connection established to {}", peer_id)
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                info!("Connection closed to {} ({:?}", peer_id, cause)
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                error!("Listener error: {:?} {}", listener_id, error);
            }
            ev => debug!("Unhandled swarm event: {:?}", ev),
        }
    }
    fn discover_peers(&mut self) {
        for p in self.get_bootstrap_peers() {
            self.swarm.behaviour_mut().rendezvous.discover(
                Some(self.discovery_namespace.clone()),
                self.discovery_token.as_ref().cloned(),
                None,
                p,
            );
        }
    }
    fn get_bootstrap_peers(&self) -> Vec<PeerId> {
        self.bootstrap
            .iter()
            .filter_map(|m| {
                m.iter().find_map(|x| {
                    if let Protocol::P2p(p) = x {
                        PeerId::from_multihash(p).ok()
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
}

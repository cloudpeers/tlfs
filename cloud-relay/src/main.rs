use std::{
    fs,
    io::{self, BufReader},
    path::PathBuf,
    time::Duration,
};

use anyhow::Context;
use clap::{AppSettings, Clap};
use libp2p::{
    core::{
        either::EitherTransport, muxing::StreamMuxerBox, transport::OrTransport,
        upgrade::AuthenticationVersion,
    },
    dns::TokioDnsConfig,
    futures::StreamExt,
    identity,
    multiaddr::Protocol,
    noise,
    pnet::{PnetConfig, PreSharedKey},
    relay::{self, Relay},
    rendezvous,
    swarm::SwarmBuilder,
    tcp::TokioTcpConfig,
    websocket::{self, WsConfig},
    yamux::YamuxConfig,
    Multiaddr, NetworkBehaviour, Swarm, Transport,
};
use tracing::*;
use tracing_subscriber::fmt;

mod acme;

#[derive(Clap)]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(long, default_value = "priv.key")]
    /// Path to a private key file. Will be created if it doesn't exist.
    libp2p_private_key: PathBuf,
    #[clap(long, default_value = "tls_priv.pkcs8")]
    /// Path to a TLS private key file. Will be created/request through LetsEncrypt if it doesn't
    /// exist.
    tls_private_key: PathBuf,
    #[clap(long, default_value = "tls_cert.pkcs8")]
    /// Path to a TLS cert file. Will be created/request through LetsEncrypt if it doesn't exist.
    tls_cert: PathBuf,
    #[clap(long)]
    /// Domain to be used to request a TLS certificate for. Required if `wss` is set`.
    tls_domain: Option<String>,
    #[clap(long)]
    /// E-Mail to be used to request a TLS certificate with. Required if `wss` is set`.
    tls_email: Option<String>,
    #[clap(long)]
    wss: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    fmt::init();
    let private_key = if opts.libp2p_private_key.is_file() {
        let mut p = hex::decode(fs::read_to_string(&opts.libp2p_private_key)?)?;
        let r =
            identity::ed25519::SecretKey::from_bytes(&mut p[..]).context("Parsing private key")?;
        info!("Read private key from file");
        r
    } else {
        let p = identity::ed25519::SecretKey::generate();
        fs::write(&opts.libp2p_private_key, hex::encode(p.as_ref()))?;
        info!("Wrote private key to {}", opts.libp2p_private_key.display());
        p
    };
    let kp = identity::Keypair::Ed25519(private_key.into());

    let tls = if opts.wss {
        if let (Some(email), Some(domain)) = (opts.tls_email, opts.tls_domain) {
            if !opts.tls_cert.is_file() {
                info!("Certificate doesn't exist, requesting one via HTTP challenge.");
                acme::get_cert(domain, email, &opts.tls_cert, &opts.tls_private_key).await?;
            }

            let der = rustls::internal::pemfile::pkcs8_private_keys(&mut BufReader::new(
                fs::File::open(&opts.tls_private_key)?,
            ))
            .map_err(|_| anyhow::anyhow!("Reading TLS private key"))?
            .into_iter()
            .next()
            .context("Extracting private key")?;
            let private_key = websocket::tls::PrivateKey::new(der.0);

            let certs = rustls::internal::pemfile::certs(&mut BufReader::new(fs::File::open(
                &opts.tls_cert,
            )?))
            .map_err(|_| anyhow::anyhow!("Reading TLS cert chain"))?
            .into_iter()
            .map(|x| websocket::tls::Certificate::new(x.0))
            .collect();
            Some((private_key, certs))
        } else {
            anyhow::bail!("Please provide `cert,` `private_key`, `domain`, and `email` options");
        }
    } else {
        None
    };
    let mut swarm = build_swarm(kp, None, Duration::from_secs(10), tls)
        .await
        .context("Creating libp2p swarm")?;

    let ws_or_wss = if opts.wss {
        Protocol::Wss("/".into())
    } else {
        Protocol::Ws("/".into())
    };
    for base in &["/ip4/0.0.0.0", "/ip6/::0"] {
        let m: Multiaddr = base.parse().unwrap();
        let tcp = m.clone().with(Protocol::Tcp(4001));
        let ws = m.with(Protocol::Tcp(4002)).with(ws_or_wss.clone());
        swarm.listen_on(tcp)?;
        swarm.listen_on(ws)?;
    }
    while let Some(event) = swarm.next().await {
        debug!("Swarm {:?}", event);
    }
    Ok(())
}
#[derive(Debug)]
enum Event {
    RelayEvent,
    Rendezvous(rendezvous::server::Event),
}
impl From<()> for Event {
    fn from(_: ()) -> Self {
        Self::RelayEvent
    }
}
impl From<rendezvous::server::Event> for Event {
    fn from(event: rendezvous::server::Event) -> Self {
        Event::Rendezvous(event)
    }
}
#[derive(NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "Event")]
struct Behaviour {
    relay: Relay,
    rendezvous: rendezvous::server::Behaviour,
}

async fn build_swarm(
    key_pair: identity::Keypair,
    psk: Option<PreSharedKey>,
    upgrade_timeout: Duration,
    tls: Option<(websocket::tls::PrivateKey, Vec<websocket::tls::Certificate>)>,
) -> anyhow::Result<Swarm<Behaviour>> {
    let tcp_base = {
        let tcp = TokioTcpConfig::new().nodelay(true);
        TokioDnsConfig::system(tcp).context("Creating TokioDnsConfig")?
    };
    let ws_base = {
        let tcp = TokioTcpConfig::new().nodelay(true);
        let mut ws = WsConfig::new(tcp);
        ws.use_deflate(true);
        if let Some((tls_key, tls_certs)) = tls {
            info!("Setting up WebSocket with TLS");
            ws.set_tls_config(
                websocket::tls::Config::new(tls_key, tls_certs).context("Creating TLS Config")?,
            );
        }
        ws
    };
    let base_transport = OrTransport::new(tcp_base, ws_base);

    let base_transport = match psk {
        Some(psk) => EitherTransport::Left(
            base_transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
        ),
        None => EitherTransport::Right(base_transport),
    };
    // TODO: think about idle timeout for relayed connections
    let (transport, relay_behaviour) =
        relay::new_transport_and_behaviour(Default::default(), base_transport);
    let xx_keypair = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&key_pair)
        .unwrap();
    let noise_config = noise::NoiseConfig::xx(xx_keypair).into_authenticated();
    let yamux_config = YamuxConfig::default();
    let transport = transport
        .upgrade()
        .authenticate_with_version(noise_config, AuthenticationVersion::V1SimultaneousOpen)
        .multiplex(yamux_config)
        .timeout(upgrade_timeout)
        .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
        .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
        .boxed();

    Ok(SwarmBuilder::new(
        transport,
        Behaviour {
            relay: relay_behaviour,
            rendezvous: rendezvous::server::Behaviour::new(rendezvous::server::Config::default()),
        },
        key_pair.public().into(),
    )
    .executor(Box::new(|f| {
        tokio::spawn(f);
    }))
    .build())
}

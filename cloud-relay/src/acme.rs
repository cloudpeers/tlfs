use libp2p::futures::channel::oneshot;
use std::path::Path;
use tracing::*;
use warp::Filter;

/// Tries to retrieve a TLS certificate through LetsEncrypt using the http-challenge. This means
/// that an ephemeral http server is spawned on port 80 to serve the requested token. If
/// successful, the certificate and the private key are stored in the given paths.
pub(crate) async fn get_cert(
    domain: String,
    email: String,
    certificate_file: impl AsRef<Path>,
    private_key_file: impl AsRef<Path>,
) -> anyhow::Result<()> {
    let url = if cfg!(debug_assertions) {
        acme_lib::DirectoryUrl::LetsEncryptStaging
    } else {
        acme_lib::DirectoryUrl::LetsEncrypt
    };

    let persist = acme_lib::persist::FilePersist::new(".");

    // Create a directory entrypoint.
    let dir = acme_lib::Directory::from_url(persist, url)?;
    let acc = dir.account(&email)?;

    let mut ord_new = acc.new_order(&domain, &[])?;
    let ord_csr = loop {
        // are we done?
        if let Some(ord_csr) = ord_new.confirm_validations() {
            break ord_csr;
        }

        let auths = ord_new.authorizations()?;
        let chall = auths[0].http_challenge();
        let token = chall.http_token().to_string();
        let proof = chall.http_proof();

        info!(%token, %proof, "Serving acme-challenge");
        let token = warp::get()
            .and(warp::path!(".well-known" / "acme-challenge" / String))
            .map(move |_| {
                info!("Challenge served.");
                proof.clone()
            });
        let (tx80, rx80) = oneshot::channel();
        tokio::spawn(
            warp::serve(token)
                .bind_with_graceful_shutdown(([0, 0, 0, 0], 80), async {
                    rx80.await.ok();
                })
                .1,
        );

        chall.validate(5000)?;
        info!("Validated!");
        tx80.send(()).unwrap();

        ord_new.refresh()?;
    };

    let pkey_pri = acme_lib::create_p384_key();
    let ord_cert = ord_csr.finalize_pkey(pkey_pri, 5000)?;

    let cert = ord_cert.download_and_save_cert()?;
    info!("Received certificate");

    std::fs::write(&certificate_file, cert.certificate())?;
    std::fs::write(&private_key_file, cert.private_key())?;
    info!(
        "Stored certificate / private key to {} / {}",
        certificate_file.as_ref().display(),
        private_key_file.as_ref().display()
    );
    Ok(())
}

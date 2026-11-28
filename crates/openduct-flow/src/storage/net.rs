// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Connections the storage steps open themselves (SFTP, FTP, Redis): to the internet's addresses only, unless the
//! installation allows its own network (`--allow-private-network`) - the rule the HTTP step keeps - with the address
//! checked once it is resolved and that very address connected to, so a name that resolves differently the second
//! time leads nowhere new. TLS trusts what the operating system trusts, or only the CA a credential gives.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls_platform_verifier::BuilderVerifierExt;
use tokio::net::TcpStream;

/// How long a connection may take to open.
pub(crate) const CONNECT_TIME: Duration = Duration::from_secs(15);

/// A host as a credential names it: a name or an address, nothing else.
pub(crate) fn host_ok(host: &str) -> Result<(), String> {
    let fine = !host.is_empty()
        && host.len() <= 253
        && host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']' | b'_'));
    if fine {
        Ok(())
    } else {
        Err(format!(
            "{host:?} is not a host: a name or an address, as files.example.com"
        ))
    }
}

/// A connection to `host`, on the internet unless `allow_private`.
pub(crate) async fn connect(
    host: &str,
    port: u16,
    allow_private: bool,
) -> Result<(TcpStream, SocketAddr), String> {
    host_ok(host)?;
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    let found: Vec<SocketAddr> = tokio::time::timeout(CONNECT_TIME, tokio::net::lookup_host((bare, port)))
        .await
        .map_err(|_| format!("cannot find {host} in time"))?
        .map_err(|e| format!("cannot find {host}: {e}"))?
        .collect();
    if found.is_empty() {
        return Err(format!("cannot find {host}"));
    }
    if !allow_private && found.iter().any(|a| crate::private(a.ip())) {
        return Err(format!(
            "{host} is on this machine's own network: start OpenDuct with --allow-private-network to let workflows \
             reach it"
        ));
    }
    let mut last = String::new();
    for address in found.iter().take(4) {
        match tokio::time::timeout(CONNECT_TIME, TcpStream::connect(address)).await {
            Ok(Ok(stream)) => {
                let _ = stream.set_nodelay(true);
                return Ok((stream, *address));
            }
            Ok(Err(e)) => last = format!("cannot reach {host}:{port}: {e}"),
            Err(_) => last = format!("{host}:{port} did not answer in time"),
        }
    }
    Err(last)
}

/// A connection to the address another connection was made to, on another port (an FTP server's data
/// connection): the server's own words on where to connect are not taken.
pub(crate) async fn connect_again(to: SocketAddr, port: u16) -> Result<TcpStream, String> {
    let address = SocketAddr::new(to.ip(), port);
    match tokio::time::timeout(CONNECT_TIME, TcpStream::connect(address)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(e)) => Err(format!("cannot open the data connection: {e}")),
        Err(_) => Err("the data connection did not open in time".to_owned()),
    }
}

/// TLS that trusts what the operating system trusts - or, when the credential gives one, only its CA (PEM).
pub(crate) fn tls_config(ca: Option<&str>) -> Result<Arc<rustls::ClientConfig>, String> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let builder =
        rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("cannot set up TLS: {e}"))?;
    let builder = match ca.map(str::trim).filter(|c| !c.is_empty()) {
        Some(pem) => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                let cert = cert.map_err(|_| "ca: not PEM certificates".to_owned())?;
                roots
                    .add(cert)
                    .map_err(|e| format!("ca: a certificate was not accepted: {e}"))?;
            }
            if roots.is_empty() {
                return Err("ca: holds no certificate".to_owned());
            }
            builder.with_root_certificates(roots)
        }
        None => builder
            .with_platform_verifier()
            .map_err(|e| format!("cannot set up TLS: {e}"))?,
    };
    Ok(Arc::new(builder.with_no_client_auth()))
}

/// `stream` over TLS, the server checked to be `host`.
pub(crate) async fn tls<S>(
    stream: S,
    config: &Arc<rustls::ClientConfig>,
    host: &str,
) -> Result<tokio_rustls::client::TlsStream<S>, String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let bare = host.trim_start_matches('[').trim_end_matches(']').to_owned();
    let name = ServerName::try_from(bare).map_err(|_| format!("{host} is not a name TLS can check"))?;
    let connector = tokio_rustls::TlsConnector::from(Arc::clone(config));
    match tokio::time::timeout(CONNECT_TIME, connector.connect(name, stream)).await {
        Ok(Ok(s)) => Ok(s),
        Ok(Err(e)) => Err(
            match e
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
            {
                Some(rustls::Error::InvalidCertificate(why)) => {
                    format!(
                        "{host}'s certificate was not accepted: {why:?} (give its CA in the credential's ca)"
                    )
                }
                _ => format!("TLS with {host} failed: {e}"),
            },
        ),
        Err(_) => Err(format!("TLS with {host} did not start in time")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_are_names_or_addresses() {
        assert!(
            host_ok("files.example.com").is_ok() && host_ok("10.0.0.5").is_ok() && host_ok("[::1]").is_ok()
        );
        for bad in ["", "a b", "x/y", "user@host", "host\r\nX", &"a".repeat(254)] {
            assert!(host_ok(bad).is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn this_machines_network_is_refused_unless_allowed() {
        let err = connect("127.0.0.1", 9, false).await.err().unwrap_or_default();
        assert!(err.contains("--allow-private-network"), "{err}");
        let err = connect("localhost", 9, false).await.err().unwrap_or_default();
        assert!(err.contains("--allow-private-network"), "{err}");
        assert!(tls_config(Some("not a certificate")).is_err());
        assert!(tls_config(None).is_ok());
    }
}

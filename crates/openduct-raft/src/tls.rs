// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! TLS between brokers: every replication connection encrypted, and both ends proven. Each broker holds a
//! certificate signed by the cluster's own certificate authority and the authority's certificate; a broker accepts
//! a connection only from one whose certificate that authority signed, and when connecting it checks the other's
//! certificate names the address it dialled. TLS 1.3 only.

use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// The TLS settings of a broker's replication connections, both ways.
#[derive(Clone)]
pub struct ReplicationTls {
    pub(crate) client: Arc<ClientConfig>,
    pub(crate) server: Arc<ServerConfig>,
}

impl std::fmt::Debug for ReplicationTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationTls")
    }
}

impl ReplicationTls {
    /// From PEM files: this broker's certificate (its chain, leaf first), its private key, and the certificate of
    /// the authority that signs every broker's.
    pub fn from_pem_files(cert: &Path, key: &Path, ca: &Path) -> Result<Self, String> {
        let read = |p: &Path| std::fs::read(p).map_err(|e| format!("cannot read {}: {e}", p.display()));
        let (cert_pem, key_pem, ca_pem) = (read(cert)?, read(key)?, read(ca)?);
        Self::from_pem(&cert_pem, &key_pem, &ca_pem)
    }

    pub fn from_pem(cert: &[u8], key: &[u8], ca: &[u8]) -> Result<Self, String> {
        let chain = CertificateDer::pem_slice_iter(cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("the broker's certificate: {e}"))?;
        if chain.is_empty() {
            return Err("the broker's certificate file holds no certificate".to_owned());
        }
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| format!("the broker's private key: {e}"))?;
        let mut roots = RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(ca) {
            let c = c.map_err(|e| format!("the authority's certificate: {e}"))?;
            roots
                .add(c)
                .map_err(|e| format!("the authority's certificate: {e}"))?;
        }
        if roots.is_empty() {
            return Err("the authority's certificate file holds no certificate".to_owned());
        }
        let roots = Arc::new(roots);
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::clone(&roots), Arc::clone(&provider))
            .build()
            .map_err(|e| format!("cannot check other brokers' certificates: {e}"))?;
        let server = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| e.to_string())?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(|e| format!("the broker's certificate and key: {e}"))?;
        let client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .map_err(|e| format!("the broker's certificate and key: {e}"))?;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}

/// The name a certificate must carry to be the broker at `addr` (`host:port`): its host, a name or an address.
pub(crate) fn server_name(addr: &str) -> std::io::Result<ServerName<'static>> {
    let host = addr
        .rsplit_once(':')
        .map_or(addr, |(h, _)| h)
        .trim_start_matches('[')
        .trim_end_matches(']');
    ServerName::try_from(host.to_owned())
        .map_err(|e| std::io::Error::other(format!("{addr} has no host a certificate can name: {e}")))
}

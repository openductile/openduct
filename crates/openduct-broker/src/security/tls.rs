// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! TLS on the client port: TLS 1.3 only. With an authority for client certificates, a client may prove who it is
//! with its certificate: its principal is the certificate's first URI name (a workload identity such as
//! `spiffe://example.org/hub`) or else its first DNS name - never the subject's common name. Client certificates can
//! be required (mutual TLS for every client) or optional (clients without one sign in with SASL).

use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;

/// Client certificates: none asked for, accepted when offered, or required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCerts {
    None,
    Optional,
    Required,
}

/// TLS for the client port.
#[derive(Clone)]
pub struct ClientTls {
    pub(crate) acceptor: tokio_rustls::TlsAcceptor,
    pub(crate) client_certs: ClientCerts,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientTls(client certificates {:?})", self.client_certs)
    }
}

impl ClientTls {
    /// The broker's certificate chain and key; with `client_ca`, the authority whose certificates identify clients.
    pub fn from_pem_files(
        cert: &Path,
        key: &Path,
        client_ca: Option<&Path>,
        client_certs: ClientCerts,
    ) -> Result<Self, String> {
        let read = |p: &Path| std::fs::read(p).map_err(|e| format!("cannot read {}: {e}", p.display()));
        let ca = client_ca.map(read).transpose()?;
        Self::from_pem(&read(cert)?, &read(key)?, ca.as_deref(), client_certs)
    }

    pub fn from_pem(
        cert: &[u8],
        key: &[u8],
        client_ca: Option<&[u8]>,
        client_certs: ClientCerts,
    ) -> Result<Self, String> {
        let chain = CertificateDer::pem_slice_iter(cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("the broker's certificate: {e}"))?;
        if chain.is_empty() {
            return Err("the broker's certificate file holds no certificate".to_owned());
        }
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| format!("the broker's private key: {e}"))?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(|e| e.to_string())?;
        let config = match (client_certs, client_ca) {
            (ClientCerts::None, _) => builder.with_no_client_auth(),
            (_, None) => return Err("client certificates need the authority that signs them".to_owned()),
            (mode, Some(ca)) => {
                let mut roots = rustls::RootCertStore::empty();
                for c in CertificateDer::pem_slice_iter(ca) {
                    roots
                        .add(c.map_err(|e| format!("the client authority: {e}"))?)
                        .map_err(|e| format!("the client authority: {e}"))?;
                }
                if roots.is_empty() {
                    return Err("the client authority file holds no certificate".to_owned());
                }
                let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
                let verifier = if mode == ClientCerts::Optional {
                    verifier.allow_unauthenticated()
                } else {
                    verifier
                };
                builder.with_client_cert_verifier(verifier.build().map_err(|e| e.to_string())?)
            }
        };
        let config = config
            .with_single_cert(chain, key)
            .map_err(|e| format!("the broker's certificate and key: {e}"))?;
        Ok(Self {
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(config)),
            client_certs,
        })
    }
}

/// The principal name a verified client certificate carries: its first URI name, else its first DNS name.
pub(crate) fn certificate_name(cert: &CertificateDer<'_>) -> Option<String> {
    let parsed = webpki::EndEntityCert::try_from(cert).ok()?;
    let name = parsed
        .valid_uri_names()
        .next()
        .or_else(|| parsed.valid_dns_names().next())?;
    (!name.is_empty() && name.len() <= 256).then(|| name.to_owned())
}

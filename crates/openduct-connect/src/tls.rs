// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! TLS for the connectors that open their own connections (MQTT, Kafka): the settings `ca`, `client_cert` and
//! `client_key`, and what a failed handshake means.

use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_platform_verifier::BuilderVerifierExt;

use crate::settings::Settings;

/// TLS that trusts what the operating system trusts, or only the CA given in `ca`; with a client certificate
/// when one is given, as AWS IoT Core, many factory
/// brokers and some Kafka clusters ask for.
pub(crate) fn config(s: &Settings<'_>) -> Result<rustls::ClientConfig, String> {
    let builder =
        rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("cannot set up TLS: {e}"))?;
    let checking = s.checking();
    let builder = match s.text("ca")? {
        Some(pem) if !checking => {
            let mut roots = rustls::RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(pem.as_bytes()) {
                let cert = cert.map_err(|_| format!("{}: is not PEM certificates", s.path("ca")))?;
                roots
                    .add(cert)
                    .map_err(|e| format!("{}: a certificate was not accepted: {e}", s.path("ca")))?;
            }
            if roots.is_empty() {
                return Err(format!("{}: holds no certificate", s.path("ca")));
            }
            builder.with_root_certificates(roots)
        }
        _ => builder
            .with_platform_verifier()
            .map_err(|e| format!("cannot set up TLS: {e}"))?,
    };
    match (s.text("client_cert")?, s.text("client_key")?) {
        (None, None) => Ok(builder.with_no_client_auth()),
        (Some(_), Some(_)) if checking => Ok(builder.with_no_client_auth()),
        (Some(cert), Some(key)) => {
            let certs = CertificateDer::pem_slice_iter(cert.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| format!("{}: is not a PEM certificate", s.path("client_cert")))?;
            let key = PrivateKeyDer::from_pem_slice(key.as_bytes())
                .map_err(|_| format!("{}: is not a PEM private key", s.path("client_key")))?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| format!("{}: {e}", s.path("client_cert")))
        }
        _ => Err(format!(
            "{} and {} go together",
            s.path("client_cert"),
            s.path("client_key")
        )),
    }
}

/// Why a TLS handshake with `what` failed, and whether it will fail the same way again (a certificate that is not
/// accepted) or may not (the network).
pub(crate) fn failure(e: &std::io::Error, what: &str) -> (bool, String) {
    match e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<rustls::Error>())
    {
        Some(rustls::Error::InvalidCertificate(why)) => (
            true,
            format!("{what}'s certificate was not accepted: {why:?} (give its CA in ca)"),
        ),
        Some(other) => (true, format!("TLS with {what} failed: {other}")),
        None => (false, format!("TLS with {what} failed: {e}")),
    }
}

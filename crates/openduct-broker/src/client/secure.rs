// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The client side of the broker's zero trust: TLS 1.3 (checking the broker's certificate against an authority,
//! optionally presenting the client's own), then signing in - SCRAM (which also proves the broker holds the
//! credential, so a broker that only pretends is caught), PLAIN over TLS, or an OAUTHBEARER token.

use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use kafka_protocol::messages::{SaslAuthenticateRequest, SaslHandshakeRequest};
use kafka_protocol::protocol::StrBytes;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use super::{Client, ClientError};
use crate::security::scram::{self, ScramMechanism};

/// How a client signs in.
#[derive(Clone)]
pub enum SignIn {
    Scram {
        mechanism: ScramMechanism,
        user: String,
        password: String,
    },
    Plain {
        user: String,
        password: String,
    },
    Token(String),
}

impl std::fmt::Debug for SignIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scram { mechanism, user, .. } => write!(f, "{} as {user}", mechanism.name()),
            Self::Plain { user, .. } => write!(f, "PLAIN as {user}"),
            Self::Token(_) => f.write_str("OAUTHBEARER"),
        }
    }
}

/// TLS and sign-in for a client connection.
#[derive(Clone, Default)]
pub struct ClientSecurity {
    pub tls: Option<Arc<rustls::ClientConfig>>,
    /// The name the broker's certificate must carry, when not the host connected to.
    pub server_name: Option<String>,
    pub sign_in: Option<SignIn>,
}

impl std::fmt::Debug for ClientSecurity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClientSecurity(tls: {}, sign-in: {:?})",
            self.tls.is_some(),
            self.sign_in
        )
    }
}

impl ClientSecurity {
    /// TLS 1.3 trusting `ca` (PEM); with `identity` (certificate chain and key, PEM), the client presents it.
    pub fn tls_from_pem(
        ca: &[u8],
        identity: Option<(&[u8], &[u8])>,
    ) -> Result<Arc<rustls::ClientConfig>, String> {
        let mut roots = rustls::RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(ca) {
            roots
                .add(c.map_err(|e| format!("the authority: {e}"))?)
                .map_err(|e| format!("the authority: {e}"))?;
        }
        if roots.is_empty() {
            return Err("the authority file holds no certificate".to_owned());
        }
        let builder =
            rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(|e| e.to_string())?
                .with_root_certificates(roots);
        let config = match identity {
            None => builder.with_no_client_auth(),
            Some((cert, key)) => {
                let chain = CertificateDer::pem_slice_iter(cert)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| format!("the client certificate: {e}"))?;
                let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| format!("the client key: {e}"))?;
                builder
                    .with_client_auth_cert(chain, key)
                    .map_err(|e| format!("the client certificate and key: {e}"))?
            }
        };
        Ok(Arc::new(config))
    }
}

fn refused(what: &str, code: i16, message: Option<&StrBytes>) -> ClientError {
    ClientError::Refused {
        code,
        message: Some(match message {
            Some(m) if !m.is_empty() => format!("{what}: {m}"),
            _ => format!("{what} (error code {code})"),
        }),
    }
}

impl Client {
    /// Signs in on this connection.
    pub(super) async fn sign_in(&mut self, how: &SignIn) -> Result<(), ClientError> {
        let mechanism = match how {
            SignIn::Scram { mechanism, .. } => mechanism.name(),
            SignIn::Plain { .. } => "PLAIN",
            SignIn::Token(_) => "OAUTHBEARER",
        };
        let hs = self
            .send(
                &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str(mechanism)),
                1,
            )
            .await?;
        if hs.error_code != 0 {
            let offered: Vec<String> = hs.mechanisms.iter().map(ToString::to_string).collect();
            return Err(ClientError::Refused {
                code: hs.error_code,
                message: Some(format!(
                    "the broker does not offer {mechanism} (it offers {})",
                    offered.join(", ")
                )),
            });
        }
        match how {
            SignIn::Plain { user, password } => {
                self.authenticate(format!("\0{user}\0{password}").into_bytes())
                    .await?;
            }
            SignIn::Token(token) => {
                self.authenticate(format!("n,,\x01auth=Bearer {token}\x01\x01").into_bytes())
                    .await?;
            }
            SignIn::Scram {
                mechanism,
                user,
                password,
            } => self.scram(*mechanism, user, password).await?,
        }
        Ok(())
    }

    async fn authenticate(&mut self, bytes: Vec<u8>) -> Result<Bytes, ClientError> {
        let res = self
            .send(
                &SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(bytes)),
                2,
            )
            .await?;
        if res.error_code != 0 {
            return Err(refused(
                "sign-in refused",
                res.error_code,
                res.error_message.as_ref(),
            ));
        }
        Ok(res.auth_bytes)
    }

    async fn scram(
        &mut self,
        mechanism: ScramMechanism,
        user: &str,
        password: &str,
    ) -> Result<(), ClientError> {
        let bad = |why: &str| ClientError::Protocol(format!("SCRAM: {why}"));
        let client_nonce = scram::nonce();
        let name = user.replace('=', "=3D").replace(',', "=2C");
        let bare = format!("n={name},r={client_nonce}");
        let first = self.authenticate(format!("n,,{bare}").into_bytes()).await?;
        let server_first =
            String::from_utf8(first.to_vec()).map_err(|_| bad("the broker's answer is not UTF-8"))?;
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;
        for attr in server_first.split(',') {
            match attr.split_once('=') {
                Some(("r", v)) => nonce = Some(v.to_owned()),
                Some(("s", v)) => salt = B64.decode(v).ok(),
                Some(("i", v)) => iterations = v.parse::<u32>().ok(),
                _ => {}
            }
        }
        let (Some(nonce), Some(salt), Some(iterations)) = (nonce, salt, iterations) else {
            return Err(bad("the broker's first answer is malformed"));
        };
        if !nonce.starts_with(&client_nonce) || nonce.len() == client_nonce.len() {
            return Err(bad("the broker changed the nonce"));
        }
        if iterations < scram::MIN_ITERATIONS {
            return Err(bad("the broker asks for too few iterations"));
        }
        let (proof, server_signature, without_proof) = scram::client_proof(
            mechanism,
            password,
            &salt,
            iterations,
            &bare,
            &server_first,
            &nonce,
        );
        let last = self
            .authenticate(format!("{without_proof},p={}", B64.encode(proof)).into_bytes())
            .await?;
        let expected = format!("v={}", B64.encode(server_signature));
        if !scram::same(&last, expected.as_bytes()) {
            return Err(bad("the broker could not prove it holds the credential"));
        }
        Ok(())
    }
}

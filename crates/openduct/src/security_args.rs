// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct serve`'s security settings, and `openduct user`: TLS on the client port, how clients sign in, and the
//! access rules they are held to. Without any of them the broker only listens on the loopback address; anything
//! wider needs all three, or OPENDUCT_ALLOW_INSECURE=true said on purpose.

use std::io::Read as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use openduct_broker::security::{Authorizer, ClientCerts, ClientTls, Credentials, JwtSettings, Security};

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ClientCertMode {
    /// No client certificates.
    None,
    /// A client may prove who it is with a certificate, or sign in with SASL.
    Optional,
    /// Every client presents a certificate the client authority signed (mutual TLS).
    Required,
}

#[derive(Debug, Args)]
pub(crate) struct SecurityArgs {
    /// The broker's TLS certificate chain (PEM) for the client port. With it, the port speaks TLS 1.3 only.
    #[arg(long, env = "OPENDUCT_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// The private key of --tls-cert (PEM).
    #[arg(long, env = "OPENDUCT_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// The authority whose certificates identify clients (PEM). A client's principal is the certificate's first URI
    /// name (a workload identity) or DNS name.
    #[arg(long, env = "OPENDUCT_TLS_CLIENT_CA", requires = "tls_cert")]
    tls_client_ca: Option<PathBuf>,
    /// Client certificates: none, optional or required.
    #[arg(long, env = "OPENDUCT_TLS_CLIENT_CERTS", value_enum, default_value = "none")]
    tls_client_certs: ClientCertMode,
    /// The SCRAM credentials file (`openduct user add`); clients sign in with SCRAM-SHA-256 or SCRAM-SHA-512.
    #[arg(long, env = "OPENDUCT_CREDENTIALS_FILE")]
    credentials_file: Option<PathBuf>,
    /// Also accept SASL PLAIN, checked against the credentials file (TLS only).
    #[arg(long, env = "OPENDUCT_SASL_PLAIN", default_value_t = false)]
    sasl_plain: bool,
    /// OAUTHBEARER: the issuer every token must carry.
    #[arg(long, env = "OPENDUCT_TOKEN_ISSUER", requires_all = ["token_audience", "token_jwks_file"])]
    token_issuer: Option<String>,
    /// OAUTHBEARER: the audience a token must be for.
    #[arg(long, env = "OPENDUCT_TOKEN_AUDIENCE", requires = "token_issuer")]
    token_audience: Option<String>,
    /// OAUTHBEARER: the issuer's public keys (a JWKS file, re-read when it changes).
    #[arg(long, env = "OPENDUCT_TOKEN_JWKS_FILE", requires = "token_issuer")]
    token_jwks_file: Option<PathBuf>,
    /// OAUTHBEARER: the claim naming the principal.
    #[arg(long, env = "OPENDUCT_TOKEN_PRINCIPAL_CLAIM", default_value = "sub")]
    token_principal_claim: String,
    /// The access rules (JSON: super users and rules; re-read when it changes). Without it every signed-in client
    /// may do everything.
    #[arg(long, env = "OPENDUCT_ACL_FILE")]
    acl_file: Option<PathBuf>,
    /// Let clients that do not sign in through as User:ANONYMOUS even when a way to sign in is set.
    #[arg(long, env = "OPENDUCT_ALLOW_ANONYMOUS", default_value_t = false)]
    allow_anonymous: bool,
    /// The longest a session lasts before the client signs in again (seconds; 0 = as long as the connection).
    #[arg(long, env = "OPENDUCT_MAX_SESSION_SECS", default_value_t = 3600)]
    max_session_secs: u64,
    /// Listen beyond the loopback address without TLS, sign-in or access rules. Only on purpose.
    #[arg(long, env = "OPENDUCT_ALLOW_INSECURE", default_value_t = false)]
    allow_insecure: bool,
}

impl SecurityArgs {
    /// Whether running beyond the loopback address without protection was said on purpose.
    pub(crate) fn allow_insecure(&self) -> bool {
        self.allow_insecure
    }

    pub(crate) fn build(&self) -> Result<Security, String> {
        let mut s = Security::open();
        let client_certs = match self.tls_client_certs {
            ClientCertMode::None => ClientCerts::None,
            ClientCertMode::Optional => ClientCerts::Optional,
            ClientCertMode::Required => ClientCerts::Required,
        };
        if let (Some(cert), Some(key)) = (&self.tls_cert, &self.tls_key) {
            s.tls = Some(ClientTls::from_pem_files(
                cert,
                key,
                self.tls_client_ca.as_deref(),
                client_certs,
            )?);
        } else if client_certs != ClientCerts::None {
            return Err("client certificates need --tls-cert and --tls-key".to_owned());
        }
        if let Some(path) = &self.credentials_file {
            s.credentials = Some(Arc::new(Credentials::from_file(path)?));
        }
        s.plain = self.sasl_plain;
        if let (Some(issuer), Some(audience), Some(jwks)) =
            (&self.token_issuer, &self.token_audience, &self.token_jwks_file)
        {
            s = s.with_tokens(JwtSettings {
                issuer: issuer.clone(),
                audience: audience.clone(),
                jwks_file: jwks.clone(),
                principal_claim: self.token_principal_claim.clone(),
                leeway: Duration::from_secs(30),
            })?;
        }
        if let Some(path) = &self.acl_file {
            s.rules = Some(Arc::new(Authorizer::from_file(path)?));
        }
        let can_sign_in = s.credentials.is_some() || s.tokens.is_some() || client_certs != ClientCerts::None;
        s.require_authentication = can_sign_in && !self.allow_anonymous;
        s.max_session = (self.max_session_secs > 0).then(|| Duration::from_secs(self.max_session_secs));
        s.allow_insecure = self.allow_insecure;
        Ok(s)
    }
}

#[derive(Debug, Subcommand)]
pub(crate) enum UserCommand {
    /// Add a name (or give it a new password), read from standard input - never from an argument.
    Add {
        /// The credentials file (created if missing, readable by its owner only).
        #[arg(long, env = "OPENDUCT_CREDENTIALS_FILE")]
        file: PathBuf,
        #[arg(long)]
        name: String,
        #[arg(long, default_value_t = openduct_broker::security::scram::DEFAULT_ITERATIONS)]
        iterations: u32,
    },
    /// Remove a name: it can no longer sign in.
    Remove {
        #[arg(long, env = "OPENDUCT_CREDENTIALS_FILE")]
        file: PathBuf,
        #[arg(long)]
        name: String,
    },
}

pub(crate) fn user(command: UserCommand) -> Result<(), String> {
    match command {
        UserCommand::Add {
            file,
            name,
            iterations,
        } => {
            let mut password = String::new();
            std::io::stdin()
                .read_to_string(&mut password)
                .map_err(|e| format!("cannot read the password from standard input: {e}"))?;
            let password = password.trim_end_matches(['\n', '\r']);
            openduct_broker::security::add_user(&file, &name, password, iterations)?;
            println!("{name} can sign in (SCRAM-SHA-256, SCRAM-SHA-512)");
            Ok(())
        }
        UserCommand::Remove { file, name } => {
            if openduct_broker::security::remove_user(&file, &name)? {
                println!("{name} removed");
            } else {
                println!("{name} was not there");
            }
            Ok(())
        }
    }
}

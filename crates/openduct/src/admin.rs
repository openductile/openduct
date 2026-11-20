// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `openduct topic ...` and `openduct health`: small conversations with a running broker.

use std::time::Duration;

use clap::{Args, Subcommand};
use openduct_broker::client::Client;

#[derive(Debug, Args)]
pub(crate) struct Target {
    /// The broker to talk to.
    #[arg(long, env = "OPENDUCT_BOOTSTRAP", default_value = "127.0.0.1:9092")]
    pub(crate) bootstrap: String,
    /// Give up after this many seconds.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub(crate) timeout_secs: u64,
    /// Connect with TLS, trusting this authority (PEM).
    #[arg(long = "tls-ca", env = "OPENDUCT_CLIENT_TLS_CA")]
    tls_ca: Option<std::path::PathBuf>,
    /// Present this certificate (PEM) as the client's identity; with --tls-key.
    #[arg(long = "tls-cert", env = "OPENDUCT_CLIENT_TLS_CERT", requires_all = ["tls_key", "tls_ca"])]
    tls_cert: Option<std::path::PathBuf>,
    #[arg(long = "tls-key", env = "OPENDUCT_CLIENT_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<std::path::PathBuf>,
    /// The name the broker's certificate must carry, when not the host of --bootstrap.
    #[arg(
        long = "tls-server-name",
        env = "OPENDUCT_CLIENT_TLS_SERVER_NAME",
        requires = "tls_ca"
    )]
    tls_server_name: Option<String>,
    /// Sign in: scram-sha-512, scram-sha-256, plain or oauthbearer.
    #[arg(long, env = "OPENDUCT_CLIENT_SASL_MECHANISM")]
    sasl_mechanism: Option<String>,
    /// The name to sign in as (SCRAM, PLAIN). The password comes from --password-file, never from an argument or the
    /// environment.
    #[arg(long, env = "OPENDUCT_CLIENT_USER")]
    user: Option<String>,
    #[arg(long, env = "OPENDUCT_CLIENT_PASSWORD_FILE")]
    password_file: Option<std::path::PathBuf>,
    /// A file holding the OAUTHBEARER token.
    #[arg(long, env = "OPENDUCT_CLIENT_TOKEN_FILE")]
    token_file: Option<std::path::PathBuf>,
}

impl Target {
    fn security(&self) -> Result<openduct_broker::client::ClientSecurity, String> {
        client_security(&ClientAuth {
            flag: "--",
            tls_ca: self.tls_ca.as_deref(),
            tls_cert: self.tls_cert.as_deref(),
            tls_key: self.tls_key.as_deref(),
            tls_server_name: self.tls_server_name.as_deref(),
            sasl_mechanism: self.sasl_mechanism.as_deref(),
            user: self.user.as_deref(),
            password_file: self.password_file.as_deref(),
            token_file: self.token_file.as_deref(),
        })
    }
}

/// How a client reaches a broker that asks for TLS or sign-in: what the flags of a command said. `flag` is what
/// those flags start with (`--`, or `--registry-broker-` for the registry's own connection), for messages that
/// name the one that is missing.
pub(crate) struct ClientAuth<'a> {
    pub(crate) flag: &'static str,
    pub(crate) tls_ca: Option<&'a std::path::Path>,
    pub(crate) tls_cert: Option<&'a std::path::Path>,
    pub(crate) tls_key: Option<&'a std::path::Path>,
    pub(crate) tls_server_name: Option<&'a str>,
    pub(crate) sasl_mechanism: Option<&'a str>,
    pub(crate) user: Option<&'a str>,
    pub(crate) password_file: Option<&'a std::path::Path>,
    pub(crate) token_file: Option<&'a std::path::Path>,
}

pub(crate) fn client_security(
    auth: &ClientAuth<'_>,
) -> Result<openduct_broker::client::ClientSecurity, String> {
    use openduct_broker::client::{ClientSecurity, SignIn};
    use openduct_broker::security::scram::ScramMechanism;
    let flag = auth.flag;
    let read =
        |p: &std::path::Path| std::fs::read(p).map_err(|e| format!("cannot read {}: {e}", p.display()));
    let mut s = ClientSecurity::default();
    if let Some(ca) = auth.tls_ca {
        let ca = read(ca)?;
        let identity = match (auth.tls_cert, auth.tls_key) {
            (Some(c), Some(k)) => Some((read(c)?, read(k)?)),
            _ => None,
        };
        s.tls = Some(ClientSecurity::tls_from_pem(
            &ca,
            identity.as_ref().map(|(c, k)| (c.as_slice(), k.as_slice())),
        )?);
        s.server_name = auth.tls_server_name.map(str::to_owned);
    }
    let password = || -> Result<String, String> {
        if let Some(p) = auth.password_file {
            let raw = read(p)?;
            return Ok(String::from_utf8_lossy(&raw)
                .trim_end_matches(['\n', '\r'])
                .to_owned());
        }
        if flag == "--" {
            crate::cluster::refuse_old("OPENDUCT_CLIENT_PASSWORD", "--password-file")?;
        }
        Err(format!(
            "signing in needs {flag}password-file: a file holding the password, only you may read"
        ))
    };
    let user = || {
        auth.user
            .map(str::to_owned)
            .ok_or_else(|| format!("signing in needs {flag}user"))
    };
    s.sign_in = match auth.sasl_mechanism.map(str::to_ascii_lowercase).as_deref() {
        None => None,
        Some("scram-sha-512") => Some(SignIn::Scram {
            mechanism: ScramMechanism::Sha512,
            user: user()?,
            password: password()?,
        }),
        Some("scram-sha-256") => Some(SignIn::Scram {
            mechanism: ScramMechanism::Sha256,
            user: user()?,
            password: password()?,
        }),
        Some("plain") => {
            if s.tls.is_none() {
                return Err(format!("PLAIN sends the password: it needs {flag}tls-ca"));
            }
            Some(SignIn::Plain {
                user: user()?,
                password: password()?,
            })
        }
        Some("oauthbearer") => {
            let path = auth
                .token_file
                .ok_or_else(|| format!("oauthbearer needs {flag}token-file"))?;
            Some(SignIn::Token(
                String::from_utf8_lossy(&read(path)?).trim().to_owned(),
            ))
        }
        Some(other) => return Err(format!("unknown {flag}sasl-mechanism {other:?}")),
    };
    Ok(s)
}

#[derive(Debug, Subcommand)]
pub(crate) enum TopicCommand {
    /// Create a topic.
    Create {
        /// The topic's name: 1 to 249 letters, digits, '.', '_' and '-'.
        name: String,
        /// How many partitions: more lets more consumers in a group share the work.
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(i32).range(1..=10_000))]
        partitions: i32,
        /// Keep records this many milliseconds; without it they are kept until the topic is deleted.
        #[arg(long, value_parser = clap::value_parser!(i64).range(1..))]
        retention_ms: Option<i64>,
        #[command(flatten)]
        target: Target,
    },
    /// Delete a topic and everything in it. This cannot be undone.
    Delete {
        /// The topic to delete.
        name: String,
        #[command(flatten)]
        target: Target,
    },
    /// List topics and their partition counts.
    List {
        #[command(flatten)]
        target: Target,
    },
}

pub(crate) async fn connect(target: &Target) -> Result<Client, String> {
    let security = target.security()?;
    Client::connect_with(
        &target.bootstrap,
        Duration::from_secs(target.timeout_secs),
        &security,
    )
    .await
    .map_err(|e| e.to_string())
}

pub(crate) async fn topic(command: TopicCommand) -> Result<(), String> {
    match command {
        TopicCommand::Create {
            name,
            partitions,
            retention_ms,
            target,
        } => {
            let mut client = connect(&target).await?;
            client
                .create_topic(&name, partitions, retention_ms)
                .await
                .map_err(|e| format!("cannot create {name}: {e}"))?;
            println!("created {name} with {partitions} partition(s)");
        }
        TopicCommand::Delete { name, target } => {
            let mut client = connect(&target).await?;
            client
                .delete_topic(&name)
                .await
                .map_err(|e| format!("cannot delete {name}: {e}"))?;
            println!("deleted {name}");
        }
        TopicCommand::List { target } => {
            let mut client = connect(&target).await?;
            let topics = client.topics().await.map_err(|e| e.to_string())?;
            if topics.is_empty() {
                println!("no topics");
            }
            for t in topics {
                println!("{}\t{} partition(s)", t.name, t.partitions);
            }
        }
    }
    Ok(())
}

pub(crate) async fn health(target: Target) -> Result<(), String> {
    let mut client = connect(&target).await?;
    let versions = client.api_versions().await.map_err(|e| e.to_string())?;
    println!(
        "ok: {} answers {} APIs",
        target.bootstrap,
        versions.api_keys.len()
    );
    Ok(())
}

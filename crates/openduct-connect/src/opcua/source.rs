// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: opcua:` - named values of a machine, PLC or SCADA server, as they change, through an OPC UA subscription.
//! Each change is one message. The first connection must succeed; after that, a connection that drops is made
//! again, and the subscription with it, for as long as the pipeline runs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use opcua::client::{ClientBuilder, DataChangeCallback, IdentityToken, Session, SessionPollResult};
use opcua::crypto::SecurityPolicy;
use opcua::types::{
    DataChangeFilter, DataChangeTrigger, DataValue, DeadbandType, ExtensionObject, MessageSecurityMode,
    MonitoredItemCreateRequest, NodeId, ReadValueId, StatusCode, TimestampsToReturn, UserTokenPolicy,
    Variant,
};
use tokio::sync::{mpsc, oneshot};

use super::values::change;
use crate::{SourceBatch, SourceMessage};

/// Changes waiting for the pipeline; past this many, the newest are left out (and counted).
const WAITING: usize = 10_000;
/// The most changes in one batch.
const BATCH: usize = 1_000;
/// The largest message taken from the server: OPC UA's usual 65,535-byte chunks, five to a message.
const MAX_MESSAGE: usize = 65_535 * 5;

enum Event {
    Changed(NodeId, Box<DataValue>),
    Ended(String),
}

/// How to sign in: anonymous, or a user name and password.
#[derive(Clone)]
pub(super) enum User {
    Anonymous,
    Password(String, String),
}

impl std::fmt::Debug for User {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Anonymous => "Anonymous",
            Self::Password(..) => "Password",
        })
    }
}

#[derive(Debug)]
pub(super) struct Settings {
    pub(super) url: String,
    pub(super) device: String,
    /// The values to read: name and node.
    pub(super) read: Vec<(String, NodeId)>,
    pub(super) every: Duration,
    pub(super) deadband: Option<f64>,
    pub(super) mode: MessageSecurityMode,
    pub(super) policy: SecurityPolicy,
    pub(super) user: User,
    pub(super) pki: PathBuf,
}

pub struct OpcUaSource {
    pub(super) shown: String,
    settings: Settings,
    names: HashMap<NodeId, String>,
    session: Option<Arc<Session>>,
    events: Option<mpsc::Receiver<Event>>,
    left_out: Arc<AtomicU64>,
    driver: Option<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for OpcUaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpcUaSource")
            .field("shown", &self.shown)
            .finish_non_exhaustive()
    }
}

/// A status that says the node cannot be read at all, not that its value is bad for now.
fn is_missing(code: StatusCode) -> bool {
    matches!(
        code.sub_code().name(),
        "BadNodeIdUnknown"
            | "BadNodeIdInvalid"
            | "BadAttributeIdInvalid"
            | "BadNotReadable"
            | "BadUserAccessDenied"
    )
}

/// The client's settings; `retries` is how often a connection is tried again (-1: for ever).
fn client_builder(s: &Settings, retries: i32) -> ClientBuilder {
    ClientBuilder::new()
        .application_name("OpenDuct")
        .application_uri("urn:openduct:client")
        .product_uri("urn:openduct")
        .pki_dir(s.pki.clone())
        .create_sample_keypair(true)
        .trust_server_certs(false)
        .verify_server_certs(true)
        .session_retry_limit(retries)
        .session_retry_initial(Duration::from_secs(1))
        .session_retry_max(Duration::from_secs(30))
        .max_message_size(MAX_MESSAGE)
        .session_name(format!("OpenDuct {}", s.device))
}

/// The library's error as words, without its debugging detail.
fn words(e: &opcua::types::Error) -> String {
    e.to_string()
}

impl OpcUaSource {
    pub(super) fn new(shown: String, settings: Settings) -> Self {
        let names = settings
            .read
            .iter()
            .map(|(name, node)| (node.clone(), name.clone()))
            .collect();
        Self {
            shown,
            settings,
            names,
            session: None,
            events: None,
            left_out: Arc::new(AtomicU64::new(0)),
            driver: None,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Connect, sign in and subscribe, so a wrong address, certificate, user or node is found at once.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let s = &self.settings;
        // The server's endpoints, asked for once: a server that cannot be reached, or that answers with something
        // that is not OPC UA, stops the pipeline with the reason instead of being asked again for ever.
        let discovery = client_builder(s, 0)
            .client()
            .map_err(|errors| format!("cannot set up OPC UA: {}", errors.join("; ")))?;
        let endpoints = discovery
            .get_server_endpoints_from_url(s.url.as_str())
            .await
            .map_err(|e| self.refused(&words(&e)))?;
        let client = client_builder(s, -1)
            .client()
            .map_err(|errors| format!("cannot set up OPC UA: {}", errors.join("; ")))?;
        let identity = match &s.user {
            User::Anonymous => IdentityToken::new_anonymous(),
            User::Password(user, password) => IdentityToken::new_user_name(user.clone(), password.clone()),
        };
        let (session, event_loop) = client
            .session_builder()
            .with_endpoints(endpoints)
            .connect_to_matching_endpoint((
                s.url.as_str(),
                s.policy.to_str(),
                s.mode,
                UserTokenPolicy::anonymous(),
            ))
            .map_err(|e| self.refused(&words(&e)))?
            .user_identity_token(identity)
            .build(client.certificate_store().clone())
            .map_err(|e| self.refused(&words(&e)))?;

        let (tx, rx) = mpsc::channel(WAITING);
        let ended = tx.clone();
        let mut stream = Box::pin(event_loop.enter());
        let device = s.device.clone();
        // The first connection's outcome, told once: the pipeline stops if it fails. After it, a connection that
        // drops is made again for as long as the pipeline runs.
        let (first_tx, first_rx) = oneshot::channel::<Result<(), StatusCode>>();
        let mut first = Some(first_tx);
        let driver = tokio::spawn(async move {
            while let Some(polled) = stream.next().await {
                match polled {
                    Ok(SessionPollResult::Reconnected(_)) => match first.take() {
                        Some(f) => {
                            let _ = f.send(Ok(()));
                        }
                        None => tracing::info!("OPC UA {device}: connected again"),
                    },
                    Ok(SessionPollResult::ReconnectFailed(code)) => match first.take() {
                        Some(f) => {
                            let _ = f.send(Err(code));
                            return;
                        }
                        None => tracing::warn!("OPC UA {device}: could not connect again yet ({code})"),
                    },
                    Ok(SessionPollResult::ConnectionLost(code)) => {
                        tracing::warn!("OPC UA {device}: the connection dropped ({code}); connecting again");
                    }
                    Ok(_) => {}
                    Err(code) => {
                        if let Some(f) = first.take() {
                            let _ = f.send(Err(code));
                            return;
                        }
                        let _ = ended
                            .send(Event::Ended(format!(
                                "the OPC UA session with {device} ended: {code}"
                            )))
                            .await;
                        return;
                    }
                }
            }
        });
        let outcome = tokio::time::timeout(Duration::from_secs(60), first_rx).await;
        match outcome {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(code))) => {
                driver.abort();
                return Err(self.refused(&code.to_string()));
            }
            _ => {
                driver.abort();
                return Err(format!(
                    "could not connect to the OPC UA server at {} within a minute",
                    s.url
                ));
            }
        }
        self.driver = Some(driver);
        let numeric = match self.check_nodes(&session).await {
            Ok(numeric) => numeric,
            Err(e) => {
                let _ = session.disconnect().await;
                return Err(e);
            }
        };
        let s = &self.settings;

        let left_out = Arc::clone(&self.left_out);
        let callback = DataChangeCallback::new(move |value, item| {
            let node = item.item_to_monitor().node_id.clone();
            if tx.try_send(Event::Changed(node, Box::new(value))).is_err() {
                left_out.fetch_add(1, Ordering::Relaxed);
            }
        });
        let subscription = session
            .create_subscription(s.every, 60, 10, 0, 0, true, callback)
            .await
            .map_err(|e| format!("the OPC UA server refused the subscription: {}", words(&e)))?;
        let requests: Vec<MonitoredItemCreateRequest> = s
            .read
            .iter()
            .enumerate()
            .zip(&numeric)
            .map(|((n, (_, node)), numeric)| {
                let mut request: MonitoredItemCreateRequest = node.clone().into();
                let p = &mut request.requested_parameters;
                p.client_handle = u32::try_from(n + 1).unwrap_or(u32::MAX);
                p.sampling_interval = s.every.as_secs_f64() * 1000.0;
                p.queue_size = 10;
                p.discard_oldest = true;
                // OPC UA has a deadband for numbers only; a server refuses one on any other value.
                if let (Some(deadband), true) = (s.deadband, *numeric) {
                    p.filter = ExtensionObject::from_message(DataChangeFilter {
                        trigger: DataChangeTrigger::StatusValue,
                        deadband_type: DeadbandType::Absolute as u32,
                        deadband_value: deadband,
                    });
                }
                request
            })
            .collect();
        let results = session
            .create_monitored_items(subscription, TimestampsToReturn::Both, requests)
            .await
            .map_err(|e| format!("the OPC UA server refused watching the values: {}", words(&e)))?;
        for ((name, node), result) in s.read.iter().zip(&results) {
            let code = result.result.status_code;
            if !code.is_good() {
                let _ = session.disconnect().await;
                return Err(format!(
                    "the OPC UA server refused watching {name} ({node}): {}",
                    code.sub_code().name()
                ));
            }
        }
        self.session = Some(session);
        self.events = Some(rx);
        Ok(())
    }

    /// Read each value once, so a node the server does not have is found now (some servers accept watching a node
    /// they do not have, and never report it); and whether each value is a number.
    async fn check_nodes(&self, session: &Session) -> Result<Vec<bool>, String> {
        let mut numeric = Vec::with_capacity(self.settings.read.len());
        for part in self.settings.read.chunks(500) {
            let ids: Vec<ReadValueId> = part.iter().map(|(_, node)| node.clone().into()).collect();
            let values = session
                .read(&ids, TimestampsToReturn::Neither, 0.0)
                .await
                .map_err(|e| format!("the OPC UA server refused reading the values: {}", words(&e)))?;
            for ((name, node), value) in part.iter().zip(&values) {
                numeric.push(matches!(
                    value.value,
                    Some(
                        Variant::SByte(_)
                            | Variant::Byte(_)
                            | Variant::Int16(_)
                            | Variant::UInt16(_)
                            | Variant::Int32(_)
                            | Variant::UInt32(_)
                            | Variant::Int64(_)
                            | Variant::UInt64(_)
                            | Variant::Float(_)
                            | Variant::Double(_)
                    )
                ));
                let code = value.status.unwrap_or(StatusCode::Good);
                if code.is_bad() && is_missing(code) {
                    return Err(format!(
                        "the OPC UA server refused watching {name} ({node}): {}",
                        code.sub_code().name()
                    ));
                }
            }
        }
        Ok(numeric)
    }

    fn refused(&self, why: &str) -> String {
        let s = &self.settings;
        if why.contains("BadCertificateUntrusted") || why.contains("BadSecurityChecksFailed") {
            return format!(
                "the OPC UA server's certificate is not trusted: check it and move it from {} to {} ({why})",
                s.pki.join("rejected").display(),
                s.pki.join("trusted").display()
            );
        }
        if why.contains("BadTcpEndpointUrlInvalid") {
            return format!(
                "the OPC UA server at {} offers no endpoint with security {} and policy {} ({why})",
                s.url,
                super::mode_name(s.mode),
                super::policy_name(s.policy)
            );
        }
        format!("the OPC UA server at {} refused the connection: {why}", s.url)
    }

    pub(crate) async fn next(&mut self, _position: Option<&str>) -> Result<SourceBatch, String> {
        let events = self.events.as_mut().ok_or("the source has not started")?;
        let mut got = Vec::new();
        match events.recv().await {
            Some(e) => got.push(e),
            None => return Err("the OPC UA session ended".to_owned()),
        }
        while got.len() < BATCH {
            match events.try_recv() {
                Ok(e) => got.push(e),
                Err(_) => break,
            }
        }
        let missed = self.left_out.swap(0, Ordering::Relaxed);
        if missed > 0 {
            tracing::warn!(
                "OPC UA {}: {missed} changes were left out: the pipeline could not keep up",
                self.settings.device
            );
        }
        let mut messages = Vec::with_capacity(got.len());
        for event in got {
            match event {
                Event::Ended(why) => return Err(why),
                Event::Changed(node, value) => {
                    let node_text = node.to_string();
                    let name = self
                        .names
                        .get(&node)
                        .cloned()
                        .unwrap_or_else(|| node_text.clone());
                    messages.push(SourceMessage {
                        key: Some(self.settings.device.clone().into_bytes()),
                        value: Some(change(&name, &node_text, &value).into_bytes()),
                        headers: vec![
                            (
                                "opcua-device".to_owned(),
                                self.settings.device.clone().into_bytes(),
                            ),
                            ("opcua-node".to_owned(), node_text.into_bytes()),
                        ],
                        ack: None,
                    });
                }
            }
        }
        Ok(SourceBatch {
            messages,
            position: None,
        })
    }

    pub(crate) async fn stop(&mut self) {
        if let Some(session) = self.session.take() {
            let _ = session.disconnect().await;
        }
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
    }
}

// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: mqtt:` and `to: mqtt:` - devices and machines through an MQTT broker (3.1.1).
//!
//! Reading subscribes with QoS 1 and acknowledges each message to the MQTT broker only once it is in the topic, in a
//! persistent session: what devices sent while the pipeline was stopped is delivered when it starts again.
//! Writing publishes with QoS 1 and counts a batch as stored once the MQTT broker has acknowledged all of it.
//!
//! ```yaml
//! from:
//!   mqtt:
//!     url: mqtts://broker.example.com:8883
//!     topics: [factory/+/temperature, factory/+/pressure]
//!     username: secret:MQTT_USER
//!     password: secret:MQTT_PASSWORD
//! ```

mod codec;
mod session;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openduct_pipeline::Connector;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use self::codec::Publish;
pub(crate) use self::codec::replay;
pub(crate) use self::session::MAX_PACKET as LARGEST_PACKET;
use self::session::{Command, MAX_PACKET, Options, Outbound, Progress, Received};
use crate::settings::{Limits, Settings};
use crate::{SinkOutcome, SinkRecord, SourceBatch, SourceMessage};

const CONNECTION_SETTINGS: &[&str] = &[
    "url",
    "client_id",
    "username",
    "password",
    "keep_alive",
    "ca",
    "client_cert",
    "client_key",
];
const SOURCE_SETTINGS: &[&str] = &["topics", "qos", "clean_session"];
const SINK_SETTINGS: &[&str] = &["topic", "qos", "retain"];
/// The most messages handed on at once.
const BATCH: usize = 1_000;
/// How long a batch written to the MQTT broker may wait for its acknowledgements.
const ACK_WAIT: Duration = Duration::from_secs(30);
/// Room for the topic and the rest of a PUBLISH around a message.
const OVERHEAD: usize = 65_545;

fn allowed(own: &[&'static str]) -> Vec<&'static str> {
    CONNECTION_SETTINGS.iter().chain(own).copied().collect()
}

#[derive(Debug, Clone)]
struct Endpoint {
    host: String,
    port: u16,
    tls: bool,
}

impl Endpoint {
    fn shown(&self) -> String {
        let scheme = if self.tls { "mqtts" } else { "mqtt" };
        format!("{scheme}://{}:{}", self.host, self.port)
    }
}

fn endpoint(url: &str, at: &str) -> Result<Endpoint, String> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| format!("{at}: is mqtt://host:port or mqtts://host:port"))?;
    let tls = match scheme {
        "mqtt" | "tcp" => false,
        "mqtts" | "ssl" => true,
        _ => return Err(format!("{at}: starts with mqtt:// or mqtts://, not {scheme}://")),
    };
    let rest = rest.trim_end_matches('/');
    if rest.contains(['/', '@', '?', '#']) {
        return Err(format!(
            "{at}: is only mqtt://host:port; the username and password are settings of their own"
        ));
    }
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (
            h.to_owned(),
            p.parse().map_err(|_| format!("{at}: {p:?} is not a port"))?,
        ),
        None => (rest.to_owned(), if tls { 8883 } else { 1883 }),
    };
    if host.is_empty() {
        return Err(format!("{at}: names no host"));
    }
    Ok(Endpoint { host, port, tls })
}

/// The connection settings both sides share.
fn options(s: &Settings<'_>, client_id: String, clean_session: bool) -> Result<(Options, Endpoint), String> {
    let at = endpoint(&s.required("url")?, &s.path("url"))?;
    s.reach(&s.path("url"), &at.host, at.port)?;
    if client_id.is_empty() || client_id.len() > 256 || client_id.chars().any(char::is_control) {
        return Err(format!("{}: is 1 to 256 characters", s.path("client_id")));
    }
    let keep_alive = s.duration("keep_alive", Duration::from_secs(30))?;
    if keep_alive < Duration::from_secs(1) || keep_alive > Duration::from_secs(65_535) {
        return Err(format!("{}: is between 1s and 18h", s.path("keep_alive")));
    }
    let username = s.text("username")?;
    let password = s.text("password")?;
    if username.is_none() && password.is_some() {
        return Err(format!("{}: needs a username too", s.path("password")));
    }
    let tls = if at.tls {
        Some(Arc::new(crate::tls::config(s)?))
    } else {
        for name in ["ca", "client_cert", "client_key"] {
            if s.text(name)?.is_some() {
                return Err(format!("{}: is for mqtts:// only", s.path(name)));
            }
        }
        None
    };
    let options = Options {
        host: at.host.clone(),
        port: at.port,
        tls,
        client_id,
        username,
        password,
        keep_alive,
        clean_session,
    };
    Ok((options, at))
}

fn qos(s: &Settings<'_>) -> Result<u8, String> {
    Ok(match s.choice("qos", &["1", "0"])? {
        "0" => 0,
        _ => 1,
    })
}

async fn finish(task: Option<JoinHandle<()>>) {
    if let Some(task) = task {
        let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    }
}

// ------------------------------------------------------------------------------------------------------ source

/// A message handed on: whether it was stored, and the connection and packet id to acknowledge it with.
type Pending = (oneshot::Receiver<Result<(), String>>, u64, u16);

#[derive(Debug)]
pub struct MqttSource {
    options: Options,
    endpoint: Endpoint,
    filters: Vec<String>,
    qos: u8,
    received: Option<mpsc::Receiver<Result<Received, String>>>,
    commands: Option<mpsc::UnboundedSender<Command>>,
    task: Option<JoinHandle<()>>,
    /// Messages handed on, each to be acknowledged once it is stored.
    pending: Vec<Pending>,
    failed: Option<String>,
}

impl MqttSource {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SOURCE_SETTINGS), limits)?;
        let client_id = s
            .text("client_id")?
            .unwrap_or_else(|| format!("openduct-{pipeline}"));
        // A persistent session, so the MQTT broker keeps what arrives while the pipeline is stopped.
        let (options, endpoint) = options(&s, client_id, s.boolean("clean_session", false)?)?;
        let filters = s.list("topics")?;
        if filters.is_empty() {
            return Err(format!(
                "{}: names at least one MQTT topic, such as sensors/+/temperature",
                s.path("topics")
            ));
        }
        if let Some(bad) = filters.iter().find(|f| !valid_filter(f)) {
            return Err(format!(
                "{}: {bad:?} is not an MQTT topic filter (+ stands for one level, # for the rest, at the end)",
                s.path("topics")
            ));
        }
        Ok(Self {
            options,
            endpoint,
            filters,
            qos: qos(&s)?,
            received: None,
            commands: None,
            task: None,
            pending: Vec::new(),
            failed: None,
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!("{} {}", self.endpoint.shown(), self.filters.join(", "))
    }

    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let (tx, rx) = mpsc::channel(10 * BATCH);
        let (commands, commands_rx) = mpsc::unbounded_channel();
        let filters = self.filters.iter().map(|f| (f.clone(), self.qos)).collect();
        self.task = Some(tokio::spawn(session::subscriber(
            self.options.clone(),
            filters,
            tx,
            commands_rx,
        )));
        self.commands = Some(commands);
        self.received = Some(rx);
        Ok(())
    }

    /// Acknowledge what the pipeline has stored since the last batch.
    async fn acknowledge(&mut self) {
        let Some(commands) = &self.commands else { return };
        for (stored, connection, pkid) in std::mem::take(&mut self.pending) {
            if let Ok(Ok(())) = stored.await {
                let _ = commands.send(Command::Ack { connection, pkid });
            }
        }
    }

    pub(crate) async fn next(&mut self) -> Result<SourceBatch, String> {
        self.acknowledge().await;
        if let Some(reason) = self.failed.take() {
            return Err(reason);
        }
        let Some(received) = self.received.as_mut() else {
            return Err("the MQTT connection was not started".to_owned());
        };
        let mut batch = Vec::new();
        match tokio::time::timeout(Duration::from_secs(1), received.recv()).await {
            Ok(Some(Ok(first))) => batch.push(first),
            Ok(Some(Err(reason))) => return Err(reason),
            Ok(None) => return Err("the MQTT connection stopped".to_owned()),
            Err(_) => return Ok(SourceBatch::default()),
        }
        while batch.len() < BATCH {
            match received.try_recv() {
                Ok(Ok(more)) => batch.push(more),
                Ok(Err(reason)) => {
                    self.failed = Some(reason);
                    break;
                }
                Err(_) => break,
            }
        }
        let mut messages = Vec::with_capacity(batch.len());
        for Received { publish, connection } in batch {
            let ack = if publish.qos == 0 {
                None
            } else {
                let (ack, stored) = oneshot::channel();
                self.pending.push((stored, connection, publish.pkid));
                Some(ack)
            };
            let topic = publish.topic.into_bytes();
            messages.push(SourceMessage {
                key: Some(topic.clone()),
                value: Some(publish.payload.to_vec()),
                headers: vec![("mqtt-topic".to_owned(), topic)],
                ack,
            });
        }
        Ok(SourceBatch {
            messages,
            position: None,
        })
    }

    /// Acknowledge what is stored and disconnect, so the MQTT broker does not send it again.
    pub(crate) async fn stop(&mut self) {
        self.acknowledge().await;
        if let Some(commands) = self.commands.take() {
            let _ = commands.send(Command::Stop);
        }
        // Messages still on their way are not acknowledged; the MQTT broker sends them again next time.
        self.received = None;
        finish(self.task.take()).await;
    }
}

/// Whether `filter` is an MQTT topic filter: `+` a whole level, `#` only as the last.
fn valid_filter(filter: &str) -> bool {
    if filter.is_empty() || filter.len() > 65_535 || filter.contains('\0') {
        return false;
    }
    let levels: Vec<&str> = filter.split('/').collect();
    let last = levels.len() - 1;
    levels.iter().enumerate().all(|(i, level)| match *level {
        "+" => true,
        "#" => i == last,
        other => !other.contains(['+', '#']),
    })
}

// ------------------------------------------------------------------------------------------------------ sink

#[derive(Debug)]
pub struct MqttSink {
    options: Options,
    endpoint: Endpoint,
    topic: String,
    qos: u8,
    retain: bool,
    requests: Option<mpsc::Sender<Outbound>>,
    progress: Option<watch::Receiver<Progress>>,
    task: Option<JoinHandle<()>>,
    sent: u64,
}

impl MqttSink {
    pub(crate) fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        let s = Settings::new(connector, &allowed(SINK_SETTINGS), limits)?;
        // Each copy of a pipeline has a connection of its own: an MQTT broker lets only one use a client id.
        let client_id = match s.text("client_id")? {
            Some(id) => id,
            None => format!("openduct-{pipeline}-{}", unique()),
        };
        let (options, endpoint) = options(&s, client_id, true)?;
        let topic = s.required("topic")?;
        if topic.replace("{key}", "x").contains(['+', '#', '\0']) || topic.len() > 65_535 {
            return Err(format!(
                "{}: a topic messages are published to has no + or #",
                s.path("topic")
            ));
        }
        Ok(Self {
            options,
            endpoint,
            topic,
            qos: qos(&s)?,
            retain: s.boolean("retain", false)?,
            requests: None,
            progress: None,
            task: None,
            sent: 0,
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!("{} {}", self.endpoint.shown(), self.topic)
    }

    fn connected(&mut self) -> (mpsc::Sender<Outbound>, watch::Receiver<Progress>) {
        if let (Some(requests), Some(progress)) = (&self.requests, &self.progress) {
            return (requests.clone(), progress.clone());
        }
        let (requests, requests_rx) = mpsc::channel(2 * BATCH);
        let (tx, progress) = watch::channel(Progress::default());
        self.task = Some(tokio::spawn(session::publisher(
            self.options.clone(),
            requests_rx,
            tx,
        )));
        self.requests = Some(requests.clone());
        self.progress = Some(progress.clone());
        self.sent = 0;
        (requests, progress)
    }

    /// The topic a message goes to, or why it cannot go.
    fn topic_for(&self, record: &SinkRecord) -> Result<String, String> {
        if !self.topic.contains("{key}") {
            return Ok(self.topic.clone());
        }
        let key = record
            .key
            .as_deref()
            .ok_or("the message has no key for {key} in the MQTT topic")?;
        let topic = self.topic.replace("{key}", key);
        if topic.contains(['+', '#', '\0']) {
            return Err(format!("{topic:?} is not an MQTT topic: it has a + or #"));
        }
        if topic.len() > 65_535 {
            return Err("the MQTT topic is longer than 65,535 bytes".to_owned());
        }
        Ok(topic)
    }

    pub(crate) async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        let (requests, mut progress) = self.connected();
        let failed = |progress: &watch::Receiver<Progress>| {
            progress
                .borrow()
                .failed
                .clone()
                .unwrap_or_else(|| "the MQTT connection stopped".to_owned())
        };
        let mut rejected = Vec::new();
        for record in records {
            if record.value.len() > MAX_PACKET - OVERHEAD {
                rejected.push((
                    record,
                    "the message is larger than the 16 MiB MQTT takes here".to_owned(),
                ));
                continue;
            }
            match self.topic_for(&record) {
                Ok(topic) => {
                    let publish = Publish {
                        topic,
                        payload: Bytes::from(record.value),
                        qos: self.qos,
                        retain: self.retain,
                        dup: false,
                        pkid: 0,
                    };
                    if requests.send(Outbound::Publish(publish)).await.is_err() {
                        return Err(failed(&progress));
                    }
                    self.sent += 1;
                }
                Err(reason) => rejected.push((record, reason)),
            }
        }
        let wanted = self.sent;
        let delivered = tokio::time::timeout(
            ACK_WAIT,
            progress.wait_for(|p| p.acked >= wanted || p.failed.is_some()),
        )
        .await
        .map_err(|_| "the MQTT broker did not acknowledge the messages within 30 seconds".to_owned())?
        .map(|p| p.failed.is_none())
        .unwrap_or(false);
        if !delivered {
            return Err(failed(&progress));
        }
        Ok(SinkOutcome {
            durable: through,
            rejected,
        })
    }

    pub(crate) async fn close(&mut self) {
        self.progress = None;
        if let Some(requests) = self.requests.take() {
            let _ = requests.send(Outbound::Stop).await;
        }
        finish(self.task.take()).await;
    }
}

/// A short text that differs between processes and runs.
fn unique() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    format!("{:x}{nanos:x}", std::process::id())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn an_mqtt_url_is_host_and_port_only() {
        let at = endpoint("mqtts://broker.example.com", "url").unwrap();
        assert_eq!(
            (at.host.as_str(), at.port, at.tls),
            ("broker.example.com", 8883, true)
        );
        assert_eq!(endpoint("mqtt://10.0.0.5:1884/", "url").unwrap().port, 1884);
        assert!(endpoint("http://broker", "url").is_err());
        assert!(
            endpoint("mqtt://user:pw@broker:1883", "url")
                .unwrap_err()
                .contains("settings of their own")
        );
        assert!(endpoint("mqtt://:1883", "url").is_err());
        assert!(endpoint("mqtt://broker:99999", "url").is_err());
    }

    #[test]
    fn topic_filters_follow_the_standard() {
        for good in ["a/b", "a/+/c", "#", "a/#", "+", "+/+", "/a"] {
            assert!(valid_filter(good), "{good}");
        }
        for bad in ["", "a/#/c", "a#", "a/b+", "a/+b/c", "a\0b"] {
            assert!(!valid_filter(bad), "{bad:?}");
        }
    }
}

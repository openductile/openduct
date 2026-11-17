// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct connectors: the systems a pipeline reads from (`from: http:`) or writes to (`to: s3:`). One side of a
//! pipeline is always a topic, so what came in can be read again. See `docs/connectors.md`.
//!
//! A **source** hands the runner batches of messages and the position after them; the runner writes them to the
//! topic, then saves the position, then tells the source they are safe (a webhook answers its sender only then).
//! A **sink** takes the messages the runner read, and says which offsets are now safely stored, so the runner
//! commits only those.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use openduct_pipeline::Connector;
use tokio::sync::oneshot;

mod elasticsearch;
#[doc(hidden)]
pub mod hostile;
mod http;
mod json;
mod jwt;
mod kafka;
mod modbus;
mod mongo;
mod mqtt;
mod mysql;
mod opcua;
pub mod postgres;
pub mod reach;
mod s3;
mod salesforce;
mod scram;
mod servicenow;
pub mod settings;
mod snowflake;
mod tls;
mod web;
mod webhook;

pub use settings::{Limits, Secrets};

/// The sources there are.
pub const SOURCES: &[&str] = &[
    "http",
    "webhook",
    "mqtt",
    "modbus",
    "opcua",
    "kafka",
    "postgres_cdc",
    "mysql_cdc",
    "mongodb_cdc",
    "salesforce",
    "servicenow",
    "snowflake",
];
/// The sinks there are.
pub const SINKS: &[&str] = &[
    "elasticsearch",
    "s3",
    "mqtt",
    "kafka",
    "salesforce",
    "servicenow",
    "snowflake",
];

/// A message a source read.
#[derive(Debug)]
pub struct SourceMessage {
    pub key: Option<Vec<u8>>,
    /// `None` for a message with no value: a deletion, on a Kafka topic that keeps the latest value of each key.
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    /// Told once the message is safely in the topic (or that it could not be stored).
    pub ack: Option<oneshot::Sender<Result<(), String>>>,
}

impl SourceMessage {
    pub(crate) fn new(value: Vec<u8>) -> Self {
        Self {
            key: None,
            value: Some(value),
            headers: Vec::new(),
            ack: None,
        }
    }
}

/// What a source read, and where to carry on from once it is written.
#[derive(Debug, Default)]
pub struct SourceBatch {
    pub messages: Vec<SourceMessage>,
    /// The position after these messages, saved once they are written; `None` keeps the last one.
    pub position: Option<String>,
}

/// A message for a sink, with where it was read from.
#[derive(Debug, Clone)]
pub struct SinkRecord {
    pub key: Option<String>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub partition: i32,
    pub offset: i64,
}

/// What a sink stored: the offsets now safe to commit, and the messages it refused, with why.
#[derive(Debug, Default)]
pub struct SinkOutcome {
    pub durable: BTreeMap<i32, i64>,
    pub rejected: Vec<(SinkRecord, String)>,
}

fn unknown(connector: &Connector, here: &[&str], there: &[&str], side: &str, other_side: &str) -> String {
    if there.contains(&connector.kind.as_str()) {
        return format!(
            "{}: {} is written under {other_side}, not {side}",
            connector.path, connector.kind
        );
    }
    let near = here
        .iter()
        .find(|n| n.starts_with(connector.kind.get(..2).unwrap_or("")));
    match near {
        Some(n) => format!(
            "{}: there is no connector {:?} (did you mean {n:?}?)",
            connector.path, connector.kind
        ),
        None => format!(
            "{}: there is no connector {:?}: the ones here are {}",
            connector.path,
            connector.kind,
            here.join(", ")
        ),
    }
}

/// A source connector, ready to start.
#[derive(Debug)]
pub enum Source {
    Http(Box<http::HttpSource>),
    Webhook(Box<webhook::WebhookSource>),
    Mqtt(Box<mqtt::MqttSource>),
    Modbus(Box<modbus::ModbusSource>),
    Kafka(Box<kafka::KafkaSource>),
    PostgresCdc(Box<postgres::PostgresCdcSource>),
    MysqlCdc(Box<mysql::MysqlCdcSource>),
    MongoCdc(Box<mongo::MongoCdcSource>),
    Salesforce(Box<salesforce::SalesforceSource>),
    ServiceNow(Box<servicenow::ServiceNowSource>),
    Snowflake(Box<snowflake::SnowflakeSource>),
    OpcUa(Box<opcua::OpcUaSource>),
}

impl Source {
    /// Check the connector's settings and set it up, without reaching the system yet.
    pub fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        match connector.kind.as_str() {
            "modbus" => Ok(Self::Modbus(Box::new(modbus::ModbusSource::build(
                connector, limits,
            )?))),
            "mqtt" => Ok(Self::Mqtt(Box::new(mqtt::MqttSource::build(
                connector, pipeline, limits,
            )?))),
            "kafka" => Ok(Self::Kafka(Box::new(kafka::KafkaSource::build(
                connector, pipeline, limits,
            )?))),
            "postgres_cdc" => Ok(Self::PostgresCdc(Box::new(postgres::PostgresCdcSource::build(
                connector, pipeline, limits,
            )?))),
            "mysql_cdc" => Ok(Self::MysqlCdc(Box::new(mysql::MysqlCdcSource::build(
                connector, pipeline, limits,
            )?))),
            "mongodb_cdc" => Ok(Self::MongoCdc(Box::new(mongo::MongoCdcSource::build(
                connector, pipeline, limits,
            )?))),
            "salesforce" => Ok(Self::Salesforce(Box::new(salesforce::SalesforceSource::build(
                connector, limits,
            )?))),
            "servicenow" => Ok(Self::ServiceNow(Box::new(servicenow::ServiceNowSource::build(
                connector, limits,
            )?))),
            "snowflake" => Ok(Self::Snowflake(Box::new(snowflake::SnowflakeSource::build(
                connector, limits,
            )?))),
            "opcua" => Ok(Self::OpcUa(Box::new(opcua::OpcUaSource::build(
                connector, limits,
            )?))),
            "http" => Ok(Self::Http(Box::new(http::HttpSource::build(connector, limits)?))),
            "webhook" => Ok(Self::Webhook(Box::new(webhook::WebhookSource::build(
                connector, limits,
            )?))),
            _ => Err(unknown(connector, SOURCES, SINKS, "from", "to")),
        }
    }

    /// What it reads, for the log.
    pub fn describe(&self) -> String {
        match self {
            Self::Http(s) => format!("http {}", s.describe()),
            Self::Webhook(s) => format!("webhook {}", s.describe()),
            Self::Mqtt(s) => format!("mqtt {}", s.describe()),
            Self::Modbus(s) => s.describe(),
            Self::Kafka(s) => format!("kafka {}", s.describe()),
            Self::PostgresCdc(s) => format!("postgres_cdc {}", s.describe()),
            Self::MysqlCdc(s) => format!("mysql_cdc {}", s.describe()),
            Self::MongoCdc(s) => format!("mongodb_cdc {}", s.describe()),
            Self::Salesforce(s) => format!("salesforce {}", s.describe()),
            Self::ServiceNow(s) => format!("servicenow {}", s.describe()),
            Self::Snowflake(s) => format!("snowflake {}", s.describe()),
            Self::OpcUa(s) => format!("opcua {}", s.describe()),
        }
    }

    /// Reach the system: listen, connect, subscribe.
    pub async fn start(&mut self) -> Result<(), String> {
        match self {
            Self::Http(_) | Self::Modbus(_) => Ok(()),
            Self::Webhook(s) => s.start().await,
            Self::Mqtt(s) => s.start().await,
            Self::Kafka(s) => s.start().await,
            Self::PostgresCdc(s) => s.start().await,
            Self::MysqlCdc(s) => s.start().await,
            Self::MongoCdc(s) => s.start().await,
            Self::Salesforce(s) => s.start().await,
            Self::ServiceNow(s) => s.start().await,
            Self::Snowflake(s) => s.start().await,
            Self::OpcUa(s) => s.start().await,
        }
    }

    /// The next batch, waiting as long as the source's own rhythm says (an HTTP source's `every`). An empty batch
    /// means nothing arrived; ask again.
    pub async fn next(&mut self, position: Option<&str>) -> Result<SourceBatch, String> {
        match self {
            Self::Http(s) => s.next(position).await,
            Self::Webhook(s) => s.next().await,
            Self::Mqtt(s) => s.next().await,
            Self::Modbus(s) => s.next().await,
            Self::Kafka(s) => s.next().await,
            Self::PostgresCdc(s) => s.next(position).await,
            Self::MysqlCdc(s) => s.next(position).await,
            Self::MongoCdc(s) => s.next(position).await,
            Self::Salesforce(s) => s.next(position).await,
            Self::ServiceNow(s) => s.next(position).await,
            Self::Snowflake(s) => s.next(position).await,
            Self::OpcUa(s) => s.next(position).await,
        }
    }

    /// Stop cleanly: what is stored is acknowledged to the system it came from.
    pub async fn stop(&mut self) {
        match self {
            Self::Mqtt(s) => s.stop().await,
            Self::Kafka(s) => s.stop().await,
            Self::PostgresCdc(s) => s.stop().await,
            Self::MysqlCdc(s) => s.stop().await,
            Self::MongoCdc(s) => s.stop().await,
            Self::OpcUa(s) => s.stop().await,
            Self::Http(_)
            | Self::Salesforce(_)
            | Self::ServiceNow(_)
            | Self::Snowflake(_)
            | Self::Webhook(_)
            | Self::Modbus(_) => {}
        }
    }

    /// Where a webhook listens, once started.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        match self {
            Self::Webhook(s) => s.local_addr(),
            Self::Http(_)
            | Self::Mqtt(_)
            | Self::Modbus(_)
            | Self::Kafka(_)
            | Self::PostgresCdc(_)
            | Self::MysqlCdc(_)
            | Self::MongoCdc(_)
            | Self::Salesforce(_)
            | Self::ServiceNow(_)
            | Self::Snowflake(_)
            | Self::OpcUa(_) => None,
        }
    }
}

/// A sink connector.
#[derive(Debug)]
pub enum Sink {
    Elasticsearch(Box<elasticsearch::ElasticsearchSink>),
    S3(Box<s3::S3Sink>),
    Mqtt(Box<mqtt::MqttSink>),
    Kafka(Box<kafka::KafkaSink>),
    Salesforce(Box<salesforce::SalesforceSink>),
    ServiceNow(Box<servicenow::ServiceNowSink>),
    Snowflake(Box<snowflake::SnowflakeSink>),
}

impl Sink {
    pub fn build(connector: &Connector, pipeline: &str, limits: Limits) -> Result<Self, String> {
        match connector.kind.as_str() {
            "elasticsearch" => Ok(Self::Elasticsearch(Box::new(
                elasticsearch::ElasticsearchSink::build(connector, limits)?,
            ))),
            "s3" => Ok(Self::S3(Box::new(s3::S3Sink::build(
                connector, pipeline, limits,
            )?))),
            "mqtt" => Ok(Self::Mqtt(Box::new(mqtt::MqttSink::build(
                connector, pipeline, limits,
            )?))),
            "kafka" => Ok(Self::Kafka(Box::new(kafka::KafkaSink::build(
                connector, pipeline, limits,
            )?))),
            "salesforce" => Ok(Self::Salesforce(Box::new(salesforce::SalesforceSink::build(
                connector, limits,
            )?))),
            "servicenow" => Ok(Self::ServiceNow(Box::new(servicenow::ServiceNowSink::build(
                connector, limits,
            )?))),
            "snowflake" => Ok(Self::Snowflake(Box::new(snowflake::SnowflakeSink::build(
                connector, limits,
            )?))),
            _ => Err(unknown(connector, SINKS, SOURCES, "to", "from")),
        }
    }

    pub fn describe(&self) -> String {
        match self {
            Self::Elasticsearch(s) => format!("elasticsearch {}", s.describe()),
            Self::S3(s) => format!("s3 {}", s.describe()),
            Self::Mqtt(s) => format!("mqtt {}", s.describe()),
            Self::Kafka(s) => format!("kafka {}", s.describe()),
            Self::Salesforce(s) => format!("salesforce {}", s.describe()),
            Self::ServiceNow(s) => format!("servicenow {}", s.describe()),
            Self::Snowflake(s) => format!("snowflake {}", s.describe()),
        }
    }

    /// Store `records`; `through` is how far the runner has read on each partition, including messages that gave
    /// nothing to store.
    pub async fn send(
        &mut self,
        records: Vec<SinkRecord>,
        through: BTreeMap<i32, i64>,
    ) -> Result<SinkOutcome, String> {
        match self {
            Self::Elasticsearch(s) => s.send(records, through).await,
            Self::S3(s) => s.send(records, through).await,
            Self::Mqtt(s) => s.send(records, through).await,
            Self::Kafka(s) => s.send(records, through).await,
            Self::Salesforce(s) => s.send(records, through).await,
            Self::ServiceNow(s) => s.send(records, through).await,
            Self::Snowflake(s) => s.send(records, through).await,
        }
    }

    /// Called now and then: a sink that gathers messages writes them out when their time is up.
    pub async fn tick(&mut self) -> Result<SinkOutcome, String> {
        match self {
            Self::Elasticsearch(_)
            | Self::Mqtt(_)
            | Self::Kafka(_)
            | Self::Salesforce(_)
            | Self::ServiceNow(_)
            | Self::Snowflake(_) => Ok(SinkOutcome::default()),
            Self::S3(s) => s.tick().await,
        }
    }

    /// The pipeline is stopping: write out anything still gathered.
    pub async fn close(&mut self) -> Result<SinkOutcome, String> {
        match self {
            Self::Elasticsearch(_)
            | Self::Kafka(_)
            | Self::Salesforce(_)
            | Self::ServiceNow(_)
            | Self::Snowflake(_) => Ok(SinkOutcome::default()),
            Self::Mqtt(s) => {
                s.close().await;
                Ok(SinkOutcome::default())
            }
            Self::S3(s) => s.close().await,
        }
    }
}

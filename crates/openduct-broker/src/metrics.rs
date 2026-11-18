// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What the broker measures, in the Prometheus text format (`openduct serve --metrics-listen`).
//!
//! Counters are kept as requests and connections pass, with atomics only: nothing is locked and nothing grows with
//! traffic. What the log holds (offsets, consumer group lag, partitions without a leader) is read from the store
//! when the metrics are asked for, so it costs nothing in between. In a cluster each broker reports the partitions it
//! leads and the groups it coordinates, so summing the brokers counts each once.
//!
//! Names follow Prometheus' conventions (`_total` counters, `_seconds` durations, `_bytes` sizes); where
//! OpenTelemetry's messaging conventions name the same thing, the meaning matches them.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::handlers::{SUPPORTED, State};

/// The bounds of the request duration histogram, in seconds: from a cached metadata answer to a long poll.
const BUCKETS: [f64; 12] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// One Kafka API's requests: how many, how many failed, and how long they took.
#[derive(Debug)]
struct ApiMeter {
    key: Option<i16>,
    name: &'static str,
    count: AtomicU64,
    failed: AtomicU64,
    micros: AtomicU64,
    /// Requests that took at most `BUCKETS[i]` (not cumulative here; the text adds them up).
    buckets: [AtomicU64; BUCKETS.len()],
}

impl ApiMeter {
    fn new(key: Option<i16>, name: &'static str) -> Self {
        Self {
            key,
            name,
            count: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            micros: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// The broker's counters.
#[derive(Debug)]
pub(crate) struct Metrics {
    started: Instant,
    apis: Vec<ApiMeter>,
    connections_open: AtomicI64,
    connections_accepted: AtomicU64,
    connections_refused: AtomicU64,
    produced_bytes: AtomicU64,
    fetched_bytes: AtomicU64,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        let mut apis: Vec<ApiMeter> = SUPPORTED
            .iter()
            .map(|(key, _, _)| ApiMeter::new(Some(*key as i16), crate::api_name(*key)))
            .collect();
        // Requests for an API the broker does not serve (its connection is closed).
        apis.push(ApiMeter::new(None, "other"));
        Self {
            started: Instant::now(),
            apis,
            connections_open: AtomicI64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_refused: AtomicU64::new(0),
            produced_bytes: AtomicU64::new(0),
            fetched_bytes: AtomicU64::new(0),
        }
    }

    /// A request of API `key` answered (or refused) after `took`.
    pub(crate) fn request(&self, key: i16, took: Duration, ok: bool) {
        let Some(meter) = self
            .apis
            .iter()
            .find(|m| m.key == Some(key))
            .or_else(|| self.apis.last())
        else {
            return;
        };
        meter.count.fetch_add(1, Ordering::Relaxed);
        if !ok {
            meter.failed.fetch_add(1, Ordering::Relaxed);
        }
        let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        meter.micros.fetch_add(micros, Ordering::Relaxed);
        let seconds = took.as_secs_f64();
        if let Some(bucket) = BUCKETS
            .iter()
            .zip(&meter.buckets)
            .find(|(bound, _)| seconds <= **bound)
            .map(|(_, count)| count)
        {
            bucket.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn connection_accepted(&self) {
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);
        self.connections_open.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn connection_closed(&self) {
        self.connections_open.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn connection_refused(&self) {
        self.connections_refused.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn produced(&self, bytes: u64) {
        self.produced_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn fetched(&self, bytes: u64) {
        self.fetched_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// A handle on a running broker's metrics, for an endpoint that serves them; cheap to clone.
#[derive(Clone, Debug)]
pub struct BrokerMetrics {
    pub(crate) state: Arc<State>,
}

impl BrokerMetrics {
    /// The metrics now, in the Prometheus text format (version 0.0.4).
    pub async fn render(&self) -> String {
        let mut out = String::with_capacity(16 * 1024);
        counters(&mut out, &self.state);
        let state = Arc::clone(&self.state);
        // The store may read the disk: off the network threads.
        if let Ok(log) = tokio::task::spawn_blocking(move || log_gauges(&state)).await {
            out.push_str(&log);
        }
        out
    }
}

fn help(out: &mut String, name: &str, kind: &str, text: &str) {
    let _ = writeln!(out, "# HELP {name} {text}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

/// A label value, escaped as the text format asks (backslash, double quote, new line).
fn label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn counters(out: &mut String, state: &State) {
    let m = &state.metrics;
    help(
        out,
        "openduct_build_info",
        "gauge",
        "The running OpenDuct's version.",
    );
    let _ = writeln!(
        out,
        "openduct_build_info{{version=\"{}\",node_id=\"{}\"}} 1",
        env!("CARGO_PKG_VERSION"),
        state.config.node_id
    );
    help(
        out,
        "openduct_uptime_seconds",
        "gauge",
        "Seconds since the broker started.",
    );
    let _ = writeln!(out, "openduct_uptime_seconds {}", m.started.elapsed().as_secs());

    help(
        out,
        "openduct_connections",
        "gauge",
        "Client connections open now.",
    );
    let _ = writeln!(
        out,
        "openduct_connections {}",
        m.connections_open.load(Ordering::Relaxed).max(0)
    );
    help(
        out,
        "openduct_connections_accepted_total",
        "counter",
        "Client connections accepted.",
    );
    let _ = writeln!(
        out,
        "openduct_connections_accepted_total {}",
        m.connections_accepted.load(Ordering::Relaxed)
    );
    help(
        out,
        "openduct_connections_refused_total",
        "counter",
        "Client connections refused before any request: an address over its share of connections, or one kept out \
         after failing to sign in too often.",
    );
    let _ = writeln!(
        out,
        "openduct_connections_refused_total {}",
        m.connections_refused.load(Ordering::Relaxed)
    );
    help(
        out,
        "openduct_sign_in_failures_total",
        "counter",
        "Failed sign-ins (SASL).",
    );
    let _ = writeln!(
        out,
        "openduct_sign_in_failures_total {}",
        state.config.security.sign_in_failures()
    );

    help(
        out,
        "openduct_produced_bytes_total",
        "counter",
        "Record batch bytes in Produce requests.",
    );
    let _ = writeln!(
        out,
        "openduct_produced_bytes_total {}",
        m.produced_bytes.load(Ordering::Relaxed)
    );
    help(
        out,
        "openduct_fetched_bytes_total",
        "counter",
        "Record batch bytes in Fetch answers.",
    );
    let _ = writeln!(
        out,
        "openduct_fetched_bytes_total {}",
        m.fetched_bytes.load(Ordering::Relaxed)
    );

    help(
        out,
        "openduct_requests_total",
        "counter",
        "Requests, by Kafka API.",
    );
    for a in &m.apis {
        let _ = writeln!(
            out,
            "openduct_requests_total{{api=\"{}\"}} {}",
            a.name,
            a.count.load(Ordering::Relaxed)
        );
    }
    help(
        out,
        "openduct_request_failures_total",
        "counter",
        "Requests that could not be answered (the connection was closed), by Kafka API.",
    );
    for a in &m.apis {
        let _ = writeln!(
            out,
            "openduct_request_failures_total{{api=\"{}\"}} {}",
            a.name,
            a.failed.load(Ordering::Relaxed)
        );
    }
    help(
        out,
        "openduct_request_duration_seconds",
        "histogram",
        "From a request read to its answer ready, by Kafka API: a Produce until its batches are on the device, a \
         Fetch including its long poll.",
    );
    for a in &m.apis {
        let mut seen = 0u64;
        for (bound, count) in BUCKETS.iter().zip(&a.buckets) {
            seen += count.load(Ordering::Relaxed);
            let _ = writeln!(
                out,
                "openduct_request_duration_seconds_bucket{{api=\"{}\",le=\"{bound}\"}} {seen}",
                a.name
            );
        }
        let total = a.count.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "openduct_request_duration_seconds_bucket{{api=\"{}\",le=\"+Inf\"}} {total}",
            a.name
        );
        #[allow(clippy::cast_precision_loss)] // microseconds summed: exact far beyond any uptime
        let sum = a.micros.load(Ordering::Relaxed) as f64 / 1e6;
        let _ = writeln!(
            out,
            "openduct_request_duration_seconds_sum{{api=\"{}\"}} {sum}",
            a.name
        );
        let _ = writeln!(
            out,
            "openduct_request_duration_seconds_count{{api=\"{}\"}} {total}",
            a.name
        );
    }
}

/// What the log holds now: the partitions this broker leads, and the groups it coordinates.
fn log_gauges(state: &State) -> String {
    let store = state.store.as_ref();
    let me = store.cluster().map(|c| c.me);
    let mut out = String::new();
    let mut ends = std::collections::HashMap::new();
    // Each family's samples together, as the text format asks: the end offsets, then the start offsets.
    let (mut end_lines, mut start_lines) = (String::new(), String::new());
    let (mut led, mut leaderless) = (0u64, 0u64);
    for topic in store.topics() {
        for partition in 0..topic.partitions {
            let placement = store.placement(&topic.name, partition);
            if placement.as_ref().is_some_and(|p| p.leader.is_none()) {
                leaderless += 1;
            }
            let leads = match (&placement, me) {
                (Some(p), Some(me)) => p.leader == Some(me),
                _ => true,
            };
            if !leads {
                continue;
            }
            led += 1;
            let Ok(offsets) = store.offsets(&topic.name, partition) else {
                continue;
            };
            ends.insert((topic.name.clone(), partition), offsets.high_watermark);
            let t = label(&topic.name);
            let _ = writeln!(
                end_lines,
                "openduct_partition_log_end_offset{{topic=\"{t}\",partition=\"{partition}\"}} {}",
                offsets.high_watermark
            );
            let _ = writeln!(
                start_lines,
                "openduct_partition_log_start_offset{{topic=\"{t}\",partition=\"{partition}\"}} {}",
                offsets.log_start
            );
        }
    }
    help(
        &mut out,
        "openduct_partitions_led",
        "gauge",
        "Partitions this broker leads (every one, on its own).",
    );
    let _ = writeln!(out, "openduct_partitions_led {led}");
    help(
        &mut out,
        "openduct_partitions_without_leader",
        "gauge",
        "Partitions of the cluster with no leader now, as this broker sees them (none on its own): nothing can be \
         written to or read from them until one is chosen.",
    );
    let _ = writeln!(out, "openduct_partitions_without_leader {leaderless}");
    help(
        &mut out,
        "openduct_partition_log_end_offset",
        "gauge",
        "The next offset to be written (the high watermark), by partition this broker leads.",
    );
    out.push_str(&end_lines);
    help(
        &mut out,
        "openduct_partition_log_start_offset",
        "gauge",
        "The first offset still kept (retention and deleted records move it), by partition this broker leads.",
    );
    out.push_str(&start_lines);
    help(
        &mut out,
        "openduct_consumer_group_lag",
        "gauge",
        "Messages a consumer group has still to read: the log end offset less its committed offset, by group and \
         partition, for the groups this broker coordinates.",
    );
    let groups = store.groups().unwrap_or_default();
    for group in groups {
        if me.is_some() && store.group_coordinator(&group) != me {
            continue;
        }
        let Ok(committed) = store.committed_offsets(&group) else {
            continue;
        };
        for c in committed {
            // A partition led here is already read; one led elsewhere is read from this broker's copy.
            let Some(end) = ends.get(&(c.topic.clone(), c.partition)).copied().or_else(|| {
                store
                    .offsets(&c.topic, c.partition)
                    .ok()
                    .map(|o| o.high_watermark)
            }) else {
                continue;
            };
            if c.offset < 0 {
                continue;
            }
            let _ = writeln!(
                out,
                "openduct_consumer_group_lag{{group=\"{}\",topic=\"{}\",partition=\"{}\"}} {}",
                label(&group),
                label(&c.topic),
                c.partition,
                (end - c.offset).max(0)
            );
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use kafka_protocol::messages::ApiKey;

    #[test]
    fn a_request_lands_in_its_api_and_bucket() {
        let m = Metrics::new();
        m.request(ApiKey::Produce as i16, Duration::from_millis(3), true);
        m.request(ApiKey::Produce as i16, Duration::from_millis(40), false);
        m.request(9_999, Duration::from_millis(1), false);
        let produce = m.apis.iter().find(|a| a.name == "Produce").unwrap();
        assert_eq!(produce.count.load(Ordering::Relaxed), 2);
        assert_eq!(produce.failed.load(Ordering::Relaxed), 1);
        // 3 ms is under the 5 ms bound, 40 ms under 50 ms.
        assert_eq!(produce.buckets[2].load(Ordering::Relaxed), 1);
        assert_eq!(produce.buckets[5].load(Ordering::Relaxed), 1);
        let other = m.apis.last().unwrap();
        assert_eq!((other.name, other.count.load(Ordering::Relaxed)), ("other", 1));
    }

    #[test]
    fn label_values_are_escaped() {
        assert_eq!(label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}

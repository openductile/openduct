// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Each principal held to its share: bytes produced and fetched per second, set per principal (or for every one,
//! `User:*`) in the rules file. A client over its quota is not refused: its answers are held back as long as its
//! excess needs - a loud tenant slows itself, not the others. (`throttle_time_ms` is left 0: some clients take a
//! throttled answer as a reason to send the request again.)
//! And no one address may hold more than its share of the broker's connections.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::Principal;

/// A principal's share; `None` is unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Quota {
    #[serde(default)]
    pub produce_bytes_per_sec: Option<u64>,
    #[serde(default)]
    pub fetch_bytes_per_sec: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Kind {
    Produce,
    Fetch,
}

/// The longest an answer is held back at once.
const MAX_THROTTLE: Duration = Duration::from_secs(10);
/// How much a principal may run ahead of its rate before it is held back (one second's worth).
const BURST: Duration = Duration::from_secs(1);

#[derive(Debug)]
struct Bucket {
    /// Bytes the principal may still send now (negative: owed).
    tokens: f64,
    at: Instant,
}

/// Every principal's use of its quotas on this broker.
#[derive(Debug, Default)]
pub(crate) struct Meter {
    buckets: Mutex<HashMap<(Principal, Kind), Bucket>>,
}

impl Meter {
    /// Records `bytes` used; how long the answer is held back to keep the principal to `rate` bytes a second.
    pub(crate) fn charge(&self, principal: &Principal, kind: Kind, bytes: u64, rate: u64) -> Duration {
        if rate == 0 {
            return MAX_THROTTLE;
        }
        let rate = rate as f64;
        let now = Instant::now();
        let mut buckets = self.buckets.lock();
        if buckets.len() > 100_000 {
            buckets.retain(|_, b| now.duration_since(b.at) < Duration::from_secs(60));
        }
        let b = buckets.entry((principal.clone(), kind)).or_insert(Bucket {
            tokens: rate * BURST.as_secs_f64(),
            at: now,
        });
        let refill = now.duration_since(b.at).as_secs_f64() * rate;
        b.tokens = (b.tokens + refill).min(rate * BURST.as_secs_f64());
        b.at = now;
        b.tokens -= bytes as f64;
        if b.tokens >= 0.0 {
            return Duration::ZERO;
        }
        Duration::from_secs_f64((-b.tokens / rate).min(MAX_THROTTLE.as_secs_f64()))
    }
}

/// Connections per address, held to a ceiling.
#[derive(Debug, Default)]
pub(crate) struct Connections {
    open: Mutex<HashMap<IpAddr, usize>>,
}

impl Connections {
    /// Takes a connection for `ip`, unless it already holds `max`.
    pub(crate) fn open(&self, ip: IpAddr, max: usize) -> bool {
        let mut open = self.open.lock();
        let n = open.entry(ip).or_insert(0);
        if *n >= max {
            return false;
        }
        *n += 1;
        true
    }

    pub(crate) fn close(&self, ip: IpAddr) {
        let mut open = self.open.lock();
        if let Some(n) = open.get_mut(&ip) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                open.remove(&ip);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_principal_over_its_rate_is_held_back_in_proportion_and_others_are_not() {
        let m = Meter::default();
        let (loud, quiet) = (Principal::user("loud"), Principal::user("quiet"));
        // One second's worth goes through at once.
        assert_eq!(m.charge(&loud, Kind::Produce, 1000, 1000), Duration::ZERO);
        // Two seconds' worth more: held back about two seconds.
        let held = m.charge(&loud, Kind::Produce, 2000, 1000);
        assert!(
            held > Duration::from_millis(1900) && held <= Duration::from_millis(2001),
            "{held:?}"
        );
        assert_eq!(m.charge(&quiet, Kind::Produce, 500, 1000), Duration::ZERO);
        assert_eq!(
            m.charge(&loud, Kind::Fetch, 500, 1000),
            Duration::ZERO,
            "fetch has its own share"
        );
    }

    #[test]
    fn an_address_gets_no_more_than_its_connections() {
        let c = Connections::default();
        let ip: IpAddr = "10.0.0.9".parse().unwrap();
        assert!(c.open(ip, 2) && c.open(ip, 2));
        assert!(!c.open(ip, 2));
        c.close(ip);
        assert!(c.open(ip, 2));
    }
}

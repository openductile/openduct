// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! How many messages a chat takes a minute: per conversation and per address, as each workflow's trigger says, on
//! this node. Counted in windows of a minute; the counts kept are bounded, so a flood of new conversations cannot
//! fill the memory - past the bound, a new one waits for the minute to pass.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);
/// The most counts kept at once.
const MAX_KEYS: usize = 200_000;

#[derive(Debug, Default)]
pub(super) struct Rates {
    seen: Mutex<HashMap<String, (Instant, u32)>>,
}

impl Rates {
    /// Whether one more is taken under `per_minute` (0: no limit), counted when it is.
    pub(super) fn allow(&self, key: &str, per_minute: u32) -> bool {
        self.allow_at(key, per_minute, Instant::now())
    }

    fn allow_at(&self, key: &str, per_minute: u32, now: Instant) -> bool {
        if per_minute == 0 {
            return true;
        }
        let Ok(mut seen) = self.seen.lock() else {
            return false;
        };
        if seen.len() >= MAX_KEYS && !seen.contains_key(key) {
            seen.retain(|_, (since, _)| now.saturating_duration_since(*since) < WINDOW);
            if seen.len() >= MAX_KEYS {
                return false;
            }
        }
        let entry = seen.entry(key.to_owned()).or_insert((now, 0));
        if now.saturating_duration_since(entry.0) >= WINDOW {
            *entry = (now, 0);
        }
        if entry.1 >= per_minute {
            return false;
        }
        entry.1 += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_takes_its_number_a_minute_and_others_are_counted_apart() {
        let rates = Rates::default();
        let start = Instant::now();
        for _ in 0..3 {
            assert!(rates.allow_at("a", 3, start));
        }
        assert!(!rates.allow_at("a", 3, start), "the fourth in the minute");
        assert!(rates.allow_at("b", 3, start), "another key");
        assert!(rates.allow_at("a", 3, start + WINDOW), "the next minute");
        assert!((0..1000).all(|_| rates.allow_at("c", 0, start)), "0: no limit");
    }

    #[test]
    fn the_counts_kept_are_bounded() {
        let rates = Rates::default();
        let start = Instant::now();
        {
            let mut seen = rates.seen.lock().unwrap();
            for i in 0..MAX_KEYS {
                seen.insert(format!("k{i}"), (start, 1));
            }
        }
        assert!(
            !rates.allow_at("new", 5, start),
            "full, and every count is of this minute"
        );
        assert!(rates.allow_at("k1", 5, start), "a key already counted goes on");
        assert!(
            rates.allow_at("new", 5, start + WINDOW),
            "a minute later the old counts go"
        );
        assert!(rates.seen.lock().unwrap().len() < 10);
    }
}

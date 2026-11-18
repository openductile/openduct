// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What workflows measure across this process, in the Prometheus text format: runs by how they ended and how long
//! they took, and AI calls with the tokens they used. Totals only - no workspace, workflow or model is named, so
//! metrics never tell one workspace about another. A workflow's own figures are in the studio.
//!
//! Only real runs count (the runner's); a test or preview does not. An AI call counts wherever it is made, preview
//! included: its tokens were spent.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// The bounds of the run duration histogram, in seconds.
const BUCKETS: [f64; 10] = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0];

static RUNS_DONE: AtomicU64 = AtomicU64::new(0);
static RUNS_SKIPPED: AtomicU64 = AtomicU64::new(0);
static RUNS_FAILED: AtomicU64 = AtomicU64::new(0);
static RUN_MICROS: AtomicU64 = AtomicU64::new(0);
static RUN_BUCKETS: [AtomicU64; BUCKETS.len()] = [const { AtomicU64::new(0) }; BUCKETS.len()];
static AI_CALLS: AtomicU64 = AtomicU64::new(0);
static AI_PROMPT_TOKENS: AtomicU64 = AtomicU64::new(0);
static AI_COMPLETION_TOKENS: AtomicU64 = AtomicU64::new(0);

/// How a run ended.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Ended {
    Done,
    Skipped,
    Failed,
}

/// A real run ended after `took`.
pub(crate) fn run(ended: Ended, took: Duration) {
    match ended {
        Ended::Done => &RUNS_DONE,
        Ended::Skipped => &RUNS_SKIPPED,
        Ended::Failed => &RUNS_FAILED,
    }
    .fetch_add(1, Ordering::Relaxed);
    RUN_MICROS.fetch_add(
        u64::try_from(took.as_micros()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    let seconds = took.as_secs_f64();
    if let Some((_, bucket)) = BUCKETS
        .iter()
        .zip(&RUN_BUCKETS)
        .find(|(bound, _)| seconds <= **bound)
    {
        bucket.fetch_add(1, Ordering::Relaxed);
    }
}

/// An AI call answered, with the tokens the model reported (none when it reported none).
pub(crate) fn ai_call(prompt_tokens: Option<u64>, completion_tokens: Option<u64>) {
    AI_CALLS.fetch_add(1, Ordering::Relaxed);
    AI_PROMPT_TOKENS.fetch_add(prompt_tokens.unwrap_or(0), Ordering::Relaxed);
    AI_COMPLETION_TOKENS.fetch_add(completion_tokens.unwrap_or(0), Ordering::Relaxed);
}

/// The workflow metrics now, in the Prometheus text format.
#[must_use]
pub fn prometheus() -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HELP openduct_workflow_runs_total Workflow runs, by how they ended."
    );
    let _ = writeln!(out, "# TYPE openduct_workflow_runs_total counter");
    for (outcome, count) in [
        ("done", &RUNS_DONE),
        ("skipped", &RUNS_SKIPPED),
        ("failed", &RUNS_FAILED),
    ] {
        let _ = writeln!(
            out,
            "openduct_workflow_runs_total{{outcome=\"{outcome}\"}} {}",
            count.load(Ordering::Relaxed)
        );
    }
    let _ = writeln!(
        out,
        "# HELP openduct_workflow_run_duration_seconds How long workflow runs took."
    );
    let _ = writeln!(out, "# TYPE openduct_workflow_run_duration_seconds histogram");
    let mut seen = 0u64;
    for (bound, count) in BUCKETS.iter().zip(&RUN_BUCKETS) {
        seen += count.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "openduct_workflow_run_duration_seconds_bucket{{le=\"{bound}\"}} {seen}"
        );
    }
    let total = RUNS_DONE.load(Ordering::Relaxed)
        + RUNS_SKIPPED.load(Ordering::Relaxed)
        + RUNS_FAILED.load(Ordering::Relaxed);
    let _ = writeln!(
        out,
        "openduct_workflow_run_duration_seconds_bucket{{le=\"+Inf\"}} {total}"
    );
    #[allow(clippy::cast_precision_loss)] // microseconds summed: exact far beyond any uptime
    let sum = RUN_MICROS.load(Ordering::Relaxed) as f64 / 1e6;
    let _ = writeln!(out, "openduct_workflow_run_duration_seconds_sum {sum}");
    let _ = writeln!(out, "openduct_workflow_run_duration_seconds_count {total}");
    let _ = writeln!(out, "# HELP openduct_ai_calls_total AI model calls answered.");
    let _ = writeln!(out, "# TYPE openduct_ai_calls_total counter");
    let _ = writeln!(
        out,
        "openduct_ai_calls_total {}",
        AI_CALLS.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "# HELP openduct_ai_tokens_total Tokens AI models reported using, by kind."
    );
    let _ = writeln!(out, "# TYPE openduct_ai_tokens_total counter");
    let _ = writeln!(
        out,
        "openduct_ai_tokens_total{{kind=\"prompt\"}} {}",
        AI_PROMPT_TOKENS.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        out,
        "openduct_ai_tokens_total{{kind=\"completion\"}} {}",
        AI_COMPLETION_TOKENS.load(Ordering::Relaxed)
    );
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn runs_and_ai_calls_are_counted_and_shown() {
        let before = prometheus();
        run(Ended::Failed, Duration::from_millis(30));
        ai_call(Some(12), Some(5));
        let after = prometheus();
        let value = |text: &str, line: &str| -> u64 {
            text.lines()
                .find(|l| l.starts_with(line))
                .and_then(|l| l.rsplit(' ').next())
                .unwrap()
                .parse()
                .unwrap()
        };
        let failed = "openduct_workflow_runs_total{outcome=\"failed\"}";
        assert_eq!(value(&after, failed), value(&before, failed) + 1);
        let prompt = "openduct_ai_tokens_total{kind=\"prompt\"}";
        assert_eq!(value(&after, prompt), value(&before, prompt) + 12);
        assert!(after.contains("openduct_workflow_run_duration_seconds_bucket{le=\"+Inf\"}"));
    }
}

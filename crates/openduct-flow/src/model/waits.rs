// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the steps that wait for something outside the run: a form, an approval, a call to a one-time address, a
//! time (see [`crate::waits`]).

use std::time::Duration;

use serde_json::{Map, Value as Json};

use super::{Kind, MAX_WAIT, Reader, duration, text};
use crate::forms::{FormSpec, MAX_TIMEOUT, TIMEOUT, Use};
use crate::waits::{OnTimeout, Pause, PauseOn};

fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

/// How long a step waits for its answer, and what the run does when it does not come.
fn timeouts(s: &Map<String, Json>, at: &str) -> Result<(Duration, OnTimeout), String> {
    let timeout = match text(s.get("timeout")) {
        None => TIMEOUT,
        Some(t) => duration(t)
            .filter(|d| *d >= Duration::from_secs(1) && *d <= MAX_TIMEOUT)
            .ok_or_else(|| {
                format!("{at}.timeout: how long it waits for its answer, as 2h or 3d, at most 90d")
            })?,
    };
    let on_timeout = match text(s.get("on_timeout")) {
        None | Some("fail") => OnTimeout::Fail,
        Some("continue") => OnTimeout::Continue,
        Some(other) => return Err(format!("{at}.on_timeout: fail or continue, not {other:?}")),
    };
    Ok((timeout, on_timeout))
}

impl Reader {
    /// A form step, or an approval: the run waits for the form to be filled in.
    pub(super) fn form_step(&self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        let usage = if kind == "approval" {
            Use::Approval
        } else {
            Use::Step
        };
        let spec = FormSpec::read(s, at, &self.language, usage)?;
        let (timeout, on_timeout) = timeouts(s, at)?;
        Ok(Kind::Pause(Pause {
            on: PauseOn::Form(Box::new(spec)),
            timeout,
            on_timeout,
        }))
    }

    /// A wait kept on the log - for a call to its address, until a time, or for longer than a run is held - or
    /// `None` for a wait held within the run (an hour at most).
    pub(super) fn durable_wait(&self, body: &Json, at: &str) -> Result<Option<Kind>, String> {
        let s = match body {
            Json::String(t) => {
                if duration(t).is_some_and(|d| d > MAX_TIMEOUT) {
                    return Err(format!("{at}: at most 90d"));
                }
                return Ok(duration(t).filter(|d| *d > MAX_WAIT).map(|d| {
                    Kind::Pause(Pause {
                        on: PauseOn::For(d),
                        timeout: d,
                        on_timeout: OnTimeout::Continue,
                    })
                }));
            }
            Json::Object(s) => s,
            _ => return Ok(None),
        };
        const SETTINGS: &[&str] = &["for", "until", "webhook", "timeout", "on_timeout"];
        if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of wait ({})",
                SETTINGS.join(", ")
            ));
        }
        let ways = ["for", "until", "webhook"]
            .iter()
            .filter(|w| s.contains_key(**w))
            .count();
        if ways != 1 {
            return Err(format!(
                "{at}: one of for (a while), until (a time) or webhook (a call to its address)"
            ));
        }
        if let Some(until) = s.get("until") {
            if s.contains_key("timeout") || s.contains_key("on_timeout") {
                return Err(format!(
                    "{at}: a wait until a time has no timeout: the time comes"
                ));
            }
            return Ok(Some(Kind::Pause(Pause {
                on: PauseOn::Until(self.expression(until, &format!("{at}.until"))?),
                timeout: MAX_TIMEOUT,
                on_timeout: OnTimeout::Continue,
            })));
        }
        if let Some(webhook) = s.get("webhook") {
            if !matches!(webhook, Json::Bool(true) | Json::Null)
                && webhook.as_object().is_none_or(|w| !w.is_empty())
            {
                return Err(format!(
                    "{at}.webhook: true (or {{}}): the run waits for a call to its address"
                ));
            }
            let (timeout, on_timeout) = timeouts(s, at)?;
            return Ok(Some(Kind::Pause(Pause {
                on: PauseOn::Webhook,
                timeout,
                on_timeout,
            })));
        }
        let written = text(s.get("for")).unwrap_or_default();
        let wait = duration(written).ok_or_else(|| format!("{at}.for: how long, as 10s, 5m or 2d"))?;
        if s.contains_key("timeout") || s.contains_key("on_timeout") {
            return Err(format!("{at}: a wait for a while has no timeout: the while ends"));
        }
        if wait <= MAX_WAIT {
            return Ok(None);
        }
        if wait > MAX_TIMEOUT {
            return Err(format!("{at}.for: at most 90d"));
        }
        Ok(Some(Kind::Pause(Pause {
            on: PauseOn::For(wait),
            timeout: wait,
            on_timeout: OnTimeout::Continue,
        })))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::panic)]

    use crate::model::{Kind, Trigger, Workflow};
    use crate::waits::{OnTimeout, PauseOn};

    fn first(steps: &str) -> Result<Kind, String> {
        Workflow::parse(&format!("name: w\ntrigger: webhook\nsteps:\n{steps}"))
            .map(|w| w.steps[0].kind.clone())
    }

    #[test]
    fn waits_read_as_held_or_kept_on_the_log() {
        assert!(matches!(first("  - wait: 10s\n").unwrap(), Kind::Wait(_)));
        assert!(matches!(first("  - wait: { for: 1h }\n").unwrap(), Kind::Wait(_)));
        let Kind::Pause(p) = first("  - wait: 2d\n").unwrap() else {
            panic!("a long wait is kept")
        };
        assert!(matches!(p.on, PauseOn::For(_)));
        let Kind::Pause(p) =
            first("  - wait: { webhook: true, timeout: 1h, on_timeout: continue }\n").unwrap()
        else {
            panic!("a wait for a call")
        };
        assert!(matches!(p.on, PauseOn::Webhook));
        assert_eq!((p.timeout.as_secs(), p.on_timeout), (3600, OnTimeout::Continue));
        let Kind::Pause(p) = first("  - wait: { until: \"value.at\" }\n").unwrap() else {
            panic!("until")
        };
        assert!(matches!(p.on, PauseOn::Until(_)));
        for wrong in [
            "  - wait: { for: 1s, until: \"value.at\" }\n",
            "  - wait: { webhook: true, timeout: 91d }\n",
            "  - wait: { webhook: true, on_timeout: maybe }\n",
            "  - wait: { until: \"value.at\", timeout: 1h }\n",
            "  - wait: { for: 100d }\n",
            "  - wait: { for: 10s, colour: red }\n",
        ] {
            assert!(first(wrong).is_err(), "{wrong}");
        }
    }

    #[test]
    fn a_form_and_an_approval_are_steps_that_wait() {
        let Kind::Pause(p) = first(
            "  - name: ask\n    form:\n      title: Size?\n      fields: [{ name: size, type: dropdown, options: [S, M] }]\n      timeout: 3d\n",
        )
        .unwrap() else {
            panic!("a form step")
        };
        assert_eq!(
            (p.kind_name(), p.timeout.as_secs(), p.on_timeout),
            ("form", 3 * 86400, OnTimeout::Fail)
        );
        let Kind::Pause(p) =
            first("  - name: ok\n    approval: { title: Refund?, comment: true }\n").unwrap()
        else {
            panic!("an approval")
        };
        assert_eq!(p.kind_name(), "approval");
        assert!(
            first("  - form: { title: T }\n")
                .unwrap_err()
                .contains("at least one field")
        );
    }

    #[test]
    fn a_form_trigger_reads_its_fields() {
        let w = Workflow::parse(
            "name: w\ntrigger:\n  form:\n    title: Contact us\n    fields:\n      - { name: email, type: email, required: true }\n    redirect: https://example.com/thanks\nsteps: []\n",
        )
        .unwrap();
        let Trigger::Form(form) = w.trigger else {
            panic!("a form trigger")
        };
        assert_eq!(form.title, "Contact us");
        assert_eq!(form.redirect, "https://example.com/thanks");
        assert!(
            Workflow::parse("name: w\ntrigger: { form: { title: T, fields: [], colour: red } }\nsteps: []\n")
                .is_err()
        );
    }
}

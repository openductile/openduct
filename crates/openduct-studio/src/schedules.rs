// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `POST /api/schedule`: a schedule trigger's settings, read as a run reads them - what it is in plain words and
//! the next times it runs, on its own clock and in UTC - or what is wrong with them, for the trigger's window to show
//! while it is being set.

use hyper::StatusCode;
use openduct_flow::Workflow;
use openduct_flow::calendar::{self, MAX_SHOWN};
use serde_json::{Value, json};

use crate::State;
use crate::api::access;
use crate::http::{Answer, Call, json, problem};
use crate::model::Role;

/// How many next times are shown unless more are asked for.
const SHOWN: usize = 5;

pub(crate) async fn next(state: &State, call: &Call) -> Answer {
    if let Err(answer) = access(state, call, Role::Viewer).await {
        return answer;
    }
    let asked = call.body.get("schedule");
    match answer(asked, call.body.get("count"), crate::workflows::now_ms()) {
        Ok(found) => json(StatusCode::OK, &found),
        Err(why) => problem(StatusCode::UNPROCESSABLE_ENTITY, &why),
    }
}

/// What a schedule's settings say, and the times it runs next after `now` (milliseconds since 1970).
fn answer(settings: Option<&Value>, count: Option<&Value>, now: i64) -> Result<Value, String> {
    let definition = json!({
        "name": "schedule",
        "trigger": { "schedule": settings.cloned().unwrap_or(Value::Null) },
    });
    let workflow = Workflow::from_json(&definition, Some("schedule"))
        .map_err(|e| e.strip_prefix("trigger: ").map(str::to_owned).unwrap_or(e))?;
    let count = count
        .and_then(Value::as_u64)
        .map_or(SHOWN, |n| usize::try_from(n).unwrap_or(MAX_SHOWN))
        .clamp(1, MAX_SHOWN);
    let says =
        calendar::says(&workflow.trigger).ok_or("a schedule's settings: every, at, on, cron, timezone")?;
    let times = calendar::next_runs(&workflow.trigger, now, count);
    let timezone = match &workflow.trigger {
        openduct_flow::Trigger::Calendar(c) => c.timezone().to_owned(),
        _ => "UTC".to_owned(),
    };
    Ok(json!({
        "says": says,
        "timezone": timezone,
        "next": times.iter().map(|t| openduct_flow::runner::iso(*t)).collect::<Vec<_>>(),
        "local": times.iter().map(|t| calendar::local(&workflow.trigger, *t)).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    /// Friday 27 November 2026, 10:00 UTC.
    const NOW: i64 = 1_795_773_600_000;

    #[test]
    fn set_times_are_said_and_shown_on_their_own_clock() {
        let settings = json!({ "every": "weekday", "at": "09:00", "timezone": "Europe/Berlin" });
        let found = answer(Some(&settings), Some(&json!(2)), NOW).unwrap();
        assert_eq!(
            found,
            json!({
                "says": "every weekday at 09:00 (Europe/Berlin)",
                "timezone": "Europe/Berlin",
                "next": ["2026-11-30T08:00:00Z", "2026-12-01T08:00:00Z"],
                "local": ["2026-11-30T09:00:00+01:00", "2026-12-01T09:00:00+01:00"],
            })
        );
    }

    #[test]
    fn an_interval_is_shown_too_and_the_count_is_bounded() {
        let found = answer(Some(&json!({ "every": "15m" })), Some(&json!(100_000)), NOW).unwrap();
        assert_eq!(found["says"], "every 15m, counted from midnight UTC");
        assert_eq!(found["next"].as_array().unwrap().len(), MAX_SHOWN);
        assert_eq!(found["next"][0], "2026-11-27T10:15:00Z");
        let found = answer(Some(&json!({ "cron": "0 9 * * *" })), None, NOW).unwrap();
        assert_eq!(found["next"].as_array().unwrap().len(), SHOWN);
    }

    #[test]
    fn a_mistake_is_said_in_plain_words() {
        let wrong = answer(Some(&json!({ "every": "day", "at": "25:00" })), None, NOW).unwrap_err();
        assert!(
            wrong.starts_with("schedule.at: \"25:00\" is not a time of day"),
            "{wrong}"
        );
        let wrong = answer(None, None, NOW).unwrap_err();
        assert!(wrong.starts_with("schedule: how often"), "{wrong}");
    }
}

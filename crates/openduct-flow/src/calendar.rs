// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Schedules at set times: a cron expression, or the plain forms - every day, every weekday, every week on chosen
//! days, every month on chosen dates, each at a time of day, and every hour at a minute - in a time zone of the
//! schedule's own (UTC unless it says).
//!
//! Daylight saving: a time the clock skips when it goes forward runs once, at the moment it jumps (02:30 on that
//! night runs at 03:00); a time the clock passes twice when it goes back runs once, the first time. A daily schedule
//! runs once a day across both changes; an hourly one at each hour the clock shows, once - so on the night the clock
//! goes back, the hour it repeats is not run again. (An interval, `every: 15m`, keeps to real time in every zone.)
//!
//! Every node of a cluster works out the same times, and queues each one to the schedule's partition, where it is
//! claimed before it runs (see [`crate::runner`]): each time runs once, across a node's loss too.

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;
use openduct_broker::client::Shared;
use serde_json::{Map, Value as Json};
use tokio::sync::watch;

use crate::model::Trigger;

mod cron;
mod plain;

pub(crate) use cron::Cron;

/// The most times (or cron expressions) one schedule lists.
pub const MAX_TIMES: usize = 24;
/// The most next times shown at once.
pub const MAX_SHOWN: usize = 50;
/// The candidates one next time looks through: a schedule of every second, from inside the hour the clock repeats.
const MAX_TRIES: usize = 100_000;
/// The longest a schedule's loop sleeps before it looks at the clock again: a clock set, or a machine that slept, is
/// noticed within it.
const LOOK_AGAIN: Duration = Duration::from_secs(60);

/// A schedule at set times, in a time zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Calendar {
    /// It runs at the times of each.
    entries: Vec<Cron>,
    zone: Tz,
    /// What it is, in plain words: `every weekday at 09:00 (Europe/Berlin)`.
    says: String,
}

impl Calendar {
    /// A schedule trigger's settings, when they name set times (`cron`, or `every: day` with `at`, ...); `None` when
    /// they are an interval (`every: 15m`), which the schedule trigger reads itself.
    pub(crate) fn from_settings(settings: &Map<String, Json>) -> Result<Option<Self>, String> {
        const ALLOWED: &[&str] = &["every", "at", "on", "cron", "timezone"];
        if let Some(k) = settings.keys().find(|k| !ALLOWED.contains(&k.as_str())) {
            return Err(format!(
                "schedule.{k}: not a setting of schedule (every, at, on, cron, timezone)"
            ));
        }
        let every = settings.get("every");
        let interval = every
            .and_then(Json::as_str)
            .is_some_and(|e| plain::Period::of(e).is_none());
        if settings.get("cron").is_none() && (interval || every.is_some_and(|e| !e.is_string())) {
            // An interval: it runs on the clock itself, the same in every time zone.
            for k in ["at", "on", "timezone"] {
                if settings.contains_key(k) {
                    return Err(format!(
                        "schedule.{k}: goes with set times - every: day, weekday, week, month or hour, or cron - \
                         not with an interval (every: 15m runs every 15 minutes of the clock, counted from \
                         midnight UTC)"
                    ));
                }
            }
            return Ok(None);
        }
        let zone = match settings.get("timezone") {
            None | Some(Json::Null) => Tz::UTC,
            Some(Json::String(name)) if name.trim().is_empty() => Tz::UTC,
            Some(Json::String(name)) => name.trim().parse::<Tz>().map_err(|_| {
                format!("schedule.timezone: {name:?} is not a time zone: a name as Europe/London or America/New_York")
            })?,
            Some(_) => return Err("schedule.timezone: a time zone's name, as Europe/London".to_owned()),
        };
        let (entries, says) = match (settings.get("cron"), every) {
            (Some(cron), None) => {
                for k in ["at", "on"] {
                    if settings.contains_key(k) {
                        return Err(format!(
                            "schedule.{k}: a cron expression names its own times: give cron, or every with at"
                        ));
                    }
                }
                crons(cron)?
            }
            (Some(_), Some(_)) => {
                return Err("schedule: either every or cron, not both".to_owned());
            }
            (None, Some(every)) => plain::read(every, settings.get("at"), settings.get("on"))?,
            (None, None) => {
                return Err(
                    "schedule: how often - every: 15m, every: day with at: \"09:00\", or cron: \
                     \"0 9 * * 1-5\""
                        .to_owned(),
                );
            }
        };
        Ok(Some(Self {
            entries,
            zone,
            says: format!("{says} ({})", zone.name()),
        }))
    }

    /// What it is, in plain words.
    pub fn says(&self) -> &str {
        &self.says
    }

    /// Its time zone's name: `UTC`, `Europe/Berlin`.
    pub fn timezone(&self) -> &str {
        self.zone.name()
    }

    /// The first time it runs after `after` (milliseconds since 1970), as milliseconds since 1970.
    pub fn next_after(&self, after: i64) -> Option<i64> {
        self.entries.iter().filter_map(|c| self.next_of(c, after)).min()
    }

    /// The next `count` times it runs after `after` (at most [`MAX_SHOWN`]).
    pub fn next(&self, after: i64, count: usize) -> Vec<i64> {
        let mut out = Vec::new();
        let mut from = after;
        while out.len() < count.min(MAX_SHOWN) {
            let Some(at) = self.next_after(from) else {
                break;
            };
            out.push(at);
            from = at;
        }
        out
    }

    /// A time as the schedule's clock shows it: `2026-11-30T09:00:00+01:00`.
    pub fn local(&self, at: i64) -> String {
        DateTime::<Utc>::from_timestamp_millis(at).map_or_else(
            || at.to_string(),
            |t| {
                t.with_timezone(&self.zone)
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, false)
            },
        )
    }

    /// The first moment after `after` that one entry names.
    fn next_of(&self, cron: &Cron, after: i64) -> Option<i64> {
        let instant = DateTime::<Utc>::from_timestamp_millis(after)?;
        let mut from = instant
            .with_timezone(&self.zone)
            .naive_local()
            .with_nanosecond(0)?
            .checked_add_signed(chrono::TimeDelta::seconds(1))?;
        // The clock face goes forward; the moments it names do too - except inside an hour the clock repeats, whose
        // times ran the first time round: those are passed over.
        for _ in 0..MAX_TRIES {
            let face = cron.next_from(from)?;
            let at = moment(self.zone, face)?;
            if at > after {
                return Some(at);
            }
            from = face.checked_add_signed(chrono::TimeDelta::seconds(1))?;
        }
        None
    }
}

/// `cron:` as one expression or a list of them.
fn crons(value: &Json) -> Result<(Vec<Cron>, String), String> {
    let texts: Vec<&str> = match value {
        Json::String(s) => vec![s.as_str()],
        Json::Array(list) if !list.is_empty() && list.len() <= MAX_TIMES => list
            .iter()
            .map(|v| v.as_str().ok_or("schedule.cron: each a cron expression, as text"))
            .collect::<Result<_, _>>()?,
        Json::Array(_) => return Err(format!("schedule.cron: 1 to {MAX_TIMES} cron expressions")),
        _ => return Err("schedule.cron: a cron expression, as \"0 9 * * 1-5\"".to_owned()),
    };
    let entries = texts
        .iter()
        .map(|t| Cron::parse(t).map_err(|e| format!("schedule.cron: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    let shown: Vec<String> = texts.iter().map(|t| t.trim().to_owned()).collect();
    Ok((entries, format!("cron {}", shown.join(" and "))))
}

/// The moment a time on the zone's clock face is: the first of two when the clock passes it twice, the moment the
/// clock jumps when it skips it.
fn moment(zone: Tz, face: NaiveDateTime) -> Option<i64> {
    match zone.from_local_datetime(&face) {
        chrono::LocalResult::Single(t) => Some(t.timestamp_millis()),
        chrono::LocalResult::Ambiguous(first, _) => Some(first.timestamp_millis()),
        chrono::LocalResult::None => jumped(zone, face),
    }
}

/// The moment the clock jumped over `face`: the first second after it that the clock shows.
fn jumped(zone: Tz, face: NaiveDateTime) -> Option<i64> {
    let shown = |t: NaiveDateTime| zone.from_local_datetime(&t).earliest();
    // A clock skips an hour, sometimes half of one or two; once, in the Pacific, a whole day.
    let mut later = [1, 30, 60, 120, 180, 1500]
        .into_iter()
        .filter_map(|m| face.checked_add_signed(chrono::TimeDelta::minutes(m)))
        .find(|t| shown(*t).is_some())?
        .and_utc()
        .timestamp();
    let mut skipped = face.and_utc().timestamp();
    while later - skipped > 1 {
        let mid = skipped + (later - skipped) / 2;
        let t = DateTime::<Utc>::from_timestamp(mid, 0)?.naive_utc();
        if shown(t).is_some() {
            later = mid;
        } else {
            skipped = mid;
        }
    }
    shown(DateTime::<Utc>::from_timestamp(later, 0)?.naive_utc()).map(|t| t.timestamp_millis())
}

/// The next `count` times a trigger starts a run after `after` (milliseconds since 1970): for a schedule, an
/// interval's or set times'; nothing for any other trigger.
pub fn next_runs(trigger: &Trigger, after: i64, count: usize) -> Vec<i64> {
    let count = count.min(MAX_SHOWN);
    match trigger {
        Trigger::Every(every) => {
            let every = i64::try_from(every.as_millis()).unwrap_or(i64::MAX).max(1);
            let first = after.div_euclid(every).saturating_add(1).saturating_mul(every);
            (0..count)
                .map_while(|i| i64::try_from(i).ok()?.checked_mul(every)?.checked_add(first))
                .collect()
        }
        Trigger::Calendar(calendar) => calendar.next(after, count),
        _ => Vec::new(),
    }
}

/// A schedule trigger in plain words - `every weekday at 09:00 (Europe/Berlin)`, `every 15m, counted from midnight
/// UTC`; nothing for any other trigger.
pub fn says(trigger: &Trigger) -> Option<String> {
    match trigger {
        Trigger::Every(every) => Some(format!("every {}, counted from midnight UTC", interval(*every))),
        Trigger::Calendar(calendar) => Some(calendar.says().to_owned()),
        _ => None,
    }
}

/// A time as a trigger's clock shows it: in a schedule's time zone, UTC for anything else.
pub fn local(trigger: &Trigger, at: i64) -> String {
    match trigger {
        Trigger::Calendar(calendar) => calendar.local(at),
        _ => crate::runner::iso(at),
    }
}

/// An interval as it is written: `15m`, `1d`, `90s`, `500ms`.
fn interval(every: Duration) -> String {
    let ms = every.as_millis();
    [(86_400_000, "d"), (3_600_000, "h"), (60_000, "m"), (1_000, "s")]
        .into_iter()
        .find(|(unit, _)| ms > 0 && ms.is_multiple_of(*unit))
        .map_or_else(
            || format!("{ms}ms"),
            |(unit, name)| format!("{}{name}", ms / unit),
        )
}

/// Queues each of the schedule's times to the workflow's own topic as it comes, until `stop` says so. In a cluster
/// every node queues each time, to the schedule's partition and naming the time: the member reading that partition
/// claims each time before it runs it, so a time runs once - across a node's loss too.
pub async fn schedule(id: String, calendar: Calendar, shared: Arc<Shared>, mut stop: watch::Receiver<bool>) {
    loop {
        let Some(due) = calendar.next_after(crate::runner::now_ms()) else {
            // Nothing within the years looked through: look again later.
            tracing::warn!(
                workflow = id,
                "the schedule names no time ahead: looking again in an hour"
            );
            tokio::select! {
                () = tokio::time::sleep(Duration::from_secs(3600)) => continue,
                _ = stop.changed() => return,
            }
        };
        loop {
            let now = crate::runner::now_ms();
            if now >= due {
                break;
            }
            let wait = Duration::from_millis(u64::try_from(due - now).unwrap_or(0)).min(LOOK_AGAIN);
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                _ = stop.changed() => return,
            }
            if *stop.borrow() {
                return;
            }
        }
        let queued = async {
            let mut client = shared.lend().await.map_err(|e| e.to_string())?;
            crate::runner::queue_scheduled(&mut client, &id, due).await
        };
        if let Err(e) = queued.await {
            tracing::warn!(workflow = id, error = e, "a scheduled run could not be queued");
        }
    }
}

#[cfg(test)]
mod tests;

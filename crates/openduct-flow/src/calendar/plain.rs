// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The plain forms of a schedule at set times, each read into the cron entries it is:
//!
//! - `every: hour` with `at: ":15"` - minute 15 of every hour (`:00` unless it says);
//! - `every: day` with `at: "09:30"` - every day at 09:30;
//! - `every: weekday` with `at` - Monday to Friday;
//! - `every: week` with `on: [monday, thursday]` and `at`;
//! - `every: month` with `on: [1, 15, last]` and `at` - a date a month lacks (the 31st) is passed over that month.
//!
//! `at` may list several times (`["09:00", "17:30"]`), and takes `9am` and `5:30 pm` as well as `17:30`.

use serde_json::Value as Json;

use super::MAX_TIMES;
use super::cron::{Cron, bit, span};

/// How often a plain form runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Period {
    Hour,
    Day,
    Weekday,
    Week,
    Month,
}

impl Period {
    /// `every:`'s word, when it is one (`15m` is an interval, not a period).
    pub(super) fn of(text: &str) -> Option<Self> {
        Some(match text.trim().to_ascii_lowercase().as_str() {
            "hour" | "hourly" => Self::Hour,
            "day" | "daily" => Self::Day,
            "weekday" | "weekdays" => Self::Weekday,
            "week" | "weekly" => Self::Week,
            "month" | "monthly" => Self::Month,
            _ => return None,
        })
    }
}

const DAYS: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

/// The entries and the plain words of `every` with its `at` and `on`.
pub(super) fn read(
    every: &Json,
    at: Option<&Json>,
    on: Option<&Json>,
) -> Result<(Vec<Cron>, String), String> {
    let period = every.as_str().and_then(Period::of).ok_or(
        "schedule.every: an interval (30s, 15m, 1h, 1d), or hour, day, weekday, week or month with at",
    )?;
    let every_day = Cron {
        seconds: bit(0),
        minutes: bit(0),
        hours: bit(0),
        days: span(1, 31),
        last_day: false,
        months: span(1, 12),
        weekdays: span(0, 6),
        any_day: true,
        any_weekday: true,
    };
    if period == Period::Hour {
        if on.is_some() {
            return Err(
                "schedule.on: every hour runs every day; for chosen days, a cron expression, as \
                 15 * * * 1-5"
                    .to_owned(),
            );
        }
        let minutes = match at {
            None | Some(Json::Null) => vec![0],
            Some(at) => list(at, "at")?
                .iter()
                .map(minute)
                .collect::<Result<Vec<_>, _>>()?,
        };
        let cron = Cron {
            minutes: minutes.iter().fold(0, |bits, m| bits | bit(*m)),
            hours: span(0, 23),
            ..every_day
        };
        let shown: Vec<String> = minutes.iter().map(|m| format!(":{m:02}")).collect();
        return Ok((vec![cron], format!("every hour at {}", and(&shown))));
    }
    let times = match at {
        None | Some(Json::Null) => {
            return Err("schedule.at: the time of day it runs at, as \"09:30\"".to_owned());
        }
        Some(at) => list(at, "at")?.iter().map(time).collect::<Result<Vec<_>, _>>()?,
    };
    let day = match period {
        Period::Day | Period::Weekday => {
            if on.is_some() {
                return Err(format!(
                    "schedule.on: every {} runs on {}; for chosen days, every: week with on",
                    if period == Period::Day { "day" } else { "weekday" },
                    if period == Period::Day {
                        "every day"
                    } else {
                        "Monday to Friday"
                    },
                ));
            }
            if period == Period::Day {
                (every_day, "every day".to_owned())
            } else {
                (
                    Cron {
                        weekdays: span(1, 5),
                        any_weekday: false,
                        ..every_day
                    },
                    "every weekday".to_owned(),
                )
            }
        }
        Period::Week => {
            let on = on.ok_or("schedule.on: the days it runs on, as [monday, thursday]")?;
            let days = list(on, "on")?
                .iter()
                .map(weekday)
                .collect::<Result<Vec<_>, _>>()?;
            let bits = days.iter().fold(0, |bits, d| bits | bit(*d));
            // Monday first, as a week is read.
            let names: Vec<String> = [1, 2, 3, 4, 5, 6, 0]
                .into_iter()
                .filter(|d| bits & bit(*d) != 0)
                .filter_map(|d| DAYS.get(usize::try_from(d).ok()?))
                .map(|d| capitalised(d))
                .collect();
            (
                Cron {
                    weekdays: bits,
                    any_weekday: false,
                    ..every_day
                },
                format!("every week on {}", and(&names)),
            )
        }
        Period::Month => {
            let on = on.ok_or("schedule.on: the dates it runs on, as [1, 15] or last")?;
            let mut days = 0;
            let mut last = false;
            for date in list(on, "on")? {
                match date_of(date)? {
                    Some(d) => days |= bit(d),
                    None => last = true,
                }
            }
            let mut names: Vec<String> = (1..=31)
                .filter(|d| days & bit(*d) != 0)
                .map(|d| format!("the {}", ordinal(d)))
                .collect();
            if last {
                names.push("the last day".to_owned());
            }
            (
                Cron {
                    days,
                    last_day: last,
                    any_day: false,
                    ..every_day
                },
                format!("every month on {}", and(&names)),
            )
        }
        Period::Hour => (every_day, String::new()),
    };
    let (base, words) = day;
    let entries = times
        .iter()
        .map(|(h, m, s)| Cron {
            seconds: bit(*s),
            minutes: bit(*m),
            hours: bit(*h),
            ..base
        })
        .collect();
    let shown: Vec<String> = times
        .iter()
        .map(|(h, m, s)| {
            if *s == 0 {
                format!("{h:02}:{m:02}")
            } else {
                format!("{h:02}:{m:02}:{s:02}")
            }
        })
        .collect();
    Ok((entries, format!("{words} at {}", and(&shown))))
}

/// One value or a list of 1 to `MAX_TIMES`.
fn list<'a>(value: &'a Json, name: &str) -> Result<Vec<&'a Json>, String> {
    match value {
        Json::Array(items) if items.is_empty() => Err(format!("schedule.{name}: an empty list")),
        Json::Array(items) if items.len() > MAX_TIMES => Err(format!("schedule.{name}: at most {MAX_TIMES}")),
        Json::Array(items) => Ok(items.iter().collect()),
        one => Ok(vec![one]),
    }
}

/// A minute past the hour: `:15`, `15` or 15.
fn minute(value: &&Json) -> Result<u32, String> {
    let text = match value {
        Json::Number(n) => n.to_string(),
        Json::String(s) => s.trim().trim_start_matches(':').to_owned(),
        _ => String::new(),
    };
    text.parse::<u32>()
        .ok()
        .filter(|m| *m < 60 && !text.is_empty() && text.len() <= 2)
        .ok_or_else(|| {
            format!(
                "schedule.at: {value} is not a minute: every hour runs at a minute past it, :00 to :59, as \":15\""
            )
        })
}

/// A time of day: `09:30`, `9:30`, `17:45:30`, `9am`, `5:30 pm`.
fn time(value: &&Json) -> Result<(u32, u32, u32), String> {
    let wrong =
        || format!("schedule.at: {value} is not a time of day: as \"09:30\", \"17:45:30\" or \"9am\"");
    let text = value.as_str().ok_or_else(wrong)?.trim().to_ascii_lowercase();
    let (clock, half) = if let Some(t) = text.strip_suffix("am") {
        (t.trim_end(), Some(false))
    } else if let Some(t) = text.strip_suffix("pm") {
        (t.trim_end(), Some(true))
    } else {
        (text.as_str(), None)
    };
    let parts: Vec<&str> = clock.split(':').collect();
    let number = |p: Option<&&str>, most: u32| -> Option<u32> {
        let p = p?;
        if p.is_empty() || p.len() > 2 || !p.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        p.parse::<u32>().ok().filter(|n| *n <= most)
    };
    let (h, m, s) = match parts.len() {
        1 if half.is_some() => (number(parts.first(), 12), Some(0), Some(0)),
        2 => (number(parts.first(), 23), number(parts.get(1), 59), Some(0)),
        3 => (
            number(parts.first(), 23),
            number(parts.get(1), 59),
            number(parts.get(2), 59),
        ),
        _ => (None, None, None),
    };
    let (Some(h), Some(m), Some(s)) = (h, m, s) else {
        return Err(wrong());
    };
    let h = match half {
        None => h,
        // 12am is midnight, 12pm noon.
        Some(pm) if (1..=12).contains(&h) => h % 12 + if pm { 12 } else { 0 },
        Some(_) => return Err(wrong()),
    };
    Ok((h, m, s))
}

/// A day of the week by its name: `monday` or `mon`; Sunday is 0.
fn weekday(value: &&Json) -> Result<u32, String> {
    let wrong = || format!("schedule.on: {value} is not a day of the week: monday to sunday, or mon to sun");
    let text = value.as_str().ok_or_else(wrong)?.trim().to_ascii_lowercase();
    DAYS.iter()
        .position(|d| text.len() >= 3 && d.starts_with(&text) && (text.len() == 3 || *d == text))
        .and_then(|i| u32::try_from(i).ok())
        .ok_or_else(wrong)
}

/// A date of the month, 1 to 31; `None` for `last`.
fn date_of(value: &Json) -> Result<Option<u32>, String> {
    let wrong = || format!("schedule.on: {value} is not a date of the month: 1 to 31, or last");
    let text = match value {
        Json::Number(n) => n.to_string(),
        Json::String(s) => s.trim().to_ascii_lowercase(),
        _ => return Err(wrong()),
    };
    if text == "last" {
        return Ok(None);
    }
    text.parse::<u32>()
        .ok()
        .filter(|d| (1..=31).contains(d))
        .map(Some)
        .ok_or_else(wrong)
}

fn capitalised(word: &str) -> String {
    let mut chars = word.chars();
    chars
        .next()
        .map(|c| c.to_ascii_uppercase().to_string() + chars.as_str())
        .unwrap_or_default()
}

/// 1st, 2nd, 3rd, 4th, ..., 11th, 12th, 13th, 21st.
fn ordinal(n: u32) -> String {
    let suffix = match (n % 10, n % 100) {
        (_, 11..=13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    };
    format!("{n}{suffix}")
}

/// `a`, `a and b`, `a, b and c`.
fn and(items: &[String]) -> String {
    match items {
        [] => String::new(),
        [one] => one.clone(),
        [rest @ .., last] => format!("{} and {last}", rest.join(", ")),
    }
}

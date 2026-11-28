// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Cron expressions: five fields (minute, hour, day of the month, month, day of the week), or six with the second
//! first. Each field is `*`, a number or a name, a range (`1-5`), a step (`*/15`, `8-18/2`), or a list of those
//! (`1,15`); the day of the month may be `L` (its last day), and `?` stands for `*` in either day field. When both
//! day fields are given (neither starts with `*`), a day matching either runs - as cron has always read them.
//!
//! A [`Cron`] knows nothing of time zones: it finds the next date and time on a calendar's own clock face, and
//! [`super::Calendar`] turns that into a moment.

use chrono::{Datelike, Days, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

/// The longest cron expression read.
const MAX_TEXT: usize = 256;
/// The most parts one field lists (`1,2,3,...`).
const MAX_PARTS: usize = 60;
/// How far ahead the next time is looked for: 30 years holds every date a schedule can name, the 29th of February
/// on a chosen weekday too.
const HORIZON_DAYS: u64 = 30 * 366;

/// The seconds, minutes, hours, days, months and weekdays a schedule runs at, each a set of bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Cron {
    pub(super) seconds: u64,
    pub(super) minutes: u64,
    pub(super) hours: u64,
    /// Bits 1 to 31.
    pub(super) days: u64,
    /// The last day of each month (`L`).
    pub(super) last_day: bool,
    /// Bits 1 to 12.
    pub(super) months: u64,
    /// Bits 0 to 6, Sunday first.
    pub(super) weekdays: u64,
    /// Whether the day of the month was written `*` (or `?`): only then does the day of the week alone choose.
    pub(super) any_day: bool,
    /// Whether the day of the week was written `*` (or `?`).
    pub(super) any_weekday: bool,
}

/// What one field is.
struct Field {
    name: &'static str,
    min: u32,
    max: u32,
    /// The names its values may go by, the first for `min`.
    names: &'static [&'static str],
    /// What it is written as, for an error.
    example: &'static str,
    /// A day field: `?` stands for `*` in it.
    day: bool,
    /// The day of the month: `L` is its last day.
    last: bool,
}

const SECOND: Field = Field {
    name: "second",
    min: 0,
    max: 59,
    names: &[],
    example: "0 to 59",
    day: false,
    last: false,
};
const MINUTE: Field = Field {
    name: "minute",
    min: 0,
    max: 59,
    names: &[],
    example: "0 to 59",
    day: false,
    last: false,
};
const HOUR: Field = Field {
    name: "hour",
    min: 0,
    max: 23,
    names: &[],
    example: "0 to 23",
    day: false,
    last: false,
};
const DAY: Field = Field {
    name: "day of the month",
    min: 1,
    max: 31,
    names: &[],
    example: "1 to 31, or L for the last",
    day: true,
    last: true,
};
const MONTH: Field = Field {
    name: "month",
    min: 1,
    max: 12,
    names: &[
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    ],
    example: "1 to 12, or jan to dec",
    day: false,
    last: false,
};
/// 0 and 7 are both Sunday.
const WEEKDAY: Field = Field {
    name: "day of the week",
    min: 0,
    max: 7,
    names: &["sun", "mon", "tue", "wed", "thu", "fri", "sat"],
    example: "0 to 7 (0 and 7 are Sunday), or sun to sat",
    day: true,
    last: false,
};

/// The shorthands cron has long had for the usual schedules.
const SHORTHANDS: &[(&str, &str)] = &[
    ("@yearly", "0 0 1 1 *"),
    ("@annually", "0 0 1 1 *"),
    ("@monthly", "0 0 1 * *"),
    ("@weekly", "0 0 * * 0"),
    ("@daily", "0 0 * * *"),
    ("@midnight", "0 0 * * *"),
    ("@hourly", "0 * * * *"),
];

/// A field read: its bits, whether it was written `*`, and whether it names the month's last day.
struct Read {
    bits: u64,
    star: bool,
    last: bool,
}

pub(super) fn bit(n: u32) -> u64 {
    1u64.checked_shl(n).unwrap_or(0)
}

/// Every bit from `lo` to `hi`.
pub(super) fn span(lo: u32, hi: u32) -> u64 {
    (lo..=hi).fold(0, |bits, n| bits | bit(n))
}

fn has(bits: u64, n: u32) -> bool {
    bits & bit(n) != 0
}

/// The first bit set at or after `from`.
fn next_bit(bits: u64, from: u32) -> Option<u32> {
    let rest = bits.checked_shr(from)?;
    (rest != 0).then(|| from + rest.trailing_zeros())
}

impl Cron {
    /// A cron expression read and checked; what is wrong with it said in plain words.
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if text.is_empty() {
            return Err(
                "an empty cron expression: five fields, as 0 9 * * 1-5 (09:00, Monday to Friday)".to_owned(),
            );
        }
        if text.len() > MAX_TEXT {
            return Err(format!("a cron expression of at most {MAX_TEXT} characters"));
        }
        let text = match text.strip_prefix('@') {
            Some(_) => SHORTHANDS
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(text))
                .map(|(_, cron)| *cron)
                .ok_or_else(|| {
                    format!("{text:?} is not a shorthand: @hourly, @daily, @weekly, @monthly or @yearly")
                })?,
            None => text,
        };
        let fields: Vec<&str> = text.split_whitespace().collect();
        let (second, rest) = match fields.as_slice() {
            [s, m, h, d, mo, w] => (Some(*s), [*m, *h, *d, *mo, *w]),
            [m, h, d, mo, w] => (None, [*m, *h, *d, *mo, *w]),
            other => {
                return Err(format!(
                    "{text:?} has {} field{}: a cron expression has 5 (minute hour day month weekday, as \
                     0 9 * * 1-5) or 6 (with the second first)",
                    other.len(),
                    if other.len() == 1 { "" } else { "s" }
                ));
            }
        };
        let [minute, hour, day, month, weekday] = rest;
        let seconds = match second {
            Some(s) => read(s, &SECOND)?.bits,
            None => bit(0),
        };
        let day = read(day, &DAY)?;
        let mut weekdays = read(weekday, &WEEKDAY)?;
        // Sunday is 0, and 7 as well.
        if has(weekdays.bits, 7) {
            weekdays.bits = (weekdays.bits & !bit(7)) | bit(0);
        }
        let cron = Self {
            seconds,
            minutes: read(minute, &MINUTE)?.bits,
            hours: read(hour, &HOUR)?.bits,
            days: day.bits,
            last_day: day.last,
            months: read(month, &MONTH)?.bits,
            weekdays: weekdays.bits,
            any_day: day.star,
            any_weekday: weekdays.star,
        };
        if !cron.ever() {
            return Err(format!(
                "{text:?} never runs: no month it names has the day it names (as 30 February)"
            ));
        }
        Ok(cron)
    }

    /// Whether the date is one this runs on.
    pub(super) fn on(&self, date: NaiveDate) -> bool {
        if !has(self.months, date.month()) {
            return false;
        }
        let day =
            has(self.days, date.day()) || (self.last_day && date.succ_opt().is_none_or(|d| d.day() == 1));
        let weekday = has(self.weekdays, date.weekday().num_days_from_sunday());
        if self.any_day || self.any_weekday {
            day && weekday
        } else {
            day || weekday
        }
    }

    /// The first time of day at or after `from` this runs at, on a day it runs on.
    fn time_from(&self, from: NaiveTime) -> Option<NaiveTime> {
        let (h0, m0, s0) = (from.hour(), from.minute(), from.second());
        let mut hour = next_bit(self.hours, h0);
        while let Some(h) = hour.filter(|h| *h < 24) {
            let mut minute = next_bit(self.minutes, if h == h0 { m0 } else { 0 });
            while let Some(m) = minute.filter(|m| *m < 60) {
                let first = if h == h0 && m == m0 { s0 } else { 0 };
                if let Some(s) = next_bit(self.seconds, first).filter(|s| *s < 60) {
                    return NaiveTime::from_hms_opt(h, m, s);
                }
                minute = next_bit(self.minutes, m + 1);
            }
            hour = next_bit(self.hours, h + 1);
        }
        None
    }

    /// The first date and time at or after `from` (a whole second) this names, on the clock face alone.
    pub(crate) fn next_from(&self, from: NaiveDateTime) -> Option<NaiveDateTime> {
        let mut date = from.date();
        let mut time = from.time();
        let end = date.checked_add_days(Days::new(HORIZON_DAYS))?;
        while date <= end {
            if !has(self.months, date.month()) {
                // On to the first of the next month.
                date = date.with_day(1)?.checked_add_months(chrono::Months::new(1))?;
                time = NaiveTime::MIN;
                continue;
            }
            if self.on(date)
                && let Some(t) = self.time_from(time)
            {
                return Some(date.and_time(t));
            }
            date = date.succ_opt()?;
            time = NaiveTime::MIN;
        }
        None
    }

    /// Whether it ever runs: over a whole 400-year turn of the calendar, every date repeats.
    fn ever(&self) -> bool {
        let Some(start) = NaiveDate::from_ymd_opt(2000, 1, 1) else {
            return false;
        };
        let mut date = start;
        for _ in 0..(400 * 12) {
            if has(self.months, date.month()) {
                let mut day = date;
                while day.month() == date.month() {
                    if self.on(day) {
                        return true;
                    }
                    let Some(next) = day.succ_opt() else {
                        return false;
                    };
                    day = next;
                }
            }
            let Some(next) = date.checked_add_months(chrono::Months::new(1)) else {
                return false;
            };
            date = next;
        }
        false
    }
}

/// One field: `*`, a value, a range, a step, or a list of them.
fn read(text: &str, field: &Field) -> Result<Read, String> {
    let name = field.name;
    let star = text.starts_with('*') || text.starts_with('?');
    let parts: Vec<&str> = text.split(',').collect();
    if parts.len() > MAX_PARTS {
        return Err(format!("{name}: at most {MAX_PARTS} parts"));
    }
    let mut out = Read {
        bits: 0,
        star,
        last: false,
    };
    for part in parts {
        if part.is_empty() {
            return Err(format!("{name}: {text:?} has an empty part"));
        }
        if field.last && part.eq_ignore_ascii_case("l") {
            out.last = true;
            continue;
        }
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => (range, Some(step)),
            None => (part, None),
        };
        let step = match step {
            None => 1,
            Some(s) => s
                .parse::<u32>()
                .ok()
                .filter(|s| (1..=field.max.max(1)).contains(s))
                .ok_or_else(|| format!("{name}: the step {s:?} in {part:?} is not 1 to {}", field.max))?,
        };
        let any = range == "*" || (range == "?" && field.day);
        let (lo, hi) = if any {
            (field.min, field.max)
        } else if range == "?" {
            return Err(format!(
                "{name}: ? stands only for a day (of the month or of the week)"
            ));
        } else if let Some((a, b)) = range.split_once('-') {
            let (a, b) = (value(a, field)?, value(b, field)?);
            if a > b {
                return Err(format!(
                    "{name}: {range:?} goes backwards: a range goes from the smaller to the larger"
                ));
            }
            (a, b)
        } else {
            let a = value(range, field)?;
            (a, if step > 1 { field.max } else { a })
        };
        let mut n = lo;
        while n <= hi {
            out.bits |= bit(n);
            n += step;
        }
    }
    Ok(out)
}

/// A value of a field: a number in its range, or one of its names.
fn value(text: &str, field: &Field) -> Result<u32, String> {
    let named = field
        .names
        .iter()
        .position(|n| n.eq_ignore_ascii_case(text))
        .and_then(|i| u32::try_from(i).ok())
        .map(|i| field.min + i);
    named
        .or_else(|| text.parse::<u32>().ok())
        .filter(|n| (field.min..=field.max).contains(n))
        .ok_or_else(|| format!("{}: {text:?} is not {}", field.name, field.example))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    fn next(cron: &str, from: &str) -> String {
        Cron::parse(cron)
            .unwrap()
            .next_from(at(from))
            .unwrap()
            .format("%Y-%m-%d %H:%M:%S %a")
            .to_string()
    }

    #[test]
    fn five_fields_run_on_the_minute_and_six_name_the_second() {
        assert_eq!(
            next("0 9 * * *", "2026-11-27 08:15:00"),
            "2026-11-27 09:00:00 Fri"
        );
        assert_eq!(
            next("0 9 * * *", "2026-11-27 09:00:01"),
            "2026-11-28 09:00:00 Sat"
        );
        assert_eq!(
            next("*/15 * * * *", "2026-11-27 10:16:00"),
            "2026-11-27 10:30:00 Fri"
        );
        assert_eq!(
            next("30 */10 * * * *", "2026-11-27 10:16:00"),
            "2026-11-27 10:20:30 Fri"
        );
        assert_eq!(
            next("* * * * * *", "2026-11-27 23:59:59"),
            "2026-11-27 23:59:59 Fri"
        );
        assert_eq!(
            next("0 0 * * *", "2026-12-31 23:59:59"),
            "2027-01-01 00:00:00 Fri"
        );
    }

    #[test]
    fn weekdays_ranges_lists_and_names() {
        // Friday evening: the next weekday morning is Monday's.
        assert_eq!(
            next("0 9 * * 1-5", "2026-11-27 18:00:00"),
            "2026-11-30 09:00:00 Mon"
        );
        assert_eq!(
            next("0 9 * * MON-FRI", "2026-11-27 18:00:00"),
            "2026-11-30 09:00:00 Mon"
        );
        assert_eq!(
            next("0 8 * * 1,4", "2026-11-27 00:00:00"),
            "2026-11-30 08:00:00 Mon"
        );
        // 0 and 7 are both Sunday.
        assert_eq!(
            next("0 8 * * 7", "2026-11-27 00:00:00"),
            "2026-11-29 08:00:00 Sun"
        );
        assert_eq!(
            next("0 8 * * sun", "2026-11-27 00:00:00"),
            "2026-11-29 08:00:00 Sun"
        );
        assert_eq!(
            next("0 12 * jan,jul *", "2026-11-27 00:00:00"),
            "2027-01-01 12:00:00 Fri"
        );
        assert_eq!(
            next("0 8-18/2 * * *", "2026-11-27 13:00:00"),
            "2026-11-27 14:00:00 Fri"
        );
        assert_eq!(
            next("0 10/5 * * *", "2026-11-27 16:00:00"),
            "2026-11-27 20:00:00 Fri"
        );
    }

    #[test]
    fn both_day_fields_given_means_either() {
        // The 1st, or any Monday: Monday the 30th comes before 1 December.
        assert_eq!(
            next("0 0 1 * 1", "2026-11-27 00:00:00"),
            "2026-11-30 00:00:00 Mon"
        );
        assert_eq!(
            next("0 0 1 * 1", "2026-11-30 00:00:01"),
            "2026-12-01 00:00:00 Tue"
        );
        // With one of them a star, only the other chooses.
        assert_eq!(
            next("0 0 1 * *", "2026-11-27 00:00:00"),
            "2026-12-01 00:00:00 Tue"
        );
        assert_eq!(
            next("0 0 */2 * 1", "2026-11-27 00:00:00"),
            "2026-12-07 00:00:00 Mon"
        );
        assert_eq!(
            next("0 0 ? * 1", "2026-11-27 00:00:00"),
            "2026-11-30 00:00:00 Mon"
        );
    }

    #[test]
    fn month_ends_and_leap_years() {
        // The 31st skips the months without one.
        assert_eq!(
            next("0 0 31 * *", "2026-11-01 00:00:00"),
            "2026-12-31 00:00:00 Thu"
        );
        assert_eq!(
            next("0 0 31 * *", "2027-01-31 00:00:01"),
            "2027-03-31 00:00:00 Wed"
        );
        // The last day of each month: February's is the 28th, in a leap year the 29th.
        assert_eq!(
            next("0 0 L * *", "2027-02-01 00:00:00"),
            "2027-02-28 00:00:00 Sun"
        );
        assert_eq!(
            next("0 0 L * *", "2028-02-01 00:00:00"),
            "2028-02-29 00:00:00 Tue"
        );
        assert_eq!(
            next("0 0 L * *", "2026-11-27 00:00:00"),
            "2026-11-30 00:00:00 Mon"
        );
        // The 29th of February: four years on, and eight across 2100, which has none.
        assert_eq!(
            next("0 0 29 2 *", "2028-03-01 00:00:00"),
            "2032-02-29 00:00:00 Sun"
        );
        assert_eq!(
            next("0 0 29 2 *", "2096-03-01 00:00:00"),
            "2104-02-29 00:00:00 Fri"
        );
    }

    #[test]
    fn shorthands() {
        assert_eq!(next("@daily", "2026-11-27 08:00:00"), "2026-11-28 00:00:00 Sat");
        assert_eq!(next("@weekly", "2026-11-27 08:00:00"), "2026-11-29 00:00:00 Sun");
        assert_eq!(next("@monthly", "2026-11-27 08:00:00"), "2026-12-01 00:00:00 Tue");
        assert_eq!(next("@HOURLY", "2026-11-27 08:00:01"), "2026-11-27 09:00:00 Fri");
        assert_eq!(next("@yearly", "2026-11-27 08:00:00"), "2027-01-01 00:00:00 Fri");
    }

    #[test]
    fn mistakes_are_said_in_plain_words() {
        let error = |text: &str| Cron::parse(text).unwrap_err();
        assert!(error("").contains("empty"), "{}", error(""));
        assert!(error("0 9 * *").contains("has 4 fields"), "{}", error("0 9 * *"));
        assert!(error("1 2 3 4 5 6 7").contains("has 7 fields"));
        assert_eq!(error("61 * * * *"), "minute: \"61\" is not 0 to 59");
        assert_eq!(error("0 24 * * *"), "hour: \"24\" is not 0 to 23");
        assert_eq!(
            error("0 0 0 * *"),
            "day of the month: \"0\" is not 1 to 31, or L for the last"
        );
        assert_eq!(error("0 0 * 13 *"), "month: \"13\" is not 1 to 12, or jan to dec");
        assert!(error("0 0 * * fun").starts_with("day of the week: \"fun\" is not 0 to 7"));
        assert!(error("0 0 * * 5-1").contains("goes backwards"));
        assert!(error("*/0 * * * *").contains("the step \"0\""));
        assert!(error("*/x * * * *").contains("the step \"x\""));
        assert!(error("1,,2 * * * *").contains("empty part"));
        assert!(error("? * * * *").contains("? stands only for a day"));
        assert!(error("0 0 L * L").starts_with("day of the week"));
        assert!(error("0 0 30 2 *").contains("never runs"));
        assert!(error("0 0 31 4,6,9,11 *").contains("never runs"));
        assert!(error("@reboot").contains("not a shorthand"));
        assert!(error(&"1,".repeat(200)).contains("at most 256"));
        assert!(error(&format!("{} * * * *", "1,".repeat(61) + "1")).contains("at most 60 parts"));
    }
}

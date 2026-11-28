// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The next times of schedules at set times: across daylight saving's changes (an hour and half an hour, forward and
//! back, a day skipped), month ends and leap years; the settings read, and their mistakes said.

use serde_json::json;

use super::*;
use crate::Workflow;

fn ms(text: &str) -> i64 {
    DateTime::parse_from_rfc3339(text).unwrap().timestamp_millis()
}

fn utc(at: i64) -> String {
    crate::runner::iso(at)
}

fn calendar(settings: serde_json::Value) -> Calendar {
    Calendar::from_settings(settings.as_object().unwrap())
        .unwrap()
        .expect("set times")
}

fn next(settings: serde_json::Value, from: &str, count: usize) -> Vec<String> {
    calendar(settings)
        .next(ms(from), count)
        .into_iter()
        .map(utc)
        .collect()
}

fn refused(settings: serde_json::Value) -> String {
    Calendar::from_settings(settings.as_object().unwrap()).unwrap_err()
}

#[test]
fn a_time_the_clock_skips_runs_once_when_it_jumps() {
    // New York, 8 March 2026: at 02:00 the clock goes to 03:00. 02:30 runs at 03:00 (07:00 UTC) that night.
    let daily = json!({ "every": "day", "at": "02:30", "timezone": "America/New_York" });
    assert_eq!(
        next(daily, "2026-03-07T00:00:00Z", 3),
        [
            "2026-03-07T07:30:00Z",
            "2026-03-08T07:00:00Z",
            "2026-03-09T06:30:00Z"
        ]
    );
    // Berlin, 29 March 2026: the four quarters of the skipped hour run once, together, as the clock jumps.
    let quarters = json!({ "cron": "*/15 * * * *", "timezone": "Europe/Berlin" });
    assert_eq!(
        next(quarters, "2026-03-29T00:50:00Z", 3),
        [
            "2026-03-29T01:00:00Z",
            "2026-03-29T01:15:00Z",
            "2026-03-29T01:30:00Z"
        ]
    );
    // Hourly: every real hour of the shorter day, the skipped one at the jump.
    let hourly = json!({ "every": "hour", "timezone": "America/New_York" });
    assert_eq!(
        next(hourly, "2026-03-08T05:30:00Z", 3),
        [
            "2026-03-08T06:00:00Z",
            "2026-03-08T07:00:00Z",
            "2026-03-08T08:00:00Z"
        ]
    );
}

#[test]
fn a_time_the_clock_repeats_runs_once_the_first_time() {
    // New York, 1 November 2026: at 02:00 the clock goes back to 01:00, so 01:30 comes at 05:30 and 06:30 UTC.
    let daily = json!({ "every": "day", "at": "1:30", "timezone": "America/New_York" });
    assert_eq!(
        next(daily.clone(), "2026-10-31T00:00:00Z", 3),
        [
            "2026-10-31T05:30:00Z",
            "2026-11-01T05:30:00Z",
            "2026-11-02T06:30:00Z"
        ]
    );
    // From inside the repeated hour, its time has run already: the next is the next day's.
    assert_eq!(next(daily, "2026-11-01T06:10:00Z", 1), ["2026-11-02T06:30:00Z"]);
    // Hourly: each hour the clock shows, once; 01:00 is not run again at 06:00 UTC.
    let hourly = json!({ "cron": "0 * * * *", "timezone": "America/New_York" });
    assert_eq!(
        next(hourly.clone(), "2026-11-01T03:30:00Z", 4),
        [
            "2026-11-01T04:00:00Z",
            "2026-11-01T05:00:00Z",
            "2026-11-01T07:00:00Z",
            "2026-11-01T08:00:00Z"
        ]
    );
    assert_eq!(next(hourly, "2026-11-01T06:10:00Z", 1), ["2026-11-01T07:00:00Z"]);
    // Every second: inside the repeated hour, the seconds already run the first time round are passed over.
    let seconds = json!({ "cron": "* * * * * *", "timezone": "America/New_York" });
    assert_eq!(next(seconds, "2026-11-01T06:30:00Z", 1), ["2026-11-01T07:00:00Z"]);
}

#[test]
fn half_hour_changes_and_a_skipped_day() {
    // Lord Howe Island goes forward half an hour (02:00 to 02:30, 4 October 2026): 02:15 runs at 02:30.
    let lord_howe = json!({ "every": "day", "at": "02:15", "timezone": "Australia/Lord_Howe" });
    assert_eq!(
        next(lord_howe, "2026-10-02T00:00:00Z", 3),
        [
            "2026-10-02T15:45:00Z",
            "2026-10-03T15:30:00Z",
            "2026-10-04T15:15:00Z"
        ]
    );
    // Samoa skipped 30 December 2011: its 09:00 ran once, as the day was skipped (midnight on the 31st).
    let samoa = json!({ "every": "day", "at": "09:00", "timezone": "Pacific/Apia" });
    assert_eq!(
        next(samoa, "2011-12-29T00:00:00Z", 3),
        [
            "2011-12-29T19:00:00Z",
            "2011-12-30T10:00:00Z",
            "2011-12-30T19:00:00Z"
        ]
    );
}

#[test]
fn month_ends_and_leap_years_in_a_zone() {
    let monthly = json!({ "every": "month", "on": [31, "last"], "at": "18:00", "timezone": "Asia/Tokyo" });
    // Tokyo is 9 hours ahead: 18:00 there is 09:00 UTC. The 31st where there is one, and every last day.
    assert_eq!(
        next(monthly, "2027-01-01T00:00:00Z", 4),
        [
            "2027-01-31T09:00:00Z",
            "2027-02-28T09:00:00Z",
            "2027-03-31T09:00:00Z",
            "2027-04-30T09:00:00Z"
        ]
    );
    let leap = json!({ "cron": "0 12 29 2 *" });
    assert_eq!(
        next(leap, "2026-01-01T00:00:00Z", 3),
        [
            "2028-02-29T12:00:00Z",
            "2032-02-29T12:00:00Z",
            "2036-02-29T12:00:00Z"
        ]
    );
}

#[test]
fn the_plain_forms_and_their_words() {
    // Friday 27 November 2026.
    let from = "2026-11-27T10:00:00Z";
    let weekdays = json!({ "every": "weekday", "at": "09:00", "timezone": "Europe/Berlin" });
    assert_eq!(
        calendar(weekdays.clone()).says(),
        "every weekday at 09:00 (Europe/Berlin)"
    );
    assert_eq!(
        next(weekdays, from, 2),
        ["2026-11-30T08:00:00Z", "2026-12-01T08:00:00Z"]
    );
    let weekly = json!({ "every": "week", "on": ["thursday", "mon"], "at": ["8am", "5:30 pm"] });
    assert_eq!(
        calendar(weekly.clone()).says(),
        "every week on Monday and Thursday at 08:00 and 17:30 (UTC)"
    );
    assert_eq!(
        next(weekly, from, 3),
        [
            "2026-11-30T08:00:00Z",
            "2026-11-30T17:30:00Z",
            "2026-12-03T08:00:00Z"
        ]
    );
    let monthly = json!({ "every": "monthly", "on": [1, "15"], "at": "06:00:30" });
    assert_eq!(
        calendar(monthly.clone()).says(),
        "every month on the 1st and the 15th at 06:00:30 (UTC)"
    );
    assert_eq!(
        next(monthly, from, 2),
        ["2026-12-01T06:00:30Z", "2026-12-15T06:00:30Z"]
    );
    let hourly = json!({ "every": "hour", "at": [":05", 35] });
    assert_eq!(calendar(hourly.clone()).says(), "every hour at :05 and :35 (UTC)");
    assert_eq!(
        next(hourly, from, 3),
        [
            "2026-11-27T10:05:00Z",
            "2026-11-27T10:35:00Z",
            "2026-11-27T11:05:00Z"
        ]
    );
    let daily = json!({ "every": "Daily", "at": ["12am", "12pm", "23:59"] });
    assert_eq!(
        calendar(daily.clone()).says(),
        "every day at 00:00, 12:00 and 23:59 (UTC)"
    );
    assert_eq!(
        next(daily, from, 3),
        [
            "2026-11-27T12:00:00Z",
            "2026-11-27T23:59:00Z",
            "2026-11-28T00:00:00Z"
        ]
    );
    let crons = json!({ "cron": ["0 9 * * 1-5", "0 12 * * 6"], "timezone": "UTC" });
    assert_eq!(
        calendar(crons.clone()).says(),
        "cron 0 9 * * 1-5 and 0 12 * * 6 (UTC)"
    );
    assert_eq!(
        next(crons, from, 2),
        ["2026-11-28T12:00:00Z", "2026-11-30T09:00:00Z"]
    );
}

#[test]
fn local_times_are_shown_on_the_schedules_clock() {
    let c = calendar(json!({ "cron": "0 9 * * *", "timezone": "Europe/Berlin" }));
    assert_eq!(c.timezone(), "Europe/Berlin");
    assert_eq!(c.local(ms("2026-11-30T08:00:00Z")), "2026-11-30T09:00:00+01:00");
    assert_eq!(c.local(ms("2026-07-01T07:00:00Z")), "2026-07-01T09:00:00+02:00");
}

#[test]
fn an_interval_stays_an_interval() {
    for settings in [
        json!({ "every": "15m" }),
        json!({ "every": "1d" }),
        json!({ "every": 5 }),
    ] {
        assert_eq!(Calendar::from_settings(settings.as_object().unwrap()), Ok(None));
    }
    let every = Workflow::parse("name: t\ntrigger:\n  schedule:\n    every: 15m\n").unwrap();
    assert_eq!(every.trigger, Trigger::Every(Duration::from_secs(900)));
    let runs: Vec<String> = next_runs(&every.trigger, ms("2026-11-27T10:07:00Z"), 3)
        .into_iter()
        .map(utc)
        .collect();
    assert_eq!(
        runs,
        [
            "2026-11-27T10:15:00Z",
            "2026-11-27T10:30:00Z",
            "2026-11-27T10:45:00Z"
        ]
    );
    assert!(next_runs(&Trigger::Manual, 0, 3).is_empty());
}

#[test]
fn a_workflow_reads_set_times_from_its_trigger() {
    let w = Workflow::parse(
        "name: t\ntrigger:\n  schedule:\n    every: weekday\n    at: '09:00'\n    timezone: Europe/London\n",
    )
    .unwrap();
    let Trigger::Calendar(c) = &w.trigger else {
        panic!("not set times: {:?}", w.trigger);
    };
    assert_eq!(c.says(), "every weekday at 09:00 (Europe/London)");
    assert_eq!(next_runs(&w.trigger, ms("2026-11-27T10:00:00Z"), 1).len(), 1);
    let w = Workflow::parse("name: t\ntrigger: { schedule: { cron: '30 6 * * *' } }\n").unwrap();
    assert!(matches!(&w.trigger, Trigger::Calendar(c) if c.says() == "cron 30 6 * * * (UTC)"));
    let wrong = Workflow::parse("name: t\ntrigger: { schedule: { cron: '30 6 * *' } }\n").unwrap_err();
    assert!(
        wrong.starts_with("trigger: schedule.cron: \"30 6 * *\" has 4 fields"),
        "{wrong}"
    );
    let wrong = Workflow::parse("name: t\ntrigger: { schedule: {} }\n").unwrap_err();
    assert!(wrong.starts_with("trigger: schedule: how often"), "{wrong}");
}

#[test]
fn mistakes_are_said_in_plain_words() {
    let said = [
        (
            json!({ "every": "day", "at": "9:00", "timezone": "Mars/Base" }),
            "is not a time zone",
        ),
        (
            json!({ "every": "day", "at": "9:00", "timezone": 3 }),
            "a time zone's name",
        ),
        (
            json!({ "every": "15m", "at": "09:00" }),
            "schedule.at: goes with set times",
        ),
        (
            json!({ "every": "1h", "timezone": "Europe/Paris" }),
            "schedule.timezone: goes with set times",
        ),
        (
            json!({ "every": "day", "cron": "0 9 * * *" }),
            "either every or cron",
        ),
        (
            json!({ "cron": "0 9 * * *", "at": "10:00" }),
            "names its own times",
        ),
        (json!({ "every": "day" }), "schedule.at: the time of day"),
        (
            json!({ "every": "day", "at": "25:00" }),
            "\"25:00\" is not a time of day",
        ),
        (json!({ "every": "day", "at": "9" }), "\"9\" is not a time of day"),
        (
            json!({ "every": "day", "at": "13pm" }),
            "\"13pm\" is not a time of day",
        ),
        (json!({ "every": "day", "at": "09:60" }), "is not a time of day"),
        (json!({ "every": "day", "at": [] }), "an empty list"),
        (json!({ "every": "day", "at": vec!["09:00"; 25] }), "at most 24"),
        (
            json!({ "every": "day", "at": "09:00", "on": ["monday"] }),
            "for chosen days, every: week",
        ),
        (json!({ "every": "week", "at": "09:00" }), "schedule.on: the days"),
        (
            json!({ "every": "week", "at": "09:00", "on": ["funday"] }),
            "\"funday\" is not a day of the week",
        ),
        (
            json!({ "every": "month", "at": "09:00" }),
            "schedule.on: the dates",
        ),
        (
            json!({ "every": "month", "at": "09:00", "on": [32] }),
            "32 is not a date of the month",
        ),
        (json!({ "every": "hour", "at": ":75" }), "is not a minute"),
        (
            json!({ "every": "hour", "on": "monday" }),
            "every hour runs every day",
        ),
        (json!({ "cron": 5 }), "schedule.cron: a cron expression"),
        (json!({ "cron": [] }), "1 to 24 cron expressions"),
        (
            json!({ "cron": "0 9 * * 8" }),
            "schedule.cron: day of the week: \"8\" is not",
        ),
        (
            json!({ "every": "day", "at": "09:00", "when": "now" }),
            "schedule.when: not a setting",
        ),
    ];
    for (settings, words) in said {
        let error = refused(settings.clone());
        assert!(error.contains(words), "{settings}: {error}");
    }
    assert!(refused(json!({})).starts_with("schedule: how often"));
}

#[test]
fn next_times_are_bounded() {
    let c = calendar(json!({ "cron": "* * * * * *" }));
    assert_eq!(c.next(0, 10_000).len(), MAX_SHOWN);
    let from = ms("2026-11-27T10:00:00Z");
    let shown = c.next(from, 3);
    assert_eq!(
        shown,
        [from + 1000, from + 2000, from + 3000],
        "no more often than every second"
    );
}

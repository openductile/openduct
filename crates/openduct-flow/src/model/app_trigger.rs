// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading a workflow's app trigger: `trigger: {gmail: {when: new_email, credential: shop-mail, every: 5m}}` - the
//! app, which of its triggers, the credential, and the trigger's own inputs; for a poll, how often and how many
//! items at most each poll starts runs for.

use std::time::Duration;

use serde_json::{Map, Value as Json};

use super::{Trigger, duration, text};
use crate::app_files::triggers::{POLL_LEAST, POLL_MOST, TriggerDef};
use crate::app_files::{AppDef, poll};

/// The largest input value a trigger takes, in bytes as JSON.
const LARGEST_INPUT: usize = 10 * 1024;

/// A trigger of an app's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppTrigger {
    /// The app's id.
    pub app: String,
    /// Which of its triggers.
    pub when: String,
    /// The credential it uses, by name.
    pub credential: String,
    /// The trigger's inputs, as given: plain values.
    pub settings: Json,
    /// How often it polls; none for a trigger the app calls.
    pub every: Option<Duration>,
    /// The most items one poll starts runs for.
    pub max_items: u32,
}

impl AppTrigger {
    /// The app and its trigger, as their definitions say.
    pub fn definition(&self) -> Option<(&'static AppDef, &'static TriggerDef)> {
        let app = crate::app_files::find(&self.app)?;
        let (_, trigger) = app.trigger(Some(&self.when))?;
        Some((app, trigger))
    }
}

/// The trigger of the app called `kind`, if there is such an app.
pub(super) fn read(kind: &str, settings: &Map<String, Json>) -> Option<Result<Trigger, String>> {
    let app = crate::app_files::find(kind)?;
    Some(parse(app, settings).map(Trigger::App))
}

fn parse(app: &'static AppDef, s: &Map<String, Json>) -> Result<AppTrigger, String> {
    let kind = &app.id;
    if app.triggers.is_empty() {
        return Err(format!(
            "{kind}: {} starts no workflows: it has no triggers",
            app.name
        ));
    }
    let names: Vec<&str> = app.triggers.iter().map(|(n, _)| n.as_str()).collect();
    let named = text(s.get("when"));
    let Some((when, def)) = app.trigger(named) else {
        return Err(match named {
            Some(n) => format!(
                "{kind}.when: {} has no trigger {n:?}: it has {}",
                app.name,
                names.join(", ")
            ),
            None => format!(
                "{kind}.when: which of {}'s triggers ({})",
                app.name,
                names.join(", ")
            ),
        });
    };
    let kinds: Vec<&str> = app.sign_ins.iter().map(|s| s.id.as_str()).collect();
    let credential = text(s.get("credential"))
        .map(str::trim)
        .filter(|c| !c.is_empty() && c.len() <= 100)
        .ok_or_else(|| {
            format!(
                "{kind}.credential: the credential to use (a {} one)",
                kinds.join(" or ")
            )
        })?
        .to_owned();
    let polls = def.polls();
    let every = match (polls, s.get("every")) {
        (Some(p), None | Some(Json::Null)) => Some(p.every),
        (Some(_), Some(e)) => {
            let written = e.as_str().unwrap_or_default();
            Some(
                duration(written)
                    .filter(|d| *d >= POLL_LEAST && *d <= POLL_MOST)
                    .ok_or_else(|| format!("{kind}.every: how often it asks, as 5m or 1h: from 1m to 1d"))?,
            )
        }
        (None, None) => None,
        (None, Some(_)) => {
            return Err(format!(
                "{kind}.every: {} sends its {} as it happens: it is not asked",
                app.name, def.label
            ));
        }
    };
    let max_items = match (polls, s.get("max_items")) {
        (_, None | Some(Json::Null)) => poll::POLL_ITEMS,
        (Some(_), Some(n)) => n
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .filter(|n| (1..=poll::MOST_POLL_ITEMS).contains(n))
            .ok_or_else(|| {
                format!(
                    "{kind}.max_items: how many items a poll runs at most, 1 to {}",
                    poll::MOST_POLL_ITEMS
                )
            })?,
        (None, Some(_)) => return Err(format!("{kind}.max_items: only a trigger that asks takes it")),
    };
    let mut settings = Map::new();
    for (k, v) in s {
        if matches!(k.as_str(), "when" | "credential" | "every" | "max_items") {
            continue;
        }
        let Some(input) = def.inputs.iter().find(|i| &i.name == k) else {
            let known: Vec<&str> = def.inputs.iter().map(|i| i.name.as_str()).collect();
            return Err(format!(
                "{kind}.{k}: not a setting of {}'s {when} (when, credential{}{})",
                app.name,
                if polls.is_some() { ", every, max_items" } else { "" },
                known.iter().map(|k| format!(", {k}")).collect::<String>()
            ));
        };
        let plain = matches!(v, Json::String(_) | Json::Number(_) | Json::Bool(_) | Json::Null);
        if !plain && !matches!(input.kind.as_str(), "list" | "record") {
            return Err(format!("{kind}.{k}: a value as text, a number or true/false"));
        }
        if v.to_string().len() > LARGEST_INPUT {
            return Err(format!("{kind}.{k}: at most {LARGEST_INPUT} bytes"));
        }
        if matches!(v, Json::String(t) if t.contains("{{")) {
            return Err(format!(
                "{kind}.{k}: a trigger's settings are values: there is no event yet to work them out of"
            ));
        }
        settings.insert(k.clone(), v.clone());
    }
    for required in def.inputs.iter().filter(|i| i.required) {
        let given = settings
            .get(&required.name)
            .is_some_and(|v| !v.is_null() && v.as_str() != Some(""));
        if !given {
            return Err(format!(
                "{kind}.{}: {}'s {when} needs it ({})",
                required.name, app.name, required.label
            ));
        }
    }
    Ok(AppTrigger {
        app: app.id.clone(),
        when: when.to_owned(),
        credential,
        settings: Json::Object(settings),
        every,
        max_items,
    })
}

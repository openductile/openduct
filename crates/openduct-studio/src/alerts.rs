// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A workspace's alerts, sent to its own messengers: Slack, WhatsApp, Telegram, Discord, Microsoft Teams and Google
//! Chat, each through a credential of the workspace's (its people's accounts; OpenDuct holds none of its own). Its
//! admins choose where they go and try each; then every minute the dashboard's alerts that need someone (critical
//! and warning) are sent, each once - claimed in the cluster's model first, so one node sends it - and again only
//! when it says something new. An alert that clears is forgotten, and sent again should it come back.
//!
//! A message is sent by running the messenger's own step, as a workflow of one step would: the same code, the same
//! guard against this machine's network, the credential's secret added there and nowhere else.

use std::sync::Arc;
use std::time::Duration;

use hyper::StatusCode;
use openduct_expr::Value as ExprValue;
use openduct_flow::{Event, Outcome, Workflow};
use serde_json::{Value, json};

use crate::State;
use crate::api::{changing, signed_in};
use crate::http::{Answer, Call, json, problem};
use crate::logs::at;
use crate::model::{AlertChannel, AlertSent, Role, SCHEMA, Workspace, now};

/// The messengers alerts go to, the field of each that says where (none for a webhook: it is its own place), and
/// whether it must be given (a Slack channel only for a bot token; a Slack webhook is its own channel).
pub(crate) const CHANNELS: &[(&str, Option<&str>, bool)] = &[
    ("slack", Some("channel"), false),
    ("whatsapp", Some("to"), true),
    ("telegram", Some("chat_id"), true),
    ("discord", None, false),
    ("teams", None, false),
    ("google_chat", None, false),
];

/// How often alerts are looked for.
const EVERY: Duration = Duration::from_secs(60);
/// How long a claim to send an alert waits for another node's, before it is sent.
const CLAIM_WAIT: Duration = Duration::from_secs(2);

/// The workspace, if the person is its admin; reading needs no session token, a change does.
async fn admin_of(state: &State, call: &Call, id: &str, change: bool) -> Result<Workspace, Answer> {
    let user = if change {
        changing(state, call).await?.0
    } else {
        signed_in(state, call).await?.0
    };
    let _ = state.model.workspaces.refresh().await;
    let _ = state.model.members.refresh().await;
    match state
        .model
        .workspaces_of(&user)
        .await
        .into_iter()
        .find(|(w, _)| w.id == id)
    {
        Some((w, Role::Admin)) => Ok(w),
        Some(_) => Err(problem(
            StatusCode::FORBIDDEN,
            "this needs an admin's role in the workspace",
        )),
        None => Err(problem(StatusCode::NOT_FOUND, "no such workspace")),
    }
}

/// The channels a request sends, checked: known messengers, a credential each, a place where one is needed.
fn channels_of(body: &Value) -> Result<Vec<AlertChannel>, String> {
    let list = body
        .get("channels")
        .and_then(Value::as_array)
        .ok_or("channels: a list")?;
    let mut out = Vec::with_capacity(list.len());
    for (i, c) in list.iter().enumerate() {
        let kind = at(c, "/kind").as_str().unwrap_or_default();
        let Some((_, _, needed)) = CHANNELS.iter().find(|(k, _, _)| *k == kind) else {
            return Err(format!(
                "channels[{i}].kind: one of slack, whatsapp, telegram, discord, teams, google_chat"
            ));
        };
        let credential = at(c, "/credential")
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_owned();
        let target = at(c, "/target").as_str().unwrap_or_default().trim().to_owned();
        let on = at(c, "/on").as_bool().unwrap_or(false);
        if on && credential.is_empty() {
            return Err(format!("{kind}: choose its credential before turning it on"));
        }
        if on && *needed && target.is_empty() {
            return Err(format!("{kind}: say where to send, before turning it on"));
        }
        out.push(AlertChannel {
            kind: kind.to_owned(),
            credential,
            target,
            on,
        });
    }
    Ok(out)
}

pub(crate) async fn route(state: &State, call: &Call, id: &str) -> Option<Answer> {
    use hyper::Method;
    Some(
        match (&call.method, call.path.rsplit('/').next().unwrap_or_default()) {
            (&Method::GET, "alerts") => match admin_of(state, call, id, false).await {
                Ok(w) => json(StatusCode::OK, &json!({ "channels": w.alerts })),
                Err(answer) => answer,
            },
            (&Method::PUT, "alerts") => {
                let mut w = match admin_of(state, call, id, true).await {
                    Ok(w) => w,
                    Err(answer) => return Some(answer),
                };
                match channels_of(&call.body) {
                    Ok(channels) => {
                        w.alerts = channels;
                        w.updated = now();
                        match state.model.workspaces.put(&w.id, &w).await {
                            Ok(()) => json(StatusCode::OK, &json!({ "channels": w.alerts })),
                            Err(e) => problem(StatusCode::SERVICE_UNAVAILABLE, &e),
                        }
                    }
                    Err(why) => problem(StatusCode::UNPROCESSABLE_ENTITY, &why),
                }
            }
            (&Method::POST, "test") => {
                let w = match admin_of(state, call, id, true).await {
                    Ok(w) => w,
                    Err(answer) => return Some(answer),
                };
                let channel = match channels_of(&json!({ "channels": [call.body.clone()] })) {
                    Ok(mut c) => c.remove(0),
                    Err(why) => return Some(problem(StatusCode::UNPROCESSABLE_ENTITY, &why)),
                };
                let text = format!("OpenDuct: a test alert for {}. Alerts will arrive here.", w.name);
                match send(state, &w.id, &channel, &text).await {
                    Ok(()) => json(StatusCode::OK, &json!({ "sent": true })),
                    Err(why) => problem(StatusCode::BAD_GATEWAY, &format!("not sent: {why}")),
                }
            }
            _ => return None,
        },
    )
}

/// The one-step workflow that sends `text` through `channel`.
fn message(channel: &AlertChannel, text: &str) -> Value {
    // The text is the alert's own: braces in it are not expressions.
    let mut fields = json!({ "credential": channel.credential, "text": text.replace("{{", "{ {") });
    if let Some((_, Some(place), _)) = CHANNELS.iter().find(|(k, _, _)| *k == channel.kind)
        && !channel.target.is_empty()
        && let Some(f) = fields.as_object_mut()
    {
        f.insert((*place).to_owned(), json!(channel.target));
    }
    json!({ "name": "Alert", "trigger": "manual", "steps": [{ "name": "send", channel.kind.clone(): fields }] })
}

/// Sends `text` through `channel`, with the workspace's own credential.
async fn send(state: &State, workspace: &str, channel: &AlertChannel, text: &str) -> Result<(), String> {
    let workflow = Workflow::from_json(&message(channel, text), Some("alert")).map_err(|e| e.to_string())?;
    let effects = state.effects_for(workspace)?;
    let event = Event {
        value: ExprValue::Null,
        details: ExprValue::Null,
    };
    let record = openduct_flow::run(&workflow, &event, effects.as_ref()).await;
    match record.outcome {
        Outcome::Failed { error, .. } => Err(error),
        _ => Ok(()),
    }
}

/// The words of an alert, as a messenger shows them.
fn text_of(workspace: &Workspace, alert: &Value) -> String {
    let mark = if at(alert, "/level") == "critical" {
        "[!]"
    } else {
        "[i]"
    };
    format!(
        "{mark} OpenDuct - {}: {}\n{}\nIn OpenDuct: {}",
        workspace.name,
        at(alert, "/title").as_str().unwrap_or_default(),
        at(alert, "/detail").as_str().unwrap_or_default(),
        at(alert, "/action/label").as_str().unwrap_or_default(),
    )
}

/// Sends each workspace's new alerts to its channels, once, every minute - until the studio stops.
pub(crate) async fn keep_alerting(state: Arc<State>) {
    // This node's name for its claims.
    let me = format!("{:x}", rand_node());
    loop {
        tokio::time::sleep(EVERY).await;
        let _ = state.model.workspaces.refresh().await;
        let workspaces: Vec<Workspace> = state
            .model
            .workspaces
            .all()
            .await
            .into_iter()
            .map(|(_, w)| w)
            .filter(|w| w.deleted.is_none() && w.alerts.iter().any(|c| c.on))
            .collect();
        for w in workspaces {
            if let Err(e) = alert_one(&state, &w, &me).await {
                tracing::warn!(workspace = w.id, error = e, "cannot send the workspace's alerts");
            }
        }
    }
}

fn rand_node() -> u64 {
    let mut bytes = [0u8; 8];
    let _ = ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes);
    u64::from_le_bytes(bytes)
}

async fn alert_one(state: &State, w: &Workspace, me: &str) -> Result<(), String> {
    let summary = crate::dashboard::summary_of(state, &w.id).await;
    let alerts: Vec<Value> = at(&summary, "/alerts")
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|a| at(a, "/level") == "critical" || at(a, "/level") == "warning")
        .collect();
    state.model.alerts_sent.refresh().await?;
    let prefix = format!("{}/", w.id);
    // Cleared alerts are forgotten, to be sent again should they come back.
    for (key, _) in state.model.alerts_sent.all().await {
        if let Some(rest) = key.strip_prefix(&prefix)
            && !alerts.iter().any(|a| at(a, "/key") == rest)
        {
            let _ = state.model.alerts_sent.remove(&key).await;
        }
    }
    for alert in alerts {
        let key = format!("{prefix}{}", at(&alert, "/key").as_str().unwrap_or_default());
        let fingerprint = format!("{}|{}", at(&alert, "/level"), at(&alert, "/title"));
        if state
            .model
            .alerts_sent
            .get(&key)
            .await
            .is_some_and(|s| s.fingerprint == fingerprint)
        {
            continue;
        }
        // Claimed, then read back after a moment: of two nodes claiming it, the later claim stands, and only its
        // node sends.
        let claim = AlertSent {
            schema: SCHEMA,
            fingerprint: fingerprint.clone(),
            by: me.to_owned(),
            at: now(),
        };
        state.model.alerts_sent.put(&key, &claim).await?;
        tokio::time::sleep(CLAIM_WAIT).await;
        state.model.alerts_sent.refresh().await?;
        if state
            .model
            .alerts_sent
            .get(&key)
            .await
            .is_none_or(|s| s.by != me || s.fingerprint != fingerprint)
        {
            continue;
        }
        let text = text_of(w, &alert);
        for channel in w.alerts.iter().filter(|c| c.on) {
            if let Err(e) = send(state, &w.id, channel, &text).await {
                tracing::warn!(
                    workspace = w.id,
                    channel = channel.kind,
                    error = e,
                    "an alert was not sent"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    #[test]
    fn channels_are_checked_and_a_message_is_its_messengers_own_step() {
        let ok = channels_of(&json!({ "channels": [
            { "kind": "slack", "credential": "team", "target": "#ops", "on": true },
            { "kind": "google_chat", "credential": "", "on": false },
        ] }))
        .unwrap();
        assert_eq!(ok.len(), 2);
        let wrong = |c: Value| channels_of(&json!({ "channels": [c] })).unwrap_err();
        assert!(wrong(json!({ "kind": "fax", "credential": "x" })).contains("one of"));
        assert!(wrong(json!({ "kind": "teams", "on": true })).contains("credential"));
        assert!(
            wrong(json!({ "kind": "telegram", "credential": "bot", "on": true })).contains("where to send")
        );
        let telegram = AlertChannel {
            kind: "telegram".into(),
            credential: "bot".into(),
            target: "42".into(),
            on: true,
        };
        let w = Workflow::from_json(&message(&telegram, "Workflow {{ x }} failed"), Some("alert")).unwrap();
        assert_eq!(w.steps.len(), 1);
        let shown = message(&telegram, "Workflow {{ x }} failed");
        assert_eq!(shown["steps"][0]["telegram"]["chat_id"], "42");
        assert_eq!(shown["steps"][0]["telegram"]["text"], "Workflow { { x }} failed");
    }
}

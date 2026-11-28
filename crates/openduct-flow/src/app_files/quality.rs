// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! How finished an app is: whether each action says what it does and what it gives and has a recorded test, how it
//! signs in, where it reaches - and a score out of 100 against [`BAR`], the bar an app meets before it ships with
//! OpenDuct (see docs/apps.md). `openduct app check` shows it for every app, and the studio for a custom app.

use serde_json::{Value as Json, json};

use super::definition::{AppDef, Grant, SignIn};

/// The score an app meets before it ships with OpenDuct, or is kept as a workspace's custom app without a warning.
pub const BAR: u32 = 80;
/// The most fields an action's `output` names.
pub const MAX_OUTPUT: usize = 60;
/// The kinds an output's field may be.
const KINDS: &[&str] = &["text", "number", "boolean", "list", "record"];

/// An action's `output` as written: each field (as the service names it) and its kind.
pub(super) fn output_of(
    written: Option<&serde_json::Map<String, Json>>,
    at: &str,
) -> Result<Vec<(String, String)>, String> {
    let Some(fields) = written else {
        return Ok(Vec::new());
    };
    if fields.len() > MAX_OUTPUT {
        return Err(format!("{at}.output: at most {MAX_OUTPUT} fields"));
    }
    fields
        .iter()
        .map(|(name, kind)| {
            if name.is_empty() || name.len() > 100 || name.chars().any(char::is_control) {
                return Err(format!(
                    "{at}.output: {name:?} is a field's name, of at most 100 characters"
                ));
            }
            match kind.as_str() {
                Some(k) if KINDS.contains(&k) => Ok((name.clone(), k.to_owned())),
                _ => Err(format!(
                    "{at}.output.{name}: text, number, boolean, list or record"
                )),
            }
        })
        .collect()
}

/// How a sign-in signs in, in plain words.
pub fn sign_in_kind(s: &SignIn) -> &'static str {
    if let Some(o) = &s.oauth2 {
        return match o.grant {
            Grant::AuthorizationCode => "a sign-in at the vendor (OAuth 2.0)",
            Grant::ClientCredentials => "a client ID and secret (OAuth 2.0)",
        };
    }
    if s.url_field.is_some() {
        return "a webhook's address";
    }
    if s.basic.is_some() {
        return "a user name and password";
    }
    if !s.query.is_empty() {
        return "a key in the address";
    }
    // The header as it would be sent, its secrets as •••.
    let nobody = crate::apps::Credential {
        kind: s.id.clone(),
        fields: std::collections::BTreeMap::new(),
    };
    let names = [(
        "credential".to_owned(),
        super::engine::credential_value(s, &nobody),
    )];
    let bearer = s.headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("authorization")
            && super::engine::render(v, &names)
                .unwrap_or_default()
                .trim_start()
                .to_ascii_lowercase()
                .starts_with("bearer")
    });
    if bearer {
        return "a bearer token";
    }
    if s.headers.is_empty() {
        "nothing"
    } else {
        "a key in a header"
    }
}

/// An app's quality: what it has, what it lacks, and its score.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub actions: usize,
    /// Actions with a description of their own.
    pub described: usize,
    /// Actions that say what they give (`output`).
    pub with_output: usize,
    /// Actions with at least one recorded test.
    pub recorded: usize,
    /// A recording of a refusal.
    pub refusal: bool,
    /// Each sign-in: its id and how it signs in.
    pub sign_ins: Vec<(String, String)>,
    pub hosts: Vec<String>,
    pub docs: bool,
    pub readme: bool,
    pub score: u32,
    /// What would raise the score, in plain words.
    pub missing: Vec<String>,
}

/// The actions a set of recordings (`(file, text)`) records, and whether one records a refusal.
fn recorded_actions(recordings: &[(String, String)]) -> (Vec<String>, bool) {
    let mut actions = Vec::new();
    let mut refusal = false;
    for (_, text) in recordings {
        let Ok(json) = crate::yaml::to_json(text) else {
            continue;
        };
        if let Some(a) = json.get("action").and_then(Json::as_str) {
            actions.push(a.to_owned());
            refusal |= json.get("error").is_some_and(|e| !e.is_null());
        }
    }
    (actions, refusal)
}

fn listed(names: &[&str]) -> String {
    const SHOWN: usize = 5;
    let mut out = names.iter().take(SHOWN).copied().collect::<Vec<_>>().join(", ");
    if names.len() > SHOWN {
        out.push_str(&format!(" and {} more", names.len() - SHOWN));
    }
    out
}

/// The share of `part` in `whole`, of `points`.
fn share(part: usize, whole: usize, points: u32) -> u32 {
    if whole == 0 {
        return 0;
    }
    let part = u32::try_from(part).unwrap_or(u32::MAX);
    let whole = u32::try_from(whole).unwrap_or(u32::MAX);
    part.saturating_mul(points) / whole
}

/// The quality of `app`, given its recordings (`(file, text)` of its tests folder) and whether it has a README.
pub fn report(app: &AppDef, recordings: &[(String, String)], readme: bool) -> Report {
    let (recorded, refusal) = recorded_actions(recordings);
    let names: Vec<&str> = app.actions.iter().map(|(n, _)| n.as_str()).collect();
    let undescribed: Vec<&str> = app
        .actions
        .iter()
        .filter(|(_, a)| a.description.trim().is_empty())
        .map(|(n, _)| n.as_str())
        .collect();
    let no_output: Vec<&str> = app
        .actions
        .iter()
        .filter(|(_, a)| a.output.is_empty())
        .map(|(n, _)| n.as_str())
        .collect();
    let unrecorded: Vec<&str> = names
        .iter()
        .copied()
        .filter(|n| !recorded.iter().any(|r| r == n))
        .collect();
    let total = names.len();
    let self_hosted = app.base_url.is_empty() || app.sign_ins.iter().all(|s| s.url_field.is_some());
    let hosts_ok = !app.hosts.is_empty() || self_hosted;
    let docs = app.docs.as_deref().is_some_and(|d| d.starts_with("https://"));
    let labelled = app.sign_ins.iter().all(|s| !s.label.trim().is_empty());
    let score = share(total - undescribed.len(), total, 20)
        + share(total - no_output.len(), total, 20)
        + share(total - unrecorded.len(), total, 30)
        + if refusal { 10 } else { 0 }
        + if hosts_ok { 5 } else { 0 }
        + if docs { 5 } else { 0 }
        + if readme { 5 } else { 0 }
        + if labelled { 5 } else { 0 };
    let mut missing = Vec::new();
    if !unrecorded.is_empty() {
        missing.push(format!("no recorded test for {}", listed(&unrecorded)));
    }
    if !undescribed.is_empty() {
        missing.push(format!("no description for {}", listed(&undescribed)));
    }
    if !no_output.is_empty() {
        missing.push(format!("no output for {}", listed(&no_output)));
    }
    if !refusal {
        missing.push("no recorded refusal (an answer of 400 or more)".to_owned());
    }
    if !hosts_ok {
        missing.push("no hosts".to_owned());
    }
    if !docs {
        missing.push("no https:// link to the vendor's API reference (docs)".to_owned());
    }
    if !readme {
        missing.push("no README.md".to_owned());
    }
    if !labelled {
        missing.push("a sign-in without a label".to_owned());
    }
    Report {
        actions: total,
        described: total - undescribed.len(),
        with_output: total - no_output.len(),
        recorded: total - unrecorded.len(),
        refusal,
        sign_ins: app
            .sign_ins
            .iter()
            .map(|s| (s.id.clone(), sign_in_kind(s).to_owned()))
            .collect(),
        hosts: app.hosts.clone(),
        docs,
        readme,
        score,
        missing,
    }
}

impl Report {
    /// Whether it meets `bar`.
    pub fn meets(&self, bar: u32) -> bool {
        self.score >= bar
    }

    /// The report in a few lines, as `openduct app check` shows it.
    pub fn lines(&self) -> Vec<String> {
        let sign_ins: Vec<String> = self
            .sign_ins
            .iter()
            .map(|(id, how)| format!("{how} ({id})"))
            .collect();
        let mut out = vec![
            format!(
                "quality {}/100 (the bar is {BAR}): {} action{}, {} described, {} with an output, {} with a recorded test{}",
                self.score,
                self.actions,
                if self.actions == 1 { "" } else { "s" },
                self.described,
                self.with_output,
                self.recorded,
                if self.refusal { ", a refusal recorded" } else { "" }
            ),
            format!("signs in with {}", sign_ins.join("; ")),
            format!(
                "reaches {}",
                if self.hosts.is_empty() {
                    "the address its credential gives".to_owned()
                } else {
                    self.hosts.join(", ")
                }
            ),
        ];
        if !self.missing.is_empty() {
            out.push(format!("to do: {}", self.missing.join("; ")));
        }
        out
    }

    /// The report as the studio shows it.
    pub fn to_json(&self) -> Json {
        let sign_ins: Vec<Json> = self
            .sign_ins
            .iter()
            .map(|(id, how)| json!({ "id": id, "kind": how }))
            .collect();
        json!({
            "score": self.score, "bar": BAR, "meets": self.meets(BAR), "actions": self.actions,
            "described": self.described, "with_output": self.with_output, "recorded": self.recorded,
            "refusal": self.refusal, "sign_ins": sign_ins, "hosts": self.hosts, "docs": self.docs,
            "readme": self.readme, "missing": self.missing,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;

    const APP: &str = r#"
app: quality_test
name: Quality
docs: https://quality.example/docs
hosts: [api.quality.example]
base_url: https://api.quality.example
sign_in:
  - { id: quality_test, label: An API key, fields: [{ name: key, label: Key, secret: true }], headers: { X-Key: "{{ credential.key }}" } }
  - { id: quality_test_token, label: A token, fields: [{ name: t, label: Token, secret: true }], headers: { Authorization: "Bearer {{ credential.t }}" } }
actions:
  list_things:
    label: List things
    description: Every thing, page by page.
    method: GET
    path: /things
    items: value
    output: { id: text, size: number }
  add_thing:
    label: Add a thing
    write: true
    method: POST
    path: /things
"#;

    #[test]
    fn the_report_counts_what_each_action_has_and_scores_it() {
        let app = AppDef::parse(APP, "q.yaml").unwrap();
        let none = report(&app, &[], false);
        assert_eq!(
            (none.actions, none.described, none.with_output, none.recorded),
            (2, 1, 1, 0)
        );
        // 10 + 10 for half described and with an output; hosts, docs and labels; no recordings, refusal or README.
        assert_eq!(none.score, 10 + 10 + 5 + 5 + 5);
        assert!(!none.meets(BAR));
        assert!(
            none.missing
                .iter()
                .any(|m| m.contains("no recorded test for add_thing, list_things"))
        );
        assert_eq!(
            none.sign_ins,
            [
                ("quality_test".to_owned(), "a key in a header".to_owned()),
                ("quality_test_token".to_owned(), "a bearer token".to_owned())
            ]
        );
        let recordings = vec![
            (
                "a.yaml".to_owned(),
                "action: list_things\nexchanges: []\n".to_owned(),
            ),
            (
                "b.yaml".to_owned(),
                "action: add_thing\nexchanges: []\nerror: no\n".to_owned(),
            ),
        ];
        let full = report(&app, &recordings, true);
        assert_eq!(full.score, 10 + 10 + 30 + 10 + 5 + 5 + 5 + 5);
        assert!(full.meets(BAR), "{:?}", full.lines());
        assert!(full.lines()[0].starts_with("quality 80/100"));
    }

    #[test]
    fn an_output_names_fields_of_known_kinds() {
        let wrong = AppDef::parse(&APP.replace("size: number", "size: amount"), "q.yaml").unwrap_err();
        assert!(wrong.contains("actions.list_things.output.size"), "{wrong}");
    }
}

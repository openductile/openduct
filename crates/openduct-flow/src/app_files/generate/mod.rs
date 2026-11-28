// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Apps made, not written: from a vendor's API description (OpenAPI 3.0 or 3.1, or Swagger 2.0, in JSON or YAML)
//! or from the tools of its MCP server - a checked definition, a README and recorded tests that pass with no account,
//! all alike. `openduct app from-openapi` and `openduct app from-mcp` write them to a folder; the studio's "Add an
//! app" page makes a workspace's custom app with the same code.
//!
//! What a description says is used, and nothing is guessed: what an app cannot take (file uploads, XML bodies,
//! callbacks and webhooks, some sign-ins) is left out and listed in the README under "Not done".

mod auth;
mod build;
mod emit;
mod mcp;
mod pages;
pub(crate) mod rank;
mod read;
mod schema;
mod spec;

use serde_json::{Map, Value as Json, json};

pub use auth::SignInChoice;
pub use mcp::{McpOptions, from_mcp};
pub use read::MAX_SPEC;

/// How many actions an app is made with unless asked, and the most.
pub const DEFAULT_ACTIONS: usize = 25;
pub const MAX_ACTIONS: usize = 100;
/// The most operations shown as proposed (the best ones).
const MAX_PROPOSED: usize = 500;

/// What to make an app from a description with.
#[derive(Debug, Clone, Default)]
pub struct Options {
    /// The app's id: lowercase letters, digits and `_`.
    pub id: String,
    /// Its name as people know it (by default the description's title).
    pub name: Option<String>,
    pub category: Option<String>,
    /// How many actions at most (by default [`DEFAULT_ACTIONS`]).
    pub actions: usize,
    /// Only operations with one of these tags.
    pub only: Vec<String>,
    /// Exactly these actions (by their proposed names), in place of the ranking's best.
    pub chosen: Option<Vec<String>>,
    /// Only these sign-ins (by their proposed ids).
    pub sign_ins: Option<Vec<String>>,
    /// A sign-in the description does not say, given by hand.
    pub sign_in: Option<SignInChoice>,
    /// The API's address, when the description names none (or to use another).
    pub base_url: Option<String>,
    /// The vendor's API reference, when the description names none.
    pub docs: Option<String>,
    /// Where the description came from, as the README says it (a URL or a file's name).
    pub source: String,
    /// The address the description was fetched from, for servers it names relatively.
    pub origin: Option<String>,
    /// The day it is made, as the README says it.
    pub made_on: String,
}

/// An operation as a possible action, as the studio shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposed {
    pub name: String,
    pub label: String,
    pub method: String,
    pub path: String,
    pub score: i32,
    pub chosen: bool,
    /// Why it cannot be an action, if it cannot.
    pub left_out: Option<String>,
}

/// A sign-in the description allows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedSignIn {
    pub id: String,
    pub label: String,
    pub kind: String,
    pub chosen: bool,
}

/// An app as made: its files, what was proposed, and what is not done.
#[derive(Debug, Clone)]
pub struct Generated {
    pub id: String,
    pub name: String,
    /// `app.yaml`, checked.
    pub app_yaml: String,
    pub readme: String,
    /// `tests/<file>` and its text.
    pub tests: Vec<(String, String)>,
    pub proposed: Vec<Proposed>,
    pub sign_ins: Vec<ProposedSignIn>,
    /// What the app does not do of what the description has.
    pub not_done: Vec<String>,
}

impl Generated {
    /// What the studio shows of it.
    pub fn to_json(&self) -> Json {
        let proposed: Vec<Json> = self
            .proposed
            .iter()
            .map(|p| {
                json!({ "name": p.name, "label": p.label, "method": p.method, "path": p.path, "score": p.score,
                        "chosen": p.chosen, "left_out": p.left_out })
            })
            .collect();
        let sign_ins: Vec<Json> = self
            .sign_ins
            .iter()
            .map(|s| json!({ "id": s.id, "label": s.label, "kind": s.kind, "chosen": s.chosen }))
            .collect();
        json!({
            "id": self.id, "name": self.name, "app_yaml": self.app_yaml, "readme": self.readme,
            "tests": self.tests.iter().map(|(f, t)| json!({ "file": f, "text": t })).collect::<Vec<_>>(),
            "proposed": proposed, "sign_ins": sign_ins, "not_done": self.not_done,
        })
    }
}

/// Today, as a README says it (`2026-11-27`, in UTC).
pub fn today() -> String {
    let days = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() / 86_400);
    // Days since 1970-01-01 as a date (Howard Hinnant's civil-from-days).
    let z = i64::try_from(days).unwrap_or(0) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// The origin of an address: `https://api.example.com`.
fn origin_of(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .map(|u| u.origin().ascii_serialization())
        .unwrap_or_default()
}

/// Checks a made definition the way a written one is checked.
fn checked(yaml: &str, id: &str) -> Result<(), String> {
    super::AppDef::parse(yaml, &format!("{id}/app.yaml"))
        .map(|_| ())
        .map_err(|e| format!("the app made does not check (a fault of OpenDuct's: please report it): {e}"))
}

/// Makes an app from the API description in `text`.
pub fn from_openapi(text: &str, options: &Options) -> Result<Generated, String> {
    if !super::definition::named(&options.id) {
        return Err(format!(
            "{:?}: an app's id is lowercase letters, digits and '_', at most 40, starting with a letter",
            options.id
        ));
    }
    let doc = read::document(text)?;
    let api = spec::read(&doc, options.origin.as_deref())?;
    let base = match &options.base_url {
        Some(b) => b.trim_end_matches('/').to_owned(),
        None => api
            .servers
            .iter()
            .find(|s| s.starts_with("https://"))
            .or_else(|| api.servers.first())
            .cloned()
            .ok_or("the description names no server: give the API's address (--base-url)")?
            .trim_end_matches('/')
            .to_owned(),
    };
    let parsed = reqwest::Url::parse(&base).map_err(|e| format!("the API's address {base:?}: {e}"))?;
    if !super::definition::secure_address(&parsed) {
        return Err(format!(
            "the API's address {base} is not an https:// address: give one (--base-url)"
        ));
    }
    let host = parsed.host_str().unwrap_or_default().to_owned();
    let origin = origin_of(&base);
    let name = options
        .name
        .clone()
        .filter(|n| !n.trim().is_empty())
        .unwrap_or_else(|| {
            let t = api.title.trim();
            let t = t.strip_suffix(" API").unwrap_or(t);
            if t.is_empty() {
                rank::words(&options.id)
            } else {
                rank::sentence(t, 60)
            }
        });
    let most = if options.actions == 0 {
        DEFAULT_ACTIONS
    } else {
        options.actions.min(MAX_ACTIONS)
    };
    let candidates = rank::rank(&api, &options.only);
    let picked = rank::choose(&candidates, most, options.chosen.as_deref());
    if picked.is_empty() {
        return Err(if options.chosen.is_some() {
            "none of the actions chosen is one the description offers".to_owned()
        } else {
            "the description offers no operation an app can make (see what is left out with --actions)"
                .to_owned()
        });
    }
    let chosen_ops: Vec<&spec::Operation<'_>> = picked
        .iter()
        .filter_map(|&i| candidates.get(i).and_then(|c| api.operations.get(c.index)))
        .collect();
    let mut signs = auth::sign_ins(&api, &chosen_ops, &options.id, &name, options.sign_in.as_ref());
    let all_sign_ins: Vec<ProposedSignIn> = signs
        .made
        .iter()
        .map(|m| ProposedSignIn {
            id: m.id.clone(),
            label: m.label.clone(),
            kind: m.kind.to_owned(),
            chosen: options.sign_ins.as_ref().is_none_or(|k| k.contains(&m.id)),
        })
        .collect();
    if let Some(keep) = &options.sign_ins {
        signs.made.retain(|m| keep.contains(&m.id));
    }
    let Some(first) = signs.made.first() else {
        let why = if signs.not_done.is_empty() {
            "the description does not say how it signs in".to_owned()
        } else {
            format!(
                "none of its sign-ins can be an app's ({})",
                signs.not_done.join("; ")
            )
        };
        return Err(format!(
            "{why}: give one by hand (--sign-in bearer, basic, header:<name> or query:<name>)"
        ));
    };
    let cx = build::Context {
        api: &api,
        base: &base,
        origin: &origin,
        sign_in: first,
    };
    let built: Vec<build::Built> = picked
        .iter()
        .filter_map(|&i| {
            let c = candidates.get(i)?;
            Some(build::action(&cx, api.operations.get(c.index)?, c))
        })
        .collect();
    let docs = options.docs.clone().or_else(|| api.docs.clone()).or_else(|| {
        options
            .source
            .starts_with("https://")
            .then(|| options.source.clone())
    });
    let description = {
        let d = rank::sentence(&api.description, 200);
        if d.is_empty() { format!("{name}'s API.") } else { d }
    };
    let mut app = Map::new();
    app.insert("app".to_owned(), json!(options.id));
    app.insert("name".to_owned(), json!(name));
    app.insert(
        "category".to_owned(),
        json!(options.category.clone().unwrap_or_else(|| "other".to_owned())),
    );
    app.insert("description".to_owned(), json!(description));
    if let Some(d) = &docs {
        app.insert("docs".to_owned(), json!(d));
    }
    app.insert("hosts".to_owned(), json!([host]));
    app.insert("base_url".to_owned(), json!(base));
    app.insert(
        "sign_in".to_owned(),
        Json::Array(signs.made.iter().map(|m| m.entry.clone()).collect()),
    );
    let actions: Map<String, Json> = built
        .iter()
        .map(|b| (b.name.clone(), b.definition.clone()))
        .collect();
    app.insert("actions".to_owned(), Json::Object(actions));
    let source = rank::sentence(&options.source, 200);
    let dialect = match api.dialect {
        spec::Dialect::Swagger2 => "Swagger 2.0",
        spec::Dialect::OpenApi30 => "OpenAPI 3.0",
        spec::Dialect::OpenApi31 => "OpenAPI 3.1",
    };
    let comment = vec![
        format!(
            "{name}: made by `openduct app from-openapi` from {source} ({} {}, {dialect}).",
            api.title, api.version
        ),
        "Checked as made. Change what you like, then run `openduct app check` and `openduct app test` again."
            .to_owned(),
    ];
    let app_yaml = emit::file(&Json::Object(app), &comment);
    checked(&app_yaml, &options.id)?;
    // What it does not do.
    let mut not_done = Vec::new();
    let mut left: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for c in &candidates {
        if let Some(why) = &c.left_out {
            *left.entry(why.clone()).or_default() += 1;
        }
    }
    for (why, n) in left {
        not_done.push(format!(
            "{n} operation{} left out: {why}",
            if n == 1 { "" } else { "s" }
        ));
    }
    let offered = candidates.iter().filter(|c| c.left_out.is_none()).count();
    if offered > built.len() {
        not_done.push(format!(
            "{} more operations it could take (made with the best {}: --actions or the studio's list chooses others)",
            offered - built.len(),
            built.len()
        ));
    }
    if api.cut > 0 {
        not_done.push(format!(
            "{} operations beyond the first {} read",
            api.cut,
            spec::MAX_OPERATIONS
        ));
    }
    if api.webhooks > 0
        || built
            .iter()
            .any(|b| b.notes.iter().any(|n| n.contains("callbacks")))
    {
        not_done.push("the vendor's webhooks and callbacks: a trigger for them is written by hand (docs/apps.md, Triggers)".to_owned());
    }
    not_done.extend(signs.not_done.iter().cloned());
    not_done.extend(api.server_notes.iter().cloned());
    for b in &built {
        not_done.extend(b.notes.iter().cloned());
    }
    // Recorded tests: one for each action, and a refusal.
    let mut tests: Vec<(String, String)> = built
        .iter()
        .map(|b| {
            (
                format!("{}.yaml", b.name),
                emit::file(
                    &b.recording,
                    &[format!("Made by `openduct app from-openapi`: {}.", b.label)],
                ),
            )
        })
        .collect();
    if let Some(b) = built.iter().find(|b| b.read).or_else(|| built.first()) {
        tests.push((
            "refused.yaml".to_owned(),
            emit::file(
                &build::refusal(b, &name),
                &["Made by `openduct app from-openapi`: a refusal.".to_owned()],
            ),
        ));
    }
    let readme = readme(
        &name,
        &options.id,
        &description,
        docs.as_deref(),
        &source,
        dialect,
        &signs.made,
        &built,
        &not_done,
        &options.made_on,
    );
    let proposed = candidates
        .iter()
        .take(MAX_PROPOSED)
        .enumerate()
        .map(|(i, c)| {
            let op = api.operations.get(c.index);
            Proposed {
                name: c.name.clone(),
                label: c.label.clone(),
                method: op.map(|o| o.method.clone()).unwrap_or_default(),
                path: op.map(|o| o.path.clone()).unwrap_or_default(),
                score: c.score,
                chosen: picked.contains(&i),
                left_out: c.left_out.clone(),
            }
        })
        .collect();
    Ok(Generated {
        id: options.id.clone(),
        name,
        app_yaml,
        readme,
        tests,
        proposed,
        sign_ins: all_sign_ins,
        not_done,
    })
}

#[allow(clippy::too_many_arguments)]
fn readme(
    name: &str,
    id: &str,
    description: &str,
    docs: Option<&str>,
    source: &str,
    dialect: &str,
    sign_ins: &[auth::Made],
    built: &[build::Built],
    not_done: &[String],
    made_on: &str,
) -> String {
    let mut out = format!("# {name}\n\n{description}\n\n");
    out.push_str(&format!(
        "Made by `openduct app from-openapi` from {source} ({dialect}){}.",
        if made_on.is_empty() {
            String::new()
        } else {
            format!(" on {made_on}")
        }
    ));
    if let Some(d) = docs {
        out.push_str(&format!(" The vendor's API reference: <{d}>."));
    }
    out.push_str("\n\n## Signing in\n\n");
    for s in sign_ins {
        out.push_str(&format!("- **{}** (`{}`): {}.\n", s.label, s.id, s.readme));
    }
    out.push_str("\n## Actions\n\n");
    for b in built {
        let what = if b.description.is_empty() {
            String::new()
        } else {
            format!(": {}", b.description.trim_end_matches('.'))
        };
        out.push_str(&format!("- **{}** (`{}`){what}.\n", b.label, b.name));
    }
    out.push_str("\n## Not done\n\n");
    if not_done.is_empty() {
        out.push_str("Nothing of the description is left out.\n");
    } else {
        for n in not_done {
            out.push_str(&format!("- {n}\n"));
        }
    }
    out.push_str(&format!(
        "\n## Tested\n\nRecorded exchanges made from the API description's schemas (`tests/`): what each request must \
         send, and an answer that fits. Not run against a live {name} account.\n\n```sh\nopenduct app check apps/{id}\n\
         openduct app test apps/{id}\n```\n"
    ));
    out
}

#[cfg(test)]
mod tests;

// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Which of a description's operations become an app's actions, and what each is called.
//!
//! Each operation is scored (docs/apps.md, "Which operations become actions"):
//!
//! - **what it does**: list 60, look up 50, create 50, change 40, read 35, another action 25, remove 15;
//! - **how it is described**: a summary 10, a description 5;
//! - **how main its resource is**: 3 for each other operation on the same first part of the path, up to 18;
//! - **how deep it is**: less 6 for each path parameter beyond the fewest any operation has.
//!
//! Deprecated operations, file uploads and XML bodies are left out (and listed). The main resources come first:
//! those with the most operations, then those the description names first; each gives up to a
//! fifth of the actions (at least 3), one of each kind - list, look up, create, change, remove - before a second of
//! any, its best first. What room is left goes to the best of the rest. Ties keep the description's order.
//! An action's name is what it does and to what - `list_files`, `get_file`, `create_folder`, `copy_file` -
//! qualified by the resource it is under when two would be the same (`list_file_comments`), then by the operation's
//! own id, then by a number.

use super::schema::list_in;
use super::spec::{Api, BodyKind, Operation, Place};

/// The longest name of an action or an input.
pub(super) const MAX_NAME: usize = 40;

/// What an operation does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Verb {
    List,
    Get,
    Read,
    Create,
    Update,
    Delete,
    /// Another action, by its own word (`copy`, `send`).
    Do(String),
}

/// An operation as a possible action.
#[derive(Debug, Clone)]
pub(super) struct Candidate {
    /// Its place among the description's operations.
    pub(super) index: usize,
    pub(super) verb: Verb,
    pub(super) score: i32,
    /// Why it is not offered, if it is not.
    pub(super) left_out: Option<String>,
    pub(super) name: String,
    pub(super) label: String,
    /// The first part of its path: what "the same resource" means for the ranking.
    pub(super) area: String,
}

/// `text` as a name: lowercase words joined by `_` (`listFilesV2` is `list_files_v2`).
pub(crate) fn snake(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::new();
    for (i, c) in chars.iter().enumerate() {
        if c.is_ascii_alphanumeric() {
            let before = i.checked_sub(1).and_then(|j| chars.get(j)).copied();
            let after = chars.get(i + 1).copied();
            let boundary = c.is_ascii_uppercase()
                && (before.is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                    || before.is_some_and(|b| b.is_ascii_uppercase())
                        && after.is_some_and(|a| a.is_ascii_lowercase()));
            if boundary && !out.ends_with('_') && !out.is_empty() {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
        }
    }
    let out = out.trim_matches('_').to_owned();
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        format!("n_{out}")
    } else {
        out
    }
}

/// A name cut to [`MAX_NAME`], at a word where it can be.
pub(super) fn cut(name: &str) -> String {
    if name.len() <= MAX_NAME {
        return name.to_owned();
    }
    let mut out = String::new();
    for word in name.split('_') {
        let more = if out.is_empty() {
            word.len()
        } else {
            out.len() + 1 + word.len()
        };
        if more > MAX_NAME {
            break;
        }
        if !out.is_empty() {
            out.push('_');
        }
        out.push_str(word);
    }
    if out.is_empty() {
        name.chars().take(MAX_NAME).collect()
    } else {
        out
    }
}

/// One of a word: `files` is `file`, `entries` is `entry`, `addresses` is `address`.
pub(super) fn singular(word: &str) -> String {
    let w = word.to_owned();
    if let Some(stem) = w.strip_suffix("ies").filter(|s| s.len() > 1) {
        return format!("{stem}y");
    }
    for ending in ["sses", "xes", "ches", "shes", "zzes"] {
        if w.ends_with(ending) {
            return w.get(..w.len() - 2).unwrap_or(&w).to_owned();
        }
    }
    match w.strip_suffix('s') {
        Some(stem) if !stem.ends_with('s') && !stem.ends_with('u') && stem.len() > 1 => stem.to_owned(),
        _ => w,
    }
}

/// A name in words for a person: `list_file_comments` is "List file comments".
pub(super) fn words(name: &str) -> String {
    let spaced = name.replace('_', " ");
    let mut c = spaced.chars();
    c.next()
        .map_or_else(String::new, |f| f.to_uppercase().collect::<String>() + c.as_str())
}

/// The first sentence of a text, without Markdown's marks, at most `most` characters (cut at a word).
pub(super) fn sentence(text: &str, most: usize) -> String {
    let flat: String = without_tags(text)
        .replace(['\r', '\n', '\t'], " ")
        .replace("**", "")
        .replace('`', "");
    let mut flat = without_links(&flat)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if let Some(end) = sentence_end(&flat) {
        flat.truncate(end + 1);
    }
    if flat.chars().count() <= most {
        return flat;
    }
    let mut out = String::new();
    for word in flat.split(' ') {
        if out.chars().count() + word.chars().count() + 1 > most.saturating_sub(1) {
            break;
        }
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    out.push('…');
    out
}

/// Where a text's first sentence ends: its first ". " that does not follow an abbreviation (`ie.`, `e.g.`).
fn sentence_end(text: &str) -> Option<usize> {
    const SHORT: &[&str] = &[
        "ie", "i.e", "eg", "e.g", "etc", "vs", "cf", "incl", "approx", "no", "mr", "ms", "dr", "st",
    ];
    let mut from = 0;
    while let Some(at) = text.get(from..).and_then(|t| t.find(". ")).map(|i| i + from) {
        let word = text
            .get(..at)
            .unwrap_or_default()
            .rsplit(' ')
            .next()
            .unwrap_or_default()
            .trim_start_matches('(')
            .to_ascii_lowercase();
        if !SHORT.contains(&word.as_str()) {
            return Some(at);
        }
        from = at + 2;
    }
    None
}

/// A text without HTML's tags (`<br>`, `<b>`): each a space.
fn without_tags(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find('<') {
        let tag = rest.get(open + 1..).unwrap_or_default();
        let is_tag = tag.starts_with(|c: char| c.is_ascii_alphabetic() || c == '/');
        match (is_tag, tag.find('>')) {
            (true, Some(close)) if close <= 40 => {
                out.push_str(rest.get(..open).unwrap_or_default());
                out.push(' ');
                rest = tag.get(close + 1..).unwrap_or_default();
            }
            _ => {
                out.push_str(rest.get(..=open).unwrap_or_default());
                rest = tag;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Markdown links as their words: `[the guide](https://...)` is "the guide".
fn without_links(text: &str) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(open) = rest.find('[') {
        let Some(mid) = rest[open..].find("](") else {
            break;
        };
        let Some(close) = rest[open + mid..].find(')') else {
            break;
        };
        out.push_str(&rest[..open]);
        out.push_str(&rest[open + 1..open + mid]);
        rest = &rest[open + mid + close + 1..];
    }
    out.push_str(rest);
    out
}

/// An operation's label: its summary's first sentence (no full stop), else its name in words.
fn label_of(op: &Operation<'_>, name: &str) -> String {
    let s = sentence(&op.summary, 80);
    let s = s.trim_end_matches('.').trim().to_owned();
    if s.is_empty() {
        return words(name);
    }
    let mut c = s.chars();
    c.next()
        .map_or_else(String::new, |f| f.to_uppercase().collect::<String>() + c.as_str())
}

fn is_param(segment: &str) -> bool {
    segment.starts_with('{') && segment.ends_with('}')
}

/// A path's part that names no resource: a version (`v2`, `2.0`) or `api`.
fn is_version(segment: &str) -> bool {
    let s = segment.to_ascii_lowercase();
    let digits = s.strip_prefix('v').unwrap_or(&s);
    (!digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit() || c == '.'))
        || matches!(s.as_str(), "api" | "rest" | "restapi")
}

/// What the resources of a path are: the static parts, each with whether a parameter follows it.
fn parts(path: &str) -> Vec<(String, bool)> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let mut out = Vec::new();
    for (i, s) in segments.iter().enumerate() {
        if is_param(s) || is_version(s) {
            continue;
        }
        let followed = segments.get(i + 1).is_some_and(|n| is_param(n));
        out.push((snake(s), followed));
    }
    out
}

/// Why an operation is not offered, if it is not.
fn left_out(op: &Operation<'_>, only: &[String]) -> Option<String> {
    if op.deprecated {
        return Some("deprecated".to_owned());
    }
    if let Some(b) = &op.body {
        match &b.kind {
            BodyKind::Multipart => return Some("a file upload (multipart)".to_owned()),
            BodyKind::Xml => return Some("an XML body".to_owned()),
            BodyKind::Other(t) => return Some(format!("a body of {t}")),
            BodyKind::Json | BodyKind::Form => {}
        }
    }
    if op.params.iter().any(|p| p.place == Place::Cookie && p.required) {
        return Some("a cookie it needs".to_owned());
    }
    if !only.is_empty()
        && !op
            .tags
            .iter()
            .any(|t| only.iter().any(|o| o.eq_ignore_ascii_case(t)))
    {
        return Some("not among the tags asked for".to_owned());
    }
    None
}

/// What an operation does, from its method and path (and whether another reads the same path).
fn verb_of(api: &Api<'_>, op: &Operation<'_>, read_too: bool) -> Verb {
    let segments: Vec<&str> = op.path.split('/').filter(|s| !s.is_empty()).collect();
    let item = segments.last().is_some_and(|s| is_param(s));
    let last = segments.last().map(|s| snake(s)).unwrap_or_default();
    let after_param = segments.len() >= 2 && segments.get(segments.len() - 2).is_some_and(|s| is_param(s));
    match (op.method.as_str(), item) {
        ("GET", true) => Verb::Get,
        ("GET", false) => {
            let list = op
                .answer
                .as_ref()
                .and_then(|a| a.schema.as_ref())
                .is_some_and(|s| list_in(api.root, s).is_some());
            if list { Verb::List } else { Verb::Read }
        }
        ("POST", true) | ("PUT" | "PATCH", _) => Verb::Update,
        ("DELETE", _) => Verb::Delete,
        // A collection another operation lists is made in; `/files/{id}/copy` and `/search` do something else.
        ("POST", false) if read_too => Verb::Create,
        ("POST", false) if !after_param && last.ends_with('s') => Verb::Create,
        ("POST", false) => Verb::Do(last),
        _ => Verb::Read,
    }
}

/// The names an operation could have, best first.
fn names_for(op: &Operation<'_>, verb: &Verb) -> Vec<String> {
    let parts = parts(&op.path);
    let item = op.path.trim_end_matches('/').ends_with('}');
    // The resource: the last part; for an action of its own (`/files/{id}/copy`), the part before it.
    let (resource, parent) = match verb {
        Verb::Do(_) => (
            parts.len().checked_sub(2).and_then(|i| parts.get(i)),
            parts.len().checked_sub(3).and_then(|i| parts.get(i)),
        ),
        _ => (
            parts.last(),
            parts.len().checked_sub(2).and_then(|i| parts.get(i)),
        ),
    };
    let resource = resource.map(|(r, _)| r.clone()).unwrap_or_default();
    let parent = parent.filter(|(_, followed)| *followed).map(|(p, _)| singular(p));
    let one = singular(&resource);
    let (word, thing) = match verb {
        Verb::List => ("list", resource.clone()),
        Verb::Get => ("get", one),
        Verb::Read => ("get", resource.clone()),
        Verb::Create => ("create", one),
        Verb::Update => ("update", if item { one } else { resource.clone() }),
        Verb::Delete => ("delete", if item { one } else { resource.clone() }),
        Verb::Do(w) => (w.as_str(), one),
    };
    let join = |parts: &[&str]| {
        cut(&parts
            .iter()
            .filter(|p| !p.is_empty())
            .copied()
            .collect::<Vec<_>>()
            .join("_"))
    };
    let mut out = Vec::new();
    if !thing.is_empty() {
        out.push(join(&[word, &thing]));
        if let Some(p) = &parent
            && p != &thing
        {
            out.push(join(&[word, p, &thing]));
        }
    }
    let id = snake(&op.id);
    if !id.is_empty() {
        out.push(cut(&id));
    }
    let fallback = cut(&snake(&format!("{} {}", op.method, op.path)));
    out.push(fallback);
    out
}

/// Every operation of `api` as a possible action, best first, each named; `only` keeps those with one of its tags.
pub(super) fn rank(api: &Api<'_>, only: &[String]) -> Vec<Candidate> {
    let ops = &api.operations;
    let area_of = |op: &Operation<'_>| {
        parts(&op.path)
            .first()
            .map(|(a, _)| a.clone())
            .unwrap_or_default()
    };
    let depth_of = |op: &Operation<'_>| op.path.matches('{').count() as i32;
    let least_depth = ops.iter().map(depth_of).min().unwrap_or(0);
    let mut per_area: std::collections::HashMap<String, i32> = std::collections::HashMap::new();
    for op in ops {
        *per_area.entry(area_of(op)).or_default() += 1;
    }
    let mut out: Vec<Candidate> = ops
        .iter()
        .enumerate()
        .map(|(index, op)| {
            let read_too = ops.iter().any(|o| o.path == op.path && o.method == "GET");
            let verb = verb_of(api, op, read_too);
            let mut score = match verb {
                Verb::List => 60,
                Verb::Get | Verb::Create => 50,
                Verb::Update => 40,
                Verb::Read => 35,
                Verb::Do(_) => 25,
                Verb::Delete => 15,
            };
            if !op.summary.trim().is_empty() {
                score += 10;
            }
            if !op.description.trim().is_empty() {
                score += 5;
            }
            let area = area_of(op);
            score += (per_area.get(&area).copied().unwrap_or(1) - 1).clamp(0, 6) * 3;
            score -= (depth_of(op) - least_depth).max(0) * 6;
            Candidate {
                index,
                verb,
                score,
                left_out: left_out(op, only),
                name: String::new(),
                label: String::new(),
                area,
            }
        })
        .collect();
    out.sort_by(|a, b| b.score.cmp(&a.score).then(a.index.cmp(&b.index)));
    // Names in the order of the ranking: the better operation keeps the plainer name.
    let mut taken: Vec<String> = Vec::new();
    for c in &mut out {
        let Some(op) = ops.get(c.index) else {
            continue;
        };
        let names = names_for(op, &c.verb);
        let mut name = names
            .iter()
            .find(|n| !taken.contains(n) && super::super::definition::named(n))
            .cloned();
        if name.is_none() {
            let base = cut(names.first().map_or("action", String::as_str));
            let base = base
                .get(..base.len().min(MAX_NAME - 4))
                .unwrap_or(&base)
                .to_owned();
            name = (2..10_000)
                .map(|n| format!("{base}_{n}"))
                .find(|n| !taken.contains(n));
        }
        let name = name.unwrap_or_else(|| format!("action_{}", c.index));
        c.label = label_of(op, &name);
        taken.push(name.clone());
        c.name = name;
    }
    out
}

/// The kind of what an operation does, for taking one of each from a resource before a second of any.
fn verb_kind(v: &Verb) -> u8 {
    match v {
        Verb::List => 0,
        Verb::Get => 1,
        Verb::Create => 2,
        Verb::Update => 3,
        Verb::Delete => 4,
        Verb::Read => 5,
        Verb::Do(_) => 6,
    }
}

/// The candidates an app is made of: those named in `chosen` (in the ranking's order), or else the best `most`.
/// The main resources come first - those with the most operations, then those the description names first - each
/// giving up to a fifth of the actions (at least 3), one of each kind (list, look up, create, change,
/// remove...) before a second of any; what room is left goes to the best of the rest.
pub(super) fn choose(candidates: &[Candidate], most: usize, chosen: Option<&[String]>) -> Vec<usize> {
    let offered: Vec<usize> = (0..candidates.len())
        .filter(|&i| candidates.get(i).is_some_and(|c| c.left_out.is_none()))
        .collect();
    if let Some(chosen) = chosen {
        return offered
            .into_iter()
            .filter(|&i| candidates.get(i).is_some_and(|c| chosen.contains(&c.name)))
            .take(most)
            .collect();
    }
    // Each resource: how many operations it offers, and where the description first names it.
    let mut areas: Vec<(&str, usize, usize)> = Vec::new();
    for &i in &offered {
        let Some(c) = candidates.get(i) else {
            continue;
        };
        match areas.iter_mut().find(|(a, _, _)| *a == c.area) {
            Some(a) => {
                a.1 += 1;
                a.2 = a.2.min(c.index);
            }
            None => areas.push((c.area.as_str(), 1, c.index)),
        }
    }
    areas.sort_by_key(|(_, n, first)| (std::cmp::Reverse(*n), *first));
    let share = (most / 5).max(3);
    let mut out: Vec<usize> = Vec::new();
    for (area, _, _) in &areas {
        let mine: Vec<usize> = offered
            .iter()
            .copied()
            .filter(|&i| candidates.get(i).is_some_and(|c| c.area == *area))
            .collect();
        let mut kinds: Vec<u8> = Vec::new();
        let mut taken = 0;
        for one_of_each in [true, false] {
            for &i in &mine {
                if taken >= share || out.len() >= most {
                    break;
                }
                let Some(c) = candidates.get(i) else {
                    continue;
                };
                let kind = verb_kind(&c.verb);
                if out.contains(&i) || (one_of_each && kinds.contains(&kind)) {
                    continue;
                }
                kinds.push(kind);
                out.push(i);
                taken += 1;
            }
        }
        if out.len() >= most {
            break;
        }
    }
    for &i in &offered {
        if out.len() >= most {
            break;
        }
        if !out.contains(&i) {
            out.push(i);
        }
    }
    // In the ranking's order.
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::indexing_slicing)]
    use super::*;
    use serde_json::json;

    #[test]
    fn names_are_plain_words() {
        assert_eq!(snake("listFilesV2"), "list_files_v2");
        assert_eq!(snake("HTTPServerList"), "http_server_list");
        assert_eq!(snake("issues/list-for-repo"), "issues_list_for_repo");
        assert_eq!(snake("X-Request-ID"), "x_request_id");
        assert_eq!(snake("2fa"), "n_2fa");
        assert_eq!(singular("entries"), "entry");
        assert_eq!(singular("addresses"), "address");
        assert_eq!(singular("files"), "file");
        assert_eq!(singular("status"), "status");
        assert_eq!(
            cut("a_very_long_name_that_goes_on_and_on_and_on_for_ever"),
            "a_very_long_name_that_goes_on_and_on_and"
        );
        assert_eq!(words("list_file_comments"), "List file comments");
        assert_eq!(
            sentence("Lists **every** [file](https://x.example). More text.", 80),
            "Lists every file."
        );
        assert!(sentence(&"word ".repeat(40), 30).ends_with('…'));
        // An abbreviation does not end a sentence; HTML's tags are not words.
        assert_eq!(
            sentence("You can make a contact (ie. a user) here. More.", 80),
            "You can make a contact (ie. a user) here."
        );
        assert_eq!(
            sentence("Lists them.<br><br>*Rate limits*: 10/s", 80),
            "Lists them."
        );
        assert_eq!(sentence("a < b and c > d", 80), "a < b and c > d");
    }

    fn api_of(doc: &serde_json::Value) -> Api<'_> {
        super::super::spec::read(doc, None).unwrap()
    }

    #[test]
    fn lists_lookups_and_creations_of_the_main_resources_come_first() {
        let list = json!({ "200": { "content": { "application/json": { "schema": { "type": "array", "items": {} } } } } });
        let one = json!({ "200": { "content": { "application/json": { "schema": { "type": "object" } } } } });
        let doc = json!({ "openapi": "3.0.0", "servers": [{ "url": "https://api.x.example" }], "paths": {
            "/files": { "get": { "summary": "List files", "responses": list }, "post": { "summary": "Add a file", "responses": one } },
            "/files/{id}": { "get": { "summary": "A file", "responses": one }, "delete": { "responses": one },
                             "patch": { "summary": "Change a file", "responses": one } },
            "/files/{id}/copy": { "post": { "summary": "Copy a file", "responses": one } },
            "/files/{id}/comments": { "get": { "summary": "Its comments", "responses": list } },
            "/folders/{id}/comments": { "get": { "summary": "Its comments", "responses": list } },
            "/old": { "get": { "deprecated": true, "responses": list } },
            "/uploads": { "post": { "requestBody": { "content": { "multipart/form-data": {} } }, "responses": one } },
        } });
        let api = api_of(&doc);
        let ranked = rank(&api, &[]);
        let names: Vec<&str> = ranked.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names[0], "list_files");
        assert!(
            names.contains(&"get_file") && names.contains(&"create_file") && names.contains(&"copy_file"),
            "{names:?}"
        );
        assert!(
            names.contains(&"update_file") && names.contains(&"delete_file"),
            "{names:?}"
        );
        // Two lists of comments: the second is qualified by what it is under.
        assert!(
            names.contains(&"list_comments") && names.contains(&"list_folder_comments"),
            "{names:?}"
        );
        let old = ranked.iter().find(|c| c.name == "list_old").unwrap();
        assert_eq!(old.left_out.as_deref(), Some("deprecated"));
        let upload = ranked.iter().find(|c| c.name == "create_upload").unwrap();
        assert!(upload.left_out.as_deref().unwrap().contains("file upload"));
        assert_eq!(ranked[0].label, "List files");
        // At most N, the left-out never.
        let chosen = choose(&ranked, 3, None);
        assert_eq!(chosen.len(), 3);
        assert!(chosen.iter().all(|&i| ranked[i].left_out.is_none()));
        let picked = choose(
            &ranked,
            25,
            Some(&["copy_file".to_owned(), "list_old".to_owned()]),
        );
        assert_eq!(picked.len(), 1);
        assert_eq!(ranked[picked[0]].name, "copy_file");
        // Tags narrow what is offered.
        let tagged = rank(&api, &["Folders".to_owned()]);
        assert!(tagged.iter().all(|c| c.left_out.is_some()));
    }
}

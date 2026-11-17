// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the steps that reach outside a workflow's own data: a lookup file, a database, an e-mail, a script.

use openduct_expr::Compiled;
use serde_json::{Map, Value as Json};

use super::{Email, Kind, MAX_PARAMS, MAX_SCRIPT, MAX_SCRIPT_TIME, Reader, SCRIPT_TIME, duration, text};

/// A step's settings, as a record.
fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

impl Reader {
    pub(super) fn lookup(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        if let Some(k) = s.keys().find(|k| !matches!(k.as_str(), "file" | "where" | "all")) {
            return Err(format!("{at}.{k}: not a setting of lookup (file, where, all)"));
        }
        let file = text(s.get("file")).ok_or_else(|| format!("{at}.file: the file's path, as /rates.csv"))?;
        Ok(Kind::Lookup {
            file: file.to_owned(),
            matching: self.pairs(s.get("where"), &format!("{at}.where"))?,
            all: s.get("all").and_then(Json::as_bool).unwrap_or(false),
        })
    }

    pub(super) fn postgres(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        if let Some(k) = s
            .keys()
            .find(|k| !matches!(k.as_str(), "credential" | "sql" | "params"))
        {
            return Err(format!(
                "{at}.{k}: not a setting of postgres (credential, sql, params)"
            ));
        }
        let credential = text(s.get("credential"))
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| format!("{at}.credential: the database's credential"))?
            .to_owned();
        let sql = text(s.get("sql"))
            .filter(|q| !q.trim().is_empty())
            .ok_or_else(|| format!("{at}.sql: the SQL to run, with $1, $2 ... where its values go"))?
            .to_owned();
        if sql.contains("{{") {
            return Err(format!(
                "{at}.sql: values are not written into SQL: put $1, $2 ... where they go, and the values in \
                 params"
            ));
        }
        let params: Vec<Compiled> = match s.get("params") {
            None | Some(Json::Null) => Vec::new(),
            Some(Json::Array(items)) => items
                .iter()
                .enumerate()
                .map(|(i, v)| self.expression(v, &format!("{at}.params[{i}]")))
                .collect::<Result<_, _>>()?,
            Some(_) => {
                return Err(format!(
                    "{at}.params: a list of expressions, the first for $1, the next for $2 ..."
                ));
            }
        };
        if params.len() > MAX_PARAMS {
            return Err(format!("{at}.params: at most {MAX_PARAMS}"));
        }
        Ok(Kind::Sql {
            credential,
            sql,
            params,
        })
    }

    pub(super) fn email(&self, body: &Json, at: &str) -> Result<Kind, String> {
        const FIELDS: &[&str] = &[
            "credential",
            "to",
            "cc",
            "bcc",
            "reply_to",
            "subject",
            "text",
            "html",
        ];
        let s = settings(body, at)?;
        if let Some(k) = s.keys().find(|k| !FIELDS.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of email ({})",
                FIELDS.join(", ")
            ));
        }
        let credential = text(s.get("credential"))
            .filter(|c| !c.trim().is_empty())
            .ok_or_else(|| format!("{at}.credential: the mail server's credential"))?
            .to_owned();
        let template = |name: &str| {
            s.get(name)
                .map(|v| self.template(v, &format!("{at}.{name}")))
                .transpose()
        };
        let to = template("to")?.ok_or_else(|| format!("{at}.to: who it goes to"))?;
        let subject = template("subject")?.ok_or_else(|| format!("{at}.subject: its subject"))?;
        let (body, html) = (template("text")?, template("html")?);
        if body.is_none() && html.is_none() {
            return Err(format!("{at}: text, html or both"));
        }
        Ok(Kind::Email(Email {
            credential,
            to,
            cc: template("cc")?,
            bcc: template("bcc")?,
            reply_to: template("reply_to")?,
            subject,
            text: body,
            html,
        }))
    }

    pub(super) fn script(&self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        let (code, time) = match body {
            Json::String(code) => (code.as_str(), None),
            Json::Object(s) => {
                if let Some(k) = s.keys().find(|k| !matches!(k.as_str(), "code" | "timeout")) {
                    return Err(format!("{at}.{k}: not a setting of {kind} (code, timeout)"));
                }
                (text(s.get("code")).unwrap_or_default(), text(s.get("timeout")))
            }
            _ => ("", None),
        };
        if code.trim().is_empty() {
            return Err(format!("{at}: the code"));
        }
        if code.len() > MAX_SCRIPT {
            return Err(format!("{at}: at most {} KiB of code", MAX_SCRIPT / 1024));
        }
        let time = match time {
            None => SCRIPT_TIME,
            Some(t) => duration(t).ok_or_else(|| format!("{at}.timeout: as 10s or 1m"))?,
        };
        if time > MAX_SCRIPT_TIME || time.is_zero() {
            return Err(format!("{at}.timeout: up to 60s"));
        }
        Ok(Kind::Script {
            language: if kind == "javascript" {
                "javascript"
            } else {
                "python"
            },
            code: code.to_owned(),
            time,
        })
    }
}

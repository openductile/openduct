// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the steps that shape a run and its data: switch, merge, list tools, group, respond, run another
//! workflow, date and time.

use serde_json::{Map, Value as Json};

use super::{Aggregate, Case, DateTimeOps, GroupOps, Kind, ListOps, MergeMode, Reader, duration, text};

/// The most cases a switch has, and the most lists a merge takes.
const MAX_CASES: usize = 20;

fn settings<'a>(body: &'a Json, at: &str) -> Result<&'a Map<String, Json>, String> {
    body.as_object().ok_or_else(|| format!("{at}: a set of settings"))
}

fn only(s: &Map<String, Json>, at: &str, allowed: &[&str]) -> Result<(), String> {
    match s.keys().find(|k| !allowed.contains(&k.as_str())) {
        Some(k) => Err(format!(
            "{at}.{k}: not one of its settings ({})",
            allowed.join(", ")
        )),
        None => Ok(()),
    }
}

impl Reader {
    pub(super) fn switch(&mut self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["cases", "default"])?;
        let Some(Json::Array(list)) = s.get("cases") else {
            return Err(format!("{at}.cases: a list of cases, each a when and its steps"));
        };
        if list.is_empty() || list.len() > MAX_CASES {
            return Err(format!("{at}.cases: 1 to {MAX_CASES} cases"));
        }
        let mut cases = Vec::with_capacity(list.len());
        for (i, case) in list.iter().enumerate() {
            let place = format!("{at}.cases[{i}]");
            let c = settings(case, &place)?;
            only(c, &place, &["when", "steps", "label"])?;
            let when = self.expression(c.get("when").unwrap_or(&Json::Null), &format!("{place}.when"))?;
            cases.push(Case {
                label: text(c.get("label")).map_or_else(|| format!("case {}", i + 1), str::to_owned),
                when,
                steps: self.steps(c.get("steps"), &format!("{place}.steps"))?,
            });
        }
        Ok(Kind::Switch {
            cases,
            otherwise: self.steps(s.get("default"), &format!("{at}.default"))?,
        })
    }

    pub(super) fn merge(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["of", "mode", "key"])?;
        let Some(Json::Array(list)) = s.get("of") else {
            return Err(format!(
                "{at}.of: the lists (or records) to merge, each an expression"
            ));
        };
        if list.is_empty() || list.len() > MAX_CASES {
            return Err(format!("{at}.of: 1 to {MAX_CASES} of them"));
        }
        let of = list
            .iter()
            .enumerate()
            .map(|(i, v)| self.expression(v, &format!("{at}.of[{i}]")))
            .collect::<Result<_, _>>()?;
        let mode = match text(s.get("mode")).unwrap_or("append") {
            "append" => MergeMode::Append,
            "fields" => MergeMode::Fields,
            "key" => MergeMode::Key,
            other => return Err(format!("{at}.mode: append, fields or key, not {other:?}")),
        };
        let key = text(s.get("key")).map(str::to_owned);
        if mode == MergeMode::Key && key.as_deref().is_none_or(|k| k.trim().is_empty()) {
            return Err(format!("{at}.key: the field records are joined on, as id"));
        }
        Ok(Kind::Merge { of, mode, key })
    }

    pub(super) fn list_ops(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["items", "filter", "sort", "order", "unique", "limit"])?;
        let optional = |name: &str| {
            s.get(name)
                .map(|v| self.expression(v, &format!("{at}.{name}")))
                .transpose()
        };
        let descending = match text(s.get("order")).unwrap_or("asc") {
            "asc" => false,
            "desc" => true,
            other => return Err(format!("{at}.order: asc or desc, not {other:?}")),
        };
        let limit = match s.get("limit") {
            None | Some(Json::Null) => None,
            Some(v) => Some(
                v.as_u64()
                    .and_then(|n| usize::try_from(n).ok())
                    .filter(|n| *n > 0)
                    .ok_or_else(|| format!("{at}.limit: how many to keep, at least 1"))?,
            ),
        };
        Ok(Kind::List(ListOps {
            items: self.expression(s.get("items").unwrap_or(&Json::Null), &format!("{at}.items"))?,
            filter: optional("filter")?,
            sort: optional("sort")?,
            descending,
            unique: optional("unique")?,
            limit,
        }))
    }

    pub(super) fn group(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["items", "by", "sum", "average", "min", "max"])?;
        let mut fields = Vec::new();
        for (name, aggregate) in [
            ("sum", Aggregate::Sum),
            ("average", Aggregate::Average),
            ("min", Aggregate::Min),
            ("max", Aggregate::Max),
        ] {
            match s.get(name) {
                None | Some(Json::Null) => {}
                Some(Json::Object(m)) => {
                    for (field, expr) in m {
                        if field == "key" || field == "count" {
                            return Err(format!("{at}.{name}.{field}: key and count are each group's own"));
                        }
                        fields.push((
                            field.clone(),
                            aggregate,
                            self.expression(expr, &format!("{at}.{name}.{field}"))?,
                        ));
                    }
                }
                Some(_) => {
                    return Err(format!(
                        "{at}.{name}: fields and their expressions, as total: item.total"
                    ));
                }
            }
        }
        Ok(Kind::Group(GroupOps {
            items: self.expression(s.get("items").unwrap_or(&Json::Null), &format!("{at}.items"))?,
            by: self.expression(s.get("by").unwrap_or(&Json::Null), &format!("{at}.by"))?,
            fields,
        }))
    }

    pub(super) fn respond(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["status", "headers", "body"])?;
        let status = match s.get("status") {
            None => 200,
            Some(v) => v
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .filter(|n| (100..600).contains(n))
                .ok_or_else(|| format!("{at}.status: an HTTP status, 100 to 599"))?,
        };
        Ok(Kind::Respond {
            status,
            headers: self.pairs(s.get("headers"), &format!("{at}.headers"))?,
            body: self.tree(s.get("body").unwrap_or(&Json::Null), &format!("{at}.body"))?,
        })
    }

    pub(super) fn run_workflow(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["workflow", "input"])?;
        let workflow = text(s.get("workflow"))
            .filter(|w| !w.trim().is_empty())
            .ok_or_else(|| format!("{at}.workflow: the workflow to run, by its id"))?
            .to_owned();
        Ok(Kind::RunWorkflow {
            workflow,
            input: s
                .get("input")
                .map(|v| self.expression(v, &format!("{at}.input")))
                .transpose()?,
        })
    }

    pub(super) fn datetime(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["value", "add", "subtract", "timezone", "format"])?;
        let by = |name: &str| -> Result<Option<std::time::Duration>, String> {
            text(s.get(name))
                .map(|t| duration(t).ok_or_else(|| format!("{at}.{name}: as 30m, 2h or 7d")))
                .transpose()
        };
        let timezone = text(s.get("timezone")).map(str::to_owned);
        if let Some(zone) = &timezone
            && zone.parse::<chrono_tz::Tz>().is_err()
        {
            return Err(format!(
                "{at}.timezone: {zone:?} is not a time zone, as Europe/London"
            ));
        }
        Ok(Kind::DateTime(DateTimeOps {
            value: s
                .get("value")
                .map(|v| self.expression(v, &format!("{at}.value")))
                .transpose()?,
            add: by("add")?,
            subtract: by("subtract")?,
            timezone,
            format: text(s.get("format")).map(str::to_owned),
        }))
    }
}

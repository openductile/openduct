// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the steps that keep data between runs: `table` (rows of a workspace's data table: get, count, insert,
//! update, upsert, delete) and `state` (the workflow's own values: get, set, delete, clear).
//!
//! ```yaml
//! - name: known
//!   table: { name: leads, action: get, where: { email: "{{ value.email }}" }, limit: 1 }
//! - table: { name: leads, action: upsert, key: email, row: { email: "{{ value.email }}", score: "{{ value.score }}" } }
//! - table: { name: leads, action: update, where: "row.score > 50", set: { hot: true } }
//! - state: { set: { seen: "{{ has(state.seen) ? state.seen + 1 : 1 }}" } }
//! ```

use openduct_expr::Compiled;
use serde_json::{Map, Value as Json};

use super::{Kind, Reader, Tree, text};
use crate::tables::{DEFAULT_GET, MAX_GET, key_problem, name_problem};
use crate::template::Template;

/// The most comparisons one condition holds.
const MAX_TESTS: usize = 20;
/// The most columns a sort, a key or a row names.
const MAX_NAMED: usize = 50;

/// A step on a data table: which table, and what it does there.
#[derive(Debug, Clone)]
pub struct TableStep {
    pub table: String,
    pub action: TableAction,
}

#[derive(Debug, Clone)]
pub enum TableAction {
    /// The rows that match, sorted (column, descending), from `offset`, at most `limit`.
    Get {
        filter: Filter,
        sort: Vec<(String, bool)>,
        limit: usize,
        offset: usize,
    },
    /// How many rows match.
    Count { filter: Filter },
    /// New rows: one, its values by column, or each record of a list.
    Insert(NewRows),
    /// The rows that match, each with these columns set (worked out for each row, which is `row` to them).
    Update {
        filter: Filter,
        set: Vec<(String, Tree)>,
    },
    /// The rows whose key columns hold the row's values, set to it; the row added when none does.
    Upsert {
        key: Vec<String>,
        row: Vec<(String, Tree)>,
    },
    /// The rows that match, taken away.
    Delete { filter: Filter },
}

#[derive(Debug, Clone)]
pub enum NewRows {
    One(Vec<(String, Tree)>),
    Many(Compiled),
}

/// Which rows a step means.
#[derive(Debug, Clone)]
pub enum Filter {
    All,
    /// An expression, true for the rows meant: `row.score > 50 && row.owner == value.owner`.
    Expression(Compiled),
    /// Columns compared with values: every comparison holds, or (`any`) one does.
    Tests {
        any: bool,
        tests: Vec<Test>,
    },
}

/// A column compared with a value (text with `{{ }}`, read as the column's type).
#[derive(Debug, Clone)]
pub struct Test {
    pub column: String,
    pub op: Op,
    pub value: Option<Template>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    /// Text that holds the value, whatever the letters' case; so do the next two.
    Contains,
    StartsWith,
    EndsWith,
    Empty,
    NotEmpty,
}

impl Op {
    fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "=" | "==" | "equal" => Self::Equal,
            "!=" | "not_equal" => Self::NotEqual,
            "<" | "less" => Self::Less,
            "<=" | "less_or_equal" => Self::LessOrEqual,
            ">" | "greater" => Self::Greater,
            ">=" | "greater_or_equal" => Self::GreaterOrEqual,
            "contains" => Self::Contains,
            "starts_with" => Self::StartsWith,
            "ends_with" => Self::EndsWith,
            "empty" => Self::Empty,
            "not_empty" => Self::NotEmpty,
            _ => return None,
        })
    }
}

/// What a `state` step does with the workflow's own values.
#[derive(Debug, Clone)]
pub enum StateAction {
    /// Every value, as a record; one value; or some, as a record.
    Get(StateKeys),
    /// Values set, each worked out with the state as it is (`state`).
    Set(Vec<(String, Tree)>),
    Delete(Vec<String>),
    Clear,
}

#[derive(Debug, Clone)]
pub enum StateKeys {
    All,
    One(String),
    Some(Vec<String>),
}

const ACTIONS: &str = "get, count, insert, update, upsert or delete";

fn allowed(s: &Map<String, Json>, action: &str, more: &[&str], at: &str) -> Result<(), String> {
    match s
        .keys()
        .find(|k| !matches!(k.as_str(), "name" | "action") && !more.contains(&k.as_str()))
    {
        Some(k) => Err(format!(
            "{at}.{k}: not a setting of a table's {action} (name, action{}{})",
            if more.is_empty() { "" } else { ", " },
            more.join(", ")
        )),
        None => Ok(()),
    }
}

fn column_name(name: &str, at: &str) -> Result<String, String> {
    match name_problem(name) {
        Some(why) => Err(format!("{at}: a column, {why}")),
        None => Ok(name.to_owned()),
    }
}

fn names(v: &Json, at: &str) -> Result<Vec<String>, String> {
    let list: Vec<&str> = match v {
        Json::String(s) => vec![s.as_str()],
        Json::Array(items) => items
            .iter()
            .map(|i| i.as_str().ok_or_else(|| format!("{at}: names, as text")))
            .collect::<Result<_, _>>()?,
        _ => return Err(format!("{at}: a name, or a list of names")),
    };
    if list.is_empty() || list.len() > MAX_NAMED {
        return Err(format!("{at}: 1 to {MAX_NAMED} names"));
    }
    Ok(list.into_iter().map(str::to_owned).collect())
}

fn count(v: Option<&Json>, at: &str, default: usize, most: usize) -> Result<usize, String> {
    match v {
        None | Some(Json::Null) => Ok(default),
        Some(n) => n
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n <= most)
            .ok_or_else(|| format!("{at}: a whole number, at most {most}")),
    }
}

impl Reader {
    pub(super) fn table(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = body
            .as_object()
            .ok_or_else(|| format!("{at}: a set of settings: name, action ..."))?;
        let table = text(s.get("name")).ok_or_else(|| format!("{at}.name: the table's name"))?;
        if let Some(why) = name_problem(table) {
            return Err(format!("{at}.name: {why}"));
        }
        let action = text(s.get("action")).ok_or_else(|| format!("{at}.action: {ACTIONS}"))?;
        let filter = |required: bool| self.filter(s, required, at);
        let action = match action {
            "get" => {
                allowed(s, action, &["where", "match", "sort", "limit", "offset"], at)?;
                let sort = match s.get("sort") {
                    None | Some(Json::Null) => Vec::new(),
                    Some(v) => names(v, &format!("{at}.sort"))?
                        .into_iter()
                        .map(|n| {
                            let (name, descending) = match n.strip_prefix('-') {
                                Some(rest) => (rest.to_owned(), true),
                                None => (n, false),
                            };
                            Ok((column_name(&name, &format!("{at}.sort"))?, descending))
                        })
                        .collect::<Result<_, String>>()?,
                };
                TableAction::Get {
                    filter: filter(false)?,
                    sort,
                    limit: count(s.get("limit"), &format!("{at}.limit"), DEFAULT_GET, MAX_GET)?.max(1),
                    offset: count(
                        s.get("offset"),
                        &format!("{at}.offset"),
                        0,
                        crate::tables::MAX_ROWS,
                    )?,
                }
            }
            "count" => {
                allowed(s, action, &["where", "match"], at)?;
                TableAction::Count {
                    filter: filter(false)?,
                }
            }
            "insert" => {
                allowed(s, action, &["row", "rows"], at)?;
                match (s.get("row"), s.get("rows")) {
                    (Some(row), None) => {
                        TableAction::Insert(NewRows::One(self.values(row, &format!("{at}.row"))?))
                    }
                    (None, Some(rows)) => {
                        TableAction::Insert(NewRows::Many(self.expression(rows, &format!("{at}.rows"))?))
                    }
                    _ => {
                        return Err(format!(
                            "{at}: row (its values by column) or rows (an expression giving a list of records)"
                        ));
                    }
                }
            }
            "update" => {
                allowed(s, action, &["where", "match", "set"], at)?;
                let set = s
                    .get("set")
                    .ok_or_else(|| format!("{at}.set: the columns to set, and their values"))?;
                TableAction::Update {
                    filter: filter(true)?,
                    set: self.values(set, &format!("{at}.set"))?,
                }
            }
            "upsert" => {
                allowed(s, action, &["key", "row"], at)?;
                let key = names(
                    s.get("key")
                        .ok_or_else(|| format!("{at}.key: the column (or columns) a row is found by"))?,
                    &format!("{at}.key"),
                )?;
                for k in &key {
                    column_name(k, &format!("{at}.key"))?;
                }
                let row = self.values(
                    s.get("row")
                        .ok_or_else(|| format!("{at}.row: the row's values by column"))?,
                    &format!("{at}.row"),
                )?;
                if let Some(missing) = key.iter().find(|k| !row.iter().any(|(c, _)| c == *k)) {
                    return Err(format!("{at}.row: the key column {missing} needs a value"));
                }
                TableAction::Upsert { key, row }
            }
            "delete" => {
                allowed(s, action, &["where", "match"], at)?;
                TableAction::Delete {
                    filter: filter(true)?,
                }
            }
            other => return Err(format!("{at}.action: {ACTIONS}, not {other:?}")),
        };
        Ok(Kind::Table(TableStep {
            table: table.to_owned(),
            action,
        }))
    }

    /// A row's values by column: each JSON with `{{ }}` in its texts.
    fn values(&self, v: &Json, at: &str) -> Result<Vec<(String, Tree)>, String> {
        let fields = v
            .as_object()
            .ok_or_else(|| format!("{at}: values by column, as {{email: \"{{{{ value.email }}}}\"}}"))?;
        if fields.is_empty() || fields.len() > MAX_NAMED {
            return Err(format!("{at}: 1 to {MAX_NAMED} columns"));
        }
        fields
            .iter()
            .map(|(k, v)| {
                let at = format!("{at}.{k}");
                Ok((column_name(k, &at)?, self.tree(v, &at)?))
            })
            .collect()
    }

    /// `where`: an expression (text), columns equal to values (a record), or comparisons (a list); with `match`.
    fn filter(&self, s: &Map<String, Json>, required: bool, at: &str) -> Result<Filter, String> {
        let any = match text(s.get("match")) {
            None | Some("all") => false,
            Some("any") => true,
            Some(other) => return Err(format!("{at}.match: all or any, not {other:?}")),
        };
        let at = format!("{at}.where");
        let tests = match s.get("where") {
            None | Some(Json::Null) if required => {
                return Err(format!("{at}: which rows (to mean every row, write \"true\")"));
            }
            None | Some(Json::Null) => return Ok(Filter::All),
            Some(Json::String(_) | Json::Bool(_)) => {
                return Ok(Filter::Expression(
                    self.expression(s.get("where").unwrap_or(&Json::Null), &at)?,
                ));
            }
            Some(Json::Object(fields)) => fields
                .iter()
                .map(|(column, value)| {
                    let here = format!("{at}.{column}");
                    Ok(Test {
                        column: column_name(column, &here)?,
                        op: Op::Equal,
                        value: Some(self.template(value, &here)?),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            Some(Json::Array(items)) => items
                .iter()
                .enumerate()
                .map(|(i, item)| self.test(item, &format!("{at}[{i}]")))
                .collect::<Result<Vec<_>, String>>()?,
            Some(_) => return Err(format!("{at}: an expression, columns and values, or comparisons")),
        };
        if tests.is_empty() || tests.len() > MAX_TESTS {
            return Err(format!("{at}: 1 to {MAX_TESTS} comparisons"));
        }
        Ok(Filter::Tests { any, tests })
    }

    fn test(&self, item: &Json, at: &str) -> Result<Test, String> {
        let t = item
            .as_object()
            .ok_or_else(|| format!("{at}: {{column: status, op: \"=\", value: new}}"))?;
        if let Some(k) = t
            .keys()
            .find(|k| !matches!(k.as_str(), "column" | "op" | "value"))
        {
            return Err(format!(
                "{at}.{k}: not a setting of a comparison (column, op, value)"
            ));
        }
        let column = column_name(
            text(t.get("column")).ok_or_else(|| format!("{at}.column: the column compared"))?,
            &format!("{at}.column"),
        )?;
        let op = match text(t.get("op")) {
            None => Op::Equal,
            Some(o) => Op::parse(o).ok_or_else(|| {
                format!(
                    "{at}.op: =, !=, <, <=, >, >=, contains, starts_with, ends_with, empty or not_empty, not {o:?}"
                )
            })?,
        };
        let value = match (op, t.get("value")) {
            (Op::Empty | Op::NotEmpty, None) => None,
            (Op::Empty | Op::NotEmpty, Some(_)) => {
                return Err(format!("{at}.value: empty and not_empty compare with nothing"));
            }
            (_, Some(v)) => Some(self.template(v, &format!("{at}.value"))?),
            (_, None) => return Err(format!("{at}.value: what the column is compared with")),
        };
        Ok(Test { column, op, value })
    }

    pub(super) fn state(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let empty = Map::new();
        let s = match body {
            Json::Null => &empty,
            Json::Object(s) => s,
            _ => return Err(format!("{at}: get, set, delete or clear")),
        };
        let keys = |v: &Json, at: &str| -> Result<Vec<String>, String> {
            let list = names(v, at)?;
            match list.iter().find_map(|k| key_problem(k)) {
                Some(why) => Err(format!("{at}: {why}")),
                None => Ok(list),
            }
        };
        let action = match s.iter().collect::<Vec<_>>()[..] {
            [] => StateAction::Get(StateKeys::All),
            [(k, v)] => match k.as_str() {
                "get" => match v {
                    Json::Null => StateAction::Get(StateKeys::All),
                    Json::String(_) => {
                        StateAction::Get(StateKeys::One(keys(v, &format!("{at}.get"))?.remove(0)))
                    }
                    _ => StateAction::Get(StateKeys::Some(keys(v, &format!("{at}.get"))?)),
                },
                "set" => {
                    let fields = v
                        .as_object()
                        .filter(|f| !f.is_empty() && f.len() <= MAX_NAMED)
                        .ok_or_else(|| format!("{at}.set: 1 to {MAX_NAMED} names and their values"))?;
                    StateAction::Set(
                        fields
                            .iter()
                            .map(|(k, v)| {
                                let here = format!("{at}.set.{k}");
                                if let Some(why) = key_problem(k) {
                                    return Err(format!("{here}: {why}"));
                                }
                                Ok((k.clone(), self.tree(v, &here)?))
                            })
                            .collect::<Result<_, String>>()?,
                    )
                }
                "delete" => StateAction::Delete(keys(v, &format!("{at}.delete"))?),
                "clear" if v.as_bool() == Some(true) => StateAction::Clear,
                "clear" => return Err(format!("{at}.clear: true, to forget every value")),
                other => return Err(format!("{at}.{other}: get, set, delete or clear")),
            },
            _ => return Err(format!("{at}: one of get, set, delete or clear")),
        };
        Ok(Kind::State(action))
    }
}

#[cfg(test)]
mod tests {
    use crate::{Kind, Workflow};

    fn step(yaml: &str) -> Result<Kind, String> {
        Workflow::parse(&format!("name: t\nsteps:\n  - {yaml}\n")).map(|w| w.steps[0].kind.clone())
    }

    #[test]
    fn a_table_step_reads_each_action_and_its_settings() {
        let Kind::Table(t) = step(r#"table: { name: leads, action: get, where: "row.score > 3", sort: ["-score", name], limit: 5 }"#).unwrap() else {
            panic!()
        };
        let super::TableAction::Get {
            sort, limit, offset, ..
        } = t.action
        else {
            panic!()
        };
        assert_eq!(sort, vec![("score".to_owned(), true), ("name".to_owned(), false)]);
        assert_eq!((limit, offset), (5, 0));
        assert!(matches!(
            step(r#"table: { name: leads, action: count, where: { status: new } }"#).unwrap(),
            Kind::Table(_)
        ));
        assert!(step(r#"table: { name: leads, action: insert, rows: "value.leads" }"#).is_ok());
        assert!(
            step(
                r#"table: { name: leads, action: upsert, key: email, row: { email: "{{ value.email }}" } }"#
            )
            .is_ok()
        );
        assert!(
            step(r#"table: { name: leads, action: update, where: "true", set: { n: "{{ row.n + 1 }}" } }"#)
                .is_ok()
        );
        assert!(step(
            r#"table: { name: leads, action: delete, match: any, where: [{ column: score, op: "<", value: "1" }, { column: email, op: empty }] }"#
        )
        .is_ok());
    }

    #[test]
    fn a_table_step_s_mistakes_say_where_they_are() {
        let wrong = [
            (r#"table: { action: get }"#, "name: the table's name"),
            (r#"table: { name: "my leads", action: get }"#, "letters, digits"),
            (r#"table: { name: leads, action: find }"#, "get, count, insert"),
            (
                r#"table: { name: leads, action: get, row: {} }"#,
                "not a setting of a table's get",
            ),
            (r#"table: { name: leads, action: delete }"#, "write \"true\""),
            (
                r#"table: { name: leads, action: update, where: "true" }"#,
                "set: the columns",
            ),
            (r#"table: { name: leads, action: insert }"#, "row (its values"),
            (
                r#"table: { name: leads, action: upsert, key: email, row: { name: x } }"#,
                "key column email",
            ),
            (
                r#"table: { name: leads, action: get, limit: 100000 }"#,
                "at most 10000",
            ),
            (
                r#"table: { name: leads, action: get, where: [{ column: a, op: like, value: x }] }"#,
                "contains",
            ),
            (
                r#"table: { name: leads, action: get, where: [{ column: a, op: empty, value: x }] }"#,
                "compare with nothing",
            ),
            (
                r#"table: { name: leads, action: get, where: "row.score >" }"#,
                "where",
            ),
            (r#"table: { name: leads, action: get, sort: "-" }"#, "sort"),
        ];
        for (yaml, says) in wrong {
            let e = step(yaml).unwrap_err();
            assert!(e.contains(says), "{yaml}: {e}");
        }
    }

    #[test]
    fn a_state_step_gets_sets_deletes_and_clears() {
        assert!(matches!(
            step("state: {}").unwrap(),
            Kind::State(super::StateAction::Get(super::StateKeys::All))
        ));
        assert!(matches!(
            step("state: { get: last_id }").unwrap(),
            Kind::State(super::StateAction::Get(super::StateKeys::One(_)))
        ));
        assert!(matches!(
            step("state: { get: [a, b] }").unwrap(),
            Kind::State(super::StateAction::Get(super::StateKeys::Some(_)))
        ));
        assert!(step(r#"state: { set: { seen: "{{ has(state.seen) ? state.seen + 1 : 1 }}" } }"#).is_ok());
        assert!(step("state: { delete: [a] }").is_ok());
        assert!(step("state: { clear: true }").is_ok());
        assert!(step("state: { set: {} }").unwrap_err().contains("1 to 50"));
        assert!(
            step("state: { get: a, set: { b: 1 } }")
                .unwrap_err()
                .contains("one of")
        );
        assert!(
            step(r#"state: { set: { "a b": 1 } }"#)
                .unwrap_err()
                .contains("letters")
        );
        assert!(step("state: { clear: false }").is_err());
    }
}

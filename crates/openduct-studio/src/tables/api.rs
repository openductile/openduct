// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The Tables page's API: a workspace's tables listed, made (empty, or from CSV), their names and columns changed,
//! deleted; their rows shown (sorted, filtered, a page at a time), added, edited, deleted, imported and exported as
//! CSV; and the workflows' state shown and cleared. A viewer looks; an editor changes.

use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Response, StatusCode};
use openduct_expr::{Record, Value};
use openduct_flow::tables::{
    Changes, Column, ColumnType, MAX_CHANGED, MAX_COLUMNS, MAX_ROW_BYTES, MAX_ROWS, MAX_STATE_BYTES,
    MAX_TABLES, MAX_WORKSPACE_BYTES, Row, TableData, check_changes, coerce, columns_problem, name_problem,
    now_ms, row_values,
};
use serde_json::{Value as Json, json};

use super::csv;
use super::store::{ColumnDef, TableDef, Write, def_write, row_writes, state_cleared};
use crate::State;
use crate::api::{Access, access};
use crate::http::{Answer, Call, json, problem};
use crate::model::{Role, SCHEMA, new_id, now};

/// The most rows a page of the Tables page shows.
const MAX_PAGE: usize = 500;
/// The longest the page's filter takes over a table's rows.
const FILTER_TIME: std::time::Duration = std::time::Duration::from_secs(3);

pub(crate) async fn route(state: &State, call: &Call) -> Option<Answer> {
    let segments: Vec<&str> = call.path.trim_matches('/').split('/').collect();
    Some(match (&call.method, segments.as_slice()) {
        (&Method::GET, ["api", "tables"]) => list(state, call).await,
        (&Method::POST, ["api", "tables"]) => create(state, call).await,
        (&Method::GET, ["api", "tables", name]) => one(state, call, name).await,
        (&Method::PUT, ["api", "tables", name]) => alter(state, call, name).await,
        (&Method::DELETE, ["api", "tables", name]) => remove(state, call, name).await,
        (&Method::GET, ["api", "tables", name, "rows"]) => rows(state, call, name).await,
        (&Method::POST, ["api", "tables", name, "rows"]) => add(state, call, name).await,
        (&Method::POST, ["api", "tables", name, "rows", "delete"]) => delete(state, call, name, None).await,
        (&Method::PUT, ["api", "tables", name, "rows", id]) => edit(state, call, name, id).await,
        (&Method::DELETE, ["api", "tables", name, "rows", id]) => delete(state, call, name, Some(id)).await,
        (&Method::POST, ["api", "tables", name, "import"]) => import(state, call, name).await,
        (&Method::GET, ["api", "tables", name, "export"]) => export(state, call, name).await,
        (&Method::GET, ["api", "workflow-state"]) => states(state, call).await,
        (&Method::DELETE, ["api", "workflow-state", workflow]) => clear(state, call, workflow).await,
        _ => return None,
    })
}

fn unavailable(e: &str) -> Answer {
    problem(StatusCode::SERVICE_UNAVAILABLE, e)
}

/// A refusal as its words say: no such table (404), or a change that cannot be made (422).
fn refused(e: &str) -> Answer {
    if e.starts_with("there is no table") {
        problem(StatusCode::NOT_FOUND, e)
    } else if e.starts_with("cannot ") {
        unavailable(e)
    } else if e.contains("is busy") {
        problem(StatusCode::CONFLICT, e)
    } else {
        problem(StatusCode::UNPROCESSABLE_ENTITY, e)
    }
}

fn shown_def(def: &TableDef, rows: usize, bytes: usize) -> Json {
    json!({
        "id": def.id, "name": def.name, "columns": def.columns, "rows": rows, "bytes": bytes,
        "created": def.created, "updated": def.updated,
    })
}

fn limits() -> Json {
    json!({
        "tables": MAX_TABLES, "columns": MAX_COLUMNS, "rows": MAX_ROWS, "row_bytes": MAX_ROW_BYTES,
        "workspace_bytes": MAX_WORKSPACE_BYTES, "state_bytes": MAX_STATE_BYTES, "rows_at_once": MAX_CHANGED,
        "types": ColumnType::ALL.iter().map(|t| t.name()).collect::<Vec<_>>(),
    })
}

async fn list(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    match state.tables.list(&access.workspace.id).await {
        Ok(found) => {
            let tables: Vec<Json> = found
                .iter()
                .map(|(d, rows, bytes)| shown_def(d, *rows, *bytes))
                .collect();
            json(StatusCode::OK, &json!({ "tables": tables, "limits": limits() }))
        }
        Err(e) => unavailable(&e),
    }
}

async fn one(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    match state.tables.list(&access.workspace.id).await {
        Ok(found) => match found.iter().find(|(d, _, _)| d.name == name) {
            Some((d, rows, bytes)) => json(StatusCode::OK, &shown_def(d, *rows, *bytes)),
            None => problem(StatusCode::NOT_FOUND, "no such table"),
        },
        Err(e) => unavailable(&e),
    }
}

/// Columns as a request gives them: `[{name, type, from?}]` - `from` the column a renamed one was.
fn columns_given(body: &Json) -> Result<Vec<(Column, Option<String>)>, String> {
    let items = body
        .get("columns")
        .and_then(Json::as_array)
        .ok_or("columns: a list of {name, type}")?;
    let mut out = Vec::new();
    for (i, item) in items.iter().enumerate().take(MAX_COLUMNS + 1) {
        let name = item.get("name").and_then(Json::as_str).unwrap_or_default().trim();
        let kind = item.get("type").and_then(Json::as_str).unwrap_or("text");
        let kind = ColumnType::parse(kind).ok_or_else(|| {
            format!(
                "column {}: the type text, number, boolean, datetime or json, not {kind:?}",
                i + 1
            )
        })?;
        let from = item.get("from").and_then(Json::as_str).map(str::to_owned);
        out.push((
            Column {
                name: name.to_owned(),
                kind,
            },
            from,
        ));
    }
    let plain: Vec<Column> = out.iter().map(|(c, _)| c.clone()).collect();
    match columns_problem(&plain) {
        Some(why) => Err(why),
        None => Ok(out),
    }
}

fn defs_of(columns: &[Column]) -> Vec<ColumnDef> {
    columns
        .iter()
        .map(|c| ColumnDef {
            name: c.name.clone(),
            kind: c.kind.name().to_owned(),
        })
        .collect()
}

fn workspace_turn(access: &Access) -> String {
    format!("tables/{}", access.workspace.id)
}

/// A new table: `{name, columns}`, or `{name, csv}` - its columns (and their types) from the CSV's header and
/// cells, and its rows.
async fn create(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let name = call
        .body
        .get("name")
        .and_then(Json::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if let Some(why) = name_problem(&name) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, &format!("name: {why}"));
    }
    let csv_text = call.body.get("csv").and_then(Json::as_str);
    let read = match csv_text.map(csv::rows).transpose() {
        Ok(r) => r,
        Err(e) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!("the CSV cannot be read: {e}"),
            );
        }
    };
    let columns: Vec<Column> = match (call.body.get("columns"), &read) {
        (Some(_), _) => match columns_given(&call.body) {
            Ok(c) => c.into_iter().map(|(c, _)| c).collect(),
            Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
        },
        (None, Some((headers, rows))) => csv::columns_of(headers, rows),
        (None, None) => Vec::new(),
    };
    if let Some(why) = columns_problem(&columns) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, &why);
    }
    let def = TableDef {
        schema: SCHEMA,
        id: new_id(),
        workspace: access.workspace.id.clone(),
        name: name.clone(),
        columns: defs_of(&columns),
        created: now(),
        updated: now(),
        by: access.user.id.clone(),
    };
    let data = TableData {
        name: name.clone(),
        columns,
        next_id: 1,
        ..TableData::default()
    };
    let rows = match &read {
        Some((headers, rows)) => match csv::values_of(&data, headers, rows) {
            Ok(v) => v,
            Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
        },
        None => Vec::new(),
    };
    let changes = match new_rows(&data, rows) {
        Ok(c) => c,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let workspace = access.workspace.id.clone();
    let made = state
        .tables
        .in_turns(&[workspace_turn(&access)], |view| {
            if view.table(&workspace, &name).is_some() {
                return Err(format!("there is a table called {name} already"));
            }
            if view.count(&workspace) >= MAX_TABLES {
                return Err(format!("a workspace has at most {MAX_TABLES} tables"));
            }
            let mut writes = vec![def_write(&def, true)];
            writes.extend(row_writes(&def.id, &data, &changes));
            Ok((writes, changes.put.len()))
        })
        .await;
    match made {
        Ok(rows) => json(StatusCode::CREATED, &shown_def(&def, rows, 0)),
        Err(e) => refused(&e),
    }
}

/// New rows of `values`, checked against the table.
fn new_rows(data: &TableData, values: Vec<Record>) -> Result<Changes, String> {
    let now = now_ms();
    let mut changes = Changes {
        next_id: data.next_id.max(1),
        ..Changes::default()
    };
    for (i, given) in values.iter().enumerate() {
        let values = row_values(data, given).map_err(|e| format!("row {}: {e}", i + 1))?;
        changes.put.push(Row {
            id: changes.next_id,
            values: Arc::new(values),
            created: now,
            updated: now,
        });
        changes.next_id += 1;
    }
    check_changes(data, &changes)?;
    Ok(changes)
}

/// A table's new name and columns: `{name?, columns: [{name, type, from?}]}`. A column renamed names the one it was
/// (`from`); one left out is deleted with its values; one whose type changes has each value read as the new type -
/// refused, saying which row, when one cannot be.
async fn alter(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let columns = match columns_given(&call.body) {
        Ok(c) => c,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let new_name = call
        .body
        .get("name")
        .and_then(Json::as_str)
        .map(|n| n.trim().to_owned());
    if let Some(why) = new_name.as_deref().and_then(name_problem) {
        return problem(StatusCode::UNPROCESSABLE_ENTITY, &format!("name: {why}"));
    }
    let workspace = access.workspace.id.clone();
    let Ok(def) = state.tables.def(&workspace, name).await else {
        return problem(StatusCode::NOT_FOUND, "no such table");
    };
    let turns = [workspace_turn(&access), format!("table/{}", def.id)];
    let by = access.user.id.clone();
    let changed = state
        .tables
        .in_turns(&turns, |view| {
            let (mut def, data) = view
                .table(&workspace, name)
                .ok_or("the table was deleted meanwhile")?;
            if let Some(n) = new_name.as_deref().filter(|n| *n != name) {
                if view.table(&workspace, n).is_some() {
                    return Err(format!("there is a table called {n} already"));
                }
                def.name = n.to_owned();
            }
            let mut put = Vec::new();
            for row in data.rows.values() {
                let mut values = Record::new();
                for (column, from) in &columns {
                    let old = from.as_deref().unwrap_or(&column.name);
                    if from.is_some() && !data.columns.iter().any(|c| c.name == old) {
                        return Err(format!(
                            "{column:?}: there is no column {old} to rename",
                            column = column.name
                        ));
                    }
                    let value = row.values.get(old).cloned().unwrap_or(Value::Null);
                    let value = coerce(column.kind, &value).map_err(|e| {
                        format!(
                            "row {}: {}: {e}: change the value first, or keep the column's type",
                            row.id, column.name
                        )
                    })?;
                    values.insert(column.name.clone(), value);
                }
                if Value::record(values.clone()).to_json() != Value::Record(Arc::clone(&row.values)).to_json()
                {
                    put.push(Row {
                        values: Arc::new(values),
                        ..row.clone()
                    });
                }
            }
            def.columns = defs_of(&columns.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>());
            def.updated = now();
            def.by = by;
            let mut writes = vec![def_write(&def, true)];
            let changes = Changes {
                put,
                next_id: data.next_id,
                ..Changes::default()
            };
            writes.extend(row_writes(&def.id, &data, &changes));
            Ok((writes, def))
        })
        .await;
    match changed {
        Ok(def) => json(StatusCode::OK, &shown_def(&def, 0, 0)),
        Err(e) => refused(&e),
    }
}

async fn remove(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let workspace = access.workspace.id.clone();
    let Ok(def) = state.tables.def(&workspace, name).await else {
        return problem(StatusCode::NOT_FOUND, "no such table");
    };
    let turns = [workspace_turn(&access), format!("table/{}", def.id)];
    let removed = state
        .tables
        .in_turns(&turns, |view| {
            let (def, data) = view
                .table(&workspace, name)
                .ok_or("the table was deleted meanwhile")?;
            let changes = Changes {
                delete: data.rows.keys().copied().collect(),
                ..Changes::default()
            };
            let mut writes: Vec<Write> = row_writes(&def.id, &data, &changes);
            writes.push((format!("n/{}", def.id), None));
            writes.push(def_write(&def, false));
            Ok((writes, data.rows.len()))
        })
        .await;
    match removed {
        Ok(rows) => json(StatusCode::OK, &json!({ "deleted": name, "rows": rows })),
        Err(e) => refused(&e),
    }
}

fn query(call: &Call) -> serde_json::Map<String, Json> {
    crate::hooks::form(&call.query)
}

/// A page of a table's rows: `?offset=&limit=&sort=<column>&desc=1&where=<expression over row>`.
async fn rows(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let data = match state.tables.table(&access.workspace.id, name).await {
        Ok(d) => d,
        Err(e) => return refused(&e),
    };
    let q = query(call);
    let number = |k: &str, default: usize| {
        q.get(k)
            .and_then(Json::as_str)
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(default)
    };
    let (offset, limit) = (number("offset", 0), number("limit", 50).clamp(1, MAX_PAGE));
    let filter = match q
        .get("where")
        .and_then(Json::as_str)
        .filter(|w| !w.trim().is_empty())
    {
        None => None,
        Some(w) => match openduct_expr::compile(openduct_expr::DEFAULT, w) {
            Ok(e) => Some(e),
            Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &format!("where: {e}")),
        },
    };
    let mut found: Vec<Value> = Vec::new();
    // However costly the expression, the page answers within a few seconds.
    let deadline = std::time::Instant::now() + FILTER_TIME;
    for row in data.shown() {
        if filter.is_some() && std::time::Instant::now() > deadline {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!(
                    "where: the expression took longer than {}s over the rows",
                    FILTER_TIME.as_secs()
                ),
            );
        }
        let keep = match &filter {
            None => true,
            Some(e) => {
                let names = [("row".to_owned(), row.clone())];
                let env = openduct_expr::Env {
                    value: &Value::Null,
                    message: &Value::Null,
                    names: &names,
                };
                matches!(e.evaluate(&env), Ok(Value::Bool(true)))
            }
        };
        if keep {
            found.push(row);
        }
    }
    if let Some(column) = q.get("sort").and_then(Json::as_str).filter(|c| !c.is_empty()) {
        let desc = q
            .get("desc")
            .and_then(Json::as_str)
            .is_some_and(|d| d == "1" || d == "true");
        let cell = |v: &Value| match v {
            Value::Record(r) => r.get(column).cloned().unwrap_or(Value::Null),
            _ => Value::Null,
        };
        found.sort_by(|a, b| {
            let (x, y) = (cell(a), cell(b));
            let o = match (&x, &y) {
                (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
                (Value::Null, _) => std::cmp::Ordering::Less,
                (_, Value::Null) => std::cmp::Ordering::Greater,
                (Value::Number(p), Value::Number(q)) => p.cmp(q),
                (Value::Time(p), Value::Time(q)) => p.cmp(q),
                _ => x.to_text().cmp(&y.to_text()),
            };
            if desc { o.reverse() } else { o }
        });
    }
    let page: Vec<Json> = found
        .iter()
        .skip(offset)
        .take(limit)
        .map(|r| serde_json::from_str(&r.to_json()).unwrap_or(Json::Null))
        .collect();
    json(
        StatusCode::OK,
        &json!({ "rows": page, "matched": found.len(), "total": data.rows.len(), "offset": offset }),
    )
}

/// Records of a request as a table's values.
fn records(items: Option<&Json>) -> Result<Vec<Record>, String> {
    let items = items.and_then(Json::as_array).ok_or("rows: a list of records")?;
    if items.len() > MAX_CHANGED {
        return Err(format!("at most {MAX_CHANGED} rows at once"));
    }
    items
        .iter()
        .map(|i| match Value::from_json(i) {
            Value::Record(r) => Ok(r.as_ref().clone()),
            _ => Err("rows: a list of records".to_owned()),
        })
        .collect()
}

/// Rows added: `{rows: [{column: value}]}`.
async fn add(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let given = match records(call.body.get("rows")) {
        Ok(r) => r,
        Err(e) => return problem(StatusCode::UNPROCESSABLE_ENTITY, &e),
    };
    let plan: openduct_flow::tables::Plan<'_> = Box::new(move |data: &TableData| {
        let changes = new_rows(data, given)?;
        let n = changes.put.len();
        Ok((changes, json_value(&json!({ "added": n }))))
    });
    changed(state.tables.change_table(&access.workspace.id, name, plan).await)
}

fn json_value(j: &Json) -> Value {
    Value::from_json(j)
}

fn changed(outcome: Result<Value, String>) -> Answer {
    match outcome {
        Ok(v) => json(
            StatusCode::OK,
            &serde_json::from_str(&v.to_json()).unwrap_or(Json::Null),
        ),
        Err(e) => refused(&e),
    }
}

/// One row's values changed: `{values: {column: value}}`.
async fn edit(state: &State, call: &Call, name: &str, id: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Ok(id) = id.parse::<u64>() else {
        return problem(StatusCode::NOT_FOUND, "no such row");
    };
    let values = match Value::from_json(call.body.get("values").unwrap_or(&Json::Null)) {
        Value::Record(r) => r,
        _ => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                "values: the columns to change, and their values",
            );
        }
    };
    let plan: openduct_flow::tables::Plan<'_> = Box::new(move |data: &TableData| {
        let row = data
            .rows
            .get(&id)
            .ok_or("there is no such row: it was deleted meanwhile")?;
        let mut merged = row.values.as_ref().clone();
        merged.extend(row_values(data, &values)?);
        let edited = Row {
            values: Arc::new(merged),
            updated: now_ms(),
            ..row.clone()
        };
        let shown = edited.shown(&data.columns);
        Ok((
            Changes {
                put: vec![edited],
                next_id: data.next_id,
                ..Changes::default()
            },
            shown,
        ))
    });
    changed(state.tables.change_table(&access.workspace.id, name, plan).await)
}

/// A row deleted, or several: `{ids: [..]}`.
async fn delete(state: &State, call: &Call, name: &str, one: Option<&str>) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let ids: Vec<u64> = match one {
        Some(id) => id.parse().ok().into_iter().collect(),
        None => call
            .body
            .get("ids")
            .and_then(Json::as_array)
            .map(|ids| {
                ids.iter()
                    .filter_map(Json::as_u64)
                    .take(MAX_CHANGED + 1)
                    .collect()
            })
            .unwrap_or_default(),
    };
    let plan: openduct_flow::tables::Plan<'_> = Box::new(move |data: &TableData| {
        let delete: Vec<u64> = ids.into_iter().filter(|id| data.rows.contains_key(id)).collect();
        let n = delete.len();
        Ok((
            Changes {
                delete,
                next_id: data.next_id,
                ..Changes::default()
            },
            json_value(&json!({ "deleted": n })),
        ))
    });
    changed(state.tables.change_table(&access.workspace.id, name, plan).await)
}

/// CSV rows added to a table: `{csv}`, its header naming the table's columns.
async fn import(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let Some(text) = call.body.get("csv").and_then(Json::as_str) else {
        return problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "csv: the CSV's text, its first row naming the columns",
        );
    };
    let (headers, rows) = match csv::rows(text) {
        Ok(r) => r,
        Err(e) => {
            return problem(
                StatusCode::UNPROCESSABLE_ENTITY,
                &format!("the CSV cannot be read: {e}"),
            );
        }
    };
    let plan: openduct_flow::tables::Plan<'_> = Box::new(move |data: &TableData| {
        let values = csv::values_of(data, &headers, &rows)?;
        let changes = new_rows(data, values)?;
        let n = changes.put.len();
        Ok((changes, json_value(&json!({ "added": n }))))
    });
    changed(state.tables.change_table(&access.workspace.id, name, plan).await)
}

async fn export(state: &State, call: &Call, name: &str) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let data = match state.tables.table(&access.workspace.id, name).await {
        Ok(d) => d,
        Err(e) => return refused(&e),
    };
    let text = csv::export(&data);
    // The page asks for it as JSON (its requests name their workspace in a header a download link cannot send).
    if query(call).get("as").and_then(Json::as_str) == Some("json") {
        return json(StatusCode::OK, &json!({ "name": data.name, "csv": text }));
    }
    let mut response = Response::new(Full::new(Bytes::from(text)));
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/csv; charset=utf-8"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{}.csv\"", data.name)) {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    response
}

/// The workflows that keep state: each one's values, size and when they last changed.
async fn states(state: &State, call: &Call) -> Answer {
    let access = match access(state, call, Role::Viewer).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let kept = match state.tables.states(&access.workspace.id).await {
        Ok(k) => k,
        Err(e) => return unavailable(&e),
    };
    let workflows = state.model.workflows.all().await;
    let out: Vec<Json> = kept
        .into_iter()
        .map(|(workflow, values, updated)| {
            let value = Value::record(values.as_ref().clone());
            let name = workflows
                .iter()
                .find(|(id, w)| *id == workflow && w.workspace == access.workspace.id)
                .map(|(_, w)| w.name.clone());
            json!({
                "workflow": workflow, "name": name, "bytes": value.size(), "updated": updated,
                "values": serde_json::from_str::<Json>(&value.to_json()).unwrap_or(Json::Null),
            })
        })
        .collect();
    json(StatusCode::OK, &json!({ "states": out }))
}

async fn clear(state: &State, call: &Call, workflow: &str) -> Answer {
    let access = match access(state, call, Role::Editor).await {
        Ok(a) => a,
        Err(answer) => return answer,
    };
    let workspace = access.workspace.id.clone();
    let turn = format!("state/{workspace}/{workflow}");
    let cleared = state
        .tables
        .in_turns(&[turn], |_| Ok((vec![state_cleared(&workspace, workflow)], ())))
        .await;
    match cleared {
        Ok(()) => json(StatusCode::OK, &json!({ "cleared": workflow })),
        Err(e) => refused(&e),
    }
}

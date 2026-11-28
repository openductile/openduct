// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading the steps that read and make files (`extract`, `convert`, `file_details`), and the file settings of the
//! steps that send them: an HTTP step's `file`, `multipart` and `response`, an e-mail's `attachments`.

use openduct_expr::Compiled;
use serde_json::{Map, Value as Json};

use super::{Body, Kind, Reader, text};
use crate::files::{Convert, Details, Extract, Format, Part, Response};

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

/// A CSV delimiter: one character, or `tab`.
fn delimiter(s: &Map<String, Json>, at: &str) -> Result<u8, String> {
    match text(s.get("delimiter")) {
        None => Ok(b','),
        Some("tab" | "\t") => Ok(b'\t'),
        Some(d) if d.len() == 1 && d != "\"" && d.is_ascii() && !d.contains(['\r', '\n']) => {
            Ok(d.bytes().next().unwrap_or(b','))
        }
        Some(other) => Err(format!(
            "{at}.delimiter: one character, as , or ; or tab - not {other:?}"
        )),
    }
}

fn flag(s: &Map<String, Json>, name: &str, at: &str) -> Result<bool, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(true),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

fn media_type(s: &Map<String, Json>, at: &str) -> Result<Option<String>, String> {
    let Some(given) = text(s.get("type")) else {
        return Ok(None);
    };
    let cleaned = crate::files::clean_type(given);
    if cleaned == "application/octet-stream" && !given.eq_ignore_ascii_case("application/octet-stream") {
        return Err(format!("{at}.type: a media type, as text/csv or application/pdf"));
    }
    Ok(Some(cleaned))
}

impl Reader {
    pub(super) fn file_step(&self, kind: &str, body: &Json, at: &str) -> Result<Kind, String> {
        match kind {
            "extract_file" => self.extract(body, at),
            "convert" => self.convert(body, at),
            _ => self.details(body, at),
        }
    }

    fn extract(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(s, at, &["file", "format", "sheet", "header", "delimiter"])?;
        let file = self.expression(
            s.get("file")
                .ok_or_else(|| format!("{at}.file: the file to read, as steps.download.body"))?,
            &format!("{at}.file"),
        )?;
        let format = match text(s.get("format")) {
            None | Some("auto") => None,
            Some(f) => Some(Format::parse(f).ok_or_else(|| {
                format!("{at}.format: auto, csv, json, xlsx, xls, text, html, pdf or base64 - not {f:?}")
            })?),
        };
        Ok(Kind::Extract(Extract {
            file,
            format,
            sheet: s
                .get("sheet")
                .map(|v| self.template(v, &format!("{at}.sheet")))
                .transpose()?,
            header: flag(s, "header", at)?,
            delimiter: delimiter(s, at)?,
        }))
    }

    fn convert(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = settings(body, at)?;
        only(
            s,
            at,
            &[
                "format",
                "rows",
                "data",
                "text",
                "base64",
                "name",
                "type",
                "columns",
                "sheet",
                "delimiter",
                "header",
            ],
        )?;
        let format = text(s.get("format"))
            .ok_or_else(|| format!("{at}.format: csv, json, xlsx, text, html or base64"))?;
        let format = Format::parse(format)
            .filter(|f| !matches!(f, Format::Xls | Format::Pdf))
            .ok_or_else(|| format!("{at}.format: csv, json, xlsx, text, html or base64 - not {format:?}"))?;
        let wanted: &[&str] = match format {
            Format::Csv | Format::Xlsx => &["rows"],
            Format::Json => &["data", "rows"],
            Format::Text | Format::Html => &["text"],
            _ => &["base64"],
        };
        let given: Vec<&str> = ["rows", "data", "text", "base64"]
            .into_iter()
            .filter(|k| s.contains_key(*k))
            .collect();
        let [one] = given[..] else {
            return Err(format!("{at}: what the file is made of: {}", wanted.join(" or ")));
        };
        if !wanted.contains(&one) {
            return Err(format!(
                "{at}.{one}: a {} file is made of {}",
                format.name(),
                wanted.join(" or ")
            ));
        }
        let source = s.get(one).unwrap_or(&Json::Null);
        let (from, body_text) = if one == "text" {
            (None, Some(self.template(source, &format!("{at}.text"))?))
        } else {
            (Some(self.expression(source, &format!("{at}.{one}"))?), None)
        };
        let columns = match s.get("columns") {
            None | Some(Json::Null) => Vec::new(),
            Some(Json::Array(list)) => list
                .iter()
                .map(|c| {
                    c.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| format!("{at}.columns: a list of column names"))
                })
                .collect::<Result<_, _>>()?,
            Some(_) => return Err(format!("{at}.columns: a list of column names")),
        };
        Ok(Kind::Convert(Convert {
            format,
            from,
            text: body_text,
            name: s
                .get("name")
                .map(|v| self.template(v, &format!("{at}.name")))
                .transpose()?,
            media_type: media_type(s, at)?,
            columns,
            sheet: text(s.get("sheet")).unwrap_or("Sheet1").to_owned(),
            delimiter: delimiter(s, at)?,
            header: flag(s, "header", at)?,
        }))
    }

    fn details(&self, body: &Json, at: &str) -> Result<Kind, String> {
        if let Json::String(_) = body {
            return Ok(Kind::FileDetails(Details {
                file: self.expression(body, at)?,
                name: None,
                media_type: None,
            }));
        }
        let s = settings(body, at)?;
        only(s, at, &["file", "name", "type"])?;
        Ok(Kind::FileDetails(Details {
            file: self.expression(
                s.get("file")
                    .ok_or_else(|| format!("{at}.file: the file, as steps.download.body"))?,
                &format!("{at}.file"),
            )?,
            name: s
                .get("name")
                .map(|v| self.template(v, &format!("{at}.name")))
                .transpose()?,
            media_type: media_type(s, at)?,
        }))
    }

    /// An HTTP step's file settings: a body made of a file or a multipart form, and what its answer is made.
    pub(super) fn http_files(
        &self,
        s: &Map<String, Json>,
        at: &str,
    ) -> Result<(Option<Body>, Response), String> {
        let response = match text(s.get("response")) {
            None | Some("auto") => Response::Auto,
            Some("file") => Response::File,
            Some("text") => Response::Text,
            Some(other) => return Err(format!("{at}.response: auto, file or text - not {other:?}")),
        };
        if let Some(file) = s.get("file") {
            return Ok((
                Some(Body::File(self.expression(file, &format!("{at}.file"))?)),
                response,
            ));
        }
        let Some(form) = s.get("multipart") else {
            return Ok((None, response));
        };
        let fields = form
            .as_object()
            .ok_or_else(|| format!("{at}.multipart: its fields, each text or {{file: steps.report}}"))?;
        if fields.len() > crate::files::multipart::MAX_PARTS {
            return Err(format!(
                "{at}.multipart: at most {} fields",
                crate::files::multipart::MAX_PARTS
            ));
        }
        let mut parts = Vec::new();
        for (name, value) in fields {
            let place = format!("{at}.multipart.{name}");
            let part = match value {
                Json::Object(f) => {
                    only(f, &place, &["file", "name", "type"])?;
                    Part::File {
                        file: self.expression(
                            f.get("file")
                                .ok_or_else(|| format!("{place}.file: the file, as steps.report"))?,
                            &format!("{place}.file"),
                        )?,
                        name: f
                            .get("name")
                            .map(|v| self.template(v, &format!("{place}.name")))
                            .transpose()?,
                        media_type: media_type(f, &place)?,
                    }
                }
                other => Part::Text(self.template(other, &place)?),
            };
            parts.push((name.clone(), part));
        }
        Ok((Some(Body::Multipart(parts)), response))
    }

    /// An e-mail's attachments: an expression giving a file or a list of files, or a list of such expressions.
    pub(super) fn attachments(&self, given: Option<&Json>, at: &str) -> Result<Vec<Compiled>, String> {
        match given {
            None | Some(Json::Null) => Ok(Vec::new()),
            Some(Json::Array(list)) => list
                .iter()
                .enumerate()
                .map(|(i, v)| self.expression(v, &format!("{at}.attachments[{i}]")))
                .collect(),
            Some(one) => Ok(vec![self.expression(one, &format!("{at}.attachments"))?]),
        }
    }
}

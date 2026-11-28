// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The pages of forms: a form, its thank-you page, and the page that says why there is no form. Plain HTML made on
//! the server - no script and nothing from elsewhere - every text written into it escaped, so nothing a workflow or
//! a person typed can become markup. The browser checks the fields as the page marks them (required, lengths,
//! numbers, e-mail addresses, dates); the server checks them again, the same way.

use std::fmt::Write as _;

use bytes::Bytes;
use http_body_util::Full;
use hyper::header::{self, HeaderValue};
use hyper::{Response, StatusCode};
use openduct_flow::forms::{Field, FieldKind, Form};

use crate::http::Answer;

/// Text made safe to write into HTML, between tags or inside a quoted attribute.
pub(crate) fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c => out.push(c),
        }
    }
    out
}

/// Text as paragraphs: a blank line between two, a line break within one.
fn paragraphs(text: &str, class: &str) -> String {
    text.split("\n\n")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| format!("<p class=\"{class}\">{}</p>", escape(p).replace('\n', "<br>")))
        .collect()
}

const STYLE: &str = "
:root{--bg:#f6f7f9;--card:#fff;--text:#16181d;--muted:#5d6472;--line:#d8dce3;--accent:#2f5bea;--bad:#c0322b;--good:#1f7a4d}
@media (prefers-color-scheme:dark){:root{--bg:#121418;--card:#1b1e24;--text:#e8eaee;--muted:#a0a7b4;--line:#333945;--accent:#7d9bff;--bad:#ff7b72;--good:#56d08f}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font:16px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif}
main{max-width:640px;margin:40px auto;padding:28px 24px;background:var(--card);border:1px solid var(--line);border-radius:12px}
@media (max-width:680px){main{margin:16px;padding:20px 16px}}
h1{font-size:1.5rem;line-height:1.25;margin:0 0 8px}
.lead{color:var(--muted);margin:0 0 12px}
.field{margin:20px 0 0}
label,legend{display:block;font-weight:600;margin:0 0 6px}
fieldset{border:0;margin:0;padding:0}
.req{color:var(--bad)}
.help{color:var(--muted);font-size:.9rem;margin:4px 0 0}
input[type=text],input[type=email],input[type=number],input[type=date],input[type=password],select,textarea{width:100%;padding:10px 12px;font:inherit;color:inherit;background:var(--bg);border:1px solid var(--line);border-radius:8px}
textarea{min-height:120px;resize:vertical}
input:focus,select:focus,textarea:focus{outline:2px solid var(--accent);outline-offset:1px}
.check{display:flex;gap:10px;align-items:flex-start;font-weight:400;margin:6px 0}
.check input{margin-top:4px}
[aria-invalid=true]{border-color:var(--bad)}
.error{color:var(--bad);font-size:.9rem;margin:4px 0 0}
.problems{border:1px solid var(--bad);border-radius:8px;padding:10px 14px;margin:16px 0 0;color:var(--bad)}
.actions{display:flex;gap:12px;flex-wrap:wrap;margin:28px 0 0}
button{font:inherit;font-weight:600;padding:10px 20px;border-radius:8px;border:1px solid var(--accent);background:var(--accent);color:#fff;cursor:pointer}
button.reject{background:transparent;color:var(--bad);border-color:var(--bad)}
button.approve{background:var(--good);border-color:var(--good)}
footer{text-align:center;color:var(--muted);font-size:.8rem;margin:0 0 32px}
";

/// A whole page around `body`.
fn page(title: &str, body: &str, head_extra: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width, initial-scale=1\"><meta name=\"robots\" content=\"noindex\">{head_extra}\
         <title>{}</title><style>{STYLE}</style></head><body><main>{body}</main><footer>A form made with \
         OpenDuct</footer></body></html>",
        escape(title)
    )
}

/// An HTML answer.
pub(crate) fn html(status: StatusCode, body: String) -> Answer {
    let mut response = Response::new(Full::new(Bytes::from(body)));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("x-robots-tag", HeaderValue::from_static("noindex"));
    response
}

/// What a page is for, beside its form: where it posts, its token, and what was sent before (with its problems).
#[derive(Debug, Default)]
pub(crate) struct Filled<'a> {
    /// The form's token (`_form`): this page's, so an answer comes from it.
    pub token: &'a str,
    /// Answers sent before, by field, shown again.
    pub values: &'a [(String, String)],
    /// Problems, by field (`""`: the whole form's).
    pub problems: &'a [(String, String)],
    /// Whether the page asks for the form's password.
    pub password: bool,
}

/// A form's page.
pub(crate) fn form_page(form: &Form, filled: &Filled<'_>) -> String {
    let mut body = format!("<h1>{}</h1>", escape(&form.title));
    body.push_str(&paragraphs(&form.description, "lead"));
    let general: Vec<&str> = filled
        .problems
        .iter()
        .filter(|(f, _)| f.is_empty() || f == "_password")
        .map(|(_, why)| why.as_str())
        .collect();
    if !filled.problems.is_empty() {
        let _ = write!(
            body,
            "<div class=\"problems\" role=\"alert\">{}</div>",
            if general.is_empty() {
                "Please look at the fields marked below.".to_owned()
            } else {
                general.iter().map(|w| escape(w)).collect::<Vec<_>>().join("<br>")
            }
        );
    }
    let files = form.fields.iter().any(|f| f.kind == FieldKind::File);
    let _ = write!(
        body,
        "<form method=\"post\" enctype=\"{}\" accept-charset=\"utf-8\">\
         <input type=\"hidden\" name=\"_form\" value=\"{}\">",
        if files {
            "multipart/form-data"
        } else {
            "application/x-www-form-urlencoded"
        },
        escape(filled.token)
    );
    for field in &form.fields {
        body.push_str(&field_html(field, filled));
    }
    if let Some(approval) = &form.approval
        && approval.comment
    {
        let comment = value_of(filled, "_comment").unwrap_or_default();
        let _ = write!(
            body,
            "<div class=\"field\"><label for=\"f-_comment\">Comment <span class=\"help\">(optional)</span></label>\
             <textarea id=\"f-_comment\" name=\"_comment\" maxlength=\"5000\">{}</textarea></div>",
            escape(comment)
        );
    }
    if filled.password {
        let _ = write!(
            body,
            "<div class=\"field\"><label for=\"f-_password\">Password <span class=\"req\">*</span></label>\
             <input type=\"password\" id=\"f-_password\" name=\"_password\" required maxlength=\"200\" \
             autocomplete=\"current-password\"></div>"
        );
    }
    body.push_str("<div class=\"actions\">");
    match &form.approval {
        Some(approval) => {
            let _ = write!(
                body,
                "<button type=\"submit\" name=\"_decision\" value=\"approve\" class=\"approve\">{}</button>\
                 <button type=\"submit\" name=\"_decision\" value=\"reject\" class=\"reject\">{}</button>",
                escape(&approval.approve),
                escape(&approval.reject)
            );
        }
        None => {
            let _ = write!(body, "<button type=\"submit\">{}</button>", escape(&form.button));
        }
    }
    body.push_str("</div></form>");
    page(&form.title, &body, "")
}

fn value_of<'a>(filled: &'a Filled<'_>, name: &str) -> Option<&'a str> {
    filled
        .values
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
}

fn field_html(field: &Field, filled: &Filled<'_>) -> String {
    let name = escape(&field.name);
    let id = format!("f-{name}");
    let problem = filled
        .problems
        .iter()
        .find(|(f, _)| *f == field.name)
        .map(|(_, w)| w.as_str());
    let given = value_of(filled, &field.name);
    let shown = given.unwrap_or(if filled.values.is_empty() {
        &field.default
    } else {
        ""
    });
    let required = if field.required { " required" } else { "" };
    let star = if field.required {
        " <span class=\"req\">*</span>"
    } else {
        ""
    };
    let mut described = Vec::new();
    if !field.help.is_empty() {
        described.push(format!("h-{name}"));
    }
    if problem.is_some() {
        described.push(format!("e-{name}"));
    }
    let aria = format!(
        "{}{}",
        if problem.is_some() {
            " aria-invalid=\"true\""
        } else {
            ""
        },
        if described.is_empty() {
            String::new()
        } else {
            format!(" aria-describedby=\"{}\"", described.join(" "))
        }
    );
    let placeholder = if field.placeholder.is_empty() {
        String::new()
    } else {
        format!(" placeholder=\"{}\"", escape(&field.placeholder))
    };
    let bounds = |f: &Field| {
        let mut out = String::new();
        if let Some(min) = &f.min {
            let _ = write!(out, " min=\"{}\"", escape(min));
        }
        if let Some(max) = &f.max {
            let _ = write!(out, " max=\"{}\"", escape(max));
        }
        out
    };
    let label = format!("<label for=\"{id}\">{}{star}</label>", escape(&field.label));
    let control = match field.kind {
        FieldKind::Text => format!(
            "{label}<input type=\"text\" id=\"{id}\" name=\"{name}\" maxlength=\"{}\" value=\"{}\"{placeholder}{required}{aria}>",
            field.longest(),
            escape(shown)
        ),
        FieldKind::LongText => format!(
            "{label}<textarea id=\"{id}\" name=\"{name}\" maxlength=\"{}\"{placeholder}{required}{aria}>{}</textarea>",
            field.longest(),
            escape(shown)
        ),
        FieldKind::Number => format!(
            "{label}<input type=\"number\" step=\"any\" inputmode=\"decimal\" id=\"{id}\" name=\"{name}\" value=\"{}\"{}{placeholder}{required}{aria}>",
            escape(shown),
            bounds(field)
        ),
        FieldKind::Email => format!(
            "{label}<input type=\"email\" id=\"{id}\" name=\"{name}\" maxlength=\"254\" autocomplete=\"email\" value=\"{}\"{placeholder}{required}{aria}>",
            escape(shown)
        ),
        FieldKind::Date => format!(
            "{label}<input type=\"date\" id=\"{id}\" name=\"{name}\" value=\"{}\"{}{required}{aria}>",
            escape(shown),
            bounds(field)
        ),
        FieldKind::Dropdown => {
            let mut options = String::from("<option value=\"\">Choose</option>");
            for o in &field.options {
                let _ = write!(
                    options,
                    "<option value=\"{0}\"{1}>{0}</option>",
                    escape(o),
                    if o == shown { " selected" } else { "" }
                );
            }
            format!("{label}<select id=\"{id}\" name=\"{name}\"{required}{aria}>{options}</select>")
        }
        FieldKind::Checkbox if field.options.is_empty() => {
            let checked = if given.is_some() || (filled.values.is_empty() && !field.default.is_empty()) {
                " checked"
            } else {
                ""
            };
            format!(
                "<label class=\"check\" for=\"{id}\"><input type=\"checkbox\" id=\"{id}\" name=\"{name}\" value=\"on\"{checked}{required}{aria}><span>{}{star}</span></label>",
                escape(&field.label)
            )
        }
        FieldKind::Checkbox => {
            let chosen: Vec<&str> = filled
                .values
                .iter()
                .filter(|(n, _)| *n == field.name)
                .map(|(_, v)| v.as_str())
                .collect();
            let mut boxes = String::new();
            for (i, o) in field.options.iter().enumerate() {
                let _ = write!(
                    boxes,
                    "<label class=\"check\" for=\"{id}-{i}\"><input type=\"checkbox\" id=\"{id}-{i}\" name=\"{name}\" value=\"{0}\"{1}><span>{0}</span></label>",
                    escape(o),
                    if chosen.contains(&o.as_str()) {
                        " checked"
                    } else {
                        ""
                    }
                );
            }
            format!(
                "<fieldset{aria}><legend>{}{star}</legend>{boxes}</fieldset>",
                escape(&field.label)
            )
        }
        FieldKind::File => {
            let accept = if field.accept.is_empty() {
                String::new()
            } else {
                format!(" accept=\"{}\"", escape(&field.accept))
            };
            format!(
                "{label}<input type=\"file\" id=\"{id}\" name=\"{name}\"{accept}{required}{aria}>\
                 <p class=\"help\">One file, at most {}.</p>",
                size_text(field.largest())
            )
        }
    };
    let mut out = format!("<div class=\"field\">{control}");
    if !field.help.is_empty() {
        let _ = write!(
            out,
            "<p class=\"help\" id=\"h-{name}\">{}</p>",
            escape(&field.help)
        );
    }
    if let Some(why) = problem {
        let _ = write!(out, "<p class=\"error\" id=\"e-{name}\">{}</p>", escape(why));
    }
    out.push_str("</div>");
    out
}

fn size_text(bytes: usize) -> String {
    if bytes >= 1024 * 1024 && bytes.is_multiple_of(1024 * 1024) {
        format!("{} MB", bytes / (1024 * 1024))
    } else {
        format!("{} KB", bytes.div_ceil(1024))
    }
}

/// The page once an answer is in: its thanks, or on to where the form sends people.
pub(crate) fn thanks_page(form: &Form) -> String {
    if !form.redirect.is_empty() {
        let to = escape(&form.redirect);
        let head = format!("<meta http-equiv=\"refresh\" content=\"0;url={to}\">");
        return page(
            &form.title,
            &format!(
                "<h1>{}</h1><p class=\"lead\"><a href=\"{to}\">Go on</a></p>",
                escape(&form.title)
            ),
            &head,
        );
    }
    page(
        &form.title,
        &format!(
            "<h1>{}</h1>{}",
            escape(&form.title),
            paragraphs(&form.thanks, "lead")
        ),
        "",
    )
}

/// A page that says something instead of a form: closed, answered already, not there.
pub(crate) fn message_page(heading: &str, text: &str) -> String {
    page(
        heading,
        &format!("<h1>{}</h1>{}", escape(heading), paragraphs(text, "lead")),
        "",
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn form() -> Form {
        Form::from_json(&serde_json::json!({
            "title": "<script>alert(1)</script>",
            "description": "First \"part\".\n\nSecond & last.",
            "fields": [
                { "name": "who", "label": "Name <b>", "type": "text", "required": true, "help": "'x'" },
                { "name": "size", "label": "Size", "type": "dropdown", "options": ["S", "M\"><img src=x>"] },
                { "name": "cv", "label": "CV", "type": "file", "accept": ".pdf" },
            ],
            "button": "Send",
            "thanks": "Thanks",
        }))
        .unwrap()
    }

    #[test]
    fn nothing_typed_becomes_markup() {
        let values = vec![("who".to_owned(), "\"><script>x</script>".to_owned())];
        let problems = vec![("who".to_owned(), "<bad>".to_owned())];
        let page = form_page(
            &form(),
            &Filled {
                token: "t\"x",
                values: &values,
                problems: &problems,
                password: false,
            },
        );
        assert!(!page.contains("<script>"), "{page}");
        assert!(!page.contains("<img"), "{page}");
        assert!(!page.contains("<b>"), "{page}");
        assert!(page.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(page.contains("value=\"t&quot;x\""));
        assert!(
            page.contains("enctype=\"multipart/form-data\""),
            "a file field posts multipart"
        );
        assert!(page.contains("aria-invalid=\"true\""));
        assert!(page.contains(" required"));
        assert!(page.contains(
            "<p class=\"lead\">First &quot;part&quot;.</p><p class=\"lead\">Second &amp; last.</p>"
        ));
    }

    #[test]
    fn a_thank_you_page_goes_on_where_the_form_sends_people() {
        let mut f = form();
        assert!(thanks_page(&f).contains("<p class=\"lead\">Thanks</p>"));
        f.redirect = "https://example.com/a?b=1&c=\"2\"".to_owned();
        let page = thanks_page(&f);
        assert!(
            page.contains("content=\"0;url=https://example.com/a?b=1&amp;c=&quot;2&quot;\""),
            "{page}"
        );
    }
}

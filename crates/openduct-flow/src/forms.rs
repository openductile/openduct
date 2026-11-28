// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Forms: the fields a person fills in - on the page a form trigger serves, or on one a run asks mid-way (a form step,
//! an approval) - read and checked as a workflow is, and an answer checked against them on the server as the browser
//! checks it on the page. A form step's texts may hold `{{ }}`, worked out from the run when it asks; a form that
//! starts a run has nothing to work them out from, so it has none.

use std::time::Duration;

use openduct_expr::{Env, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json};

use crate::template::Template;

mod check;

pub use check::{Given, Problems, check, sample};

/// The most fields a form has.
pub const MAX_FIELDS: usize = 50;
/// The most choices a dropdown or a set of boxes offers.
pub const MAX_OPTIONS: usize = 100;
/// The longest label, help text, placeholder, choice, title or button, in characters.
const MAX_LABEL: usize = 300;
/// The longest description or thank-you text, in characters.
const MAX_DESCRIPTION: usize = 5_000;
/// The longest answer a text field takes, and a long-text one, unless it says less.
const TEXT_LENGTH: usize = 1_000;
const LONG_TEXT_LENGTH: usize = 20_000;
/// The most a field may say it takes.
const MAX_TEXT_LENGTH: usize = 100_000;
/// The largest file a file field takes unless it says, and the most it may say.
const FILE_SIZE: usize = 1024 * 1024;
const MAX_FILE_SIZE: usize = 2 * 1024 * 1024;
/// All the files of one answer together, at most: an answer must fit in a step's result once written as text.
pub const MAX_FILES: usize = 2 * 1024 * 1024;
/// Answers a form trigger takes from one address in a minute, unless it says.
const RATE_LIMIT: u32 = 10;
/// Field names an answer keeps for itself.
const RESERVED: &[&str] = &["timed_out", "answered_at"];
/// Field names an approval keeps for itself, beside those.
const APPROVAL_RESERVED: &[&str] = &["approved", "decision", "comment"];

/// What a field takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldKind {
    Text,
    LongText,
    Number,
    Email,
    Date,
    Dropdown,
    Checkbox,
    File,
}

impl FieldKind {
    fn read(name: &str) -> Option<Self> {
        Some(match name {
            "text" => Self::Text,
            "long_text" | "textarea" => Self::LongText,
            "number" => Self::Number,
            "email" => Self::Email,
            "date" => Self::Date,
            "dropdown" | "select" => Self::Dropdown,
            "checkbox" => Self::Checkbox,
            "file" => Self::File,
            _ => return None,
        })
    }
}

/// One field, as a page shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    /// What the answer is kept under: `steps.<form>.<name>`, or `value.<name>` in a run a form started.
    pub name: String,
    pub label: String,
    #[serde(rename = "type")]
    pub kind: FieldKind,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub help: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub placeholder: String,
    /// A dropdown's choices; a checkbox's when it offers several (none: one box, yes or no).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    /// A number's (or a date's, as 2026-01-31) least and greatest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<String>,
    /// The longest text it takes, in characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,
    /// The largest file it takes, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_size: Option<usize>,
    /// The kinds of file it takes, as a page's `accept`: `.pdf,image/*`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub accept: String,
    /// What it holds to start with.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub default: String,
}

impl Field {
    /// The longest text it takes.
    pub fn longest(&self) -> usize {
        self.max_length.unwrap_or(match self.kind {
            FieldKind::LongText => LONG_TEXT_LENGTH,
            _ => TEXT_LENGTH,
        })
    }

    /// The largest file it takes.
    pub fn largest(&self) -> usize {
        self.max_size.unwrap_or(FILE_SIZE)
    }
}

/// An approval's two answers, and whether it asks for a comment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Approval {
    pub approve: String,
    pub reject: String,
    pub comment: bool,
}

/// A form, as its page shows it: its texts worked out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Form {
    pub title: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(default)]
    pub fields: Vec<Field>,
    pub button: String,
    /// What the page says once the answer is in.
    pub thanks: String,
    /// Where the page goes once the answer is in, instead (`https://` only).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub redirect: String,
    /// A form trigger's password, asked on its page: none when empty. Never shown.
    #[serde(default, skip_serializing)]
    pub password: String,
    /// Answers a form trigger takes from one address in a minute: 0 takes them all.
    #[serde(default)]
    pub rate_limit: u32,
    /// An approval: approve or reject, and a comment if it asks for one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<Approval>,
}

impl Form {
    /// As JSON, for the record of a run that waits (its password left out).
    pub fn to_json(&self) -> Json {
        serde_json::to_value(self).unwrap_or(Json::Null)
    }

    /// Read back from [`Form::to_json`].
    pub fn from_json(json: &Json) -> Option<Self> {
        serde_json::from_value(json.clone()).ok()
    }
}

/// Where a form is: starting a run (its texts plain), a run's form step, or an approval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Use {
    Trigger,
    Step,
    Approval,
}

/// A field's texts that may hold `{{ }}`.
#[derive(Debug, Clone)]
struct FieldSpec {
    field: Field,
    label: Template,
    help: Option<Template>,
    default: Option<Template>,
    /// Choices worked out from the run (`{{ steps.list.names }}`), instead of the field's own.
    options: Option<Template>,
}

/// A form as a workflow has it: its texts still to work out.
#[derive(Debug, Clone)]
pub struct FormSpec {
    form: Form,
    title: Template,
    description: Option<Template>,
    thanks: Option<Template>,
    redirect: Option<Template>,
    fields: Vec<FieldSpec>,
}

fn text(v: Option<&Json>) -> Option<&str> {
    v.and_then(Json::as_str)
}

fn short(value: &str, at: &str, longest: usize) -> Result<String, String> {
    if value.chars().count() > longest {
        return Err(format!("{at}: at most {longest} characters"));
    }
    Ok(value.to_owned())
}

/// A field or form name: letters, digits and `_`, starting with a letter.
fn is_name(name: &str) -> bool {
    name.chars().next().is_some_and(|c| c.is_ascii_alphabetic())
        && name.len() <= 64
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A size: bytes as a number, or `500KB`, `2MB`.
fn size(v: &Json) -> Option<usize> {
    if let Some(n) = v.as_u64() {
        return usize::try_from(n).ok();
    }
    let t = v.as_str()?.trim().to_ascii_uppercase();
    let digits: String = t.chars().take_while(char::is_ascii_digit).collect();
    let n: usize = digits.parse().ok()?;
    match t.get(digits.len()..)?.trim() {
        "" | "B" => Some(n),
        "KB" | "KIB" => n.checked_mul(1024),
        "MB" | "MIB" => n.checked_mul(1024 * 1024),
        _ => None,
    }
}

/// The settings a form takes, by where it is.
fn allowed(kind: Use) -> &'static [&'static str] {
    match kind {
        Use::Trigger => &[
            "title",
            "description",
            "fields",
            "button",
            "thanks",
            "redirect",
            "password",
            "rate_limit",
        ],
        Use::Step => &[
            "title",
            "description",
            "fields",
            "button",
            "thanks",
            "redirect",
            "timeout",
            "on_timeout",
        ],
        Use::Approval => &[
            "title",
            "description",
            "fields",
            "comment",
            "approve",
            "reject",
            "thanks",
            "redirect",
            "timeout",
            "on_timeout",
        ],
    }
}

impl FormSpec {
    /// Reads a form's settings; `language` compiles its `{{ }}`.
    pub(crate) fn read(s: &Map<String, Json>, at: &str, language: &str, kind: Use) -> Result<Self, String> {
        let allowed = allowed(kind);
        if let Some(k) = s.keys().find(|k| !allowed.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of a form ({})",
                allowed.join(", ")
            ));
        }
        let template = |name: &str, longest: usize| -> Result<Option<Template>, String> {
            match s.get(name) {
                None | Some(Json::Null) => Ok(None),
                Some(Json::String(t)) => {
                    short(t, &format!("{at}.{name}"), longest)?;
                    if kind == Use::Trigger && t.contains("{{") {
                        return Err(format!(
                            "{at}.{name}: a form that starts a run has no {{{{ }}}}: there is nothing to fill them \
                             from yet"
                        ));
                    }
                    Template::compile(t, language)
                        .map(Some)
                        .map_err(|e| format!("{at}.{name}: {e}"))
                }
                Some(_) => Err(format!("{at}.{name}: text")),
            }
        };
        let title = template("title", MAX_LABEL)?.ok_or_else(|| format!("{at}.title: the form's title"))?;
        let description = template("description", MAX_DESCRIPTION)?;
        let thanks = template("thanks", MAX_DESCRIPTION)?;
        let redirect = template("redirect", 2_000)?;
        if let Some(Json::String(r)) = s.get("redirect")
            && !r.contains("{{")
            && !r.starts_with("https://")
            && !r.starts_with("http://")
        {
            return Err(format!(
                "{at}.redirect: an address, as https://example.com/thanks"
            ));
        }
        let button = match text(s.get("button")) {
            Some(b) => short(b, &format!("{at}.button"), MAX_LABEL)?,
            None => "Send".to_owned(),
        };
        let password = match s.get("password") {
            None | Some(Json::Null) => String::new(),
            Some(Json::String(p)) if !p.is_empty() && p.len() <= 200 => p.clone(),
            Some(_) => return Err(format!("{at}.password: text of 1 to 200 characters")),
        };
        let rate_limit = match s.get("rate_limit") {
            None | Some(Json::Null) => RATE_LIMIT,
            Some(n) => n
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .filter(|n| *n <= 1_000)
                .ok_or_else(|| format!("{at}.rate_limit: answers from one address in a minute, 0 to 1000"))?,
        };
        let approval = match kind {
            Use::Approval => {
                let label = |name: &str, otherwise: &str| -> Result<String, String> {
                    text(s.get(name)).map_or_else(
                        || Ok(otherwise.to_owned()),
                        |t| short(t, &format!("{at}.{name}"), MAX_LABEL),
                    )
                };
                Some(Approval {
                    approve: label("approve", "Approve")?,
                    reject: label("reject", "Reject")?,
                    comment: match s.get("comment") {
                        None | Some(Json::Null) => false,
                        Some(Json::Bool(b)) => *b,
                        Some(_) => return Err(format!("{at}.comment: true to ask for a comment")),
                    },
                })
            }
            _ => None,
        };
        let fields = fields(s.get("fields"), &format!("{at}.fields"), language, kind)?;
        if kind == Use::Step && fields.is_empty() {
            return Err(format!("{at}.fields: at least one field"));
        }
        if kind == Use::Trigger && fields.is_empty() {
            return Err(format!("{at}.fields: at least one field"));
        }
        Ok(Self {
            form: Form {
                title: String::new(),
                description: String::new(),
                fields: fields.iter().map(|f| f.field.clone()).collect(),
                button,
                thanks: String::new(),
                redirect: String::new(),
                password,
                rate_limit,
                approval,
            },
            title,
            description,
            thanks,
            redirect,
            fields,
        })
    }

    /// The form as its page shows it: its texts worked out from the run.
    pub fn render(&self, env: &Env<'_>) -> Result<Form, String> {
        let mut form = self.form.clone();
        let render = |t: &Template, what: &str| t.render(env).map_err(|e| format!("{what}: {e}"));
        form.title = render(&self.title, "title")?;
        form.description = self
            .description
            .as_ref()
            .map(|t| render(t, "description"))
            .transpose()?
            .unwrap_or_default();
        form.thanks = match &self.thanks {
            Some(t) => render(t, "thanks")?,
            None => "Thank you: your answer is in.".to_owned(),
        };
        form.redirect = match &self.redirect {
            Some(t) => {
                let to = render(t, "redirect")?;
                if !to.is_empty() && !to.starts_with("https://") && !to.starts_with("http://") {
                    return Err(format!("redirect: {to:?} is not an http or https address"));
                }
                to
            }
            None => String::new(),
        };
        for (shown, spec) in form.fields.iter_mut().zip(&self.fields) {
            shown.label = render(&spec.label, &format!("{}.label", spec.field.name))?;
            if let Some(help) = &spec.help {
                shown.help = render(help, &format!("{}.help", spec.field.name))?;
            }
            if let Some(default) = &spec.default {
                shown.default = render(default, &format!("{}.default", spec.field.name))?;
            }
            if let Some(options) = &spec.options {
                let at = format!("{}.options", spec.field.name);
                let Value::List(items) = options.value(env).map_err(|e| format!("{at}: {e}"))? else {
                    return Err(format!("{at}: a list of choices"));
                };
                if items.is_empty() || items.len() > MAX_OPTIONS {
                    return Err(format!("{at}: 1 to {MAX_OPTIONS} choices, not {}", items.len()));
                }
                shown.options = items
                    .iter()
                    .map(|i| crate::template::as_text(i).chars().take(MAX_LABEL).collect())
                    .collect();
            }
        }
        Ok(form)
    }

    /// Whether it is an approval.
    pub fn is_approval(&self) -> bool {
        self.form.approval.is_some()
    }
}

fn fields(source: Option<&Json>, at: &str, language: &str, kind: Use) -> Result<Vec<FieldSpec>, String> {
    let list = match source {
        None | Some(Json::Null) => return Ok(Vec::new()),
        Some(Json::Array(list)) => list,
        Some(_) => return Err(format!("{at}: a list of fields, each a name and a type")),
    };
    if list.len() > MAX_FIELDS {
        return Err(format!("{at}: at most {MAX_FIELDS} fields"));
    }
    let mut out: Vec<FieldSpec> = Vec::with_capacity(list.len());
    for (i, item) in list.iter().enumerate() {
        let place = format!("{at}[{i}]");
        let f = field(item, &place, language, kind)?;
        if out.iter().any(|o| o.field.name == f.field.name) {
            return Err(format!(
                "{place}.name: another field has the name {:?}",
                f.field.name
            ));
        }
        out.push(f);
    }
    Ok(out)
}

const FIELD_SETTINGS: &[&str] = &[
    "name",
    "label",
    "type",
    "required",
    "help",
    "placeholder",
    "options",
    "min",
    "max",
    "max_length",
    "max_size",
    "accept",
    "default",
];

fn field(item: &Json, at: &str, language: &str, kind: Use) -> Result<FieldSpec, String> {
    let s = item
        .as_object()
        .ok_or_else(|| format!("{at}: a field, as {{name: email, type: email, required: true}}"))?;
    if let Some(k) = s.keys().find(|k| !FIELD_SETTINGS.contains(&k.as_str())) {
        return Err(format!(
            "{at}.{k}: not a setting of a field ({})",
            FIELD_SETTINGS.join(", ")
        ));
    }
    let name = text(s.get("name")).ok_or_else(|| format!("{at}.name: what its answer is kept under"))?;
    if !is_name(name) {
        return Err(format!(
            "{at}.name: letters, digits and '_', starting with a letter, as full_name"
        ));
    }
    if RESERVED.contains(&name) || (kind == Use::Approval && APPROVAL_RESERVED.contains(&name)) {
        return Err(format!("{at}.name: {name:?} is the answer's own: another name"));
    }
    let at = format!("{at} ({name})");
    let type_name = text(s.get("type")).unwrap_or("text");
    let field_kind = FieldKind::read(type_name).ok_or_else(|| {
        format!(
            "{at}.type: text, long_text, number, email, date, dropdown, checkbox or file, not {type_name:?}"
        )
    })?;
    let template = |key: &str, longest: usize| -> Result<Option<Template>, String> {
        match s.get(key) {
            None | Some(Json::Null) => Ok(None),
            Some(v @ (Json::String(_) | Json::Number(_) | Json::Bool(_))) => {
                let t = match v {
                    Json::String(t) => t.clone(),
                    other => other.to_string(),
                };
                short(&t, &format!("{at}.{key}"), longest)?;
                if kind == Use::Trigger && t.contains("{{") {
                    return Err(format!("{at}.{key}: a form that starts a run has no {{{{ }}}}"));
                }
                Template::compile(&t, language)
                    .map(Some)
                    .map_err(|e| format!("{at}.{key}: {e}"))
            }
            Some(_) => Err(format!("{at}.{key}: text")),
        }
    };
    let label = template("label", MAX_LABEL)?.unwrap_or_else(|| Template::text(&name.replace('_', " ")));
    let mut options_from = None;
    let options: Vec<String> = match s.get("options") {
        None | Some(Json::Null) => Vec::new(),
        Some(Json::Array(list)) => {
            if list.is_empty() || list.len() > MAX_OPTIONS {
                return Err(format!("{at}.options: 1 to {MAX_OPTIONS} choices"));
            }
            let mut out: Vec<String> = Vec::with_capacity(list.len());
            for (i, o) in list.iter().enumerate() {
                let o = match o {
                    Json::String(t) => t.clone(),
                    Json::Number(n) => n.to_string(),
                    _ => return Err(format!("{at}.options[{i}]: text")),
                };
                let o = short(&o, &format!("{at}.options[{i}]"), MAX_LABEL)?;
                if o.is_empty() || out.contains(&o) {
                    return Err(format!("{at}.options[{i}]: each choice once, none empty"));
                }
                out.push(o);
            }
            out
        }
        Some(Json::String(t)) if t.contains("{{") && kind != Use::Trigger => {
            options_from = Some(Template::compile(t, language).map_err(|e| format!("{at}.options: {e}"))?);
            Vec::new()
        }
        Some(_) => return Err(format!("{at}.options: a list of choices")),
    };
    if field_kind == FieldKind::Dropdown && options.is_empty() && options_from.is_none() {
        return Err(format!("{at}.options: a dropdown's choices"));
    }
    if !options.is_empty() && !matches!(field_kind, FieldKind::Dropdown | FieldKind::Checkbox) {
        return Err(format!("{at}.options: only a dropdown or a checkbox has choices"));
    }
    let bound = |key: &str| -> Result<Option<String>, String> {
        let Some(v) = s.get(key).filter(|v| !v.is_null()) else {
            return Ok(None);
        };
        let t = match v {
            Json::Number(n) => n.to_string(),
            Json::String(t) => t.clone(),
            _ => return Err(format!("{at}.{key}: a number, or a date as 2026-01-31")),
        };
        let fits = match field_kind {
            FieldKind::Number => t.parse::<rust_decimal::Decimal>().is_ok(),
            FieldKind::Date => chrono::NaiveDate::parse_from_str(&t, "%Y-%m-%d").is_ok(),
            _ => false,
        };
        if !fits {
            return Err(format!(
                "{at}.{key}: a number's or a date's bound: {t:?} is not one for a {type_name} field"
            ));
        }
        Ok(Some(t))
    };
    let max_length = match s.get("max_length") {
        None | Some(Json::Null) => None,
        Some(v) => Some(
            v.as_u64()
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| (1..=MAX_TEXT_LENGTH).contains(n))
                .ok_or_else(|| format!("{at}.max_length: 1 to {MAX_TEXT_LENGTH} characters"))?,
        ),
    };
    let max_size = match s.get("max_size") {
        None | Some(Json::Null) => None,
        Some(v) => Some(
            size(v)
                .filter(|n| (1..=MAX_FILE_SIZE).contains(n))
                .ok_or_else(|| format!("{at}.max_size: up to 2MB, as 500KB or 2MB"))?,
        ),
    };
    if max_size.is_some() && field_kind != FieldKind::File {
        return Err(format!("{at}.max_size: only a file field takes a size"));
    }
    let required = match s.get("required") {
        None | Some(Json::Null) => false,
        Some(Json::Bool(b)) => *b,
        Some(_) => return Err(format!("{at}.required: true or false")),
    };
    let accept = text(s.get("accept")).unwrap_or_default();
    if accept.len() > 200
        || accept
            .chars()
            .any(|c| !(c.is_ascii_alphanumeric() || ".,/*+-_ ".contains(c)))
    {
        return Err(format!("{at}.accept: file kinds, as .pdf,image/*"));
    }
    Ok(FieldSpec {
        field: Field {
            name: name.to_owned(),
            label: String::new(),
            kind: field_kind,
            required,
            help: String::new(),
            placeholder: short(
                text(s.get("placeholder")).unwrap_or_default(),
                &format!("{at}.placeholder"),
                MAX_LABEL,
            )?,
            options,
            min: bound("min")?,
            max: bound("max")?,
            max_length,
            max_size,
            accept: accept.to_owned(),
            default: String::new(),
        },
        label,
        help: template("help", MAX_LABEL)?,
        default: template("default", MAX_TEXT_LENGTH)?,
        options: options_from,
    })
}

/// A form trigger's form: its texts are plain, so it is worked out once.
pub(crate) fn trigger(s: &Map<String, Json>, at: &str) -> Result<Form, String> {
    let spec = FormSpec::read(s, at, openduct_expr::DEFAULT, Use::Trigger)?;
    let names: Vec<(String, Value)> = Vec::new();
    let env = Env {
        value: &Value::Null,
        message: &Value::Null,
        names: &names,
    };
    spec.render(&env).map_err(|e| format!("{at}: {e}"))
}

/// The longest a run waits for a form, an approval or a call, and how long when the step does not say.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(90 * 24 * 3600);
pub(crate) const TIMEOUT: Duration = Duration::from_secs(7 * 24 * 3600);

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

    use serde_json::json;

    use super::*;

    fn spec(json: Json, kind: Use) -> Result<FormSpec, String> {
        FormSpec::read(json.as_object().unwrap(), "form", openduct_expr::DEFAULT, kind)
    }

    fn rendered(json: Json) -> Form {
        let names: Vec<(String, Value)> = vec![(
            "steps".to_owned(),
            Value::from_json(&json!({ "order": { "id": "A-7", "sizes": ["S", "M"] } })),
        )];
        let env = Env {
            value: &Value::from_json(&json!({ "name": "Ada" })),
            message: &Value::Null,
            names: &names,
        };
        spec(json, Use::Step).unwrap().render(&env).unwrap()
    }

    #[test]
    fn a_form_step_works_out_its_texts_and_choices_from_the_run() {
        let form = rendered(json!({
            "title": "Order {{ steps.order.id }} for {{ value.name }}",
            "fields": [
                { "name": "size", "type": "dropdown", "options": "{{ steps.order.sizes }}", "required": true },
                { "name": "note", "type": "long_text", "label": "A note for {{ value.name }}", "default": "hi" },
            ],
        }));
        assert_eq!(form.title, "Order A-7 for Ada");
        assert_eq!(form.fields[0].options, ["S", "M"]);
        assert_eq!(form.fields[1].label, "A note for Ada");
        assert_eq!(form.fields[1].default, "hi");
        assert_eq!(form.thanks, "Thank you: your answer is in.");
        assert_eq!(form.button, "Send");
        assert_eq!(Form::from_json(&form.to_json()), Some(form));
    }

    #[test]
    fn a_form_says_what_is_wrong_with_it() {
        let wrong = |json: Json, kind: Use| spec(json, kind).err().unwrap();
        assert!(wrong(json!({ "fields": [{ "name": "a" }] }), Use::Step).contains("title"));
        assert!(wrong(json!({ "title": "T", "fields": [] }), Use::Step).contains("at least one field"));
        assert!(wrong(json!({ "title": "T", "fields": [{ "name": "1a" }] }), Use::Step).contains("letters"));
        assert!(
            wrong(
                json!({ "title": "T", "fields": [{ "name": "a" }, { "name": "a" }] }),
                Use::Step
            )
            .contains("another field")
        );
        assert!(
            wrong(
                json!({ "title": "T", "fields": [{ "name": "a", "type": "colour" }] }),
                Use::Step
            )
            .contains("dropdown")
        );
        assert!(
            wrong(
                json!({ "title": "T", "fields": [{ "name": "a", "type": "dropdown" }] }),
                Use::Step
            )
            .contains("choices")
        );
        assert!(
            wrong(
                json!({ "title": "T", "fields": [{ "name": "a", "type": "number", "min": "x" }] }),
                Use::Step
            )
            .contains("bound")
        );
        assert!(
            wrong(
                json!({ "title": "T", "fields": [{ "name": "a", "type": "file", "max_size": "9MB" }] }),
                Use::Step
            )
            .contains("2MB")
        );
        assert!(
            wrong(
                json!({ "title": "Hi {{ value.x }}", "fields": [{ "name": "a" }] }),
                Use::Trigger
            )
            .contains("nothing to fill")
        );
        assert!(
            wrong(
                json!({ "title": "T", "fields": [{ "name": "timed_out" }] }),
                Use::Step
            )
            .contains("answer's own")
        );
        assert!(
            wrong(
                json!({ "title": "T", "comment": true, "fields": [{ "name": "approved" }] }),
                Use::Approval
            )
            .contains("answer's own")
        );
        assert!(
            wrong(
                json!({ "title": "T", "redirect": "javascript:alert(1)", "fields": [{ "name": "a" }] }),
                Use::Step
            )
            .contains("address")
        );
        assert!(
            wrong(
                json!({ "title": "T", "colour": "red", "fields": [{ "name": "a" }] }),
                Use::Step
            )
            .contains("not a setting")
        );
        let many: Vec<Json> = (0..=MAX_FIELDS)
            .map(|i| json!({ "name": format!("f{i}") }))
            .collect();
        assert!(wrong(json!({ "title": "T", "fields": many }), Use::Step).contains("at most 50"));
    }

    #[test]
    fn a_trigger_form_is_plain_and_has_its_protection() {
        let s =
            json!({ "title": "Contact", "fields": [{ "name": "msg" }], "password": "pw", "rate_limit": 3 });
        let form = trigger(s.as_object().unwrap(), "trigger.form").unwrap();
        assert_eq!((form.password.as_str(), form.rate_limit), ("pw", 3));
        assert!(
            !form.to_json().to_string().contains("pw"),
            "the password is never shown"
        );
        let s = json!({ "title": "Contact", "fields": [{ "name": "msg", "default": "{{ value }}" }] });
        assert!(trigger(s.as_object().unwrap(), "trigger.form").is_err());
    }
}

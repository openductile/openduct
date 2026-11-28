// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Files in a run: what an HTTP answer, a webhook's upload or a `convert` step brings, passed from step to step by
//! reference. A file is a name, a media type, a size and its bytes; in a run it is a record -
//! `{file, name, type, size}` - whose `file` says where its bytes are, so `steps.report` or `value.photo` names it
//! like any other value and only a step that needs the bytes reads them.
//!
//! The bytes are kept in the log, in a topic of the workspace's own with the same retention as the runs' history
//! (see [`FileStore`]): a file goes when the runs that made it go, any node of a cluster reads it, and the workspace's
//! lookup files - its reference data, kept until someone deletes them - stay apart from what runs make. A test run
//! keeps its files in memory only, for its own steps.
//!
//! Every file is bounded: at most [`MAX_FILE`] bytes, [`MAX_FILES`] made in one run, and [`MAX_RUN_BYTES`] held by one
//! run in all (the workflows it runs included).

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use openduct_expr::{Compiled, Record, Value};

use crate::run::Effects;
use crate::template::Template;

mod csv;
mod extract;
mod html;
pub mod multipart;
mod pdf;
pub(crate) mod steps;
mod xls;
mod xlsx;
mod xml;
pub(crate) mod zip; // storage: archives packed and unpacked

/// The largest file.
pub const MAX_FILE: usize = 10 * 1024 * 1024;
/// The most files one run makes.
pub const MAX_FILES: usize = 20;
/// The most bytes of files one run holds, made and read.
pub const MAX_RUN_BYTES: usize = 32 * 1024 * 1024;
/// The longest file name.
const MAX_NAME: usize = 200;

/// A file's bytes, with its name and media type.
#[derive(Debug, Clone)]
pub struct FileData {
    pub name: String,
    pub media_type: String,
    pub bytes: Arc<Vec<u8>>,
}

type Pending<'a, T> = Pin<Box<dyn Future<Output = Result<T, String>> + Send + 'a>>;

/// Where a workspace keeps its runs' files: the studio's, in the log.
pub trait FileStore: Send + Sync + Debug {
    /// Keeps the bytes; answers where they are, as text [`FileStore::load`] reads.
    fn keep<'a>(&'a self, bytes: Arc<Vec<u8>>) -> Pending<'a, String>;
    /// The bytes kept at `place`.
    fn load<'a>(&'a self, place: &'a str) -> Pending<'a, Vec<u8>>;
}

/// An HTTP answer as it came: its status and headers (or a test run's answer, which sent nothing), and its body.
#[derive(Debug, Clone)]
pub struct RawAnswer {
    /// `{status, headers}`; or, with no bytes, the whole answer as effects that answer only so give it (a test
    /// run's `{test, request}`, a stand-in's `{status, body}`).
    pub answer: Value,
    /// The body's bytes as they came; `None` when the answer is whole already.
    pub body: Option<Vec<u8>>,
}

/// What an HTTP step makes of an answer's body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Response {
    /// A file when the answer is one - not text, JSON or a form, or sent as an attachment - otherwise as before.
    Auto,
    /// Always a file.
    File,
    /// Always text (or JSON): never a file.
    Text,
}

/// The formats a file is read from and written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Csv,
    Json,
    Xlsx,
    Xls,
    Text,
    Html,
    Pdf,
    Base64,
}

impl Format {
    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "csv" => Self::Csv,
            "json" => Self::Json,
            "xlsx" => Self::Xlsx,
            "xls" => Self::Xls,
            "text" => Self::Text,
            "html" => Self::Html,
            "pdf" => Self::Pdf,
            "base64" => Self::Base64,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Xlsx => "xlsx",
            Self::Xls => "xls",
            Self::Text => "text",
            Self::Html => "html",
            Self::Pdf => "pdf",
            Self::Base64 => "base64",
        }
    }

    /// The media type a file of this format is made with, and its name's extension.
    pub(crate) fn media_type(self) -> (&'static str, &'static str) {
        match self {
            Self::Csv => ("text/csv", "csv"),
            Self::Json => ("application/json", "json"),
            Self::Xlsx => (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "xlsx",
            ),
            Self::Xls => ("application/vnd.ms-excel", "xls"),
            Self::Text => ("text/plain", "txt"),
            Self::Html => ("text/html", "html"),
            Self::Pdf => ("application/pdf", "pdf"),
            Self::Base64 => ("application/octet-stream", "bin"),
        }
    }

    /// The format a file is in, from its media type and then its name.
    pub(crate) fn of(file: &FileData) -> Option<Self> {
        let kind = file.media_type.to_ascii_lowercase();
        let by_type = [
            ("csv", Self::Csv),
            ("spreadsheetml", Self::Xlsx),
            ("ms-excel", Self::Xls),
            ("json", Self::Json),
            ("pdf", Self::Pdf),
            ("html", Self::Html),
            ("text/", Self::Text),
        ];
        if let Some((_, f)) = by_type.iter().find(|(t, _)| kind.contains(t)) {
            return Some(*f);
        }
        let extension = file.name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase())?;
        Some(match extension.as_str() {
            "csv" | "tsv" => Self::Csv,
            "json" | "jsonl" | "ndjson" => Self::Json,
            "xlsx" | "xlsm" => Self::Xlsx,
            "xls" => Self::Xls,
            "txt" | "md" | "log" => Self::Text,
            "html" | "htm" => Self::Html,
            "pdf" => Self::Pdf,
            _ => return None,
        })
    }
}

/// An `extract` step: rows, a value or text read from a file.
#[derive(Debug, Clone)]
pub struct Extract {
    pub file: Compiled,
    /// `None`: from the file's media type, then its name.
    pub format: Option<Format>,
    /// A spreadsheet's sheet, by name; the first when not given.
    pub sheet: Option<Template>,
    /// Whether the first row names the columns (CSV and spreadsheets).
    pub header: bool,
    pub delimiter: u8,
}

/// A `convert` step: a file made of rows, a value, text or Base64.
#[derive(Debug, Clone)]
pub struct Convert {
    pub format: Format,
    /// What it is made of: rows (CSV, XLSX, JSON), a value (JSON), Base64 text.
    pub from: Option<Compiled>,
    /// A text or HTML file's text.
    pub text: Option<Template>,
    pub name: Option<Template>,
    pub media_type: Option<String>,
    /// Which columns, in this order; when empty, every column in the order the rows hold them (rows an expression
    /// gives hold their fields by name, in alphabetical order).
    pub columns: Vec<String>,
    pub sheet: String,
    pub delimiter: u8,
    pub header: bool,
}

/// A `file_details` step: a file's details, and the file again - renamed, or its type changed, when asked.
#[derive(Debug, Clone)]
pub struct Details {
    pub file: Compiled,
    pub name: Option<Template>,
    pub media_type: Option<String>,
}

/// A part of a multipart body: text, or a file.
#[derive(Debug, Clone)]
pub enum Part {
    Text(Template),
    File {
        file: Compiled,
        name: Option<Template>,
        media_type: Option<String>,
    },
}

/// A file named in a run: where its bytes are, what they hash to, what it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reference {
    /// Where the store keeps it; `None`: only the run that made it holds it.
    place: Option<String>,
    sha256: String,
    name: String,
    media_type: String,
    size: usize,
}

impl Reference {
    fn text(&self) -> String {
        format!("{}/{}", self.place.as_deref().unwrap_or("m"), self.sha256)
    }

    /// The file as a run's value: `{file, name, type, size}`.
    fn value(&self) -> Value {
        let mut out = Record::new();
        out.insert("file".to_owned(), Value::text(self.text()));
        out.insert("name".to_owned(), Value::text(self.name.as_str()));
        out.insert("type".to_owned(), Value::text(self.media_type.as_str()));
        out.insert("size".to_owned(), Value::from_json(&serde_json::json!(self.size)));
        Value::record(out)
    }

    /// The file a value names: a record with `file`, `name`, `type` and `size`.
    fn of(value: &Value) -> Option<Self> {
        let Value::Record(r) = value else { return None };
        let reference = match r.get("file")? {
            Value::Text(t) => t.to_string(),
            _ => return None,
        };
        let (place, sha256) = parse_reference(&reference)?;
        let text = |name: &str| match r.get(name) {
            Some(Value::Text(t)) => Some(t.to_string()),
            _ => None,
        };
        let size = r.get("size").map(crate::template::as_text)?.parse().ok()?;
        Some(Self {
            place,
            sha256: sha256.to_owned(),
            name: text("name").unwrap_or_else(|| "file".to_owned()),
            media_type: text("type").unwrap_or_else(|| "application/octet-stream".to_owned()),
            size,
        })
    }
}

/// Where a file's reference (its value's `file`) says its bytes are kept (`None`: only the run that made it held
/// them), and the SHA-256 of its bytes.
pub fn parse_reference(reference: &str) -> Option<(Option<String>, String)> {
    let (place, sha256) = reference.rsplit_once('/')?;
    if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let place = match place {
        "m" => None,
        p if !p.is_empty() && p.len() <= 64 && p.bytes().all(|b| b.is_ascii_digit() || b == b'.') => {
            Some(p.to_owned())
        }
        _ => return None,
    };
    Some((place, sha256.to_owned()))
}

/// Whether a value is a file.
pub(crate) fn is_file(value: &Value) -> bool {
    Reference::of(value).is_some()
}

/// A file name as kept: no folders, no control characters, at most [`MAX_NAME`] characters; `file` when empty.
pub(crate) fn clean_name(name: &str) -> String {
    let cleaned: String = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .take(MAX_NAME)
        .collect();
    let cleaned = cleaned.trim().trim_matches('.').to_owned();
    if cleaned.is_empty() {
        "file".to_owned()
    } else {
        cleaned
    }
}

/// A media type as kept: `type/subtype` with its parameters, or `application/octet-stream`.
pub(crate) fn clean_type(media_type: &str) -> String {
    let t = media_type.trim();
    let essence = t.split(';').next().unwrap_or_default().trim();
    let fine = t.len() <= 200
        && essence.split_once('/').is_some_and(|(a, b)| {
            !a.is_empty()
                && !b.is_empty()
                && essence
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"/+-.".contains(&c))
        })
        && t.bytes().all(|c| (0x20..0x7f).contains(&c));
    if fine {
        t.to_ascii_lowercase()
    } else {
        "application/octet-stream".to_owned()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    ring::digest::digest(&ring::digest::SHA256, bytes)
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// The files a run holds: those its steps made and those it read, for the steps after - shared with the workflows
/// it runs, and counted against the run's limits.
#[derive(Debug, Default)]
pub(crate) struct RunFiles {
    held: Mutex<Held>,
}

#[derive(Debug, Default)]
struct Held {
    files: Vec<(String, Arc<Vec<u8>>)>,
    made: usize,
    bytes: usize,
}

impl RunFiles {
    /// Room for `size` more bytes (and, when `making`, one more file made), taken.
    fn take_room(&self, size: usize, making: bool) -> Result<(), String> {
        if size > MAX_FILE {
            return Err(format!("a file of more than {MAX_FILE} bytes"));
        }
        let mut held = self
            .held
            .lock()
            .map_err(|_| "the run's files cannot be read".to_owned())?;
        if making && held.made >= MAX_FILES {
            return Err(format!("a run makes at most {MAX_FILES} files"));
        }
        if held.bytes.saturating_add(size) > MAX_RUN_BYTES {
            return Err(format!("a run holds at most {MAX_RUN_BYTES} bytes of files"));
        }
        held.bytes += size;
        if making {
            held.made += 1;
        }
        Ok(())
    }

    fn hold(&self, reference: String, bytes: Arc<Vec<u8>>) {
        if let Ok(mut held) = self.held.lock() {
            held.files.push((reference, bytes));
        }
    }

    fn held(&self, reference: &str) -> Option<Arc<Vec<u8>>> {
        let held = self.held.lock().ok()?;
        held.files
            .iter()
            .find(|(r, _)| r == reference)
            .map(|(_, b)| Arc::clone(b))
    }

    /// A file made by a step: kept where the workspace keeps its runs' files (only held here, in a test run), and
    /// held for the steps after. Its value, for the step's result.
    pub(crate) async fn make(
        &self,
        effects: &dyn Effects,
        name: &str,
        media_type: &str,
        bytes: Vec<u8>,
    ) -> Result<Value, String> {
        self.take_room(bytes.len(), true)?;
        let bytes = Arc::new(bytes);
        let place = effects.keep_file(Arc::clone(&bytes)).await?;
        let reference = Reference {
            place,
            sha256: sha256_hex(&bytes),
            name: clean_name(name),
            media_type: clean_type(media_type),
            size: bytes.len(),
        };
        self.hold(reference.text(), bytes);
        Ok(reference.value())
    }

    /// The file a value names, with its bytes: from what this run holds, or from where it was kept (checked to be
    /// the bytes it names).
    pub(crate) async fn read(&self, effects: &dyn Effects, value: &Value) -> Result<FileData, String> {
        let reference = Reference::of(value).ok_or_else(|| {
            format!(
                "{} is not a file: a file is what an HTTP answer, an upload or a convert step gives",
                value.describe()
            )
        })?;
        let data = |bytes| FileData {
            name: reference.name.clone(),
            media_type: reference.media_type.clone(),
            bytes,
        };
        if let Some(bytes) = self.held(&reference.text()) {
            return Ok(data(bytes));
        }
        let Some(place) = &reference.place else {
            return Err(format!(
                "the file {} was made by another test run: run this one on it again",
                reference.name
            ));
        };
        self.take_room(reference.size, false)?;
        let bytes = effects
            .load_file(place)
            .await
            .map_err(|e| format!("the file {} cannot be read: {e}", reference.name))?;
        if bytes.len() != reference.size || sha256_hex(&bytes) != reference.sha256 {
            return Err(format!("the file {} is not the one kept", reference.name));
        }
        let bytes = Arc::new(bytes);
        self.hold(reference.text(), Arc::clone(&bytes));
        Ok(data(bytes))
    }
}

/// A value made a file's value from bytes kept elsewhere: what the studio gives a webhook's upload.
pub fn value_of(place: &str, name: &str, media_type: &str, bytes: &[u8]) -> Value {
    Reference {
        place: Some(place.to_owned()),
        sha256: sha256_hex(bytes),
        name: clean_name(name),
        media_type: clean_type(media_type),
        size: bytes.len(),
    }
    .value()
}

/// What an `extract` step gives for a file of these bytes - read in `format` (`csv`, `xlsx`, `pdf`...; or from its
/// name and media type, when `None`), its first sheet, its first row naming the columns. For hostile-input tests.
pub fn extract_bytes(
    bytes: Vec<u8>,
    name: &str,
    media_type: &str,
    format: Option<&str>,
) -> Result<Value, String> {
    let file = FileData {
        name: name.to_owned(),
        media_type: media_type.to_owned(),
        bytes: Arc::new(bytes),
    };
    let format = match format {
        Some(f) => Format::parse(f).ok_or_else(|| format!("{f:?} is not a format"))?,
        None => Format::of(&file).ok_or("which format is not known")?,
    };
    let asked = extract::Asked {
        format,
        sheet: None,
        header: true,
        delimiter: b',',
    };
    extract::extract(&file, &asked)
}

/// Checks bytes read back are those a file value names.
pub fn matches(bytes: &[u8], sha256: &str) -> bool {
    sha256_hex(bytes) == sha256
}

/// An answer's body as before files: JSON when its type says so, otherwise text.
pub(crate) fn body_value(
    headers: &serde_json::Map<String, serde_json::Value>,
    body: &[u8],
) -> serde_json::Value {
    let is_json = headers
        .get("content-type")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|c| c.contains("json"));
    let text = String::from_utf8_lossy(body).into_owned();
    if is_json {
        serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text))
    } else {
        serde_json::Value::String(text)
    }
}

/// An answer given as a value, by effects that answer only that way (a test run's, a stand-in's): taken as it is,
/// its body already read.
pub(crate) fn raw_of(answer: Value) -> RawAnswer {
    RawAnswer { answer, body: None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DryRun;

    fn block<T>(f: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn a_file_made_is_a_value_the_run_reads_back_and_limits_hold() {
        let files = RunFiles::default();
        let made = block(files.make(
            &DryRun::default(),
            "../../etc/report.csv",
            "TEXT/CSV",
            b"a,b\n1,2\n".to_vec(),
        ))
        .unwrap();
        let json: serde_json::Value = serde_json::from_str(&made.to_json()).unwrap();
        assert_eq!(json["name"], "report.csv", "no folders in a name");
        assert_eq!(json["type"], "text/csv");
        assert_eq!(json["size"], 8);
        assert!(json["file"].as_str().unwrap().starts_with("m/"));
        let back = block(files.read(&DryRun::default(), &made)).unwrap();
        assert_eq!(back.bytes.as_slice(), b"a,b\n1,2\n");
        assert!(is_file(&made) && !is_file(&Value::text("report.csv")));

        let other = RunFiles::default();
        let err = block(other.read(&DryRun::default(), &made)).unwrap_err();
        assert!(err.contains("another test run"), "{err}");
        let err = block(other.read(&DryRun::default(), &Value::text("x"))).unwrap_err();
        assert!(err.contains("is not a file"), "{err}");

        let many = RunFiles::default();
        for i in 0..MAX_FILES {
            block(many.make(&DryRun::default(), &format!("{i}.txt"), "text/plain", vec![b'x'])).unwrap();
        }
        let err = block(many.make(&DryRun::default(), "one-more.txt", "text/plain", vec![b'x'])).unwrap_err();
        assert!(err.contains("at most 20 files"), "{err}");
        let err = block(RunFiles::default().make(&DryRun::default(), "big", "x/y", vec![0; MAX_FILE + 1]))
            .unwrap_err();
        assert!(err.contains("more than"), "{err}");
        let full = RunFiles::default();
        for _ in 0..3 {
            block(full.make(&DryRun::default(), "big", "x/y", vec![0; MAX_FILE])).unwrap();
        }
        assert!(block(full.make(&DryRun::default(), "big", "x/y", vec![0; MAX_FILE])).is_err());
    }

    #[test]
    fn names_and_types_are_kept_safe() {
        assert_eq!(clean_name("C:\\Users\\a\\photo.jpg"), "photo.jpg");
        assert_eq!(clean_name("..."), "file");
        assert_eq!(clean_name("a\r\nb.txt"), "ab.txt");
        assert_eq!(clean_name(&"x".repeat(500)).len(), MAX_NAME);
        assert_eq!(clean_type("text/csv; charset=utf-8"), "text/csv; charset=utf-8");
        assert_eq!(clean_type("text/html\r\nx: y"), "application/octet-stream");
        assert_eq!(clean_type("nonsense"), "application/octet-stream");
        let pdf = FileData {
            name: "Invoice.PDF".into(),
            media_type: "application/octet-stream".into(),
            bytes: Arc::new(Vec::new()),
        };
        assert_eq!(Format::of(&pdf), Some(Format::Pdf));
    }

    #[test]
    fn a_forged_reference_is_not_a_file() {
        let forged = |file: &str| {
            Value::from_json(
                &serde_json::json!({ "file": file, "name": "x", "type": "text/plain", "size": 1 }),
            )
        };
        assert!(is_file(&forged(&format!("12.1/{}", "a".repeat(64)))));
        for bad in [
            format!("../x/{}", "a".repeat(64)),
            format!("12.1/{}", "z".repeat(64)),
            "12.1/abc".to_owned(),
            format!("w.other.topic/{}", "a".repeat(64)),
        ] {
            assert!(!is_file(&forged(&bad)), "{bad}");
        }
    }
}

// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Storage: files in folders and on servers, read into a run's files and written from them - a folder of this
//! machine that the installation's admin allows (`--files-root invoices=/srv/invoices`), an SFTP server, an FTP server
//! (with TLS or without), Amazon S3 or a store that speaks its API (MinIO, Cloudflare R2, Wasabi, Backblaze B2), and -
//! over their own APIs - Azure Blob Storage, Google Cloud Storage, Dropbox, Box, OneDrive and SharePoint, Google Drive
//! ([`cloud`]) - and archives packed and unpacked ([`archive`]), and Redis as a store of keys and values ([`redis`]).
//!
//! **One step for every place.** A `storage` step lists, reads, writes, moves, copies and deletes files and makes
//! folders the same way wherever they are: a folder of this machine is named by `root`, a server by a credential,
//! whose kind (`sftp`, `ftp`, `s3`) says how it is reached. So a workflow moves from an FTP server to S3 by changing its
//! credential alone, and every place has the same checks, limits and preview rules.
//!
//! **Never outside.** Paths are checked before anything is reached ([`paths`]): no `..`, no backslash, no control
//! characters; inside a folder of this machine no path from the top of the disk, and a link that leads out is
//! refused ([`local`]). A server is reached only on the internet's addresses unless the installation allows its own
//! network (`--allow-private-network`), as the HTTP step is.
//!
//! **Runs that only look.** A test run reaches nothing: each step says what it would do (a read gives an empty file
//! of the name it would read). A preview does what reads and shows what writes. Every step answers, so an agent may
//! use one as a tool.

use std::sync::Arc;
use std::time::Duration;

use openduct_expr::{Compiled, Env, Record, Value};
use serde_json::json;

use crate::apps::Credential;
use crate::files::RunFiles;
use crate::run::Effects;
use crate::template::Template;

pub mod archive;
pub(crate) mod cloud; // storage places
mod ftp;
mod listen;
pub mod local;
pub(crate) mod net; // vector stores and messaging: MongoDB's and the mailbox's connections are opened as storage's are
pub mod paths;
pub mod redis;
mod remote;
mod s3;
mod sftp;
mod tar;
pub(crate) mod watch;

pub use cloud::Export; // storage places
pub use listen::listen;
pub use local::Roots;
pub use watch::Watch;

/// The most entries a list gives, by default and at most.
pub(crate) const LIST_ITEMS: usize = 1_000;
pub(crate) const MOST_LIST_ITEMS: usize = 10_000;
/// How long one step's work with a place may take.
const STEP_TIME: Duration = Duration::from_secs(120);

/// A step of this capability: files in a place, an archive, or Redis.
#[derive(Debug, Clone)]
pub enum StorageStep {
    Files(Box<FilesStep>),
    Archive(Box<archive::ArchiveStep>),
    Redis(Box<redis::RedisStep>),
}

impl StorageStep {
    pub(crate) fn kind_name(&self) -> &'static str {
        match self {
            Self::Files(_) => "storage",
            Self::Archive(_) => "compress",
            Self::Redis(_) => "redis",
        }
    }
}

/// What a `storage` step does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    List,
    Read,
    Write,
    Move,
    Copy,
    Delete,
    MakeFolder,
    /// An address that lets someone without the credential get (or put) one object for a while: S3, Azure, Google
    /// Cloud Storage.
    Presign,
}

impl Action {
    pub(crate) const ALL: &'static [&'static str] = &[
        "list",
        "read",
        "write",
        "move",
        "copy",
        "delete",
        "make_folder",
        "presign",
    ];

    pub(crate) fn parse(text: &str) -> Option<Self> {
        Some(match text {
            "list" => Self::List,
            "read" => Self::Read,
            "write" => Self::Write,
            "move" => Self::Move,
            "copy" => Self::Copy,
            "delete" => Self::Delete,
            "make_folder" => Self::MakeFolder,
            "presign" => Self::Presign,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Read => "read",
            Self::Write => "write",
            Self::Move => "move",
            Self::Copy => "copy",
            Self::Delete => "delete",
            Self::MakeFolder => "make_folder",
            Self::Presign => "presign",
        }
    }

    /// Whether it changes the place: a preview shows it instead.
    pub(crate) fn writes(self) -> bool {
        !matches!(self, Self::List | Self::Read | Self::Presign)
    }
}

/// Where a step's files are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Place {
    /// A folder of this machine the installation's admin allows, by its name.
    Root(String),
    /// A server or a store, by the credential that reaches it: an `sftp`, `ftp` or `s3` one, or a place's of
    /// [`cloud`].
    Credential(String),
}

/// A `storage` step, as read.
#[derive(Debug, Clone)]
pub struct FilesStep {
    pub action: Action,
    pub place: Place,
    pub path: Template,
    /// Where a move or a copy goes.
    pub to: Option<Template>,
    /// What a write writes: a run's file, or text.
    pub file: Option<Compiled>,
    /// A list's names: those matching it.
    pub pattern: Option<paths::Pattern>,
    /// A list goes into the folders below too.
    pub recursive: bool,
    /// The most entries a list gives.
    pub limit: usize,
    /// A write, a move or a copy replaces a file already there (otherwise it fails, saying so).
    pub overwrite: bool,
    /// How long a presigned address works.
    pub expires: Duration,
    /// What a presigned address lets its holder do: `GET` or `PUT`.
    pub method: &'static str,
    /// An S3 bucket, when the credential names none.
    pub bucket: Option<Template>,
    /// The folder paths are inside, by its id (Dropbox, Box, OneDrive, Google Drive). // storage places
    pub folder_id: Option<Template>,
    /// Which drive (OneDrive and SharePoint): `me`, a drive's id, or `site:<id>`. // storage places
    pub drive: Option<Template>,
    /// What a Google Docs editors file is read as. // storage places
    pub export: Export,
}

/// The folders the installation's admin allows, and whether servers on this machine's own network may be reached.
#[derive(Debug, Clone, Default)]
pub struct Places {
    pub roots: Arc<Roots>,
    pub allow_private: bool,
}

/// What a run's storage steps may reach.
#[derive(Debug, Clone, Copy)]
pub enum Reach<'a> {
    /// A test run: nothing; each step says what it would do.
    Shown,
    /// A preview: what reads is done, what writes is shown.
    Reads(&'a Places),
    /// A real run.
    Live(&'a Places),
}

impl<'a> Reach<'a> {
    /// The same places, for a preview: writes shown, not done.
    #[must_use]
    pub fn read_only(self) -> Self {
        match self {
            Self::Live(p) | Self::Reads(p) => Self::Reads(p),
            Self::Shown => Self::Shown,
        }
    }

    /// The places, when an action may be done; `None`, with why, when it is only shown.
    pub(crate) fn allows(self, writes: bool) -> Result<&'a Places, &'static str> {
        match self {
            Self::Shown => Err("not sent"),
            Self::Reads(_) if writes => Err("not written"),
            Self::Reads(p) | Self::Live(p) => Ok(p),
        }
    }
}

/// An entry of a listing: a file or a folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Entry {
    pub name: String,
    /// Its path, as the step names paths (inside the credential's own folder, when it has one).
    pub path: String,
    pub folder: bool,
    pub size: u64,
    /// When it last changed, in milliseconds since 1970, when the place says.
    pub modified: Option<i64>,
    /// What tells one version of it from another: its size and time, or its ETag.
    pub version: String,
}

impl Entry {
    pub(crate) fn value(&self) -> Value {
        Value::from_json(&json!({
            "name": self.name,
            "path": self.path,
            "kind": if self.folder { "folder" } else { "file" },
            "size": self.size,
            "modified": self.modified.map(crate::runner::iso),
        }))
    }
}

/// What a step asks, its templates worked out.
#[derive(Debug, Clone)]
pub(crate) struct Asked {
    pub action: Action,
    pub path: String,
    pub to: Option<String>,
    pub pattern: Option<paths::Pattern>,
    pub recursive: bool,
    pub limit: usize,
    pub overwrite: bool,
    pub expires: Duration,
    pub method: &'static str,
    pub bucket: Option<String>,
    pub folder_id: Option<String>, // storage places
    pub drive: Option<String>,     // storage places
    pub export: Export,            // storage places
}

impl Asked {
    /// What it says of its place beside the credential. // storage places
    pub(crate) fn within(&self) -> cloud::Within {
        cloud::Within {
            bucket: self.bucket.clone(),
            folder_id: self.folder_id.clone(),
            drive: self.drive.clone(),
            export: self.export,
        }
    }
}

/// Where a step's place is, by its kind.
enum Target {
    Local(String),
    /// The credential, and its name: a host key seen the first time is kept in it.
    Sftp(String, Credential),
    Ftp(Credential),
    S3(Credential),
    /// A place reached over its vendor's API, and the credential's name: a refreshed token is kept in it. // storage places
    Cloud(String, Credential),
    /// A test run's stand-in for a credential the workspace does not have.
    Test,
}

impl Target {
    fn label(&self) -> &'static str {
        match self {
            Self::Local(_) => "folder",
            Self::Sftp(..) => "sftp",
            Self::Ftp(_) => "ftp",
            Self::S3(_) => "s3",
            Self::Cloud(_, c) => cloud::label(&c.kind), // storage places
            Self::Test => "test",
        }
    }
}

/// Where a step's or a trigger's files are: a folder of this machine (`root`) or a server's credential.
pub(crate) fn place(s: &serde_json::Map<String, serde_json::Value>, at: &str) -> Result<Place, String> {
    let text = |n: &str| s.get(n).and_then(serde_json::Value::as_str);
    match (text("root"), text("credential")) {
        (Some(_), Some(_)) => Err(format!(
            "{at}: root (a folder of this machine) or credential (a server), not both"
        )),
        (Some(root), None) => {
            let fine = !root.is_empty()
                && root.len() <= 64
                && root
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            if !fine {
                return Err(format!(
                    "{at}.root: the name of a folder this installation allows"
                ));
            }
            Ok(Place::Root(root.to_owned()))
        }
        (None, Some(c)) if !c.trim().is_empty() => Ok(Place::Credential(c.to_owned())),
        _ => Err(format!(
            "{at}: root (a folder of this machine the admin allows) or credential (a server's or a store's)"
        )),
    }
}

fn target(effects: &dyn Effects, place: &Place) -> Result<Target, String> {
    let Place::Credential(name) = place else {
        let Place::Root(root) = place else {
            return Err("no place".to_owned());
        };
        return Ok(Target::Local(root.clone()));
    };
    let credential = effects.credential(name)?;
    Ok(match credential.kind.as_str() {
        "sftp" => Target::Sftp(name.clone(), credential),
        "ftp" => Target::Ftp(credential),
        "s3" => Target::S3(credential),
        "test" => Target::Test,
        kind if cloud::takes(kind) => Target::Cloud(name.clone(), credential), // storage places
        other => {
            return Err(format!(
                "{name} is a {other} credential: a storage step takes an sftp, ftp, s3, azure_blob, gcs, dropbox, box, \
                 microsoft_365 or google_drive one (or a root, a folder of this machine)"
            ));
        }
    })
}

/// The step's result.
pub(crate) async fn run(
    effects: &dyn Effects,
    files: &RunFiles,
    step: &StorageStep,
    name: &str,
    env: &Env<'_>,
) -> Result<Value, String> {
    match step {
        StorageStep::Files(f) => run_files(effects, files, f, env).await,
        StorageStep::Archive(a) => archive::run(effects, files, a, name, env).await,
        StorageStep::Redis(r) => redis::run(effects, r, env).await,
    }
}

async fn run_files(
    effects: &dyn Effects,
    files: &RunFiles,
    f: &FilesStep,
    env: &Env<'_>,
) -> Result<Value, String> {
    let render = |t: &Template, what: &str| t.render(env).map_err(|e| format!("{what}: {e}"));
    let asked = Asked {
        action: f.action,
        path: render(&f.path, "path")?,
        to: f.to.as_ref().map(|t| render(t, "to")).transpose()?,
        pattern: f.pattern.clone(),
        recursive: f.recursive,
        limit: f.limit,
        overwrite: f.overwrite,
        expires: f.expires,
        method: f.method,
        bucket: f.bucket.as_ref().map(|t| render(t, "bucket")).transpose()?,
        folder_id: f.folder_id.as_ref().map(|t| render(t, "folder_id")).transpose()?, // storage places
        drive: f.drive.as_ref().map(|t| render(t, "drive")).transpose()?,             // storage places
        export: f.export,                                                             // storage places
    };
    let target = target(effects, &f.place)?;
    let presigns = match &target {
        Target::S3(_) | Target::Test => true,
        Target::Cloud(_, c) => cloud::presigns(&c.kind), // storage places
        _ => false,
    };
    if f.action == Action::Presign && !presigns {
        return Err(format!(
            "presign: only S3 (and the stores that speak its API), Azure Blob Storage and Google Cloud Storage give \
             addresses like that, not {}",
            target.label()
        ));
    }
    // What a write writes: the run's file (a test run holds those its steps made), or text.
    let upload = match (&f.file, f.action) {
        (Some(file), Action::Write) => Some(crate::files::steps::file_of(effects, files, file, env).await?),
        _ => None,
    };
    let places = match effects.storage().allows(f.action.writes()) {
        Ok(p) => p,
        Err(test) => return shown(effects, files, &target, &asked, upload.as_ref(), test).await,
    };
    let work = async {
        let mut session = match &target {
            Target::Local(root) => remote::Session::Local(places.roots.session(root)?),
            Target::Sftp(name, c) => {
                remote::Session::Sftp(Box::new(sftp::Session::open(effects, name, c, places).await?))
            }
            Target::Ftp(c) => remote::Session::Ftp(Box::new(ftp::Session::open(c, places).await?)),
            Target::S3(c) => {
                remote::Session::S3(Box::new(s3::Session::new(effects, c, asked.bucket.as_deref())?))
            }
            // storage places
            Target::Cloud(name, c) => remote::Session::Cloud(Box::new(
                cloud::Session::open(effects, name, c, &asked.within()).await?,
            )),
            Target::Test => return shown(effects, files, &target, &asked, upload.as_ref(), "not sent").await,
        };
        let done = remote::act(&mut session, effects, files, &asked, upload.as_ref()).await;
        session.close().await;
        done
    };
    match tokio::time::timeout(STEP_TIME, work).await {
        Ok(done) => done,
        Err(_) => Err(format!(
            "{} took more than {}s",
            target.label(),
            STEP_TIME.as_secs()
        )),
    }
}

/// What a step would do, shaped as its result will be, saying it was not done (`test`): a test run's, or a
/// preview's write.
async fn shown(
    effects: &dyn Effects,
    files: &RunFiles,
    target: &Target,
    asked: &Asked,
    upload: Option<&crate::files::FileData>,
    test: &str,
) -> Result<Value, String> {
    let mut out = Record::new();
    let put = |out: &mut Record, k: &str, v: Value| {
        out.insert(k.to_owned(), v);
    };
    match asked.action {
        Action::Read => {
            // An empty file of the name it would read, so the steps after it read as they will.
            let name = paths::Rel::parse(&asked.path, true)?
                .name()
                .unwrap_or("file")
                .to_owned();
            let made = files
                .make(effects, &name, crate::files::steps::guess_type(&name), Vec::new())
                .await?;
            if let Value::Record(r) = made {
                out = (*r).clone();
            }
        }
        Action::List => {
            put(&mut out, "files", Value::list(Vec::new()));
            put(&mut out, "count", Value::from_json(&json!(0)));
        }
        Action::Write => {
            put(&mut out, "written", Value::Bool(false));
            put(
                &mut out,
                "size",
                Value::from_json(&json!(upload.map_or(0, |u| u.bytes.len()))),
            );
        }
        Action::Move => put(&mut out, "moved", Value::Bool(false)),
        Action::Copy => put(&mut out, "copied", Value::Bool(false)),
        Action::Delete => put(&mut out, "deleted", Value::Bool(false)),
        Action::MakeFolder => put(&mut out, "made", Value::Bool(false)),
        Action::Presign => put(&mut out, "url", Value::Null),
    }
    put(&mut out, "test", Value::text(test));
    put(&mut out, "storage", Value::text(target.label()));
    put(&mut out, "action", Value::text(asked.action.name()));
    put(&mut out, "path", Value::text(asked.path.as_str()));
    if let Some(to) = &asked.to {
        put(&mut out, "to", Value::text(to.as_str()));
    }
    Ok(Value::record(out))
}

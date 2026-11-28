// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! The places a `storage` step reaches over a vendor's web API, beside a folder of this machine, SFTP, FTP and S3:
//! Azure Blob Storage ([`azure`]), Google Cloud Storage ([`gcs`]), Dropbox ([`dropbox`]), Box ([`boxfiles`]),
//! OneDrive and SharePoint document libraries ([`onedrive`]) and Google Drive ([`gdrive`]) - each by its credential's
//! kind, with the same actions, path checks, limits and test and preview rules as every other place. Their requests
//! go through the run's HTTP effects, so the guard against this machine's own network holds for them as for an HTTP
//! step; a link a vendor gives back (a next page, an upload's address) is followed only on its own host.
//!
//! New files are watched by [`feeds`]: by the vendor's list of changes where it keeps one (Dropbox, OneDrive, Google
//! Drive), by a folder's listing, polled, elsewhere (Azure, Google Cloud Storage, Box).

pub(crate) mod azure;
mod azure_key;
pub(crate) mod boxfiles;
pub(crate) mod connected;
pub(crate) mod dropbox;
pub(crate) mod feeds;
pub(crate) mod gcs;
pub(crate) mod gdrive;
mod google_key;
pub(crate) mod onedrive;
pub(crate) mod tree;
pub(crate) mod web;

use std::time::Duration;

use openduct_expr::Value;

pub use gdrive::Export;

use super::paths::Rel;
use super::remote::Listed;
use crate::apps::Credential;
use crate::files::FileData;
use crate::run::Effects;

/// The credential kinds of these places: those of OpenDuct's own, then the sign-ins a person connects.
pub(crate) const KINDS: &[&str] = &[
    "azure_blob",
    "gcs",
    "box_token",
    "dropbox",
    "box",
    "microsoft_365",
    "google_drive",
];

/// What a step or a trigger says of its place beside the credential.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Within {
    /// An S3 or Google Cloud Storage bucket, or an Azure container, when the credential names none.
    pub bucket: Option<String>,
    /// The folder the paths are inside, by its id (Dropbox, Box, OneDrive, Google Drive).
    pub folder_id: Option<String>,
    /// Which drive (OneDrive): `me`, a drive's id, or `site:<id>`.
    pub drive: Option<String>,
    /// What a Google Docs editors file is read as.
    pub export: Export,
}

/// A place reached over its vendor's API.
#[derive(Debug)]
pub(crate) enum Session<'a> {
    Azure(Box<azure::Session<'a>>),
    Gcs(Box<gcs::Session<'a>>),
    Dropbox(Box<dropbox::Session<'a>>),
    Box(Box<boxfiles::Session<'a>>),
    OneDrive(Box<onedrive::Session<'a>>),
    Drive(Box<gdrive::Session<'a>>),
}

/// Whether credentials of `kind` reach one of these places.
pub(crate) fn takes(kind: &str) -> bool {
    KINDS.contains(&kind)
}

/// The label a place's results and errors name it by.
pub(crate) fn label(kind: &str) -> &'static str {
    match kind {
        "azure_blob" => "azure",
        "gcs" => "gcs",
        "dropbox" => "dropbox",
        "box" | "box_token" => "box",
        "microsoft_365" => "onedrive",
        "google_drive" => "google_drive",
        _ => "storage",
    }
}

impl<'a> Session<'a> {
    /// The place credential `name` (of kind `c.kind`) reaches.
    pub(crate) async fn open(
        effects: &'a dyn Effects,
        name: &str,
        c: &Credential,
        within: &Within,
    ) -> Result<Self, String> {
        let bearer = |vendor| connected::Bearer::open(effects, name, c.clone(), vendor);
        Ok(match c.kind.as_str() {
            "azure_blob" => Self::Azure(Box::new(azure::Session::new(
                effects,
                c,
                within.bucket.as_deref(),
            )?)),
            "gcs" => Self::Gcs(Box::new(
                gcs::Session::open(effects, c, within.bucket.as_deref()).await?,
            )),
            "dropbox" => Self::Dropbox(Box::new(
                dropbox::Session::open(bearer("Dropbox").await?, within.folder_id.as_deref()).await?,
            )),
            "box" | "box_token" => Self::Box(Box::new(boxfiles::Session::new(
                bearer("Box").await?,
                within.folder_id.as_deref(),
            )?)),
            "microsoft_365" => Self::OneDrive(Box::new(onedrive::Session::new(
                bearer("Microsoft Graph").await?,
                within.drive.as_deref(),
                within.folder_id.as_deref(),
            )?)),
            "google_drive" => Self::Drive(Box::new(gdrive::Session::new(
                bearer("Google Drive").await?,
                within.folder_id.as_deref(),
                within.export,
            )?)),
            other => {
                return Err(format!(
                    "{name} is a {other} credential, not one of a place to keep files"
                ));
            }
        })
    }

    /// The prefix or folder every path is inside (Azure, Google Cloud Storage, Dropbox's `folder_id`).
    pub(crate) fn base(&self) -> Rel {
        match self {
            Self::Azure(s) => s.base.clone(),
            Self::Gcs(s) => s.base.clone(),
            Self::Dropbox(s) => s.base.clone(),
            Self::Box(_) | Self::OneDrive(_) | Self::Drive(_) => Rel::default(),
        }
    }

    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Azure(_) => "azure",
            Self::Gcs(_) => "gcs",
            Self::Dropbox(_) => "dropbox",
            Self::Box(_) => "box",
            Self::OneDrive(_) => "onedrive",
            Self::Drive(_) => "google_drive",
        }
    }

    pub(crate) async fn list(
        &mut self,
        dir: &Rel,
        recursive: bool,
        most: usize,
    ) -> Result<(Vec<Listed>, bool), String> {
        match self {
            Self::Azure(s) => s.list(dir, recursive, most).await,
            Self::Gcs(s) => s.list(dir, recursive, most).await,
            Self::Dropbox(s) => s.list(dir, recursive, most).await,
            Self::Box(s) => s.list(dir, recursive, most).await,
            Self::OneDrive(s) => s.list(dir, recursive, most).await,
            Self::Drive(s) => s.list(dir, recursive, most).await,
        }
    }

    /// A file's bytes and time, and its name when the place gives it another (a Google Doc exported).
    pub(crate) async fn read(
        &mut self,
        rel: &Rel,
        most: usize,
    ) -> Result<(Vec<u8>, Option<i64>, Option<String>), String> {
        let plain = |r: Result<(Vec<u8>, Option<i64>), String>| r.map(|(b, m)| (b, m, None));
        match self {
            Self::Azure(s) => plain(s.read(rel, most).await),
            Self::Gcs(s) => plain(s.read(rel, most).await),
            Self::Dropbox(s) => plain(s.read(rel, most).await),
            Self::Box(s) => plain(s.read(rel, most).await),
            Self::OneDrive(s) => plain(s.read(rel, most).await),
            Self::Drive(s) => s.read(rel, most).await,
        }
    }

    pub(crate) async fn write(&mut self, rel: &Rel, file: &FileData, overwrite: bool) -> Result<(), String> {
        match self {
            Self::Azure(s) => s.write(rel, file, overwrite).await,
            Self::Gcs(s) => s.write(rel, file, overwrite).await,
            Self::Dropbox(s) => s.write(rel, file, overwrite).await,
            Self::Box(s) => s.write(rel, file, overwrite).await,
            Self::OneDrive(s) => s.write(rel, file, overwrite).await,
            Self::Drive(s) => s.write(rel, file, overwrite).await,
        }
    }

    pub(crate) async fn rename(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<(), String> {
        match self {
            Self::Azure(s) => s.rename(from, to, overwrite).await,
            Self::Gcs(s) => s.rename(from, to, overwrite).await,
            Self::Dropbox(s) => s.rename(from, to, overwrite).await,
            Self::Box(s) => s.rename(from, to, overwrite).await,
            Self::OneDrive(s) => s.rename(from, to, overwrite).await,
            Self::Drive(s) => s.rename(from, to, overwrite).await,
        }
    }

    pub(crate) async fn copy(&mut self, from: &Rel, to: &Rel, overwrite: bool) -> Result<u64, String> {
        match self {
            Self::Azure(s) => s.copy(from, to, overwrite).await,
            Self::Gcs(s) => s.copy(from, to, overwrite).await,
            Self::Dropbox(s) => s.copy(from, to, overwrite).await,
            Self::Box(s) => s.copy(from, to, overwrite).await,
            Self::OneDrive(s) => s.copy(from, to, overwrite).await,
            Self::Drive(s) => s.copy(from, to, overwrite).await,
        }
    }

    pub(crate) async fn delete(&mut self, rel: &Rel) -> Result<bool, String> {
        match self {
            Self::Azure(s) => s.delete(rel).await,
            Self::Gcs(s) => s.delete(rel).await,
            Self::Dropbox(s) => s.delete(rel).await,
            Self::Box(s) => s.delete(rel).await,
            Self::OneDrive(s) => s.delete(rel).await,
            Self::Drive(s) => s.delete(rel).await,
        }
    }

    pub(crate) async fn make_folder(&mut self, rel: &Rel) -> Result<bool, String> {
        match self {
            Self::Azure(s) => s.make_folder(rel).await,
            Self::Gcs(s) => s.make_folder(rel).await,
            Self::Dropbox(s) => s.make_folder(rel).await,
            Self::Box(s) => s.make_folder(rel).await,
            Self::OneDrive(s) => s.make_folder(rel).await,
            Self::Drive(s) => s.make_folder(rel).await,
        }
    }

    /// An address for one object without the credential: Azure (its shared key) and Google Cloud Storage.
    pub(crate) fn presign(&self, rel: &Rel, method: &str, expires: Duration) -> Result<Value, String> {
        match self {
            Self::Azure(s) => s.presign(rel, method, expires),
            Self::Gcs(s) => s.presign(rel, method, expires),
            _ => Err(format!(
                "presign: {} gives no addresses like that (S3, Azure and Google Cloud Storage do)",
                self.label()
            )),
        }
    }
}

/// Whether a place of credential kind `kind` gives presigned addresses.
pub(crate) fn presigns(kind: &str) -> bool {
    matches!(kind, "azure_blob" | "gcs")
}

// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A preview: a workflow run on sample or recent real data with every step's output shown, where what only reads
//! is done for real - a model's answer, a GET, a lookup file - and what writes - a message sent, a POST, a topic
//! written - is only shown. A write was built with the real credential; before it is shown, every secret this
//! preview handed out is replaced by •••, so a preview shows no more than a test run does.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openduct_expr::Value;

use crate::apps::{Credential, MASK, credential_kind};
use crate::run::{DryRun, Effects, EmailRequest, HttpRequest, ScriptRequest, SqlRequest, WRITES, sql_shown};

type Pending<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

/// The effects of a preview: reads by `live`, writes shown by a test run's effects.
#[derive(Debug)]
pub struct Preview {
    live: Arc<dyn Effects>,
    shown: DryRun,
    /// The secret values handed out, to be hidden in what is shown.
    secrets: Mutex<Vec<String>>,
}

impl std::fmt::Debug for dyn Effects {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Effects")
    }
}

impl Preview {
    pub fn new(live: Arc<dyn Effects>) -> Self {
        Self {
            live,
            shown: DryRun::default(),
            secrets: Mutex::new(Vec::new()),
        }
    }

    fn hidden(&self, text: &str) -> String {
        let mut out = text.to_owned();
        if let Ok(secrets) = self.secrets.lock() {
            for secret in secrets.iter() {
                out = out.replace(secret.as_str(), MASK);
            }
        }
        out
    }
}

impl Effects for Preview {
    fn http<'a>(&'a self, mut request: HttpRequest) -> Pending<'a> {
        if request.read {
            return self.live.http(request);
        }
        request.url = self.hidden(&request.url);
        for (_, value) in &mut request.headers {
            *value = self.hidden(value);
        }
        for (_, value) in &mut request.query {
            *value = self.hidden(value);
        }
        if let Some((_, body)) = &mut request.body {
            *body = self.hidden(&String::from_utf8_lossy(body)).into_bytes();
        }
        self.shown.http(request)
    }

    fn write<'a>(&'a self, topic: &'a str, key: Option<String>, value: String) -> Pending<'a> {
        self.shown.write(topic, key, value)
    }

    fn credential(&self, name: &str) -> Result<Credential, String> {
        let credential = self.live.credential(name)?;
        let secrets = credential_kind(&credential.kind).map_or(&[][..], |k| k.secrets);
        if let Ok(mut held) = self.secrets.lock() {
            for (field, value) in &credential.fields {
                // A value too short to find safely is hidden by the field it is in, in what a test run shows.
                if secrets.contains(&field.as_str()) && value.len() >= 4 && !held.contains(value) {
                    held.push(value.clone());
                }
            }
        }
        Ok(credential)
    }

    fn file<'a>(&'a self, path: &'a str) -> Pending<'a> {
        self.live.file(path)
    }

    /// An e-mail is a write: shown, never sent.
    fn email<'a>(&'a self, mut request: EmailRequest) -> Pending<'a> {
        for text in [&mut request.subject]
            .into_iter()
            .chain(request.text.as_mut())
            .chain(request.html.as_mut())
        {
            *text = self.hidden(text);
        }
        Box::pin(async move {
            crate::email::check(&request)?;
            Ok(crate::email::shown("not sent", &request))
        })
    }

    /// Another workflow, run as a preview too: its reads done, its writes shown.
    fn workflow<'a>(&'a self, id: &'a str) -> Pending<'a> {
        self.live.workflow(id)
    }

    /// A script only computes: it runs.
    fn script<'a>(&'a self, request: ScriptRequest) -> Pending<'a> {
        self.live.script(request)
    }

    /// Run in a read-only transaction: a SELECT answers for real; SQL that writes is refused by the database itself,
    /// and shown.
    fn sql<'a>(&'a self, mut request: SqlRequest) -> Pending<'a> {
        request.read_only = true;
        Box::pin(async move {
            let shown = request.clone();
            match self.live.sql(request).await {
                Err(e) if e == WRITES => {
                    let mut hidden = shown;
                    hidden.sql = self.hidden(&hidden.sql);
                    for value in hidden.params.iter_mut().flatten() {
                        *value = self.hidden(value);
                    }
                    Ok(sql_shown("not written", &hidden))
                }
                other => other,
            }
        })
    }

    /// SQL that only reads runs for real, to read only; SQL that writes is shown. // sql databases
    fn database<'a>(&'a self, request: crate::databases::DatabaseRequest) -> Pending<'a> {
        Box::pin(
            async move { crate::databases::preview(self.live.as_ref(), request, &|t| self.hidden(t)).await },
        )
    }

    fn sleep<'a>(&'a self, _: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }

    // G2 AI agents: an agent's memory is read (a read), and what it would keep only shown.
    fn recall<'a>(&'a self, key: &'a str) -> Pending<'a> {
        self.live.recall(key)
    }

    // vector stores: a memory in a store of the workflow's own is read under the live run's keys.
    fn memory_prefix(&self) -> String {
        self.live.memory_prefix()
    }

    // G2 AI agents: an agent's memory is read (a read), and what it would keep only shown.
    fn remember<'a>(&'a self, key: &'a str, turns: Option<Value>, _: Duration) -> Pending<'a> {
        Box::pin(async move { Ok(crate::agent::not_kept(key, turns.as_ref())) })
    }
    // storage: folders and servers are read for real; a write is shown. A credential's new fields (an SFTP server's
    // key seen the first time) are kept, as a real run keeps them: that changes nothing outside.
    fn storage(&self) -> crate::storage::Reach<'_> {
        self.live.storage().read_only()
    }

    // web: a page, a site, a sitemap or a feed is only read: fetched for real.
    fn web(&self) -> crate::web::Reach<'_> {
        self.live.web()
    }

    // storage
    fn keep_credential<'a>(
        &'a self,
        name: &'a str,
        fields: std::collections::BTreeMap<String, String>,
    ) -> Pending<'a> {
        self.live.keep_credential(name, fields)
    }

    /// Tables are read for real; a change to one is worked out and shown, not written. // data tables
    fn tables(&self) -> crate::tables::Access<'_> {
        self.live.tables().read_only()
    }

    // G6 files: a read's answer as it came; a write only shown. Files it makes are held by the preview alone, and
    // files kept by real runs (an upload the last event brought) are read.
    fn http_bytes<'a>(
        &'a self,
        request: HttpRequest,
    ) -> Pin<Box<dyn Future<Output = Result<crate::files::RawAnswer, String>> + Send + 'a>> {
        if request.read {
            return self.live.http_bytes(request);
        }
        Box::pin(async move { Ok(crate::files::raw_of(self.http(request).await?)) })
    }

    fn load_file<'a>(
        &'a self,
        place: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send + 'a>> {
        self.live.load_file(place)
    }
}

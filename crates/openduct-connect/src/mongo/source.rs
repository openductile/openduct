// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! `from: mongodb_cdc:` - every document inserted, updated, replaced and deleted in chosen collections, from
//! MongoDB's change streams (a replica set or a sharded cluster, MongoDB 6.0 or later). The pipeline saves the
//! stream's resume token once the changes before it are stored, so a restart carries on after the last stored
//! change. The first time, the documents already there are read too: the stream's starting point is noted, the
//! collections are read, and the stream starts from the noted point, so a change made in between comes after the
//! document it changed.

use std::collections::VecDeque;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use mongodb::bson::{Bson, Document, doc};
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::{ChangeStreamOptions, FullDocumentBeforeChangeType, FullDocumentType};
use mongodb::{Client, Cursor};

use super::values;
use crate::json::json_string;
use crate::{SourceBatch, SourceMessage};

const BATCH: usize = 1_000;
const WAIT: Duration = Duration::from_millis(500);
/// The wire version of MongoDB 6.0, the first that keeps documents as they were before a change.
const PRE_IMAGES_WIRE_VERSION: i64 = 17;

type Events = ChangeStream<ChangeStreamEvent<Document>>;

#[derive(Debug)]
pub struct MongoCdcSource {
    pub(super) shown: String,
    pub(super) connect: super::Connect,
    pub(super) pipeline: String,
    pub(super) collections: Vec<(String, String)>,
    pub(super) snapshot: bool,
    client: Option<Client>,
    pre_images: bool,
    phase: Phase,
}

#[derive(Debug)]
enum Phase {
    New,
    Snapshot {
        from: ResumeToken,
        left: VecDeque<(String, String)>,
        cursor: Option<Box<Cursor<Document>>>,
    },
    Streaming(Box<Events>),
}

/// What a failure from the driver means, in words, never with the password.
fn failure(e: &mongodb::error::Error, doing: &str) -> String {
    use mongodb::error::ErrorKind;
    match e.kind.as_ref() {
        ErrorKind::Authentication { .. } => {
            format!("the MongoDB server refused the username or password ({doing})")
        }
        ErrorKind::ServerSelection { message, .. } => {
            format!("no MongoDB server could be reached ({doing}): {message}")
        }
        ErrorKind::Command(c) if c.code == 286 || c.code == 280 => format!(
            "the change stream's place is no longer in the server's oplog, so the changes since are lost to it (a new \
             pipeline name starts again from the collections as they are): {}",
            c.message
        ),
        ErrorKind::Command(c) if c.code == 40573 || c.message.contains("replica set") => {
            "change streams need a replica set or a sharded cluster (a single-node replica set will do)"
                .to_owned()
        }
        _ => format!("MongoDB failed {doing}: {e}"),
    }
}

/// A resume token as the text of a saved position, and back.
fn position(token: &ResumeToken) -> Option<String> {
    let bson = mongodb::bson::serialize_to_bson(token).ok()?;
    let bytes = doc! { "t": bson }.to_vec().ok()?;
    Some(format!("resume:{}", STANDARD.encode(bytes)))
}

fn token(position: &str) -> Option<ResumeToken> {
    let bytes = STANDARD.decode(position.strip_prefix("resume:")?).ok()?;
    let doc = Document::from_reader(bytes.as_slice()).ok()?;
    mongodb::bson::deserialize_from_bson(doc.get("t")?.clone()).ok()
}

impl MongoCdcSource {
    pub(super) fn new(
        shown: String,
        connect: super::Connect,
        pipeline: String,
        collections: Vec<(String, String)>,
        snapshot: bool,
    ) -> Self {
        Self {
            shown,
            connect,
            pipeline,
            collections,
            snapshot,
            client: None,
            pre_images: false,
            phase: Phase::New,
        }
    }

    pub(crate) fn describe(&self) -> String {
        self.shown.clone()
    }

    /// Reach the server, check it can give change streams, and that the collections are there.
    pub(crate) async fn start(&mut self) -> Result<(), String> {
        let options = self.connect.options(&self.pipeline).await?;
        let client = Client::with_options(options).map_err(|e| failure(&e, "setting up"))?;
        let hello = client
            .database("admin")
            .run_command(doc! { "hello": 1 })
            .await
            .map_err(|e| failure(&e, "connecting"))?;
        let replicated = hello.contains_key("setName") || hello.get_str("msg").is_ok_and(|m| m == "isdbgrid");
        if !replicated {
            return Err(
                "the MongoDB server is a single server: change streams need a replica set or a sharded cluster (a \
                 single-node replica set will do)"
                    .to_owned(),
            );
        }
        let wire = hello
            .get("maxWireVersion")
            .and_then(Bson::as_i64)
            .or_else(|| hello.get_i32("maxWireVersion").ok().map(i64::from));
        self.pre_images = wire.is_some_and(|w| w >= PRE_IMAGES_WIRE_VERSION);
        let mut databases: Vec<&str> = self.collections.iter().map(|(d, _)| d.as_str()).collect();
        databases.dedup();
        for db in databases {
            let names = client
                .database(db)
                .list_collection_names()
                .await
                .map_err(|e| failure(&e, &format!("listing the collections of {db}")))?;
            for (_, coll) in self.collections.iter().filter(|(d, _)| d == db) {
                if !names.iter().any(|n| n == coll) {
                    return Err(format!(
                        "the MongoDB database {db} has no collection {coll} (or this user may not see it)"
                    ));
                }
            }
        }
        self.client = Some(client);
        Ok(())
    }

    pub(crate) async fn next(&mut self, saved: Option<&str>) -> Result<SourceBatch, String> {
        match &self.phase {
            Phase::New => self.begin(saved).await,
            Phase::Snapshot { .. } => self.read_snapshot().await,
            Phase::Streaming(_) => self.read_stream().await,
        }
    }

    pub(crate) async fn stop(&mut self) {
        self.phase = Phase::New;
        if let Some(client) = self.client.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), client.shutdown()).await;
        }
    }

    /// The change stream: of one database when every collection is in it, else of the cluster; only the
    /// collections asked for.
    async fn open(&self, after: Option<ResumeToken>) -> Result<Events, String> {
        let client = self
            .client
            .as_ref()
            .ok_or("the mongodb_cdc source was not started")?;
        let names: Vec<Document> = self
            .collections
            .iter()
            .map(|(d, c)| doc! { "db": d, "coll": c })
            .collect();
        let pipeline = vec![doc! { "$match": { "ns": { "$in": names } } }];
        let first_db = self
            .collections
            .first()
            .map(|(d, _)| d.clone())
            .unwrap_or_default();
        let one_database = self.collections.iter().all(|(d, _)| *d == first_db);
        let mut options = ChangeStreamOptions::default();
        options.full_document = Some(FullDocumentType::UpdateLookup);
        options.full_document_before_change = self
            .pre_images
            .then_some(FullDocumentBeforeChangeType::WhenAvailable);
        options.max_await_time = Some(WAIT);
        options.batch_size = Some(u32::try_from(BATCH).unwrap_or(1000));
        options.start_after = after;
        let stream = if one_database {
            client
                .database(&first_db)
                .watch()
                .pipeline(pipeline)
                .with_options(options)
                .await
        } else {
            client.watch().pipeline(pipeline).with_options(options).await
        };
        stream.map_err(|e| failure(&e, "opening the change stream"))
    }

    async fn begin(&mut self, saved: Option<&str>) -> Result<SourceBatch, String> {
        if let Some(from) = saved.and_then(token) {
            let stream = self.open(Some(from)).await?;
            self.phase = Phase::Streaming(Box::new(stream));
            return Ok(SourceBatch::default());
        }
        let stream = self.open(None).await?;
        let from = stream
            .resume_token()
            .ok_or("the MongoDB server gave the new change stream no starting point")?;
        if !self.snapshot {
            self.phase = Phase::Streaming(Box::new(stream));
            return Ok(SourceBatch {
                messages: Vec::new(),
                position: position(&from),
            });
        }
        drop(stream);
        self.phase = Phase::Snapshot {
            from,
            left: self.collections.iter().cloned().collect(),
            cursor: None,
        };
        Ok(SourceBatch::default())
    }

    async fn read_snapshot(&mut self) -> Result<SourceBatch, String> {
        let client = self
            .client
            .clone()
            .ok_or("the mongodb_cdc source was not started")?;
        loop {
            let Phase::Snapshot { from, left, cursor } = &mut self.phase else {
                return Ok(SourceBatch::default());
            };
            let Some((db, coll)) = left.front().cloned() else {
                let from = from.clone();
                let stream = self.open(Some(from.clone())).await?;
                self.phase = Phase::Streaming(Box::new(stream));
                return Ok(SourceBatch {
                    messages: Vec::new(),
                    position: position(&from),
                });
            };
            if cursor.is_none() {
                let found = client
                    .database(&db)
                    .collection::<Document>(&coll)
                    .find(doc! {})
                    .batch_size(u32::try_from(BATCH).unwrap_or(1000))
                    .await
                    .map_err(|e| failure(&e, &format!("reading {db}.{coll}")))?;
                *cursor = Some(Box::new(found));
            }
            let Some(c) = cursor.as_mut() else { continue };
            let mut messages = Vec::new();
            while messages.len() < BATCH {
                let more = c
                    .advance()
                    .await
                    .map_err(|e| failure(&e, &format!("reading {db}.{coll}")))?;
                if !more {
                    *cursor = None;
                    left.pop_front();
                    break;
                }
                let document = c
                    .deserialize_current()
                    .map_err(|e| failure(&e, &format!("reading {db}.{coll}")))?;
                messages.push(message("snapshot", &db, &coll, None, Some(&document), None, None));
            }
            if !messages.is_empty() {
                return Ok(SourceBatch {
                    messages,
                    position: None,
                });
            }
        }
    }

    async fn read_stream(&mut self) -> Result<SourceBatch, String> {
        let Phase::Streaming(stream) = &mut self.phase else {
            return Ok(SourceBatch::default());
        };
        let mut messages = Vec::new();
        let mut last: Option<ResumeToken> = None;
        while messages.len() < BATCH {
            // Waits up to the stream's await time for the next changes; nothing in that time ends the batch.
            let Some(event) = stream
                .next_if_any()
                .await
                .map_err(|e| failure(&e, "reading the change stream"))?
            else {
                break;
            };
            last = Some(event.id.clone());
            if let Some(m) = event_message(&event)? {
                messages.push(m);
            }
        }
        // With nothing new, the stream's own token still moves on, so the saved place keeps up with the oplog.
        let token = last.or_else(|| stream.resume_token());
        Ok(SourceBatch {
            messages,
            position: token.as_ref().and_then(position),
        })
    }
}

/// The message a change becomes; `None` for a change of no row. A collection dropped under a stream of its own
/// ends it.
fn event_message(e: &ChangeStreamEvent<Document>) -> Result<Option<SourceMessage>, String> {
    let (db, coll) =
        e.ns.as_ref()
            .map(|n| (n.db.clone(), n.coll.clone().unwrap_or_default()))
            .unwrap_or_default();
    let committed = e.wall_time.map(|t| t.timestamp_millis());
    let op = match &e.operation_type {
        OperationType::Insert => "insert",
        OperationType::Update => "update",
        OperationType::Replace => "replace",
        OperationType::Delete => "delete",
        OperationType::Drop => "drop",
        OperationType::Rename => "rename",
        OperationType::DropDatabase => "drop_database",
        OperationType::Invalidate => {
            return Err("the change stream ended: its database was dropped".to_owned());
        }
        _ => return Ok(None),
    };
    // Before: the document as it was, when the collection keeps it; for a delete without it, the document's key.
    let before = e.full_document_before_change.as_ref().or(if op == "delete" {
        e.document_key.as_ref()
    } else {
        None
    });
    let changes = e.update_description.as_ref().map(|u| {
        let removed: Vec<String> = u.removed_fields.iter().map(|f| json_string(f)).collect();
        format!(
            "{{\"updated\":{},\"removed\":[{}]}}",
            values::document(&u.updated_fields),
            removed.join(",")
        )
    });
    Ok(Some(
        message(
            op,
            &db,
            &coll,
            before,
            e.full_document.as_ref(),
            changes,
            committed,
        )
        .with_key_from(e.document_key.as_ref()),
    ))
}

trait KeyFrom {
    fn with_key_from(self, key: Option<&Document>) -> Self;
}

impl KeyFrom for SourceMessage {
    /// Key the message by the change's document key, which a delete has though it has no document.
    fn with_key_from(mut self, key: Option<&Document>) -> Self {
        if let (Some(id), Some(k)) = (key.and_then(|k| k.get("_id")), self.key.as_mut()) {
            if let Some(at) = k.iter().position(|b| *b == b'/') {
                k.truncate(at);
            }
            k.push(b'/');
            k.extend_from_slice(values::id_text(id).as_bytes());
        }
        self
    }
}

/// One change as a message: `{"op", "table", "before", "after", "changes", "committed"}`, keyed by collection and
/// document id so one document's changes stay in order.
fn message(
    op: &str,
    db: &str,
    coll: &str,
    before: Option<&Document>,
    after: Option<&Document>,
    changes: Option<String>,
    committed_millis: Option<i64>,
) -> SourceMessage {
    let table = format!("{db}.{coll}");
    let mut value = format!(
        "{{\"op\":{},\"table\":{},\"before\":{},\"after\":{}",
        json_string(op),
        json_string(&table),
        before.map_or_else(|| "null".to_owned(), values::document),
        after.map_or_else(|| "null".to_owned(), values::document),
    );
    if let Some(changes) = changes {
        value.push_str(&format!(",\"changes\":{changes}"));
    }
    if let Some(at) = committed_millis.and_then(chrono::DateTime::from_timestamp_millis) {
        value.push_str(&format!(
            ",\"committed\":{}",
            json_string(&at.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        ));
    }
    value.push('}');
    let mut key = table.clone();
    if let Some(id) = after.or(before).and_then(|d| d.get("_id")) {
        key.push('/');
        key.push_str(&values::id_text(id));
    }
    SourceMessage {
        key: Some(key.into_bytes()),
        value: Some(value.into_bytes()),
        headers: vec![
            ("mongodb-collection".to_owned(), table.into_bytes()),
            ("mongodb-op".to_owned(), op.as_bytes().to_vec()),
        ],
        ack: None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use mongodb::bson::oid::ObjectId;

    use super::*;

    #[test]
    fn a_change_is_one_message_keyed_by_collection_and_id() {
        let id = ObjectId::parse_str("6738a1f2c4e5d6a7b8c9d0e1").unwrap();
        let after = doc! { "_id": id, "status": "paid" };
        let m = message(
            "update",
            "shop",
            "orders",
            None,
            Some(&after),
            Some("{\"updated\":{\"status\":\"paid\"},\"removed\":[]}".into()),
            Some(1_794_736_351_977),
        );
        assert_eq!(
            m.key.as_deref(),
            Some(&b"shop.orders/6738a1f2c4e5d6a7b8c9d0e1"[..])
        );
        assert_eq!(
            String::from_utf8(m.value.unwrap()).unwrap(),
            r#"{"op":"update","table":"shop.orders","before":null,"after":{"_id":"6738a1f2c4e5d6a7b8c9d0e1","status":"paid"},"changes":{"updated":{"status":"paid"},"removed":[]},"committed":"2026-11-15T09:52:31.977Z"}"#
        );
        let gone = message("delete", "shop", "orders", None, None, None, None)
            .with_key_from(Some(&doc! { "_id": 7 }));
        assert_eq!(gone.key.as_deref(), Some(&b"shop.orders/7"[..]));
        assert_eq!(token("resume:not-base64!"), None);
        assert_eq!(token("binlog:x:1"), None);
    }
}

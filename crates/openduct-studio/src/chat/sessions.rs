// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A chat's conversations, as this node knows them: read from the workflow's own history of runs, which every node
//! of a cluster shares - so a conversation goes on whichever node its next message reaches, and across a restart.
//!
//! One follower per chat workflow in use on this node reads that history as it is written: each finished run of the
//! chat adds its turn (the message and the answer) to its conversation, and the message waiting for that run's answer
//! is given it. Started by a chat's first message (reading back the last [`BACKLOG`] runs), it stops after
//! [`IDLE`] without one. What it keeps is bounded: [`TURNS`] turns and [`SESSION_BYTES`] a conversation,
//! [`MAX_SESSIONS`] conversations and [`MAX_BYTES`] in all, a conversation unheard of for [`TTL`] forgotten.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kafka_protocol::records::RecordBatchDecoder;
use openduct_broker::client::{EARLIEST, Fetched, LATEST, Shared, records};
use openduct_flow::chat::{Answer, Turn, answer_of, cut};
use openduct_flow::runner::{self, Topics};
use serde_json::Value as Json;
use tokio::sync::oneshot;

/// How many runs back a follower reads when it starts.
const BACKLOG: i64 = 1_000;
/// The most bytes it reads back when it starts.
const BACKLOG_BYTES: usize = 32 * 1024 * 1024;
/// How long a follower goes on without a message.
const IDLE: Duration = Duration::from_secs(15 * 60);
/// How long a conversation is remembered after its last turn.
const TTL: Duration = Duration::from_secs(24 * 3600);
const TURNS: usize = openduct_flow::chat::MAX_HISTORY;
const SESSION_BYTES: usize = 48 * 1024;
/// The longest message, and answer, a turn keeps (in characters).
const KEPT_CHARS: usize = 8_000;
const MAX_SESSIONS: usize = 20_000;
const MAX_BYTES: usize = 16 * 1024 * 1024;
/// The most messages of one chat waiting for their answers on this node.
const MAX_WAITING: usize = 512;

/// Every chat's follower on this node.
#[derive(Debug, Default)]
pub(super) struct Conversations {
    followers: tokio::sync::Mutex<HashMap<String, Arc<Follower>>>,
}

impl Conversations {
    /// The follower of workflow `id`'s chat, started (and its recent conversations read back) if it is not going.
    pub(super) async fn follower(&self, shared: &Arc<Shared>, id: &str) -> Result<Arc<Follower>, String> {
        let mut held = self.followers.lock().await;
        if let Some(follower) = held.get(id)
            && follower.claim()
        {
            return Ok(Arc::clone(follower));
        }
        let follower = Arc::new(Follower::new(id));
        tokio::time::timeout(Duration::from_secs(15), follower.load(shared))
            .await
            .map_err(|_| "the chat's conversations could not be read in time".to_owned())??;
        tokio::spawn(follow(Arc::clone(&follower), Arc::clone(shared)));
        held.retain(|_, f| f.alive());
        held.insert(id.to_owned(), Arc::clone(&follower));
        Ok(follower)
    }
}

/// One chat's conversations, and the messages waiting for their answers.
#[derive(Debug)]
pub(super) struct Follower {
    workflow: String,
    topic: String,
    kept: Mutex<Kept>,
}

#[derive(Debug)]
struct Kept {
    sessions: HashMap<String, Session>,
    bytes: usize,
    waiting: HashMap<String, oneshot::Sender<Json>>,
    /// The offset of the next run to read.
    next: i64,
    used: Instant,
    stopped: bool,
}

#[derive(Debug, Default)]
struct Session {
    turns: VecDeque<Turn>,
    bytes: usize,
    /// When its last turn was, in milliseconds since 1970.
    at: i64,
}

/// A message waiting for its run's answer: the run's record, as its history keeps it.
#[derive(Debug)]
pub(super) struct Waiting {
    follower: Arc<Follower>,
    reply: String,
    pub(super) answer: oneshot::Receiver<Json>,
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if let Ok(mut kept) = self.follower.kept.lock() {
            kept.waiting.remove(&self.reply);
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

fn ttl_ms() -> i64 {
    i64::try_from(TTL.as_millis()).unwrap_or(i64::MAX)
}

impl Follower {
    fn new(workflow: &str) -> Self {
        Self {
            workflow: workflow.to_owned(),
            topic: Topics::of(workflow).runs,
            kept: Mutex::new(Kept {
                sessions: HashMap::new(),
                bytes: 0,
                waiting: HashMap::new(),
                next: 0,
                used: Instant::now(),
                stopped: false,
            }),
        }
    }

    /// Whether it is still going; if so, it is in use from now.
    fn claim(&self) -> bool {
        let Ok(mut kept) = self.kept.lock() else {
            return false;
        };
        if kept.stopped {
            return false;
        }
        kept.used = Instant::now();
        true
    }

    fn alive(&self) -> bool {
        self.kept.lock().is_ok_and(|k| !k.stopped)
    }

    /// Stops it when no message came for [`IDLE`] and none waits: whether it stopped.
    fn stop_if_idle(&self) -> bool {
        let Ok(mut kept) = self.kept.lock() else {
            return true;
        };
        if kept.used.elapsed() >= IDLE && kept.waiting.is_empty() {
            kept.stopped = true;
        }
        kept.stopped
    }

    /// The conversation's last `turns` turns, oldest first, within what a run is given.
    pub(super) fn history(&self, session: &str, turns: usize) -> Vec<Turn> {
        let Ok(mut kept) = self.kept.lock() else {
            return Vec::new();
        };
        let expired = kept
            .sessions
            .get(session)
            .is_some_and(|s| s.at < now_ms() - ttl_ms());
        if expired && let Some(gone) = kept.sessions.remove(session) {
            kept.bytes = kept.bytes.saturating_sub(gone.bytes);
        }
        let Some(found) = kept.sessions.get(session) else {
            return Vec::new();
        };
        let all: Vec<Turn> = found.turns.iter().cloned().collect();
        openduct_flow::chat::bounded(&all, turns).to_vec()
    }

    /// Waits for the answer of the run whose trigger names `reply`; `None` when too many wait already.
    pub(super) fn wait(self: &Arc<Self>, reply: &str) -> Option<Waiting> {
        let mut kept = self.kept.lock().ok()?;
        if kept.waiting.len() >= MAX_WAITING {
            return None;
        }
        let (sender, answer) = oneshot::channel();
        kept.waiting.insert(reply.to_owned(), sender);
        Some(Waiting {
            follower: Arc::clone(self),
            reply: reply.to_owned(),
            answer,
        })
    }

    /// Reads back the last runs: the conversations of the last day, as far as [`BACKLOG`] reaches.
    async fn load(&self, shared: &Shared) -> Result<(), String> {
        let unreachable = |e: openduct_broker::client::ClientError| format!("cannot reach the broker: {e}");
        let mut client = shared.lend().await.map_err(unreachable)?;
        runner::prepare(&mut client, &self.workflow).await?;
        let end = client
            .list_offset(&self.topic, 0, LATEST)
            .await
            .map_err(unreachable)?;
        let earliest = client.list_offset(&self.topic, 0, EARLIEST).await.unwrap_or(0);
        let mut next = (end - BACKLOG).max(earliest).max(0);
        let mut read = 0;
        while next < end && read < BACKLOG_BYTES {
            let fetched = client
                .fetch(&self.topic, &[(0, next)], 0, 4 * 1024 * 1024)
                .await
                .map_err(unreachable)?;
            let Some(answer) = fetched.into_iter().next() else {
                break;
            };
            if answer.error_code == 1 && next < answer.log_start {
                next = answer.log_start;
                continue;
            }
            read += answer.batches.len();
            let before = next;
            next = self.read(answer, next);
            if next == before {
                break;
            }
        }
        if let Ok(mut kept) = self.kept.lock() {
            kept.next = end.max(next);
        }
        Ok(())
    }

    /// Takes in the runs of a fetch's answer from offset `from`: where the next one is.
    fn read(&self, answer: Fetched, from: i64) -> i64 {
        let mut next = from;
        let mut batches = answer.batches;
        while !batches.is_empty() {
            let Ok(set) =
                RecordBatchDecoder::decode_with_custom_compression(&mut batches, Some(records::decompress))
            else {
                break;
            };
            for record in set.records {
                if record.offset < next || record.control {
                    continue;
                }
                next = record.offset + 1;
                if let Some(value) = &record.value
                    && let Ok(run) = serde_json::from_slice::<Json>(value)
                {
                    self.take(run);
                }
            }
        }
        next
    }

    /// A finished run: the message waiting for it is given it, and its turn is added to its conversation.
    fn take(&self, run: Json) {
        let trigger = run.get("trigger").cloned().unwrap_or(Json::Null);
        if trigger.get("trigger").and_then(Json::as_str) != Some("chat") {
            return;
        }
        let at = run.get("at").and_then(Json::as_i64).unwrap_or_else(now_ms);
        let turn = match (
            trigger.get("session").and_then(Json::as_str),
            run.pointer("/event/message").and_then(Json::as_str),
            answer_of(&run),
        ) {
            (Some(session), Some(message), Answer::Said { text, .. }) => Some((
                session.to_owned(),
                Turn {
                    message: cut(message, KEPT_CHARS),
                    answer: cut(&text, KEPT_CHARS),
                },
            )),
            _ => None,
        };
        let Ok(mut kept) = self.kept.lock() else {
            return;
        };
        if let Some(reply) = trigger.get("reply").and_then(Json::as_str)
            && let Some(waiting) = kept.waiting.remove(reply)
        {
            let _ = waiting.send(run);
        }
        if let Some((session, turn)) = turn
            && at >= now_ms() - ttl_ms()
        {
            kept.add(session, turn, at);
        }
    }

    fn next(&self) -> i64 {
        self.kept.lock().map_or(0, |k| k.next)
    }

    fn moved_to(&self, next: i64) {
        if let Ok(mut kept) = self.kept.lock() {
            kept.next = kept.next.max(next);
        }
    }
}

impl Kept {
    fn add(&mut self, key: String, turn: Turn, at: i64) {
        let session = self.sessions.entry(key).or_default();
        session.bytes += turn.bytes();
        self.bytes += turn.bytes();
        session.turns.push_back(turn);
        session.at = session.at.max(at);
        while session.turns.len() > TURNS || (session.bytes > SESSION_BYTES && session.turns.len() > 1) {
            let Some(old) = session.turns.pop_front() else {
                break;
            };
            session.bytes = session.bytes.saturating_sub(old.bytes());
            self.bytes = self.bytes.saturating_sub(old.bytes());
        }
        if self.sessions.len() > MAX_SESSIONS || self.bytes > MAX_BYTES {
            self.forget_oldest();
        }
    }

    /// The conversations heard of least recently forgotten, until a tenth below the bounds.
    fn forget_oldest(&mut self) {
        let mut by_age: Vec<(i64, String)> = self.sessions.iter().map(|(k, s)| (s.at, k.clone())).collect();
        by_age.sort();
        for (_, key) in by_age {
            if self.sessions.len() <= MAX_SESSIONS * 9 / 10 && self.bytes <= MAX_BYTES * 9 / 10 {
                break;
            }
            if let Some(gone) = self.sessions.remove(&key) {
                self.bytes = self.bytes.saturating_sub(gone.bytes);
            }
        }
    }
}

/// Reads the workflow's history as it is written, until idle.
async fn follow(follower: Arc<Follower>, shared: Arc<Shared>) {
    loop {
        if follower.stop_if_idle() {
            return;
        }
        let next = follower.next();
        match tokio::time::timeout(
            Duration::from_secs(10),
            shared.fetch(&follower.topic, &[(0, next)]),
        )
        .await
        {
            // Nothing new: the history is asked again, so an idle follower notices it is idle.
            Err(_) => {}
            Ok(Err(e)) => {
                tracing::debug!(workflow = follower.workflow, error = %e, "a chat cannot read its runs yet");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(Ok(fetched)) => {
                for answer in fetched {
                    if answer.error_code == 1 && next < answer.log_start {
                        follower.moved_to(answer.log_start);
                    } else if answer.error_code == 0 {
                        let at = follower.read(answer, next);
                        follower.moved_to(at);
                    } else {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(session: &str, reply: &str, message: &str, answer: &str) -> Json {
        json!({
            "status": "done", "at": now_ms(),
            "trigger": { "trigger": "chat", "session": session, "reply": reply },
            "event": { "message": message, "session": session, "history": [] },
            "steps": [{ "name": "answer", "kind": "ai", "result": { "text": answer }, "error": null }],
        })
    }

    #[test]
    fn a_finished_run_answers_its_message_and_adds_its_turn() {
        let follower = Arc::new(Follower::new("w"));
        let mut waiting = follower.wait("r-1").unwrap();
        follower.take(run("s-1", "r-1", "Hi", "Hello!"));
        follower.take(run("s-1", "r-2", "Open on Sunday?", "Yes, 10 to 4."));
        follower.take(run("s-2", "r-3", "Other", "Apart"));
        let given = waiting.answer.try_recv().unwrap();
        assert_eq!(given["trigger"]["reply"], "r-1");
        let history = follower.history("s-1", 10);
        assert_eq!(history.len(), 2);
        assert_eq!(history[1].answer, "Yes, 10 to 4.");
        assert_eq!(
            follower.history("s-1", 1).len(),
            1,
            "as many turns as the trigger gives"
        );
        assert!(follower.history("nobody", 10).is_empty());
        drop(waiting);
        assert!(follower.kept.lock().unwrap().waiting.is_empty());
    }

    #[test]
    fn failed_runs_other_triggers_and_old_turns_add_nothing() {
        let follower = Follower::new("w");
        let mut failed = run("s", "r", "Hi", "x");
        failed["status"] = json!("failed");
        follower.take(failed);
        let mut webhook = run("s", "r", "Hi", "x");
        webhook["trigger"]["trigger"] = json!("webhook");
        follower.take(webhook);
        let mut old = run("s", "r", "Hi", "x");
        old["at"] = json!(now_ms() - ttl_ms() - 1);
        follower.take(old);
        assert!(follower.history("s", 10).is_empty());
    }

    #[test]
    fn what_is_kept_is_bounded() {
        let follower = Follower::new("w");
        for i in 0..(TURNS + 10) {
            follower.take(run("s", "r", &format!("q{i}"), &"a".repeat(100)));
        }
        let kept = follower.kept.lock().unwrap();
        let session = kept.sessions.get("s").unwrap();
        assert_eq!(session.turns.len(), TURNS);
        assert_eq!(session.turns.back().unwrap().message, format!("q{}", TURNS + 9));
        drop(kept);
        let big = "é".repeat(KEPT_CHARS * 2);
        follower.take(run("t", "r", &big, &big));
        let kept = follower.kept.lock().unwrap();
        let turn = kept.sessions.get("t").unwrap().turns.back().unwrap();
        assert_eq!(turn.message.chars().count(), KEPT_CHARS);
        drop(kept);
        let mut many = Kept {
            sessions: HashMap::new(),
            bytes: 0,
            waiting: HashMap::new(),
            next: 0,
            used: Instant::now(),
            stopped: false,
        };
        for i in 0..=MAX_SESSIONS {
            many.add(
                format!("s{i}"),
                Turn {
                    message: "q".to_owned(),
                    answer: "a".to_owned(),
                },
                i64::try_from(i).unwrap(),
            );
        }
        assert!(many.sessions.len() <= MAX_SESSIONS * 9 / 10 + 1);
        assert!(!many.sessions.contains_key("s0"), "the oldest went first");
        assert!(many.sessions.contains_key(&format!("s{MAX_SESSIONS}")));
    }

    #[test]
    fn too_many_waiting_are_refused_and_an_idle_follower_stops() {
        let follower = Arc::new(Follower::new("w"));
        let held: Vec<Waiting> = (0..MAX_WAITING)
            .map(|i| follower.wait(&format!("r{i}")).unwrap())
            .collect();
        assert!(follower.wait("one more").is_none());
        drop(held);
        assert!(!follower.stop_if_idle(), "just used");
        follower.kept.lock().unwrap().used = Instant::now() - IDLE;
        assert!(follower.stop_if_idle());
        assert!(!follower.claim() && !follower.alive());
    }
}

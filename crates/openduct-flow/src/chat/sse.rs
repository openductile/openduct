// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! A model's answer as it is made: server-sent events, read part by part as they come, in either shape the AI step
//! speaks - the chat completions API's (`choices[0].delta.content`, then `data: [DONE]`) or the messages API's
//! (`message_start`, `content_block_delta`, `message_delta`). What has been read is then given back as the answer
//! the same request would have had without streaming, so the AI step reads it as it always does.

use serde_json::{Value as Json, json};

/// The longest line of events kept while it is not ended.
const MAX_LINE: usize = 1024 * 1024;
/// The most text kept from an answer.
const MAX_TEXT: usize = crate::effects::MAX_ANSWER;

#[derive(Debug, Default)]
pub(crate) struct Reader {
    /// The messages API's events, not the chat completions API's.
    messages: bool,
    /// What has come of a line not yet ended.
    line: Vec<u8>,
    /// The event being read: its data lines.
    data: String,
    pub(crate) text: String,
    model: Option<Json>,
    finish: Option<String>,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    pub(crate) error: Option<String>,
    /// How many events were read: none means the answer was not events at all.
    pub(crate) events: usize,
    /// How many bytes were read.
    pub(crate) fed: usize,
}

impl Reader {
    pub(crate) fn new(messages: bool) -> Self {
        Self {
            messages,
            ..Self::default()
        }
    }

    /// Reads more of the answer; the text it added.
    pub(crate) fn feed(&mut self, bytes: &[u8]) -> String {
        self.fed += bytes.len();
        let mut added = String::new();
        for &b in bytes {
            if b != b'\n' {
                if self.line.len() < MAX_LINE {
                    self.line.push(b);
                }
                continue;
            }
            let line = std::mem::take(&mut self.line);
            let line = String::from_utf8_lossy(line.strip_suffix(b"\r").unwrap_or(&line)).into_owned();
            self.line_read(&line, &mut added);
        }
        added
    }

    /// The end of the answer: an event not followed by a blank line is read too.
    pub(crate) fn finish(&mut self) -> String {
        let mut added = String::new();
        if !self.line.is_empty() {
            let line = String::from_utf8_lossy(&std::mem::take(&mut self.line)).into_owned();
            self.line_read(line.trim_end_matches('\r'), &mut added);
        }
        self.dispatch(&mut added);
        added
    }

    fn line_read(&mut self, line: &str, added: &mut String) {
        if line.is_empty() {
            self.dispatch(added);
        } else if let Some(data) = line.strip_prefix("data:") {
            if !self.data.is_empty() {
                self.data.push('\n');
            }
            self.data.push_str(data.strip_prefix(' ').unwrap_or(data));
        }
        // `event:`, `id:`, `retry:` and comments (`: ping`): what an event is, is in its data.
    }

    fn dispatch(&mut self, added: &mut String) {
        let data = std::mem::take(&mut self.data);
        if data.trim().is_empty() {
            return;
        }
        self.events += 1;
        if data.trim() == "[DONE]" {
            return;
        }
        let Ok(event) = serde_json::from_str::<Json>(&data) else {
            return;
        };
        if let Some(error) = event.get("error").filter(|e| !e.is_null()) {
            let said = error
                .get("message")
                .and_then(Json::as_str)
                .map_or_else(|| error.to_string(), str::to_owned);
            self.error = Some(said);
            return;
        }
        let piece = if self.messages {
            self.messages_event(&event)
        } else {
            self.completions_event(&event)
        };
        if let Some(piece) = piece.filter(|p| !p.is_empty())
            && self.text.len() + piece.len() <= MAX_TEXT
        {
            self.text.push_str(&piece);
            added.push_str(&piece);
        }
    }

    fn completions_event(&mut self, event: &Json) -> Option<String> {
        if let Some(model) = event.get("model").filter(|m| m.is_string()) {
            self.model = Some(model.clone());
        }
        if let Some(usage) = event.get("usage").filter(|u| u.is_object()) {
            self.prompt_tokens = usage
                .get("prompt_tokens")
                .and_then(Json::as_u64)
                .or(self.prompt_tokens);
            self.completion_tokens = usage
                .get("completion_tokens")
                .and_then(Json::as_u64)
                .or(self.completion_tokens);
        }
        let choice = event.pointer("/choices/0")?;
        if let Some(finish) = choice.get("finish_reason").and_then(Json::as_str) {
            self.finish = Some(finish.to_owned());
        }
        choice
            .pointer("/delta/content")
            .and_then(Json::as_str)
            .map(str::to_owned)
    }

    fn messages_event(&mut self, event: &Json) -> Option<String> {
        match event.get("type").and_then(Json::as_str)? {
            "message_start" => {
                let message = event.get("message")?;
                if let Some(model) = message.get("model").filter(|m| m.is_string()) {
                    self.model = Some(model.clone());
                }
                self.prompt_tokens = message.pointer("/usage/input_tokens").and_then(Json::as_u64);
                None
            }
            "content_block_delta" => {
                let delta = event.get("delta")?;
                (delta.get("type").and_then(Json::as_str) == Some("text_delta"))
                    .then(|| delta.get("text").and_then(Json::as_str).map(str::to_owned))
                    .flatten()
            }
            "message_delta" => {
                if let Some(reason) = event.pointer("/delta/stop_reason").and_then(Json::as_str) {
                    self.finish = Some(reason.to_owned());
                }
                if let Some(n) = event.pointer("/usage/output_tokens").and_then(Json::as_u64) {
                    self.completion_tokens = Some(n);
                }
                None
            }
            _ => None,
        }
    }

    /// The answer the request would have had whole, in its API's shape; `None` if no event was read.
    pub(crate) fn body(&self) -> Option<Json> {
        if self.events == 0 {
            return None;
        }
        let model = self.model.clone().unwrap_or(Json::Null);
        Some(if self.messages {
            json!({
                "type": "message", "role": "assistant", "model": model,
                "content": [{ "type": "text", "text": self.text }],
                "stop_reason": self.finish.clone().unwrap_or_else(|| "end_turn".to_owned()),
                "usage": { "input_tokens": self.prompt_tokens, "output_tokens": self.completion_tokens },
            })
        } else {
            let total = self.prompt_tokens.zip(self.completion_tokens).map(|(p, c)| p + c);
            json!({
                "object": "chat.completion", "model": model,
                "choices": [{
                    "index": 0,
                    "message": { "role": "assistant", "content": self.text },
                    "finish_reason": self.finish.clone().unwrap_or_else(|| "stop".to_owned()),
                }],
                "usage": {
                    "prompt_tokens": self.prompt_tokens, "completion_tokens": self.completion_tokens,
                    "total_tokens": total,
                },
            })
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn chat_completions_events_read_in_any_pieces_make_the_whole_answer() {
        let events = concat!(
            "data: {\"model\":\"m-1\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n",
            ": a comment\n\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\r\n\r\n",
            "data: {\"choices\":[{\"delta\":{\"content\":\"lo, Ada\"},\"finish_reason\":\"stop\"}]}\n\n",
            "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n",
            "data: [DONE]\n\n",
        );
        for size in [1, 3, 17, events.len()] {
            let mut reader = Reader::new(false);
            let mut said = String::new();
            for piece in events.as_bytes().chunks(size) {
                said.push_str(&reader.feed(piece));
            }
            said.push_str(&reader.finish());
            assert_eq!(said, "Hello, Ada", "in pieces of {size}");
            let body = reader.body().unwrap();
            assert_eq!(body["choices"][0]["message"]["content"], "Hello, Ada");
            assert_eq!(body["choices"][0]["finish_reason"], "stop");
            assert_eq!(body["usage"]["total_tokens"], 10);
            assert_eq!(body["model"], "m-1");
        }
    }

    #[test]
    fn messages_events_make_the_messages_answer_and_an_error_is_kept() {
        let events = concat!(
            "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"model\":\"m-2\",\"usage\":{\"input_tokens\":12}}}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Yes\"}}\n\n",
            "event: ping\ndata: {\"type\":\"ping\"}\n\n",
            "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\", we do.\"}}\n\n",
            "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"max_tokens\"},\"usage\":{\"output_tokens\":4}}\n\n",
            "event: message_stop\ndata: {\"type\":\"message_stop\"}",
        );
        let mut reader = Reader::new(true);
        let mut said = reader.feed(events.as_bytes());
        said.push_str(&reader.finish());
        assert_eq!(said, "Yes, we do.");
        let body = reader.body().unwrap();
        assert_eq!(body["content"][0]["text"], "Yes, we do.");
        assert_eq!(body["stop_reason"], "max_tokens");
        assert_eq!(body["usage"]["input_tokens"], 12);
        assert_eq!(body["usage"]["output_tokens"], 4);

        let mut failed = Reader::new(true);
        failed.feed(b"event: error\ndata: {\"type\":\"error\",\"error\":{\"message\":\"overloaded\"}}\n\n");
        assert_eq!(failed.error.as_deref(), Some("overloaded"));
        assert!(
            Reader::new(false).body().is_none(),
            "no events: not an answer of events"
        );
    }

    #[test]
    fn a_line_that_never_ends_is_kept_short() {
        let mut reader = Reader::new(false);
        reader.feed(&vec![b'x'; MAX_LINE * 2]);
        assert!(reader.line.len() <= MAX_LINE);
        assert_eq!(reader.finish(), "");
    }
}

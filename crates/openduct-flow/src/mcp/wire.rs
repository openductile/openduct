// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! What MCP's messages are made of, for its client here and the studio's server: the protocol versions, JSON-RPC 2.0
//! requests, results and errors, the headers Streamable HTTP mirrors a request's fields into (a value that is not
//! plain ASCII Base64-encoded between `=?base64?` and `?=`), and the messages of an answer sent as Server-Sent Events.

use base64::Engine as _;
use serde_json::{Map, Value as Json, json};

/// The protocol version this side speaks first: every request carries it, and no handshake comes before.
pub const MODERN: &str = "2026-07-28";
/// The earlier versions, which start with an `initialize` handshake: newest first.
pub const LEGACY: &[&str] = &["2025-11-25", "2025-06-18", "2025-03-26"];
/// The version a request without `MCP-Protocol-Version` is taken to speak.
pub const UNNAMED: &str = "2025-03-26";

/// The `_meta` keys every modern request carries.
pub const META_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
pub const META_CLIENT: &str = "io.modelcontextprotocol/clientInfo";
pub const META_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
pub const META_SERVER: &str = "io.modelcontextprotocol/serverInfo";

/// JSON-RPC's own error codes, and MCP's.
pub const PARSE_ERROR: i64 = -32700;
pub const INVALID_REQUEST: i64 = -32600;
pub const METHOD_NOT_FOUND: i64 = -32601;
pub const INVALID_PARAMS: i64 = -32602;
pub const INTERNAL_ERROR: i64 = -32603;
pub const HEADER_MISMATCH: i64 = -32020;
pub const MISSING_CAPABILITY: i64 = -32021;
pub const UNSUPPORTED_VERSION: i64 = -32022;
/// Too many calls: OpenDuct's own code, outside the range JSON-RPC and MCP keep.
pub const TOO_MANY_CALLS: i64 = -31429;

/// The largest SSE answer read for its messages, and the most messages read from it.
const MAX_EVENTS: usize = 1000;

/// What a message holds at `path` (a JSON pointer, as `/error/code`): null when it holds nothing there.
pub fn at<'a>(message: &'a Json, path: &str) -> &'a Json {
    static NOTHING: Json = Json::Null;
    message.pointer(path).unwrap_or(&NOTHING)
}

/// Every version this side speaks: the modern one first.
pub fn versions() -> Vec<&'static str> {
    let mut all = vec![MODERN];
    all.extend_from_slice(LEGACY);
    all
}

/// Who this side is, as MCP names an implementation.
pub fn implementation() -> Json {
    json!({ "name": "OpenDuct", "version": env!("CARGO_PKG_VERSION") })
}

/// A JSON-RPC request; `id` none for a notification.
pub fn request(id: Option<u64>, method: &str, params: Json) -> Json {
    let mut message = json!({ "jsonrpc": "2.0", "method": method });
    if let Some(fields) = message.as_object_mut() {
        if let Some(id) = id {
            fields.insert("id".to_owned(), json!(id));
        }
        if !params.is_null() {
            fields.insert("params".to_owned(), params);
        }
    }
    message
}

/// A JSON-RPC result answering the request `id`.
pub fn result(id: &Json, result: Json) -> Json {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// A JSON-RPC error answering the request `id` (`null` when it could not be read).
pub fn error(id: &Json, code: i64, message: &str, data: Option<Json>) -> Json {
    let mut error = json!({ "code": code, "message": message });
    if let (Some(data), Some(fields)) = (data, error.as_object_mut()) {
        fields.insert("data".to_owned(), data);
    }
    let mut out = json!({ "jsonrpc": "2.0", "error": error });
    if !id.is_null()
        && let Some(fields) = out.as_object_mut()
    {
        fields.insert("id".to_owned(), id.clone());
    }
    out
}

/// Whether an answer's error is one only a server of the modern protocol gives: the sign that the server speaks it,
/// whatever else the request got wrong.
pub fn modern_error(message: &Json) -> bool {
    matches!(
        at(message, "/error/code").as_i64(),
        Some(HEADER_MISMATCH | MISSING_CAPABILITY | UNSUPPORTED_VERSION)
    )
}

/// A value as a header carries it: as it is when it is plain visible ASCII without spaces at its ends, else (or when
/// it looks like an encoded one) Base64 between `=?base64?` and `?=`.
pub fn header_value(value: &str) -> String {
    let plain = value.bytes().all(|b| (0x20..=0x7e).contains(&b) || b == b'\t')
        && value.trim() == value
        && !(value.starts_with("=?base64?") && value.ends_with("?="));
    if plain {
        value.to_owned()
    } else {
        format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode(value.as_bytes())
        )
    }
}

/// A header's value as it was meant: decoded when Base64-encoded; `None` when it is encoded badly.
pub fn header_text(value: &str) -> Option<String> {
    match value.strip_prefix("=?base64?").and_then(|v| v.strip_suffix("?=")) {
        Some(encoded) => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()
            .and_then(|b| String::from_utf8(b).ok()),
        None => Some(value.to_owned()),
    }
}

/// The JSON messages of an answer sent as Server-Sent Events: each event's `data` lines, joined.
pub fn events(text: &str) -> Vec<Json> {
    fn end(data: &mut Vec<&str>, out: &mut Vec<Json>) {
        if !data.is_empty()
            && out.len() < MAX_EVENTS
            && let Ok(message) = serde_json::from_str::<Json>(&data.join("\n"))
        {
            out.push(message);
        }
        data.clear();
    }
    let mut out = Vec::new();
    let mut data: Vec<&str> = Vec::new();
    for line in text.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            end(&mut data, &mut out);
        } else if let Some(rest) = line.strip_prefix("data:") {
            data.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
        // Comments (`:`), `event:`, `id:` and `retry:` carry no message.
    }
    end(&mut data, &mut out);
    out
}

/// The `_meta` a modern request carries: the version, who asks, and that it asks for no capability of the client.
pub fn request_meta() -> Json {
    let mut meta = Map::new();
    meta.insert(META_VERSION.to_owned(), json!(MODERN));
    meta.insert(META_CLIENT.to_owned(), implementation());
    meta.insert(META_CAPABILITIES.to_owned(), json!({}));
    Json::Object(meta)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn a_header_value_that_is_not_plain_is_sent_in_base64_and_read_back() {
        assert_eq!(header_value("us-west1"), "us-west1");
        assert_eq!(header_value("Hello, 世界"), "=?base64?SGVsbG8sIOS4lueVjA==?=");
        assert_eq!(header_value(" padded "), "=?base64?IHBhZGRlZCA=?=");
        assert_eq!(header_value("line1\nline2"), "=?base64?bGluZTEKbGluZTI=?=");
        assert_eq!(
            header_value("=?base64?literal?="),
            "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="
        );
        for v in ["us-west1", "Hello, 世界", " padded ", "=?base64?literal?="] {
            assert_eq!(header_text(&header_value(v)).unwrap(), v);
        }
        assert_eq!(header_text("=?base64?!!?="), None);
    }

    #[test]
    fn the_messages_of_an_event_stream_are_read_whatever_comes_between() {
        let text = ": keep-alive\r\nevent: message\r\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\"}\r\n\r\n\
                    id: 7\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"id\":1,\"result\":{}}\n\ndata: not json\n\n";
        let found = events(text);
        assert_eq!(found.len(), 2);
        assert_eq!(found[1]["id"], 1);
        assert!(events("").is_empty());
    }

    #[test]
    fn errors_and_requests_are_json_rpc() {
        let e = error(
            &json!(3),
            UNSUPPORTED_VERSION,
            "Unsupported protocol version",
            Some(json!({ "supported": [MODERN] })),
        );
        assert_eq!(e["id"], 3);
        assert!(modern_error(&e));
        assert!(!modern_error(&error(&Json::Null, INVALID_PARAMS, "x", None)));
        assert!(error(&Json::Null, PARSE_ERROR, "x", None).get("id").is_none());
        assert!(
            request(None, "notifications/initialized", Json::Null)
                .get("id")
                .is_none()
        );
        assert_eq!(versions()[0], MODERN);
    }
}

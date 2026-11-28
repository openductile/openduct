// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! An MCP client: a server reached over Streamable HTTP through a run's [`Effects`], so the HTTP step's rules hold -
//! this machine's own network refused unless allowed, a test run sending nothing, a preview sending only what reads.
//!
//! It speaks the current protocol first (each request carrying its version; no handshake), and falls back to the
//! earlier one (an `initialize` handshake, perhaps a session) when the server's answer shows it does not speak it -
//! a refusal that is not one only a current server gives, or a current server naming only earlier versions. What a
//! server spoke is kept for an hour per address, so each is probed once.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use openduct_expr::Value;
use serde_json::{Map, Value as Json, json};

use super::wire::{self, LEGACY, MODERN};
use crate::apps::Credential;
use crate::run::{Effects, HttpRequest};

/// The most tools read from a server, and the most pages of them.
pub const MAX_TOOLS: usize = 500;
const MAX_PAGES: usize = 20;
/// The most content items read from a tool's answer.
const MAX_CONTENT: usize = 100;
/// The longest description of a tool kept, in bytes.
const MAX_TOOL_DESCRIPTION: usize = 8 * 1024;
/// How long what a server spoke is remembered, and for how many servers.
const ERA_KEPT: Duration = Duration::from_secs(3600);
const MAX_ERAS: usize = 1024;

/// An MCP server: where it is, the credential it is signed in to with (bearer, header or basic), how long each
/// request may take.
#[derive(Debug, Clone)]
pub struct Server {
    pub url: String,
    pub credential: Option<Credential>,
    pub timeout: Duration,
}

/// A tool a server offers.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolInfo {
    pub name: String,
    pub title: Option<String>,
    pub description: String,
    /// The JSON Schema of its arguments.
    pub input_schema: Json,
    /// What the server says of it (`readOnlyHint` and the like): to be trusted only as far as the server is.
    pub annotations: Json,
    /// The JSON Schema of its structured answer, when it says (null when not). // app generator
    pub output_schema: Json,
}

/// What a tool answered.
#[derive(Debug, Clone, PartialEq)]
pub struct ToolAnswer {
    /// Its content items as sent: text, images, links, resources.
    pub content: Vec<Json>,
    /// Its text items, joined by new lines.
    pub text: String,
    pub structured: Option<Json>,
    /// The tool failed and says why (in `text`).
    pub is_error: bool,
}

impl ToolAnswer {
    /// The answer as one value: its structured content when it has some, else its text read as JSON when it is
    /// JSON, else its text.
    pub fn value(&self) -> Json {
        if let Some(s) = &self.structured {
            return s.clone();
        }
        serde_json::from_str(self.text.trim()).unwrap_or_else(|_| Json::String(self.text.clone()))
    }

    fn read(result: &Json) -> Self {
        let content: Vec<Json> = result["content"]
            .as_array()
            .map(|items| items.iter().take(MAX_CONTENT).cloned().collect())
            .unwrap_or_default();
        let text = content
            .iter()
            .filter(|c| c["type"] == "text")
            .filter_map(|c| c["text"].as_str())
            .collect::<Vec<_>>()
            .join("\n");
        Self {
            content,
            text,
            structured: result.get("structuredContent").filter(|s| !s.is_null()).cloned(),
            is_error: result["isError"].as_bool().unwrap_or(false),
        }
    }
}

/// What asking a server came to: its answer, or - in a test run or a preview, which do not send what they cannot
/// undo - the request that would have been sent.
#[derive(Debug, Clone)]
pub enum Reached<T> {
    Answered(T),
    NotSent(Value),
}

#[derive(Debug, Clone, PartialEq)]
enum Era {
    /// Not known yet: the current protocol is tried first.
    Unknown,
    Modern,
    /// The earlier protocol: its version (once agreed) and the session the server gave, if it gave one.
    Legacy {
        version: String,
        ready: bool,
        session: Option<String>,
    },
}

fn legacy() -> Era {
    Era::Legacy {
        version: LEGACY.first().copied().unwrap_or(wire::UNNAMED).to_owned(),
        ready: false,
        session: None,
    }
}

fn eras() -> &'static Mutex<HashMap<String, (bool, Instant)>> {
    static ERAS: OnceLock<Mutex<HashMap<String, (bool, Instant)>>> = OnceLock::new();
    ERAS.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Whether the server at `url` spoke the earlier protocol when last asked, within the hour.
fn remembered(url: &str) -> Option<bool> {
    let eras = eras().lock().ok()?;
    eras.get(url)
        .filter(|(_, at)| at.elapsed() < ERA_KEPT)
        .map(|(legacy, _)| *legacy)
}

fn remember(url: &str, legacy: bool) {
    if let Ok(mut eras) = eras().lock() {
        if eras.len() >= MAX_ERAS {
            eras.retain(|_, (_, at)| at.elapsed() < ERA_KEPT);
            if eras.len() >= MAX_ERAS {
                eras.clear();
            }
        }
        eras.insert(url.to_owned(), (legacy, Instant::now()));
    }
}

/// One answer: its status, headers (names in small letters) and the JSON-RPC message it holds, if any.
struct Reply {
    status: u64,
    headers: Map<String, Json>,
    message: Option<Json>,
    /// What it said, when it held no message: for the error.
    said: String,
}

enum Asked {
    NotSent(Value),
    Answered(Json),
    /// A JSON-RPC error from the server.
    Refused {
        code: i64,
        message: String,
    },
}

/// Talking to one MCP server: what it speaks, found on the first request, and the tools it listed.
pub struct Session<'e> {
    effects: &'e dyn Effects,
    server: Server,
    era: Era,
    /// Whether what it speaks was found in this session (not only remembered).
    confirmed: bool,
    next: u64,
    tools: Option<Vec<ToolInfo>>,
}

impl std::fmt::Debug for Session<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("url", &self.server.url)
            .field("protocol", &self.protocol())
            .finish_non_exhaustive()
    }
}

impl<'e> Session<'e> {
    /// A session with `server`: nothing is sent until the first request.
    pub fn new(effects: &'e dyn Effects, server: Server) -> Self {
        let era = match remembered(&server.url) {
            Some(true) => legacy(),
            Some(false) => Era::Modern,
            None => Era::Unknown,
        };
        Self {
            effects,
            server,
            era,
            confirmed: false,
            next: 0,
            tools: None,
        }
    }

    /// The protocol version it speaks with the server, once known.
    pub fn protocol(&self) -> Option<&str> {
        match &self.era {
            Era::Modern if self.confirmed => Some(MODERN),
            Era::Legacy {
                version, ready: true, ..
            } => Some(version),
            _ => None,
        }
    }

    /// The tools the server offers (at most [`MAX_TOOLS`]); a tool whose definition breaks the protocol's rules is
    /// left out.
    pub async fn tools(&mut self) -> Result<Reached<Vec<ToolInfo>>, String> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut params = Map::new();
            if let Some(c) = &cursor {
                params.insert("cursor".to_owned(), json!(c));
            }
            let result = match self.ask("tools/list", params, None, Vec::new(), true).await? {
                Asked::NotSent(shown) => return Ok(Reached::NotSent(shown)),
                Asked::Answered(r) => r,
                Asked::Refused { code, message } => {
                    return Err(format!("the server would not list its tools: {message} ({code})"));
                }
            };
            for tool in wire::at(&result, "/tools").as_array().into_iter().flatten() {
                if out.len() >= MAX_TOOLS {
                    break;
                }
                match ToolInfo::read(tool) {
                    Ok(info) => out.push(info),
                    Err(why) => tracing::warn!(tool = %tool["name"], why = %why, "an MCP tool left out"),
                }
            }
            cursor = wire::at(&result, "/nextCursor")
                .as_str()
                .filter(|c| !c.is_empty() && c.len() <= 1024)
                .map(str::to_owned);
            if cursor.is_none() || out.len() >= MAX_TOOLS {
                break;
            }
        }
        self.tools = Some(out.clone());
        Ok(Reached::Answered(out))
    }

    /// Calls tool `name` with `arguments` (a record). `read_only` says the tool only reads, so a preview may call
    /// it; otherwise a preview shows the call instead.
    pub async fn call(
        &mut self,
        name: &str,
        arguments: &Json,
        read_only: bool,
    ) -> Result<Reached<ToolAnswer>, String> {
        let mut params = Map::new();
        params.insert("name".to_owned(), json!(name));
        params.insert("arguments".to_owned(), arguments.clone());
        let mut listed = self.tools.is_some();
        loop {
            let headers = self.param_headers(name, arguments);
            match self
                .ask("tools/call", params.clone(), Some(name), headers, read_only)
                .await?
            {
                Asked::NotSent(shown) => return Ok(Reached::NotSent(shown)),
                Asked::Answered(result) => return Ok(Reached::Answered(ToolAnswer::read(&result))),
                // The tool's definition names arguments to send as headers too: read it, and ask again once.
                Asked::Refused {
                    code: wire::HEADER_MISMATCH,
                    ..
                } if !listed => {
                    listed = true;
                    if let Reached::NotSent(shown) = self.tools().await? {
                        return Ok(Reached::NotSent(shown));
                    }
                }
                Asked::Refused { code, message } => {
                    return Err(format!(
                        "the server refused the call of {name}: {message} ({code})"
                    ));
                }
            }
        }
    }

    /// Ends a session the server gave (the earlier protocol's): the server may then forget it.
    pub async fn close(self) {
        let Era::Legacy {
            version,
            session: Some(session),
            ..
        } = &self.era
        else {
            return;
        };
        let mut request = HttpRequest {
            read: true,
            method: "DELETE".to_owned(),
            url: self.server.url.clone(),
            headers: vec![
                ("mcp-protocol-version".to_owned(), version.clone()),
                ("mcp-session-id".to_owned(), session.clone()),
            ],
            query: Vec::new(),
            body: None,
            timeout: self.server.timeout,
        };
        if let Some(c) = &self.server.credential
            && crate::apps::authorize(&mut request, c).is_err()
        {
            return;
        }
        let _ = self.effects.http(request).await;
    }

    /// The `Mcp-Param-*` headers the tool's definition (when listed) asks for, from the call's arguments.
    fn param_headers(&self, name: &str, arguments: &Json) -> Vec<(String, String)> {
        let mut out = Vec::new();
        if let Some(tool) = self.tools.iter().flatten().find(|t| t.name == name) {
            mirrored(&tool.input_schema, arguments, &mut out, 0);
        }
        out
    }

    fn id(&mut self) -> u64 {
        self.next += 1;
        self.next
    }

    /// Sends `method`, in the protocol the server speaks - found out on the way if not known yet.
    async fn ask(
        &mut self,
        method: &str,
        params: Map<String, Json>,
        name: Option<&str>,
        extra: Vec<(String, String)>,
        read: bool,
    ) -> Result<Asked, String> {
        // What the server speaks is changed once at most on the way: from what was remembered, or from the first guess.
        // When the other protocol's handshake fails too, the first refusal is the one that says what is wrong.
        let mut switched: Option<(u64, Json, String)> = None;
        for _ in 0..3 {
            let asked = match self.era.clone() {
                Era::Unknown | Era::Modern => self.modern(method, params.clone(), name, &extra, read).await?,
                Era::Legacy { .. } => self.legacy(method, params.clone(), read).await?,
            };
            let (reply, legacy) = match asked {
                Ok(done) => return Ok(done),
                Err(reply) => reply,
            };
            if [401, 403].contains(&reply.status) {
                return Err(format!(
                    "the MCP server refused the sign-in ({}): check the step's credential",
                    reply.status
                ));
            }
            let message = reply.message.unwrap_or(Json::Null);
            let code = wire::at(&message, "/error/code").as_i64();
            if code == Some(wire::UNSUPPORTED_VERSION) && !legacy {
                // A current server that does not speak this version: the earlier protocol, if it speaks one of those.
                let supported: Vec<&str> = wire::at(&message, "/error/data/supported")
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Json::as_str)
                    .collect();
                let Some(v) = LEGACY.iter().find(|v| supported.contains(v)) else {
                    return Err(format!(
                        "the MCP server speaks {}; OpenDuct speaks {}",
                        supported.join(", "),
                        wire::versions().join(", ")
                    ));
                };
                self.era = Era::Legacy {
                    version: (*v).to_owned(),
                    ready: false,
                    session: None,
                };
                continue;
            }
            // A refusal of the current protocol that only a current server would not give, or of the handshake by a
            // server remembered as an earlier one: the other protocol, once.
            let other_era = switched.is_none()
                && !self.confirmed
                && (400..500).contains(&reply.status)
                && (legacy
                    || !wire::modern_error(&message)
                        && !(reply.status == 404 && code == Some(wire::METHOD_NOT_FOUND)));
            if other_era {
                switched = Some((reply.status, message, reply.said));
                self.era = if legacy { Era::Unknown } else { self::legacy() };
                continue;
            }
            let (status, message, said) = match switched.take() {
                Some(first) if !self.confirmed => first,
                _ => (reply.status, message, reply.said),
            };
            return refused(status, &message, &said);
        }
        Err("the MCP server did not answer as either protocol version would".to_owned())
    }

    /// A request in the current protocol: its version, who asks and what it may do, in `_meta`; the method and
    /// the name also in headers. A result is the answer; anything else is the reply, to be looked at.
    async fn modern(
        &mut self,
        method: &str,
        mut params: Map<String, Json>,
        name: Option<&str>,
        extra: &[(String, String)],
        read: bool,
    ) -> Result<Result<Asked, (Reply, bool)>, String> {
        params.insert("_meta".to_owned(), wire::request_meta());
        let message = wire::request(Some(self.id()), method, Json::Object(params));
        let mut headers = vec![
            ("mcp-protocol-version".to_owned(), MODERN.to_owned()),
            ("mcp-method".to_owned(), method.to_owned()),
        ];
        if let Some(name) = name {
            headers.push(("mcp-name".to_owned(), wire::header_value(name)));
        }
        headers.extend(extra.iter().cloned());
        let reply = match self.post(&message, headers, read).await? {
            Posted::NotSent(shown) => return Ok(Ok(Asked::NotSent(shown))),
            Posted::Reply(r) => r,
        };
        match result_of(&reply)? {
            Some(result) => {
                if !self.confirmed {
                    remember(&self.server.url, false);
                }
                self.confirmed = true;
                self.era = Era::Modern;
                Ok(Ok(Asked::Answered(result)))
            }
            None => Ok(Err((reply, false))),
        }
    }

    /// A request in the earlier protocol: after the handshake, with the version agreed and the session given.
    async fn legacy(
        &mut self,
        method: &str,
        params: Map<String, Json>,
        read: bool,
    ) -> Result<Result<Asked, (Reply, bool)>, String> {
        for again in [false, true] {
            if let Era::Legacy { ready: false, .. } = self.era {
                match self.initialize().await? {
                    Ok(None) => {}
                    Ok(Some(shown)) => return Ok(Ok(Asked::NotSent(shown))),
                    Err(reply) => return Ok(Err((reply, true))),
                }
            }
            let Era::Legacy { version, session, .. } = self.era.clone() else {
                return Err("no session with the MCP server".to_owned());
            };
            let message = wire::request(Some(self.id()), method, Json::Object(params.clone()));
            let mut headers = vec![("mcp-protocol-version".to_owned(), version)];
            if let Some(s) = &session {
                headers.push(("mcp-session-id".to_owned(), s.clone()));
            }
            let reply = match self.post(&message, headers, read).await? {
                Posted::NotSent(shown) => return Ok(Ok(Asked::NotSent(shown))),
                Posted::Reply(r) => r,
            };
            // A session the server no longer has: a new one, once.
            if reply.status == 404 && session.is_some() && !again {
                self.era = legacy();
                continue;
            }
            return Ok(match result_of(&reply)? {
                Some(result) => Ok(Asked::Answered(result)),
                None => Err((reply, true)),
            });
        }
        Err("the MCP server's sessions keep ending".to_owned())
    }

    /// The earlier protocol's handshake: `initialize`, the version the server chose, its session, `initialized`.
    async fn initialize(&mut self) -> Result<Result<Option<Value>, Reply>, String> {
        let asked = match &self.era {
            Era::Legacy { version, .. } => version.clone(),
            _ => return Err("not the earlier protocol".to_owned()),
        };
        let message = wire::request(
            Some(self.id()),
            "initialize",
            json!({ "protocolVersion": asked, "capabilities": {}, "clientInfo": wire::implementation() }),
        );
        let reply = match self.post(&message, Vec::new(), true).await? {
            Posted::NotSent(shown) => return Ok(Ok(Some(shown))),
            Posted::Reply(r) => r,
        };
        let Some(result) = result_of(&reply)? else {
            return Ok(Err(reply));
        };
        let version = wire::at(&result, "/protocolVersion").as_str().unwrap_or_default();
        if !LEGACY.contains(&version) {
            return Err(format!(
                "the MCP server answered in version {version:?}, which OpenDuct does not speak ({})",
                wire::versions().join(", ")
            ));
        }
        let session = reply
            .headers
            .get("mcp-session-id")
            .and_then(Json::as_str)
            .filter(|s| !s.is_empty() && s.len() <= 256 && s.bytes().all(|b| (0x21..=0x7e).contains(&b)))
            .map(str::to_owned);
        self.era = Era::Legacy {
            version: version.to_owned(),
            ready: true,
            session: session.clone(),
        };
        self.confirmed = true;
        remember(&self.server.url, true);
        let mut headers = vec![("mcp-protocol-version".to_owned(), version.to_owned())];
        if let Some(s) = session {
            headers.push(("mcp-session-id".to_owned(), s));
        }
        // The server answers 202; what it answers does not change what comes next.
        let note = wire::request(None, "notifications/initialized", Json::Null);
        let _ = self.post(&note, headers, true).await?;
        Ok(Ok(None))
    }

    /// One message POSTed: the reply, or (a test run, a preview of a write) what would have been sent.
    async fn post(
        &self,
        message: &Json,
        headers: Vec<(String, String)>,
        read: bool,
    ) -> Result<Posted, String> {
        let mut request = HttpRequest {
            read,
            method: "POST".to_owned(),
            url: self.server.url.clone(),
            headers: vec![(
                "accept".to_owned(),
                "application/json, text/event-stream".to_owned(),
            )],
            query: Vec::new(),
            body: Some(("application/json".to_owned(), message.to_string().into_bytes())),
            timeout: self.server.timeout,
        };
        request.headers.extend(headers);
        if let Some(c) = &self.server.credential {
            crate::apps::authorize(&mut request, c)?;
        }
        let answer = self.effects.http(request).await?;
        let Value::Record(fields) = &answer else {
            return Err("the MCP server gave no answer".to_owned());
        };
        if fields.get("test").is_some() {
            return Ok(Posted::NotSent(answer));
        }
        let as_json = |v: Option<&Value>| {
            v.and_then(|v| serde_json::from_str::<Json>(&v.to_json()).ok())
                .unwrap_or(Json::Null)
        };
        let status = as_json(fields.get("status")).as_u64().unwrap_or(0);
        let headers = match as_json(fields.get("headers")) {
            Json::Object(h) => h,
            _ => Map::new(),
        };
        let body = as_json(fields.get("body"));
        let id = message.get("id");
        let (message, said) = match body {
            Json::Object(_) => (Some(body), String::new()),
            Json::String(text) => {
                let streamed = headers
                    .get("content-type")
                    .and_then(Json::as_str)
                    .is_some_and(|c| c.contains("text/event-stream"));
                let found = if streamed {
                    wire::events(&text).into_iter().find(|m| {
                        m.get("id") == id && (m.get("result").is_some() || m.get("error").is_some())
                    })
                } else {
                    serde_json::from_str::<Json>(text.trim())
                        .ok()
                        .filter(Json::is_object)
                };
                (found, text)
            }
            _ => (None, String::new()),
        };
        Ok(Posted::Reply(Reply {
            status,
            headers,
            message,
            said,
        }))
    }
}

enum Posted {
    NotSent(Value),
    Reply(Reply),
}

/// The result a reply holds, when it is one that completed; `None` when it holds none.
fn result_of(reply: &Reply) -> Result<Option<Json>, String> {
    let Some(result) = reply.message.as_ref().and_then(|m| m.get("result")) else {
        return Ok(None);
    };
    if !(200..300).contains(&reply.status) {
        return Ok(None);
    }
    match result.get("resultType").and_then(Json::as_str) {
        None | Some("complete") => Ok(Some(result.clone())),
        Some("input_required") => Err(
            "the MCP server asks for more input (a person's answer), which a workflow cannot give".to_owned(),
        ),
        Some(other) => Err(format!(
            "the MCP server answered with a result of a kind OpenDuct does not know ({other})"
        )),
    }
}

/// A refusal as the caller is told it: the server's JSON-RPC error, or what it answered.
fn refused(status: u64, message: &Json, said: &str) -> Result<Asked, String> {
    match wire::at(message, "/error/code").as_i64() {
        Some(code) => Ok(Asked::Refused {
            code,
            message: wire::at(message, "/error/message")
                .as_str()
                .unwrap_or("no reason given")
                .chars()
                .take(500)
                .collect(),
        }),
        None if said.trim().is_empty() => Err(format!("the MCP server answered {status}")),
        None => Err(format!(
            "the MCP server answered {status}: {}",
            said.trim().chars().take(300).collect::<String>()
        )),
    }
}

impl ToolInfo {
    fn read(tool: &Json) -> Result<Self, String> {
        let name = tool["name"]
            .as_str()
            .filter(|n| !n.is_empty() && n.len() <= 128 && !n.chars().any(char::is_control))
            .ok_or("a tool without a usable name")?;
        let schema = tool
            .get("inputSchema")
            .filter(|s| s.is_object())
            .ok_or("no input schema")?;
        if !headers_ok(schema) {
            return Err("its x-mcp-header settings break the protocol's rules".to_owned());
        }
        let mut description = tool["description"].as_str().unwrap_or_default().to_owned();
        if description.len() > MAX_TOOL_DESCRIPTION {
            let mut cut = MAX_TOOL_DESCRIPTION;
            while !description.is_char_boundary(cut) {
                cut -= 1;
            }
            description.truncate(cut);
        }
        Ok(Self {
            name: name.to_owned(),
            title: tool["title"].as_str().map(str::to_owned),
            description,
            input_schema: schema.clone(),
            annotations: tool.get("annotations").cloned().unwrap_or(Json::Null),
            output_schema: tool
                .get("outputSchema")
                .filter(|s| s.is_object())
                .cloned()
                .unwrap_or(Json::Null), // app generator
        })
    }
}

/// Whether a schema's `x-mcp-header` settings are as the protocol says: each on a property reached from the top
/// through `properties` alone, of type string, integer or boolean, a header name's characters, none twice.
fn headers_ok(schema: &Json) -> bool {
    fn walk(node: &Json, reachable: bool, property: bool, depth: usize, seen: &mut Vec<String>) -> bool {
        if depth > 32 {
            return false;
        }
        match node {
            Json::Object(fields) => {
                if let Some(header) = fields.get("x-mcp-header") {
                    let Some(h) = header.as_str() else {
                        return false;
                    };
                    let token = !h.is_empty()
                        && h.bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
                    let lower = h.to_ascii_lowercase();
                    if !reachable
                        || !property
                        || !token
                        || seen.contains(&lower)
                        || !matches!(fields["type"].as_str(), Some("string" | "integer" | "boolean"))
                    {
                        return false;
                    }
                    seen.push(lower);
                }
                fields.iter().all(|(k, v)| match (k.as_str(), v) {
                    ("x-mcp-header", _) => true,
                    ("properties", Json::Object(props)) => {
                        props.values().all(|p| walk(p, reachable, true, depth + 1, seen))
                    }
                    _ => walk(v, false, false, depth + 1, seen),
                })
            }
            Json::Array(items) => items.iter().all(|v| walk(v, false, false, depth + 1, seen)),
            _ => true,
        }
    }
    walk(schema, true, false, 0, &mut Vec::new())
}

/// The `Mcp-Param-*` headers for the properties a schema marks `x-mcp-header`, from the arguments at their paths.
fn mirrored(schema: &Json, arguments: &Json, out: &mut Vec<(String, String)>, depth: usize) {
    let Some(props) = schema["properties"].as_object() else {
        return;
    };
    if depth > 32 {
        return;
    }
    for (name, p) in props {
        let value = &arguments[name.as_str()];
        if let Some(header) = p["x-mcp-header"].as_str() {
            let text = match value {
                Json::String(s) => Some(s.clone()),
                Json::Bool(b) => Some(b.to_string()),
                Json::Number(n) if n.is_i64() || n.is_u64() => Some(n.to_string()),
                _ => None,
            };
            if let Some(text) = text {
                out.push((format!("mcp-param-{header}"), wire::header_value(&text)));
            }
        }
        if value.is_object() {
            mirrored(p, value, out, depth + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn x_mcp_header_settings_are_kept_only_where_the_protocol_allows_them() {
        let good = json!({
            "type": "object",
            "properties": {
                "region": { "type": "string", "x-mcp-header": "Region" },
                "where": { "type": "object", "properties": { "zone": { "type": "integer", "x-mcp-header": "Zone" } } },
                "query": { "type": "string" },
            },
        });
        assert!(headers_ok(&good));
        let mut out = Vec::new();
        mirrored(
            &good,
            &json!({ "region": "Hello, 世界", "where": { "zone": 7 }, "query": "x" }),
            &mut out,
            0,
        );
        assert_eq!(
            out,
            vec![
                (
                    "mcp-param-Region".to_owned(),
                    "=?base64?SGVsbG8sIOS4lueVjA==?=".to_owned()
                ),
                ("mcp-param-Zone".to_owned(), "7".to_owned()),
            ]
        );
        for bad in [
            json!({ "properties": { "n": { "type": "number", "x-mcp-header": "N" } } }),
            json!({ "properties": { "a": { "type": "string", "x-mcp-header": "" } } }),
            json!({ "properties": { "a": { "type": "string", "x-mcp-header": "A B" } } }),
            json!({ "properties": { "a": { "type": "string", "x-mcp-header": "A" }, "b": { "type": "string", "x-mcp-header": "a" } } }),
            json!({ "properties": { "l": { "type": "array", "items": { "type": "string", "x-mcp-header": "L" } } } }),
            json!({ "anyOf": [{ "properties": { "a": { "type": "string", "x-mcp-header": "A" } } }] }),
        ] {
            assert!(!headers_ok(&bad), "{bad}");
        }
    }

    #[test]
    fn a_tools_answer_is_its_structured_content_its_json_or_its_text() {
        let structured = ToolAnswer::read(&json!({
            "content": [{ "type": "text", "text": "{\"t\": 22}" }], "structuredContent": { "t": 22 },
        }));
        assert_eq!(structured.value(), json!({ "t": 22 }));
        let text_json = ToolAnswer::read(&json!({ "content": [{ "type": "text", "text": "[1, 2]" }] }));
        assert_eq!(text_json.value(), json!([1, 2]));
        let text = ToolAnswer::read(&json!({
            "content": [{ "type": "text", "text": "Sunny" }, { "type": "image", "data": "", "mimeType": "image/png" }, { "type": "text", "text": "22C" }],
            "isError": true,
        }));
        assert_eq!(text.value(), json!("Sunny\n22C"));
        assert!(text.is_error);
        assert_eq!(text.content.len(), 3);
    }

    #[test]
    fn a_tool_without_a_name_or_a_schema_is_left_out() {
        assert!(ToolInfo::read(&json!({ "name": "a", "inputSchema": { "type": "object" } })).is_ok());
        assert!(ToolInfo::read(&json!({ "name": "", "inputSchema": { "type": "object" } })).is_err());
        assert!(ToolInfo::read(&json!({ "name": "a" })).is_err());
        assert!(ToolInfo::read(&json!({ "name": "a", "inputSchema": null })).is_err());
        let long = ToolInfo::read(
            &json!({ "name": "a", "description": "é".repeat(MAX_TOOL_DESCRIPTION), "inputSchema": {} }),
        )
        .unwrap();
        assert!(long.description.len() <= MAX_TOOL_DESCRIPTION);
    }
}

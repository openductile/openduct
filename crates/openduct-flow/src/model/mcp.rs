// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Reading an `mcp` step (see [`crate::mcp`]): the server's address, its credential, the tool and its arguments.

use serde_json::{Map, Value as Json};

use super::{Kind, Reader, duration, text};
use crate::mcp::McpStep;
use crate::mcp::step::{MAX_MCP_TIME, MCP_TIME};

const SETTINGS: &[&str] = &[
    "url",
    "credential",
    "tool",
    "arguments",
    "timeout",
    "read_only",
    "fail_on_error",
];

fn flag(s: &Map<String, Json>, name: &str, at: &str, default: bool) -> Result<bool, String> {
    match s.get(name) {
        None | Some(Json::Null) => Ok(default),
        Some(Json::Bool(b)) => Ok(*b),
        Some(_) => Err(format!("{at}.{name}: true or false")),
    }
}

impl Reader {
    pub(super) fn mcp(&self, body: &Json, at: &str) -> Result<Kind, String> {
        let s = body
            .as_object()
            .ok_or_else(|| format!("{at}: a set of settings: url, tool, arguments"))?;
        if let Some(k) = s.keys().find(|k| !SETTINGS.contains(&k.as_str())) {
            return Err(format!(
                "{at}.{k}: not a setting of mcp ({})",
                SETTINGS.join(", ")
            ));
        }
        let address = s
            .get("url")
            .ok_or_else(|| format!("{at}.url: the MCP server's address, as https://tools.example.com/mcp"))?;
        if let Some(written) = address.as_str()
            && !written.contains("{{")
            && !(written.starts_with("https://") || written.starts_with("http://"))
        {
            return Err(format!(
                "{at}.url: an http or https address, as https://tools.example.com/mcp"
            ));
        }
        let url = self.template(address, &format!("{at}.url"))?;
        let credential = match s.get("credential") {
            None | Some(Json::Null) => None,
            Some(Json::String(c)) if c.trim().is_empty() => None,
            Some(Json::String(c)) => Some(c.clone()),
            Some(_) => return Err(format!("{at}.credential: the credential's name")),
        };
        let tool = match s.get("tool") {
            None | Some(Json::Null) => None,
            Some(Json::String(t)) if t.trim().is_empty() => None,
            Some(t) => Some(self.template(t, &format!("{at}.tool"))?),
        };
        let arguments = match s.get("arguments") {
            None | Some(Json::Null) => None,
            Some(a @ (Json::Object(_) | Json::String(_))) => Some(self.tree(a, &format!("{at}.arguments"))?),
            Some(_) => {
                return Err(format!(
                    "{at}.arguments: a record of the tool's arguments, each a value or text with {{{{ }}}}"
                ));
            }
        };
        if tool.is_none() && arguments.is_some() {
            return Err(format!(
                "{at}.tool: the tool these arguments are for (without a tool, the step lists the server's tools)"
            ));
        }
        let timeout = match text(s.get("timeout")) {
            None => MCP_TIME,
            Some(t) => duration(t)
                .filter(|d| !d.is_zero() && *d <= MAX_MCP_TIME)
                .ok_or_else(|| format!("{at}.timeout: as 30s, at most 5m"))?,
        };
        Ok(Kind::Mcp(McpStep {
            url,
            credential,
            tool,
            arguments,
            timeout,
            read_only: flag(s, "read_only", at, false)?,
            fail_on_error: flag(s, "fail_on_error", at, true)?,
        }))
    }
}

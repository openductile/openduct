// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! MCP, the Model Context Protocol: how AI assistants find and call tools. OpenDuct speaks it both ways - an `mcp`
//! step calls a tool of any MCP server, and the studio serves each workspace's workflows marked as tools
//! ([`crate::Tool`]) to assistants as an MCP server of its own.
//!
//! The protocol is JSON-RPC 2.0 over Streamable HTTP, version [`wire::MODERN`] first, the earlier versions of
//! [`wire::LEGACY`] (with their `initialize` handshake) when a server speaks only those.
//!
//! # Using a server's tools from code (an agent step's tools)
//!
//! A [`Session`] talks to one server through a run's effects, so it obeys what any HTTP request of a run obeys. An
//! agent offers each [`ToolInfo`] of [`Session::tools`] to its model (its `name`, `description` and `input_schema`
//! are what a model's tool definition takes), and when the model picks one, answers it with [`Session::call`]:
//!
//! ```no_run
//! # async fn agent(effects: &dyn openduct_flow::Effects) -> Result<(), String> {
//! use openduct_flow::mcp::{Reached, Server, Session};
//! let server = Server { url: "https://tools.example.com/mcp".into(), credential: None, timeout: std::time::Duration::from_secs(30) };
//! let mut session = Session::new(effects, server);
//! if let Reached::Answered(tools) = session.tools().await? {
//!     // ... the model chooses tools[0] and fills its arguments ...
//!     let arguments = serde_json::json!({ "city": "Lisbon" });
//!     if let Reached::Answered(answer) = session.call(&tools[0].name, &arguments, false).await? {
//!         let _for_the_model = (answer.is_error, answer.value());
//!     }
//! }
//! session.close().await;
//! # Ok(()) }
//! ```
//!
//! A test run sends nothing, and a preview sends no call of a tool not marked as only reading: each answers
//! [`Reached::NotSent`] with the request it would have sent, for the step's record.

mod client;
pub(crate) mod step;
pub mod wire;

pub use client::{MAX_TOOLS, Reached, Server, Session, ToolAnswer, ToolInfo};
pub use step::McpStep;

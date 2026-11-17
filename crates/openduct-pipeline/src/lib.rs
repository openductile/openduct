// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct pipelines: read a topic, change each message with expressions, write another topic. This crate reads the
//! pipeline file and works out what each message becomes; `openduct pipeline` moves the messages. See
//! `docs/pipelines.md`.

#![forbid(unsafe_code)]

mod config;
mod process;

pub use config::{Connector, Format, Pipeline, Setting, Start};
pub mod secrets;
pub use process::{Failure, Message, Output, test_lines};

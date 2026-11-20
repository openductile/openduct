// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! OpenDuct's schema registry (see `registry` for what it keeps, `api` for the REST it answers).

pub mod api;
pub mod avro;
pub mod http;
pub mod json_schema;
pub mod log;
pub mod protobuf;
pub mod registry;
pub mod service;

// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Stand-ins for database servers, speaking their protocols as far as the SQL steps use them.

#![allow(dead_code, unreachable_pub)] // each test file uses some of them

pub mod mysql;
pub mod tds;

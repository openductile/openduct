// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Doors for the hostile-input tests (`crates/openduct-hostile` and `fuzz/`), not an API: each reads bytes as a
//! protocol a connector reads from the other side - the systems a pipeline connects to are not to be trusted any
//! more than its clients - by the same code the connector uses, without the network. Each must return for any bytes,
//! without a panic and without taking more memory than the bytes justify.

use bytes::Bytes;

/// Packets from an MQTT broker, as the `mqtt` connector's session takes them.
pub fn mqtt(bytes: &[u8]) {
    crate::mqtt::replay(bytes, crate::mqtt::LARGEST_PACKET);
}

/// Replication messages from PostgreSQL (`postgres_cdc`): frames, changes, and the messages they become.
pub fn postgres_replication(chunks: Vec<Bytes>) {
    crate::postgres::replay(chunks);
}

/// Binary log events from MySQL (`mysql_cdc`), with or without their CRC32: events, tables, rows and messages.
pub fn mysql_binlog(checksum: bool, chunks: Vec<Bytes>) {
    crate::mysql::replay(checksum, chunks);
}

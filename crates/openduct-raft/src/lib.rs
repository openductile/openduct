// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Replication for OpenDuct: several brokers keep the same data, so a broker can be lost without losing what was
//! written, and clients carry on with the others.
//!
//! Brokers form Raft groups (with [openraft](https://docs.rs/openraft)): the **controller**, over every broker, holds
//! the cluster's state - topics and where their partitions live, producer ids, consumer groups' committed offsets -
//! and each **partition** is a group of its own over the brokers holding it, so partitions' leaders, and the work,
//! spread over the brokers. Every change is a [`Command`] in a group's log, answered once most of the group holds it
//! on disk (Kafka's `acks=all`, with the quorum of a Raft group), and applied in the same order by every member.
//!
//! - [`log`]: a group's Raft log on this broker's disk; [`flush`]: one flusher for all of them.
//! - [`controller`] and [`partition`]: the two kinds of state machine; [`snapshot`]: their snapshots on disk.
//! - [`net`]: how brokers reach each other; [`members`]: the groups a broker takes part in.
//! - [`replica`]: a broker's replicated store, as the broker uses it.

#![forbid(unsafe_code)]

pub mod controller;
pub mod features;
pub mod flush;
pub mod log;
pub mod members;
pub mod net;
pub mod offsets;
pub mod partition;
pub mod replica;
pub mod snapshot;
pub(crate) mod stable;
pub mod tier;
pub mod tls;
pub mod types;

pub use members::Readiness;
pub use net::ClusterAuth;
pub use replica::{Replica, ReplicaConfig, Timing};
pub use tier::{S3, S3Settings, TierConfig};
pub use tls::ReplicationTls;
pub use types::{Broker, Command, GroupId, Refusal, Reply, TypeConfig};

// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! openraft's own checks of a log store and a state machine, run against OpenDuct's: the Raft log with the
//! partition's state machine, and with the controller's.

// A test file: a failed step should stop the test where it failed.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use openduct_raft::TypeConfig;
use openduct_raft::controller::ControllerMachine;
use openduct_raft::flush::Flusher;
use openduct_raft::log::LogStore;
use openduct_raft::partition::PartitionMachine;
use openduct_storage::FileStoreConfig;
use openraft::StorageError;
use openraft::testing::log::{StoreBuilder, Suite};

/// Small segments, so the suite's appends, truncations and purges cross segment boundaries.
fn log(dir: &tempfile::TempDir) -> LogStore {
    LogStore::open(dir.path().join("log"), 300, Flusher::start().unwrap()).unwrap()
}

struct Partition;

impl StoreBuilder<TypeConfig, LogStore, PartitionMachine, tempfile::TempDir> for Partition {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, LogStore, PartitionMachine), StorageError<TypeConfig>> {
        let dir = tempfile::tempdir().unwrap();
        let machine =
            PartitionMachine::open(dir.path().join("machine"), FileStoreConfig::default(), None).unwrap();
        let log = log(&dir);
        Ok((dir, log, machine))
    }
}

struct Controller;

impl StoreBuilder<TypeConfig, LogStore, ControllerMachine, tempfile::TempDir> for Controller {
    async fn build(
        &self,
    ) -> Result<(tempfile::TempDir, LogStore, ControllerMachine), StorageError<TypeConfig>> {
        let dir = tempfile::tempdir().unwrap();
        let machine = ControllerMachine::open(dir.path().join("machine")).unwrap();
        let log = log(&dir);
        Ok((dir, log, machine))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_and_a_partition_s_state_machine_pass_openraft_s_storage_suite() {
    Suite::test_all(Partition).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_log_and_the_controller_s_state_machine_pass_openraft_s_storage_suite() {
    Suite::test_all(Controller).await.unwrap();
}

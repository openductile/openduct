// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

//! Tiered storage: each partition's leader copies closed segments to object storage (S3) and records each one in the
//! partition's own log (`Command::Tiered`), so every copy knows what is there; once a segment is stored, local
//! copies past the local retention go (`Command::Offload`), and reads of their offsets come back from object
//! storage. Appends never wait on object storage, and a broker rebuilt from a snapshot fetches old segments only
//! when they are read.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use openduct_storage::{Remote, StoreError};
use tokio::runtime::Handle;

mod credentials;
mod s3;

pub use s3::{S3, S3Settings};

/// Tiered storage's settings.
#[derive(Debug, Clone)]
pub struct TierConfig {
    pub s3: Arc<S3>,
    /// Object keys start with this, then the topic's id and the partition.
    pub prefix: String,
    /// Each partition keeps local copies of stored segments up to this many bytes...
    pub local_retention_bytes: u64,
    /// ... and no older than this (by a segment's newest batch); `None`: by bytes only.
    pub local_retention_ms: Option<i64>,
    /// How often each leader looks for segments to store.
    pub every: Duration,
}

/// Object storage as a partition's log reads it back: blocking, off the async threads.
#[derive(Debug)]
pub(crate) struct Fetch {
    pub(crate) s3: Arc<S3>,
    pub(crate) runtime: Handle,
}

impl Remote for Fetch {
    fn get(&self, key: &str, dest: &Path) -> Result<(), StoreError> {
        let bytes = self
            .runtime
            .block_on(self.s3.get(key))
            .map_err(StoreError::Unavailable)?;
        std::fs::write(dest, bytes)?;
        Ok(())
    }
}

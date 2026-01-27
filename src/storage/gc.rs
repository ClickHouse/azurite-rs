//! Garbage collection for orphaned extents.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, info, warn};

use super::{ExtentStore, MetadataStore};

/// Garbage collector for cleaning up orphaned extents.
pub struct GarbageCollector {
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
    interval: Duration,
}

impl GarbageCollector {
    pub fn new(
        metadata: Arc<dyn MetadataStore>,
        extents: Arc<dyn ExtentStore>,
        interval: Duration,
    ) -> Self {
        Self {
            metadata,
            extents,
            interval,
        }
    }

    /// Starts the garbage collection loop.
    pub async fn run(&self) {
        let mut interval = time::interval(self.interval);

        loop {
            interval.tick().await;
            if let Err(e) = self.collect().await {
                warn!("Garbage collection failed: {}", e);
            }
        }
    }

    /// Performs a single garbage collection pass.
    pub async fn collect(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        debug!("Starting garbage collection");
        // In a full implementation, this would:
        // 1. Scan all blobs to find referenced extent IDs
        // 2. Scan all extents
        // 3. Delete extents not referenced by any blob
        //
        // For now, this is a placeholder that doesn't do anything
        // since we don't have a way to enumerate all extents.
        Ok(())
    }
}

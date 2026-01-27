//! Page blob data models.

use serde::{Deserialize, Serialize};

use super::blob::ExtentChunk;

/// A range of pages in a page blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRange {
    /// Start byte offset (inclusive).
    pub start: u64,
    /// End byte offset (inclusive).
    pub end: u64,
}

impl PageRange {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Returns the length of this page range.
    pub fn length(&self) -> u64 {
        self.end - self.start + 1
    }
}

/// Page range with clear indicator for diff operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageRangeDiff {
    /// Start byte offset (inclusive).
    pub start: u64,
    /// End byte offset (inclusive).
    pub end: u64,
    /// Whether this range was cleared (true) or modified (false).
    pub is_clear: bool,
}

impl PageRangeDiff {
    pub fn new(start: u64, end: u64, is_clear: bool) -> Self {
        Self { start, end, is_clear }
    }
}

/// Persistency representation of a page range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistencyPageRange {
    /// Start byte offset.
    pub start: u64,
    /// End byte offset.
    pub end: u64,
    /// Reference to extent data (None for cleared ranges).
    pub extent_chunk: Option<ExtentChunk>,
}

impl PersistencyPageRange {
    pub fn new(start: u64, end: u64, extent_chunk: Option<ExtentChunk>) -> Self {
        Self {
            start,
            end,
            extent_chunk,
        }
    }
}

/// Page blob constants.
pub const PAGE_SIZE: u64 = 512;
pub const MAX_PAGE_BLOB_SIZE: u64 = 8 * 1024 * 1024 * 1024 * 1024; // 8 TiB
pub const MAX_PAGE_RANGE_SIZE: u64 = 4 * 1024 * 1024; // 4 MiB per write operation

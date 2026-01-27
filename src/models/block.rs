//! Block data models for block blobs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::blob::ExtentChunk;

/// Block state in the block list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockState {
    /// Block has been staged but not committed.
    Uncommitted,
    /// Block has been committed.
    Committed,
    /// Block is in the latest committed block list.
    Latest,
}

impl BlockState {
    pub fn as_str(&self) -> &'static str {
        match self {
            BlockState::Uncommitted => "Uncommitted",
            BlockState::Committed => "Committed",
            BlockState::Latest => "Latest",
        }
    }
}

/// Block list type for GetBlockList operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BlockListType {
    Committed,
    Uncommitted,
    #[default]
    All,
}

impl BlockListType {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "committed" => BlockListType::Committed,
            "uncommitted" => BlockListType::Uncommitted,
            _ => BlockListType::All,
        }
    }
}

/// A staged (uncommitted) block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockModel {
    /// Account name.
    pub account: String,
    /// Container name.
    pub container: String,
    /// Blob name.
    pub blob: String,
    /// Block ID (base64 encoded).
    pub block_id: String,
    /// Block size in bytes.
    pub size: u64,
    /// Reference to extent data.
    pub extent_chunk: ExtentChunk,
    /// When the block was staged.
    pub staged_time: DateTime<Utc>,
}

impl BlockModel {
    /// Creates a new block model.
    pub fn new(
        account: String,
        container: String,
        blob: String,
        block_id: String,
        size: u64,
        extent_chunk: ExtentChunk,
    ) -> Self {
        Self {
            account,
            container,
            blob,
            block_id,
            size,
            extent_chunk,
            staged_time: Utc::now(),
        }
    }

    /// Returns the unique key for this block.
    pub fn key(&self) -> (String, String, String, String) {
        (
            self.account.clone(),
            self.container.clone(),
            self.blob.clone(),
            self.block_id.clone(),
        )
    }
}

/// Block entry in a committed block list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedBlock {
    pub block_id: String,
    pub size: u64,
}

/// Persistency representation of a block for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistencyBlock {
    pub name: String,
    pub size: u64,
    pub extent_chunk: ExtentChunk,
}

impl From<BlockModel> for PersistencyBlock {
    fn from(block: BlockModel) -> Self {
        Self {
            name: block.block_id,
            size: block.size,
            extent_chunk: block.extent_chunk,
        }
    }
}

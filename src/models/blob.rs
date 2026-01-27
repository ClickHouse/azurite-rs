//! Blob data models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Blob types supported by Azure Blob Storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobType {
    BlockBlob,
    PageBlob,
    AppendBlob,
}

impl BlobType {
    pub fn as_str(&self) -> &'static str {
        match self {
            BlobType::BlockBlob => "BlockBlob",
            BlobType::PageBlob => "PageBlob",
            BlobType::AppendBlob => "AppendBlob",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "BlockBlob" => Some(BlobType::BlockBlob),
            "PageBlob" => Some(BlobType::PageBlob),
            "AppendBlob" => Some(BlobType::AppendBlob),
            _ => None,
        }
    }
}

/// Access tiers for blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum AccessTier {
    #[default]
    Hot,
    Cool,
    Cold,
    Archive,
}

impl AccessTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            AccessTier::Hot => "Hot",
            AccessTier::Cool => "Cool",
            AccessTier::Cold => "Cold",
            AccessTier::Archive => "Archive",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "hot" => Some(AccessTier::Hot),
            "cool" => Some(AccessTier::Cool),
            "cold" => Some(AccessTier::Cold),
            "archive" => Some(AccessTier::Archive),
            _ => None,
        }
    }
}

/// Lease state for containers and blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LeaseState {
    #[default]
    Available,
    Leased,
    Expired,
    Breaking,
    Broken,
}

impl LeaseState {
    pub fn as_str(&self) -> &'static str {
        match self {
            LeaseState::Available => "available",
            LeaseState::Leased => "leased",
            LeaseState::Expired => "expired",
            LeaseState::Breaking => "breaking",
            LeaseState::Broken => "broken",
        }
    }
}

/// Lease status for containers and blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LeaseStatus {
    #[default]
    Unlocked,
    Locked,
}

impl LeaseStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            LeaseStatus::Unlocked => "unlocked",
            LeaseStatus::Locked => "locked",
        }
    }
}

/// Lease duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LeaseDuration {
    #[default]
    Infinite,
    Fixed,
}

impl LeaseDuration {
    pub fn as_str(&self) -> &'static str {
        match self {
            LeaseDuration::Infinite => "infinite",
            LeaseDuration::Fixed => "fixed",
        }
    }
}

/// Copy status for blob copy operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CopyStatus {
    Pending,
    Success,
    Aborted,
    Failed,
}

impl CopyStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            CopyStatus::Pending => "pending",
            CopyStatus::Success => "success",
            CopyStatus::Aborted => "aborted",
            CopyStatus::Failed => "failed",
        }
    }
}

/// Reference to data stored in an extent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtentChunk {
    /// UUID of the extent containing this data.
    pub id: String,
    /// Byte offset within the extent.
    pub offset: u64,
    /// Number of bytes.
    pub count: u64,
}

impl ExtentChunk {
    pub fn new(id: String, offset: u64, count: u64) -> Self {
        Self { id, offset, count }
    }
}

/// Blob properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobProperties {
    pub content_length: u64,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_md5: Option<String>,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub created_on: DateTime<Utc>,
    pub blob_type: BlobType,
    pub access_tier: AccessTier,
    pub lease_state: LeaseState,
    pub lease_status: LeaseStatus,
    pub lease_duration: Option<LeaseDuration>,
    pub lease_id: Option<String>,
    pub lease_expiry: Option<DateTime<Utc>>,
    pub lease_break_time: Option<DateTime<Utc>>,
    /// Sequence number for page blobs.
    pub sequence_number: Option<u64>,
    /// Committed block count for append blobs.
    pub committed_block_count: Option<u32>,
    /// Whether the append blob is sealed.
    pub is_sealed: Option<bool>,
    /// Server-side encryption status.
    pub server_encrypted: bool,
    /// Copy ID for ongoing/completed copy operations.
    pub copy_id: Option<String>,
    /// Copy source URL.
    pub copy_source: Option<String>,
    /// Copy status.
    pub copy_status: Option<CopyStatus>,
    /// Copy progress (bytes copied / total bytes).
    pub copy_progress: Option<String>,
    /// Copy completion time.
    pub copy_completion_time: Option<DateTime<Utc>>,
    /// Copy status description (for failed copies).
    pub copy_status_description: Option<String>,
    /// Blob version ID.
    pub version_id: Option<String>,
    /// Whether this is the current version.
    pub is_current_version: Option<bool>,
}

impl Default for BlobProperties {
    fn default() -> Self {
        let now = Utc::now();
        Self {
            content_length: 0,
            content_type: Some("application/octet-stream".to_string()),
            content_encoding: None,
            content_language: None,
            content_md5: None,
            content_disposition: None,
            cache_control: None,
            etag: format!("\"0x{}\"", uuid::Uuid::new_v4().simple()),
            last_modified: now,
            created_on: now,
            blob_type: BlobType::BlockBlob,
            access_tier: AccessTier::Hot,
            lease_state: LeaseState::Available,
            lease_status: LeaseStatus::Unlocked,
            lease_duration: None,
            lease_id: None,
            lease_expiry: None,
            lease_break_time: None,
            sequence_number: None,
            committed_block_count: None,
            is_sealed: None,
            server_encrypted: true,
            copy_id: None,
            copy_source: None,
            copy_status: None,
            copy_progress: None,
            copy_completion_time: None,
            copy_status_description: None,
            version_id: None,
            is_current_version: None,
        }
    }
}

impl BlobProperties {
    /// Creates new blob properties for the given blob type.
    pub fn new(blob_type: BlobType, content_length: u64) -> Self {
        let mut props = Self::default();
        props.blob_type = blob_type;
        props.content_length = content_length;

        match blob_type {
            BlobType::PageBlob => {
                props.sequence_number = Some(0);
            }
            BlobType::AppendBlob => {
                props.committed_block_count = Some(0);
                props.is_sealed = Some(false);
            }
            BlobType::BlockBlob => {}
        }

        props
    }

    /// Updates the ETag and last modified time.
    pub fn update_etag(&mut self) {
        self.etag = format!("\"0x{}\"", uuid::Uuid::new_v4().simple());
        self.last_modified = Utc::now();
    }
}

/// Complete blob model stored in metadata store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobModel {
    /// Account name.
    pub account: String,
    /// Container name.
    pub container: String,
    /// Blob name (full path).
    pub name: String,
    /// Snapshot timestamp (empty for base blob).
    pub snapshot: String,
    /// Blob properties.
    pub properties: BlobProperties,
    /// User-defined metadata.
    pub metadata: HashMap<String, String>,
    /// Tags for blob indexing.
    pub tags: HashMap<String, String>,
    /// References to extent data chunks.
    pub extent_chunks: Vec<ExtentChunk>,
    /// Whether the blob is soft-deleted.
    pub deleted: bool,
    /// Soft-delete expiry time.
    pub deleted_time: Option<DateTime<Utc>>,
    /// Remaining retention days after soft-delete.
    pub remaining_retention_days: Option<u32>,
}

impl BlobModel {
    /// Creates a new blob model.
    pub fn new(
        account: String,
        container: String,
        name: String,
        blob_type: BlobType,
        content_length: u64,
    ) -> Self {
        Self {
            account,
            container,
            name,
            snapshot: String::new(),
            properties: BlobProperties::new(blob_type, content_length),
            metadata: HashMap::new(),
            tags: HashMap::new(),
            extent_chunks: Vec::new(),
            deleted: false,
            deleted_time: None,
            remaining_retention_days: None,
        }
    }

    /// Returns the unique key for this blob.
    pub fn key(&self) -> (String, String, String, String) {
        (
            self.account.clone(),
            self.container.clone(),
            self.name.clone(),
            self.snapshot.clone(),
        )
    }

    /// Creates a snapshot of this blob.
    pub fn create_snapshot(&self) -> Self {
        let mut snapshot = self.clone();
        // Azure snapshot format: 2024-01-27T12:34:56.1234567Z (7 decimal places)
        let now = Utc::now();
        snapshot.snapshot = format!(
            "{}.{:07}Z",
            now.format("%Y-%m-%dT%H:%M:%S"),
            now.timestamp_subsec_nanos() / 100  // Convert nanoseconds to 100-nanosecond units (7 digits)
        );
        snapshot
    }
}

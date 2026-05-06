//! Extent store for blob data.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use uuid::Uuid;

use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::ExtentChunk;

/// Trait for extent (blob data) storage operations.
///
/// Extents are reference-counted: `write` returns an extent with refcount 1,
/// `add_ref` increments the count for sharing (snapshots, copies), and `delete`
/// decrements; the underlying data is freed when the count reaches zero.
#[async_trait]
pub trait ExtentStore: Send + Sync {
    /// Writes data to the extent store and returns an ExtentChunk reference.
    /// The returned extent has a refcount of 1.
    async fn write(&self, data: Bytes) -> StorageResult<ExtentChunk>;

    /// Reads data from the extent store.
    async fn read(&self, chunk: &ExtentChunk) -> StorageResult<Bytes>;

    /// Reads a range of data from the extent store.
    async fn read_range(
        &self,
        chunk: &ExtentChunk,
        offset: u64,
        count: u64,
    ) -> StorageResult<Bytes>;

    /// Decrements the refcount; deletes the extent when it reaches zero.
    /// Idempotent: calling on a missing extent is a no-op.
    async fn delete(&self, extent_id: &str) -> StorageResult<()>;

    /// Increments the refcount for an existing extent.
    /// Used when sharing data across blobs (snapshots, copies).
    async fn add_ref(&self, extent_id: &str) -> StorageResult<()>;

    /// Returns the total size of all extents.
    async fn total_size(&self) -> u64;
}

/// Number of shards for the extent store (must be power of 2).
const NUM_SHARDS: usize = 64;

/// One stored extent: data plus a refcount. The extent lives until its
/// refcount drops to zero, at which point it is removed from the shard.
struct ExtentEntry {
    data: Bytes,
    refcount: AtomicU32,
}

/// Sharded in-memory implementation of the extent store.
/// Uses multiple DashMaps to reduce lock contention.
pub struct MemoryExtentStore {
    /// Sharded extents - each shard handles a subset of extent IDs.
    shards: Vec<DashMap<Arc<str>, Arc<ExtentEntry>>>,
    /// Current total size in bytes.
    current_size: AtomicU64,
    /// Maximum size limit (0 = unlimited).
    size_limit: u64,
}

impl MemoryExtentStore {
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS).map(|_| DashMap::new()).collect();
        Self {
            shards,
            current_size: AtomicU64::new(0),
            size_limit: 0,
        }
    }

    pub fn with_limit(limit: u64) -> Self {
        let shards = (0..NUM_SHARDS).map(|_| DashMap::new()).collect();
        Self {
            shards,
            current_size: AtomicU64::new(0),
            size_limit: limit,
        }
    }

    /// Get the shard for a given extent ID.
    #[inline]
    fn get_shard(&self, extent_id: &str) -> &DashMap<Arc<str>, Arc<ExtentEntry>> {
        // Use a simple hash of the first few bytes of the UUID
        let hash = extent_id
            .bytes()
            .take(8)
            .fold(0usize, |acc, b| acc.wrapping_mul(31).wrapping_add(b as usize));
        &self.shards[hash % NUM_SHARDS]
    }
}

impl Default for MemoryExtentStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExtentStore for MemoryExtentStore {
    async fn write(&self, data: Bytes) -> StorageResult<ExtentChunk> {
        let size = data.len() as u64;

        // Check size limit
        if self.size_limit > 0 {
            let current = self.current_size.load(Ordering::Relaxed);
            if current + size > self.size_limit {
                return Err(StorageError::with_message(
                    ErrorCode::RequestBodyTooLarge,
                    "Storage limit exceeded",
                ));
            }
        }

        let extent_id = Uuid::new_v4().to_string();
        let extent_id_arc: Arc<str> = Arc::from(extent_id.as_str());

        let entry = Arc::new(ExtentEntry {
            data,
            refcount: AtomicU32::new(1),
        });

        let shard = self.get_shard(&extent_id);
        shard.insert(extent_id_arc, entry);
        self.current_size.fetch_add(size, Ordering::Relaxed);

        Ok(ExtentChunk::new(extent_id, 0, size))
    }

    async fn read(&self, chunk: &ExtentChunk) -> StorageResult<Bytes> {
        let shard = self.get_shard(&chunk.id);
        let entry = shard
            .get(chunk.id.as_str())
            .ok_or_else(|| StorageError::new(ErrorCode::InternalError))?;

        let start = chunk.offset as usize;
        let end = start + chunk.count as usize;

        if end > entry.data.len() {
            return Err(StorageError::new(ErrorCode::InternalError));
        }

        Ok(entry.data.slice(start..end))
    }

    async fn read_range(
        &self,
        chunk: &ExtentChunk,
        offset: u64,
        count: u64,
    ) -> StorageResult<Bytes> {
        let shard = self.get_shard(&chunk.id);
        let entry = shard
            .get(chunk.id.as_str())
            .ok_or_else(|| StorageError::new(ErrorCode::InternalError))?;

        let start = (chunk.offset + offset) as usize;
        let end = start + count as usize;

        if end > entry.data.len() {
            return Err(StorageError::new(ErrorCode::InternalError));
        }

        Ok(entry.data.slice(start..end))
    }

    async fn delete(&self, extent_id: &str) -> StorageResult<()> {
        let shard = self.get_shard(extent_id);
        // remove_if so that the refcount check and removal are atomic relative
        // to other delete/add_ref operations on this entry.
        let removed = shard.remove_if(extent_id, |_, entry| {
            // fetch_sub returns previous value; remove only when it was 1
            // (i.e. this delete call is the last reference).
            entry.refcount.fetch_sub(1, Ordering::AcqRel) == 1
        });
        if let Some((_, entry)) = removed {
            self.current_size
                .fetch_sub(entry.data.len() as u64, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn add_ref(&self, extent_id: &str) -> StorageResult<()> {
        let shard = self.get_shard(extent_id);
        let entry = shard
            .get(extent_id)
            .ok_or_else(|| StorageError::new(ErrorCode::InternalError))?;
        entry.refcount.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn total_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }
}

/// Per-extent metadata held by the file-system store.
struct FsExtentMeta {
    size: u64,
    refcount: AtomicU32,
}

/// File system implementation of the extent store.
pub struct FsExtentStore {
    /// Base directory for extent files.
    base_path: PathBuf,
    /// Metadata for extents (size and refcount).
    extent_meta: DashMap<Arc<str>, Arc<FsExtentMeta>>,
    /// Current total size in bytes.
    current_size: AtomicU64,
}

impl FsExtentStore {
    pub async fn new(base_path: PathBuf) -> StorageResult<Self> {
        fs::create_dir_all(&base_path).await.map_err(|e| {
            StorageError::with_message(
                ErrorCode::InternalError,
                format!("Failed to create extent directory: {}", e),
            )
        })?;

        Ok(Self {
            base_path,
            extent_meta: DashMap::new(),
            current_size: AtomicU64::new(0),
        })
    }

    fn extent_path(&self, extent_id: &str) -> PathBuf {
        self.base_path.join(extent_id)
    }
}

#[async_trait]
impl ExtentStore for FsExtentStore {
    async fn write(&self, data: Bytes) -> StorageResult<ExtentChunk> {
        let size = data.len() as u64;
        let extent_id = Uuid::new_v4().to_string();
        let path = self.extent_path(&extent_id);

        let mut file = fs::File::create(&path).await.map_err(|e| {
            StorageError::with_message(
                ErrorCode::InternalError,
                format!("Failed to create extent file: {}", e),
            )
        })?;

        file.write_all(&data).await.map_err(|e| {
            StorageError::with_message(
                ErrorCode::InternalError,
                format!("Failed to write extent data: {}", e),
            )
        })?;

        let extent_id_arc: Arc<str> = Arc::from(extent_id.as_str());
        self.extent_meta.insert(
            extent_id_arc,
            Arc::new(FsExtentMeta {
                size,
                refcount: AtomicU32::new(1),
            }),
        );
        self.current_size.fetch_add(size, Ordering::Relaxed);

        Ok(ExtentChunk::new(extent_id, 0, size))
    }

    async fn read(&self, chunk: &ExtentChunk) -> StorageResult<Bytes> {
        self.read_range(chunk, 0, chunk.count).await
    }

    async fn read_range(
        &self,
        chunk: &ExtentChunk,
        offset: u64,
        count: u64,
    ) -> StorageResult<Bytes> {
        let path = self.extent_path(&chunk.id);

        let mut file = fs::File::open(&path).await.map_err(|e| {
            StorageError::with_message(
                ErrorCode::InternalError,
                format!("Failed to open extent file: {}", e),
            )
        })?;

        let start = chunk.offset + offset;
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|e| {
                StorageError::with_message(
                    ErrorCode::InternalError,
                    format!("Failed to seek in extent file: {}", e),
                )
            })?;

        let mut buffer = vec![0u8; count as usize];
        file.read_exact(&mut buffer).await.map_err(|e| {
            StorageError::with_message(
                ErrorCode::InternalError,
                format!("Failed to read extent data: {}", e),
            )
        })?;

        Ok(Bytes::from(buffer))
    }

    async fn delete(&self, extent_id: &str) -> StorageResult<()> {
        let removed = self.extent_meta.remove_if(extent_id, |_, meta| {
            meta.refcount.fetch_sub(1, Ordering::AcqRel) == 1
        });
        if let Some((_, meta)) = removed {
            self.current_size.fetch_sub(meta.size, Ordering::Relaxed);
            let path = self.extent_path(extent_id);
            fs::remove_file(&path).await.ok(); // Ignore errors if file doesn't exist
        }
        Ok(())
    }

    async fn add_ref(&self, extent_id: &str) -> StorageResult<()> {
        let meta = self
            .extent_meta
            .get(extent_id)
            .ok_or_else(|| StorageError::new(ErrorCode::InternalError))?;
        meta.refcount.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn total_size(&self) -> u64 {
        self.current_size.load(Ordering::Relaxed)
    }
}

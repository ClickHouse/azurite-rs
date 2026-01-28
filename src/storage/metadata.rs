//! Metadata store for containers, blobs, and blocks.

use async_trait::async_trait;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{BlobModel, BlockModel, ContainerModel, ServiceProperties};

/// Trait for metadata storage operations.
#[async_trait]
pub trait MetadataStore: Send + Sync {
    // Container operations
    async fn create_container(&self, container: ContainerModel) -> StorageResult<()>;
    async fn get_container(&self, account: &str, name: &str) -> StorageResult<ContainerModel>;
    async fn update_container(&self, container: ContainerModel) -> StorageResult<()>;
    async fn delete_container(&self, account: &str, name: &str) -> StorageResult<()>;
    async fn list_containers(
        &self,
        account: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        maxresults: Option<u32>,
    ) -> StorageResult<(Vec<ContainerModel>, Option<String>)>;
    async fn container_exists(&self, account: &str, name: &str) -> bool;

    // Blob operations
    async fn create_blob(&self, blob: BlobModel) -> StorageResult<()>;
    async fn get_blob(
        &self,
        account: &str,
        container: &str,
        name: &str,
        snapshot: &str,
    ) -> StorageResult<BlobModel>;
    async fn update_blob(&self, blob: BlobModel) -> StorageResult<()>;
    async fn delete_blob(
        &self,
        account: &str,
        container: &str,
        name: &str,
        snapshot: &str,
    ) -> StorageResult<()>;
    async fn list_blobs(
        &self,
        account: &str,
        container: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        marker: Option<&str>,
        maxresults: Option<u32>,
        include_snapshots: bool,
        include_deleted: bool,
    ) -> StorageResult<(Vec<BlobModel>, Vec<String>, Option<String>)>;
    async fn blob_exists(
        &self,
        account: &str,
        container: &str,
        name: &str,
        snapshot: &str,
    ) -> bool;

    // Block operations
    async fn stage_block(&self, block: BlockModel) -> StorageResult<()>;
    async fn get_staged_blocks(
        &self,
        account: &str,
        container: &str,
        blob: &str,
    ) -> StorageResult<Vec<BlockModel>>;
    async fn get_staged_block(
        &self,
        account: &str,
        container: &str,
        blob: &str,
        block_id: &str,
    ) -> StorageResult<BlockModel>;
    async fn delete_staged_blocks(
        &self,
        account: &str,
        container: &str,
        blob: &str,
    ) -> StorageResult<()>;

    // Service properties
    async fn get_service_properties(&self, account: &str) -> StorageResult<ServiceProperties>;
    async fn set_service_properties(
        &self,
        account: &str,
        properties: ServiceProperties,
    ) -> StorageResult<()>;
}

/// Key type for containers - uses Arc<str> to avoid allocations.
type ContainerKey = (Arc<str>, Arc<str>);

/// Key type for blobs - uses Arc<str> to avoid allocations.
type BlobKey = (Arc<str>, Arc<str>, Arc<str>, Arc<str>);

/// Key type for blocks - uses Arc<str> to avoid allocations.
type BlockKey = (Arc<str>, Arc<str>, Arc<str>, Arc<str>);

/// In-memory implementation of the metadata store with optimized concurrent access.
pub struct MemoryMetadataStore {
    /// Containers indexed by (account, name).
    containers: DashMap<ContainerKey, ContainerModel>,

    /// Blobs indexed by (account, container, name, snapshot).
    blobs: DashMap<BlobKey, BlobModel>,

    /// Secondary index: account+container -> set of blob names (for faster listing).
    blob_index: DashMap<(Arc<str>, Arc<str>), HashSet<Arc<str>>>,

    /// Staged (uncommitted) blocks indexed by (account, container, blob, block_id).
    blocks: DashMap<BlockKey, BlockModel>,

    /// Secondary index: account+container+blob -> set of block_ids.
    block_index: DashMap<(Arc<str>, Arc<str>, Arc<str>), HashSet<Arc<str>>>,

    /// Service properties indexed by account.
    service_properties: DashMap<Arc<str>, ServiceProperties>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self {
            containers: DashMap::new(),
            blobs: DashMap::new(),
            blob_index: DashMap::new(),
            blocks: DashMap::new(),
            block_index: DashMap::new(),
            service_properties: DashMap::new(),
        }
    }

    /// Create an Arc<str> key from a string slice.
    #[inline]
    fn arc_str(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    /// Create a container key.
    #[inline]
    fn container_key(account: &str, name: &str) -> ContainerKey {
        (Self::arc_str(account), Self::arc_str(name))
    }

    /// Create a blob key.
    #[inline]
    fn blob_key(account: &str, container: &str, name: &str, snapshot: &str) -> BlobKey {
        (
            Self::arc_str(account),
            Self::arc_str(container),
            Self::arc_str(name),
            Self::arc_str(snapshot),
        )
    }

    /// Create a block key.
    #[inline]
    fn block_key(account: &str, container: &str, blob: &str, block_id: &str) -> BlockKey {
        (
            Self::arc_str(account),
            Self::arc_str(container),
            Self::arc_str(blob),
            Self::arc_str(block_id),
        )
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    async fn create_container(&self, container: ContainerModel) -> StorageResult<()> {
        let key = Self::container_key(&container.account, &container.name);
        if self.containers.contains_key(&key) {
            return Err(StorageError::new(ErrorCode::ContainerAlreadyExists));
        }
        self.containers.insert(key, container);
        Ok(())
    }

    async fn get_container(&self, account: &str, name: &str) -> StorageResult<ContainerModel> {
        let key = Self::container_key(account, name);
        self.containers
            .get(&key)
            .map(|c| c.value().clone())
            .ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))
    }

    async fn update_container(&self, container: ContainerModel) -> StorageResult<()> {
        let key = Self::container_key(&container.account, &container.name);
        if !self.containers.contains_key(&key) {
            return Err(StorageError::new(ErrorCode::ContainerNotFound));
        }
        self.containers.insert(key, container);
        Ok(())
    }

    async fn delete_container(&self, account: &str, name: &str) -> StorageResult<()> {
        let key = Self::container_key(account, name);
        self.containers
            .remove(&key)
            .map(|_| ())
            .ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))
    }

    async fn list_containers(
        &self,
        account: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        maxresults: Option<u32>,
    ) -> StorageResult<(Vec<ContainerModel>, Option<String>)> {
        let maxresults = maxresults.unwrap_or(5000) as usize;
        let account_arc = Self::arc_str(account);

        // Collect matching container names first (just the keys, minimal lock time)
        let mut matching_names: Vec<Arc<str>> = self
            .containers
            .iter()
            .filter_map(|entry| {
                let (acct, name) = entry.key();
                if acct.as_ref() != account_arc.as_ref() {
                    return None;
                }
                if entry.value().deleted {
                    return None;
                }
                if let Some(p) = prefix {
                    if !name.starts_with(p) {
                        return None;
                    }
                }
                if let Some(m) = marker {
                    if name.as_ref() <= m {
                        return None;
                    }
                }
                Some(name.clone())
            })
            .collect();

        // Sort names
        matching_names.sort();

        // Truncate to maxresults + 1 to check if there are more
        matching_names.truncate(maxresults + 1);

        // Now fetch the actual containers (separate lock acquisitions)
        let has_more = matching_names.len() > maxresults;
        if has_more {
            matching_names.pop();
        }

        let mut containers = Vec::with_capacity(matching_names.len());
        for name in &matching_names {
            let key = (account_arc.clone(), name.clone());
            if let Some(c) = self.containers.get(&key) {
                containers.push(c.value().clone());
            }
        }

        let next_marker = if has_more {
            matching_names.last().map(|n| n.to_string())
        } else {
            None
        };

        Ok((containers, next_marker))
    }

    async fn container_exists(&self, account: &str, name: &str) -> bool {
        let key = Self::container_key(account, name);
        self.containers
            .get(&key)
            .map(|c| !c.deleted)
            .unwrap_or(false)
    }

    async fn create_blob(&self, blob: BlobModel) -> StorageResult<()> {
        let key = Self::blob_key(&blob.account, &blob.container, &blob.name, &blob.snapshot);
        let index_key = (Self::arc_str(&blob.account), Self::arc_str(&blob.container));
        let blob_name = Self::arc_str(&blob.name);

        // Update the secondary index
        self.blob_index
            .entry(index_key)
            .or_default()
            .insert(blob_name);

        self.blobs.insert(key, blob);
        Ok(())
    }

    async fn get_blob(
        &self,
        account: &str,
        container: &str,
        name: &str,
        snapshot: &str,
    ) -> StorageResult<BlobModel> {
        // First check if container exists
        if !self.container_exists(account, container).await {
            return Err(StorageError::new(ErrorCode::ContainerNotFound));
        }

        let key = Self::blob_key(account, container, name, snapshot);
        self.blobs
            .get(&key)
            .filter(|b| !b.deleted)
            .map(|b| b.value().clone())
            .ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))
    }

    async fn update_blob(&self, blob: BlobModel) -> StorageResult<()> {
        let key = Self::blob_key(&blob.account, &blob.container, &blob.name, &blob.snapshot);
        self.blobs.insert(key, blob);
        Ok(())
    }

    async fn delete_blob(
        &self,
        account: &str,
        container: &str,
        name: &str,
        snapshot: &str,
    ) -> StorageResult<()> {
        // First check if container exists
        if !self.container_exists(account, container).await {
            return Err(StorageError::new(ErrorCode::ContainerNotFound));
        }

        let key = Self::blob_key(account, container, name, snapshot);

        // Remove from main store
        let removed = self.blobs.remove(&key);

        // Update secondary index if this was the base blob (not a snapshot)
        if snapshot.is_empty() {
            let index_key = (Self::arc_str(account), Self::arc_str(container));
            if let Some(mut entry) = self.blob_index.get_mut(&index_key) {
                entry.remove(name);
            }
        }

        removed
            .map(|_| ())
            .ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))
    }

    async fn list_blobs(
        &self,
        account: &str,
        container: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        marker: Option<&str>,
        maxresults: Option<u32>,
        include_snapshots: bool,
        include_deleted: bool,
    ) -> StorageResult<(Vec<BlobModel>, Vec<String>, Option<String>)> {
        let maxresults = maxresults.unwrap_or(5000) as usize;

        // First, check that the container exists
        if !self.container_exists(account, container).await {
            return Err(StorageError::new(ErrorCode::ContainerNotFound));
        }

        let account_arc = Self::arc_str(account);
        let container_arc = Self::arc_str(container);
        let index_key = (account_arc.clone(), container_arc.clone());

        // Use the secondary index to get blob names in this container
        let blob_names: Vec<Arc<str>> = self
            .blob_index
            .get(&index_key)
            .map(|entry| {
                entry
                    .iter()
                    .filter(|name| {
                        // Filter by prefix
                        if let Some(p) = prefix {
                            if !name.starts_with(p) {
                                return false;
                            }
                        }
                        // Filter by marker
                        if let Some(m) = marker {
                            if name.as_ref() <= m {
                                return false;
                            }
                        }
                        true
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();

        // Sort blob names
        let mut sorted_names: Vec<_> = blob_names;
        sorted_names.sort();

        // Fetch blobs and handle snapshots
        let empty_snapshot = Self::arc_str("");
        let mut blobs: Vec<BlobModel> = Vec::new();

        for name in &sorted_names {
            // Get the base blob
            let key = (
                account_arc.clone(),
                container_arc.clone(),
                name.clone(),
                empty_snapshot.clone(),
            );
            if let Some(entry) = self.blobs.get(&key) {
                let blob = entry.value();
                if include_deleted || !blob.deleted {
                    blobs.push(blob.clone());
                }
            }

            // If including snapshots, we need to scan for them
            if include_snapshots {
                // This requires scanning, but it's opt-in and less common
                for entry in self.blobs.iter() {
                    let (acct, cont, blob_name, snapshot) = entry.key();
                    if acct.as_ref() == account
                        && cont.as_ref() == container
                        && blob_name.as_ref() == name.as_ref()
                        && !snapshot.is_empty()
                    {
                        let blob = entry.value();
                        if include_deleted || !blob.deleted {
                            blobs.push(blob.clone());
                        }
                    }
                }
            }
        }

        // Sort by (name, snapshot)
        blobs.sort_by(|a, b| (&a.name, &a.snapshot).cmp(&(&b.name, &b.snapshot)));

        // Handle delimiter for hierarchical listing
        let mut prefixes: Vec<String> = Vec::new();
        if let Some(delim) = delimiter {
            let prefix_str = prefix.unwrap_or("");
            let mut seen_prefixes = HashSet::new();

            blobs.retain(|blob| {
                let name_after_prefix = &blob.name[prefix_str.len()..];
                if let Some(idx) = name_after_prefix.find(delim) {
                    // This blob is under a virtual directory
                    let virtual_prefix =
                        format!("{}{}{}", prefix_str, &name_after_prefix[..idx], delim);
                    if seen_prefixes.insert(virtual_prefix.clone()) {
                        prefixes.push(virtual_prefix);
                    }
                    false
                } else {
                    true
                }
            });
        }

        prefixes.sort();

        let next_marker = if blobs.len() > maxresults {
            blobs.truncate(maxresults);
            blobs.last().map(|b| b.name.clone())
        } else {
            None
        };

        Ok((blobs, prefixes, next_marker))
    }

    async fn blob_exists(
        &self,
        account: &str,
        container: &str,
        name: &str,
        snapshot: &str,
    ) -> bool {
        let key = Self::blob_key(account, container, name, snapshot);
        self.blobs.get(&key).map(|b| !b.deleted).unwrap_or(false)
    }

    async fn stage_block(&self, block: BlockModel) -> StorageResult<()> {
        let key = Self::block_key(
            &block.account,
            &block.container,
            &block.blob,
            &block.block_id,
        );
        let index_key = (
            Self::arc_str(&block.account),
            Self::arc_str(&block.container),
            Self::arc_str(&block.blob),
        );
        let block_id = Self::arc_str(&block.block_id);

        // Update the secondary index
        self.block_index
            .entry(index_key)
            .or_default()
            .insert(block_id);

        self.blocks.insert(key, block);
        Ok(())
    }

    async fn get_staged_blocks(
        &self,
        account: &str,
        container: &str,
        blob: &str,
    ) -> StorageResult<Vec<BlockModel>> {
        let index_key = (
            Self::arc_str(account),
            Self::arc_str(container),
            Self::arc_str(blob),
        );

        // Use the secondary index to get block IDs
        let block_ids: Vec<Arc<str>> = self
            .block_index
            .get(&index_key)
            .map(|entry| entry.iter().cloned().collect())
            .unwrap_or_default();

        // Fetch blocks
        let account_arc = Self::arc_str(account);
        let container_arc = Self::arc_str(container);
        let blob_arc = Self::arc_str(blob);

        let mut blocks = Vec::with_capacity(block_ids.len());
        for block_id in block_ids {
            let key = (
                account_arc.clone(),
                container_arc.clone(),
                blob_arc.clone(),
                block_id,
            );
            if let Some(entry) = self.blocks.get(&key) {
                blocks.push(entry.value().clone());
            }
        }

        Ok(blocks)
    }

    async fn get_staged_block(
        &self,
        account: &str,
        container: &str,
        blob: &str,
        block_id: &str,
    ) -> StorageResult<BlockModel> {
        let key = Self::block_key(account, container, blob, block_id);
        self.blocks
            .get(&key)
            .map(|b| b.value().clone())
            .ok_or_else(|| StorageError::new(ErrorCode::InvalidBlockId))
    }

    async fn delete_staged_blocks(
        &self,
        account: &str,
        container: &str,
        blob: &str,
    ) -> StorageResult<()> {
        let index_key = (
            Self::arc_str(account),
            Self::arc_str(container),
            Self::arc_str(blob),
        );

        // Get and remove block IDs from index
        let block_ids: Vec<Arc<str>> = self
            .block_index
            .remove(&index_key)
            .map(|(_, set)| set.into_iter().collect())
            .unwrap_or_default();

        // Remove blocks from main store
        let account_arc = Self::arc_str(account);
        let container_arc = Self::arc_str(container);
        let blob_arc = Self::arc_str(blob);

        for block_id in block_ids {
            let key = (
                account_arc.clone(),
                container_arc.clone(),
                blob_arc.clone(),
                block_id,
            );
            self.blocks.remove(&key);
        }

        Ok(())
    }

    async fn get_service_properties(&self, account: &str) -> StorageResult<ServiceProperties> {
        let key = Self::arc_str(account);
        Ok(self
            .service_properties
            .get(&key)
            .map(|p| p.value().clone())
            .unwrap_or_default())
    }

    async fn set_service_properties(
        &self,
        account: &str,
        properties: ServiceProperties,
    ) -> StorageResult<()> {
        let key = Self::arc_str(account);
        self.service_properties.insert(key, properties);
        Ok(())
    }
}

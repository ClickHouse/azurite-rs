//! Metadata store for containers, blobs, and blocks.

use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;

use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{
    BlobModel, BlockModel, ContainerModel, ServiceProperties,
};

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

/// In-memory implementation of the metadata store.
pub struct MemoryMetadataStore {
    /// Containers indexed by (account, name).
    containers: DashMap<(String, String), ContainerModel>,
    /// Blobs indexed by (account, container, name, snapshot).
    blobs: DashMap<(String, String, String, String), BlobModel>,
    /// Staged (uncommitted) blocks indexed by (account, container, blob, block_id).
    blocks: DashMap<(String, String, String, String), BlockModel>,
    /// Service properties indexed by account.
    service_properties: DashMap<String, ServiceProperties>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self {
            containers: DashMap::new(),
            blobs: DashMap::new(),
            blocks: DashMap::new(),
            service_properties: DashMap::new(),
        }
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
        let key = container.key();
        if self.containers.contains_key(&key) {
            return Err(StorageError::new(ErrorCode::ContainerAlreadyExists));
        }
        self.containers.insert(key, container);
        Ok(())
    }

    async fn get_container(&self, account: &str, name: &str) -> StorageResult<ContainerModel> {
        let key = (account.to_string(), name.to_string());
        self.containers
            .get(&key)
            .map(|c| c.value().clone())
            .ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))
    }

    async fn update_container(&self, container: ContainerModel) -> StorageResult<()> {
        let key = container.key();
        if !self.containers.contains_key(&key) {
            return Err(StorageError::new(ErrorCode::ContainerNotFound));
        }
        self.containers.insert(key, container);
        Ok(())
    }

    async fn delete_container(&self, account: &str, name: &str) -> StorageResult<()> {
        let key = (account.to_string(), name.to_string());
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

        let mut containers: Vec<_> = self
            .containers
            .iter()
            .filter(|entry| {
                let (acct, name) = entry.key();
                if acct != account {
                    return false;
                }
                if let Some(p) = prefix {
                    if !name.starts_with(p) {
                        return false;
                    }
                }
                if let Some(m) = marker {
                    if name.as_str() <= m {
                        return false;
                    }
                }
                !entry.value().deleted
            })
            .map(|entry| entry.value().clone())
            .collect();

        containers.sort_by(|a, b| a.name.cmp(&b.name));

        let next_marker = if containers.len() > maxresults {
            containers.truncate(maxresults);
            containers.last().map(|c| c.name.clone())
        } else {
            None
        };

        Ok((containers, next_marker))
    }

    async fn container_exists(&self, account: &str, name: &str) -> bool {
        let key = (account.to_string(), name.to_string());
        self.containers
            .get(&key)
            .map(|c| !c.deleted)
            .unwrap_or(false)
    }

    async fn create_blob(&self, blob: BlobModel) -> StorageResult<()> {
        let key = blob.key();
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
        let key = (
            account.to_string(),
            container.to_string(),
            name.to_string(),
            snapshot.to_string(),
        );
        self.blobs
            .get(&key)
            .filter(|b| !b.deleted)
            .map(|b| b.value().clone())
            .ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))
    }

    async fn update_blob(&self, blob: BlobModel) -> StorageResult<()> {
        let key = blob.key();
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
        let key = (
            account.to_string(),
            container.to_string(),
            name.to_string(),
            snapshot.to_string(),
        );
        self.blobs
            .remove(&key)
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

        let mut blobs: Vec<_> = self
            .blobs
            .iter()
            .filter(|entry| {
                let (acct, cont, name, snapshot) = entry.key();
                if acct != account || cont != container {
                    return false;
                }
                // Filter by prefix
                if let Some(p) = prefix {
                    if !name.starts_with(p) {
                        return false;
                    }
                }
                // Filter by marker
                if let Some(m) = marker {
                    if name.as_str() <= m {
                        return false;
                    }
                }
                // Filter snapshots
                if !include_snapshots && !snapshot.is_empty() {
                    return false;
                }
                // Filter deleted
                if !include_deleted && entry.value().deleted {
                    return false;
                }
                true
            })
            .map(|entry| entry.value().clone())
            .collect();

        blobs.sort_by(|a, b| {
            (&a.name, &a.snapshot).cmp(&(&b.name, &b.snapshot))
        });

        // Handle delimiter for hierarchical listing
        let mut prefixes: Vec<String> = Vec::new();
        if let Some(delim) = delimiter {
            let prefix_str = prefix.unwrap_or("");
            let mut seen_prefixes = std::collections::HashSet::new();

            blobs.retain(|blob| {
                let name_after_prefix = &blob.name[prefix_str.len()..];
                if let Some(idx) = name_after_prefix.find(delim) {
                    // This blob is under a virtual directory
                    let virtual_prefix = format!(
                        "{}{}{}",
                        prefix_str,
                        &name_after_prefix[..idx],
                        delim
                    );
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
        let key = (
            account.to_string(),
            container.to_string(),
            name.to_string(),
            snapshot.to_string(),
        );
        self.blobs
            .get(&key)
            .map(|b| !b.deleted)
            .unwrap_or(false)
    }

    async fn stage_block(&self, block: BlockModel) -> StorageResult<()> {
        let key = block.key();
        self.blocks.insert(key, block);
        Ok(())
    }

    async fn get_staged_blocks(
        &self,
        account: &str,
        container: &str,
        blob: &str,
    ) -> StorageResult<Vec<BlockModel>> {
        let blocks: Vec<_> = self
            .blocks
            .iter()
            .filter(|entry| {
                let (acct, cont, b, _) = entry.key();
                acct == account && cont == container && b == blob
            })
            .map(|entry| entry.value().clone())
            .collect();
        Ok(blocks)
    }

    async fn get_staged_block(
        &self,
        account: &str,
        container: &str,
        blob: &str,
        block_id: &str,
    ) -> StorageResult<BlockModel> {
        let key = (
            account.to_string(),
            container.to_string(),
            blob.to_string(),
            block_id.to_string(),
        );
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
        let keys_to_remove: Vec<_> = self
            .blocks
            .iter()
            .filter(|entry| {
                let (acct, cont, b, _) = entry.key();
                acct == account && cont == container && b == blob
            })
            .map(|entry| entry.key().clone())
            .collect();

        for key in keys_to_remove {
            self.blocks.remove(&key);
        }
        Ok(())
    }

    async fn get_service_properties(&self, account: &str) -> StorageResult<ServiceProperties> {
        Ok(self
            .service_properties
            .get(account)
            .map(|p| p.value().clone())
            .unwrap_or_default())
    }

    async fn set_service_properties(
        &self,
        account: &str,
        properties: ServiceProperties,
    ) -> StorageResult<()> {
        self.service_properties.insert(account.to_string(), properties);
        Ok(())
    }
}

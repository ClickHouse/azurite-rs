//! Service-level data models.

use serde::{Deserialize, Serialize};

/// CORS rule for a storage service.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CorsRule {
    pub allowed_origins: Vec<String>,
    pub allowed_methods: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub exposed_headers: Vec<String>,
    pub max_age_in_seconds: u32,
}

/// Logging configuration for a storage service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub version: String,
    pub read: bool,
    pub write: bool,
    pub delete: bool,
    pub retention_policy: RetentionPolicy,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            version: "1.0".to_string(),
            read: false,
            write: false,
            delete: false,
            retention_policy: RetentionPolicy::default(),
        }
    }
}

/// Metrics configuration for a storage service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub version: String,
    pub enabled: bool,
    pub include_apis: bool,
    pub retention_policy: RetentionPolicy,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            version: "1.0".to_string(),
            enabled: false,
            include_apis: false,
            retention_policy: RetentionPolicy::default(),
        }
    }
}

/// Retention policy for logs and metrics.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetentionPolicy {
    pub enabled: bool,
    pub days: Option<u32>,
}

/// Static website configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StaticWebsite {
    pub enabled: bool,
    pub index_document: Option<String>,
    pub error_document_404_path: Option<String>,
    pub default_index_document_path: Option<String>,
}

/// Delete retention policy for soft-delete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRetentionPolicy {
    pub enabled: bool,
    pub days: Option<u32>,
    pub allow_permanent_delete: bool,
}

impl Default for DeleteRetentionPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            days: None,
            allow_permanent_delete: false,
        }
    }
}

/// Service properties for blob storage.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ServiceProperties {
    pub logging: LoggingConfig,
    pub hour_metrics: MetricsConfig,
    pub minute_metrics: MetricsConfig,
    pub cors: Vec<CorsRule>,
    pub default_service_version: Option<String>,
    pub delete_retention_policy: DeleteRetentionPolicy,
    pub static_website: StaticWebsite,
}

/// Service statistics (for read-only secondary endpoints).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStats {
    pub geo_replication: GeoReplication,
}

impl Default for ServiceStats {
    fn default() -> Self {
        Self {
            geo_replication: GeoReplication::default(),
        }
    }
}

/// Geo-replication status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoReplication {
    pub status: GeoReplicationStatus,
    pub last_sync_time: Option<String>,
}

impl Default for GeoReplication {
    fn default() -> Self {
        Self {
            status: GeoReplicationStatus::Live,
            last_sync_time: None,
        }
    }
}

/// Geo-replication status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum GeoReplicationStatus {
    #[default]
    Live,
    Bootstrap,
    Unavailable,
}

impl GeoReplicationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            GeoReplicationStatus::Live => "live",
            GeoReplicationStatus::Bootstrap => "bootstrap",
            GeoReplicationStatus::Unavailable => "unavailable",
        }
    }
}

/// User delegation key for SAS generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDelegationKey {
    pub signed_oid: String,
    pub signed_tid: String,
    pub signed_start: String,
    pub signed_expiry: String,
    pub signed_service: String,
    pub signed_version: String,
    pub value: String,
}

/// Account kind for GetAccountInfo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccountKind {
    #[default]
    StorageV2,
    Storage,
    BlobStorage,
    BlockBlobStorage,
    FileStorage,
}

impl AccountKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            AccountKind::StorageV2 => "StorageV2",
            AccountKind::Storage => "Storage",
            AccountKind::BlobStorage => "BlobStorage",
            AccountKind::BlockBlobStorage => "BlockBlobStorage",
            AccountKind::FileStorage => "FileStorage",
        }
    }
}

/// SKU name for GetAccountInfo.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SkuName {
    #[default]
    StandardLRS,
    StandardGRS,
    StandardRAGRS,
    StandardZRS,
    PremiumLRS,
    PremiumZRS,
    StandardGZRS,
    StandardRAGZRS,
}

impl SkuName {
    pub fn as_str(&self) -> &'static str {
        match self {
            SkuName::StandardLRS => "Standard_LRS",
            SkuName::StandardGRS => "Standard_GRS",
            SkuName::StandardRAGRS => "Standard_RAGRS",
            SkuName::StandardZRS => "Standard_ZRS",
            SkuName::PremiumLRS => "Premium_LRS",
            SkuName::PremiumZRS => "Premium_ZRS",
            SkuName::StandardGZRS => "Standard_GZRS",
            SkuName::StandardRAGZRS => "Standard_RAGZRS",
        }
    }
}

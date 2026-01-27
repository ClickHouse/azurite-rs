//! Container data models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::blob::{LeaseDuration, LeaseState, LeaseStatus};

/// Public access level for a container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum PublicAccessLevel {
    #[default]
    None,
    Container,
    Blob,
}

impl PublicAccessLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            PublicAccessLevel::None => "",
            PublicAccessLevel::Container => "container",
            PublicAccessLevel::Blob => "blob",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "" | "none" | "private" => Some(PublicAccessLevel::None),
            "container" => Some(PublicAccessLevel::Container),
            "blob" => Some(PublicAccessLevel::Blob),
            _ => None,
        }
    }
}

/// Container properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerProperties {
    pub etag: String,
    pub last_modified: DateTime<Utc>,
    pub lease_state: LeaseState,
    pub lease_status: LeaseStatus,
    pub lease_duration: Option<LeaseDuration>,
    pub lease_id: Option<String>,
    pub lease_expiry: Option<DateTime<Utc>>,
    pub lease_break_time: Option<DateTime<Utc>>,
    pub public_access: PublicAccessLevel,
    pub has_immutability_policy: bool,
    pub has_legal_hold: bool,
    pub default_encryption_scope: Option<String>,
    pub deny_encryption_scope_override: bool,
}

impl Default for ContainerProperties {
    fn default() -> Self {
        Self {
            etag: format!("\"0x{}\"", uuid::Uuid::new_v4().simple()),
            last_modified: Utc::now(),
            lease_state: LeaseState::Available,
            lease_status: LeaseStatus::Unlocked,
            lease_duration: None,
            lease_id: None,
            lease_expiry: None,
            lease_break_time: None,
            public_access: PublicAccessLevel::None,
            has_immutability_policy: false,
            has_legal_hold: false,
            default_encryption_scope: None,
            deny_encryption_scope_override: false,
        }
    }
}

impl ContainerProperties {
    /// Updates the ETag and last modified time.
    pub fn update_etag(&mut self) {
        self.etag = format!("\"0x{}\"", uuid::Uuid::new_v4().simple());
        self.last_modified = Utc::now();
    }
}

/// Signed identifier for container access policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedIdentifier {
    pub id: String,
    pub access_policy: AccessPolicy,
}

/// Access policy for a signed identifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPolicy {
    pub start: Option<DateTime<Utc>>,
    pub expiry: Option<DateTime<Utc>>,
    pub permission: String,
}

/// Complete container model stored in metadata store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerModel {
    /// Account name.
    pub account: String,
    /// Container name.
    pub name: String,
    /// Container properties.
    pub properties: ContainerProperties,
    /// User-defined metadata.
    pub metadata: HashMap<String, String>,
    /// Signed identifiers for stored access policies.
    pub signed_identifiers: Vec<SignedIdentifier>,
    /// Whether the container is soft-deleted.
    pub deleted: bool,
    /// Soft-delete version (for restoring specific versions).
    pub deleted_version: Option<String>,
    /// Soft-delete expiry time.
    pub deleted_time: Option<DateTime<Utc>>,
    /// Remaining retention days after soft-delete.
    pub remaining_retention_days: Option<u32>,
}

impl ContainerModel {
    /// Creates a new container model.
    pub fn new(account: String, name: String) -> Self {
        Self {
            account,
            name,
            properties: ContainerProperties::default(),
            metadata: HashMap::new(),
            signed_identifiers: Vec::new(),
            deleted: false,
            deleted_version: None,
            deleted_time: None,
            remaining_retention_days: None,
        }
    }

    /// Returns the unique key for this container.
    pub fn key(&self) -> (String, String) {
        (self.account.clone(), self.name.clone())
    }
}

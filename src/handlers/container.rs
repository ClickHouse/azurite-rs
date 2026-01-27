//! Container-level handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{header::HeaderName, HeaderMap, HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use chrono::Utc;
use std::sync::Arc;

use crate::context::{format_http_date, ListParams, RequestContext};
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{
    ContainerModel, LeaseDuration, LeaseState, LeaseStatus, PublicAccessLevel,
};
use crate::storage::MetadataStore;
use crate::xml::{
    deserialize::parse_signed_identifiers,
    serialize::{serialize_blob_list, serialize_signed_identifiers},
};

use super::{add_blob_headers, build_response, common_headers};

/// PUT /{container}?restype=container - Create container.
pub async fn create_container(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    // Validate container name
    validate_container_name(container_name)?;

    let mut container = ContainerModel::new(ctx.account.clone(), container_name.clone());

    // Set public access level from header
    if let Some(access) = ctx.header("x-ms-blob-public-access") {
        container.properties.public_access =
            PublicAccessLevel::from_str(access).unwrap_or(PublicAccessLevel::None);
    }

    // Set metadata
    container.metadata = ctx.metadata();

    metadata.create_container(container.clone()).await?;

    let mut headers = common_headers();
    headers.insert("ETag", HeaderValue::from_str(&container.properties.etag).unwrap());
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&format_http_date(&container.properties.last_modified)).unwrap(),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// DELETE /{container}?restype=container - Delete container.
pub async fn delete_container(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    // Check lease
    let container = metadata.get_container(&ctx.account, container_name).await?;
    check_container_lease(&container, ctx.lease_id())?;

    metadata.delete_container(&ctx.account, container_name).await?;

    let headers = common_headers();

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::empty()))
}

/// GET/HEAD /{container}?restype=container - Get container properties.
pub async fn get_container_properties(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    let container = metadata.get_container(&ctx.account, container_name).await?;

    let mut headers = common_headers();
    headers.insert("ETag", HeaderValue::from_str(&container.properties.etag).unwrap());
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&format_http_date(&container.properties.last_modified)).unwrap(),
    );
    headers.insert(
        "x-ms-lease-status",
        HeaderValue::from_static(container.properties.lease_status.as_str()),
    );
    headers.insert(
        "x-ms-lease-state",
        HeaderValue::from_static(container.properties.lease_state.as_str()),
    );
    if container.properties.public_access != PublicAccessLevel::None {
        headers.insert(
            "x-ms-blob-public-access",
            HeaderValue::from_str(container.properties.public_access.as_str()).unwrap(),
        );
    }
    headers.insert(
        "x-ms-has-immutability-policy",
        HeaderValue::from_str(&container.properties.has_immutability_policy.to_string()).unwrap(),
    );
    headers.insert(
        "x-ms-has-legal-hold",
        HeaderValue::from_str(&container.properties.has_legal_hold.to_string()).unwrap(),
    );

    // Add metadata headers
    for (key, value) in &container.metadata {
        if let Ok(header_value) = HeaderValue::from_str(value) {
            headers.insert(
                format!("x-ms-meta-{}", key).parse::<HeaderName>().unwrap(),
                header_value,
            );
        }
    }

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}?restype=container&comp=metadata - Set container metadata.
pub async fn set_container_metadata(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    let mut container = metadata.get_container(&ctx.account, container_name).await?;
    check_container_lease(&container, ctx.lease_id())?;

    container.metadata = ctx.metadata();
    container.properties.update_etag();

    metadata.update_container(container.clone()).await?;

    let mut headers = common_headers();
    headers.insert("ETag", HeaderValue::from_str(&container.properties.etag).unwrap());
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&format_http_date(&container.properties.last_modified)).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// GET /{container}?restype=container&comp=acl - Get container access policy.
pub async fn get_container_acl(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    let container = metadata.get_container(&ctx.account, container_name).await?;
    let xml = serialize_signed_identifiers(&container.signed_identifiers);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));
    headers.insert("ETag", HeaderValue::from_str(&container.properties.etag).unwrap());
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&format_http_date(&container.properties.last_modified)).unwrap(),
    );
    if container.properties.public_access != PublicAccessLevel::None {
        headers.insert(
            "x-ms-blob-public-access",
            HeaderValue::from_str(container.properties.public_access.as_str()).unwrap(),
        );
    }

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// PUT /{container}?restype=container&comp=acl - Set container access policy.
pub async fn set_container_acl(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    let mut container = metadata.get_container(&ctx.account, container_name).await?;
    check_container_lease(&container, ctx.lease_id())?;

    // Parse signed identifiers from body
    if !body.is_empty() {
        let xml = std::str::from_utf8(&body)
            .map_err(|_| StorageError::new(ErrorCode::InvalidXmlDocument))?;
        container.signed_identifiers = parse_signed_identifiers(xml)?;
    } else {
        container.signed_identifiers.clear();
    }

    // Set public access level from header
    if let Some(access) = ctx.header("x-ms-blob-public-access") {
        container.properties.public_access =
            PublicAccessLevel::from_str(access).unwrap_or(PublicAccessLevel::None);
    } else {
        container.properties.public_access = PublicAccessLevel::None;
    }

    container.properties.update_etag();
    metadata.update_container(container.clone()).await?;

    let mut headers = common_headers();
    headers.insert("ETag", HeaderValue::from_str(&container.properties.etag).unwrap());
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&format_http_date(&container.properties.last_modified)).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// GET /{container}?restype=container&comp=list - List blobs.
pub async fn list_blobs(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    let list_params = ListParams::from_query(&ctx.query_params);
    let include_snapshots = list_params.include.contains(&"snapshots".to_string());
    let include_deleted = list_params.include.contains(&"deleted".to_string());

    let maxresults = list_params.maxresults.unwrap_or(5000);

    let (blobs, prefixes, next_marker) = metadata
        .list_blobs(
            &ctx.account,
            container_name,
            list_params.prefix.as_deref(),
            list_params.delimiter.as_deref(),
            list_params.marker.as_deref(),
            Some(maxresults),
            include_snapshots,
            include_deleted,
        )
        .await?;

    let xml = serialize_blob_list(
        &blobs,
        &prefixes,
        list_params.prefix.as_deref(),
        list_params.delimiter.as_deref(),
        list_params.marker.as_deref(),
        maxresults,
        next_marker.as_deref(),
        &ctx.account,
        container_name,
    );

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// PUT /{container}?comp=lease&restype=container - Container lease operations.
pub async fn container_lease(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container_name = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidResourceName))?;

    let action = ctx
        .header("x-ms-lease-action")
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    let mut container = metadata.get_container(&ctx.account, container_name).await?;
    let mut headers = common_headers();

    match action.to_lowercase().as_str() {
        "acquire" => {
            // Check if already leased
            if container.properties.lease_state == LeaseState::Leased {
                return Err(StorageError::new(ErrorCode::LeaseAlreadyPresent));
            }

            let lease_id = ctx
                .header("x-ms-proposed-lease-id")
                .map(String::from)
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            let duration: i32 = ctx
                .header("x-ms-lease-duration")
                .and_then(|s| s.parse().ok())
                .unwrap_or(-1);

            container.properties.lease_state = LeaseState::Leased;
            container.properties.lease_status = LeaseStatus::Locked;
            container.properties.lease_id = Some(lease_id.clone());
            container.properties.lease_duration = if duration == -1 {
                Some(LeaseDuration::Infinite)
            } else {
                container.properties.lease_expiry = Some(Utc::now() + chrono::Duration::seconds(duration as i64));
                Some(LeaseDuration::Fixed)
            };

            headers.insert("x-ms-lease-id", HeaderValue::from_str(&lease_id).unwrap());
        }
        "release" => {
            let provided_lease_id = ctx.lease_id().ok_or_else(|| {
                StorageError::new(ErrorCode::LeaseIdMissing)
            })?;

            if container.properties.lease_id.as_deref() != Some(provided_lease_id) {
                return Err(StorageError::new(ErrorCode::LeaseIdMismatchWithContainerOperation));
            }

            container.properties.lease_state = LeaseState::Available;
            container.properties.lease_status = LeaseStatus::Unlocked;
            container.properties.lease_id = None;
            container.properties.lease_duration = None;
            container.properties.lease_expiry = None;
        }
        "renew" => {
            let provided_lease_id = ctx.lease_id().ok_or_else(|| {
                StorageError::new(ErrorCode::LeaseIdMissing)
            })?;

            if container.properties.lease_id.as_deref() != Some(provided_lease_id) {
                return Err(StorageError::new(ErrorCode::LeaseIdMismatchWithContainerOperation));
            }

            if container.properties.lease_state != LeaseState::Leased {
                return Err(StorageError::new(ErrorCode::LeaseIsBrokenAndCannotBeRenewed));
            }

            // Renew the lease expiry
            if let Some(LeaseDuration::Fixed) = container.properties.lease_duration {
                container.properties.lease_expiry = Some(Utc::now() + chrono::Duration::seconds(60));
            }

            headers.insert(
                "x-ms-lease-id",
                HeaderValue::from_str(provided_lease_id).unwrap(),
            );
        }
        "break" => {
            if container.properties.lease_state == LeaseState::Available {
                return Err(StorageError::new(ErrorCode::LeaseNotPresentWithContainerOperation));
            }

            let break_period: u32 = ctx
                .header("x-ms-lease-break-period")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if break_period == 0 {
                container.properties.lease_state = LeaseState::Broken;
                container.properties.lease_status = LeaseStatus::Unlocked;
                container.properties.lease_id = None;
                headers.insert("x-ms-lease-time", HeaderValue::from_static("0"));
            } else {
                container.properties.lease_state = LeaseState::Breaking;
                container.properties.lease_break_time = Some(Utc::now() + chrono::Duration::seconds(break_period as i64));
                headers.insert(
                    "x-ms-lease-time",
                    HeaderValue::from_str(&break_period.to_string()).unwrap(),
                );
            }
        }
        "change" => {
            let provided_lease_id = ctx.lease_id().ok_or_else(|| {
                StorageError::new(ErrorCode::LeaseIdMissing)
            })?;

            if container.properties.lease_id.as_deref() != Some(provided_lease_id) {
                return Err(StorageError::new(ErrorCode::LeaseIdMismatchWithContainerOperation));
            }

            let new_lease_id = ctx
                .header("x-ms-proposed-lease-id")
                .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

            container.properties.lease_id = Some(new_lease_id.to_string());
            headers.insert("x-ms-lease-id", HeaderValue::from_str(new_lease_id).unwrap());
        }
        _ => {
            return Err(StorageError::with_message(
                ErrorCode::InvalidHeaderValue,
                "Invalid x-ms-lease-action header value",
            ));
        }
    }

    container.properties.update_etag();
    metadata.update_container(container.clone()).await?;

    headers.insert("ETag", HeaderValue::from_str(&container.properties.etag).unwrap());
    headers.insert(
        "Last-Modified",
        HeaderValue::from_str(&format_http_date(&container.properties.last_modified)).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}?restype=container&comp=undelete - Restore deleted container.
pub async fn restore_container(
    _ctx: &RequestContext,
    _metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    // Simplified implementation - just return OK
    // Full implementation would restore soft-deleted containers
    let headers = common_headers();

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// Validates a container name.
fn validate_container_name(name: &str) -> StorageResult<()> {
    // Container names must be 3-63 characters
    if name.len() < 3 || name.len() > 63 {
        return Err(StorageError::with_message(
            ErrorCode::InvalidResourceName,
            "Container name must be between 3 and 63 characters",
        ));
    }

    // Must start with a letter or number
    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphanumeric() {
        return Err(StorageError::with_message(
            ErrorCode::InvalidResourceName,
            "Container name must start with a letter or number",
        ));
    }

    // Can only contain lowercase letters, numbers, and hyphens
    // Note: $root and $logs are special containers
    if name != "$root" && name != "$logs" && name != "$web" {
        for c in name.chars() {
            if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' {
                return Err(StorageError::with_message(
                    ErrorCode::InvalidResourceName,
                    "Container name can only contain lowercase letters, numbers, and hyphens",
                ));
            }
        }

        // Cannot have consecutive hyphens
        if name.contains("--") {
            return Err(StorageError::with_message(
                ErrorCode::InvalidResourceName,
                "Container name cannot have consecutive hyphens",
            ));
        }
    }

    Ok(())
}

/// Checks if the container lease allows the operation.
fn check_container_lease(container: &ContainerModel, provided_lease_id: Option<&str>) -> StorageResult<()> {
    if container.properties.lease_state == LeaseState::Leased {
        match (container.properties.lease_id.as_deref(), provided_lease_id) {
            (Some(expected), Some(provided)) if expected == provided => Ok(()),
            (Some(_), Some(_)) => Err(StorageError::new(ErrorCode::LeaseIdMismatchWithContainerOperation)),
            (Some(_), None) => Err(StorageError::new(ErrorCode::LeaseIdMissing)),
            _ => Ok(()),
        }
    } else {
        Ok(())
    }
}

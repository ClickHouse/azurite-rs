//! Blob-level handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{header::HeaderName, HeaderMap, HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use chrono::Utc;
use std::sync::Arc;

use crate::context::{format_http_date, format_iso8601, RequestContext};
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{
    AccessTier, BlobModel, BlobType, CopyStatus, ExtentChunk, LeaseDuration, LeaseState,
    LeaseStatus,
};
use crate::storage::{ExtentStore, MetadataStore};
use crate::xml::{deserialize::parse_tags, serialize::serialize_tags};

use super::{add_blob_headers, build_response, common_headers};

/// GET /{container}/{blob} - Download blob.
pub async fn download_blob(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;
    let snapshot = ctx.snapshot().unwrap_or("");

    let blob = metadata.get_blob(&ctx.account, container, blob_name, snapshot).await?;

    // Check conditional headers
    check_conditional_headers(ctx, &blob)?;

    // Check lease for non-snapshot reads
    if snapshot.is_empty() {
        // Lease check is not required for reads
    }

    // Handle range request
    let (data, status, content_range) = if let Some((start, end)) = ctx.range() {
        let end = end.unwrap_or(blob.properties.content_length.saturating_sub(1));

        if start >= blob.properties.content_length {
            return Err(StorageError::new(ErrorCode::InvalidRange));
        }

        let actual_end = end.min(blob.properties.content_length.saturating_sub(1));
        let length = actual_end - start + 1;

        // Read range from extents
        let mut result = Vec::new();
        let mut bytes_read = 0u64;
        let mut current_pos = 0u64;

        for chunk in &blob.extent_chunks {
            let chunk_end = current_pos + chunk.count;

            if current_pos < start + length && chunk_end > start {
                let chunk_start = if current_pos < start {
                    start - current_pos
                } else {
                    0
                };
                let chunk_read_end = (chunk.count).min(start + length - current_pos);
                let bytes_to_read = chunk_read_end - chunk_start;

                let data = extents.read_range(chunk, chunk_start, bytes_to_read).await?;
                result.extend_from_slice(&data);
                bytes_read += bytes_to_read;
            }

            current_pos = chunk_end;
            if bytes_read >= length {
                break;
            }
        }

        let range_str = format!(
            "bytes {}-{}/{}",
            start,
            actual_end,
            blob.properties.content_length
        );
        (Bytes::from(result), StatusCode::PARTIAL_CONTENT, Some(range_str))
    } else {
        // Read full blob
        let mut result = Vec::new();
        for chunk in &blob.extent_chunks {
            let data = extents.read(chunk).await?;
            result.extend_from_slice(&data);
        }
        (Bytes::from(result), StatusCode::OK, None)
    };

    let mut headers = common_headers();
    add_blob_headers(&mut headers, &blob.properties.etag, &blob.properties.last_modified);

    headers.insert(
        "Content-Length",
        HeaderValue::from_str(&data.len().to_string()).unwrap(),
    );
    headers.insert(
        "x-ms-blob-type",
        HeaderValue::from_static(blob.properties.blob_type.as_str()),
    );

    if let Some(ref ct) = blob.properties.content_type {
        headers.insert("Content-Type", HeaderValue::from_str(ct).unwrap());
    }
    if let Some(ref ce) = blob.properties.content_encoding {
        headers.insert("Content-Encoding", HeaderValue::from_str(ce).unwrap());
    }
    if let Some(ref cl) = blob.properties.content_language {
        headers.insert("Content-Language", HeaderValue::from_str(cl).unwrap());
    }
    if let Some(ref md5) = blob.properties.content_md5 {
        headers.insert("Content-MD5", HeaderValue::from_str(md5).unwrap());
    }
    if let Some(ref cd) = blob.properties.content_disposition {
        headers.insert("Content-Disposition", HeaderValue::from_str(cd).unwrap());
    }
    if let Some(ref cc) = blob.properties.cache_control {
        headers.insert("Cache-Control", HeaderValue::from_str(cc).unwrap());
    }
    if let Some(range) = content_range {
        headers.insert("Content-Range", HeaderValue::from_str(&range).unwrap());
    }

    headers.insert(
        "x-ms-lease-status",
        HeaderValue::from_static(blob.properties.lease_status.as_str()),
    );
    headers.insert(
        "x-ms-lease-state",
        HeaderValue::from_static(blob.properties.lease_state.as_str()),
    );
    headers.insert(
        "x-ms-server-encrypted",
        HeaderValue::from_str(&blob.properties.server_encrypted.to_string()).unwrap(),
    );
    headers.insert(
        "Accept-Ranges",
        HeaderValue::from_static("bytes"),
    );

    // Add metadata headers
    for (key, value) in &blob.metadata {
        if let Ok(header_value) = HeaderValue::from_str(value) {
            headers.insert(
                format!("x-ms-meta-{}", key).parse::<HeaderName>().unwrap(),
                header_value,
            );
        }
    }

    Ok(build_response(status, headers, Body::from(data)))
}

/// HEAD /{container}/{blob} - Get blob properties.
pub async fn get_blob_properties(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;
    let snapshot = ctx.snapshot().unwrap_or("");

    let blob = metadata.get_blob(&ctx.account, container, blob_name, snapshot).await?;

    // Check conditional headers
    check_conditional_headers(ctx, &blob)?;

    let mut headers = common_headers();
    add_blob_headers(&mut headers, &blob.properties.etag, &blob.properties.last_modified);

    headers.insert(
        "Content-Length",
        HeaderValue::from_str(&blob.properties.content_length.to_string()).unwrap(),
    );
    headers.insert(
        "x-ms-blob-type",
        HeaderValue::from_static(blob.properties.blob_type.as_str()),
    );

    if let Some(ref ct) = blob.properties.content_type {
        headers.insert("Content-Type", HeaderValue::from_str(ct).unwrap());
    }
    if let Some(ref ce) = blob.properties.content_encoding {
        headers.insert("Content-Encoding", HeaderValue::from_str(ce).unwrap());
    }
    if let Some(ref cl) = blob.properties.content_language {
        headers.insert("Content-Language", HeaderValue::from_str(cl).unwrap());
    }
    if let Some(ref md5) = blob.properties.content_md5 {
        headers.insert("Content-MD5", HeaderValue::from_str(md5).unwrap());
    }
    if let Some(ref cd) = blob.properties.content_disposition {
        headers.insert("Content-Disposition", HeaderValue::from_str(cd).unwrap());
    }
    if let Some(ref cc) = blob.properties.cache_control {
        headers.insert("Cache-Control", HeaderValue::from_str(cc).unwrap());
    }

    headers.insert(
        "x-ms-lease-status",
        HeaderValue::from_static(blob.properties.lease_status.as_str()),
    );
    headers.insert(
        "x-ms-lease-state",
        HeaderValue::from_static(blob.properties.lease_state.as_str()),
    );
    headers.insert(
        "x-ms-server-encrypted",
        HeaderValue::from_str(&blob.properties.server_encrypted.to_string()).unwrap(),
    );
    headers.insert(
        "x-ms-access-tier",
        HeaderValue::from_static(blob.properties.access_tier.as_str()),
    );
    headers.insert("Accept-Ranges", HeaderValue::from_static("bytes"));
    headers.insert(
        "x-ms-creation-time",
        HeaderValue::from_str(&format_http_date(&blob.properties.created_on)).unwrap(),
    );

    // Page blob specific
    if blob.properties.blob_type == BlobType::PageBlob {
        if let Some(seq) = blob.properties.sequence_number {
            headers.insert(
                "x-ms-blob-sequence-number",
                HeaderValue::from_str(&seq.to_string()).unwrap(),
            );
        }
    }

    // Append blob specific
    if blob.properties.blob_type == BlobType::AppendBlob {
        if let Some(count) = blob.properties.committed_block_count {
            headers.insert(
                "x-ms-blob-committed-block-count",
                HeaderValue::from_str(&count.to_string()).unwrap(),
            );
        }
        if let Some(sealed) = blob.properties.is_sealed {
            headers.insert(
                "x-ms-blob-sealed",
                HeaderValue::from_str(&sealed.to_string()).unwrap(),
            );
        }
    }

    // Copy properties
    if let Some(ref copy_id) = blob.properties.copy_id {
        headers.insert("x-ms-copy-id", HeaderValue::from_str(copy_id).unwrap());
    }
    if let Some(ref copy_source) = blob.properties.copy_source {
        headers.insert("x-ms-copy-source", HeaderValue::from_str(copy_source).unwrap());
    }
    if let Some(ref copy_status) = blob.properties.copy_status {
        headers.insert(
            "x-ms-copy-status",
            HeaderValue::from_static(copy_status.as_str()),
        );
    }
    if let Some(ref copy_progress) = blob.properties.copy_progress {
        headers.insert(
            "x-ms-copy-progress",
            HeaderValue::from_str(copy_progress).unwrap(),
        );
    }

    // Add metadata headers
    for (key, value) in &blob.metadata {
        if let Ok(header_value) = HeaderValue::from_str(value) {
            headers.insert(
                format!("x-ms-meta-{}", key).parse::<HeaderName>().unwrap(),
                header_value,
            );
        }
    }

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// DELETE /{container}/{blob} - Delete blob.
pub async fn delete_blob(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;
    let snapshot = ctx.snapshot().unwrap_or("");

    let blob = metadata.get_blob(&ctx.account, container, blob_name, snapshot).await?;

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Check conditional headers
    check_conditional_headers(ctx, &blob)?;

    // Handle delete snapshots header
    let delete_snapshots = ctx.header("x-ms-delete-snapshots");

    // Delete the blob
    metadata
        .delete_blob(&ctx.account, container, blob_name, snapshot)
        .await?;

    // Clean up extent data
    for chunk in &blob.extent_chunks {
        let _ = extents.delete(&chunk.id).await;
    }

    let headers = common_headers();

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=properties - Set blob HTTP headers.
pub async fn set_blob_properties(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let mut blob = metadata.get_blob(&ctx.account, container, blob_name, "").await?;

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Check conditional headers
    check_conditional_headers(ctx, &blob)?;

    // Update content headers
    if let Some(ct) = ctx.header("x-ms-blob-content-type") {
        blob.properties.content_type = Some(ct.to_string());
    }
    if let Some(ce) = ctx.header("x-ms-blob-content-encoding") {
        blob.properties.content_encoding = Some(ce.to_string());
    }
    if let Some(cl) = ctx.header("x-ms-blob-content-language") {
        blob.properties.content_language = Some(cl.to_string());
    }
    if let Some(md5) = ctx.header("x-ms-blob-content-md5") {
        blob.properties.content_md5 = Some(md5.to_string());
    }
    if let Some(cd) = ctx.header("x-ms-blob-content-disposition") {
        blob.properties.content_disposition = Some(cd.to_string());
    }
    if let Some(cc) = ctx.header("x-ms-blob-cache-control") {
        blob.properties.cache_control = Some(cc.to_string());
    }

    blob.properties.update_etag();
    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(&mut headers, &blob.properties.etag, &blob.properties.last_modified);

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=metadata - Set blob metadata.
pub async fn set_blob_metadata(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let mut blob = metadata.get_blob(&ctx.account, container, blob_name, "").await?;

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Check conditional headers
    check_conditional_headers(ctx, &blob)?;

    blob.metadata = ctx.metadata();
    blob.properties.update_etag();

    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(&mut headers, &blob.properties.etag, &blob.properties.last_modified);

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=snapshot - Create blob snapshot.
pub async fn create_snapshot(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let blob = metadata.get_blob(&ctx.account, container, blob_name, "").await?;

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Check conditional headers
    check_conditional_headers(ctx, &blob)?;

    // Create snapshot
    let snapshot = blob.create_snapshot();
    let snapshot_time = snapshot.snapshot.clone();

    // Apply any metadata from request
    let mut snapshot = snapshot;
    let request_metadata = ctx.metadata();
    if !request_metadata.is_empty() {
        snapshot.metadata = request_metadata;
    }

    metadata.create_blob(snapshot.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(&mut headers, &snapshot.properties.etag, &snapshot.properties.last_modified);
    headers.insert(
        "x-ms-snapshot",
        HeaderValue::from_str(&snapshot_time).unwrap(),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=lease - Blob lease operations.
pub async fn blob_lease(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let action = ctx
        .header("x-ms-lease-action")
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    let mut blob = metadata.get_blob(&ctx.account, container, blob_name, "").await?;
    let mut headers = common_headers();

    match action.to_lowercase().as_str() {
        "acquire" => {
            if blob.properties.lease_state == LeaseState::Leased {
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

            blob.properties.lease_state = LeaseState::Leased;
            blob.properties.lease_status = LeaseStatus::Locked;
            blob.properties.lease_id = Some(lease_id.clone());
            blob.properties.lease_duration = if duration == -1 {
                Some(LeaseDuration::Infinite)
            } else {
                blob.properties.lease_expiry =
                    Some(Utc::now() + chrono::Duration::seconds(duration as i64));
                Some(LeaseDuration::Fixed)
            };

            headers.insert("x-ms-lease-id", HeaderValue::from_str(&lease_id).unwrap());
        }
        "release" => {
            let provided_lease_id = ctx
                .lease_id()
                .ok_or_else(|| StorageError::new(ErrorCode::LeaseIdMissing))?;

            if blob.properties.lease_id.as_deref() != Some(provided_lease_id) {
                return Err(StorageError::new(ErrorCode::LeaseIdMismatchWithBlobOperation));
            }

            blob.properties.lease_state = LeaseState::Available;
            blob.properties.lease_status = LeaseStatus::Unlocked;
            blob.properties.lease_id = None;
            blob.properties.lease_duration = None;
            blob.properties.lease_expiry = None;
        }
        "renew" => {
            let provided_lease_id = ctx
                .lease_id()
                .ok_or_else(|| StorageError::new(ErrorCode::LeaseIdMissing))?;

            if blob.properties.lease_id.as_deref() != Some(provided_lease_id) {
                return Err(StorageError::new(ErrorCode::LeaseIdMismatchWithBlobOperation));
            }

            if blob.properties.lease_state != LeaseState::Leased {
                return Err(StorageError::new(ErrorCode::LeaseIsBrokenAndCannotBeRenewed));
            }

            if let Some(LeaseDuration::Fixed) = blob.properties.lease_duration {
                blob.properties.lease_expiry =
                    Some(Utc::now() + chrono::Duration::seconds(60));
            }

            headers.insert(
                "x-ms-lease-id",
                HeaderValue::from_str(provided_lease_id).unwrap(),
            );
        }
        "break" => {
            if blob.properties.lease_state == LeaseState::Available {
                return Err(StorageError::new(ErrorCode::LeaseNotPresentWithBlobOperation));
            }

            let break_period: u32 = ctx
                .header("x-ms-lease-break-period")
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if break_period == 0 {
                blob.properties.lease_state = LeaseState::Broken;
                blob.properties.lease_status = LeaseStatus::Unlocked;
                blob.properties.lease_id = None;
                headers.insert("x-ms-lease-time", HeaderValue::from_static("0"));
            } else {
                blob.properties.lease_state = LeaseState::Breaking;
                blob.properties.lease_break_time =
                    Some(Utc::now() + chrono::Duration::seconds(break_period as i64));
                headers.insert(
                    "x-ms-lease-time",
                    HeaderValue::from_str(&break_period.to_string()).unwrap(),
                );
            }
        }
        "change" => {
            let provided_lease_id = ctx
                .lease_id()
                .ok_or_else(|| StorageError::new(ErrorCode::LeaseIdMissing))?;

            if blob.properties.lease_id.as_deref() != Some(provided_lease_id) {
                return Err(StorageError::new(ErrorCode::LeaseIdMismatchWithBlobOperation));
            }

            let new_lease_id = ctx
                .header("x-ms-proposed-lease-id")
                .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

            blob.properties.lease_id = Some(new_lease_id.to_string());
            headers.insert("x-ms-lease-id", HeaderValue::from_str(new_lease_id).unwrap());
        }
        _ => {
            return Err(StorageError::with_message(
                ErrorCode::InvalidHeaderValue,
                "Invalid x-ms-lease-action header value",
            ));
        }
    }

    blob.properties.update_etag();
    metadata.update_blob(blob.clone()).await?;

    add_blob_headers(&mut headers, &blob.properties.etag, &blob.properties.last_modified);

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=tier - Set blob access tier.
pub async fn set_blob_tier(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let tier = ctx
        .header("x-ms-access-tier")
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    let access_tier = AccessTier::from_str(tier)
        .ok_or_else(|| StorageError::new(ErrorCode::InvalidBlobTier))?;

    let mut blob = metadata.get_blob(&ctx.account, container, blob_name, "").await?;
    blob.properties.access_tier = access_tier;
    blob.properties.update_etag();

    metadata.update_blob(blob).await?;

    let headers = common_headers();

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// GET /{container}/{blob}?comp=tags - Get blob tags.
pub async fn get_blob_tags(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;
    let snapshot = ctx.snapshot().unwrap_or("");

    let blob = metadata.get_blob(&ctx.account, container, blob_name, snapshot).await?;
    let xml = serialize_tags(&blob.tags);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// PUT /{container}/{blob}?comp=tags - Set blob tags.
pub async fn set_blob_tags(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let mut blob = metadata.get_blob(&ctx.account, container, blob_name, "").await?;

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Parse tags from body
    if !body.is_empty() {
        let xml = std::str::from_utf8(&body)
            .map_err(|_| StorageError::new(ErrorCode::InvalidXmlDocument))?;
        blob.tags = parse_tags(xml)?;
    } else {
        blob.tags.clear();
    }

    metadata.update_blob(blob).await?;

    let headers = common_headers();

    Ok(build_response(StatusCode::NO_CONTENT, headers, Body::empty()))
}

/// PUT /{container}/{blob} with x-ms-copy-source - Copy blob.
pub async fn copy_blob(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx.container.as_ref().ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx.blob.as_ref().ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let copy_source = ctx
        .copy_source()
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    // Parse source URL to extract account, container, blob
    let source_parts = parse_copy_source(copy_source)?;

    // Get source blob
    let source_blob = metadata
        .get_blob(
            &source_parts.account,
            &source_parts.container,
            &source_parts.blob,
            &source_parts.snapshot,
        )
        .await?;

    // Create destination blob as a copy
    let copy_id = uuid::Uuid::new_v4().to_string();
    let mut dest_blob = BlobModel::new(
        ctx.account.clone(),
        container.clone(),
        blob_name.clone(),
        source_blob.properties.blob_type,
        source_blob.properties.content_length,
    );

    // Copy properties
    dest_blob.properties.content_type = source_blob.properties.content_type.clone();
    dest_blob.properties.content_encoding = source_blob.properties.content_encoding.clone();
    dest_blob.properties.content_language = source_blob.properties.content_language.clone();
    dest_blob.properties.content_md5 = source_blob.properties.content_md5.clone();
    dest_blob.properties.content_disposition = source_blob.properties.content_disposition.clone();
    dest_blob.properties.cache_control = source_blob.properties.cache_control.clone();

    // Copy extent references (for same-account copies)
    if source_parts.account == ctx.account {
        dest_blob.extent_chunks = source_blob.extent_chunks.clone();
    } else {
        // For cross-account copies, we would need to copy the actual data
        // For simplicity, we'll just reference the same extents
        dest_blob.extent_chunks = source_blob.extent_chunks.clone();
    }

    // Set copy metadata
    dest_blob.properties.copy_id = Some(copy_id.clone());
    dest_blob.properties.copy_source = Some(copy_source.to_string());
    dest_blob.properties.copy_status = Some(CopyStatus::Success);
    dest_blob.properties.copy_progress = Some(format!(
        "{}/{}",
        source_blob.properties.content_length, source_blob.properties.content_length
    ));
    dest_blob.properties.copy_completion_time = Some(Utc::now());

    // Apply request metadata (overrides source metadata)
    let request_metadata = ctx.metadata();
    dest_blob.metadata = if request_metadata.is_empty() {
        source_blob.metadata.clone()
    } else {
        request_metadata
    };

    metadata.create_blob(dest_blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &dest_blob.properties.etag,
        &dest_blob.properties.last_modified,
    );
    headers.insert("x-ms-copy-id", HeaderValue::from_str(&copy_id).unwrap());
    headers.insert("x-ms-copy-status", HeaderValue::from_static("success"));

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=copy&copyid={id} - Abort copy.
pub async fn abort_copy(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    // Simplified implementation - copy is always synchronous in our implementation
    Err(StorageError::new(ErrorCode::NoPendingCopyOperation))
}

/// PUT /{container}/{blob}?comp=undelete - Undelete blob.
pub async fn undelete_blob(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    // Simplified implementation
    let headers = common_headers();

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// Checks if the blob lease allows the operation.
pub fn check_blob_lease(blob: &BlobModel, provided_lease_id: Option<&str>) -> StorageResult<()> {
    if blob.properties.lease_state == LeaseState::Leased {
        match (blob.properties.lease_id.as_deref(), provided_lease_id) {
            (Some(expected), Some(provided)) if expected == provided => Ok(()),
            (Some(_), Some(_)) => Err(StorageError::new(ErrorCode::LeaseIdMismatchWithBlobOperation)),
            (Some(_), None) => Err(StorageError::new(ErrorCode::LeaseIdMissing)),
            _ => Ok(()),
        }
    } else {
        Ok(())
    }
}

/// Checks conditional request headers.
fn check_conditional_headers(ctx: &RequestContext, blob: &BlobModel) -> StorageResult<()> {
    // If-Match
    if let Some(etag) = ctx.if_match() {
        if etag != "*" && etag != blob.properties.etag {
            return Err(StorageError::new(ErrorCode::ConditionNotMet));
        }
    }

    // If-None-Match
    if let Some(etag) = ctx.if_none_match() {
        if etag == "*" || etag == blob.properties.etag {
            return Err(StorageError::new(ErrorCode::ConditionNotMet));
        }
    }

    // If-Modified-Since
    if let Some(since) = ctx.if_modified_since() {
        if blob.properties.last_modified <= since {
            return Err(StorageError::new(ErrorCode::ConditionNotMet));
        }
    }

    // If-Unmodified-Since
    if let Some(since) = ctx.if_unmodified_since() {
        if blob.properties.last_modified > since {
            return Err(StorageError::new(ErrorCode::ConditionNotMet));
        }
    }

    Ok(())
}

/// Parsed copy source URL components.
struct CopySourceParts {
    account: String,
    container: String,
    blob: String,
    snapshot: String,
}

/// Parses a copy source URL.
fn parse_copy_source(url: &str) -> StorageResult<CopySourceParts> {
    // Handle both full URLs and relative paths
    let path = if url.starts_with("http://") || url.starts_with("https://") {
        url::Url::parse(url)
            .map_err(|_| StorageError::new(ErrorCode::InvalidSourceBlobUrl))?
            .path()
            .to_string()
    } else {
        url.to_string()
    };

    let parts: Vec<&str> = path.trim_start_matches('/').splitn(3, '/').collect();

    if parts.len() < 3 {
        return Err(StorageError::new(ErrorCode::InvalidSourceBlobUrl));
    }

    let account = parts[0].to_string();
    let container = parts[1].to_string();
    let blob_and_query = parts[2];

    let (blob, snapshot) = if let Some(idx) = blob_and_query.find('?') {
        let blob = &blob_and_query[..idx];
        let query = &blob_and_query[idx + 1..];
        let snapshot = query
            .split('&')
            .find(|s| s.starts_with("snapshot="))
            .map(|s| s.strip_prefix("snapshot=").unwrap_or(""))
            .unwrap_or("")
            .to_string();
        (blob.to_string(), snapshot)
    } else {
        (blob_and_query.to_string(), String::new())
    };

    Ok(CopySourceParts {
        account,
        container,
        blob,
        snapshot,
    })
}

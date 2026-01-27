//! Append blob handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use std::sync::Arc;

use crate::context::{format_http_date, RequestContext};
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{BlobModel, BlobType};
use crate::storage::{ExtentStore, MetadataStore};

use super::{add_blob_headers, blob::check_blob_lease, build_response, common_headers};

/// Maximum number of append blocks (50,000).
const MAX_APPEND_BLOCK_COUNT: u32 = 50_000;
/// Maximum size of a single append block (100 MiB).
const MAX_APPEND_BLOCK_SIZE: u64 = 100 * 1024 * 1024;

/// PUT /{container}/{blob} (x-ms-blob-type: AppendBlob) - Create append blob.
pub async fn create_append_blob(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx
        .blob
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    // Verify container exists
    if !metadata.container_exists(&ctx.account, container).await {
        return Err(StorageError::new(ErrorCode::ContainerNotFound));
    }

    // Check if blob exists and validate lease
    if let Ok(existing_blob) = metadata.get_blob(&ctx.account, container, blob_name, "").await {
        check_blob_lease(&existing_blob, ctx.lease_id())?;
    }

    // Create append blob model
    let mut blob = BlobModel::new(
        ctx.account.clone(),
        container.clone(),
        blob_name.clone(),
        BlobType::AppendBlob,
        0, // Initial size is 0
    );

    // Set content properties from headers
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

    // Initialize append blob specific properties
    blob.properties.committed_block_count = Some(0);
    blob.properties.is_sealed = Some(false);

    // Set access tier
    if let Some(tier) = ctx.header("x-ms-access-tier") {
        if let Some(t) = crate::models::AccessTier::from_str(tier) {
            blob.properties.access_tier = t;
        }
    }

    // Set metadata
    blob.metadata = ctx.metadata();

    // Create blob
    metadata.create_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-request-server-encrypted",
        HeaderValue::from_static("true"),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=appendblock - Append block.
pub async fn append_block(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let container = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx
        .blob
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let block_size = body.len() as u64;

    // Validate block size
    if block_size > MAX_APPEND_BLOCK_SIZE {
        return Err(StorageError::with_message(
            ErrorCode::RequestBodyTooLarge,
            format!("Append block size cannot exceed {} bytes", MAX_APPEND_BLOCK_SIZE),
        ));
    }

    let mut blob = metadata
        .get_blob(&ctx.account, container, blob_name, "")
        .await?;

    // Verify blob type
    if blob.properties.blob_type != BlobType::AppendBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Check if blob is sealed
    if blob.properties.is_sealed == Some(true) {
        return Err(StorageError::with_message(
            ErrorCode::InvalidOperation,
            "Cannot append to a sealed blob",
        ));
    }

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Check block count limit
    let current_block_count = blob.properties.committed_block_count.unwrap_or(0);
    if current_block_count >= MAX_APPEND_BLOCK_COUNT {
        return Err(StorageError::new(ErrorCode::BlockCountExceedsLimit));
    }

    // Check appendpos condition
    if let Some(expected_pos) = ctx.header("x-ms-blob-condition-appendpos") {
        let expected: u64 = expected_pos.parse().map_err(|_| {
            StorageError::new(ErrorCode::InvalidHeaderValue)
        })?;
        if blob.properties.content_length != expected {
            return Err(StorageError::new(ErrorCode::AppendPositionConditionNotMet));
        }
    }

    // Check maxsize condition
    if let Some(max_size) = ctx.header("x-ms-blob-condition-maxsize") {
        let max: u64 = max_size.parse().map_err(|_| {
            StorageError::new(ErrorCode::InvalidHeaderValue)
        })?;
        if blob.properties.content_length + block_size > max {
            return Err(StorageError::new(ErrorCode::MaxBlobSizeConditionNotMet));
        }
    }

    let append_offset = blob.properties.content_length;

    // Store block data
    let extent_chunk = extents.write(body).await?;

    // Update blob
    blob.extent_chunks.push(extent_chunk);
    blob.properties.content_length += block_size;
    blob.properties.committed_block_count = Some(current_block_count + 1);
    blob.properties.update_etag();

    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-append-offset",
        HeaderValue::from_str(&append_offset.to_string()).unwrap(),
    );
    headers.insert(
        "x-ms-blob-committed-block-count",
        HeaderValue::from_str(&blob.properties.committed_block_count.unwrap_or(0).to_string())
            .unwrap(),
    );
    headers.insert(
        "x-ms-request-server-encrypted",
        HeaderValue::from_static("true"),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=appendblock&fromURL - Append block from URL.
pub async fn append_block_from_url(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
) -> StorageResult<Response<Body>> {
    // Simplified implementation - would need to fetch from source URL
    Err(StorageError::with_message(
        ErrorCode::InvalidOperation,
        "Append block from URL not implemented",
    ))
}

/// PUT /{container}/{blob}?comp=seal - Seal append blob.
pub async fn seal_append_blob(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let container = ctx
        .container
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::ContainerNotFound))?;
    let blob_name = ctx
        .blob
        .as_ref()
        .ok_or_else(|| StorageError::new(ErrorCode::BlobNotFound))?;

    let mut blob = metadata
        .get_blob(&ctx.account, container, blob_name, "")
        .await?;

    // Verify blob type
    if blob.properties.blob_type != BlobType::AppendBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Check if already sealed
    if blob.properties.is_sealed == Some(true) {
        return Err(StorageError::with_message(
            ErrorCode::InvalidOperation,
            "Blob is already sealed",
        ));
    }

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Seal the blob
    blob.properties.is_sealed = Some(true);
    blob.properties.update_etag();

    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert("x-ms-blob-sealed", HeaderValue::from_static("true"));

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

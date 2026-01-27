//! Page blob handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use std::sync::Arc;

use crate::context::{format_http_date, RequestContext};
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{
    BlobModel, BlobType, ExtentChunk, PageRange, PageRangeDiff, PAGE_SIZE,
};
use crate::storage::{ExtentStore, MetadataStore};
use crate::xml::serialize::{serialize_page_ranges, serialize_page_ranges_diff};

use super::{add_blob_headers, blob::check_blob_lease, build_response, common_headers};

/// PUT /{container}/{blob} (x-ms-blob-type: PageBlob) - Create page blob.
pub async fn create_page_blob(
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

    // Get content length (required for page blobs)
    let content_length: u64 = ctx
        .header("x-ms-blob-content-length")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    // Content length must be aligned to 512 bytes
    if content_length % PAGE_SIZE != 0 {
        return Err(StorageError::with_message(
            ErrorCode::InvalidHeaderValue,
            "Page blob size must be aligned to 512 bytes",
        ));
    }

    // Check if blob exists and validate lease
    if let Ok(existing_blob) = metadata.get_blob(&ctx.account, container, blob_name, "").await {
        check_blob_lease(&existing_blob, ctx.lease_id())?;
    }

    // Create page blob model
    let mut blob = BlobModel::new(
        ctx.account.clone(),
        container.clone(),
        blob_name.clone(),
        BlobType::PageBlob,
        content_length,
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

    // Set sequence number
    if let Some(seq) = ctx.header("x-ms-blob-sequence-number") {
        blob.properties.sequence_number = seq.parse().ok();
    } else {
        blob.properties.sequence_number = Some(0);
    }

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

/// PUT /{container}/{blob}?comp=page (x-ms-page-write: update) - Upload pages.
pub async fn upload_pages(
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

    let page_write = ctx.header("x-ms-page-write").unwrap_or("update");

    let mut blob = metadata
        .get_blob(&ctx.account, container, blob_name, "")
        .await?;

    // Verify blob type
    if blob.properties.blob_type != BlobType::PageBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Parse range
    let (start, end) = ctx
        .range()
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;
    let end = end.ok_or_else(|| StorageError::new(ErrorCode::InvalidRange))?;

    // Validate alignment
    if start % PAGE_SIZE != 0 || (end + 1) % PAGE_SIZE != 0 {
        return Err(StorageError::with_message(
            ErrorCode::InvalidPageRange,
            "Page ranges must be aligned to 512 bytes",
        ));
    }

    // Validate range is within blob size
    if end >= blob.properties.content_length {
        return Err(StorageError::new(ErrorCode::InvalidPageRange));
    }

    // Check sequence number conditions
    if let Some(if_seq_le) = ctx.header("x-ms-if-sequence-number-le") {
        let expected: u64 = if_seq_le.parse().map_err(|_| {
            StorageError::new(ErrorCode::InvalidHeaderValue)
        })?;
        if blob.properties.sequence_number.unwrap_or(0) > expected {
            return Err(StorageError::new(ErrorCode::SequenceNumberConditionNotMet));
        }
    }
    if let Some(if_seq_lt) = ctx.header("x-ms-if-sequence-number-lt") {
        let expected: u64 = if_seq_lt.parse().map_err(|_| {
            StorageError::new(ErrorCode::InvalidHeaderValue)
        })?;
        if blob.properties.sequence_number.unwrap_or(0) >= expected {
            return Err(StorageError::new(ErrorCode::SequenceNumberConditionNotMet));
        }
    }
    if let Some(if_seq_eq) = ctx.header("x-ms-if-sequence-number-eq") {
        let expected: u64 = if_seq_eq.parse().map_err(|_| {
            StorageError::new(ErrorCode::InvalidHeaderValue)
        })?;
        if blob.properties.sequence_number.unwrap_or(0) != expected {
            return Err(StorageError::new(ErrorCode::SequenceNumberConditionNotMet));
        }
    }

    if page_write == "update" {
        // Store page data
        let extent_chunk = extents.write(body).await?;

        // Simplified page management - in a full implementation, we'd need to
        // track page ranges and merge/split as needed
        // For now, we'll just append the extent chunk
        blob.extent_chunks.push(extent_chunk);
    } else if page_write == "clear" {
        // Clear pages - in a full implementation, we'd mark the range as cleared
    }

    blob.properties.update_etag();
    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-sequence-number",
        HeaderValue::from_str(&blob.properties.sequence_number.unwrap_or(0).to_string()).unwrap(),
    );
    headers.insert(
        "x-ms-request-server-encrypted",
        HeaderValue::from_static("true"),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=page (x-ms-page-write: clear) - Clear pages.
pub async fn clear_pages(
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
    if blob.properties.blob_type != BlobType::PageBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    // Parse range
    let (start, end) = ctx
        .range()
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;
    let end = end.ok_or_else(|| StorageError::new(ErrorCode::InvalidRange))?;

    // Validate alignment
    if start % PAGE_SIZE != 0 || (end + 1) % PAGE_SIZE != 0 {
        return Err(StorageError::with_message(
            ErrorCode::InvalidPageRange,
            "Page ranges must be aligned to 512 bytes",
        ));
    }

    // In a full implementation, we'd mark the page range as cleared
    blob.properties.update_etag();
    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-sequence-number",
        HeaderValue::from_str(&blob.properties.sequence_number.unwrap_or(0).to_string()).unwrap(),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// GET /{container}/{blob}?comp=pagelist - Get page ranges.
pub async fn get_page_ranges(
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
    let snapshot = ctx.snapshot().unwrap_or("");

    let blob = metadata
        .get_blob(&ctx.account, container, blob_name, snapshot)
        .await?;

    // Verify blob type
    if blob.properties.blob_type != BlobType::PageBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Build page ranges from extent chunks
    // Simplified - in a full implementation, we'd track actual page ranges
    let ranges: Vec<PageRange> = if !blob.extent_chunks.is_empty() {
        // Return a single range covering all written data
        let total_size: u64 = blob.extent_chunks.iter().map(|c| c.count).sum();
        if total_size > 0 {
            vec![PageRange::new(0, total_size - 1)]
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    let xml = serialize_page_ranges(&ranges);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-content-length",
        HeaderValue::from_str(&blob.properties.content_length.to_string()).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// GET /{container}/{blob}?comp=pagelist&prevsnapshot={snapshot} - Get page ranges diff.
pub async fn get_page_ranges_diff(
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

    let prev_snapshot = ctx
        .query_param("prevsnapshot")
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredQueryParameter))?;

    let current_snapshot = ctx.snapshot().unwrap_or("");

    let current_blob = metadata
        .get_blob(&ctx.account, container, blob_name, current_snapshot)
        .await?;
    let _prev_blob = metadata
        .get_blob(&ctx.account, container, blob_name, prev_snapshot)
        .await?;

    // Verify blob type
    if current_blob.properties.blob_type != BlobType::PageBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Simplified diff - in a full implementation, we'd compare actual page ranges
    let ranges: Vec<PageRangeDiff> = vec![];

    let xml = serialize_page_ranges_diff(&ranges);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));
    add_blob_headers(
        &mut headers,
        &current_blob.properties.etag,
        &current_blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-content-length",
        HeaderValue::from_str(&current_blob.properties.content_length.to_string()).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// PUT /{container}/{blob}?comp=properties (resize) - Resize page blob.
pub async fn resize_page_blob(
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

    let new_size: u64 = ctx
        .header("x-ms-blob-content-length")
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    // Validate alignment
    if new_size % PAGE_SIZE != 0 {
        return Err(StorageError::with_message(
            ErrorCode::InvalidHeaderValue,
            "Page blob size must be aligned to 512 bytes",
        ));
    }

    let mut blob = metadata
        .get_blob(&ctx.account, container, blob_name, "")
        .await?;

    // Verify blob type
    if blob.properties.blob_type != BlobType::PageBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    blob.properties.content_length = new_size;
    blob.properties.update_etag();

    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-sequence-number",
        HeaderValue::from_str(&blob.properties.sequence_number.unwrap_or(0).to_string()).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=properties (sequence number) - Update sequence number.
pub async fn update_sequence_number(
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

    let action = ctx
        .header("x-ms-sequence-number-action")
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    let mut blob = metadata
        .get_blob(&ctx.account, container, blob_name, "")
        .await?;

    // Verify blob type
    if blob.properties.blob_type != BlobType::PageBlob {
        return Err(StorageError::new(ErrorCode::InvalidBlobType));
    }

    // Check lease
    check_blob_lease(&blob, ctx.lease_id())?;

    let current_seq = blob.properties.sequence_number.unwrap_or(0);

    match action.to_lowercase().as_str() {
        "max" => {
            let new_seq: u64 = ctx
                .header("x-ms-blob-sequence-number")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;
            blob.properties.sequence_number = Some(current_seq.max(new_seq));
        }
        "update" => {
            let new_seq: u64 = ctx
                .header("x-ms-blob-sequence-number")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;
            blob.properties.sequence_number = Some(new_seq);
        }
        "increment" => {
            blob.properties.sequence_number = Some(current_seq + 1);
        }
        _ => {
            return Err(StorageError::with_message(
                ErrorCode::InvalidHeaderValue,
                "Invalid x-ms-sequence-number-action",
            ));
        }
    }

    blob.properties.update_etag();
    metadata.update_blob(blob.clone()).await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );
    headers.insert(
        "x-ms-blob-sequence-number",
        HeaderValue::from_str(&blob.properties.sequence_number.unwrap_or(0).to_string()).unwrap(),
    );

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=incrementalcopy - Incremental copy.
pub async fn copy_incremental(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    // Simplified implementation
    Err(StorageError::with_message(
        ErrorCode::InvalidOperation,
        "Incremental copy not implemented",
    ))
}

//! Block blob handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Response, StatusCode},
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use bytes::Bytes;
use md5::{Digest, Md5};
use std::sync::Arc;

use crate::context::{format_http_date, RequestContext};
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{BlobModel, BlobType, BlockModel, BlockState, ExtentChunk};
use crate::storage::{ExtentStore, MetadataStore};
use crate::xml::{deserialize::BlockListRequest, serialize::serialize_block_list};

use super::{add_blob_headers, blob::check_blob_lease, build_response, common_headers};

/// PUT /{container}/{blob} - Upload block blob (single PUT).
pub async fn upload_block_blob(
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

    // Verify container exists
    if !metadata.container_exists(&ctx.account, container).await {
        return Err(StorageError::new(ErrorCode::ContainerNotFound));
    }

    // Check if blob exists and validate lease
    if let Ok(existing_blob) = metadata.get_blob(&ctx.account, container, blob_name, "").await {
        check_blob_lease(&existing_blob, ctx.lease_id())?;
    }

    // Validate Content-MD5 if provided
    if let Some(expected_md5) = ctx.content_md5() {
        let computed_md5 = BASE64.encode(Md5::digest(&body));
        if computed_md5 != expected_md5 {
            return Err(StorageError::new(ErrorCode::Md5Mismatch));
        }
    }

    // Store blob data in extent store
    let content_length = body.len() as u64;
    let extent_chunk = if content_length > 0 {
        Some(extents.write(body).await?)
    } else {
        None
    };

    // Create blob model
    let mut blob = BlobModel::new(
        ctx.account.clone(),
        container.clone(),
        blob_name.clone(),
        BlobType::BlockBlob,
        content_length,
    );

    // Set content properties from headers
    if let Some(ct) = ctx.header("x-ms-blob-content-type").or_else(|| ctx.content_type()) {
        blob.properties.content_type = Some(ct.to_string());
    }
    if let Some(ce) = ctx.header("x-ms-blob-content-encoding") {
        blob.properties.content_encoding = Some(ce.to_string());
    }
    if let Some(cl) = ctx.header("x-ms-blob-content-language") {
        blob.properties.content_language = Some(cl.to_string());
    }
    if let Some(md5) = ctx.header("x-ms-blob-content-md5").or_else(|| ctx.content_md5()) {
        blob.properties.content_md5 = Some(md5.to_string());
    }
    if let Some(cd) = ctx.header("x-ms-blob-content-disposition") {
        blob.properties.content_disposition = Some(cd.to_string());
    }
    if let Some(cc) = ctx.header("x-ms-blob-cache-control") {
        blob.properties.cache_control = Some(cc.to_string());
    }

    // Set access tier
    if let Some(tier) = ctx.header("x-ms-access-tier") {
        if let Some(t) = crate::models::AccessTier::from_str(tier) {
            blob.properties.access_tier = t;
        }
    }

    // Set metadata
    blob.metadata = ctx.metadata();

    // Set extent chunks
    if let Some(chunk) = extent_chunk {
        blob.extent_chunks = vec![chunk];
    }

    // Create or update blob
    metadata.create_blob(blob.clone()).await?;

    // Clear any staged blocks for this blob
    metadata
        .delete_staged_blocks(&ctx.account, container, blob_name)
        .await?;

    let mut headers = common_headers();
    add_blob_headers(
        &mut headers,
        &blob.properties.etag,
        &blob.properties.last_modified,
    );

    // Compute and return Content-MD5 if we computed it
    let content_md5 = BASE64.encode(Md5::digest(&[]));
    headers.insert("Content-MD5", HeaderValue::from_str(&content_md5).unwrap());
    headers.insert(
        "x-ms-request-server-encrypted",
        HeaderValue::from_static("true"),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=block&blockid={id} - Stage block.
pub async fn stage_block(
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
    let block_id = ctx
        .query_param("blockid")
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredQueryParameter))?;

    // Verify container exists
    if !metadata.container_exists(&ctx.account, container).await {
        return Err(StorageError::new(ErrorCode::ContainerNotFound));
    }

    // Validate block ID (must be base64 encoded, <= 64 bytes when decoded)
    let decoded_id = BASE64.decode(block_id).map_err(|_| {
        StorageError::with_message(ErrorCode::InvalidBlockId, "Block ID must be base64 encoded")
    })?;
    if decoded_id.len() > 64 {
        return Err(StorageError::with_message(
            ErrorCode::InvalidBlockId,
            "Block ID must be <= 64 bytes when decoded",
        ));
    }

    // Check lease if blob exists
    if let Ok(existing_blob) = metadata.get_blob(&ctx.account, container, blob_name, "").await {
        check_blob_lease(&existing_blob, ctx.lease_id())?;
    }

    // Validate Content-MD5 if provided
    if let Some(expected_md5) = ctx.content_md5() {
        let computed_md5 = BASE64.encode(Md5::digest(&body));
        if computed_md5 != expected_md5 {
            return Err(StorageError::new(ErrorCode::Md5Mismatch));
        }
    }

    // Store block data
    let block_size = body.len() as u64;
    let extent_chunk = extents.write(body).await?;

    // Create block model
    let block = BlockModel::new(
        ctx.account.clone(),
        container.clone(),
        blob_name.clone(),
        block_id.to_string(),
        block_size,
        extent_chunk,
    );

    // Stage the block
    metadata.stage_block(block).await?;

    let mut headers = common_headers();
    headers.insert(
        "x-ms-request-server-encrypted",
        HeaderValue::from_static("true"),
    );
    headers.insert(
        "x-ms-content-crc64",
        HeaderValue::from_static("AAAAAAAAAA=="),
    );

    Ok(build_response(StatusCode::CREATED, headers, Body::empty()))
}

/// PUT /{container}/{blob}?comp=blocklist - Commit block list.
pub async fn commit_block_list(
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

    // Verify container exists
    if !metadata.container_exists(&ctx.account, container).await {
        return Err(StorageError::new(ErrorCode::ContainerNotFound));
    }

    // Check lease if blob exists
    let existing_blob = metadata
        .get_blob(&ctx.account, container, blob_name, "")
        .await
        .ok();
    if let Some(ref blob) = existing_blob {
        check_blob_lease(blob, ctx.lease_id())?;
    }

    // Parse block list from request body
    let xml = std::str::from_utf8(&body)
        .map_err(|_| StorageError::new(ErrorCode::InvalidXmlDocument))?;
    let block_list = BlockListRequest::parse(xml)?;

    // Get staged blocks
    let staged_blocks = metadata
        .get_staged_blocks(&ctx.account, container, blob_name)
        .await?;

    // Get committed blocks from existing blob (if any)
    let committed_chunks: Vec<(String, ExtentChunk, u64)> = existing_blob
        .as_ref()
        .map(|blob| {
            // This is simplified - in reality we'd need to track block IDs with committed blocks
            Vec::new()
        })
        .unwrap_or_default();

    // Build final extent chunks list
    let mut extent_chunks = Vec::new();
    let mut total_size = 0u64;

    for block_id in block_list
        .latest
        .iter()
        .chain(block_list.uncommitted.iter())
        .chain(block_list.committed.iter())
    {
        // Look for block in staged blocks
        if let Some(staged) = staged_blocks.iter().find(|b| &b.block_id == block_id) {
            extent_chunks.push(staged.extent_chunk.clone());
            total_size += staged.size;
        } else {
            // Block not found
            return Err(StorageError::with_message(
                ErrorCode::InvalidBlockList,
                format!("Block {} not found", block_id),
            ));
        }
    }

    // Create or update blob
    let mut blob = existing_blob.unwrap_or_else(|| {
        BlobModel::new(
            ctx.account.clone(),
            container.clone(),
            blob_name.clone(),
            BlobType::BlockBlob,
            0,
        )
    });

    blob.properties.content_length = total_size;
    blob.extent_chunks = extent_chunks;
    blob.properties.update_etag();

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

    // Set access tier
    if let Some(tier) = ctx.header("x-ms-access-tier") {
        if let Some(t) = crate::models::AccessTier::from_str(tier) {
            blob.properties.access_tier = t;
        }
    }

    // Set metadata
    let request_metadata = ctx.metadata();
    if !request_metadata.is_empty() {
        blob.metadata = request_metadata;
    }

    // Save blob
    metadata.create_blob(blob.clone()).await?;

    // Clear staged blocks
    metadata
        .delete_staged_blocks(&ctx.account, container, blob_name)
        .await?;

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

/// GET /{container}/{blob}?comp=blocklist - Get block list.
pub async fn get_block_list(
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

    let block_list_type = ctx
        .query_param("blocklisttype")
        .map(|s| crate::models::BlockListType::from_str(s))
        .unwrap_or(crate::models::BlockListType::All);

    // Get blob (may not exist yet if only staging blocks)
    let blob = metadata
        .get_blob(&ctx.account, container, blob_name, snapshot)
        .await
        .ok();

    // Get staged blocks
    let staged_blocks = if snapshot.is_empty() {
        metadata
            .get_staged_blocks(&ctx.account, container, blob_name)
            .await?
    } else {
        Vec::new()
    };

    // Build committed blocks list
    // In a full implementation, we'd track block IDs with the committed blob
    let committed_blocks: Vec<BlockModel> = Vec::new();

    let xml = serialize_block_list(&committed_blocks, &staged_blocks);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    if let Some(blob) = blob {
        add_blob_headers(
            &mut headers,
            &blob.properties.etag,
            &blob.properties.last_modified,
        );
        headers.insert(
            "x-ms-blob-content-length",
            HeaderValue::from_str(&blob.properties.content_length.to_string()).unwrap(),
        );
    }

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// PUT /{container}/{blob}?comp=block&blockid={id}&fromURL - Stage block from URL.
pub async fn stage_block_from_url(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
) -> StorageResult<Response<Body>> {
    // Simplified implementation - would need to fetch from source URL
    Err(StorageError::with_message(
        ErrorCode::InvalidOperation,
        "Stage block from URL not implemented",
    ))
}

/// PUT /{container}/{blob} with x-ms-copy-source-url - Put blob from URL.
pub async fn put_blob_from_url(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
) -> StorageResult<Response<Body>> {
    // This is similar to copy_blob but synchronous
    // For simplicity, we'll delegate to copy_blob
    super::blob::copy_blob(ctx, metadata, extents).await
}

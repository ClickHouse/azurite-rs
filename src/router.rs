//! Request routing for Azure Blob Storage API.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, Method, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::{delete, get, head, post, put},
    Router,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::authenticate;
use crate::config::Config;
use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::handlers;
use crate::storage::{ExtentStore, MetadataStore};

/// Application state shared between handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub metadata: Arc<dyn MetadataStore>,
    pub extents: Arc<dyn ExtentStore>,
}

/// Creates the main router for the blob service.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        // Service-level routes (no container/blob)
        .route("/", get(service_handler).put(service_handler).post(service_handler).head(service_handler))
        .route("/:account", get(service_handler).put(service_handler).post(service_handler).head(service_handler))
        // Container-level routes
        .route("/:account/:container", get(container_handler).put(container_handler).delete(container_handler).head(container_handler).post(container_handler))
        // Blob-level routes (with catch-all for blob path)
        .route("/:account/:container/*blob", get(blob_handler).put(blob_handler).delete(blob_handler).head(blob_handler).post(blob_handler))
        .with_state(state)
}

/// Handler for service-level operations.
async fn service_handler(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response<Body> {
    let ctx = match RequestContext::new(method.clone(), uri, headers.clone(), params, query) {
        Ok(ctx) => ctx,
        Err(e) => return e.into_response(),
    };

    // Authenticate
    if let Err(e) = authenticate(&ctx, &state.config) {
        return e.into_response();
    }

    let result = route_service_request(&ctx, &state, body).await;
    match result {
        Ok(response) => response,
        Err(e) => e.with_request_id(&ctx.request_id).into_response(),
    }
}

/// Handler for container-level operations.
async fn container_handler(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response<Body> {
    let ctx = match RequestContext::new(method.clone(), uri, headers.clone(), params, query) {
        Ok(ctx) => ctx,
        Err(e) => return e.into_response(),
    };

    // Authenticate
    if let Err(e) = authenticate(&ctx, &state.config) {
        return e.into_response();
    }

    let result = route_container_request(&ctx, &state, body).await;
    match result {
        Ok(response) => response,
        Err(e) => e.with_request_id(&ctx.request_id).into_response(),
    }
}

/// Handler for blob-level operations.
async fn blob_handler(
    State(state): State<AppState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    Path(params): Path<HashMap<String, String>>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response<Body> {
    let ctx = match RequestContext::new(method.clone(), uri, headers.clone(), params, query) {
        Ok(ctx) => ctx,
        Err(e) => return e.into_response(),
    };

    // Authenticate
    if let Err(e) = authenticate(&ctx, &state.config) {
        return e.into_response();
    }

    let result = route_blob_request(&ctx, &state, body).await;
    match result {
        Ok(response) => response,
        Err(e) => e.with_request_id(&ctx.request_id).into_response(),
    }
}

/// Routes service-level requests.
async fn route_service_request(
    ctx: &RequestContext,
    state: &AppState,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let restype = ctx.restype();
    let comp = ctx.comp();

    match (ctx.method.as_str(), restype, comp) {
        // List containers
        ("GET", None, Some("list")) => {
            handlers::list_containers(ctx, state.metadata.clone()).await
        }
        // Get service properties
        ("GET", Some("service"), Some("properties")) => {
            handlers::get_service_properties(ctx, state.metadata.clone()).await
        }
        // Set service properties
        ("PUT", Some("service"), Some("properties")) => {
            handlers::set_service_properties(ctx, state.metadata.clone(), body).await
        }
        // Get service stats
        ("GET", Some("service"), Some("stats")) => {
            handlers::get_service_stats(ctx).await
        }
        // Get account info
        ("GET" | "HEAD", Some("account"), Some("properties")) => {
            handlers::get_account_info(ctx).await
        }
        // Get user delegation key
        ("POST", Some("service"), Some("userdelegationkey")) => {
            handlers::get_user_delegation_key(ctx, body).await
        }
        // Filter blobs (service level)
        ("GET", None, Some("blobs")) => {
            handlers::filter_blobs_service(ctx, state.metadata.clone()).await
        }
        // Submit batch
        ("POST", None, Some("batch")) => {
            handlers::submit_batch(ctx, state.metadata.clone(), state.extents.clone(), body).await
        }
        _ => Err(StorageError::new(ErrorCode::UnsupportedHttpVerb)),
    }
}

/// Routes container-level requests.
async fn route_container_request(
    ctx: &RequestContext,
    state: &AppState,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let restype = ctx.restype();
    let comp = ctx.comp();

    match (ctx.method.as_str(), restype, comp) {
        // Create container
        ("PUT", Some("container"), None) => {
            handlers::create_container(ctx, state.metadata.clone()).await
        }
        // Delete container
        ("DELETE", Some("container"), None) => {
            handlers::delete_container(ctx, state.metadata.clone()).await
        }
        // Get container properties
        ("GET" | "HEAD", Some("container"), None) => {
            handlers::get_container_properties(ctx, state.metadata.clone()).await
        }
        // Set container metadata
        ("PUT", Some("container"), Some("metadata")) => {
            handlers::set_container_metadata(ctx, state.metadata.clone()).await
        }
        // Get container ACL
        ("GET", Some("container"), Some("acl")) => {
            handlers::get_container_acl(ctx, state.metadata.clone()).await
        }
        // Set container ACL
        ("PUT", Some("container"), Some("acl")) => {
            handlers::set_container_acl(ctx, state.metadata.clone(), body).await
        }
        // List blobs
        ("GET", Some("container"), Some("list")) => {
            handlers::list_blobs(ctx, state.metadata.clone()).await
        }
        // Container lease
        ("PUT", Some("container"), Some("lease")) => {
            handlers::container_lease(ctx, state.metadata.clone()).await
        }
        // Restore container
        ("PUT", Some("container"), Some("undelete")) => {
            handlers::restore_container(ctx, state.metadata.clone()).await
        }
        // Filter blobs (container level)
        ("GET", Some("container"), Some("blobs")) => {
            // Similar to list blobs but with tag filtering
            handlers::list_blobs(ctx, state.metadata.clone()).await
        }
        // Submit batch (container level)
        ("POST", Some("container"), Some("batch")) => {
            handlers::submit_batch(ctx, state.metadata.clone(), state.extents.clone(), body).await
        }
        _ => Err(StorageError::new(ErrorCode::UnsupportedHttpVerb)),
    }
}

/// Routes blob-level requests.
async fn route_blob_request(
    ctx: &RequestContext,
    state: &AppState,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let comp = ctx.comp();
    let blob_type = ctx.blob_type();

    match (ctx.method.as_str(), comp) {
        // Download blob
        ("GET", None) => {
            handlers::download_blob(ctx, state.metadata.clone(), state.extents.clone()).await
        }
        // Get blob properties
        ("HEAD", None) => {
            handlers::get_blob_properties(ctx, state.metadata.clone()).await
        }
        // Delete blob
        ("DELETE", None) => {
            handlers::delete_blob(ctx, state.metadata.clone(), state.extents.clone()).await
        }
        // Upload blob or copy
        ("PUT", None) => {
            if ctx.copy_source().is_some() {
                handlers::copy_blob(ctx, state.metadata.clone(), state.extents.clone()).await
            } else {
                match blob_type {
                    Some("PageBlob") => {
                        handlers::create_page_blob(ctx, state.metadata.clone()).await
                    }
                    Some("AppendBlob") => {
                        handlers::create_append_blob(ctx, state.metadata.clone()).await
                    }
                    _ => {
                        handlers::upload_block_blob(ctx, state.metadata.clone(), state.extents.clone(), body).await
                    }
                }
            }
        }
        // Stage block
        ("PUT", Some("block")) => {
            if ctx.query_param("fromURL").is_some() {
                handlers::stage_block_from_url(ctx, state.metadata.clone(), state.extents.clone()).await
            } else {
                handlers::stage_block(ctx, state.metadata.clone(), state.extents.clone(), body).await
            }
        }
        // Commit block list
        ("PUT", Some("blocklist")) => {
            handlers::commit_block_list(ctx, state.metadata.clone(), state.extents.clone(), body).await
        }
        // Get block list
        ("GET", Some("blocklist")) => {
            handlers::get_block_list(ctx, state.metadata.clone()).await
        }
        // Page operations
        ("PUT", Some("page")) => {
            let page_write = ctx.header("x-ms-page-write").unwrap_or("update");
            if page_write == "clear" {
                handlers::clear_pages(ctx, state.metadata.clone()).await
            } else {
                handlers::upload_pages(ctx, state.metadata.clone(), state.extents.clone(), body).await
            }
        }
        // Get page ranges
        ("GET", Some("pagelist")) => {
            if ctx.query_param("prevsnapshot").is_some() {
                handlers::get_page_ranges_diff(ctx, state.metadata.clone()).await
            } else {
                handlers::get_page_ranges(ctx, state.metadata.clone()).await
            }
        }
        // Append block
        ("PUT", Some("appendblock")) => {
            if ctx.query_param("fromUrl").is_some() || ctx.query_param("fromURL").is_some() {
                handlers::append_block_from_url(ctx, state.metadata.clone(), state.extents.clone()).await
            } else {
                handlers::append_block(ctx, state.metadata.clone(), state.extents.clone(), body).await
            }
        }
        // Seal append blob
        ("PUT", Some("seal")) => {
            handlers::seal_append_blob(ctx, state.metadata.clone()).await
        }
        // Set blob properties
        ("PUT", Some("properties")) => {
            // Check if this is a page blob resize or sequence number update
            if ctx.header("x-ms-blob-content-length").is_some() {
                handlers::resize_page_blob(ctx, state.metadata.clone()).await
            } else if ctx.header("x-ms-sequence-number-action").is_some() {
                handlers::update_sequence_number(ctx, state.metadata.clone()).await
            } else {
                handlers::set_blob_properties(ctx, state.metadata.clone()).await
            }
        }
        // Set blob metadata
        ("PUT", Some("metadata")) => {
            handlers::set_blob_metadata(ctx, state.metadata.clone()).await
        }
        // Blob lease
        ("PUT", Some("lease")) => {
            handlers::blob_lease(ctx, state.metadata.clone()).await
        }
        // Create snapshot
        ("PUT", Some("snapshot")) => {
            handlers::create_snapshot(ctx, state.metadata.clone()).await
        }
        // Abort copy
        ("PUT", Some("copy")) => {
            handlers::abort_copy(ctx, state.metadata.clone()).await
        }
        // Set tier
        ("PUT", Some("tier")) => {
            handlers::set_blob_tier(ctx, state.metadata.clone()).await
        }
        // Get tags
        ("GET", Some("tags")) => {
            handlers::get_blob_tags(ctx, state.metadata.clone()).await
        }
        // Set tags
        ("PUT", Some("tags")) => {
            handlers::set_blob_tags(ctx, state.metadata.clone(), body).await
        }
        // Undelete blob
        ("PUT", Some("undelete")) => {
            handlers::undelete_blob(ctx, state.metadata.clone()).await
        }
        // Incremental copy (page blob)
        ("PUT", Some("incrementalcopy")) => {
            handlers::copy_incremental(ctx, state.metadata.clone()).await
        }
        // Query blob
        ("POST", Some("query")) => {
            // Simplified - return the blob content as-is
            handlers::download_blob(ctx, state.metadata.clone(), state.extents.clone()).await
        }
        _ => Err(StorageError::new(ErrorCode::UnsupportedHttpVerb)),
    }
}

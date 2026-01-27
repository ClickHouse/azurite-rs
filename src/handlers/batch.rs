//! Batch operation handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use std::sync::Arc;

use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::storage::{ExtentStore, MetadataStore};

use super::{build_response, common_headers};

/// POST /?comp=batch or POST /{container}?restype=container&comp=batch - Submit batch.
pub async fn submit_batch(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    // Parse the multipart/mixed request
    let content_type = ctx
        .content_type()
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    if !content_type.starts_with("multipart/mixed") {
        return Err(StorageError::with_message(
            ErrorCode::InvalidHeaderValue,
            "Content-Type must be multipart/mixed for batch requests",
        ));
    }

    // Extract boundary from content type
    let boundary = content_type
        .split(';')
        .find_map(|part| {
            let part = part.trim();
            if part.starts_with("boundary=") {
                Some(part.strip_prefix("boundary=").unwrap().trim_matches('"'))
            } else {
                None
            }
        })
        .ok_or_else(|| {
            StorageError::with_message(
                ErrorCode::InvalidHeaderValue,
                "Missing boundary in Content-Type",
            )
        })?;

    // Simplified batch response - in a full implementation, we'd parse and execute
    // each sub-request and return the results
    let response_boundary = format!("batchresponse_{}", uuid::Uuid::new_v4());

    let response_body = format!(
        "--{}\r\n\
         Content-Type: application/http\r\n\
         Content-Transfer-Encoding: binary\r\n\r\n\
         HTTP/1.1 202 Accepted\r\n\
         x-ms-request-id: {}\r\n\
         x-ms-version: 2021-10-04\r\n\r\n\
         --{}--",
        response_boundary,
        uuid::Uuid::new_v4(),
        response_boundary
    );

    let mut headers = common_headers();
    headers.insert(
        "Content-Type",
        HeaderValue::from_str(&format!("multipart/mixed; boundary={}", response_boundary)).unwrap(),
    );

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::from(response_body)))
}

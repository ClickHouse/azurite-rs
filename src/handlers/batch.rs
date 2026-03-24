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
    _extents: Arc<dyn ExtentStore>,
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

    let body_str = String::from_utf8_lossy(&body);

    let account = &ctx.account;
    let container = ctx.container.as_deref().unwrap_or("");

    // Parse sub-requests from the multipart body to extract Content-ID and DELETE paths
    let subrequests = parse_batch_subrequests(&body_str, boundary, account, container);

    let response_boundary = format!("batchresponse_{}", uuid::Uuid::new_v4());

    // Build response parts for each sub-request
    let mut response_body = String::new();

    for subrequest in &subrequests {
        let blob_path = &subrequest.path;

        // Try to delete the blob
        let (status_code, status_text) = match metadata.delete_blob(account, container, blob_path, "").await {
            Ok(_) => (202, "Accepted"),
            Err(e) => match e.code {
                ErrorCode::BlobNotFound => (404, "The specified blob does not exist."),
                ErrorCode::ContainerNotFound => (404, "The specified container does not exist."),
                _ => (404, "The specified blob does not exist."),
            },
        };

        response_body.push_str(&format!(
            "--{}\r\n\
             Content-Type: application/http\r\n\
             Content-Transfer-Encoding: binary\r\n\
             Content-ID: {}\r\n\
             \r\n\
             HTTP/1.1 {} {}\r\n\
             x-ms-request-id: {}\r\n\
             x-ms-version: 2021-10-04\r\n\
             \r\n",
            response_boundary,
            subrequest.content_id,
            status_code,
            status_text,
            uuid::Uuid::new_v4(),
        ));
    }

    // If no subrequests were found, still return a valid response
    if subrequests.is_empty() {
        response_body.push_str(&format!(
            "--{}\r\n\
             Content-Type: application/http\r\n\
             Content-Transfer-Encoding: binary\r\n\
             Content-ID: 0\r\n\
             \r\n\
             HTTP/1.1 202 Accepted\r\n\
             x-ms-request-id: {}\r\n\
             x-ms-version: 2021-10-04\r\n\
             \r\n",
            response_boundary,
            uuid::Uuid::new_v4(),
        ));
    }

    response_body.push_str(&format!("--{}--\r\n", response_boundary));

    let mut headers = common_headers();
    headers.insert(
        "Content-Type",
        HeaderValue::from_str(&format!("multipart/mixed; boundary={}", response_boundary)).unwrap(),
    );

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::from(response_body)))
}

/// A parsed sub-request from a batch body.
struct BatchSubrequest {
    content_id: String,
    path: String,
}

/// Parse batch sub-requests from a multipart/mixed body.
/// Extracts Content-ID and the blob path from each DELETE sub-request.
fn parse_batch_subrequests(body: &str, boundary: &str, account: &str, container: &str) -> Vec<BatchSubrequest> {
    let mut subrequests = Vec::new();
    let delimiter = format!("--{}", boundary);

    for part in body.split(&delimiter) {
        let part = part.trim();
        if part.is_empty() || part == "--" || part.starts_with("--") {
            continue;
        }

        // Extract Content-ID
        let content_id = part
            .lines()
            .find_map(|line| {
                let line = line.trim();
                if line.starts_with("Content-ID:") {
                    Some(line.strip_prefix("Content-ID:").unwrap().trim().to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "0".to_string());

        // Find the HTTP request line (e.g., "DELETE /account/container/blob HTTP/1.1")
        let path = part
            .lines()
            .find_map(|line| {
                let line = line.trim();
                if line.starts_with("DELETE ") {
                    // Extract path from "DELETE /path?query HTTP/1.1"
                    let tokens: Vec<&str> = line.splitn(3, ' ').collect();
                    if tokens.len() >= 2 {
                        let full_path = tokens[1];
                        // Strip query string (e.g., trailing "?")
                        let path = full_path.split('?').next().unwrap_or(full_path);
                        // URL-decode (e.g., %20 -> space)
                        let path = percent_encoding::percent_decode_str(path)
                            .decode_utf8_lossy();
                        // Remove leading slash
                        let path = path.trim_start_matches('/');
                        // Path format: account/container/blob_path
                        // The blob_path may contain slashes (e.g., "path/to/blob.txt").
                        // Strip the known account and container prefix.
                        let remainder = path
                            .strip_prefix(account)
                            .and_then(|p| p.strip_prefix('/'))
                            .unwrap_or(path);
                        let remainder = remainder
                            .strip_prefix(container)
                            .and_then(|p| p.strip_prefix('/'))
                            .unwrap_or(remainder);
                        if !remainder.is_empty() {
                            Some(remainder.to_string())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap_or_default();

        if !path.is_empty() {
            subrequests.push(BatchSubrequest { content_id, path });
        }
    }

    subrequests
}

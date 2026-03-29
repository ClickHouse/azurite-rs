//! Batch operation handlers for Azure Blob Storage API.
//!
//! Implements the Azure Blob Batch API using a single-level multipart/mixed structure
//! matching the official Azurite and Azure C++ SDK expectations.
//! Each sub-request has a Content-ID (zero-based integer) that is echoed in the response.

use axum::{
    body::Body,
    http::{HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use percent_encoding::percent_decode_str;
use std::sync::Arc;

use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::storage::{ExtentStore, MetadataStore};

use super::{build_response, common_headers};

/// A parsed sub-request from a batch body.
struct SubRequest {
    content_id: String,
    method: String,
    path: String,
}

/// POST /?comp=batch or POST /{container}?restype=container&comp=batch - Submit batch.
pub async fn submit_batch(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let content_type = ctx
        .content_type()
        .ok_or_else(|| StorageError::new(ErrorCode::MissingRequiredHeader))?;

    if !content_type.starts_with("multipart/mixed") {
        return Err(StorageError::with_message(
            ErrorCode::InvalidHeaderValue,
            "Content-Type must be multipart/mixed for batch requests",
        ));
    }

    let batch_boundary = extract_boundary(content_type)?;

    let body_str = std::str::from_utf8(&body)
        .map_err(|_| StorageError::new(ErrorCode::InvalidInput))?;

    // Parse sub-requests from the batch body.
    // The body may have either a flat structure (sub-requests directly in the batch boundary)
    // or a nested changeset structure. We handle both.
    let sub_requests = parse_batch_body(body_str, &batch_boundary);

    // Execute each sub-request and build a flat multipart response.
    // The response uses the same boundary as the request (matching official Azurite behavior).
    let response_boundary = batch_boundary.clone();

    let mut response_body = String::new();
    for req in &sub_requests {
        let (status_code, status_text, resp_headers, resp_body) = execute_sub_request(
            ctx, &metadata, &extents, &req.method, &req.path,
        ).await;

        response_body.push_str(&format!("--{}\r\n", response_boundary));
        response_body.push_str("Content-Type: application/http\r\n");
        response_body.push_str("Content-Transfer-Encoding: binary\r\n");
        response_body.push_str(&format!("Content-ID: {}\r\n", req.content_id));
        response_body.push_str("\r\n");
        response_body.push_str(&format!("HTTP/1.1 {} {}\r\n", status_code, status_text));
        response_body.push_str(&format!("x-ms-request-id: {}\r\n", uuid::Uuid::new_v4()));
        response_body.push_str("x-ms-version: 2021-10-04\r\n");
        for (name, value) in &resp_headers {
            response_body.push_str(&format!("{}: {}\r\n", name, value));
        }
        response_body.push_str("\r\n");
        if !resp_body.is_empty() {
            response_body.push_str(&resp_body);
            response_body.push_str("\r\n");
        }
    }
    response_body.push_str(&format!("--{}--", response_boundary));

    let mut headers = common_headers();
    headers.insert(
        "Content-Type",
        HeaderValue::from_str(&format!("multipart/mixed; boundary={}", response_boundary)).unwrap(),
    );

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::from(response_body)))
}

/// Extracts the boundary parameter from a Content-Type header value.
fn extract_boundary(content_type: &str) -> StorageResult<String> {
    content_type
        .split(';')
        .find_map(|part| {
            let part = part.trim();
            if let Some(rest) = part.strip_prefix("boundary=") {
                Some(rest.trim_matches('"').to_string())
            } else {
                None
            }
        })
        .ok_or_else(|| {
            StorageError::with_message(
                ErrorCode::InvalidHeaderValue,
                "Missing boundary in Content-Type",
            )
        })
}

/// Parses the batch body and returns all sub-requests.
/// Handles both flat and nested (changeset) structures.
fn parse_batch_body(body: &str, boundary: &str) -> Vec<SubRequest> {
    let parts = split_multipart(body, boundary);
    let mut sub_requests = Vec::new();

    for part in parts {
        // Check if this part is a changeset (nested multipart)
        if let Some(changeset_boundary) = extract_nested_boundary(part) {
            let changeset_parts = split_multipart(part, &changeset_boundary);
            for cp in changeset_parts {
                if let Some(req) = parse_sub_request(cp) {
                    sub_requests.push(req);
                }
            }
        } else if let Some(req) = parse_sub_request(part) {
            sub_requests.push(req);
        }
    }

    sub_requests
}

/// Splits a multipart body by the given boundary, returning the content of each part.
fn split_multipart<'a>(body: &'a str, boundary: &str) -> Vec<&'a str> {
    let delimiter = format!("--{}", boundary);
    let mut parts = Vec::new();
    let mut remaining = body;

    loop {
        let start = match remaining.find(&delimiter) {
            Some(pos) => pos,
            None => break,
        };

        let after_delim = &remaining[start + delimiter.len()..];

        // Check for end marker
        if after_delim.starts_with("--") {
            break;
        }

        // Skip past CRLF or LF after the delimiter
        let content_start = if after_delim.starts_with("\r\n") {
            &after_delim[2..]
        } else if after_delim.starts_with('\n') {
            &after_delim[1..]
        } else {
            after_delim
        };

        // Find the next delimiter
        let content_end = content_start
            .find(&delimiter)
            .unwrap_or(content_start.len());

        let part = content_start[..content_end].trim_end_matches("\r\n").trim_end_matches('\n');
        if !part.is_empty() {
            parts.push(part);
        }

        remaining = &content_start[content_end..];
    }

    parts
}

/// Extracts a nested multipart boundary from a part's Content-Type header line.
fn extract_nested_boundary(part: &str) -> Option<String> {
    for line in part.lines() {
        let line_lower = line.to_lowercase();
        if line_lower.starts_with("content-type:") && line_lower.contains("multipart/mixed") {
            return extract_boundary_from_line(line);
        }
    }
    None
}

/// Extracts boundary= value from a header line.
fn extract_boundary_from_line(line: &str) -> Option<String> {
    line.split(';').find_map(|s| {
        let s = s.trim();
        let lower = s.to_lowercase();
        if lower.starts_with("boundary=") {
            Some(s[9..].trim_matches('"').to_string())
        } else {
            None
        }
    })
}

/// Parses a sub-request part to extract Content-ID, method, and path.
fn parse_sub_request(part: &str) -> Option<SubRequest> {
    let mut content_id = String::new();
    let mut found_blank = false;
    let mut method = String::new();
    let mut path = String::new();

    for line in part.lines() {
        let trimmed = line.trim_end_matches('\r');
        if !found_blank {
            let lower = trimmed.to_lowercase();
            if lower.starts_with("content-id:") {
                content_id = trimmed.split_once(':')?.1.trim().to_string();
            } else if trimmed.is_empty() {
                found_blank = true;
            }
        } else {
            // After the blank line, look for the HTTP request line
            let trimmed = trimmed.trim();
            if !trimmed.is_empty() && method.is_empty() {
                // Parse "DELETE /account/container/blob HTTP/1.1"
                let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
                if parts.len() >= 2 {
                    method = parts[0].to_string();
                    path = parts[1].to_string();
                }
                break;
            }
        }
    }

    if method.is_empty() {
        return None;
    }

    Some(SubRequest {
        content_id,
        method,
        path,
    })
}

/// Executes a single sub-request.
/// Returns (status_code, status_text, extra_headers, body).
async fn execute_sub_request(
    ctx: &RequestContext,
    metadata: &Arc<dyn MetadataStore>,
    extents: &Arc<dyn ExtentStore>,
    method: &str,
    path: &str,
) -> (u16, &'static str, Vec<(&'static str, String)>, String) {
    // URL-decode the path since Azure SDK URL-encodes blob paths in batch sub-requests
    let decoded_path = percent_decode_str(path).decode_utf8_lossy();
    let path_clean = decoded_path.split('?').next().unwrap_or(&decoded_path);
    let segments: Vec<&str> = path_clean
        .trim_start_matches('/')
        .splitn(3, '/')
        .collect();

    let (account, container, blob_name) = match segments.len() {
        3 => (segments[0], segments[1], segments[2]),
        2 => {
            if let Some(ref ctx_container) = ctx.container {
                (ctx.account.as_str(), ctx_container.as_str(), segments[1])
            } else {
                (ctx.account.as_str(), segments[0], segments[1])
            }
        }
        _ => return (400, "Bad Request", vec![], String::new()),
    };

    match method {
        "DELETE" => {
            let blob = match metadata.get_blob(account, container, blob_name, "").await {
                Ok(blob) => blob,
                Err(_) => {
                    let error_body = format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n\
                         <Error>\n  <Code>BlobNotFound</Code>\n  \
                         <Message>The specified blob does not exist.\nRequestId:{}\n\
                         Time:{}</Message>\n</Error>",
                        uuid::Uuid::new_v4(),
                        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                    );
                    return (404, "The specified blob does not exist.", vec![], error_body);
                }
            };

            if let Err(_) = metadata.delete_blob(account, container, blob_name, "").await {
                return (404, "The specified blob does not exist.", vec![], String::new());
            }

            for chunk in &blob.extent_chunks {
                let _ = extents.delete(&chunk.id).await;
            }

            (202, "Accepted", vec![("x-ms-delete-type-permanent", "true".to_string())], String::new())
        }
        _ => (400, "Bad Request", vec![], String::new()),
    }
}

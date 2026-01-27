//! Request context extraction and handling.

use axum::{
    extract::{FromRequestParts, Path, Query},
    http::{header::HeaderMap, request::Parts, HeaderValue, Method, Uri},
};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::{ErrorCode, StorageError, StorageResult};

/// Extracted request context containing all relevant information.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// Unique request ID.
    pub request_id: String,
    /// HTTP method.
    pub method: Method,
    /// Request URI.
    pub uri: Uri,
    /// Account name extracted from path.
    pub account: String,
    /// Container name (if present).
    pub container: Option<String>,
    /// Blob name (if present).
    pub blob: Option<String>,
    /// Query parameters.
    pub query_params: HashMap<String, String>,
    /// Request headers.
    pub headers: HeaderMap,
    /// API version from x-ms-version header.
    pub api_version: Option<String>,
    /// Client request ID from x-ms-client-request-id header.
    pub client_request_id: Option<String>,
    /// Request timestamp.
    pub timestamp: DateTime<Utc>,
}

impl RequestContext {
    /// Creates a new request context from request parts.
    pub fn new(
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        path_params: HashMap<String, String>,
        query_params: HashMap<String, String>,
    ) -> StorageResult<Self> {
        let request_id = Uuid::new_v4().to_string();
        let timestamp = Utc::now();

        let account = path_params
            .get("account")
            .cloned()
            .unwrap_or_else(|| "devstoreaccount1".to_string());

        let container = path_params.get("container").cloned();
        let blob = path_params.get("blob").cloned();

        let api_version = headers
            .get("x-ms-version")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        let client_request_id = headers
            .get("x-ms-client-request-id")
            .and_then(|v| v.to_str().ok())
            .map(String::from);

        Ok(Self {
            request_id,
            method,
            uri,
            account,
            container,
            blob,
            query_params,
            headers,
            api_version,
            client_request_id,
            timestamp,
        })
    }

    /// Returns the value of a query parameter.
    pub fn query_param(&self, name: &str) -> Option<&str> {
        self.query_params.get(name).map(|s| s.as_str())
    }

    /// Returns the value of a header.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    /// Returns the x-ms-* headers sorted alphabetically.
    pub fn ms_headers(&self) -> Vec<(&str, &str)> {
        let mut headers: Vec<_> = self
            .headers
            .iter()
            .filter_map(|(name, value)| {
                let name_str = name.as_str();
                if name_str.starts_with("x-ms-") {
                    value.to_str().ok().map(|v| (name_str, v))
                } else {
                    None
                }
            })
            .collect();
        headers.sort_by(|a, b| a.0.cmp(b.0));
        headers
    }

    /// Returns the Content-MD5 header value.
    pub fn content_md5(&self) -> Option<&str> {
        self.header("content-md5")
    }

    /// Returns the Content-Length header value.
    pub fn content_length(&self) -> Option<u64> {
        self.header("content-length")
            .and_then(|v| v.parse().ok())
    }

    /// Returns the Content-Type header value.
    pub fn content_type(&self) -> Option<&str> {
        self.header("content-type")
    }

    /// Returns the Range header value parsed as (start, end).
    pub fn range(&self) -> Option<(u64, Option<u64>)> {
        self.header("range").or_else(|| self.header("x-ms-range")).and_then(parse_range_header)
    }

    /// Returns the If-Match header value.
    pub fn if_match(&self) -> Option<&str> {
        self.header("if-match")
    }

    /// Returns the If-None-Match header value.
    pub fn if_none_match(&self) -> Option<&str> {
        self.header("if-none-match")
    }

    /// Returns the If-Modified-Since header value.
    pub fn if_modified_since(&self) -> Option<DateTime<Utc>> {
        self.header("if-modified-since").and_then(parse_http_date)
    }

    /// Returns the If-Unmodified-Since header value.
    pub fn if_unmodified_since(&self) -> Option<DateTime<Utc>> {
        self.header("if-unmodified-since").and_then(parse_http_date)
    }

    /// Returns the x-ms-lease-id header value.
    pub fn lease_id(&self) -> Option<&str> {
        self.header("x-ms-lease-id")
    }

    /// Returns the x-ms-blob-type header value.
    pub fn blob_type(&self) -> Option<&str> {
        self.header("x-ms-blob-type")
    }

    /// Returns the x-ms-copy-source header value.
    pub fn copy_source(&self) -> Option<&str> {
        self.header("x-ms-copy-source")
    }

    /// Returns user-defined metadata from x-ms-meta-* headers.
    pub fn metadata(&self) -> HashMap<String, String> {
        self.headers
            .iter()
            .filter_map(|(name, value)| {
                let name_str = name.as_str();
                if let Some(key) = name_str.strip_prefix("x-ms-meta-") {
                    value.to_str().ok().map(|v| (key.to_string(), v.to_string()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the snapshot query parameter.
    pub fn snapshot(&self) -> Option<&str> {
        self.query_param("snapshot")
    }

    /// Returns the versionid query parameter.
    pub fn version_id(&self) -> Option<&str> {
        self.query_param("versionid")
    }

    /// Returns the timeout query parameter in seconds.
    pub fn timeout(&self) -> Option<u32> {
        self.query_param("timeout").and_then(|v| v.parse().ok())
    }

    /// Returns the restype query parameter.
    pub fn restype(&self) -> Option<&str> {
        self.query_param("restype")
    }

    /// Returns the comp query parameter.
    pub fn comp(&self) -> Option<&str> {
        self.query_param("comp")
    }

    /// Returns whether this is a service-level request.
    pub fn is_service_request(&self) -> bool {
        self.container.is_none()
    }

    /// Returns whether this is a container-level request.
    pub fn is_container_request(&self) -> bool {
        self.container.is_some() && self.blob.is_none()
    }

    /// Returns whether this is a blob-level request.
    pub fn is_blob_request(&self) -> bool {
        self.container.is_some() && self.blob.is_some()
    }
}

/// Parses a Range header value like "bytes=0-1023" or "bytes=0-".
fn parse_range_header(value: &str) -> Option<(u64, Option<u64>)> {
    let value = value.strip_prefix("bytes=")?;
    let parts: Vec<&str> = value.split('-').collect();
    if parts.len() != 2 {
        return None;
    }
    let start: u64 = parts[0].parse().ok()?;
    let end: Option<u64> = if parts[1].is_empty() {
        None
    } else {
        Some(parts[1].parse().ok()?)
    };
    Some((start, end))
}

/// Parses an HTTP date in RFC 1123 format.
fn parse_http_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            // Try other common formats
            chrono::NaiveDateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S GMT")
                .ok()
                .map(|dt| dt.and_utc())
        })
}

/// Query parameters for list operations.
#[derive(Debug, Clone, Default)]
pub struct ListParams {
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub marker: Option<String>,
    pub maxresults: Option<u32>,
    pub include: Vec<String>,
}

impl ListParams {
    pub fn from_query(query: &HashMap<String, String>) -> Self {
        let include = query
            .get("include")
            .map(|s| s.split(',').map(String::from).collect())
            .unwrap_or_default();

        Self {
            prefix: query.get("prefix").cloned(),
            delimiter: query.get("delimiter").cloned(),
            marker: query.get("marker").cloned(),
            maxresults: query.get("maxresults").and_then(|v| v.parse().ok()),
            include,
        }
    }
}

/// Formats a DateTime as RFC 1123 format for HTTP headers.
pub fn format_http_date(dt: &DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Formats a DateTime as ISO 8601 format.
pub fn format_iso8601(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S.%7fZ").to_string()
}

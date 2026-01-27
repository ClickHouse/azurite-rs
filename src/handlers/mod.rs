//! Request handlers for Azure Blob Storage API.

mod append_blob;
mod batch;
mod blob;
mod block_blob;
mod container;
mod page_blob;
mod service;

pub use append_blob::*;
pub use batch::*;
pub use blob::*;
pub use block_blob::*;
pub use container::*;
pub use page_blob::*;
pub use service::*;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Response, StatusCode};
use chrono::Utc;
use uuid::Uuid;

use crate::context::format_http_date;

/// Creates common response headers for Azure Blob Storage API responses.
pub fn common_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-ms-request-id", HeaderValue::from_str(&Uuid::new_v4().to_string()).unwrap());
    headers.insert("x-ms-version", HeaderValue::from_static("2021-10-04"));
    headers.insert("Date", HeaderValue::from_str(&format_http_date(&Utc::now())).unwrap());
    headers.insert("server", HeaderValue::from_static("Azurite-Blob/3.31.0"));
    headers
}

/// Adds ETag and Last-Modified headers.
pub fn add_blob_headers(headers: &mut HeaderMap, etag: &str, last_modified: &chrono::DateTime<Utc>) {
    headers.insert("ETag", HeaderValue::from_str(etag).unwrap());
    headers.insert("Last-Modified", HeaderValue::from_str(&format_http_date(last_modified)).unwrap());
}

/// Builds a response with the given status, headers, and body.
pub fn build_response(status: StatusCode, headers: HeaderMap, body: Body) -> Response<Body> {
    let mut response = Response::builder()
        .status(status)
        .body(body)
        .unwrap();
    *response.headers_mut() = headers;
    response
}

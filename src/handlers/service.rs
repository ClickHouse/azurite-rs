//! Service-level handlers for Azure Blob Storage API.

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Response, StatusCode},
};
use bytes::Bytes;
use std::sync::Arc;

use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{AccountKind, ServiceProperties, ServiceStats, SkuName, UserDelegationKey};
use crate::storage::MetadataStore;
use crate::xml::{
    deserialize::{parse_service_properties, parse_user_delegation_key_request},
    serialize::{
        serialize_container_list, serialize_service_properties, serialize_service_stats,
        serialize_user_delegation_key,
    },
};

use super::{build_response, common_headers};

/// GET /?comp=list - List containers.
pub async fn list_containers(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let prefix = ctx.query_param("prefix");
    let marker = ctx.query_param("marker");
    let maxresults = ctx
        .query_param("maxresults")
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000u32);

    let (containers, next_marker) = metadata
        .list_containers(&ctx.account, prefix, marker, Some(maxresults))
        .await?;

    let xml = serialize_container_list(
        &containers,
        prefix,
        marker,
        maxresults,
        next_marker.as_deref(),
        &ctx.account,
    );

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// GET /?restype=service&comp=properties - Get service properties.
pub async fn get_service_properties(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    let properties = metadata.get_service_properties(&ctx.account).await?;
    let xml = serialize_service_properties(&properties);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// PUT /?restype=service&comp=properties - Set service properties.
pub async fn set_service_properties(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let xml = std::str::from_utf8(&body)
        .map_err(|_| StorageError::new(ErrorCode::InvalidXmlDocument))?;

    let properties = parse_service_properties(xml)?;
    metadata
        .set_service_properties(&ctx.account, properties)
        .await?;

    let headers = common_headers();

    Ok(build_response(StatusCode::ACCEPTED, headers, Body::empty()))
}

/// GET /?restype=service&comp=stats - Get service statistics.
pub async fn get_service_stats(
    ctx: &RequestContext,
) -> StorageResult<Response<Body>> {
    let stats = ServiceStats::default();
    let xml = serialize_service_stats(&stats);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// GET/HEAD /?restype=account&comp=properties - Get account info.
pub async fn get_account_info(
    ctx: &RequestContext,
) -> StorageResult<Response<Body>> {
    let mut headers = common_headers();
    headers.insert("x-ms-sku-name", HeaderValue::from_static(SkuName::StandardLRS.as_str()));
    headers.insert("x-ms-account-kind", HeaderValue::from_static(AccountKind::StorageV2.as_str()));
    headers.insert("x-ms-is-hns-enabled", HeaderValue::from_static("false"));

    Ok(build_response(StatusCode::OK, headers, Body::empty()))
}

/// POST /?restype=service&comp=userdelegationkey - Get user delegation key.
pub async fn get_user_delegation_key(
    ctx: &RequestContext,
    body: Bytes,
) -> StorageResult<Response<Body>> {
    let xml = std::str::from_utf8(&body)
        .map_err(|_| StorageError::new(ErrorCode::InvalidXmlDocument))?;

    let (start, expiry) = parse_user_delegation_key_request(xml)?;

    // Generate a mock user delegation key
    let key = UserDelegationKey {
        signed_oid: uuid::Uuid::new_v4().to_string(),
        signed_tid: uuid::Uuid::new_v4().to_string(),
        signed_start: start,
        signed_expiry: expiry,
        signed_service: "b".to_string(),
        signed_version: "2021-10-04".to_string(),
        value: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            uuid::Uuid::new_v4().as_bytes(),
        ),
    };

    let xml = serialize_user_delegation_key(&key);

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

/// GET /?comp=blobs - Filter blobs (service level).
pub async fn filter_blobs_service(
    ctx: &RequestContext,
    metadata: Arc<dyn MetadataStore>,
) -> StorageResult<Response<Body>> {
    // This is a simplified implementation
    // Full implementation would support tag-based filtering across all containers
    let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<EnumerationResults>
  <Blobs />
</EnumerationResults>"#;

    let mut headers = common_headers();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));

    Ok(build_response(StatusCode::OK, headers, Body::from(xml)))
}

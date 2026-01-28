//! Blob/Container SAS token validation for Azure Blob Storage API.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

use crate::config::Config;
use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};

type HmacSha256 = Hmac<Sha256>;

/// Blob/Container SAS token parameters.
#[derive(Debug, Clone)]
pub struct BlobSasParameters {
    /// Signed version (sv).
    pub signed_version: String,
    /// Signed resource (sr) - b=blob, c=container, bs=blob snapshot, bv=blob version.
    pub signed_resource: String,
    /// Signed permissions (sp).
    pub signed_permissions: String,
    /// Signed expiry (se).
    pub signed_expiry: DateTime<Utc>,
    /// Signed start (st) - optional.
    pub signed_start: Option<DateTime<Utc>>,
    /// Signed IP (sip) - optional.
    pub signed_ip: Option<String>,
    /// Signed protocol (spr) - optional.
    pub signed_protocol: Option<String>,
    /// Signed identifier (si) - optional, for stored access policy.
    pub signed_identifier: Option<String>,
    /// Cache-Control override (rscc) - optional.
    pub cache_control: Option<String>,
    /// Content-Disposition override (rscd) - optional.
    pub content_disposition: Option<String>,
    /// Content-Encoding override (rsce) - optional.
    pub content_encoding: Option<String>,
    /// Content-Language override (rscl) - optional.
    pub content_language: Option<String>,
    /// Content-Type override (rsct) - optional.
    pub content_type: Option<String>,
    /// Signature (sig).
    pub signature: String,
}

impl BlobSasParameters {
    /// Parses blob SAS parameters from query string.
    pub fn from_query(params: &HashMap<String, String>) -> Option<Self> {
        // Check if this looks like a blob SAS (has sr but not ss)
        if !params.contains_key("sr") || params.contains_key("ss") {
            return None;
        }

        let signed_version = params.get("sv")?.clone();
        let signed_resource = params.get("sr")?.clone();
        let signed_permissions = params.get("sp").cloned().unwrap_or_default();
        let signed_expiry_str = params.get("se")?;
        let signed_expiry = parse_sas_datetime(signed_expiry_str)?;
        let signed_start = params.get("st").and_then(|s| parse_sas_datetime(s));
        let signed_ip = params.get("sip").cloned();
        let signed_protocol = params.get("spr").cloned();
        let signed_identifier = params.get("si").cloned();
        let cache_control = params.get("rscc").cloned();
        let content_disposition = params.get("rscd").cloned();
        let content_encoding = params.get("rsce").cloned();
        let content_language = params.get("rscl").cloned();
        let content_type = params.get("rsct").cloned();
        let signature = params.get("sig")?.clone();

        Some(Self {
            signed_version,
            signed_resource,
            signed_permissions,
            signed_expiry,
            signed_start,
            signed_ip,
            signed_protocol,
            signed_identifier,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            signature,
        })
    }

    /// Validates the blob SAS token.
    pub fn validate(
        &self,
        ctx: &RequestContext,
        config: &Config,
        required_permission: char,
    ) -> StorageResult<()> {
        // Check resource type matches request
        match self.signed_resource.as_str() {
            "c" => {
                // Container SAS - valid for container and blob operations
            }
            "b" | "bs" | "bv" => {
                // Blob SAS - only valid for blob operations
                if ctx.blob.is_none() {
                    return Err(StorageError::new(
                        ErrorCode::AuthorizationResourceTypeMismatch,
                    ));
                }
            }
            _ => {
                return Err(StorageError::new(
                    ErrorCode::AuthorizationResourceTypeMismatch,
                ));
            }
        }

        // Check permission
        if !self.signed_permissions.contains(required_permission) {
            return Err(StorageError::new(
                ErrorCode::AuthorizationPermissionMismatch,
            ));
        }

        // Check expiry
        let now = Utc::now();
        if now > self.signed_expiry {
            return Err(StorageError::with_message(
                ErrorCode::AuthenticationFailed,
                "SAS token has expired",
            ));
        }

        // Check start time
        if let Some(ref start) = self.signed_start {
            if now < *start {
                return Err(StorageError::with_message(
                    ErrorCode::AuthenticationFailed,
                    "SAS token is not yet valid",
                ));
            }
        }

        // Validate signature
        self.validate_signature(ctx, config)?;

        Ok(())
    }

    /// Validates the signature.
    fn validate_signature(&self, ctx: &RequestContext, config: &Config) -> StorageResult<()> {
        let account_key = config
            .get_account_key(&ctx.account)
            .ok_or_else(|| StorageError::new(ErrorCode::AuthorizationFailure))?;

        let string_to_sign = self.build_string_to_sign(ctx);
        let expected_signature = compute_signature(&string_to_sign, account_key)?;

        // URL-decode the provided signature for comparison
        let provided_signature = percent_encoding::percent_decode_str(&self.signature)
            .decode_utf8()
            .map_err(|_| StorageError::new(ErrorCode::AuthenticationFailed))?;

        if provided_signature != expected_signature {
            tracing::debug!(
                "Blob SAS signature mismatch:\n  Expected: {}\n  Provided: {}\n  StringToSign: {:?}",
                expected_signature,
                provided_signature,
                string_to_sign
            );
            return Err(StorageError::new(ErrorCode::AuthenticationFailed));
        }

        Ok(())
    }

    /// Builds the string-to-sign for blob SAS.
    fn build_string_to_sign(&self, ctx: &RequestContext) -> String {
        let mut parts = Vec::new();

        parts.push(self.signed_permissions.clone());
        parts.push(
            self.signed_start
                .map(|dt| format_sas_datetime(&dt))
                .unwrap_or_default(),
        );
        parts.push(format_sas_datetime(&self.signed_expiry));

        // Canonicalized resource
        let canonicalized_resource = self.build_canonicalized_resource(ctx);
        parts.push(canonicalized_resource);

        parts.push(self.signed_identifier.clone().unwrap_or_default());
        parts.push(self.signed_ip.clone().unwrap_or_default());
        parts.push(self.signed_protocol.clone().unwrap_or_default());
        parts.push(self.signed_version.clone());
        parts.push(self.signed_resource.clone());

        // Snapshot time (for version 2018-11-09 and later)
        parts.push(String::new());

        // Encryption scope (for version 2020-12-06 and later)
        parts.push(String::new());

        // Response headers
        parts.push(self.cache_control.clone().unwrap_or_default());
        parts.push(self.content_disposition.clone().unwrap_or_default());
        parts.push(self.content_encoding.clone().unwrap_or_default());
        parts.push(self.content_language.clone().unwrap_or_default());
        parts.push(self.content_type.clone().unwrap_or_default());

        parts.join("\n")
    }

    /// Builds the canonicalized resource for blob SAS.
    fn build_canonicalized_resource(&self, ctx: &RequestContext) -> String {
        let mut resource = format!("/blob/{}", ctx.account);

        if let Some(ref container) = ctx.container {
            resource.push('/');
            resource.push_str(container);
        }

        // Only include blob in canonicalized resource for blob SAS (sr=b, bs, bv),
        // not for container SAS (sr=c)
        if self.signed_resource != "c" {
            if let Some(ref blob) = ctx.blob {
                resource.push('/');
                resource.push_str(blob);
            }
        }

        resource
    }
}

/// Parses a SAS datetime string.
fn parse_sas_datetime(s: &str) -> Option<DateTime<Utc>> {
    // Try ISO 8601 format first
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            // Try without timezone
            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
                .ok()
                .map(|dt| dt.and_utc())
        })
        .or_else(|| {
            // Try date only
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .ok()
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
        })
}

/// Formats a datetime for SAS token.
fn format_sas_datetime(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Computes HMAC-SHA256 signature.
fn compute_signature(string_to_sign: &str, account_key: &str) -> StorageResult<String> {
    let key_bytes = BASE64.decode(account_key).map_err(|_| {
        StorageError::with_message(ErrorCode::InternalError, "Invalid account key encoding")
    })?;

    let mut mac = HmacSha256::new_from_slice(&key_bytes).map_err(|_| {
        StorageError::with_message(ErrorCode::InternalError, "Failed to create HMAC")
    })?;

    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize();

    Ok(BASE64.encode(result.into_bytes()))
}

/// Returns the required permission character for the blob operation.
pub fn get_blob_required_permission(ctx: &RequestContext) -> char {
    match ctx.method.as_str() {
        "GET" | "HEAD" => 'r', // read
        "PUT" => {
            if ctx.comp() == Some("block") || ctx.comp() == Some("appendblock") {
                'a' // add
            } else if ctx.comp() == Some("blocklist") {
                'w' // write
            } else if ctx.copy_source().is_some() {
                'w' // write (copy)
            } else {
                'c' // create
            }
        }
        "DELETE" => 'd', // delete
        "POST" => 'w',   // write
        _ => 'r',
    }
}

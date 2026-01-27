//! Account SAS token validation for Azure Blob Storage API.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;

use crate::config::Config;
use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};

type HmacSha256 = Hmac<Sha256>;

/// Account SAS token parameters.
#[derive(Debug, Clone)]
pub struct AccountSasParameters {
    /// Signed version (sv).
    pub signed_version: String,
    /// Signed services (ss) - b for blob.
    pub signed_services: String,
    /// Signed resource types (srt) - s=service, c=container, o=object.
    pub signed_resource_types: String,
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
    /// Signature (sig).
    pub signature: String,
}

impl AccountSasParameters {
    /// Parses account SAS parameters from query string.
    pub fn from_query(params: &HashMap<String, String>) -> Option<Self> {
        // Check if this looks like an account SAS (has ss and srt)
        if !params.contains_key("ss") || !params.contains_key("srt") {
            return None;
        }

        let signed_version = params.get("sv")?.clone();
        let signed_services = params.get("ss")?.clone();
        let signed_resource_types = params.get("srt")?.clone();
        let signed_permissions = params.get("sp")?.clone();
        let signed_expiry = parse_sas_datetime(params.get("se")?)?;
        let signed_start = params.get("st").and_then(|s| parse_sas_datetime(s));
        let signed_ip = params.get("sip").cloned();
        let signed_protocol = params.get("spr").cloned();
        let signature = params.get("sig")?.clone();

        Some(Self {
            signed_version,
            signed_services,
            signed_resource_types,
            signed_permissions,
            signed_expiry,
            signed_start,
            signed_ip,
            signed_protocol,
            signature,
        })
    }

    /// Validates the account SAS token.
    pub fn validate(
        &self,
        ctx: &RequestContext,
        config: &Config,
        resource_type: char,
        required_permission: char,
    ) -> StorageResult<()> {
        // Check if blob service is allowed
        if !self.signed_services.contains('b') {
            return Err(StorageError::new(ErrorCode::AuthorizationServiceMismatch));
        }

        // Check resource type
        if !self.signed_resource_types.contains(resource_type) {
            return Err(StorageError::new(
                ErrorCode::AuthorizationResourceTypeMismatch,
            ));
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

        let string_to_sign = self.build_string_to_sign(&ctx.account);
        let expected_signature = compute_signature(&string_to_sign, account_key)?;

        // URL-decode the provided signature for comparison
        let provided_signature = percent_encoding::percent_decode_str(&self.signature)
            .decode_utf8()
            .map_err(|_| StorageError::new(ErrorCode::AuthenticationFailed))?;

        if provided_signature != expected_signature {
            tracing::debug!(
                "Account SAS signature mismatch:\n  Expected: {}\n  Provided: {}\n  StringToSign: {:?}",
                expected_signature,
                provided_signature,
                string_to_sign
            );
            return Err(StorageError::new(ErrorCode::AuthenticationFailed));
        }

        Ok(())
    }

    /// Builds the string-to-sign for account SAS.
    fn build_string_to_sign(&self, account: &str) -> String {
        let mut parts = Vec::new();

        parts.push(account.to_string());
        parts.push(self.signed_permissions.clone());
        parts.push(self.signed_services.clone());
        parts.push(self.signed_resource_types.clone());
        parts.push(
            self.signed_start
                .map(|dt| format_sas_datetime(&dt))
                .unwrap_or_default(),
        );
        parts.push(format_sas_datetime(&self.signed_expiry));
        parts.push(self.signed_ip.clone().unwrap_or_default());
        parts.push(self.signed_protocol.clone().unwrap_or_default());
        parts.push(self.signed_version.clone());
        // For 2020-12-06 and later versions, there are additional fields
        // but we'll use the simpler format for compatibility
        parts.push(String::new()); // signed encryption scope (optional)

        parts.join("\n")
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

/// Returns the resource type character for the request.
pub fn get_resource_type(ctx: &RequestContext) -> char {
    if ctx.is_service_request() {
        's' // service
    } else if ctx.is_container_request() {
        'c' // container
    } else {
        'o' // object (blob)
    }
}

/// Returns the required permission character for the request method.
pub fn get_required_permission(ctx: &RequestContext) -> char {
    match ctx.method.as_str() {
        "GET" | "HEAD" => 'r', // read
        "PUT" => {
            if ctx.comp() == Some("block") || ctx.comp() == Some("appendblock") {
                'a' // add (for staging blocks)
            } else if ctx.blob.is_some() && ctx.query_params.is_empty() {
                'c' // create
            } else {
                'w' // write
            }
        }
        "DELETE" => 'd', // delete
        "POST" => 'w',   // write
        _ => 'r',
    }
}

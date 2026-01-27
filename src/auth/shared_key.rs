//! SharedKey authentication for Azure Blob Storage API.

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use percent_encoding;
use sha2::Sha256;
use std::collections::BTreeMap;

use crate::config::Config;
use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};

type HmacSha256 = Hmac<Sha256>;

/// Validates SharedKey authentication.
pub fn validate_shared_key(
    ctx: &RequestContext,
    config: &Config,
) -> StorageResult<()> {
    let auth_header = ctx
        .header("authorization")
        .ok_or_else(|| StorageError::new(ErrorCode::AuthenticationFailed))?;

    // Parse "SharedKey account:signature" or "SharedKeyLite account:signature"
    let (scheme, credentials) = auth_header
        .split_once(' ')
        .ok_or_else(|| StorageError::new(ErrorCode::AuthenticationFailed))?;

    if scheme != "SharedKey" && scheme != "SharedKeyLite" {
        return Err(StorageError::new(ErrorCode::AuthenticationFailed));
    }

    let (account, provided_signature) = credentials
        .split_once(':')
        .ok_or_else(|| StorageError::new(ErrorCode::AuthenticationFailed))?;

    // Verify account matches
    if account != ctx.account {
        return Err(StorageError::new(ErrorCode::AuthorizationFailure));
    }

    // Get account key
    let account_key = config
        .get_account_key(account)
        .ok_or_else(|| StorageError::new(ErrorCode::AuthorizationFailure))?;

    // Compute expected signature
    let string_to_sign = if scheme == "SharedKey" {
        build_string_to_sign(ctx)?
    } else {
        build_string_to_sign_lite(ctx)?
    };

    let expected_signature = compute_signature(&string_to_sign, account_key)?;

    // Compare signatures
    if provided_signature != expected_signature {
        tracing::warn!(
            "Signature mismatch:\n  Expected: {}\n  Provided: {}\n  StringToSign:\n{}\n  StringToSign (escaped): {:?}",
            expected_signature,
            provided_signature,
            string_to_sign,
            string_to_sign
        );
        return Err(StorageError::new(ErrorCode::AuthenticationFailed));
    }

    Ok(())
}

/// Builds the string-to-sign for SharedKey authentication.
fn build_string_to_sign(ctx: &RequestContext) -> StorageResult<String> {
    let mut parts = Vec::new();

    // VERB
    parts.push(ctx.method.as_str().to_uppercase());

    // Content headers (must be in this exact order)
    let content_headers = [
        "content-encoding",
        "content-language",
        "content-length",
        "content-md5",
        "content-type",
    ];

    for header in &content_headers {
        let value = if *header == "content-length" {
            // Content-Length should be empty string if 0 or not present
            match ctx.content_length() {
                Some(0) | None => String::new(),
                Some(len) => len.to_string(),
            }
        } else {
            ctx.header(header).unwrap_or("").to_string()
        };
        parts.push(value);
    }

    // Date header - if x-ms-date is present, leave Date empty (x-ms-date is in canonicalized headers)
    let date = if ctx.header("x-ms-date").is_some() {
        ""
    } else {
        ctx.header("date").unwrap_or("")
    };
    parts.push(date.to_string());

    // Conditional headers
    let conditional_headers = [
        "if-modified-since",
        "if-match",
        "if-none-match",
        "if-unmodified-since",
        "range",
    ];

    for header in &conditional_headers {
        parts.push(ctx.header(header).unwrap_or("").to_string());
    }

    // Build the string-to-sign following Azure's exact format:
    // [headers].join("\n") + "\n" + canonicalizedHeaders + canonicalizedResource
    // Note: canonicalizedHeaders already has trailing \n for each header
    let headers_str = parts.join("\n");
    let canonicalized_headers = build_canonicalized_headers_with_trailing_newline(ctx);
    let canonicalized_resource = build_canonicalized_resource(ctx);

    Ok(format!("{}\n{}{}", headers_str, canonicalized_headers, canonicalized_resource))
}

/// Builds the string-to-sign for SharedKeyLite authentication.
fn build_string_to_sign_lite(ctx: &RequestContext) -> StorageResult<String> {
    let mut parts = Vec::new();

    // VERB
    parts.push(ctx.method.as_str().to_uppercase());

    // Content-MD5
    parts.push(ctx.header("content-md5").unwrap_or("").to_string());

    // Content-Type
    parts.push(ctx.header("content-type").unwrap_or("").to_string());

    // Date (use x-ms-date if present)
    let date = ctx
        .header("x-ms-date")
        .or_else(|| ctx.header("date"))
        .unwrap_or("");
    parts.push(date.to_string());

    // Build the string-to-sign following Azure's format
    let headers_str = parts.join("\n");
    let canonicalized_headers = build_canonicalized_headers_with_trailing_newline(ctx);
    let canonicalized_resource = build_canonicalized_resource_lite(ctx);

    Ok(format!("{}\n{}{}", headers_str, canonicalized_headers, canonicalized_resource))
}

/// Builds canonicalized headers string with trailing newline after each header.
/// This matches Azure's format where each header line ends with \n.
fn build_canonicalized_headers_with_trailing_newline(ctx: &RequestContext) -> String {
    let ms_headers = ctx.ms_headers();
    if ms_headers.is_empty() {
        return String::new();
    }

    let mut result = String::new();
    for (name, value) in ms_headers.iter() {
        // Normalize header name to lowercase and trim whitespace from value
        let normalized_value = value.split_whitespace().collect::<Vec<_>>().join(" ");
        result.push_str(&name.to_lowercase());
        result.push(':');
        result.push_str(&normalized_value);
        result.push('\n');
    }
    result
}

/// Builds canonicalized resource string for SharedKey.
fn build_canonicalized_resource(ctx: &RequestContext) -> String {
    // The canonicalized resource is /{account}{path}
    // where path already includes the account (e.g., /devstoreaccount1/container)
    // So the result is /devstoreaccount1/devstoreaccount1/container
    let mut resource = format!("/{}{}", ctx.account, ctx.uri.path());

    // Add query parameters (sorted alphabetically by lowercase key)
    if !ctx.query_params.is_empty() {
        let mut sorted_params: Vec<_> = ctx.query_params.iter().collect();
        sorted_params.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));

        for (key, value) in sorted_params {
            resource.push('\n');
            resource.push_str(&key.to_lowercase());
            resource.push(':');
            // URL-decode the value (Azure SDK sends encoded values)
            let decoded = percent_encoding::percent_decode_str(value)
                .decode_utf8_lossy()
                .to_string();
            resource.push_str(&decoded);
        }
    }

    resource
}

/// Builds canonicalized resource string for SharedKeyLite.
fn build_canonicalized_resource_lite(ctx: &RequestContext) -> String {
    // The canonicalized resource is /{account}{path}
    let mut resource = format!("/{}{}", ctx.account, ctx.uri.path());

    // Only add comp parameter for Lite
    if let Some(comp) = ctx.query_param("comp") {
        resource.push_str("?comp=");
        // URL-decode the value
        let decoded = percent_encoding::percent_decode_str(comp)
            .decode_utf8_lossy()
            .to_string();
        resource.push_str(&decoded);
    }

    resource
}

/// Computes HMAC-SHA256 signature.
fn compute_signature(string_to_sign: &str, account_key: &str) -> StorageResult<String> {
    let key_bytes = BASE64.decode(account_key).map_err(|_| {
        StorageError::with_message(
            ErrorCode::InternalError,
            "Invalid account key encoding",
        )
    })?;

    let mut mac = HmacSha256::new_from_slice(&key_bytes).map_err(|_| {
        StorageError::with_message(ErrorCode::InternalError, "Failed to create HMAC")
    })?;

    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize();

    Ok(BASE64.encode(result.into_bytes()))
}

/// Computes the signature for a given string-to-sign (used for SAS generation).
pub fn sign_string(string_to_sign: &str, account_key: &str) -> StorageResult<String> {
    compute_signature(string_to_sign, account_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_signature() {
        let key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        let string_to_sign = "test string";
        let signature = compute_signature(string_to_sign, key).unwrap();
        assert!(!signature.is_empty());
    }
}

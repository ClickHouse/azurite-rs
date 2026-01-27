//! Authentication middleware for Azure Blob Storage API.

use std::sync::Arc;

use crate::config::Config;
use crate::context::RequestContext;
use crate::error::{ErrorCode, StorageError, StorageResult};

use super::{
    account_sas::{get_required_permission, get_resource_type, AccountSasParameters},
    blob_sas::{get_blob_required_permission, BlobSasParameters},
    shared_key::validate_shared_key,
};

/// Authentication result containing the authenticated account.
#[derive(Debug, Clone)]
pub struct AuthResult {
    pub account: String,
    pub is_anonymous: bool,
}

/// Authenticates a request using available authentication methods.
pub fn authenticate(ctx: &RequestContext, config: &Config) -> StorageResult<AuthResult> {
    // Check for Authorization header (SharedKey)
    if ctx.header("authorization").is_some() {
        validate_shared_key(ctx, config)?;
        return Ok(AuthResult {
            account: ctx.account.clone(),
            is_anonymous: false,
        });
    }

    // Check for Account SAS token
    if let Some(account_sas) = AccountSasParameters::from_query(&ctx.query_params) {
        let resource_type = get_resource_type(ctx);
        let required_permission = get_required_permission(ctx);
        account_sas.validate(ctx, config, resource_type, required_permission)?;
        return Ok(AuthResult {
            account: ctx.account.clone(),
            is_anonymous: false,
        });
    }

    // Check for Blob SAS token
    if let Some(blob_sas) = BlobSasParameters::from_query(&ctx.query_params) {
        let required_permission = get_blob_required_permission(ctx);
        blob_sas.validate(ctx, config, required_permission)?;
        return Ok(AuthResult {
            account: ctx.account.clone(),
            is_anonymous: false,
        });
    }

    // Check if account exists (for anonymous access)
    if config.get_account_key(&ctx.account).is_some() {
        // Allow anonymous access (public containers will be checked at handler level)
        return Ok(AuthResult {
            account: ctx.account.clone(),
            is_anonymous: true,
        });
    }

    // No valid authentication
    Err(StorageError::new(ErrorCode::AuthenticationFailed))
}

/// Checks if a request requires authentication.
pub fn requires_auth(ctx: &RequestContext) -> bool {
    // Most operations require authentication
    // Public containers allow anonymous read access, but that's checked separately
    true
}

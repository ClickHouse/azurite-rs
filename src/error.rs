//! Azure Blob Storage error types and error response formatting.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use hyper::ext::ReasonPhrase;
use thiserror::Error;

/// Azure Storage error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // General errors
    AccountAlreadyExists,
    AccountBeingCreated,
    AccountIsDisabled,
    AuthenticationFailed,
    AuthorizationFailure,
    AuthorizationPermissionMismatch,
    AuthorizationProtocolMismatch,
    AuthorizationResourceTypeMismatch,
    AuthorizationServiceMismatch,
    AuthorizationSourceIPMismatch,
    ConditionHeadersNotSupported,
    ConditionNotMet,
    EmptyMetadataKey,
    InsufficientAccountPermissions,
    InternalError,
    InvalidAuthenticationInfo,
    InvalidHeaderValue,
    InvalidHttpVerb,
    InvalidInput,
    InvalidMd5,
    InvalidMetadata,
    InvalidQueryParameterValue,
    InvalidRange,
    InvalidResourceName,
    InvalidUri,
    InvalidXmlDocument,
    InvalidXmlNodeValue,
    Md5Mismatch,
    MetadataTooLarge,
    MissingContentLengthHeader,
    MissingRequiredQueryParameter,
    MissingRequiredHeader,
    MissingRequiredXmlNode,
    MultipleConditionHeadersNotSupported,
    OperationTimedOut,
    OutOfRangeInput,
    OutOfRangeQueryParameterValue,
    RequestBodyTooLarge,
    ResourceTypeMismatch,
    RequestUrlFailedToParse,
    ResourceAlreadyExists,
    ResourceNotFound,
    ServerBusy,
    UnsupportedHeader,
    UnsupportedXmlNode,
    UnsupportedQueryParameter,
    UnsupportedHttpVerb,

    // Blob-specific errors
    AppendPositionConditionNotMet,
    BlobAlreadyExists,
    BlobArchived,
    BlobBeingRehydrated,
    BlobImmutableDueToPolicy,
    BlobNotArchived,
    BlobNotFound,
    BlobOverwritten,
    BlobTierInadequateForContentLength,
    BlobUsesCustomerSpecifiedEncryption,
    BlockCountExceedsLimit,
    BlockListTooLong,
    CannotChangeToLowerTier,
    CannotVerifyCopySource,
    ContainerAlreadyExists,
    ContainerBeingDeleted,
    ContainerDisabled,
    ContainerNotFound,
    ContentLengthLargerThanTierLimit,
    CopyAcrossAccountsNotSupported,
    CopyIdMismatch,
    FeatureVersionMismatch,
    IncrementalCopyBlobMismatch,
    IncrementalCopyOfEarlierVersionSnapshotNotAllowed,
    IncrementalCopySourceMustBeSnapshot,
    InfiniteLeaseDurationRequired,
    InvalidBlobOrBlock,
    InvalidBlobTier,
    InvalidBlobType,
    InvalidBlockId,
    InvalidBlockList,
    InvalidOperation,
    InvalidPageRange,
    InvalidSourceBlobType,
    InvalidSourceBlobUrl,
    InvalidVersionForPageBlobOperation,
    LeaseAlreadyBroken,
    LeaseAlreadyPresent,
    LeaseIdMismatch,
    LeaseIdMismatchWithBlobOperation,
    LeaseIdMismatchWithContainerOperation,
    LeaseIdMismatchWithLeaseOperation,
    LeaseIdMissing,
    LeaseIsBreakingAndCannotBeAcquired,
    LeaseIsBreakingAndCannotBeChanged,
    LeaseIsBrokenAndCannotBeRenewed,
    LeaseLost,
    LeaseNotPresentWithBlobOperation,
    LeaseNotPresentWithContainerOperation,
    LeaseNotPresentWithLeaseOperation,
    MaxBlobSizeConditionNotMet,
    NoPendingCopyOperation,
    OperationNotAllowedOnIncrementalCopyBlob,
    PendingCopyOperation,
    PreviousSnapshotCannotBeNewer,
    PreviousSnapshotNotFound,
    PreviousSnapshotOperationNotSupported,
    SequenceNumberConditionNotMet,
    SequenceNumberIncrementTooLarge,
    SnapshotCountExceeded,
    SnapshotOperationRateExceeded,
    SnapshotsPresent,
    SourceConditionNotMet,
    SystemInUse,
    TargetConditionNotMet,
    UnauthorizedBlobOverwrite,
    UnsupportedBlobType,
}

impl ErrorCode {
    /// Returns the string representation of the error code.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorCode::AccountAlreadyExists => "AccountAlreadyExists",
            ErrorCode::AccountBeingCreated => "AccountBeingCreated",
            ErrorCode::AccountIsDisabled => "AccountIsDisabled",
            ErrorCode::AuthenticationFailed => "AuthenticationFailed",
            ErrorCode::AuthorizationFailure => "AuthorizationFailure",
            ErrorCode::AuthorizationPermissionMismatch => "AuthorizationPermissionMismatch",
            ErrorCode::AuthorizationProtocolMismatch => "AuthorizationProtocolMismatch",
            ErrorCode::AuthorizationResourceTypeMismatch => "AuthorizationResourceTypeMismatch",
            ErrorCode::AuthorizationServiceMismatch => "AuthorizationServiceMismatch",
            ErrorCode::AuthorizationSourceIPMismatch => "AuthorizationSourceIPMismatch",
            ErrorCode::ConditionHeadersNotSupported => "ConditionHeadersNotSupported",
            ErrorCode::ConditionNotMet => "ConditionNotMet",
            ErrorCode::EmptyMetadataKey => "EmptyMetadataKey",
            ErrorCode::InsufficientAccountPermissions => "InsufficientAccountPermissions",
            ErrorCode::InternalError => "InternalError",
            ErrorCode::InvalidAuthenticationInfo => "InvalidAuthenticationInfo",
            ErrorCode::InvalidHeaderValue => "InvalidHeaderValue",
            ErrorCode::InvalidHttpVerb => "InvalidHttpVerb",
            ErrorCode::InvalidInput => "InvalidInput",
            ErrorCode::InvalidMd5 => "InvalidMd5",
            ErrorCode::InvalidMetadata => "InvalidMetadata",
            ErrorCode::InvalidQueryParameterValue => "InvalidQueryParameterValue",
            ErrorCode::InvalidRange => "InvalidRange",
            ErrorCode::InvalidResourceName => "InvalidResourceName",
            ErrorCode::InvalidUri => "InvalidUri",
            ErrorCode::InvalidXmlDocument => "InvalidXmlDocument",
            ErrorCode::InvalidXmlNodeValue => "InvalidXmlNodeValue",
            ErrorCode::Md5Mismatch => "Md5Mismatch",
            ErrorCode::MetadataTooLarge => "MetadataTooLarge",
            ErrorCode::MissingContentLengthHeader => "MissingContentLengthHeader",
            ErrorCode::MissingRequiredQueryParameter => "MissingRequiredQueryParameter",
            ErrorCode::MissingRequiredHeader => "MissingRequiredHeader",
            ErrorCode::MissingRequiredXmlNode => "MissingRequiredXmlNode",
            ErrorCode::MultipleConditionHeadersNotSupported => {
                "MultipleConditionHeadersNotSupported"
            }
            ErrorCode::OperationTimedOut => "OperationTimedOut",
            ErrorCode::OutOfRangeInput => "OutOfRangeInput",
            ErrorCode::OutOfRangeQueryParameterValue => "OutOfRangeQueryParameterValue",
            ErrorCode::RequestBodyTooLarge => "RequestBodyTooLarge",
            ErrorCode::ResourceTypeMismatch => "ResourceTypeMismatch",
            ErrorCode::RequestUrlFailedToParse => "RequestUrlFailedToParse",
            ErrorCode::ResourceAlreadyExists => "ResourceAlreadyExists",
            ErrorCode::ResourceNotFound => "ResourceNotFound",
            ErrorCode::ServerBusy => "ServerBusy",
            ErrorCode::UnsupportedHeader => "UnsupportedHeader",
            ErrorCode::UnsupportedXmlNode => "UnsupportedXmlNode",
            ErrorCode::UnsupportedQueryParameter => "UnsupportedQueryParameter",
            ErrorCode::UnsupportedHttpVerb => "UnsupportedHttpVerb",
            ErrorCode::AppendPositionConditionNotMet => "AppendPositionConditionNotMet",
            ErrorCode::BlobAlreadyExists => "BlobAlreadyExists",
            ErrorCode::BlobArchived => "BlobArchived",
            ErrorCode::BlobBeingRehydrated => "BlobBeingRehydrated",
            ErrorCode::BlobImmutableDueToPolicy => "BlobImmutableDueToPolicy",
            ErrorCode::BlobNotArchived => "BlobNotArchived",
            ErrorCode::BlobNotFound => "BlobNotFound",
            ErrorCode::BlobOverwritten => "BlobOverwritten",
            ErrorCode::BlobTierInadequateForContentLength => "BlobTierInadequateForContentLength",
            ErrorCode::BlobUsesCustomerSpecifiedEncryption => "BlobUsesCustomerSpecifiedEncryption",
            ErrorCode::BlockCountExceedsLimit => "BlockCountExceedsLimit",
            ErrorCode::BlockListTooLong => "BlockListTooLong",
            ErrorCode::CannotChangeToLowerTier => "CannotChangeToLowerTier",
            ErrorCode::CannotVerifyCopySource => "CannotVerifyCopySource",
            ErrorCode::ContainerAlreadyExists => "ContainerAlreadyExists",
            ErrorCode::ContainerBeingDeleted => "ContainerBeingDeleted",
            ErrorCode::ContainerDisabled => "ContainerDisabled",
            ErrorCode::ContainerNotFound => "ContainerNotFound",
            ErrorCode::ContentLengthLargerThanTierLimit => "ContentLengthLargerThanTierLimit",
            ErrorCode::CopyAcrossAccountsNotSupported => "CopyAcrossAccountsNotSupported",
            ErrorCode::CopyIdMismatch => "CopyIdMismatch",
            ErrorCode::FeatureVersionMismatch => "FeatureVersionMismatch",
            ErrorCode::IncrementalCopyBlobMismatch => "IncrementalCopyBlobMismatch",
            ErrorCode::IncrementalCopyOfEarlierVersionSnapshotNotAllowed => {
                "IncrementalCopyOfEarlierVersionSnapshotNotAllowed"
            }
            ErrorCode::IncrementalCopySourceMustBeSnapshot => "IncrementalCopySourceMustBeSnapshot",
            ErrorCode::InfiniteLeaseDurationRequired => "InfiniteLeaseDurationRequired",
            ErrorCode::InvalidBlobOrBlock => "InvalidBlobOrBlock",
            ErrorCode::InvalidBlobTier => "InvalidBlobTier",
            ErrorCode::InvalidBlobType => "InvalidBlobType",
            ErrorCode::InvalidBlockId => "InvalidBlockId",
            ErrorCode::InvalidBlockList => "InvalidBlockList",
            ErrorCode::InvalidOperation => "InvalidOperation",
            ErrorCode::InvalidPageRange => "InvalidPageRange",
            ErrorCode::InvalidSourceBlobType => "InvalidSourceBlobType",
            ErrorCode::InvalidSourceBlobUrl => "InvalidSourceBlobUrl",
            ErrorCode::InvalidVersionForPageBlobOperation => "InvalidVersionForPageBlobOperation",
            ErrorCode::LeaseAlreadyBroken => "LeaseAlreadyBroken",
            ErrorCode::LeaseAlreadyPresent => "LeaseAlreadyPresent",
            ErrorCode::LeaseIdMismatch => "LeaseIdMismatch",
            ErrorCode::LeaseIdMismatchWithBlobOperation => "LeaseIdMismatchWithBlobOperation",
            ErrorCode::LeaseIdMismatchWithContainerOperation => {
                "LeaseIdMismatchWithContainerOperation"
            }
            ErrorCode::LeaseIdMismatchWithLeaseOperation => "LeaseIdMismatchWithLeaseOperation",
            ErrorCode::LeaseIdMissing => "LeaseIdMissing",
            ErrorCode::LeaseIsBreakingAndCannotBeAcquired => "LeaseIsBreakingAndCannotBeAcquired",
            ErrorCode::LeaseIsBreakingAndCannotBeChanged => "LeaseIsBreakingAndCannotBeChanged",
            ErrorCode::LeaseIsBrokenAndCannotBeRenewed => "LeaseIsBrokenAndCannotBeRenewed",
            ErrorCode::LeaseLost => "LeaseLost",
            ErrorCode::LeaseNotPresentWithBlobOperation => "LeaseNotPresentWithBlobOperation",
            ErrorCode::LeaseNotPresentWithContainerOperation => {
                "LeaseNotPresentWithContainerOperation"
            }
            ErrorCode::LeaseNotPresentWithLeaseOperation => "LeaseNotPresentWithLeaseOperation",
            ErrorCode::MaxBlobSizeConditionNotMet => "MaxBlobSizeConditionNotMet",
            ErrorCode::NoPendingCopyOperation => "NoPendingCopyOperation",
            ErrorCode::OperationNotAllowedOnIncrementalCopyBlob => {
                "OperationNotAllowedOnIncrementalCopyBlob"
            }
            ErrorCode::PendingCopyOperation => "PendingCopyOperation",
            ErrorCode::PreviousSnapshotCannotBeNewer => "PreviousSnapshotCannotBeNewer",
            ErrorCode::PreviousSnapshotNotFound => "PreviousSnapshotNotFound",
            ErrorCode::PreviousSnapshotOperationNotSupported => {
                "PreviousSnapshotOperationNotSupported"
            }
            ErrorCode::SequenceNumberConditionNotMet => "SequenceNumberConditionNotMet",
            ErrorCode::SequenceNumberIncrementTooLarge => "SequenceNumberIncrementTooLarge",
            ErrorCode::SnapshotCountExceeded => "SnapshotCountExceeded",
            ErrorCode::SnapshotOperationRateExceeded => "SnapshotOperationRateExceeded",
            ErrorCode::SnapshotsPresent => "SnapshotsPresent",
            ErrorCode::SourceConditionNotMet => "SourceConditionNotMet",
            ErrorCode::SystemInUse => "SystemInUse",
            ErrorCode::TargetConditionNotMet => "TargetConditionNotMet",
            ErrorCode::UnauthorizedBlobOverwrite => "UnauthorizedBlobOverwrite",
            ErrorCode::UnsupportedBlobType => "UnsupportedBlobType",
        }
    }

    /// Returns the HTTP status code for this error.
    pub fn status_code(&self) -> StatusCode {
        match self {
            // 400 Bad Request
            ErrorCode::InvalidHeaderValue
            | ErrorCode::InvalidHttpVerb
            | ErrorCode::InvalidInput
            | ErrorCode::InvalidMd5
            | ErrorCode::InvalidMetadata
            | ErrorCode::InvalidQueryParameterValue
            | ErrorCode::InvalidRange
            | ErrorCode::InvalidResourceName
            | ErrorCode::InvalidUri
            | ErrorCode::InvalidXmlDocument
            | ErrorCode::InvalidXmlNodeValue
            | ErrorCode::Md5Mismatch
            | ErrorCode::MetadataTooLarge
            | ErrorCode::MissingContentLengthHeader
            | ErrorCode::MissingRequiredQueryParameter
            | ErrorCode::MissingRequiredHeader
            | ErrorCode::MissingRequiredXmlNode
            | ErrorCode::MultipleConditionHeadersNotSupported
            | ErrorCode::OutOfRangeInput
            | ErrorCode::OutOfRangeQueryParameterValue
            | ErrorCode::RequestBodyTooLarge
            | ErrorCode::UnsupportedHeader
            | ErrorCode::UnsupportedXmlNode
            | ErrorCode::UnsupportedQueryParameter
            | ErrorCode::UnsupportedHttpVerb
            | ErrorCode::InvalidBlobOrBlock
            | ErrorCode::InvalidBlobTier
            | ErrorCode::InvalidBlobType
            | ErrorCode::InvalidBlockId
            | ErrorCode::InvalidBlockList
            | ErrorCode::InvalidOperation
            | ErrorCode::InvalidPageRange
            | ErrorCode::InvalidSourceBlobType
            | ErrorCode::InvalidSourceBlobUrl
            | ErrorCode::InvalidVersionForPageBlobOperation
            | ErrorCode::BlockCountExceedsLimit
            | ErrorCode::BlockListTooLong
            | ErrorCode::EmptyMetadataKey => StatusCode::BAD_REQUEST,

            // 401 Unauthorized
            ErrorCode::AuthenticationFailed | ErrorCode::InvalidAuthenticationInfo => {
                StatusCode::UNAUTHORIZED
            }

            // 403 Forbidden
            ErrorCode::AccountIsDisabled
            | ErrorCode::AuthorizationFailure
            | ErrorCode::AuthorizationPermissionMismatch
            | ErrorCode::AuthorizationProtocolMismatch
            | ErrorCode::AuthorizationResourceTypeMismatch
            | ErrorCode::AuthorizationServiceMismatch
            | ErrorCode::AuthorizationSourceIPMismatch
            | ErrorCode::InsufficientAccountPermissions => StatusCode::FORBIDDEN,

            // 404 Not Found
            ErrorCode::BlobNotFound
            | ErrorCode::ContainerNotFound
            | ErrorCode::ResourceNotFound
            | ErrorCode::PreviousSnapshotNotFound => StatusCode::NOT_FOUND,

            // 405 Method Not Allowed
            ErrorCode::UnsupportedBlobType => StatusCode::METHOD_NOT_ALLOWED,

            // 409 Conflict
            ErrorCode::AccountAlreadyExists
            | ErrorCode::AccountBeingCreated
            | ErrorCode::BlobAlreadyExists
            | ErrorCode::BlobArchived
            | ErrorCode::BlobBeingRehydrated
            | ErrorCode::BlobImmutableDueToPolicy
            | ErrorCode::BlobNotArchived
            | ErrorCode::BlobOverwritten
            | ErrorCode::ContainerAlreadyExists
            | ErrorCode::ContainerBeingDeleted
            | ErrorCode::ContainerDisabled
            | ErrorCode::LeaseAlreadyBroken
            | ErrorCode::LeaseAlreadyPresent
            | ErrorCode::LeaseIdMismatch
            | ErrorCode::LeaseIdMismatchWithBlobOperation
            | ErrorCode::LeaseIdMismatchWithContainerOperation
            | ErrorCode::LeaseIdMismatchWithLeaseOperation
            | ErrorCode::LeaseIsBreakingAndCannotBeAcquired
            | ErrorCode::LeaseIsBreakingAndCannotBeChanged
            | ErrorCode::LeaseIsBrokenAndCannotBeRenewed
            | ErrorCode::LeaseLost
            | ErrorCode::LeaseNotPresentWithBlobOperation
            | ErrorCode::LeaseNotPresentWithContainerOperation
            | ErrorCode::LeaseNotPresentWithLeaseOperation
            | ErrorCode::NoPendingCopyOperation
            | ErrorCode::PendingCopyOperation
            | ErrorCode::ResourceAlreadyExists
            | ErrorCode::SnapshotsPresent
            | ErrorCode::SystemInUse => StatusCode::CONFLICT,

            // 412 Precondition Failed
            ErrorCode::AppendPositionConditionNotMet
            | ErrorCode::ConditionNotMet
            | ErrorCode::LeaseIdMissing
            | ErrorCode::MaxBlobSizeConditionNotMet
            | ErrorCode::SequenceNumberConditionNotMet
            | ErrorCode::SourceConditionNotMet
            | ErrorCode::TargetConditionNotMet => StatusCode::PRECONDITION_FAILED,

            // 416 Range Not Satisfiable
            ErrorCode::InvalidRange => StatusCode::RANGE_NOT_SATISFIABLE,

            // 500 Internal Server Error
            ErrorCode::InternalError | ErrorCode::OperationTimedOut => {
                StatusCode::INTERNAL_SERVER_ERROR
            }

            // 503 Service Unavailable
            ErrorCode::ServerBusy => StatusCode::SERVICE_UNAVAILABLE,

            // Default to 400 for anything not explicitly handled
            _ => StatusCode::BAD_REQUEST,
        }
    }

    /// Returns the default message for this error code.
    pub fn default_message(&self) -> &'static str {
        match self {
            ErrorCode::AuthenticationFailed => {
                "Server failed to authenticate the request. Make sure the value of the \
                 Authorization header is formed correctly including the signature."
            }
            ErrorCode::AuthorizationFailure => {
                "This request is not authorized to perform this operation."
            }
            ErrorCode::BlobNotFound => "The specified blob does not exist.",
            ErrorCode::ContainerAlreadyExists => "The specified container already exists.",
            ErrorCode::ContainerNotFound => "The specified container does not exist.",
            ErrorCode::InvalidBlockId => "The specified block ID is invalid.",
            ErrorCode::InvalidBlockList => "The specified block list is invalid.",
            ErrorCode::InvalidHeaderValue => "The value for one of the HTTP headers is not valid.",
            ErrorCode::InvalidRange => "The range specified is invalid for the current size of the resource.",
            ErrorCode::InvalidResourceName => "The specified resource name contains invalid characters.",
            ErrorCode::InvalidXmlDocument => "The XML request body is invalid.",
            ErrorCode::LeaseIdMissing => "There is currently a lease on the resource and no lease ID was specified in the request.",
            ErrorCode::MissingRequiredHeader => "A required header was not specified.",
            ErrorCode::MissingRequiredQueryParameter => "A required query parameter was not specified.",
            ErrorCode::ResourceNotFound => "The specified resource does not exist.",
            ErrorCode::InternalError => "The server encountered an internal error. Please retry the request.",
            _ => "An error occurred while processing the request.",
        }
    }
}

/// Storage error with code and message.
#[derive(Debug, Error)]
#[error("{code:?}: {message}")]
pub struct StorageError {
    pub code: ErrorCode,
    pub message: String,
    pub request_id: Option<String>,
}

impl StorageError {
    /// Creates a new storage error with the given code and default message.
    pub fn new(code: ErrorCode) -> Self {
        Self {
            message: code.default_message().to_string(),
            code,
            request_id: None,
        }
    }

    /// Creates a new storage error with a custom message.
    pub fn with_message(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            request_id: None,
        }
    }

    /// Sets the request ID for this error.
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Converts the error to an XML error response body.
    pub fn to_xml(&self) -> String {
        format!(
            r#"<?xml version="1.0" encoding="utf-8"?><Error><Code>{}</Code><Message>{}</Message></Error>"#,
            self.code.as_str(),
            xml_escape(&self.message)
        )
    }
}

impl IntoResponse for StorageError {
    fn into_response(self) -> Response {
        let status = self.code.status_code();
        let request_id = self.request_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ");

        // Match original Azurite's XML format with pretty-printing and included RequestId/Time
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Error>
  <Code>{}</Code>
  <Message>{}
RequestId:{}
Time:{}</Message>
</Error>"#,
            self.code.as_str(),
            xml_escape(&self.message),
            request_id,
            timestamp
        );

        // Build the response
        let mut response = Response::builder()
            .status(status)
            .header("Content-Type", "application/xml")
            .header("x-ms-request-id", &request_id)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-error-code", self.code.as_str())
            .body(xml.into())
            .unwrap();

        // Set custom reason phrase to match original Azurite behavior (for HTTP/1.1)
        // This puts the error message in the HTTP status line so clients can see it
        if let Ok(reason) = ReasonPhrase::try_from(self.message.as_bytes()) {
            response.extensions_mut().insert(reason);
        }

        response
    }
}

/// Escapes special XML characters in a string.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Result type alias for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

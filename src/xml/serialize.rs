//! XML response serialization for Azure Blob Storage API.

use crate::context::format_http_date;
use crate::models::{
    AccessTier, BlobModel, BlobType, BlockModel, BlockState, ContainerModel,
    CorsRule, DeleteRetentionPolicy, GeoReplicationStatus, LeaseState, LeaseStatus,
    LoggingConfig, MetricsConfig, PageRange, PageRangeDiff, PublicAccessLevel,
    RetentionPolicy, ServiceProperties, ServiceStats, SignedIdentifier, StaticWebsite,
    UserDelegationKey,
};

/// Escapes special XML characters.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Serializes a list of containers to XML.
pub fn serialize_container_list(
    containers: &[ContainerModel],
    prefix: Option<&str>,
    marker: Option<&str>,
    maxresults: u32,
    next_marker: Option<&str>,
    account: &str,
) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<EnumerationResults");
    xml.push_str(&format!(
        r#" ServiceEndpoint="http://127.0.0.1:10000/{}/""#,
        xml_escape(account)
    ));
    xml.push('>');

    if let Some(p) = prefix {
        xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(p)));
    }
    if let Some(m) = marker {
        xml.push_str(&format!("<Marker>{}</Marker>", xml_escape(m)));
    }
    xml.push_str(&format!("<MaxResults>{}</MaxResults>", maxresults));

    xml.push_str("<Containers>");
    for container in containers {
        xml.push_str(&serialize_container(container));
    }
    xml.push_str("</Containers>");

    if let Some(nm) = next_marker {
        xml.push_str(&format!("<NextMarker>{}</NextMarker>", xml_escape(nm)));
    }

    xml.push_str("</EnumerationResults>");
    xml
}

/// Serializes a single container for list results.
fn serialize_container(container: &ContainerModel) -> String {
    let mut xml = String::from("<Container>");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(&container.name)));
    xml.push_str("<Properties>");
    xml.push_str(&format!(
        "<Last-Modified>{}</Last-Modified>",
        format_http_date(&container.properties.last_modified)
    ));
    xml.push_str(&format!(
        "<Etag>{}</Etag>",
        xml_escape(&container.properties.etag)
    ));
    xml.push_str(&format!(
        "<LeaseStatus>{}</LeaseStatus>",
        container.properties.lease_status.as_str()
    ));
    xml.push_str(&format!(
        "<LeaseState>{}</LeaseState>",
        container.properties.lease_state.as_str()
    ));
    if container.properties.public_access != PublicAccessLevel::None {
        xml.push_str(&format!(
            "<PublicAccess>{}</PublicAccess>",
            container.properties.public_access.as_str()
        ));
    }
    xml.push_str(&format!(
        "<HasImmutabilityPolicy>{}</HasImmutabilityPolicy>",
        container.properties.has_immutability_policy
    ));
    xml.push_str(&format!(
        "<HasLegalHold>{}</HasLegalHold>",
        container.properties.has_legal_hold
    ));
    xml.push_str("</Properties>");

    if !container.metadata.is_empty() {
        xml.push_str("<Metadata>");
        for (key, value) in &container.metadata {
            xml.push_str(&format!(
                "<{}>{}</{}>",
                xml_escape(key),
                xml_escape(value),
                xml_escape(key)
            ));
        }
        xml.push_str("</Metadata>");
    }

    xml.push_str("</Container>");
    xml
}

/// Serializes a list of blobs to XML.
pub fn serialize_blob_list(
    blobs: &[BlobModel],
    blob_prefixes: &[String],
    prefix: Option<&str>,
    delimiter: Option<&str>,
    marker: Option<&str>,
    maxresults: u32,
    next_marker: Option<&str>,
    account: &str,
    container: &str,
) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<EnumerationResults");
    xml.push_str(&format!(
        r#" ServiceEndpoint="http://127.0.0.1:10000/{}/""#,
        xml_escape(account)
    ));
    xml.push_str(&format!(
        r#" ContainerName="{}""#,
        xml_escape(container)
    ));
    xml.push('>');

    if let Some(p) = prefix {
        xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(p)));
    }
    if let Some(m) = marker {
        xml.push_str(&format!("<Marker>{}</Marker>", xml_escape(m)));
    }
    xml.push_str(&format!("<MaxResults>{}</MaxResults>", maxresults));
    if let Some(d) = delimiter {
        xml.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(d)));
    }

    xml.push_str("<Blobs>");
    for blob in blobs {
        xml.push_str(&serialize_blob(blob));
    }
    for prefix in blob_prefixes {
        xml.push_str(&format!(
            "<BlobPrefix><Name>{}</Name></BlobPrefix>",
            xml_escape(prefix)
        ));
    }
    xml.push_str("</Blobs>");

    if let Some(nm) = next_marker {
        xml.push_str(&format!("<NextMarker>{}</NextMarker>", xml_escape(nm)));
    }

    xml.push_str("</EnumerationResults>");
    xml
}

/// Serializes a single blob for list results.
fn serialize_blob(blob: &BlobModel) -> String {
    let mut xml = String::from("<Blob>");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(&blob.name)));

    if !blob.snapshot.is_empty() {
        xml.push_str(&format!(
            "<Snapshot>{}</Snapshot>",
            xml_escape(&blob.snapshot)
        ));
    }

    xml.push_str("<Properties>");
    xml.push_str(&format!(
        "<Creation-Time>{}</Creation-Time>",
        format_http_date(&blob.properties.created_on)
    ));
    xml.push_str(&format!(
        "<Last-Modified>{}</Last-Modified>",
        format_http_date(&blob.properties.last_modified)
    ));
    xml.push_str(&format!(
        "<Etag>{}</Etag>",
        xml_escape(&blob.properties.etag)
    ));
    xml.push_str(&format!(
        "<Content-Length>{}</Content-Length>",
        blob.properties.content_length
    ));
    if let Some(ref ct) = blob.properties.content_type {
        xml.push_str(&format!("<Content-Type>{}</Content-Type>", xml_escape(ct)));
    }
    if let Some(ref ce) = blob.properties.content_encoding {
        xml.push_str(&format!(
            "<Content-Encoding>{}</Content-Encoding>",
            xml_escape(ce)
        ));
    }
    if let Some(ref cl) = blob.properties.content_language {
        xml.push_str(&format!(
            "<Content-Language>{}</Content-Language>",
            xml_escape(cl)
        ));
    }
    if let Some(ref md5) = blob.properties.content_md5 {
        xml.push_str(&format!("<Content-MD5>{}</Content-MD5>", xml_escape(md5)));
    }
    if let Some(ref cd) = blob.properties.content_disposition {
        xml.push_str(&format!(
            "<Content-Disposition>{}</Content-Disposition>",
            xml_escape(cd)
        ));
    }
    if let Some(ref cc) = blob.properties.cache_control {
        xml.push_str(&format!(
            "<Cache-Control>{}</Cache-Control>",
            xml_escape(cc)
        ));
    }
    xml.push_str(&format!(
        "<BlobType>{}</BlobType>",
        blob.properties.blob_type.as_str()
    ));
    xml.push_str(&format!(
        "<AccessTier>{}</AccessTier>",
        blob.properties.access_tier.as_str()
    ));
    xml.push_str("<AccessTierInferred>true</AccessTierInferred>");
    xml.push_str(&format!(
        "<LeaseStatus>{}</LeaseStatus>",
        blob.properties.lease_status.as_str()
    ));
    xml.push_str(&format!(
        "<LeaseState>{}</LeaseState>",
        blob.properties.lease_state.as_str()
    ));
    xml.push_str(&format!(
        "<ServerEncrypted>{}</ServerEncrypted>",
        blob.properties.server_encrypted
    ));

    if blob.properties.blob_type == BlobType::PageBlob {
        if let Some(seq) = blob.properties.sequence_number {
            xml.push_str(&format!(
                "<x-ms-blob-sequence-number>{}</x-ms-blob-sequence-number>",
                seq
            ));
        }
    }

    if blob.properties.blob_type == BlobType::AppendBlob {
        if let Some(sealed) = blob.properties.is_sealed {
            xml.push_str(&format!("<Sealed>{}</Sealed>", sealed));
        }
    }

    xml.push_str("</Properties>");

    if !blob.metadata.is_empty() {
        xml.push_str("<Metadata>");
        for (key, value) in &blob.metadata {
            xml.push_str(&format!(
                "<{}>{}</{}>",
                xml_escape(key),
                xml_escape(value),
                xml_escape(key)
            ));
        }
        xml.push_str("</Metadata>");
    }

    if !blob.tags.is_empty() {
        xml.push_str("<Tags><TagSet>");
        for (key, value) in &blob.tags {
            xml.push_str(&format!(
                "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                xml_escape(key),
                xml_escape(value)
            ));
        }
        xml.push_str("</TagSet></Tags>");
    }

    xml.push_str("</Blob>");
    xml
}

/// Serializes a block list to XML.
pub fn serialize_block_list(
    committed: &[BlockModel],
    uncommitted: &[BlockModel],
) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<BlockList>");

    xml.push_str("<CommittedBlocks>");
    for block in committed {
        xml.push_str("<Block>");
        xml.push_str(&format!("<Name>{}</Name>", xml_escape(&block.block_id)));
        xml.push_str(&format!("<Size>{}</Size>", block.size));
        xml.push_str("</Block>");
    }
    xml.push_str("</CommittedBlocks>");

    xml.push_str("<UncommittedBlocks>");
    for block in uncommitted {
        xml.push_str("<Block>");
        xml.push_str(&format!("<Name>{}</Name>", xml_escape(&block.block_id)));
        xml.push_str(&format!("<Size>{}</Size>", block.size));
        xml.push_str("</Block>");
    }
    xml.push_str("</UncommittedBlocks>");

    xml.push_str("</BlockList>");
    xml
}

/// Serializes page ranges to XML.
pub fn serialize_page_ranges(ranges: &[PageRange]) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<PageList>");
    for range in ranges {
        xml.push_str("<PageRange>");
        xml.push_str(&format!("<Start>{}</Start>", range.start));
        xml.push_str(&format!("<End>{}</End>", range.end));
        xml.push_str("</PageRange>");
    }
    xml.push_str("</PageList>");
    xml
}

/// Serializes page range diff to XML.
pub fn serialize_page_ranges_diff(ranges: &[PageRangeDiff]) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<PageList>");
    for range in ranges {
        if range.is_clear {
            xml.push_str("<ClearRange>");
            xml.push_str(&format!("<Start>{}</Start>", range.start));
            xml.push_str(&format!("<End>{}</End>", range.end));
            xml.push_str("</ClearRange>");
        } else {
            xml.push_str("<PageRange>");
            xml.push_str(&format!("<Start>{}</Start>", range.start));
            xml.push_str(&format!("<End>{}</End>", range.end));
            xml.push_str("</PageRange>");
        }
    }
    xml.push_str("</PageList>");
    xml
}

/// Serializes service properties to XML.
pub fn serialize_service_properties(props: &ServiceProperties) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<StorageServiceProperties>");

    // Logging
    xml.push_str("<Logging>");
    xml.push_str(&format!("<Version>{}</Version>", props.logging.version));
    xml.push_str(&format!("<Read>{}</Read>", props.logging.read));
    xml.push_str(&format!("<Write>{}</Write>", props.logging.write));
    xml.push_str(&format!("<Delete>{}</Delete>", props.logging.delete));
    xml.push_str(&serialize_retention_policy(&props.logging.retention_policy));
    xml.push_str("</Logging>");

    // Hour metrics
    xml.push_str("<HourMetrics>");
    xml.push_str(&format!("<Version>{}</Version>", props.hour_metrics.version));
    xml.push_str(&format!("<Enabled>{}</Enabled>", props.hour_metrics.enabled));
    if props.hour_metrics.enabled {
        xml.push_str(&format!(
            "<IncludeAPIs>{}</IncludeAPIs>",
            props.hour_metrics.include_apis
        ));
    }
    xml.push_str(&serialize_retention_policy(
        &props.hour_metrics.retention_policy,
    ));
    xml.push_str("</HourMetrics>");

    // Minute metrics
    xml.push_str("<MinuteMetrics>");
    xml.push_str(&format!(
        "<Version>{}</Version>",
        props.minute_metrics.version
    ));
    xml.push_str(&format!(
        "<Enabled>{}</Enabled>",
        props.minute_metrics.enabled
    ));
    if props.minute_metrics.enabled {
        xml.push_str(&format!(
            "<IncludeAPIs>{}</IncludeAPIs>",
            props.minute_metrics.include_apis
        ));
    }
    xml.push_str(&serialize_retention_policy(
        &props.minute_metrics.retention_policy,
    ));
    xml.push_str("</MinuteMetrics>");

    // CORS
    if !props.cors.is_empty() {
        xml.push_str("<Cors>");
        for rule in &props.cors {
            xml.push_str(&serialize_cors_rule(rule));
        }
        xml.push_str("</Cors>");
    }

    // Default service version
    if let Some(ref version) = props.default_service_version {
        xml.push_str(&format!(
            "<DefaultServiceVersion>{}</DefaultServiceVersion>",
            xml_escape(version)
        ));
    }

    // Delete retention policy
    xml.push_str("<DeleteRetentionPolicy>");
    xml.push_str(&format!(
        "<Enabled>{}</Enabled>",
        props.delete_retention_policy.enabled
    ));
    if let Some(days) = props.delete_retention_policy.days {
        xml.push_str(&format!("<Days>{}</Days>", days));
    }
    xml.push_str("</DeleteRetentionPolicy>");

    // Static website
    xml.push_str("<StaticWebsite>");
    xml.push_str(&format!(
        "<Enabled>{}</Enabled>",
        props.static_website.enabled
    ));
    if let Some(ref doc) = props.static_website.index_document {
        xml.push_str(&format!(
            "<IndexDocument>{}</IndexDocument>",
            xml_escape(doc)
        ));
    }
    if let Some(ref doc) = props.static_website.error_document_404_path {
        xml.push_str(&format!(
            "<ErrorDocument404Path>{}</ErrorDocument404Path>",
            xml_escape(doc)
        ));
    }
    xml.push_str("</StaticWebsite>");

    xml.push_str("</StorageServiceProperties>");
    xml
}

fn serialize_retention_policy(policy: &RetentionPolicy) -> String {
    let mut xml = String::from("<RetentionPolicy>");
    xml.push_str(&format!("<Enabled>{}</Enabled>", policy.enabled));
    if let Some(days) = policy.days {
        xml.push_str(&format!("<Days>{}</Days>", days));
    }
    xml.push_str("</RetentionPolicy>");
    xml
}

fn serialize_cors_rule(rule: &CorsRule) -> String {
    let mut xml = String::from("<CorsRule>");
    xml.push_str(&format!(
        "<AllowedOrigins>{}</AllowedOrigins>",
        rule.allowed_origins.join(",")
    ));
    xml.push_str(&format!(
        "<AllowedMethods>{}</AllowedMethods>",
        rule.allowed_methods.join(",")
    ));
    xml.push_str(&format!(
        "<AllowedHeaders>{}</AllowedHeaders>",
        rule.allowed_headers.join(",")
    ));
    xml.push_str(&format!(
        "<ExposedHeaders>{}</ExposedHeaders>",
        rule.exposed_headers.join(",")
    ));
    xml.push_str(&format!(
        "<MaxAgeInSeconds>{}</MaxAgeInSeconds>",
        rule.max_age_in_seconds
    ));
    xml.push_str("</CorsRule>");
    xml
}

/// Serializes service stats to XML.
pub fn serialize_service_stats(stats: &ServiceStats) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<StorageServiceStats>");
    xml.push_str("<GeoReplication>");
    xml.push_str(&format!(
        "<Status>{}</Status>",
        stats.geo_replication.status.as_str()
    ));
    if let Some(ref time) = stats.geo_replication.last_sync_time {
        xml.push_str(&format!("<LastSyncTime>{}</LastSyncTime>", xml_escape(time)));
    }
    xml.push_str("</GeoReplication>");
    xml.push_str("</StorageServiceStats>");
    xml
}

/// Serializes signed identifiers (access policy) to XML.
pub fn serialize_signed_identifiers(identifiers: &[SignedIdentifier]) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<SignedIdentifiers>");
    for id in identifiers {
        xml.push_str("<SignedIdentifier>");
        xml.push_str(&format!("<Id>{}</Id>", xml_escape(&id.id)));
        xml.push_str("<AccessPolicy>");
        if let Some(ref start) = id.access_policy.start {
            xml.push_str(&format!(
                "<Start>{}</Start>",
                start.format("%Y-%m-%dT%H:%M:%SZ")
            ));
        }
        if let Some(ref expiry) = id.access_policy.expiry {
            xml.push_str(&format!(
                "<Expiry>{}</Expiry>",
                expiry.format("%Y-%m-%dT%H:%M:%SZ")
            ));
        }
        xml.push_str(&format!(
            "<Permission>{}</Permission>",
            xml_escape(&id.access_policy.permission)
        ));
        xml.push_str("</AccessPolicy>");
        xml.push_str("</SignedIdentifier>");
    }
    xml.push_str("</SignedIdentifiers>");
    xml
}

/// Serializes user delegation key to XML.
pub fn serialize_user_delegation_key(key: &UserDelegationKey) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<UserDelegationKey>");
    xml.push_str(&format!(
        "<SignedOid>{}</SignedOid>",
        xml_escape(&key.signed_oid)
    ));
    xml.push_str(&format!(
        "<SignedTid>{}</SignedTid>",
        xml_escape(&key.signed_tid)
    ));
    xml.push_str(&format!(
        "<SignedStart>{}</SignedStart>",
        xml_escape(&key.signed_start)
    ));
    xml.push_str(&format!(
        "<SignedExpiry>{}</SignedExpiry>",
        xml_escape(&key.signed_expiry)
    ));
    xml.push_str(&format!(
        "<SignedService>{}</SignedService>",
        xml_escape(&key.signed_service)
    ));
    xml.push_str(&format!(
        "<SignedVersion>{}</SignedVersion>",
        xml_escape(&key.signed_version)
    ));
    xml.push_str(&format!("<Value>{}</Value>", xml_escape(&key.value)));
    xml.push_str("</UserDelegationKey>");
    xml
}

/// Serializes blob tags to XML.
pub fn serialize_tags(tags: &std::collections::HashMap<String, String>) -> String {
    let mut xml = String::from(r#"<?xml version="1.0" encoding="utf-8"?>"#);
    xml.push_str("<Tags><TagSet>");
    for (key, value) in tags {
        xml.push_str(&format!(
            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
            xml_escape(key),
            xml_escape(value)
        ));
    }
    xml.push_str("</TagSet></Tags>");
    xml
}

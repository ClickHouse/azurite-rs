//! XML request deserialization for Azure Blob Storage API.

use chrono::{DateTime, Utc};
use quick_xml::events::Event;
use quick_xml::Reader;
use std::collections::HashMap;

use crate::error::{ErrorCode, StorageError, StorageResult};
use crate::models::{
    AccessPolicy, CorsRule, DeleteRetentionPolicy, LoggingConfig, MetricsConfig,
    RetentionPolicy, ServiceProperties, SignedIdentifier, StaticWebsite,
};

/// Parses a BlockList XML request body.
#[derive(Debug, Default)]
pub struct BlockListRequest {
    /// Block IDs from Committed section.
    pub committed: Vec<String>,
    /// Block IDs from Uncommitted section.
    pub uncommitted: Vec<String>,
    /// Block IDs from Latest section.
    pub latest: Vec<String>,
}

impl BlockListRequest {
    pub fn parse(xml: &str) -> StorageResult<Self> {
        let mut reader = Reader::from_str(xml);
        reader.trim_text(true);

        let mut result = Self::default();
        let mut buf = Vec::new();
        let mut current_element: Option<String> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    current_element = Some(String::from_utf8_lossy(e.name().as_ref()).to_string());
                },
                Ok(Event::End(_)) => {
                    current_element = None;
                },
                Ok(Event::Text(e)) => {
                    if let Some(ref elem) = current_element {
                        let block_id = e.unescape().map_err(|_| {
                            StorageError::new(ErrorCode::InvalidXmlDocument)
                        })?.to_string();

                        // Skip empty strings (whitespace between elements)
                        if block_id.trim().is_empty() {
                            continue;
                        }

                        match elem.as_str() {
                            "Committed" => result.committed.push(block_id),
                            "Uncommitted" => result.uncommitted.push(block_id),
                            "Latest" => result.latest.push(block_id),
                            _ => {}
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(_) => return Err(StorageError::new(ErrorCode::InvalidXmlDocument)),
                _ => {}
            }
            buf.clear();
        }

        Ok(result)
    }

    /// Returns all block IDs in order.
    pub fn all_blocks(&self) -> Vec<(String, BlockListType)> {
        let mut result = Vec::new();
        for id in &self.committed {
            result.push((id.clone(), BlockListType::Committed));
        }
        for id in &self.uncommitted {
            result.push((id.clone(), BlockListType::Uncommitted));
        }
        for id in &self.latest {
            result.push((id.clone(), BlockListType::Latest));
        }
        result
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BlockListType {
    Committed,
    Uncommitted,
    Latest,
}

/// Parses service properties XML.
pub fn parse_service_properties(xml: &str) -> StorageResult<ServiceProperties> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut props = ServiceProperties::default();
    let mut buf = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut current_text = String::new();

    // Temporary storage for nested structures
    let mut logging = LoggingConfig::default();
    let mut hour_metrics = MetricsConfig::default();
    let mut minute_metrics = MetricsConfig::default();
    let mut cors_rules: Vec<CorsRule> = Vec::new();
    let mut current_cors_rule = CorsRule::default();
    let mut delete_retention = DeleteRetentionPolicy::default();
    let mut static_website = StaticWebsite::default();
    let mut retention = RetentionPolicy::default();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                path.push(name);
                current_text.clear();
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                let path_str: Vec<&str> = path.iter().map(|s| s.as_str()).collect();

                match path_str.as_slice() {
                    [_, "Logging", "Version"] if name == "Version" => {
                        logging.version = current_text.clone();
                    }
                    [_, "Logging", "Read"] if name == "Read" => {
                        logging.read = current_text == "true";
                    }
                    [_, "Logging", "Write"] if name == "Write" => {
                        logging.write = current_text == "true";
                    }
                    [_, "Logging", "Delete"] if name == "Delete" => {
                        logging.delete = current_text == "true";
                    }
                    [_, "Logging", "RetentionPolicy", "Enabled"] if name == "Enabled" => {
                        retention.enabled = current_text == "true";
                    }
                    [_, "Logging", "RetentionPolicy", "Days"] if name == "Days" => {
                        retention.days = current_text.parse().ok();
                    }
                    [_, "Logging", "RetentionPolicy"] if name == "RetentionPolicy" => {
                        logging.retention_policy = retention.clone();
                        retention = RetentionPolicy::default();
                    }
                    [_, "Logging"] if name == "Logging" => {
                        props.logging = logging.clone();
                    }
                    [_, "HourMetrics", "Version"] if name == "Version" => {
                        hour_metrics.version = current_text.clone();
                    }
                    [_, "HourMetrics", "Enabled"] if name == "Enabled" => {
                        hour_metrics.enabled = current_text == "true";
                    }
                    [_, "HourMetrics", "IncludeAPIs"] if name == "IncludeAPIs" => {
                        hour_metrics.include_apis = current_text == "true";
                    }
                    [_, "HourMetrics", "RetentionPolicy", "Enabled"] if name == "Enabled" => {
                        retention.enabled = current_text == "true";
                    }
                    [_, "HourMetrics", "RetentionPolicy", "Days"] if name == "Days" => {
                        retention.days = current_text.parse().ok();
                    }
                    [_, "HourMetrics", "RetentionPolicy"] if name == "RetentionPolicy" => {
                        hour_metrics.retention_policy = retention.clone();
                        retention = RetentionPolicy::default();
                    }
                    [_, "HourMetrics"] if name == "HourMetrics" => {
                        props.hour_metrics = hour_metrics.clone();
                    }
                    [_, "MinuteMetrics", "Version"] if name == "Version" => {
                        minute_metrics.version = current_text.clone();
                    }
                    [_, "MinuteMetrics", "Enabled"] if name == "Enabled" => {
                        minute_metrics.enabled = current_text == "true";
                    }
                    [_, "MinuteMetrics", "IncludeAPIs"] if name == "IncludeAPIs" => {
                        minute_metrics.include_apis = current_text == "true";
                    }
                    [_, "MinuteMetrics", "RetentionPolicy", "Enabled"]
                        if name == "Enabled" =>
                    {
                        retention.enabled = current_text == "true";
                    }
                    [_, "MinuteMetrics", "RetentionPolicy", "Days"] if name == "Days" => {
                        retention.days = current_text.parse().ok();
                    }
                    [_, "MinuteMetrics", "RetentionPolicy"]
                        if name == "RetentionPolicy" =>
                    {
                        minute_metrics.retention_policy = retention.clone();
                        retention = RetentionPolicy::default();
                    }
                    [_, "MinuteMetrics"] if name == "MinuteMetrics" => {
                        props.minute_metrics = minute_metrics.clone();
                    }
                    [_, "Cors", "CorsRule", "AllowedOrigins"]
                        if name == "AllowedOrigins" =>
                    {
                        current_cors_rule.allowed_origins =
                            current_text.split(',').map(String::from).collect();
                    }
                    [_, "Cors", "CorsRule", "AllowedMethods"]
                        if name == "AllowedMethods" =>
                    {
                        current_cors_rule.allowed_methods =
                            current_text.split(',').map(String::from).collect();
                    }
                    [_, "Cors", "CorsRule", "AllowedHeaders"]
                        if name == "AllowedHeaders" =>
                    {
                        current_cors_rule.allowed_headers =
                            current_text.split(',').map(String::from).collect();
                    }
                    [_, "Cors", "CorsRule", "ExposedHeaders"]
                        if name == "ExposedHeaders" =>
                    {
                        current_cors_rule.exposed_headers =
                            current_text.split(',').map(String::from).collect();
                    }
                    [_, "Cors", "CorsRule", "MaxAgeInSeconds"]
                        if name == "MaxAgeInSeconds" =>
                    {
                        current_cors_rule.max_age_in_seconds =
                            current_text.parse().unwrap_or(0);
                    }
                    [_, "Cors", "CorsRule"] if name == "CorsRule" => {
                        cors_rules.push(current_cors_rule.clone());
                        current_cors_rule = CorsRule::default();
                    }
                    [_, "Cors"] if name == "Cors" => {
                        props.cors = cors_rules.clone();
                    }
                    [_, "DefaultServiceVersion"] if name == "DefaultServiceVersion" => {
                        props.default_service_version = Some(current_text.clone());
                    }
                    [_, "DeleteRetentionPolicy", "Enabled"] if name == "Enabled" => {
                        delete_retention.enabled = current_text == "true";
                    }
                    [_, "DeleteRetentionPolicy", "Days"] if name == "Days" => {
                        delete_retention.days = current_text.parse().ok();
                    }
                    [_, "DeleteRetentionPolicy"] if name == "DeleteRetentionPolicy" => {
                        props.delete_retention_policy = delete_retention.clone();
                    }
                    [_, "StaticWebsite", "Enabled"] if name == "Enabled" => {
                        static_website.enabled = current_text == "true";
                    }
                    [_, "StaticWebsite", "IndexDocument"] if name == "IndexDocument" => {
                        static_website.index_document = Some(current_text.clone());
                    }
                    [_, "StaticWebsite", "ErrorDocument404Path"]
                        if name == "ErrorDocument404Path" =>
                    {
                        static_website.error_document_404_path = Some(current_text.clone());
                    }
                    [_, "StaticWebsite"] if name == "StaticWebsite" => {
                        props.static_website = static_website.clone();
                    }
                    _ => {}
                }

                path.pop();
                current_text.clear();
            }
            Ok(Event::Text(e)) => {
                current_text = e.unescape().map_err(|_| {
                    StorageError::new(ErrorCode::InvalidXmlDocument)
                })?.to_string();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(StorageError::new(ErrorCode::InvalidXmlDocument)),
            _ => {}
        }
        buf.clear();
    }

    Ok(props)
}

/// Parses signed identifiers (access policy) XML.
pub fn parse_signed_identifiers(xml: &str) -> StorageResult<Vec<SignedIdentifier>> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut identifiers = Vec::new();
    let mut buf = Vec::new();
    let mut current_text = String::new();

    let mut current_id = String::new();
    let mut current_start: Option<DateTime<Utc>> = None;
    let mut current_expiry: Option<DateTime<Utc>> = None;
    let mut current_permission = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(_e)) => {
                current_text.clear();
            }
            Ok(Event::End(e)) => {
                let name = e.name();
                let name_bytes = name.as_ref();

                if name_bytes == b"Id" {
                    current_id = current_text.clone();
                } else if name_bytes == b"Start" {
                    current_start = DateTime::parse_from_rfc3339(&current_text)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc));
                } else if name_bytes == b"Expiry" {
                    current_expiry = DateTime::parse_from_rfc3339(&current_text)
                        .ok()
                        .map(|dt| dt.with_timezone(&Utc));
                } else if name_bytes == b"Permission" {
                    current_permission = current_text.clone();
                } else if name_bytes == b"SignedIdentifier" {
                    identifiers.push(SignedIdentifier {
                        id: current_id.clone(),
                        access_policy: AccessPolicy {
                            start: current_start.take(),
                            expiry: current_expiry.take(),
                            permission: current_permission.clone(),
                        },
                    });
                    current_id.clear();
                    current_permission.clear();
                }

                current_text.clear();
            }
            Ok(Event::Text(e)) => {
                current_text = e.unescape().map_err(|_| {
                    StorageError::new(ErrorCode::InvalidXmlDocument)
                })?.to_string();
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(StorageError::new(ErrorCode::InvalidXmlDocument)),
            _ => {}
        }
        buf.clear();
    }

    Ok(identifiers)
}

/// Parses blob tags XML.
pub fn parse_tags(xml: &str) -> StorageResult<HashMap<String, String>> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut tags = HashMap::new();
    let mut buf = Vec::new();
    let mut current_text = String::new();
    let mut current_key = String::new();
    let mut in_key = false;
    let mut in_value = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.name().as_ref() {
                b"Key" => in_key = true,
                b"Value" => in_value = true,
                _ => {}
            },
            Ok(Event::End(e)) => match e.name().as_ref() {
                b"Key" => {
                    current_key = current_text.clone();
                    current_text.clear();
                    in_key = false;
                }
                b"Value" => {
                    tags.insert(current_key.clone(), current_text.clone());
                    current_text.clear();
                    current_key.clear();
                    in_value = false;
                }
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if in_key || in_value {
                    current_text = e.unescape().map_err(|_| {
                        StorageError::new(ErrorCode::InvalidXmlDocument)
                    })?.to_string();
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(StorageError::new(ErrorCode::InvalidXmlDocument)),
            _ => {}
        }
        buf.clear();
    }

    Ok(tags)
}

/// Parses user delegation key request XML.
pub fn parse_user_delegation_key_request(xml: &str) -> StorageResult<(String, String)> {
    let mut reader = Reader::from_str(xml);
    reader.trim_text(true);

    let mut buf = Vec::new();
    let mut start = String::new();
    let mut expiry = String::new();
    let mut current_text = String::new();
    let mut in_start = false;
    let mut in_expiry = false;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match e.name().as_ref() {
                b"Start" => in_start = true,
                b"Expiry" => in_expiry = true,
                _ => {}
            },
            Ok(Event::End(e)) => match e.name().as_ref() {
                b"Start" => {
                    start = current_text.clone();
                    current_text.clear();
                    in_start = false;
                }
                b"Expiry" => {
                    expiry = current_text.clone();
                    current_text.clear();
                    in_expiry = false;
                }
                _ => {}
            },
            Ok(Event::Text(e)) => {
                if in_start || in_expiry {
                    current_text = e.unescape().map_err(|_| {
                        StorageError::new(ErrorCode::InvalidXmlDocument)
                    })?.to_string();
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => return Err(StorageError::new(ErrorCode::InvalidXmlDocument)),
            _ => {}
        }
        buf.clear();
    }

    Ok((start, expiry))
}

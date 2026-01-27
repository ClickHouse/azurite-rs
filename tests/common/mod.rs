//! Common test utilities.

use std::sync::Arc;
use tokio::net::TcpListener;

use azurite_rs::{BlobServer, BlobServerBuilder, Config, MemoryExtentStore, MemoryMetadataStore};

/// Test server wrapper.
pub struct TestServer {
    pub base_url: String,
    pub account: String,
    pub key: String,
}

impl TestServer {
    /// Creates and starts a test server on a random port.
    pub async fn start() -> Self {
        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let config = Config {
            host: "127.0.0.1".to_string(),
            blob_port: port,
            ..Config::default()
        };

        let account = config.accounts[0].name.clone();
        let key = config.accounts[0].key.clone();
        let base_url = format!("http://127.0.0.1:{}", port);

        let server = BlobServer::new(config);

        // Start server in background
        tokio::spawn(async move {
            server.run().await.unwrap();
        });

        // Wait for server to be ready
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Self {
            base_url,
            account,
            key,
        }
    }

    /// Returns the URL for a container.
    pub fn container_url(&self, container: &str) -> String {
        format!("{}/{}/{}", self.base_url, self.account, container)
    }

    /// Returns the URL for a blob.
    pub fn blob_url(&self, container: &str, blob: &str) -> String {
        format!("{}/{}/{}/{}", self.base_url, self.account, container, blob)
    }
}

/// Creates authorization header value for SharedKey.
pub fn create_auth_header(
    method: &str,
    account: &str,
    key: &str,
    path: &str,
    content_length: Option<u64>,
    content_type: Option<&str>,
    date: &str,
    extra_headers: &[(&str, &str)],
) -> String {
    use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    // Build canonicalized headers
    let mut ms_headers: Vec<_> = extra_headers
        .iter()
        .filter(|(k, _)| k.starts_with("x-ms-"))
        .map(|(k, v)| (k.to_lowercase(), *v))
        .collect();
    ms_headers.push(("x-ms-date".to_string(), date));
    ms_headers.push(("x-ms-version".to_string(), "2021-10-04"));
    ms_headers.sort_by(|a, b| a.0.cmp(&b.0));

    let canonicalized_headers: String = ms_headers
        .iter()
        .map(|(k, v)| format!("{}:{}", k, v))
        .collect::<Vec<_>>()
        .join("\n");

    // Build canonicalized resource
    let canonicalized_resource = format!("/{}{}", account, path);

    // Build string to sign
    let content_length_str = content_length
        .filter(|&l| l > 0)
        .map(|l| l.to_string())
        .unwrap_or_default();

    let string_to_sign = format!(
        "{}\n\n\n{}\n\n{}\n\n\n\n\n\n\n{}\n{}",
        method,
        content_length_str,
        content_type.unwrap_or(""),
        canonicalized_headers,
        canonicalized_resource
    );

    // Compute signature
    let key_bytes = BASE64.decode(key).unwrap();
    let mut mac = HmacSha256::new_from_slice(&key_bytes).unwrap();
    mac.update(string_to_sign.as_bytes());
    let signature = BASE64.encode(mac.finalize().into_bytes());

    format!("SharedKey {}:{}", account, signature)
}

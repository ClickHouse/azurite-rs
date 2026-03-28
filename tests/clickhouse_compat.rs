//! ClickHouse compatibility tests.
//!
//! These tests verify that the emulator works correctly with ClickHouse's
//! Azure Blob Storage integration.

mod common;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use common::TestServer;

async fn create_container(server: &TestServer, name: &str) {
    let client = reqwest::Client::new();
    let url = format!("{}?restype=container", server.container_url(name));
    client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();
}

/// Test multipart upload with 3+ blocks (common in ClickHouse for large files).
#[tokio::test]
async fn test_clickhouse_multipart_upload() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-data").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("clickhouse-data", "data/part_000.bin");

    // Simulate ClickHouse multipart upload with multiple blocks
    let num_blocks = 5;
    let block_size = 64 * 1024; // 64 KB blocks
    let block_ids: Vec<String> = (0..num_blocks)
        .map(|i| BASE64.encode(format!("{:08x}", i)))
        .collect();

    // Stage blocks
    for (i, block_id) in block_ids.iter().enumerate() {
        let stage_url = format!("{}?comp=block&blockid={}", blob_url, block_id);
        let data = vec![i as u8; block_size];

        let response = client
            .put(&stage_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .body(data)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 201);
    }

    // Commit blocks
    let block_list: String = block_ids
        .iter()
        .map(|id| format!("<Latest>{}</Latest>", id))
        .collect();

    let commit_url = format!("{}?comp=blocklist", blob_url);
    let response = client
        .put(&commit_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("Content-Type", "application/xml")
        .body(format!("<BlockList>{}</BlockList>", block_list))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);

    // Verify size
    let response = client
        .head(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    let size: usize = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(size, num_blocks * block_size);
}

/// Test partial download with range requests (common for ClickHouse reads).
#[tokio::test]
async fn test_clickhouse_range_read() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-range").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("clickhouse-range", "marks/part.mrk");

    // Upload a blob with known content
    let content: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    client
        .put(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .body(content.clone())
        .send()
        .await
        .unwrap();

    // Read specific ranges (simulating ClickHouse mark reads)
    let test_ranges = vec![
        (0, 63),
        (100, 199),
        (512, 767),
        (900, 1023),
    ];

    for (start, end) in test_ranges {
        let response = client
            .get(&blob_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("Range", format!("bytes={}-{}", start, end))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 206);

        let body = response.bytes().await.unwrap();
        let expected = &content[start..=end];
        assert_eq!(body.len(), expected.len(), "Range {}-{} size mismatch", start, end);
        assert_eq!(&body[..], expected, "Range {}-{} content mismatch", start, end);
    }
}

/// Test listing blobs with prefix (common for ClickHouse partition discovery).
#[tokio::test]
async fn test_clickhouse_list_prefix() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-list").await;

    let client = reqwest::Client::new();

    // Upload blobs simulating ClickHouse partition structure
    let partitions = vec![
        "data/all_1_1_0/",
        "data/all_2_2_0/",
        "data/all_3_3_0/",
        "data/detached/",
    ];

    for partition in &partitions {
        for file in &["data.bin", "data.mrk", "columns.txt"] {
            let blob_url = server.blob_url("clickhouse-list", &format!("{}{}", partition, file));
            client
                .put(&blob_url)
                .header("x-ms-version", "2021-10-04")
                .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
                .header("x-ms-blob-type", "BlockBlob")
                .body("content")
                .send()
                .await
                .unwrap();
        }
    }

    // List all blobs under data/
    let list_url = format!(
        "{}?restype=container&comp=list&prefix=data/",
        server.container_url("clickhouse-list")
    );
    let response = client
        .get(&list_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("all_1_1_0"));
    assert!(body.contains("all_2_2_0"));
    assert!(body.contains("all_3_3_0"));

    // List only all_1_1_0 partition
    let list_url = format!(
        "{}?restype=container&comp=list&prefix=data/all_1_1_0/",
        server.container_url("clickhouse-list")
    );
    let response = client
        .get(&list_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("data/all_1_1_0/data.bin"));
    assert!(!body.contains("all_2_2_0"));
}

/// Test copy operation (used by ClickHouse for merges).
#[tokio::test]
async fn test_clickhouse_copy() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-copy").await;

    let client = reqwest::Client::new();

    // Upload source blob
    let source_url = server.blob_url("clickhouse-copy", "data/all_1_1_0/data.bin");
    let content = vec![0u8; 10000];
    client
        .put(&source_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .body(content.clone())
        .send()
        .await
        .unwrap();

    // Copy to new location
    let dest_url = server.blob_url("clickhouse-copy", "data/all_1_1_1/data.bin");
    let response = client
        .put(&dest_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-copy-source", &source_url)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202);
    assert!(response.headers().contains_key("x-ms-copy-id"));

    // Verify copy completed
    let response = client
        .head(&dest_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let size: usize = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(size, content.len());
}

/// Test batch delete (used by ClickHouse's BlobKillerThread for cleanup).
/// Verifies the multipart/mixed batch protocol matches Azure C++ SDK expectations.
#[tokio::test]
async fn test_clickhouse_batch_delete() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-batch").await;

    let client = reqwest::Client::new();

    // Upload 5 blobs to delete in a batch
    let blob_names: Vec<String> = (0..5)
        .map(|i| format!("data/part_{}.bin", i))
        .collect();

    for name in &blob_names {
        let url = server.blob_url("clickhouse-batch", name);
        client
            .put(&url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("x-ms-blob-type", "BlockBlob")
            .body("test content")
            .send()
            .await
            .unwrap();
    }

    // Build a batch delete request matching the Azure C++ SDK format.
    // The SDK uses a flat multipart structure (no changeset nesting).
    let batch_boundary = format!("batch_{}", uuid::Uuid::new_v4());
    let mut batch_body = String::new();

    for (i, name) in blob_names.iter().enumerate() {
        batch_body.push_str(&format!("--{}\r\n", batch_boundary));
        batch_body.push_str("Content-Type: application/http\r\n");
        batch_body.push_str("Content-Transfer-Encoding: binary\r\n");
        batch_body.push_str(&format!("Content-ID: {}\r\n", i));
        batch_body.push_str("\r\n");
        batch_body.push_str(&format!(
            "DELETE /{}/clickhouse-batch/{} HTTP/1.1\r\n",
            server.account, name
        ));
        batch_body.push_str("x-ms-version: 2021-10-04\r\n");
        batch_body.push_str(&format!(
            "x-ms-date: {}\r\n",
            chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT")
        ));
        batch_body.push_str("\r\n");
    }
    batch_body.push_str(&format!("--{}--\r\n", batch_boundary));

    // Submit batch at service level
    let batch_url = format!("{}/{}?comp=batch", server.base_url, server.account);
    let response = client
        .post(&batch_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header(
            "Content-Type",
            format!("multipart/mixed; boundary={}", batch_boundary),
        )
        .body(batch_body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202, "Batch request should return 202 Accepted");

    let resp_content_type = response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        resp_content_type.starts_with("multipart/mixed; boundary="),
        "Response Content-Type must be multipart/mixed"
    );

    let resp_body = response.text().await.unwrap();

    // Verify the response body follows the flat multipart format expected by the Azure C++ SDK:
    // 1. Body starts with --boundary (the SDK's Parser::Consume expects this)
    let resp_boundary = resp_content_type
        .strip_prefix("multipart/mixed; boundary=")
        .unwrap();
    assert!(
        resp_body.starts_with(&format!("--{}", resp_boundary)),
        "Response body must start with --boundary. Got: {:?}",
        &resp_body[..resp_body.len().min(100)]
    );

    // 2. Each sub-response must have Content-ID matching the request
    for i in 0..5 {
        assert!(
            resp_body.contains(&format!("Content-ID: {}", i)),
            "Response must contain Content-ID: {}",
            i
        );
    }

    // 3. Each sub-response must have HTTP/1.1 202 status
    let accepted_count = resp_body.matches("HTTP/1.1 202").count();
    assert_eq!(
        accepted_count, 5,
        "All 5 sub-requests should return 202 Accepted, got {}",
        accepted_count
    );

    // 4. Body must end with --boundary--
    assert!(
        resp_body.contains(&format!("--{}--", resp_boundary)),
        "Response body must contain end boundary marker"
    );

    // Verify blobs are actually deleted
    for name in &blob_names {
        let url = server.blob_url("clickhouse-batch", name);
        let response = client
            .head(&url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            404,
            "Blob {} should be deleted",
            name
        );
    }
}

/// Test batch delete at container level (the other endpoint ClickHouse can use).
#[tokio::test]
async fn test_clickhouse_batch_delete_container_level() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-batch2").await;

    let client = reqwest::Client::new();

    // Upload 3 blobs
    for i in 0..3 {
        let url = server.blob_url("clickhouse-batch2", &format!("blob_{}", i));
        client
            .put(&url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("x-ms-blob-type", "BlockBlob")
            .body("data")
            .send()
            .await
            .unwrap();
    }

    // Build batch delete at container level
    let batch_boundary = format!("batch_{}", uuid::Uuid::new_v4());
    let mut batch_body = String::new();

    for i in 0..3 {
        batch_body.push_str(&format!("--{}\r\n", batch_boundary));
        batch_body.push_str("Content-Type: application/http\r\n");
        batch_body.push_str("Content-Transfer-Encoding: binary\r\n");
        batch_body.push_str(&format!("Content-ID: {}\r\n", i));
        batch_body.push_str("\r\n");
        batch_body.push_str(&format!(
            "DELETE /{}/clickhouse-batch2/blob_{} HTTP/1.1\r\n",
            server.account, i
        ));
        batch_body.push_str("x-ms-version: 2021-10-04\r\n");
        batch_body.push_str("\r\n");
    }
    batch_body.push_str(&format!("--{}--\r\n", batch_boundary));

    // Submit batch at container level
    let batch_url = format!(
        "{}/{}?restype=container&comp=batch",
        server.container_url("clickhouse-batch2"), ""
    ).replace("/?", "?");
    let response = client
        .post(&batch_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header(
            "Content-Type",
            format!("multipart/mixed; boundary={}", batch_boundary),
        )
        .body(batch_body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202);

    let resp_body = response.text().await.unwrap();
    let accepted_count = resp_body.matches("HTTP/1.1 202").count();
    assert_eq!(accepted_count, 3, "All 3 sub-requests should return 202");
}

/// Test batch delete with non-existent blobs (should return 404 per sub-request).
#[tokio::test]
async fn test_clickhouse_batch_delete_not_found() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-batch3").await;

    let client = reqwest::Client::new();

    let batch_boundary = format!("batch_{}", uuid::Uuid::new_v4());
    let mut batch_body = String::new();

    // Request to delete non-existent blobs
    for i in 0..2 {
        batch_body.push_str(&format!("--{}\r\n", batch_boundary));
        batch_body.push_str("Content-Type: application/http\r\n");
        batch_body.push_str("Content-Transfer-Encoding: binary\r\n");
        batch_body.push_str(&format!("Content-ID: {}\r\n", i));
        batch_body.push_str("\r\n");
        batch_body.push_str(&format!(
            "DELETE /{}/clickhouse-batch3/nonexistent_{} HTTP/1.1\r\n",
            server.account, i
        ));
        batch_body.push_str("x-ms-version: 2021-10-04\r\n");
        batch_body.push_str("\r\n");
    }
    batch_body.push_str(&format!("--{}--\r\n", batch_boundary));

    let batch_url = format!("{}/{}?comp=batch", server.base_url, server.account);
    let response = client
        .post(&batch_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header(
            "Content-Type",
            format!("multipart/mixed; boundary={}", batch_boundary),
        )
        .body(batch_body)
        .send()
        .await
        .unwrap();

    // Batch itself returns 202 even if sub-requests fail
    assert_eq!(response.status(), 202);

    let resp_body = response.text().await.unwrap();

    // Sub-responses should be 404
    let not_found_count = resp_body.matches("HTTP/1.1 404").count();
    assert_eq!(not_found_count, 2, "Both sub-requests should return 404");

    // Content-IDs must still be present
    assert!(resp_body.contains("Content-ID: 0"));
    assert!(resp_body.contains("Content-ID: 1"));
}

/// Test batch delete with URL-encoded paths (Azure C++ SDK encodes slashes as %2F).
#[tokio::test]
async fn test_clickhouse_batch_delete_url_encoded() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-batch-enc").await;

    let client = reqwest::Client::new();

    // Upload blobs with slashes in paths (like ClickHouse partition structure)
    let blob_names = vec![
        "data/all_1_1_0/data.bin",
        "data/all_1_1_0/data.mrk",
        "data/all_2_2_0/columns.txt",
    ];

    for name in &blob_names {
        let url = server.blob_url("clickhouse-batch-enc", name);
        client
            .put(&url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("x-ms-blob-type", "BlockBlob")
            .body("test content")
            .send()
            .await
            .unwrap();
    }

    // Build batch delete with URL-encoded paths (as the Azure C++ SDK sends them)
    let batch_boundary = format!("batch_{}", uuid::Uuid::new_v4());
    let mut batch_body = String::new();

    for (i, name) in blob_names.iter().enumerate() {
        // URL-encode the path: slashes become %2F
        let encoded_name = name.replace("/", "%2F");
        batch_body.push_str(&format!("--{}\r\n", batch_boundary));
        batch_body.push_str("Content-Type: application/http\r\n");
        batch_body.push_str("Content-Transfer-Encoding: binary\r\n");
        batch_body.push_str(&format!("Content-ID: {}\r\n", i));
        batch_body.push_str("\r\n");
        batch_body.push_str(&format!(
            "DELETE /{}/clickhouse-batch-enc/{} HTTP/1.1\r\n",
            server.account, encoded_name
        ));
        batch_body.push_str("x-ms-version: 2021-10-04\r\n");
        batch_body.push_str("\r\n");
    }
    batch_body.push_str(&format!("--{}--\r\n", batch_boundary));

    let batch_url = format!("{}/{}?comp=batch", server.base_url, server.account);
    let response = client
        .post(&batch_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header(
            "Content-Type",
            format!("multipart/mixed; boundary={}", batch_boundary),
        )
        .body(batch_body)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202);

    let resp_body = response.text().await.unwrap();
    let accepted_count = resp_body.matches("HTTP/1.1 202").count();
    assert_eq!(
        accepted_count, 3,
        "All 3 URL-encoded batch deletes should return 202, got {}. Response:\n{}",
        accepted_count, resp_body
    );

    // Verify blobs are actually deleted
    for name in &blob_names {
        let url = server.blob_url("clickhouse-batch-enc", name);
        let response = client
            .head(&url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "Blob {} should be deleted", name);
    }
}

/// Test hierarchical listing with delimiter (used by ClickHouse for virtual directories).
#[tokio::test]
async fn test_clickhouse_hierarchical_list() {
    let server = TestServer::start().await;
    create_container(&server, "clickhouse-hier").await;

    let client = reqwest::Client::new();

    // Upload blobs in hierarchical structure
    let blobs = vec![
        "data/partition1/file1.bin",
        "data/partition1/file2.bin",
        "data/partition2/file1.bin",
        "data/partition2/file2.bin",
        "data/file_at_root.bin",
    ];

    for blob in &blobs {
        let blob_url = server.blob_url("clickhouse-hier", blob);
        client
            .put(&blob_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("x-ms-blob-type", "BlockBlob")
            .body("content")
            .send()
            .await
            .unwrap();
    }

    // List with delimiter to get virtual directories
    let list_url = format!(
        "{}?restype=container&comp=list&prefix=data/&delimiter=/",
        server.container_url("clickhouse-hier")
    );
    let response = client
        .get(&list_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();

    // Should have BlobPrefix entries for virtual directories
    assert!(body.contains("BlobPrefix"));
    assert!(body.contains("data/partition1/"));
    assert!(body.contains("data/partition2/"));
    // And the file at root level
    assert!(body.contains("data/file_at_root.bin"));
}

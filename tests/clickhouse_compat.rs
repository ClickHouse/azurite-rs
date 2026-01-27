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

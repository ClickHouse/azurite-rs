//! Block blob operation tests.

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

#[tokio::test]
async fn test_stage_and_commit_blocks() {
    let server = TestServer::start().await;
    create_container(&server, "blockcontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("blockcontainer", "multipartblob.txt");

    // Stage blocks
    let block_ids: Vec<String> = (0..3)
        .map(|i| BASE64.encode(format!("block{:05}", i)))
        .collect();

    for (i, block_id) in block_ids.iter().enumerate() {
        let stage_url = format!("{}?comp=block&blockid={}", blob_url, block_id);
        let response = client
            .put(&stage_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .body(format!("Content of block {}", i))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 201, "Failed to stage block {}", i);
    }

    // Commit block list
    let block_list_xml = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<BlockList>
  <Latest>{}</Latest>
  <Latest>{}</Latest>
  <Latest>{}</Latest>
</BlockList>"#,
        block_ids[0], block_ids[1], block_ids[2]
    );

    let commit_url = format!("{}?comp=blocklist", blob_url);
    let response = client
        .put(&commit_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("Content-Type", "application/xml")
        .body(block_list_xml)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);

    // Download and verify
    let response = client
        .get(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("Content of block 0"));
    assert!(body.contains("Content of block 1"));
    assert!(body.contains("Content of block 2"));
}

#[tokio::test]
async fn test_get_block_list() {
    let server = TestServer::start().await;
    create_container(&server, "blocklistcontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("blocklistcontainer", "blocklistblob.txt");

    // Stage some blocks
    let block_ids: Vec<String> = (0..2)
        .map(|i| BASE64.encode(format!("block{:05}", i)))
        .collect();

    for (i, block_id) in block_ids.iter().enumerate() {
        let stage_url = format!("{}?comp=block&blockid={}", blob_url, block_id);
        client
            .put(&stage_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .body(format!("Block {}", i))
            .send()
            .await
            .unwrap();
    }

    // Get uncommitted block list
    let get_url = format!("{}?comp=blocklist&blocklisttype=uncommitted", blob_url);
    let response = client
        .get(&get_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("UncommittedBlocks"));
}

#[tokio::test]
async fn test_large_blob_multipart() {
    let server = TestServer::start().await;
    create_container(&server, "largecontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("largecontainer", "largefile.bin");

    // Create blocks with 1MB each (simulating larger file)
    let block_size = 1024 * 1024; // 1 MB
    let num_blocks = 5;
    let block_ids: Vec<String> = (0..num_blocks)
        .map(|i| BASE64.encode(format!("block{:05}", i)))
        .collect();

    for (i, block_id) in block_ids.iter().enumerate() {
        let stage_url = format!("{}?comp=block&blockid={}", blob_url, block_id);
        let data = vec![b'A' + (i as u8); block_size];

        let response = client
            .put(&stage_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .body(data)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), 201, "Failed to stage block {}", i);
    }

    // Commit all blocks
    let block_list_entries: String = block_ids
        .iter()
        .map(|id| format!("<Latest>{}</Latest>", id))
        .collect();

    let block_list_xml = format!(
        r#"<?xml version="1.0" encoding="utf-8"?>
<BlockList>{}</BlockList>"#,
        block_list_entries
    );

    let commit_url = format!("{}?comp=blocklist", blob_url);
    let response = client
        .put(&commit_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("Content-Type", "application/xml")
        .body(block_list_xml)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);

    // Verify blob size
    let response = client
        .head(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let content_length: usize = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(content_length, block_size * num_blocks);
}

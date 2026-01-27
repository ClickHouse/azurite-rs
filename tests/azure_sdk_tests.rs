//! Tests using the official Azure SDK for Rust.
//!
//! These tests verify that azurite-rs is compatible with the official Azure SDK.

mod common;

use azure_core::request_options::Metadata;
use azure_storage::prelude::*;
use azure_storage_blobs::blob::BlobType;
use azure_storage_blobs::prelude::*;
use common::TestServer;
use futures_util::StreamExt;

/// Creates a ClientBuilder for the test server.
fn create_builder(server: &TestServer) -> ClientBuilder {
    let storage_credentials = StorageCredentials::access_key(server.account.clone(), server.key.clone());
    let blob_endpoint = format!("{}/{}", server.base_url, server.account);

    ClientBuilder::with_location(
        azure_storage::CloudLocation::Custom {
            account: server.account.clone(),
            uri: blob_endpoint,
        },
        storage_credentials,
    )
}

// ============================================================================
// Container tests
// ============================================================================

#[tokio::test]
async fn test_sdk_create_container() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-test-container");

    container_client.create().await.unwrap();

    // Verify container exists by getting properties
    let props = container_client.get_properties().await.unwrap();
    // Container properties exist - just checking the call succeeded
    let _container = &props.container;
}

#[tokio::test]
async fn test_sdk_delete_container() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-delete-container");

    container_client.create().await.unwrap();
    container_client.delete().await.unwrap();

    // Verify container is deleted
    let result = container_client.get_properties().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_sdk_list_containers() {
    let server = TestServer::start().await;

    // Create several containers
    for i in 0..3 {
        let builder = create_builder(&server);
        let container_client = builder.container_client(format!("sdk-list-{}", i));
        container_client.create().await.unwrap();
    }

    // List and verify
    let builder = create_builder(&server);
    let service_client = builder.blob_service_client();
    let mut stream = service_client.list_containers().into_stream();
    let mut names = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result.unwrap();
        for container in page.containers {
            names.push(container.name);
        }
    }

    assert!(names.contains(&"sdk-list-0".to_string()));
    assert!(names.contains(&"sdk-list-1".to_string()));
    assert!(names.contains(&"sdk-list-2".to_string()));
}

#[tokio::test]
async fn test_sdk_list_containers_with_prefix() {
    let server = TestServer::start().await;

    // Create containers with different prefixes
    for prefix in &["alpha", "beta"] {
        for i in 0..2 {
            let builder = create_builder(&server);
            let name = format!("{}-container-{}", prefix, i);
            builder.container_client(&name).create().await.unwrap();
        }
    }

    // List with prefix filter
    let builder = create_builder(&server);
    let service_client = builder.blob_service_client();
    let mut stream = service_client.list_containers().prefix("alpha").into_stream();
    let mut names = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result.unwrap();
        for container in page.containers {
            names.push(container.name);
        }
    }

    assert_eq!(names.len(), 2);
    assert!(names.iter().all(|n| n.starts_with("alpha")));
}

// ============================================================================
// Blob upload/download tests
// ============================================================================

#[tokio::test]
async fn test_sdk_upload_download_blob() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-blob-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("test-blob.txt");
    let content = "Hello, Azure SDK!";

    // Upload
    blob_client.put_block_blob(content).await.unwrap();

    // Download
    let mut stream = blob_client.get().into_stream();
    let mut data = Vec::new();
    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        let bytes = chunk.data.collect().await.unwrap();
        data.extend_from_slice(&bytes);
    }

    assert_eq!(String::from_utf8(data).unwrap(), content);
}

#[tokio::test]
async fn test_sdk_upload_large_blob() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-large-blob-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("large-blob.bin");

    // Create 100KB of data
    let content: Vec<u8> = (0..100 * 1024).map(|i| (i % 256) as u8).collect();

    // Upload
    blob_client.put_block_blob(content.clone()).await.unwrap();

    // Download and verify
    let mut stream = blob_client.get().into_stream();
    let mut data = Vec::new();
    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        let bytes = chunk.data.collect().await.unwrap();
        data.extend_from_slice(&bytes);
    }

    assert_eq!(data.len(), content.len());
    assert_eq!(data, content);
}

#[tokio::test]
async fn test_sdk_blob_with_content_type() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-content-type-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("data.json");
    let content = r#"{"key": "value"}"#;

    // Upload with content type
    blob_client
        .put_block_blob(content)
        .content_type("application/json")
        .await
        .unwrap();

    // Verify content type
    let props = blob_client.get_properties().await.unwrap();
    assert_eq!(
        props.blob.properties.content_type.as_str(),
        "application/json"
    );
}

#[tokio::test]
async fn test_sdk_blob_with_metadata() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-blob-meta-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("meta-blob.txt");
    let content = "content with metadata";

    // Upload with metadata
    let mut metadata = Metadata::new();
    metadata.insert("author", "test");
    metadata.insert("version", "1.0");

    blob_client
        .put_block_blob(content)
        .metadata(metadata)
        .await
        .unwrap();

    // Verify metadata
    let props = blob_client.get_properties().await.unwrap();
    let metadata = props.blob.metadata.as_ref().expect("metadata should exist");
    assert_eq!(metadata.get("author"), Some(&"test".to_string()));
    assert_eq!(metadata.get("version"), Some(&"1.0".to_string()));
}

// ============================================================================
// Blob properties tests
// ============================================================================

#[tokio::test]
async fn test_sdk_get_blob_properties() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-props-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("props-blob.txt");
    let content = "Test content for properties";

    blob_client.put_block_blob(content).await.unwrap();

    let props = blob_client.get_properties().await.unwrap();

    // etag and last_modified are not Option types - just verify they exist
    let _etag = &props.blob.properties.etag;
    let _last_modified = &props.blob.properties.last_modified;
    assert_eq!(props.blob.properties.content_length, content.len() as u64);
    assert_eq!(props.blob.properties.blob_type, BlobType::BlockBlob);
}

#[tokio::test]
async fn test_sdk_blob_not_exists() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-exists-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("nonexistent-blob.txt");

    // Should not exist
    let result = blob_client.get_properties().await;
    assert!(result.is_err());
}

// ============================================================================
// Blob delete tests
// ============================================================================

#[tokio::test]
async fn test_sdk_delete_blob() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-delete-blob-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("delete-me.txt");

    // Upload
    blob_client.put_block_blob("delete me").await.unwrap();

    // Delete
    blob_client.delete().await.unwrap();

    // Verify deleted
    let result = blob_client.get_properties().await;
    assert!(result.is_err());
}

// ============================================================================
// List blobs tests
// ============================================================================

#[tokio::test]
async fn test_sdk_list_blobs() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-list-blobs-container");
    container_client.create().await.unwrap();

    // Upload several blobs
    for i in 0..5 {
        let blob_client = container_client.blob_client(format!("blob-{}.txt", i));
        blob_client
            .put_block_blob(format!("content {}", i))
            .await
            .unwrap();
    }

    // List blobs
    let mut stream = container_client.list_blobs().into_stream();
    let mut names = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result.unwrap();
        for blob in page.blobs.blobs() {
            names.push(blob.name.clone());
        }
    }

    assert_eq!(names.len(), 5);
    for i in 0..5 {
        assert!(names.contains(&format!("blob-{}.txt", i)));
    }
}

#[tokio::test]
async fn test_sdk_list_blobs_with_prefix() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-prefix-blobs-container");
    container_client.create().await.unwrap();

    // Upload blobs with different prefixes
    for dir in &["photos", "documents"] {
        for i in 0..3 {
            let blob_client = container_client.blob_client(format!("{}/file-{}.txt", dir, i));
            blob_client.put_block_blob("content").await.unwrap();
        }
    }

    // List with prefix
    let mut stream = container_client.list_blobs().prefix("photos/").into_stream();
    let mut names = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result.unwrap();
        for blob in page.blobs.blobs() {
            names.push(blob.name.clone());
        }
    }

    assert_eq!(names.len(), 3);
    assert!(names.iter().all(|n| n.starts_with("photos/")));
}

#[tokio::test]
async fn test_sdk_list_blobs_hierarchical() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-hierarchy-container");
    container_client.create().await.unwrap();

    // Create a directory-like structure
    let paths = [
        "root.txt",
        "dir1/file1.txt",
        "dir1/file2.txt",
        "dir1/subdir/file3.txt",
        "dir2/file4.txt",
    ];

    for path in &paths {
        let blob_client = container_client.blob_client(*path);
        blob_client.put_block_blob("content").await.unwrap();
    }

    // List with delimiter to get "directory" view
    let mut stream = container_client.list_blobs().delimiter("/").into_stream();

    let mut blobs = Vec::new();
    let mut prefixes = Vec::new();

    while let Some(result) = stream.next().await {
        let page = result.unwrap();
        for item in &page.blobs.items {
            match item {
                azure_storage_blobs::container::operations::BlobItem::Blob(blob) => {
                    blobs.push(blob.name.clone());
                }
                azure_storage_blobs::container::operations::BlobItem::BlobPrefix(prefix) => {
                    prefixes.push(prefix.name.clone());
                }
            }
        }
    }

    assert!(blobs.contains(&"root.txt".to_string()));
    assert!(prefixes.contains(&"dir1/".to_string()));
    assert!(prefixes.contains(&"dir2/".to_string()));
}

// ============================================================================
// Range download tests
// ============================================================================

#[tokio::test]
async fn test_sdk_download_range() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-range-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("range-blob.txt");
    let content = "0123456789ABCDEFGHIJ";

    blob_client.put_block_blob(content).await.unwrap();

    // Download a range (use u64 for range)
    let mut stream = blob_client.get().range(5u64..15u64).into_stream();
    let mut data = Vec::new();

    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        let bytes = chunk.data.collect().await.unwrap();
        data.extend_from_slice(&bytes);
    }

    assert_eq!(String::from_utf8(data).unwrap(), "56789ABCDE");
}

// ============================================================================
// Copy blob tests
// ============================================================================

#[tokio::test]
async fn test_sdk_copy_blob() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-copy-container");
    container_client.create().await.unwrap();

    let source_blob = container_client.blob_client("source.txt");
    let dest_blob = container_client.blob_client("destination.txt");

    let content = "Content to be copied";
    source_blob.put_block_blob(content).await.unwrap();

    // Get source URL
    let source_url = format!(
        "{}/{}/sdk-copy-container/source.txt",
        server.base_url, server.account
    );

    // Copy blob
    let url = url::Url::parse(&source_url).unwrap();
    dest_blob.copy(url).await.unwrap();

    // Verify copy - download and compare
    let mut stream = dest_blob.get().into_stream();
    let mut data = Vec::new();
    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        let bytes = chunk.data.collect().await.unwrap();
        data.extend_from_slice(&bytes);
    }

    assert_eq!(String::from_utf8(data).unwrap(), content);
}

// ============================================================================
// Error handling tests
// ============================================================================

#[tokio::test]
async fn test_sdk_container_not_found() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("nonexistent-container");

    let result = container_client.get_properties().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_sdk_blob_not_found() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-error-container");
    container_client.create().await.unwrap();

    let blob_client = container_client.blob_client("nonexistent-blob.txt");

    let result = blob_client.get_properties().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_sdk_container_already_exists() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-duplicate-container");

    container_client.create().await.unwrap();
    let result = container_client.create().await;

    assert!(result.is_err());
}

// ============================================================================
// Concurrent operations tests
// ============================================================================

#[tokio::test]
async fn test_sdk_concurrent_uploads() {
    let server = TestServer::start().await;
    let builder = create_builder(&server);
    let container_client = builder.container_client("sdk-concurrent-container");
    container_client.create().await.unwrap();

    // Upload 10 blobs concurrently
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let blob_client = container_client.blob_client(format!("concurrent-{}.txt", i));
            let content = format!("Content {}", i);
            tokio::spawn(async move { blob_client.put_block_blob(content).await })
        })
        .collect();

    // Wait for all uploads
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    // Verify all blobs exist
    let mut stream = container_client.list_blobs().into_stream();
    let mut count = 0;

    while let Some(result) = stream.next().await {
        let page = result.unwrap();
        count += page.blobs.blobs().count();
    }

    assert_eq!(count, 10);
}

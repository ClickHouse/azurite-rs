//! Blob operation tests.

mod common;

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
async fn test_upload_and_download_blob() {
    let server = TestServer::start().await;
    create_container(&server, "blobcontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("blobcontainer", "testblob.txt");
    let content = "Hello, Azure Blob Storage!";

    // Upload
    let response = client
        .put(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .header("Content-Type", "text/plain")
        .body(content)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);

    // Download
    let response = client
        .get(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, content);
}

#[tokio::test]
async fn test_blob_properties() {
    let server = TestServer::start().await;
    create_container(&server, "propscontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("propscontainer", "propsblob.txt");
    let content = "Test content";

    // Upload
    client
        .put(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .header("Content-Type", "text/plain")
        .body(content)
        .send()
        .await
        .unwrap();

    // Get properties (HEAD)
    let response = client
        .head(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("etag"));
    assert!(response.headers().contains_key("last-modified"));
    assert!(response.headers().contains_key("content-length"));
    assert_eq!(
        response.headers().get("x-ms-blob-type").map(|v| v.to_str().unwrap()),
        Some("BlockBlob")
    );
}

#[tokio::test]
async fn test_delete_blob() {
    let server = TestServer::start().await;
    create_container(&server, "deletecontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("deletecontainer", "deleteblob.txt");

    // Upload
    client
        .put(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .body("content")
        .send()
        .await
        .unwrap();

    // Delete
    let response = client
        .delete(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202);

    // Verify deleted
    let response = client
        .get(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn test_range_download() {
    let server = TestServer::start().await;
    create_container(&server, "rangecontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("rangecontainer", "rangeblob.txt");
    let content = "0123456789ABCDEF";

    // Upload
    client
        .put(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .body(content)
        .send()
        .await
        .unwrap();

    // Download range
    let response = client
        .get(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("Range", "bytes=5-10")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 206);
    let body = response.text().await.unwrap();
    assert_eq!(body, "56789A");
}

#[tokio::test]
async fn test_list_blobs() {
    let server = TestServer::start().await;
    create_container(&server, "listcontainer").await;

    let client = reqwest::Client::new();

    // Upload some blobs
    for i in 0..3 {
        let blob_url = server.blob_url("listcontainer", &format!("blob{}.txt", i));
        client
            .put(&blob_url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .header("x-ms-blob-type", "BlockBlob")
            .body(format!("content {}", i))
            .send()
            .await
            .unwrap();
    }

    // List blobs
    let list_url = format!("{}?restype=container&comp=list", server.container_url("listcontainer"));
    let response = client
        .get(&list_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("blob0.txt"));
    assert!(body.contains("blob1.txt"));
    assert!(body.contains("blob2.txt"));
}

#[tokio::test]
async fn test_list_blobs_with_prefix() {
    let server = TestServer::start().await;
    create_container(&server, "prefixcontainer").await;

    let client = reqwest::Client::new();

    // Upload blobs with different prefixes
    for prefix in &["dir1/", "dir2/"] {
        for i in 0..2 {
            let blob_url = server.blob_url("prefixcontainer", &format!("{}file{}.txt", prefix, i));
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

    // List blobs with prefix
    let list_url = format!(
        "{}?restype=container&comp=list&prefix=dir1/",
        server.container_url("prefixcontainer")
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
    assert!(body.contains("dir1/file0.txt"));
    assert!(body.contains("dir1/file1.txt"));
    assert!(!body.contains("dir2/"));
}

#[tokio::test]
async fn test_blob_metadata() {
    let server = TestServer::start().await;
    create_container(&server, "metacontainer").await;

    let client = reqwest::Client::new();
    let blob_url = server.blob_url("metacontainer", "metablob.txt");

    // Upload with metadata
    client
        .put(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .header("x-ms-meta-author", "test")
        .header("x-ms-meta-version", "1.0")
        .body("content")
        .send()
        .await
        .unwrap();

    // Get properties
    let response = client
        .head(&blob_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers().get("x-ms-meta-author").map(|v| v.to_str().unwrap()),
        Some("test")
    );
    assert_eq!(
        response.headers().get("x-ms-meta-version").map(|v| v.to_str().unwrap()),
        Some("1.0")
    );
}

#[tokio::test]
async fn test_copy_blob() {
    let server = TestServer::start().await;
    create_container(&server, "copycontainer").await;

    let client = reqwest::Client::new();
    let source_url = server.blob_url("copycontainer", "source.txt");
    let dest_url = server.blob_url("copycontainer", "dest.txt");
    let content = "Copy this content";

    // Upload source
    client
        .put(&source_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-blob-type", "BlockBlob")
        .body(content)
        .send()
        .await
        .unwrap();

    // Copy
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
    assert_eq!(
        response.headers().get("x-ms-copy-status").map(|v| v.to_str().unwrap()),
        Some("success")
    );

    // Download destination
    let response = client
        .get(&dest_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, content);
}

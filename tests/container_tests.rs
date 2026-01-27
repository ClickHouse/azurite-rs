//! Container operation tests.

mod common;

use common::TestServer;

#[tokio::test]
async fn test_create_container() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let url = format!("{}?restype=container", server.container_url("testcontainer"));

    let response = client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    assert!(response.headers().contains_key("etag"));
    assert!(response.headers().contains_key("last-modified"));
}

#[tokio::test]
async fn test_create_duplicate_container() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let url = format!("{}?restype=container", server.container_url("dupcontainer"));

    // Create first time
    let response = client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);

    // Create second time - should fail
    let response = client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 409);
}

#[tokio::test]
async fn test_delete_container() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let url = format!("{}?restype=container", server.container_url("deletecontainer"));

    // Create
    client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    // Delete
    let response = client
        .delete(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 202);
}

#[tokio::test]
async fn test_get_container_properties() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let url = format!("{}?restype=container", server.container_url("propscontainer"));

    // Create
    client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    // Get properties
    let response = client
        .get(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("etag"));
    assert!(response.headers().contains_key("last-modified"));
    assert!(response.headers().contains_key("x-ms-lease-status"));
    assert!(response.headers().contains_key("x-ms-lease-state"));
}

#[tokio::test]
async fn test_list_containers() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();

    // Create some containers
    for i in 0..3 {
        let url = format!("{}?restype=container", server.container_url(&format!("listcontainer{}", i)));
        client
            .put(&url)
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
            .send()
            .await
            .unwrap();
    }

    // List containers
    let url = format!("{}/{}?comp=list", server.base_url, server.account);
    let response = client
        .get(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert!(body.contains("listcontainer0"));
    assert!(body.contains("listcontainer1"));
    assert!(body.contains("listcontainer2"));
}

#[tokio::test]
async fn test_container_metadata() {
    let server = TestServer::start().await;

    let client = reqwest::Client::new();
    let container_url = format!("{}?restype=container", server.container_url("metacontainer"));

    // Create with metadata
    client
        .put(&container_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .header("x-ms-meta-key1", "value1")
        .header("x-ms-meta-key2", "value2")
        .send()
        .await
        .unwrap();

    // Get properties and check metadata
    let response = client
        .get(&container_url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.headers().get("x-ms-meta-key1").map(|v| v.to_str().unwrap()), Some("value1"));
    assert_eq!(response.headers().get("x-ms-meta-key2").map(|v| v.to_str().unwrap()), Some("value2"));
}

//! Tests verifying that the extent store frees data on overwrite, delete,
//! and discarded staged blocks. Without these reclamation paths the in-memory
//! emulator grows without bound under workloads (such as the ClickHouse
//! merge-tree) that constantly rewrite or drop blobs.

mod common;

use common::TestServer;
use reqwest::Client;

const PAYLOAD_4MB: usize = 4 * 1024 * 1024;

fn date() -> String {
    chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string()
}

async fn put_block_blob(server: &TestServer, container: &str, blob: &str, data: Vec<u8>) {
    let client = Client::new();
    let url = server.blob_url(container, blob);
    let resp = client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", date())
        .header("x-ms-blob-type", "BlockBlob")
        .header("Content-Type", "application/octet-stream")
        .body(data)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "PUT failed: {}", resp.status());
}

async fn delete_blob(server: &TestServer, container: &str, blob: &str) {
    let client = Client::new();
    let url = server.blob_url(container, blob);
    let resp = client
        .delete(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", date())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "DELETE blob failed: {}",
        resp.status()
    );
}

async fn create_container(server: &TestServer, container: &str) {
    let client = Client::new();
    let url = format!("{}?restype=container", server.container_url(container));
    let resp = client
        .put(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", date())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create container failed: {}",
        resp.status()
    );
}

async fn delete_container(server: &TestServer, container: &str) {
    let client = Client::new();
    let url = format!("{}?restype=container", server.container_url(container));
    let resp = client
        .delete(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", date())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "delete container failed: {}",
        resp.status()
    );
}

/// Total bytes reported by the container listing — sum of every
/// `<Content-Length>` element. Lets HTTP-level tests indirectly check that
/// blob data isn't doubling under repeated overwrites.
async fn list_total_bytes(server: &TestServer, container: &str) -> u64 {
    let client = Client::new();
    let url = format!(
        "{}?restype=container&comp=list",
        server.container_url(container)
    );
    let resp = client
        .get(&url)
        .header("x-ms-version", "2021-10-04")
        .header("x-ms-date", date())
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    let mut total = 0u64;
    for chunk in body.split("<Content-Length>").skip(1) {
        if let Some(end) = chunk.find("</Content-Length>") {
            if let Ok(n) = chunk[..end].parse::<u64>() {
                total += n;
            }
        }
    }
    total
}

/// Direct extent-store assertions — the HTTP layer cannot observe the extent
/// store's internal byte count, so these tests pin reclamation behaviour at
/// the lowest level.
mod direct {
    use azurite_rs::storage::{ExtentStore, MemoryExtentStore, MemoryMetadataStore};
    use azurite_rs::{BlobServerBuilder, Config};
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn write_then_delete_frees_data() {
        let store = MemoryExtentStore::new();
        let chunk = store.write(Bytes::from(vec![0u8; 1024])).await.unwrap();
        assert_eq!(store.total_size().await, 1024);
        store.delete(&chunk.id).await.unwrap();
        assert_eq!(store.total_size().await, 0);
    }

    #[tokio::test]
    async fn add_ref_then_two_deletes_frees_data() {
        let store = MemoryExtentStore::new();
        let chunk = store.write(Bytes::from(vec![0u8; 2048])).await.unwrap();
        store.add_ref(&chunk.id).await.unwrap();
        // refcount is now 2
        store.delete(&chunk.id).await.unwrap();
        assert_eq!(
            store.total_size().await,
            2048,
            "first delete should not free shared data"
        );
        store.delete(&chunk.id).await.unwrap();
        assert_eq!(
            store.total_size().await,
            0,
            "second delete should free data when refcount reaches 0"
        );
    }

    #[tokio::test]
    async fn delete_unknown_extent_is_idempotent() {
        let store = MemoryExtentStore::new();
        store.delete("nonexistent-id").await.unwrap();
        assert_eq!(store.total_size().await, 0);
    }

    #[tokio::test]
    async fn add_ref_unknown_extent_errors() {
        let store = MemoryExtentStore::new();
        let result = store.add_ref("nonexistent-id").await;
        assert!(result.is_err());
    }

    /// End-to-end overwrite test that *also* observes the extent store: the
    /// HTTP-level `overwrite_blob_does_not_grow_listed_size` test cannot see
    /// the extent store, so it cannot distinguish a true reclamation from a
    /// listing artefact. Here we hand a shared `Arc<MemoryExtentStore>` to
    /// the server and read `total_size` directly after the run.
    #[tokio::test]
    async fn server_overwrites_release_extent_bytes() {
        use azurite_rs::storage::MetadataStore;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        let extents = Arc::new(MemoryExtentStore::new());
        let extents_dyn: Arc<dyn ExtentStore> = extents.clone();

        let config = Config {
            host: "127.0.0.1".to_string(),
            blob_port: port,
            ..Config::default()
        };

        let server = BlobServerBuilder::new()
            .config(config)
            .metadata(metadata)
            .extents(extents_dyn)
            .build();

        tokio::spawn(async move {
            server.run().await.unwrap();
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        let base = format!("http://127.0.0.1:{}/devstoreaccount1", port);
        let client = reqwest::Client::new();

        // Create container.
        let resp = client
            .put(format!("{}/mem?restype=container", base))
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", super::date())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        let payload_size = 256 * 1024;

        // Five overwrites of the same blob.
        for _ in 0..5 {
            let resp = client
                .put(format!("{}/mem/rotating", base))
                .header("x-ms-version", "2021-10-04")
                .header("x-ms-date", super::date())
                .header("x-ms-blob-type", "BlockBlob")
                .body(vec![0u8; payload_size])
                .send()
                .await
                .unwrap();
            assert!(resp.status().is_success());
        }

        assert_eq!(
            extents.total_size().await,
            payload_size as u64,
            "extent store should hold exactly one payload after 5 overwrites"
        );

        // Delete blob → extent store should be empty.
        let resp = client
            .delete(format!("{}/mem/rotating", base))
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", super::date())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        assert_eq!(
            extents.total_size().await,
            0,
            "extent store should be empty after delete"
        );
    }

    /// Block-blob staging path: stage 3 blocks, then commit only block-1, then
    /// verify the other two stages were freed (not just untracked).
    #[tokio::test]
    async fn server_uncommitted_staged_blocks_are_freed() {
        use azurite_rs::storage::MetadataStore;
        use base64::{engine::general_purpose::STANDARD as BASE64, Engine};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        let extents = Arc::new(MemoryExtentStore::new());
        let extents_dyn: Arc<dyn ExtentStore> = extents.clone();

        let config = Config {
            host: "127.0.0.1".to_string(),
            blob_port: port,
            ..Config::default()
        };
        let server = BlobServerBuilder::new()
            .config(config)
            .metadata(metadata)
            .extents(extents_dyn)
            .build();
        tokio::spawn(async move {
            server.run().await.unwrap();
        });
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        let base = format!("http://127.0.0.1:{}/devstoreaccount1", port);
        let client = reqwest::Client::new();

        // Create container.
        let resp = client
            .put(format!("{}/blocks?restype=container", base))
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", super::date())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success());

        let stage = |client: reqwest::Client, base: String, id: &'static str, size: usize| async move {
            let block_id = BASE64.encode(id.as_bytes());
            let resp = client
                .put(format!(
                    "{}/blocks/streaming?comp=block&blockid={}",
                    base, block_id
                ))
                .header("x-ms-version", "2021-10-04")
                .header("x-ms-date", super::date())
                .body(vec![0u8; size])
                .send()
                .await
                .unwrap();
            assert!(resp.status().is_success(), "stage {} failed", id);
            block_id
        };

        let id_a = stage(client.clone(), base.clone(), "blk-a", 4096).await;
        let _id_b = stage(client.clone(), base.clone(), "blk-b", 8192).await;
        let _id_c = stage(client.clone(), base.clone(), "blk-c", 16384).await;

        // 28KB total staged.
        assert_eq!(extents.total_size().await, (4096 + 8192 + 16384) as u64);

        // Commit only block-a.
        let body = format!(
            r#"<?xml version="1.0" encoding="utf-8"?><BlockList><Latest>{}</Latest></BlockList>"#,
            id_a
        );
        let resp = client
            .put(format!("{}/blocks/streaming?comp=blocklist", base))
            .header("x-ms-version", "2021-10-04")
            .header("x-ms-date", super::date())
            .header("Content-Type", "application/xml")
            .body(body)
            .send()
            .await
            .unwrap();
        assert!(
            resp.status().is_success(),
            "commit failed: {}",
            resp.status()
        );

        // Only block-a's extent (4096B) should remain.
        assert_eq!(
            extents.total_size().await,
            4096,
            "uncommitted staged blocks must be freed"
        );
    }
}

#[tokio::test]
async fn overwrite_blob_does_not_grow_listed_size() {
    let server = TestServer::start().await;
    create_container(&server, "ctest1").await;

    for _ in 0..5 {
        put_block_blob(&server, "ctest1", "rotating", vec![0u8; PAYLOAD_4MB]).await;
    }

    let total = list_total_bytes(&server, "ctest1").await;
    assert_eq!(
        total, PAYLOAD_4MB as u64,
        "after 5 overwrites the single blob must still report a single payload"
    );
}

#[tokio::test]
async fn delete_container_releases_blobs_for_relisting() {
    let server = TestServer::start().await;
    create_container(&server, "ctest2").await;

    put_block_blob(&server, "ctest2", "a", vec![1u8; 1024]).await;
    put_block_blob(&server, "ctest2", "b", vec![2u8; 1024]).await;

    delete_container(&server, "ctest2").await;
    create_container(&server, "ctest2").await;

    let total = list_total_bytes(&server, "ctest2").await;
    assert_eq!(
        total, 0,
        "freshly recreated container should have zero blobs"
    );
}

#[tokio::test]
async fn delete_blob_then_recreate_smaller_does_not_inflate() {
    let server = TestServer::start().await;
    create_container(&server, "ctest3").await;

    put_block_blob(&server, "ctest3", "rotating", vec![0u8; PAYLOAD_4MB]).await;
    delete_blob(&server, "ctest3", "rotating").await;
    put_block_blob(&server, "ctest3", "rotating", vec![0u8; 64]).await;

    let total = list_total_bytes(&server, "ctest3").await;
    assert_eq!(total, 64);
}

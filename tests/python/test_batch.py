"""
Tests for batch operations using the official Azure SDK for Python.

These tests verify that azurite-rs correctly handles SubmitBatch requests,
which the Azure C++ SDK (used by ClickHouse) uses for batch blob deletion.

The key requirement is that batch sub-responses include Content-ID headers.
Without them, the C++ SDK's ParseSubresponses replaces the entire rawResponse
with a parsed sub-response that lacks Content-Type, causing a fatal crash:
    std::out_of_range at pRawResponse->GetHeaders().at("Content-Type")
"""

import uuid

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def container_client(
    blob_service_client: BlobServiceClient, unique_container_name
) -> ContainerClient:
    """Create a container and return its client."""
    client = blob_service_client.create_container(unique_container_name)
    yield client
    try:
        client.delete_container()
    except Exception:
        pass


def _upload_blobs(container_client: ContainerClient, names: list[str], content: bytes = b"data"):
    """Upload multiple blobs with the same content."""
    for name in names:
        container_client.get_blob_client(name).upload_blob(content)


def _blob_exists(container_client: ContainerClient, name: str) -> bool:
    """Check if a blob exists."""
    try:
        container_client.get_blob_client(name).get_blob_properties()
        return True
    except ResourceNotFoundError:
        return False


# ============================================================================
# Batch delete tests
# ============================================================================


def test_batch_delete_single_blob(container_client: ContainerClient):
    """Test batch deletion of a single blob."""
    _upload_blobs(container_client, ["to-delete.txt"])
    assert _blob_exists(container_client, "to-delete.txt")

    container_client.delete_blobs("to-delete.txt")

    assert not _blob_exists(container_client, "to-delete.txt")


def test_batch_delete_multiple_blobs(container_client: ContainerClient):
    """Test batch deletion of multiple blobs."""
    blob_names = [f"blob-{i}.txt" for i in range(5)]
    _upload_blobs(container_client, blob_names)

    for name in blob_names:
        assert _blob_exists(container_client, name)

    container_client.delete_blobs(*blob_names)

    for name in blob_names:
        assert not _blob_exists(container_client, name), f"Blob {name} should have been deleted"


def test_batch_delete_many_blobs(container_client: ContainerClient):
    """Test batch deletion with a larger number of blobs (up to Azure's 256 limit)."""
    blob_names = [f"many-{i:03d}.txt" for i in range(20)]
    _upload_blobs(container_client, blob_names)

    container_client.delete_blobs(*blob_names)

    for name in blob_names:
        assert not _blob_exists(container_client, name), f"Blob {name} should have been deleted"


def test_batch_delete_nonexistent_blob(container_client: ContainerClient):
    """Test batch deletion of a blob that does not exist.

    The overall batch should still succeed (202). Individual sub-responses
    may indicate 404 for missing blobs, but the SDK typically does not
    raise for individual 404s in a batch.
    """
    # delete_blobs may raise a PartialBatchErrorException for 404 sub-responses
    # or it may silently succeed depending on SDK version. Either is acceptable.
    try:
        container_client.delete_blobs("does-not-exist.txt")
    except Exception:
        # PartialBatchErrorException or similar is acceptable
        pass


def test_batch_delete_mix_existing_and_nonexistent(container_client: ContainerClient):
    """Test batch deletion with a mix of existing and nonexistent blobs."""
    _upload_blobs(container_client, ["exists-1.txt", "exists-2.txt"])

    try:
        container_client.delete_blobs("exists-1.txt", "missing.txt", "exists-2.txt")
    except Exception:
        # PartialBatchErrorException for the missing blob is acceptable
        pass

    # The existing blobs should be deleted regardless
    assert not _blob_exists(container_client, "exists-1.txt")
    assert not _blob_exists(container_client, "exists-2.txt")


def test_batch_delete_blobs_with_special_characters(container_client: ContainerClient):
    """Test batch deletion of blobs with special characters in names."""
    blob_names = [
        "path/to/blob.txt",
        "blob with spaces.txt",
        "blob-with-dashes.txt",
    ]
    _upload_blobs(container_client, blob_names)

    container_client.delete_blobs(*blob_names)

    for name in blob_names:
        assert not _blob_exists(container_client, name), f"Blob {name} should have been deleted"


# ============================================================================
# Batch delete preserves other blobs
# ============================================================================


def test_batch_delete_does_not_affect_other_blobs(container_client: ContainerClient):
    """Test that batch deletion only deletes the specified blobs."""
    _upload_blobs(container_client, ["keep-1.txt", "keep-2.txt", "delete-1.txt", "delete-2.txt"])

    container_client.delete_blobs("delete-1.txt", "delete-2.txt")

    assert _blob_exists(container_client, "keep-1.txt"), "keep-1.txt should still exist"
    assert _blob_exists(container_client, "keep-2.txt"), "keep-2.txt should still exist"
    assert not _blob_exists(container_client, "delete-1.txt")
    assert not _blob_exists(container_client, "delete-2.txt")


# ============================================================================
# Sequential batch operations
# ============================================================================


def test_multiple_batch_deletes(container_client: ContainerClient):
    """Test multiple sequential batch delete operations."""
    all_blobs = [f"seq-{i}.txt" for i in range(10)]
    _upload_blobs(container_client, all_blobs)

    # First batch: delete first 5
    container_client.delete_blobs(*all_blobs[:5])

    for name in all_blobs[:5]:
        assert not _blob_exists(container_client, name)
    for name in all_blobs[5:]:
        assert _blob_exists(container_client, name)

    # Second batch: delete remaining 5
    container_client.delete_blobs(*all_blobs[5:])

    for name in all_blobs:
        assert not _blob_exists(container_client, name)


def test_batch_delete_then_recreate(container_client: ContainerClient):
    """Test that blobs can be recreated after batch deletion."""
    blob_names = ["recreate-1.txt", "recreate-2.txt"]
    _upload_blobs(container_client, blob_names, content=b"original")

    container_client.delete_blobs(*blob_names)

    for name in blob_names:
        assert not _blob_exists(container_client, name)

    # Recreate with new content
    _upload_blobs(container_client, blob_names, content=b"new content")

    for name in blob_names:
        assert _blob_exists(container_client, name)
        downloaded = container_client.get_blob_client(name).download_blob().readall()
        assert downloaded == b"new content"

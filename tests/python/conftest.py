"""
Pytest fixtures for Azure SDK Python tests.
"""

import os
import signal
import socket
import subprocess
import time

import pytest


def find_free_port():
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_server(host, port, timeout=30):
    """Wait for server to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (ConnectionRefusedError, socket.timeout):
            time.sleep(0.1)
    return False


@pytest.fixture(scope="session")
def azurite_server():
    """Start azurite-rs server for testing."""
    # Find project root (where Cargo.toml is)
    test_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(test_dir))

    # Build the project first
    subprocess.run(
        ["cargo", "build", "--release"],
        cwd=project_root,
        check=True,
        capture_output=True,
    )

    port = find_free_port()
    binary_path = os.path.join(project_root, "target", "release", "azurite-rs")

    # Start the server
    process = subprocess.Popen(
        [binary_path, "--blob-port", str(port), "--in-memory"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to be ready
    if not wait_for_server("127.0.0.1", port):
        process.kill()
        stdout, stderr = process.communicate()
        raise RuntimeError(
            f"Server failed to start.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
        )

    server_info = {
        "host": "127.0.0.1",
        "port": port,
        "account": "devstoreaccount1",
        "key": "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
        "process": process,
    }

    yield server_info

    # Cleanup
    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()


@pytest.fixture
def connection_string(azurite_server):
    """Return connection string for Azure SDK."""
    return (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={azurite_server['account']};"
        f"AccountKey={azurite_server['key']};"
        f"BlobEndpoint=http://{azurite_server['host']}:{azurite_server['port']}/{azurite_server['account']};"
    )


@pytest.fixture
def blob_service_client(connection_string):
    """Create BlobServiceClient for testing."""
    from azure.storage.blob import BlobServiceClient

    return BlobServiceClient.from_connection_string(connection_string)


@pytest.fixture
def unique_container_name():
    """Generate a unique container name for testing."""
    import uuid

    return f"test-container-{uuid.uuid4().hex[:8]}"

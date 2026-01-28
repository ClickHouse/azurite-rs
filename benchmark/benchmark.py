#!/usr/bin/env python3
"""
Benchmark script to compare azurite-rs with Azurite (Node.js).

Tests single blob read/write operations with single and concurrent clients.
"""

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from azure.storage.blob import BlobServiceClient


@dataclass
class BenchmarkResult:
    """Results from a benchmark run."""
    operation: str
    server: str
    blob_size: int
    num_clients: int
    num_operations: int
    total_time: float
    ops_per_second: float
    mb_per_second: float


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
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.1)
    return False


class ServerManager:
    """Manages starting and stopping storage servers."""

    def __init__(self):
        self.process: Optional[subprocess.Popen] = None
        self.port: int = 0
        self.account = "devstoreaccount1"
        self.key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

    def start_azurite_rs(self, binary_path: str) -> str:
        """Start azurite-rs server."""
        self.port = find_free_port()
        self.process = subprocess.Popen(
            [binary_path, "--blob-port", str(self.port), "--in-memory", "--silent"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if not wait_for_server("127.0.0.1", self.port):
            self.stop()
            raise RuntimeError("azurite-rs failed to start")
        return self._connection_string()

    def start_azurite_node(self, azurite_path: str) -> str:
        """Start Azurite (Node.js) server."""
        self.port = find_free_port()
        self.process = subprocess.Popen(
            ["node", os.path.join(azurite_path, "dist/src/blob/main.js"),
             "--blobPort", str(self.port),
             "--inMemoryPersistence",
             "--silent",
             "--skipApiVersionCheck"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=azurite_path,
        )
        if not wait_for_server("127.0.0.1", self.port):
            self.stop()
            raise RuntimeError("Azurite (Node.js) failed to start")
        return self._connection_string()

    def _connection_string(self) -> str:
        return (
            f"DefaultEndpointsProtocol=http;"
            f"AccountName={self.account};"
            f"AccountKey={self.key};"
            f"BlobEndpoint=http://127.0.0.1:{self.port}/{self.account};"
        )

    def stop(self):
        """Stop the server."""
        if self.process:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None


def run_write_benchmark(
    client: BlobServiceClient,
    container_name: str,
    blob_size: int,
    num_operations: int,
    num_clients: int,
) -> float:
    """Run write benchmark and return total time."""
    container_client = client.get_container_client(container_name)
    try:
        container_client.create_container()
    except Exception:
        pass

    data = b"x" * blob_size

    def write_blob(i: int):
        blob_client = container_client.get_blob_client(f"blob-{i}")
        blob_client.upload_blob(data, overwrite=True)

    start = time.perf_counter()

    if num_clients == 1:
        for i in range(num_operations):
            write_blob(i)
    else:
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(write_blob, i) for i in range(num_operations)]
            for future in as_completed(futures):
                future.result()

    return time.perf_counter() - start


def run_read_benchmark(
    client: BlobServiceClient,
    container_name: str,
    blob_size: int,
    num_operations: int,
    num_clients: int,
) -> float:
    """Run read benchmark and return total time."""
    container_client = client.get_container_client(container_name)

    # Create a blob to read
    data = b"x" * blob_size
    blob_client = container_client.get_blob_client("read-blob")
    blob_client.upload_blob(data, overwrite=True)

    def read_blob(_: int):
        blob_client.download_blob().readall()

    start = time.perf_counter()

    if num_clients == 1:
        for i in range(num_operations):
            read_blob(i)
    else:
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(read_blob, i) for i in range(num_operations)]
            for future in as_completed(futures):
                future.result()

    return time.perf_counter() - start


def run_benchmarks(
    connection_string: str,
    server_name: str,
    blob_sizes: list,
    num_operations: int,
    client_counts: list,
) -> list:
    """Run all benchmarks for a server."""
    results = []
    client = BlobServiceClient.from_connection_string(connection_string)

    for blob_size in blob_sizes:
        for num_clients in client_counts:
            container_name = f"bench-{blob_size}-{num_clients}"

            # Write benchmark
            total_time = run_write_benchmark(
                client, container_name, blob_size, num_operations, num_clients
            )
            ops_per_sec = num_operations / total_time
            mb_per_sec = (num_operations * blob_size) / total_time / (1024 * 1024)
            results.append(BenchmarkResult(
                operation="write",
                server=server_name,
                blob_size=blob_size,
                num_clients=num_clients,
                num_operations=num_operations,
                total_time=total_time,
                ops_per_second=ops_per_sec,
                mb_per_second=mb_per_sec,
            ))

            # Read benchmark
            total_time = run_read_benchmark(
                client, container_name, blob_size, num_operations, num_clients
            )
            ops_per_sec = num_operations / total_time
            mb_per_sec = (num_operations * blob_size) / total_time / (1024 * 1024)
            results.append(BenchmarkResult(
                operation="read",
                server=server_name,
                blob_size=blob_size,
                num_clients=num_clients,
                num_operations=num_operations,
                total_time=total_time,
                ops_per_second=ops_per_sec,
                mb_per_second=mb_per_sec,
            ))

    return results


def format_size(size: int) -> str:
    """Format byte size for display."""
    if size >= 1024 * 1024:
        return f"{size // (1024 * 1024)}MB"
    elif size >= 1024:
        return f"{size // 1024}KB"
    else:
        return f"{size}B"


def print_results(results: list):
    """Print benchmark results in a table."""
    print("\n" + "=" * 100)
    print("BENCHMARK RESULTS")
    print("=" * 100)

    # Group by operation and blob size
    operations = ["write", "read"]
    blob_sizes = sorted(set(r.blob_size for r in results))
    client_counts = sorted(set(r.num_clients for r in results))
    servers = sorted(set(r.server for r in results))

    for operation in operations:
        print(f"\n{operation.upper()} Operations")
        print("-" * 100)

        # Header
        header = f"{'Blob Size':<12} {'Clients':<10}"
        for server in servers:
            header += f" {server + ' ops/s':<18} {server + ' MB/s':<15}"
        if len(servers) == 2:
            header += f" {'Speedup':<10}"
        print(header)
        print("-" * 100)

        for blob_size in blob_sizes:
            for num_clients in client_counts:
                row = f"{format_size(blob_size):<12} {num_clients:<10}"

                server_results = {}
                for server in servers:
                    for r in results:
                        if (r.operation == operation and r.blob_size == blob_size
                                and r.num_clients == num_clients and r.server == server):
                            server_results[server] = r
                            row += f" {r.ops_per_second:>14.1f}     {r.mb_per_second:>11.2f}    "

                # Calculate speedup if we have both servers
                if len(server_results) == 2 and "azurite-rs" in server_results and "azurite" in server_results:
                    speedup = server_results["azurite-rs"].ops_per_second / server_results["azurite"].ops_per_second
                    row += f" {speedup:>6.2f}x"

                print(row)

    print("\n" + "=" * 100)


def main():
    parser = argparse.ArgumentParser(description="Benchmark azurite-rs vs Azurite")
    parser.add_argument("--azurite-rs", default="./target/release/azurite-rs",
                        help="Path to azurite-rs binary")
    parser.add_argument("--azurite", default="../Azurite",
                        help="Path to Azurite directory")
    parser.add_argument("--operations", type=int, default=1000,
                        help="Number of operations per benchmark")
    parser.add_argument("--blob-sizes", default="1024,10240,102400,1048576",
                        help="Comma-separated blob sizes in bytes")
    parser.add_argument("--clients", default="1,4,16",
                        help="Comma-separated client counts")
    parser.add_argument("--only", choices=["azurite-rs", "azurite"],
                        help="Only benchmark one server")
    args = parser.parse_args()

    blob_sizes = [int(s) for s in args.blob_sizes.split(",")]
    client_counts = [int(c) for c in args.clients.split(",")]

    all_results = []
    server_manager = ServerManager()

    # Build azurite-rs first
    print("Building azurite-rs...")
    subprocess.run(["cargo", "build", "--release"], check=True, capture_output=True)

    try:
        if args.only != "azurite":
            print(f"\nStarting azurite-rs benchmark...")
            conn_str = server_manager.start_azurite_rs(args.azurite_rs)
            results = run_benchmarks(conn_str, "azurite-rs", blob_sizes,
                                     args.operations, client_counts)
            all_results.extend(results)
            server_manager.stop()

        if args.only != "azurite-rs":
            print(f"\nStarting Azurite (Node.js) benchmark...")
            conn_str = server_manager.start_azurite_node(args.azurite)
            results = run_benchmarks(conn_str, "azurite", blob_sizes,
                                     args.operations, client_counts)
            all_results.extend(results)
            server_manager.stop()

        print_results(all_results)

    finally:
        server_manager.stop()


if __name__ == "__main__":
    main()

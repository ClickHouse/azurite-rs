//! Azurite-rs: Azure Blob Storage emulator in Rust.
//!
//! This crate provides an Azure Blob Storage emulator for local development
//! and testing. It implements the full Azure Blob Storage REST API.
//!
//! # Example
//!
//! ```no_run
//! use azurite_rs::{BlobServer, Config};
//!
//! #[tokio::main]
//! async fn main() {
//!     let server = BlobServer::new(Config::default());
//!     server.run().await.unwrap();
//! }
//! ```

pub mod auth;
pub mod config;
pub mod context;
pub mod error;
pub mod handlers;
pub mod models;
pub mod router;
pub mod server;
pub mod storage;
pub mod xml;

// Re-exports for convenience
pub use config::{Args, Config, DEFAULT_ACCOUNT, DEFAULT_ACCOUNT_KEY, DEFAULT_BLOB_PORT};
pub use error::{ErrorCode, StorageError, StorageResult};
pub use server::{BlobServer, BlobServerBuilder};
pub use storage::{ExtentStore, MemoryExtentStore, MemoryMetadataStore, MetadataStore};

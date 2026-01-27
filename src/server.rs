//! HTTP server for Azure Blob Storage emulator.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, Level};

use crate::config::Config;
use crate::router::{create_router, AppState};
use crate::storage::{ExtentStore, MemoryExtentStore, MemoryMetadataStore, MetadataStore};

/// Blob storage server.
pub struct BlobServer {
    config: Arc<Config>,
    metadata: Arc<dyn MetadataStore>,
    extents: Arc<dyn ExtentStore>,
}

impl BlobServer {
    /// Creates a new blob server with in-memory storage.
    pub fn new(config: Config) -> Self {
        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        let extents: Arc<dyn ExtentStore> = Arc::new(MemoryExtentStore::new());

        Self {
            config: Arc::new(config),
            metadata,
            extents,
        }
    }

    /// Creates a new blob server with custom storage.
    pub fn with_storage(
        config: Config,
        metadata: Arc<dyn MetadataStore>,
        extents: Arc<dyn ExtentStore>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            metadata,
            extents,
        }
    }

    /// Runs the server.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = self.config.blob_bind_address().parse()?;

        let state = AppState {
            config: self.config.clone(),
            metadata: self.metadata.clone(),
            extents: self.extents.clone(),
        };

        // Create router with middleware
        let app = create_router(state)
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any)
                    .expose_headers(Any),
            )
            .layer(TraceLayer::new_for_http());

        info!("Azurite Blob service is starting at http://{}", addr);
        info!(
            "Default account: {}, key: {}...",
            self.config.accounts.first().map(|a| a.name.as_str()).unwrap_or("unknown"),
            self.config.accounts.first()
                .map(|a| &a.key[..20])
                .unwrap_or("unknown")
        );

        let listener = TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Returns the bind address.
    pub fn bind_address(&self) -> String {
        self.config.blob_bind_address()
    }

    /// Returns the base URL for the blob service.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.bind_address())
    }
}

/// Builder for creating a blob server.
pub struct BlobServerBuilder {
    config: Config,
    metadata: Option<Arc<dyn MetadataStore>>,
    extents: Option<Arc<dyn ExtentStore>>,
}

impl BlobServerBuilder {
    /// Creates a new builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: Config::default(),
            metadata: None,
            extents: None,
        }
    }

    /// Sets the configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Sets the host address.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.config.host = host.into();
        self
    }

    /// Sets the blob service port.
    pub fn port(mut self, port: u16) -> Self {
        self.config.blob_port = port;
        self
    }

    /// Enables loose mode.
    pub fn loose(mut self, loose: bool) -> Self {
        self.config.loose = loose;
        self
    }

    /// Sets the metadata store.
    pub fn metadata(mut self, metadata: Arc<dyn MetadataStore>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the extent store.
    pub fn extents(mut self, extents: Arc<dyn ExtentStore>) -> Self {
        self.extents = Some(extents);
        self
    }

    /// Builds the server.
    pub fn build(self) -> BlobServer {
        let metadata = self
            .metadata
            .unwrap_or_else(|| Arc::new(MemoryMetadataStore::new()));
        let extents = self
            .extents
            .unwrap_or_else(|| Arc::new(MemoryExtentStore::new()));

        BlobServer::with_storage(self.config, metadata, extents)
    }
}

impl Default for BlobServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

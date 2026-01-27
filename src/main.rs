//! Azurite-rs: Azure Blob Storage emulator in Rust.
//!
//! A drop-in replacement for Azurite, implementing the Azure Blob Storage REST API.

use clap::Parser;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use azurite_rs::{Args, BlobServer, Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse command-line arguments
    let args = Args::parse();

    // Set up logging
    let log_level = if args.debug {
        Level::DEBUG
    } else if args.silent {
        Level::ERROR
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .compact()
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    // Create configuration from arguments
    let config = Config::from(args);

    // Create and run the server
    let server = BlobServer::new(config);

    println!(
        r#"
Azurite Blob service is starting at {}

Default account: devstoreaccount1
Default key: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

Connection string:
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint={}/devstoreaccount1;

Press Ctrl+C to stop the server.
"#,
        server.bind_address(),
        server.base_url()
    );

    server.run().await
}

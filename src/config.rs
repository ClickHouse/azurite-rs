//! Server configuration.

use clap::Parser;
use std::path::PathBuf;

/// Default account name for development storage.
pub const DEFAULT_ACCOUNT: &str = "devstoreaccount1";

/// Default account key for development storage (base64 encoded).
pub const DEFAULT_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Default blob service port.
pub const DEFAULT_BLOB_PORT: u16 = 10000;

/// Default API version.
pub const DEFAULT_API_VERSION: &str = "2021-10-04";

/// Command-line arguments for the server.
#[derive(Parser, Debug, Clone)]
#[command(name = "azurite-rs")]
#[command(about = "Azure Blob Storage emulator in Rust")]
#[command(version)]
pub struct Args {
    /// Host address to bind to.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Port for blob service.
    #[arg(long, default_value_t = DEFAULT_BLOB_PORT)]
    pub blob_port: u16,

    /// Location for workspace data.
    #[arg(long, short = 'l')]
    pub location: Option<PathBuf>,

    /// Enable loose mode (skip strict validation).
    #[arg(long)]
    pub loose: bool,

    /// Skip API version check.
    #[arg(long)]
    pub skip_api_version_check: bool,

    /// Disable production style URL (use path-style only).
    #[arg(long)]
    pub disable_production_style_url: bool,

    /// Enable debug logging.
    #[arg(long, short = 'd')]
    pub debug: bool,

    /// Enable silent mode (minimal logging).
    #[arg(long, short = 's')]
    pub silent: bool,

    /// In-memory mode (no persistence).
    #[arg(long)]
    pub in_memory: bool,

    /// OAuth authentication mode.
    #[arg(long)]
    pub oauth: Option<String>,

    /// Certificate file for HTTPS.
    #[arg(long)]
    pub cert: Option<PathBuf>,

    /// Key file for HTTPS.
    #[arg(long)]
    pub key: Option<PathBuf>,

    /// Enable PWD-based certificates.
    #[arg(long)]
    pub pwd: Option<String>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            blob_port: DEFAULT_BLOB_PORT,
            location: None,
            loose: false,
            skip_api_version_check: false,
            disable_production_style_url: false,
            debug: false,
            silent: false,
            in_memory: true,
            oauth: None,
            cert: None,
            key: None,
            pwd: None,
        }
    }
}

/// Server configuration derived from command-line arguments.
#[derive(Debug, Clone)]
pub struct Config {
    /// Host address to bind to.
    pub host: String,
    /// Port for blob service.
    pub blob_port: u16,
    /// Location for workspace data.
    pub location: Option<PathBuf>,
    /// Enable loose mode (skip strict validation).
    pub loose: bool,
    /// Skip API version check.
    pub skip_api_version_check: bool,
    /// In-memory mode (no persistence).
    pub in_memory: bool,
    /// Enable debug logging.
    pub debug: bool,
    /// Default account credentials.
    pub accounts: Vec<AccountConfig>,
}

/// Account configuration.
#[derive(Debug, Clone)]
pub struct AccountConfig {
    pub name: String,
    pub key: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            blob_port: DEFAULT_BLOB_PORT,
            location: None,
            loose: false,
            skip_api_version_check: false,
            in_memory: true,
            debug: false,
            accounts: vec![AccountConfig {
                name: DEFAULT_ACCOUNT.to_string(),
                key: DEFAULT_ACCOUNT_KEY.to_string(),
            }],
        }
    }
}

impl From<Args> for Config {
    fn from(args: Args) -> Self {
        let in_memory = args.in_memory || args.location.is_none();
        Self {
            host: args.host,
            blob_port: args.blob_port,
            location: args.location,
            loose: args.loose,
            skip_api_version_check: args.skip_api_version_check,
            in_memory,
            debug: args.debug,
            accounts: vec![AccountConfig {
                name: DEFAULT_ACCOUNT.to_string(),
                key: DEFAULT_ACCOUNT_KEY.to_string(),
            }],
        }
    }
}

impl Config {
    /// Returns the account key for the given account name.
    pub fn get_account_key(&self, account: &str) -> Option<&str> {
        self.accounts
            .iter()
            .find(|a| a.name == account)
            .map(|a| a.key.as_str())
    }

    /// Returns the bind address for the blob service.
    pub fn blob_bind_address(&self) -> String {
        format!("{}:{}", self.host, self.blob_port)
    }
}

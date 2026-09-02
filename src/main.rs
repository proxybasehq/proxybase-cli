use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod update;
mod bridge;
mod proxy_parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::time::{interval, Duration};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Default backend URL — dev vs release
// ---------------------------------------------------------------------------
const DEFAULT_BACKEND_URL: &str = if cfg!(debug_assertions) {
    "http://localhost:8080"
} else {
    "https://api.proxybase.xyz"
};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(name = "proxybase-cli")]
#[command(about = "ProxyBase Markets CLI — wallet, seller, and buyer operations")]
struct Cli {
    /// Backend API base URL
    #[arg(long, default_value = DEFAULT_BACKEND_URL, global = true)]
    backend: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Wallet management
    Wallet {
        #[command(subcommand)]
        cmd: WalletCmd,
    },
    /// Authenticate and save session token
    Login,
    /// Seller operations
    Seller {
        #[command(subcommand)]
        cmd: SellerCmd,
    },
    /// Buyer operations
    Buyer {
        #[command(subcommand)]
        cmd: BuyerCmd,
    },
    /// Market operations
    Market {
        #[command(subcommand)]
        cmd: MarketCmd,
    },
    /// Local SOCKS5 bridge for buyer sessions
    Bridge {
        #[command(subcommand)]
        cmd: BridgeCmd,
    },
    /// Backend health check
    Health,
    /// Print version
    Version,
    /// Check for or install the latest release
    Update {
        /// Only check for a new version, don't download
        #[arg(long)]
        check: bool,
    },
}

#[derive(Subcommand)]
enum WalletCmd {
    /// Generate a new wallet
    Create,
    /// Import an existing mnemonic. Without --hd-index uses the legacy
    /// raw-seed derivation; with --hd-index derives the BIP-44 child
    /// wallet at m/44'/60'/0'/0/{index} from the master phrase.
    Import {
        phrase: String,
        /// BIP-44 HD child derivation index
        #[arg(long)]
        hd_index: Option<u32>,
    },
    /// Show wallet info
    Info,
    /// Sweep available seller earnings from a range of HD child wallets
    /// (m/44'/60'/0'/0/{i} for i in start_index..start_index+count) into a
    /// single central Tempo address. Each child is authenticated in memory;
    /// the on-disk wallet and session token are left untouched.
    Sweep {
        /// Master mnemonic phrase
        phrase: String,
        /// First child index to sweep
        #[arg(long, default_value = "0")]
        start_index: u32,
        /// Number of children to sweep
        #[arg(long)]
        count: u32,
        /// Central Tempo wallet address receiving the payouts
        #[arg(long)]
        target_tempo: String,
        /// Only create a payout when a child's available earnings are at
        /// least this many microcredits (1,000,000 = $1.00)
        #[arg(long, default_value = "1000000")]
        min_threshold: i64,
    },
}

#[derive(Subcommand)]
enum SellerCmd {
    /// Start selling bandwidth (daemonizes by default, use --foreground to keep in terminal).
    /// Add --upstream or --upstream-file to resell external proxies simultaneously.
    Start {
        /// Upstream proxy host:port or connection string (repeatable)
        #[arg(long = "upstream")]
        upstream_hosts: Vec<String>,
        /// Upstream proxy username (repeatable, pairs with --upstream)
        #[arg(long = "upstream-user")]
        upstream_users: Vec<String>,
        /// Upstream proxy password (repeatable, pairs with --upstream)
        #[arg(long = "upstream-pass")]
        upstream_passes: Vec<String>,
        /// Path to a file containing upstream proxies (one per line), or '-' for stdin
        #[arg(long = "upstream-file", short = 'f')]
        upstream_file: Option<std::path::PathBuf>,
        /// Fail on any invalid proxy line in upstream file (default: skip with warning)
        #[arg(long = "upstream-strict")]
        upstream_strict: bool,
        /// Disable direct (own bandwidth). Only use --upstream proxies.
        #[arg(long)]
        no_direct: bool,
        /// Run as a volunteer node (donate bandwidth without earnings).
        #[arg(long)]
        volunteer: bool,
        /// Run in foreground (don't daemonize). Used internally by the service manager.
        #[arg(long)]
        foreground: bool,
    },
    /// Stop the background seller daemon
    Stop,
    /// Show seller status (daemon + backend stats)
    Status,
    /// Parse and validate an upstream proxy file without starting the seller
    ParseUpstreams {
        /// Path to proxy file or '-' for stdin
        file: std::path::PathBuf,
        /// Output formatted report as JSON
        #[arg(long)]
        json: bool,
    },
    /// Test TCP connection and latency to upstream SOCKS5 proxies
    TestUpstreams {
        /// Path to proxy file or '-' for stdin
        #[arg(short = 'f', long = "upstream-file")]
        file: Option<std::path::PathBuf>,
        /// Inline upstream proxies (repeatable)
        #[arg(long = "upstream")]
        upstream: Vec<String>,
        /// Connection timeout in seconds
        #[arg(long, default_value = "5")]
        timeout: u64,
    },
    /// Manage payouts
    Payout {
        #[command(subcommand)]
        cmd: PayoutCmd,
    },
    /// Install seller as a system service (launchd/systemd) — survives reboots
    Install,
}

#[derive(Subcommand)]
enum PayoutCmd {
    /// Lock seller earnings for payout
    Create {
        /// Amount in microcredits
        #[arg(long)]
        amount: i64,
        /// Destination Tempo wallet address
        #[arg(long)]
        tempo_address: String,
    },
    /// Check payout status
    Status {
        /// Payout ID
        #[arg(long)]
        id: String,
    },
    /// List payout history
    List,
}

#[derive(Subcommand)]
enum BuyerCmd {
    /// Show current credit balance
    Balance,
    /// Manage deposits
    Deposit {
        #[command(subcommand)]
        cmd: DepositCmd,
    },
    /// Transfer seller earnings to buyer balance
    Transfer {
        /// Amount in microcredits
        amount: i64,
    },
}

#[derive(Subcommand)]
enum DepositCmd {
    /// Create a new deposit invoice
    Create {
        #[arg(long)]
        amount: i64,
        #[arg(long, default_value = "usdcsol")]
        currency: String,
    },
    /// Check deposit status
    Status {
        #[arg(long)]
        id: String,
    },
    /// List deposit history
    List,
}

/// Upstream SOCKS5 proxy for resell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UpstreamProxy {
    address: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    /// Parsed from --upstream-user (e.g. "type_residential" → "residential") or query parameters.
    country: Option<String>,
    proxy_category: Option<String>,
    #[serde(default)]
    label: Option<String>,
}

/// Parse country and proxy_category from an upstream username.
/// Format: `user_2930d5,type_residential,country_US,session_usresidential`
/// Extracts: country="US", proxy_category="residential"
fn parse_upstream_metadata(username: &str) -> (Option<String>, Option<String>) {
    proxy_parser::parse_upstream_metadata(username)
}

/// Resolve the remote SOCKS5 gateway address from the backend URL.
/// The v2 buyer gateway always listens on port 1082 on the same host as the
/// backend API (127.0.0.1 in dev, api.proxybase.xyz in production).
fn socks5_proxy_address(backend_url: &str) -> String {
    let candidate = if backend_url.contains("://") {
        backend_url.to_string()
    } else {
        format!("http://{}", backend_url)
    };
    let host = url::Url::parse(&candidate)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "127.0.0.1".to_string());
    format!("{}:1082", host)
}

/// Print remote SOCKS5 connection instructions for a session.
fn print_session_credentials(sid: &str, token: &str, proxy_addr: &str, backend_url: &str) {
    let base = backend_url.trim_end_matches('/');
    println!("SOCKS5 proxy: {}", proxy_addr);
    println!("  Username: {}", sid);
    println!("  Password: {}", token);
    println!("");
    println!("Example:");
    println!("  curl --socks5 {} --proxy-user {}:{} {}/v2/ip", proxy_addr, sid, token, base);
}

// ---------------------------------------------------------------------------
// Seller config persistence (for daemon / reboot survival)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct SellerConfig {
    #[serde(default)]
    upstream_proxies: Vec<UpstreamProxyConfig>,
    #[serde(default)]
    no_direct: bool,
    /// Volunteer mode: donate bandwidth without earnings.
    /// Defaulted so configs written by older CLI versions still load.
    #[serde(default)]
    volunteer: bool,
    /// Optional path to upstream proxy file (if loaded from file)
    #[serde(default)]
    upstream_file: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct UpstreamProxyConfig {
    address: String,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    country: Option<String>,
    #[serde(default)]
    proxy_category: Option<String>,
    #[serde(default)]
    label: Option<String>,
}

fn seller_config_path() -> std::path::PathBuf {
    wallet_dir().join("seller_config.json")
}

fn save_seller_config(config: &SellerConfig) -> Result<()> {
    let path = seller_config_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(config)?)?;
    Ok(())
}

fn load_seller_config() -> Result<SellerConfig> {
    let path = seller_config_path();
    if !path.exists() {
        let default_config = SellerConfig::default();
        let _ = save_seller_config(&default_config);
        return Ok(default_config);
    }
    let content = std::fs::read_to_string(&path)
        .context("Failed to read seller_config.json")?;
    Ok(serde_json::from_str(&content)?)
}

fn load_seller_config_or_default() -> SellerConfig {
    load_seller_config().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Bridge state persistence (one bridge process per buyer session)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct BridgeState {
    session_id: String,
    port: u16,
    pid: u32,
    backend_url: String,
    upstream_addr: String,
}

fn bridge_state_dir() -> std::path::PathBuf {
    wallet_dir().join("bridges")
}

fn bridge_state_path(session_id: &str) -> std::path::PathBuf {
    bridge_state_dir().join(format!("{}.json", session_id))
}

fn save_bridge_state(state: &BridgeState) -> Result<()> {
    let path = bridge_state_path(&state.session_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(state)?)?;
    Ok(())
}

fn load_bridge_state(session_id: &str) -> Result<BridgeState> {
    let content = std::fs::read_to_string(bridge_state_path(session_id))?;
    Ok(serde_json::from_str(&content)?)
}

/// daemon-kit daemon handle for one session's bridge process. The pid file
/// lives in the wallet dir next to the seller's (proxybase-bridge-<sid>.pid).
fn bridge_daemon(session_id: &str) -> daemon_kit::Daemon {
    let config = daemon_kit::DaemonConfig::new(&format!("proxybase-bridge-{}", session_id))
        .pid_dir(wallet_dir())
        .log_file(wallet_dir().join(format!("bridge-{}.log", session_id)))
        .service_args(vec![
            "bridge".to_string(),
            "start".to_string(),
            session_id.to_string(),
            "--foreground".to_string(),
        ])
        .description("ProxyBase Local SOCKS5 Bridge");
    daemon_kit::Daemon::new(config)
}

/// Spawn the bridge as a detached background process (same pattern as
/// 'seller start'). The child binds the port and writes its state file;
/// we poll for it so the caller learns the actual bound port.
async fn start_bridge_background(
    backend_url: &str,
    session_id: &str,
    upstream: &str,
    port: Option<u16>,
) -> Result<u16> {
    let daemon = bridge_daemon(session_id);
    if daemon.is_running() {
        if let Ok(state) = load_bridge_state(session_id) {
            println!(
                "Bridge for session {} is already running on port {}.",
                session_id, state.port
            );
            return Ok(state.port);
        }
        anyhow::bail!(
            "Bridge daemon already running (PID: {}). Use 'bridge stop {}' first, or 'bridge list'.",
            daemon.running_pid().unwrap_or(0),
            session_id
        );
    }

    // Clear stale state from a previous run so the poll below only succeeds
    // once THIS child has reported its port.
    let _ = std::fs::remove_file(bridge_state_path(session_id));

    let exe = std::env::current_exe()
        .context("Cannot determine current executable path")?;
    let log_path = wallet_dir().join(format!("bridge-{}.log", session_id));
    if let Some(parent) = log_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .context("Cannot open bridge log file")?;

    let mut cmd = std::process::Command::new(&exe);
    cmd.arg("bridge")
        .arg("start")
        .arg(session_id)
        .arg("--foreground")
        .arg("--backend")
        .arg(backend_url)
        .arg("--upstream")
        .arg(upstream);
    if let Some(p) = port {
        cmd.arg("--port").arg(p.to_string());
    }
    cmd.stdin(std::process::Stdio::null())
        .stdout(log_file.try_clone()?)
        .stderr(log_file);
    let child = cmd
        .spawn()
        .context("Failed to spawn bridge daemon process")?;

    // The child may take a moment to bind (and may fall back to an ephemeral
    // port). Poll its state file, then give up and point at the log.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match load_bridge_state(session_id) {
            Ok(state) if state.pid == child.id() => return Ok(state.port),
            _ => {}
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!(
        "Bridge process started (PID {}) but did not report its port within 5s. Check {}",
        child.id(),
        log_path.display()
    );
}

fn seller_daemon() -> daemon_kit::Daemon {
    let config = daemon_kit::DaemonConfig::new("proxybase-seller")
        .pid_dir(wallet_dir())
        .log_file(wallet_dir().join("seller.log"))
        .service_args(vec![
            "seller".to_string(),
            "start".to_string(),
            "--foreground".to_string(),
        ])
        .description("ProxyBase Seller — bandwidth resale daemon");
    daemon_kit::Daemon::new(config)
}

/// Build the list of paths: direct (None) + each upstream proxy.
fn build_paths(upstreams: &[UpstreamProxy], include_direct: bool) -> Vec<(String, Option<UpstreamProxy>)> {
    let mut paths: Vec<(String, Option<UpstreamProxy>)> = Vec::new();
    if include_direct {
        paths.push(("direct".to_string(), None));
    }
    for (i, u) in upstreams.iter().enumerate() {
        paths.push((format!("upstream_{}", i), Some(u.clone())));
    }
    if paths.is_empty() {
        // At least one path — direct with no upstream
        paths.push(("direct".to_string(), None));
    }
    paths
}

/// Shared async seller entry point. Opens one WebSocket connection per path
/// (direct + each upstream) so each path is independently classified and matched.
async fn run_seller(backend_url: &str, proxies: &[UpstreamProxy], include_direct: bool, volunteer: bool) {
    let client = BackendClient::new(backend_url);
    if !client.is_authenticated() {
        eprintln!("[seller] Not authenticated. Run 'proxybase-cli login' first.");
        return;
    }
    let node_type = if volunteer { "volunteer" } else { "standard" };
    let _ = client.register_seller(node_type).await;
    if volunteer {
        eprintln!("[seller] Volunteer mode: donating bandwidth without earnings.");
    }

    let paths = build_paths(proxies, include_direct);
    let token = std::sync::Arc::new(tokio::sync::Mutex::new(
        client.token.as_deref().unwrap_or("").to_string(),
    ));
    let base_url = backend_url.to_string();

    eprintln!("[seller] Starting {} path(s): {:?}", paths.len(), paths.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>());

    // Spawn one connection per path — each runs independently with its own reconnect loop.
    // Token is shared via Arc<Mutex<>> so re-auth by one path benefits all.
    let mut handles = Vec::new();
    for (path_id, upstream) in paths {
        let token = token.clone();
        let url = base_url.clone();
        handles.push(tokio::spawn(async move {
            run_single_path_loop(&url, token, &path_id, upstream.as_ref()).await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }
}

#[derive(Subcommand)]
enum MarketCmd {
    /// List available countries
    Countries,
    /// List available payment currencies
    Currencies,
    /// Fetch pricing
    Prices {
        #[arg(long)]
        country: String,
        #[arg(long)]
        network_type: String,
    },
    /// Open a purchased proxy session
    Buy {
        #[arg(long)]
        country: String,
        #[arg(long)]
        network_type: String,
        #[arg(long, default_value = "rotating")]
        session_type: String,
        #[arg(long)]
        sticky_duration: Option<u64>,
        /// Also start a local unauthenticated SOCKS5 bridge for the session
        #[arg(long)]
        bridge: bool,
        /// Preferred local port for the --bridge listener
        #[arg(long)]
        bridge_port: Option<u16>,
    },
    /// Close a session
    Close {
        session_id: String,
    },
    /// Rotate exit node for an active sticky session
    Rotate {
        /// Session ID to rotate
        #[arg(long)]
        id: String,
    },
    /// List active/past sessions
    Sessions,
    /// Get a single session's details
    SessionStatus {
        /// Session ID
        #[arg(long)]
        id: String,
    },
    /// Send a keepalive ping to an active session (prevents the 1h idle timeout)
    Keepalive {
        /// Session ID
        #[arg(long)]
        id: String,
        /// Repeat every 5 minutes until interrupted
        #[arg(long = "loop")]
        loop_: bool,
    },
}

/// Local unauthenticated SOCKS5 bridge for a buyer session. Forwards to the
/// authenticated remote gateway (username = session id, password = session
/// token) so apps without SOCKS5 auth support can use the session.
#[derive(Subcommand)]
enum BridgeCmd {
    /// Start a bridge. Daemonizes by default; use --foreground to keep it in
    /// the terminal (used internally by 'bridge start' in background mode).
    Start {
        /// Session ID (from 'market buy')
        session_id: String,
        /// Preferred local port (falls back to an ephemeral port if taken)
        #[arg(long)]
        port: Option<u16>,
        /// Override the upstream SOCKS5 gateway address (default: derived from --backend)
        #[arg(long)]
        upstream: Option<String>,
        /// Run in foreground (don't daemonize)
        #[arg(long)]
        foreground: bool,
    },
    /// Stop a running bridge
    Stop {
        session_id: String,
    },
    /// List running bridges
    List,
}

// ---------------------------------------------------------------------------
// Backend API client
// ---------------------------------------------------------------------------

struct BackendClient {
    http: reqwest::Client,
    base_url: String,
    token: Option<String>,
}

impl BackendClient {
    fn new(base_url: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            token: Self::load_token(),
        }
    }

    fn token_path() -> std::path::PathBuf {
        data_dir().join("session_token")
    }

    fn load_token() -> Option<String> {
        let path = Self::token_path();
        Self::harden_token_file(&path);
        std::fs::read_to_string(path).ok()
    }

    fn save_token(token: &str) {
        Self::save_token_in(&data_dir(), token);
    }

    /// The session token is password-equivalent: it can spend the account
    /// balance, so it must never land world-readable on disk.
    fn save_token_in(dir: &std::path::Path, token: &str) {
        let path = dir.join("session_token");
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ =
                    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
            }
        }
        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)
            {
                if file.write_all(token.as_bytes()).is_ok() {
                    // OpenOptions .mode only applies at creation; an existing
                    // file keeps its old mode, so enforce it here as well.
                    let _ = std::fs::set_permissions(
                        &path,
                        std::fs::Permissions::from_mode(0o600),
                    );
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = std::fs::write(&path, token);
        }
    }

    /// Tighten permissions left behind by older versions.
    fn harden_token_file(path: &std::path::Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(meta) = std::fs::metadata(path) {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    let _ =
                        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = path;
        }
    }

    fn bearer(&self) -> String {
        format!("Bearer {}", self.token.as_deref().unwrap_or(""))
    }

    fn is_authenticated(&self) -> bool {
        self.token.is_some()
    }

    // --- Auth ---

    async fn auth_challenge(&self, wallet_address: &str) -> Result<ChallengeResponse> {
        let resp = self
            .http
            .post(format!("{}/v2/auth/challenge", self.base_url))
            .json(&serde_json::json!({"wallet_address": wallet_address}))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn auth_verify(
        &self,
        public_key_hex: &str,
        nonce: &str,
        timestamp: &str,
        signature_hex: &str,
    ) -> Result<VerifyResponse> {
        let resp = self
            .http
            .post(format!("{}/v2/auth/verify", self.base_url))
            .json(&serde_json::json!({
                "public_key_hex": public_key_hex,
                "nonce": nonce,
                "timestamp": timestamp,
                "signature_hex": signature_hex,
            }))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    // --- Wallet ---

    async fn get_balance(&self) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/v2/wallet/balance", self.base_url))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn transfer(&self, amount: i64) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/wallet/transfer", self.base_url))
            .header("Authorization", self.bearer())
            .json(&serde_json::json!({"amount_microcredits": amount}))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    // --- Deposits ---

    async fn create_deposit(&self, amount: i64, currency: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/deposits", self.base_url))
            .header("Authorization", self.bearer())
            .json(&serde_json::json!({
                "amount_microcredits": amount,
                "pay_currency": currency,
            }))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn get_deposit(&self, deposit_id: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/v2/deposits/{}", self.base_url, deposit_id))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn list_deposits(&self) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/deposits", self.base_url))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
    }

    async fn list_payouts(&self) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/payouts", self.base_url))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
    }

    async fn create_payout(&self, amount: i64, tempo_address: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/payouts", self.base_url))
            .header("Authorization", self.bearer())
            .json(&serde_json::json!({
                "amount_microcredits": amount,
                "tempo_address": tempo_address,
            }))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn get_payout(&self, payout_id: &str) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/payouts/{}", self.base_url, payout_id))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
    }

    async fn list_currencies(&self) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/currencies", self.base_url))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
    }

    async fn list_sessions(&self) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/sessions", self.base_url))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
    }

    // --- Seller ---

    async fn register_seller(&self, node_type: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/seller/register", self.base_url))
            .header("Authorization", self.bearer())
            .json(&serde_json::json!({ "node_type": node_type }))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn seller_status(&self) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/v2/seller/status", self.base_url))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    // --- Market ---

    async fn list_countries(&self) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/v2/catalog/countries", self.base_url))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn list_pricing(&self) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(format!("{}/v2/catalog/pricing", self.base_url))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn create_session(
        &self,
        country: &str,
        network_type: &str,
        session_type: &str,
        spend_cap: Option<i64>,
    ) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/sessions", self.base_url))
            .header("Authorization", self.bearer())
            .json(&serde_json::json!({
                "country": country,
                "network_type": network_type,
                "session_type": session_type,
                "spend_cap_microcredits": spend_cap,
            }))
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn close_session(&self, session_id: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .delete(format!("{}/v2/sessions/{}", self.base_url, session_id))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        Ok(resp.json().await?)
    }

    async fn rotate_session(&self, session_id: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/sessions/{}/rotate", self.base_url, session_id))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await?;
        if !status.is_success() {
            let msg = body
                .get("reason")
                .or_else(|| body.get("error"))
                .and_then(|v| v.as_str())
                .unwrap_or("Failed to rotate session");
            anyhow::bail!("{}: {}", status, msg);
        }
        Ok(body)
    }

    async fn get_session(&self, session_id: &str) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/sessions/{}", self.base_url, session_id))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
    }

    async fn keepalive_session(&self, session_id: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/sessions/{}/keepalive", self.base_url, session_id))
            .header("Authorization", self.bearer())
            .send()
            .await?;
        let status = resp.status();
        let body: serde_json::Value = resp.json().await?;
        if !status.is_success() {
            let msg = body
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Failed to send keepalive");
            anyhow::bail!("{}: {}", status, msg);
        }
        Ok(body)
    }

    async fn health(&self) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/health", self.base_url))
            .send().await?;
        Ok(resp.json().await?)
    }
}

// ---------------------------------------------------------------------------
// JSON response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ChallengeResponse {
    nonce: String,
    timestamp: String,
}

#[derive(Debug, Deserialize)]
struct VerifyResponse {
    session_token: String,
    wallet_address: String,
    role: String,
    buyer_available: i64,
    spendable_balance: i64,
}

// ---------------------------------------------------------------------------
// Wallet helper
// ---------------------------------------------------------------------------

/// Run a bidirectional relay for one stream. Handles both direct TCP and upstream SOCKS5.
async fn run_stream_relay(
    target_dest: &str, // Domain or IP for SOCKS5 routing
    target_ip: &str,   // IP only for direct TCP routing
    target_port: u16,
    upstream: Option<&UpstreamProxy>,
    relay_tx: &tokio::sync::mpsc::UnboundedSender<Message>,
    mut tcp_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    sid: &str,
) {
    let sid = sid.to_string();
    // Connect to target — via upstream proxy or directly
    let connect_result: anyhow::Result<(
        Box<dyn tokio::io::AsyncRead + Unpin + Send>,
        Box<dyn tokio::io::AsyncWrite + Unpin + Send>,
    )> = match upstream {
        Some(proxy) => {
            let has_auth = proxy.username.as_ref().map(|u| !u.is_empty()).unwrap_or(false);
            if has_auth {
                let u = proxy.username.clone().unwrap_or_default();
                let p = proxy.password.clone().unwrap_or_default();
                eprintln!("[RELAY {}] Using upstream proxy {} (user={})", sid, proxy.address, u);
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(10),
                    fast_socks5::client::Socks5Stream::connect_with_password(
                        &proxy.address,
                        target_dest.to_string(),
                        target_port,
                        u,
                        p,
                        fast_socks5::client::Config::default(),
                    ),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        let (r, w) = tokio::io::split(stream);
                        Ok((Box::new(r), Box::new(w)))
                    }
                    Ok(Err(e)) => Err(anyhow::anyhow!("SOCKS5 upstream connect failed: {:?}", e)),
                    Err(_) => Err(anyhow::anyhow!("SOCKS5 upstream connect timed out after 10s")),
                }
            } else {
                eprintln!("[RELAY {}] Using upstream proxy {} (unauthenticated)", sid, proxy.address);
                match tokio::time::timeout(
                    tokio::time::Duration::from_secs(10),
                    fast_socks5::client::Socks5Stream::connect(
                        &proxy.address,
                        target_dest.to_string(),
                        target_port,
                        fast_socks5::client::Config::default(),
                    ),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        let (r, w) = tokio::io::split(stream);
                        Ok((Box::new(r), Box::new(w)))
                    }
                    Ok(Err(e)) => Err(anyhow::anyhow!("SOCKS5 upstream connect failed: {:?}", e)),
                    Err(_) => Err(anyhow::anyhow!("SOCKS5 upstream connect timed out after 10s")),
                }
            }
        }
        None => {
            eprintln!("[RELAY {}] Direct connect (no upstream proxy)", sid);
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(10),
                tokio::net::TcpStream::connect(format!("{}:{}", target_ip, target_port)),
            )
            .await
            {
                Ok(Ok(tcp)) => {
                    let (r, w) = tokio::io::split(tcp);
                    Ok((Box::new(r), Box::new(w)))
                }
                Ok(Err(e)) => Err(anyhow::anyhow!("TCP connect failed: {}", e)),
                Err(_) => Err(anyhow::anyhow!("TCP connect timed out after 10s")),
            }
        }
    };

    let (mut tcp_r, mut tcp_w) = match connect_result {
        Ok(streams) => {
            eprintln!("[RELAY {}] Connected to target", sid);
            streams
        }
        Err(e) => {
            eprintln!("[RELAY {}] Connect failed: {}", sid, e);
            return;
        }
    };

    // Race TCP→WS and WS→TCP via tokio::select! so that when EITHER
    // direction finishes (TCP EOF or WS channel closed), the other is
    // immediately canceled and both tcp_r + tcp_w are dropped.
    // This prevents CLOSE_WAIT leaks: previously the write half was held
    // indefinitely waiting on tcp_rx.recv() while the read half was already
    // closed, leaking file descriptors until "Too many open files".
    let tx2 = relay_tx.clone();
    let sid2 = sid.clone();
    let sid3 = sid.clone();
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

    // True inactivity timeout: traffic in either direction resets the clock.
    // (A hard cap would silently kill long-lived buyer sessions after 60s; an
    // idle timeout still closes abandoned probe/keep-alive connections so FDs
    // cannot accumulate.)
    let deadline = std::sync::Arc::new(std::sync::Mutex::new(
        tokio::time::Instant::now() + IDLE_TIMEOUT,
    ));

    let tcp_to_ws = {
        let deadline = deadline.clone();
        async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match tokio::io::AsyncReadExt::read(&mut tcp_r, &mut buf).await {
                    Ok(0) => { eprintln!("[RELAY {}] TCP closed", sid2); break; }
                    Ok(n) => {
                        *deadline.lock().unwrap() = tokio::time::Instant::now() + IDLE_TIMEOUT;
                        let enc = base64_encode(&buf[..n]);
                        let m = serde_json::json!({"type":"relay_response","session_id":&sid2,"data":enc});
                        if tx2.send(Message::Text(serde_json::to_string(&m).unwrap_or_default())).is_err() {
                            break;
                        }
                    }
                    Err(e) => { eprintln!("[RELAY {}] Read error: {}", sid2, e); break; }
                }
            }
        }
    };

    let ws_to_tcp = {
        let deadline = deadline.clone();
        let sid = sid3;
        async move {
            while let Some(data) = tcp_rx.recv().await {
                *deadline.lock().unwrap() = tokio::time::Instant::now() + IDLE_TIMEOUT;
                if tokio::io::AsyncWriteExt::write_all(&mut tcp_w, &data).await.is_err() {
                    eprintln!("[RELAY {}] Write failed", sid);
                    break;
                }
            }
        }
    };

    // Sleep until the current idle deadline, re-checking it on each wake so
    // any traffic keeps the relay alive indefinitely.
    let idle_waiter = {
        let deadline = deadline.clone();
        async move {
            loop {
                let next = *deadline.lock().unwrap();
                if tokio::time::Instant::now() >= next {
                    break;
                }
                tokio::time::sleep_until(next).await;
            }
        }
    };

    tokio::select! {
        _ = tcp_to_ws => {}
        _ = ws_to_tcp => {}
        _ = idle_waiter => {
            eprintln!("[RELAY {}] Idle timeout — closing", sid);
        }
    }
    eprintln!("[RELAY {}] Closed", sid);
}

fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        out.push(if chunk.len() > 1 { CHARS[((triple >> 6) & 0x3F) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { CHARS[(triple & 0x3F) as usize] as char } else { '=' });
    }
    out
}

fn base64_decode(encoded: &str) -> Option<Vec<u8>> {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;
    for &b in encoded.as_bytes() {
        if b == b'=' { break; }
        let val = CHARS.iter().position(|&c| c == b)? as u32;
        buf = (buf << 6) | val;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

/// Re-authenticate with the backend and return a fresh session token.
/// Loads the wallet from disk, signs a challenge, and saves the new token.
async fn re_authenticate_single(backend_url: &str) -> Result<String> {
    let wm = load_wallet()
        .context("No wallet found. Cannot re-authenticate.")?;
    let address = wm.address()
        .ok_or_else(|| anyhow::anyhow!("Wallet not loaded"))?;

    let client = BackendClient::new(backend_url);
    let challenge = client.auth_challenge(address).await?;
    let message = format!("{}:{}:{}", address, challenge.nonce, challenge.timestamp);
    let signature = wm.sign(message.as_bytes())?;
    let sig_hex = hex::encode(&signature);
    let public_key_hex = wm.public_key_hex()
        .ok_or_else(|| anyhow::anyhow!("Cannot get public key"))?;

    let auth = client.auth_verify(&public_key_hex, &challenge.nonce, &challenge.timestamp, &sig_hex).await?;
    BackendClient::save_token(&auth.session_token);
    Ok(auth.session_token)
}

/// Exponential backoff with ±20% jitter so multiple sellers/daemons do not
/// reconnect in lockstep after an outage.
fn jittered_backoff(secs: u64) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let factor = 0.8 + (nanos % 401) as f64 / 1000.0; // 0.8 ..= 1.2
    Duration::from_secs_f64(secs as f64 * factor)
}

/// Single-path WebSocket connection loop. Handles one path (direct or one upstream).
/// Reconnects with exponential backoff. Sends auth token + path_info on each connect.
/// On token expiry, re-authenticates and updates the shared token.
async fn run_single_path_loop(
    backend_url: &str,
    token: std::sync::Arc<tokio::sync::Mutex<String>>,
    path_id: &str,
    upstream: Option<&UpstreamProxy>,
) {
    let upstream = upstream.cloned();
    let path_id = path_id.to_string();
    let mut backoff_secs = 1u64;

    loop {
        let current_token = token.lock().await.clone();
        // Percent-encode the token so query-string special characters can
        // never corrupt the URL (tokens are hex today, but be safe).
        let encoded_token: String =
            url::form_urlencoded::byte_serialize(current_token.as_bytes()).collect();
        let ws_url = format!(
            "{}/v2/ws/seller?token={}",
            backend_url.replace("https://", "wss://").replace("http://", "ws://"),
            encoded_token
        );

        eprintln!("[{}] Connecting (backoff={}s)...", path_id, backoff_secs);
        match try_single_path_connection(&ws_url, &current_token, &path_id, upstream.as_ref()).await {
            Ok(()) => {
                backoff_secs = 1;
                eprintln!("[{}] Disconnected. Reconnecting...", path_id);
            }
            Err(e) if e.to_string().contains("AUTH_EXPIRED") || e.to_string().contains("401") => {
                eprintln!("[{}] Session token expired. Re-authenticating...", path_id);
                match re_authenticate_single(backend_url).await {
                    Ok(new_token) => {
                        *token.lock().await = new_token;
                        eprintln!("[{}] Re-authenticated successfully.", path_id);
                        backoff_secs = 1;
                    }
                    Err(auth_err) => {
                        eprintln!("[{}] Re-auth failed: {}. Retrying in {}s...", path_id, auth_err, backoff_secs);
                        tokio::time::sleep(jittered_backoff(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(60);
                    }
                }
            }
            Err(e) => {
                eprintln!("[{}] Connection failed: {}. Retrying in {}s...", path_id, e, backoff_secs);
                tokio::time::sleep(jittered_backoff(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    }
}

/// Establish one WebSocket connection for a single path and relay until disconnect.
async fn try_single_path_connection(
    ws_url: &str,
    token: &str,
    path_id: &str,
    upstream: Option<&UpstreamProxy>,
) -> Result<()> {
    let (ws, _resp) = tokio::time::timeout(
        tokio::time::Duration::from_secs(15),
        connect_async(ws_url),
    )
    .await
    .context("connect_async timed out after 15s")?
    .context("Failed to connect WebSocket")?;
    let conn_id = uuid::Uuid::new_v4().to_string();
    eprintln!("[{}] Connected (conn={}).", path_id, &conn_id[..8]);

    let (mut ws_sink, mut ws_stream) = ws.split();

    // Send auth token as first message
    ws_sink
        .send(Message::Text(token.to_string()))
        .await
        .context("Failed to send auth token")?;

    // Send path_info to identify this connection's path.
    // Country and proxy_category are NOT sent — the backend discovers them
    // through QoS probes + IP intelligence, same as direct connections.
    let path_info = serde_json::json!({"type": "path_info", "path_id": path_id});
    ws_sink
        .send(Message::Text(serde_json::to_string(&path_info).unwrap_or_default()))
        .await
        .context("Failed to send path_info")?;

    let (relay_tx, mut relay_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let active: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>> = Default::default();

    let relay_drain = tokio::spawn(async move {
        while let Some(msg) = relay_rx.recv().await {
            if ws_sink.send(msg).await.is_err() { break; }
        }
    });

    // Handles to every relay task spawned below; aborted together with the
    // path connection so a hung connect cannot leak sockets between reconnects.
    let mut relay_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    let upstream = upstream.cloned();
    let mut ping_tick = interval(Duration::from_secs(30));
    let mut heartbeat_tick = interval(Duration::from_secs(15));
    let mut watchdog = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(90),
        Duration::from_secs(90),
    );
    const MAX_STREAMS: usize = 100;

    loop {
        tokio::select! {
            _ = watchdog.tick() => {
                relay_drain.abort();
                for h in &relay_tasks { h.abort(); }
                return Err(anyhow::anyhow!("Connection watchdog: no message in 90s"));
            }
            _ = ping_tick.tick() => { let _ = relay_tx.send(Message::Ping(vec![].into())); }
            _ = heartbeat_tick.tick() => {
                let current_streams = active.lock().await.len() as u32;
                let hb = serde_json::json!({"type":"heartbeat","active_streams":current_streams,"version":env!("CARGO_PKG_VERSION"),"conn_id":conn_id});
                let _ = relay_tx.send(Message::Text(serde_json::to_string(&hb).unwrap_or_default()));
            }
            msg = ws_stream.next() => {
                watchdog.reset();
                match msg {
                    Some(Ok(Message::Ping(d))) => { let _ = relay_tx.send(Message::Pong(d)); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(p) = serde_json::from_str::<serde_json::Value>(&text) {
                            if p.get("error").and_then(|v| v.as_str()) == Some("invalid_token") {
                                relay_drain.abort();
                                for h in &relay_tasks { h.abort(); }
                                return Err(anyhow::anyhow!("AUTH_EXPIRED"));
                            }
                            match p.get("type").and_then(|v| v.as_str()) {
                                Some("relay_data") => {
                                    if let Some(enc) = p.get("data").and_then(|v| v.as_str()) {
                                        if let Some(dec) = base64_decode(enc) {
                                            let streams = active.lock().await;
                                            let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                                            // Unknown sid: the stream ended on our side before the
                                            // backend learned about it. Drop — sending to an
                                            // arbitrary other stream would corrupt its traffic.
                                            if let Some(s) = streams.get(sid) { let _ = s.send(dec); }
                                        }
                                    }
                                }
                                Some("stream_close") => {
                                    let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                                    active.lock().await.remove(sid);
                                }
                                Some("stream_open") => {
                                    if active.lock().await.len() >= MAX_STREAMS { continue; }
                                    let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("?").to_string();
                                    let tip = p.get("target_ip").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string();
                                    let tport = p.get("target_port").and_then(|v| v.as_u64()).unwrap_or(443) as u16;
                                    let thost = p.get("target_host").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    let dest = thost.unwrap_or_else(|| tip.clone());
                                    eprintln!("[{}] STREAM {} → {}:{} (direct_ip={})", path_id, sid, dest, tport, tip);

                                    // F2: ACK the command
                                    if let Some(seq) = p.get("seq").and_then(|v| v.as_u64()) {
                                        let ack = serde_json::json!({"type": "cmd_ack", "seq": seq});
                                        let _ = relay_tx.send(Message::Text(serde_json::to_string(&ack).unwrap_or_default()));
                                    }

                                    let streams = active.clone();
                                    let tx = relay_tx.clone();
                                    let up = upstream.clone();

                                    let (tcp_tx, tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
                                    streams.lock().await.insert(sid.clone(), tcp_tx);

                                    let handle = tokio::spawn(async move {
                                        run_stream_relay(&dest, &tip, tport, up.as_ref(), &tx, tcp_rx, &sid).await;
                                        streams.lock().await.remove(&sid);
                                    });
                                    relay_tasks.push(handle);
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => { eprintln!("[{}] Backend closed connection", path_id); break; }
                    Some(Err(e)) => { eprintln!("[{}] WS error: {}", path_id, e); break; }
                    _ => {}
                }
            }
        }
    }
    relay_drain.abort();
    for h in &relay_tasks { h.abort(); }
    Ok(())
}

/// Shared client state directory. PROXYBASE_DIR (containers, isolated scratch
/// runs) wins over the default ~/.proxybase.
fn data_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("PROXYBASE_DIR") {
        if !dir.is_empty() {
            return std::path::PathBuf::from(dir);
        }
    }
    dirs::home_dir().unwrap_or_default().join(".proxybase")
}

pub(crate) fn wallet_dir() -> std::path::PathBuf {
    data_dir()
}

fn load_wallet() -> Result<libproxybase::WalletManager> {
    let mut wm = libproxybase::WalletManager::new(wallet_dir())?;

    let env_pw = std::env::var("PROXYBASE_PASSWORD").unwrap_or_default();
    let passwords_to_try: Vec<&str> = if env_pw.is_empty() {
        vec![""]
    } else {
        vec![&env_pw, ""]
    };

    let mut loaded = false;
    for pw in &passwords_to_try {
        if wm.load(pw).is_ok() {
            loaded = true;
            break;
        }
    }
    if !loaded {
        anyhow::bail!(
            "Failed to load wallet. If password-protected, set PROXYBASE_PASSWORD env var."
        );
    }
    Ok(wm)
}

async fn authenticate(client: &BackendClient, wm: &libproxybase::WalletManager) -> Result<String> {
    let address = wm
        .address()
        .ok_or_else(|| anyhow::anyhow!("Wallet not loaded"))?;

    // Step 1: Get challenge nonce
    let challenge = client.auth_challenge(address).await?;
    println!("Got challenge nonce: {}...", &challenge.nonce[..16]);

    // Step 2: Sign the challenge
    let message = format!("{}:{}:{}", address, challenge.nonce, challenge.timestamp);
    let signature = wm.sign(message.as_bytes())?;
    let sig_hex = hex::encode(&signature);

    // Public key = SEC1-encoded verifying key hex (NOT the derived address)
    let public_key_hex = wm.public_key_hex()
        .ok_or_else(|| anyhow::anyhow!("Wallet not loaded — cannot get public key"))?;

    // Step 3: Verify with backend
    let auth = client
        .auth_verify(&public_key_hex, &challenge.nonce, &challenge.timestamp, &sig_hex)
        .await?;

    BackendClient::save_token(&auth.session_token);
    println!("Authenticated as: {}", auth.wallet_address);
    println!("  Role: {}", auth.role);
    println!("  Buyer available: {} microcredits", auth.buyer_available);
    println!("  Spendable balance: {} microcredits", auth.spendable_balance);
    println!("Session token saved.");

    Ok(auth.session_token)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    update::cleanup_stale();
    let _ = rustls::crypto::ring::default_provider().install_default();
    let cli = Cli::parse();
    if !matches!(cli.command, Commands::Update { .. }) {
        update::check_and_notify().await;
    }
    let mut client = BackendClient::new(&cli.backend);

    match cli.command {
        // --- Wallet ---
        Commands::Wallet { cmd } => match cmd {
            WalletCmd::Create => {
                let mut wm = libproxybase::WalletManager::new(wallet_dir())?;
                let mnemonic = wm.create("")?;
                println!("Wallet created successfully!");
                println!("Address: {}", wm.address().unwrap_or("unknown"));
                println!("Mnemonic (SAVE THIS SECURELY):");
                println!("  {}", mnemonic);
            }
            WalletCmd::Import { phrase, hd_index } => {
                let mut wm = libproxybase::WalletManager::new(wallet_dir())?;
                // Keystore password comes from PROXYBASE_PASSWORD (containers),
                // defaulting to "" for interactive/legacy use. load_wallet()
                // tries the same order, so both sides stay consistent.
                let password = std::env::var("PROXYBASE_PASSWORD").unwrap_or_default();
                match hd_index {
                    Some(index) => {
                        wm.import_hd(&phrase, index, &password)?;
                        println!("Wallet imported successfully (HD index {}: m/44'/60'/0'/0/{})", index, index);
                    }
                    None => {
                        wm.import(&phrase, &password)?;
                        println!("Wallet imported successfully!");
                    }
                }
                println!("Address: {}", wm.address().unwrap_or("unknown"));
            }
            WalletCmd::Sweep { phrase, start_index, count, target_tempo, min_threshold } => {
                if count == 0 {
                    anyhow::bail!("--count must be at least 1");
                }
                let seed = libproxybase::wallet::mnemonic::mnemonic_to_seed(&phrase, "")?;
                // Preserve the operator's on-disk session token: child logins
                // only mutate the in-memory client.
                let saved_token = client.token.clone();
                let mut payouts = 0u32;
                let mut swept_total = 0i64;

                println!(
                    "Sweeping HD children {}..{} -> {} (min threshold {} microcredits)",
                    start_index,
                    start_index + count - 1,
                    target_tempo,
                    min_threshold,
                );
                for index in start_index..start_index + count {
                    let (sk, vk) = match libproxybase::wallet::hd::derive_bip44_keypair(&seed, index) {
                        Ok(pair) => pair,
                        Err(e) => {
                            eprintln!("[{index}] derivation failed: {e}");
                            continue;
                        }
                    };
                    let address = libproxybase::wallet::keypair::public_key_to_address(&vk)?;

                    let challenge = match client.auth_challenge(&address).await {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("[{index}] {address}: challenge failed: {e}");
                            continue;
                        }
                    };
                    use k256::ecdsa::signature::Signer;
                    let message = format!("{}:{}:{}", address, challenge.nonce, challenge.timestamp);
                    let signature: k256::ecdsa::Signature = sk.sign(message.as_bytes());
                    let sig_hex = hex::encode(signature.to_vec());
                    let public_key_hex = hex::encode(vk.to_sec1_bytes());
                    let auth = match client
                        .auth_verify(&public_key_hex, &challenge.nonce, &challenge.timestamp, &sig_hex)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("[{index}] {address}: auth verify failed: {e}");
                            continue;
                        }
                    };
                    client.token = Some(auth.session_token);

                    // Query the ledger directly. GET /v2/seller/status only
                    // reports earnings for nodes currently connected to the
                    // in-memory seller pool, so an offline child would read
                    // as zero. GET /v2/wallet/balance returns seller_available
                    // from the ledger regardless of connection state.
                    let balance = match client.get_balance().await {
                        Ok(b) => b,
                        Err(e) => {
                            eprintln!("[{index}] {address}: balance lookup failed: {e}");
                            continue;
                        }
                    };
                    let available = balance
                        .get("seller_available")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    if available < min_threshold {
                        println!("[{index}] {address}: {available} microcredits (below threshold {min_threshold})");
                        continue;
                    }
                    match client.create_payout(available, &target_tempo).await {
                        Ok(resp) => {
                            let payout_id = resp.pointer("/payout_id").and_then(|v| v.as_str()).unwrap_or("?");
                            println!("[{index}] {address}: payout {available} microcredits -> {target_tempo} (payout {payout_id})");
                            swept_total += available;
                            payouts += 1;
                        }
                        Err(e) => eprintln!("[{index}] {address}: payout failed: {e}"),
                    }
                }
                client.token = saved_token;
                println!("Sweep complete: {payouts} payout(s), {swept_total} microcredits total.");
            }
            WalletCmd::Info => {
                match load_wallet() {
                    Ok(wm) => {
                        println!("Address: {}", wm.address().unwrap_or("unknown"));
                        println!("Status: Loaded");
                    }
                    Err(_) => {
                        println!("No wallet found. Run 'proxybase-cli wallet create' first.");
                    }
                }
            }
        },

        // --- Login ---
        Commands::Login => {
            let wm = load_wallet()
                .context("No wallet found. Run 'wallet create' first.")?;
            authenticate(&client, &wm).await?;
        }

        // --- Seller ---
        Commands::Seller { cmd } => {
            // Standalone inspection / diagnostic / daemon commands (no auth required)
            match &cmd {
                SellerCmd::ParseUpstreams { file, json } => {
                    let report = if file == std::path::Path::new("-") {
                        proxy_parser::parse_proxy_stdin()?
                    } else {
                        proxy_parser::parse_proxy_file(file)?
                    };

                    if *json {
                        println!("{}", serde_json::to_string_pretty(&report)?);
                        return Ok(());
                    }

                    println!("════════════════════════════════════════════════════════════════════════════════════════");
                    println!(" Upstream SOCKS5 Proxy Parsing Report");
                    println!(" Source: {}", file.display());
                    println!(" Total lines: {} | Parsed: {} | Skipped/Comments: {} | Duplicates: {} | Warnings: {}",
                        report.total_lines,
                        report.parsed_count,
                        report.skipped_empty_or_comments,
                        report.duplicates_deduplicated,
                        report.warnings.len(),
                    );
                    println!("════════════════════════════════════════════════════════════════════════════════════════");

                    if !report.proxies.is_empty() {
                        println!("{:<4} {:<30} {:<18} {:<10} {:<14} {:<15}", "#", "Address", "Auth", "Country", "Category", "Label");
                        println!("{:-<4} {:-<30} {:-<18} {:-<10} {:-<14} {:-<15}", "", "", "", "", "", "");
                        for (i, p) in report.proxies.iter().enumerate() {
                            let auth_str = match (&p.username, &p.password) {
                                (Some(u), Some(_)) => format!("user: {}", u),
                                (Some(u), None) => format!("user: {}", u),
                                _ => "none (open)".to_string(),
                            };
                            let auth_display = if auth_str.len() > 17 {
                                format!("{}…", &auth_str[..16])
                            } else {
                                auth_str
                            };
                            let country_str = p.country.as_deref().unwrap_or("—");
                            let cat_str = p.proxy_category.as_deref().unwrap_or("—");
                            let label_str = p.label.as_deref().unwrap_or("—");
                            println!("{:<4} {:<30} {:<18} {:<10} {:<14} {:<15}", i + 1, p.address, auth_display, country_str, cat_str, label_str);
                        }
                    }

                    if !report.warnings.is_empty() {
                        println!("\nWarnings ({} line(s) skipped):", report.warnings.len());
                        for w in &report.warnings {
                            println!("  [Line {}] {}: {}", w.line_number, w.reason, w.raw_line);
                        }
                    }
                    println!("");
                    return Ok(());
                }
                SellerCmd::TestUpstreams { file, upstream, timeout } => {
                    let mut parsed_proxies = Vec::new();
                    if let Some(f) = file {
                        let report = if f == std::path::Path::new("-") {
                            proxy_parser::parse_proxy_stdin()?
                        } else {
                            proxy_parser::parse_proxy_file(f)?
                        };
                        for w in &report.warnings {
                            eprintln!("Warning: [Line {}] {}: {}", w.line_number, w.reason, w.raw_line);
                        }
                        parsed_proxies.extend(report.proxies);
                    }
                    for u_str in upstream {
                        if let Ok(Some(p)) = proxy_parser::parse_proxy_line(u_str) {
                            parsed_proxies.push(p);
                        }
                    }

                    if parsed_proxies.is_empty() {
                        println!("No proxies provided to test. Specify -f <FILE> or --upstream <PROXY>.");
                        return Ok(());
                    }

                    println!("Testing {} upstream SOCKS5 proxy endpoint(s) (timeout: {}s)...", parsed_proxies.len(), timeout);
                    println!("════════════════════════════════════════════════════════════════════════════════════════");

                    let timeout_dur = tokio::time::Duration::from_secs(*timeout);
                    let mut success_count = 0;
                    let mut fail_count = 0;

                    for (i, p) in parsed_proxies.iter().enumerate() {
                        let start = std::time::Instant::now();
                        let res = match (&p.username, &p.password) {
                            (Some(u), Some(pass)) => {
                                tokio::time::timeout(
                                    timeout_dur,
                                    fast_socks5::client::Socks5Stream::connect_with_password(
                                        &p.address,
                                        "1.1.1.1".to_string(),
                                        80,
                                        u.clone(),
                                        pass.clone(),
                                        fast_socks5::client::Config::default(),
                                    ),
                                ).await
                            }
                            _ => {
                                tokio::time::timeout(
                                    timeout_dur,
                                    fast_socks5::client::Socks5Stream::connect(
                                        &p.address,
                                        "1.1.1.1".to_string(),
                                        80,
                                        fast_socks5::client::Config::default(),
                                    ),
                                ).await
                            }
                        };
                        let elapsed = start.elapsed();

                        match res {
                            Ok(Ok(_stream)) => {
                                success_count += 1;
                                let country_str = p.country.as_deref().unwrap_or("—");
                                let cat_str = p.proxy_category.as_deref().unwrap_or("—");
                                println!("[✓ OK]   #{:<3} {:<28} {:>5}ms (country: {}, type: {})", i + 1, p.address, elapsed.as_millis(), country_str, cat_str);
                            }
                            Ok(Err(e)) => {
                                fail_count += 1;
                                println!("[✗ FAIL] #{:<3} {:<28} SOCKS5 connect failed: {:?}", i + 1, p.address, e);
                            }
                            Err(_) => {
                                fail_count += 1;
                                println!("[✗ FAIL] #{:<3} {:<28} Timed out after {}s", i + 1, p.address, timeout);
                            }
                        }
                    }
                    println!("════════════════════════════════════════════════════════════════════════════════════════");
                    println!("Test summary: {}/{} passed, {} failed", success_count, parsed_proxies.len(), fail_count);
                    return Ok(());
                }
                SellerCmd::Stop => {
                    let daemon = seller_daemon();
                    // Stop the running daemon
                    match daemon.stop() {
                        Ok(()) => println!("Seller daemon stopped."),
                        Err(daemon_kit::DaemonError::NotRunning) => println!("Seller daemon is not running."),
                        Err(e) => anyhow::bail!("Failed to stop daemon: {e}"),
                    }
                    // Also remove the OS autostart service
                    if let Err(e) = daemon.uninstall_service() {
                        eprintln!("Warning: could not uninstall autostart service: {e}");
                    } else {
                        println!("Autostart service removed.");
                    }
                    return Ok(());
                }
                SellerCmd::Install => {
                    let daemon = seller_daemon();
                    daemon.install_service()?;
                    println!("Seller service installed. It will auto-start on boot.");
                    return Ok(());
                }
                _ => {}
            }

            // Remaining commands require auth
            if !client.is_authenticated() {
                anyhow::bail!("Not authenticated. Run 'proxybase-cli login' first.");
            }
            match cmd {
                SellerCmd::Start {
                    upstream_hosts,
                    upstream_users,
                    upstream_passes,
                    upstream_file,
                    upstream_strict,
                    no_direct,
                    volunteer,
                    foreground,
                } => {
                    let mut collected_proxies: Vec<UpstreamProxy> = Vec::new();
                    let mut seen_keys = std::collections::HashSet::new();

                    // 1. Load from file if provided
                    if let Some(ref file_path) = upstream_file {
                        let report = if file_path == std::path::Path::new("-") {
                            proxy_parser::parse_proxy_stdin()?
                        } else {
                            proxy_parser::parse_proxy_file(file_path)?
                        };

                        if !report.warnings.is_empty() {
                            for w in &report.warnings {
                                eprintln!("Warning: [Line {}] {}: {}", w.line_number, w.reason, w.raw_line);
                            }
                            if upstream_strict {
                                anyhow::bail!(
                                    "Aborting seller start due to {} invalid proxy line(s) in upstream file (strict mode)",
                                    report.warnings.len()
                                );
                            }
                        }

                        for p in report.proxies {
                            let key = format!("{}|{}", p.address.to_lowercase(), p.username.as_deref().unwrap_or(""));
                            if seen_keys.insert(key) {
                                collected_proxies.push(UpstreamProxy {
                                    address: p.address,
                                    username: p.username,
                                    password: p.password,
                                    country: p.country,
                                    proxy_category: p.proxy_category,
                                    label: p.label,
                                });
                            }
                        }
                        println!(
                            "Loaded {} upstream proxy(s) from '{}' ({} skipped comments/blank)",
                            collected_proxies.len(),
                            file_path.display(),
                            report.skipped_empty_or_comments
                        );
                    }

                    // 2. Parse inline --upstream arguments (connection strings or separate --upstream-user/pass)
                    for (i, host_arg) in upstream_hosts.iter().enumerate() {
                        // If it's a full connection string (e.g. socks5://... or host:port:user:pass)
                        if let Ok(Some(parsed)) = proxy_parser::parse_proxy_line(host_arg) {
                            if parsed.username.is_some() || parsed.password.is_some() || upstream_users.is_empty() {
                                let key = format!("{}|{}", parsed.address.to_lowercase(), parsed.username.as_deref().unwrap_or(""));
                                if seen_keys.insert(key) {
                                    collected_proxies.push(UpstreamProxy {
                                        address: parsed.address,
                                        username: parsed.username,
                                        password: parsed.password,
                                        country: parsed.country,
                                        proxy_category: parsed.proxy_category,
                                        label: parsed.label,
                                    });
                                }
                                continue;
                            }
                        }

                        // Otherwise pair with --upstream-user / --upstream-pass if available
                        let user_opt = upstream_users.get(i).cloned();
                        let pass_opt = upstream_passes.get(i).cloned();
                        let (country, proxy_category) = user_opt.as_deref()
                            .map(parse_upstream_metadata)
                            .unwrap_or((None, None));

                        let key = format!("{}|{}", host_arg.to_lowercase(), user_opt.as_deref().unwrap_or(""));
                        if seen_keys.insert(key) {
                            collected_proxies.push(UpstreamProxy {
                                address: host_arg.clone(),
                                username: user_opt,
                                password: pass_opt,
                                country,
                                proxy_category,
                                label: None,
                            });
                        }
                    }

                    let has_upstream_args = !upstream_hosts.is_empty()
                        || !upstream_users.is_empty()
                        || !upstream_passes.is_empty()
                        || upstream_file.is_some();
                    let has_explicit_args = has_upstream_args || volunteer || no_direct;

                    let config = if has_explicit_args {
                        let cfg = SellerConfig {
                            upstream_proxies: if has_upstream_args {
                                collected_proxies.iter().map(|p| UpstreamProxyConfig {
                                    address: p.address.clone(),
                                    username: p.username.clone(),
                                    password: p.password.clone(),
                                    country: p.country.clone(),
                                    proxy_category: p.proxy_category.clone(),
                                    label: p.label.clone(),
                                }).collect()
                            } else {
                                load_seller_config_or_default().upstream_proxies
                            },
                            no_direct,
                            volunteer,
                            upstream_file: upstream_file.as_ref().map(|p| p.to_string_lossy().to_string()),
                        };
                        save_seller_config(&cfg)?;
                        cfg
                    } else {
                        // Direct-only default start (or foreground service manager start):
                        // load saved config or create and persist default direct-only config.
                        let cfg = load_seller_config_or_default();
                        if !seller_config_path().exists() {
                            let _ = save_seller_config(&cfg);
                        }
                        cfg
                    };

                    let (proxies, include_direct, volunteer_mode) = {
                        let p: Vec<UpstreamProxy> = config.upstream_proxies.iter().map(|u| UpstreamProxy {
                            address: u.address.clone(),
                            username: u.username.clone(),
                            password: u.password.clone(),
                            country: u.country.clone(),
                            proxy_category: u.proxy_category.clone(),
                            label: u.label.clone(),
                        }).collect();
                        let include = !config.no_direct;
                        (p, include, config.volunteer)
                    };

                    let total_paths = proxies.len() + if include_direct { 1 } else { 0 };
                    match (include_direct, proxies.len()) {
                        (true, 0) => println!("Selling own bandwidth (direct only)"),
                        (true, n) => println!("Selling direct + reselling via {} upstream(s) — {} total paths", n, total_paths),
                        (false, n) => println!("Reselling via {} upstream(s) only (no direct)", n),
                    }
                    if volunteer_mode {
                        println!("Node Type: Volunteer (bandwidth donation — unpaid)");
                    }

                    if foreground {
                        // Write PID file so 'seller stop' can find us.
                        let pid_path = wallet_dir().join("proxybase-seller.pid");
                        if let Some(parent) = pid_path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        let _ = std::fs::write(&pid_path, std::process::id().to_string());

                        // Already inside a tokio runtime — run directly.
                        run_seller(&cli.backend, &proxies, include_direct, volunteer_mode).await;
                    } else {
                        let daemon = seller_daemon();

                        if daemon.is_running() {
                            anyhow::bail!("Seller daemon already running (PID: {}). Use 'seller stop' first, or 'seller status'.", daemon.running_pid().unwrap_or(0));
                        }

                        // Spawn a detached child process instead of forking within the
                        // tokio runtime (avoids "Cannot start a runtime from within a
                        // runtime" panic from daemonize2 + tokio interaction).
                        let exe = std::env::current_exe()
                            .context("Cannot determine current executable path")?;

                        // Open log file for the daemon's stdout/stderr
                        let log_path = wallet_dir().join("seller.log");
                        if let Some(parent) = log_path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        let log_file = std::fs::OpenOptions::new()
                            .create(true)
                            .append(true)
                            .open(&log_path)
                            .context("Cannot open seller log file")?;

                        let mut cmd = std::process::Command::new(&exe);
                        cmd.arg("seller")
                           .arg("start")
                           .arg("--foreground")
                           .arg("--backend")
                           .arg(&cli.backend)
                           .stdin(std::process::Stdio::null())
                           .stdout(log_file.try_clone()?)
                           .stderr(log_file);
                        let child = cmd.spawn()
                            .context("Failed to spawn seller daemon process")?;

                        // Write PID file ourselves (daemon-kit expects it for stop/status).
                        let pid_path = wallet_dir().join("proxybase-seller.pid");
                        if let Some(parent) = pid_path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        let _ = std::fs::write(&pid_path, child.id().to_string());

                        println!("Seller daemon started in background (PID: {}).", child.id());
                        println!("Logs: {}", log_path.display());
                    }
                }
                SellerCmd::Status => {
                    let daemon = seller_daemon();
                    if let Some(pid) = daemon.running_pid() {
                        println!("Daemon:  running (PID: {pid})");
                    } else {
                        println!("Daemon:  not running");
                    }
                    if let Ok(cfg) = load_seller_config() {
                        println!("Node Type: {}", if cfg.volunteer { "Volunteer (Bandwidth Donation - Unpaid)" } else { "Standard" });
                    }
                    match client.seller_status().await {
                        Ok(status) => {
                            if let Some(node_type) = status.get("node_type").and_then(|v| v.as_str()) {
                                println!("Backend node_type: {}", if node_type == "volunteer" { "Volunteer (Bandwidth Donation - Unpaid)" } else { "Standard" });
                            }
                            println!("{}", serde_json::to_string_pretty(&status)?)
                        }
                        Err(e) => println!("Backend: unreachable ({e})"),
                    }
                }
                SellerCmd::Stop
                | SellerCmd::Install
                | SellerCmd::ParseUpstreams { .. }
                | SellerCmd::TestUpstreams { .. } => {
                    // Handled above (before auth check)
                    unreachable!();
                }
                SellerCmd::Payout { cmd } => match cmd {
                    PayoutCmd::Create { amount, tempo_address } => {
                        let payout = client.create_payout(amount, &tempo_address).await?;
                        println!("{}", serde_json::to_string_pretty(&payout)?);
                    }
                    PayoutCmd::Status { id } => {
                        let payout = client.get_payout(&id).await?;
                        println!("{}", serde_json::to_string_pretty(&payout)?);
                    }
                    PayoutCmd::List => {
                        let payouts = client.list_payouts().await?;
                        println!("{}", serde_json::to_string_pretty(&payouts)?);
                    }
                },
            }
        }



        // --- Buyer ---
        Commands::Buyer { cmd } => {
            if !client.is_authenticated() {
                anyhow::bail!("Not authenticated. Run 'proxybase-cli login' first.");
            }
            match cmd {
                BuyerCmd::Balance => {
                    let bal = client.get_balance().await?;
                    println!("{}", serde_json::to_string_pretty(&bal)?);
                }
                BuyerCmd::Deposit { cmd } => match cmd {
                    DepositCmd::Create { amount, currency } => {
                        let deposit = client.create_deposit(amount, &currency).await?;
                        println!("{}", serde_json::to_string_pretty(&deposit)?);
                    }
                    DepositCmd::Status { id } => {
                        let dep = client.get_deposit(&id).await?;
                        println!("{}", serde_json::to_string_pretty(&dep)?);
                    }
                    DepositCmd::List => {
                        let deposits = client.list_deposits().await?;
                        println!("{}", serde_json::to_string_pretty(&deposits)?);
                    }
                },
                BuyerCmd::Transfer { amount } => {
                    let result = client.transfer(amount).await?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
            }
        }

        // --- Market ---
        Commands::Market { cmd } => {
            if !client.is_authenticated() {
                anyhow::bail!("Not authenticated. Run 'proxybase-cli login' first.");
            }
            match cmd {
                MarketCmd::Countries => {
                    let countries = client.list_countries().await?;
                    println!("{}", serde_json::to_string_pretty(&countries)?);
                }
                MarketCmd::Currencies => {
                    let currencies = client.list_currencies().await?;
                    println!("{}", serde_json::to_string_pretty(&currencies)?);
                }
                MarketCmd::Prices { country, network_type } => {
                    let pricing = client.list_pricing().await?;
                    // Filter by country/type if provided
                    if let Some(entries) = pricing.get("pricing").and_then(|p| p.as_array()) {
                        let filtered: Vec<_> = entries
                            .iter()
                            .filter(|e| {
                                let c = e.get("country").and_then(|v| v.as_str()).unwrap_or("");
                                let t = e.get("proxy_category").and_then(|v| v.as_str()).unwrap_or("");
                                c == country && t == network_type
                            })
                            .collect();
                        println!("{}", serde_json::to_string_pretty(&filtered)?);
                    } else {
                        println!("{}", serde_json::to_string_pretty(&pricing)?);
                    }
                }
                MarketCmd::Buy {
                    country,
                    network_type,
                    session_type,
                    sticky_duration: _,
                    bridge,
                    bridge_port,
                } => {
                    let session = client
                        .create_session(&country, &network_type, &session_type, None)
                        .await?;
                    println!("{}", serde_json::to_string_pretty(&session)?);
                    if let Some(sid) = session.get("session_id").and_then(|v| v.as_str()) {
                        let token = client.token.as_deref().unwrap_or("");
                        let proxy_addr = socks5_proxy_address(&cli.backend);
                        println!("Session {} opened.", sid);
                        println!("");
                        print_session_credentials(sid, token, &proxy_addr, &cli.backend);
                        if bridge {
                            let port = start_bridge_background(&cli.backend, sid, &proxy_addr, bridge_port).await?;
                            println!("");
                            println!("Local bridge (no auth): 127.0.0.1:{}", port);
                            println!("Example:");
                            println!("  curl --socks5 127.0.0.1:{} {}/v2/ip", port, cli.backend.trim_end_matches('/'));
                        }
                    }
                }
                MarketCmd::Close { session_id } => {
                    let result = client.close_session(&session_id).await?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                MarketCmd::Rotate { id } => {
                    let result = client.rotate_session(&id).await?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                MarketCmd::Sessions => {
                    let sessions = client.list_sessions().await?;
                    println!("{}", serde_json::to_string_pretty(&sessions)?);
                    let entries = sessions
                        .get("sessions")
                        .and_then(|s| s.as_array())
                        .cloned()
                        .unwrap_or_default();
                    if !entries.is_empty() {
                        let proxy_addr = socks5_proxy_address(&cli.backend);
                        let token = client.token.as_deref().unwrap_or("");
                        println!("\nActive sessions:");
                        for s in &entries {
                            let sid = s.get("session_id").and_then(|v| v.as_str()).unwrap_or("?");
                            let country = s.get("country").and_then(|v| v.as_str()).unwrap_or("?");
                            let network = s.get("network_type").and_then(|v| v.as_str()).unwrap_or("?");
                            let stype = s.get("session_type").and_then(|v| v.as_str()).unwrap_or("?");
                            println!("  {}  {} {} {}  →  {} (user {} / pass {})", sid, country, network, stype, proxy_addr, sid, token);
                        }
                        if let Some(sid) = entries.first().and_then(|s| s.get("session_id")).and_then(|v| v.as_str()) {
                            println!("\nExample:");
                            println!("  curl --socks5 {} --proxy-user {}:{} {}/v2/ip", proxy_addr, sid, token, cli.backend.trim_end_matches('/'));
                        }
                    }
                }
                MarketCmd::SessionStatus { id } => {
                    let session = client.get_session(&id).await?;
                    println!("{}", serde_json::to_string_pretty(&session)?);
                    if session.get("status").and_then(|v| v.as_str()) == Some("active") {
                        let sid = session.get("session_id").and_then(|v| v.as_str()).unwrap_or(id.as_str());
                        let token = client.token.as_deref().unwrap_or("");
                        let proxy_addr = socks5_proxy_address(&cli.backend);
                        println!("");
                        print_session_credentials(sid, token, &proxy_addr, &cli.backend);
                    }
                }
                MarketCmd::Keepalive { id, loop_ } => {
                    if loop_ {
                        let interval_secs = 300u64;
                        println!("Sending keepalive for session {} every {}s. Press Ctrl+C to stop.", id, interval_secs);
                        let mut tick = interval(Duration::from_secs(interval_secs));
                        loop {
                            tick.tick().await;
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            match client.keepalive_session(&id).await {
                                Ok(resp) => {
                                    let status = resp.get("status").and_then(|v| v.as_str()).unwrap_or("alive");
                                    println!("[{}] Session {} keepalive: {}", now, id, status);
                                }
                                Err(e) => eprintln!("[{}] Session {} keepalive failed: {}", now, id, e),
                            }
                        }
                    } else {
                        let resp = client.keepalive_session(&id).await?;
                        println!("{}", serde_json::to_string_pretty(&resp)?);
                        println!("Session {} keepalive sent successfully.", id);
                    }
                }
            }
        }

        // --- Bridge ---
        Commands::Bridge { cmd } => match cmd {
            // Stop/List are daemon-only (no auth required)
            BridgeCmd::Stop { session_id } => {
                let daemon = bridge_daemon(&session_id);
                match daemon.stop() {
                    Ok(()) => {
                        let _ = std::fs::remove_file(bridge_state_path(&session_id));
                        println!("Bridge {} stopped.", session_id);
                    }
                    Err(daemon_kit::DaemonError::NotRunning) => {
                        let _ = std::fs::remove_file(bridge_state_path(&session_id));
                        println!("Bridge {} is not running.", session_id);
                    }
                    Err(e) => anyhow::bail!("Failed to stop bridge: {e}"),
                }
            }
            BridgeCmd::List => {
                let dir = bridge_state_dir();
                let mut found = false;
                if let Ok(entries) = std::fs::read_dir(&dir) {
                    for entry in entries.flatten() {
                        let path = entry.path();
                        if path.extension().and_then(|e| e.to_str()) != Some("json") {
                            continue;
                        }
                        let state: BridgeState = match std::fs::read_to_string(&path)
                            .ok()
                            .and_then(|c| serde_json::from_str(&c).ok())
                        {
                            Some(s) => s,
                            None => continue,
                        };
                        let daemon = bridge_daemon(&state.session_id);
                        if daemon.is_running() {
                            found = true;
                            println!(
                                "Session {}: 127.0.0.1:{} (PID {})",
                                state.session_id,
                                state.port,
                                daemon.running_pid().unwrap_or(0)
                            );
                        } else {
                            // Stale state from a killed process — clean it up.
                            let _ = std::fs::remove_file(&path);
                        }
                    }
                }
                if !found {
                    println!("No bridges running.");
                }
            }
            BridgeCmd::Start { session_id, port, upstream, foreground } => {
                if !client.is_authenticated() {
                    anyhow::bail!("Not authenticated. Run 'proxybase-cli login' first.");
                }
                let upstream_addr = upstream.unwrap_or_else(|| socks5_proxy_address(&cli.backend));

                if foreground {
                    // Bound listener, report port via state file, then run
                    // until the process is killed (SIGTERM/SIGINT). The OS
                    // releases the listener socket on exit; 'bridge stop' and
                    // 'bridge list' clean up stale state.
                    let local_port = bridge::start_bridge(
                        &session_id,
                        &cli.backend,
                        &upstream_addr,
                        port,
                    )
                    .await
                    .map_err(anyhow::Error::msg)?;
                    // PID file first so 'bridge stop' can find us the moment
                    // the parent's state-file poll succeeds.
                    let _ = std::fs::write(
                        wallet_dir().join(format!("proxybase-bridge-{}.pid", session_id)),
                        std::process::id().to_string(),
                    );
                    let state = BridgeState {
                        session_id: session_id.clone(),
                        port: local_port,
                        pid: std::process::id(),
                        backend_url: cli.backend.clone(),
                        upstream_addr: upstream_addr.clone(),
                    };
                    save_bridge_state(&state)?;
                    println!(
                        "Bridge for session {} listening on 127.0.0.1:{} (no auth) — PID {}",
                        session_id,
                        local_port,
                        std::process::id()
                    );
                    std::future::pending::<()>().await;
                }

                let local_port = start_bridge_background(&cli.backend, &session_id, &upstream_addr, port).await?;
                println!("Bridge started for session {}.", session_id);
                println!("  Local SOCKS5: 127.0.0.1:{} (no auth)", local_port);
                println!("Example:");
                println!("  curl --socks5 127.0.0.1:{} {}/v2/ip", local_port, cli.backend.trim_end_matches('/'));
            }
        }

        Commands::Health => {
            let health = client.health().await?;
            println!("{}", serde_json::to_string_pretty(&health)?);
        }

        Commands::Version => {
            println!("proxybase-cli v{}", env!("CARGO_PKG_VERSION"));
        }

        Commands::Update { check } => {
            update::run_update(check).await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_upstream(addr: &str, user: &str, pass: &str) -> UpstreamProxy {
        UpstreamProxy {
            address: addr.to_string(),
            username: Some(user.to_string()),
            password: Some(pass.to_string()),
            country: None,
            proxy_category: None,
            label: None,
        }
    }

    #[test]
    fn test_build_paths_direct_only() {
        let paths = build_paths(&[], true);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, "direct");
        assert!(paths[0].1.is_none());
    }

    #[test]
    fn test_build_paths_no_direct() {
        let upstreams = vec![make_upstream("proxy1:1080", "u1", "p1")];
        let paths = build_paths(&upstreams, false);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, "upstream_0");
        assert_eq!(paths[0].1.as_ref().unwrap().address, "proxy1:1080");
    }

    #[test]
    fn test_build_paths_direct_plus_one_upstream() {
        let upstreams = vec![make_upstream("proxy1:1080", "u1", "p1")];
        let paths = build_paths(&upstreams, true);
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].0, "direct");
        assert!(paths[0].1.is_none());
        assert_eq!(paths[1].0, "upstream_0");
        assert_eq!(paths[1].1.as_ref().unwrap().address, "proxy1:1080");
    }

    #[test]
    fn test_build_paths_direct_plus_multiple_upstreams() {
        let upstreams = vec![
            make_upstream("proxy1:1080", "u1", "p1"),
            make_upstream("proxy2:1081", "u2", "p2"),
            make_upstream("proxy3:1082", "u3", "p3"),
        ];
        let paths = build_paths(&upstreams, true);
        assert_eq!(paths.len(), 4);
        assert_eq!(paths[0].0, "direct");
        assert_eq!(paths[1].0, "upstream_0");
        assert_eq!(paths[2].0, "upstream_1");
        assert_eq!(paths[3].0, "upstream_2");
        assert_eq!(paths[2].1.as_ref().unwrap().address, "proxy2:1081");
    }

    #[test]
    fn test_seller_config_default_direct_only() {
        let default_cfg = SellerConfig::default();
        assert!(default_cfg.upstream_proxies.is_empty());
        assert!(!default_cfg.no_direct, "default config must include direct path");
        assert!(!default_cfg.volunteer, "default config is non-volunteer");
    }

    #[test]
    fn test_seller_config_volunteer_defaults_false_for_legacy_configs() {
        // Configs written by CLI versions before --volunteer existed must load.
        let legacy = r#"{"upstream_proxies":[],"no_direct":false}"#;
        let config: SellerConfig = serde_json::from_str(legacy).expect("legacy seller config must parse");
        assert!(!config.volunteer, "legacy configs default to non-volunteer");

        let volunteer = r#"{"upstream_proxies":[],"no_direct":false,"volunteer":true}"#;
        let config: SellerConfig = serde_json::from_str(volunteer).expect("volunteer config must parse");
        assert!(config.volunteer);
    }

    #[test]
    fn test_parse_upstream_metadata_unchanged() {
        let (country, category) = parse_upstream_metadata("user_2930d5,type_residential,country_US,session_usresidential");
        assert_eq!(country.as_deref(), Some("US"));
        assert_eq!(category.as_deref(), Some("residential"));
    }

    #[test]
    fn test_build_paths_empty_without_direct_still_gives_direct() {
        let paths = build_paths(&[], false);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].0, "direct");
    }

    #[test]
    fn test_build_paths_only_upstreams_no_direct() {
        let upstreams = vec![
            make_upstream("a:1", "ua", "pa"),
            make_upstream("b:2", "ub", "pb"),
        ];
        let paths = build_paths(&upstreams, false);
        assert_eq!(paths.len(), 2);
        assert_eq!(paths[0].0, "upstream_0");
        assert_eq!(paths[1].0, "upstream_1");
    }

    #[test]
    fn test_upstream_proxy_preserves_credentials() {
        let upstreams = vec![make_upstream(
            "portal.anyip.io:1080",
            "user_2930d5,type_residential,country_US",
            "8198c6",
        )];
        let paths = build_paths(&upstreams, false);
        let p = paths[0].1.as_ref().unwrap();
        assert_eq!(p.address, "portal.anyip.io:1080");
        assert_eq!(p.username.as_deref(), Some("user_2930d5,type_residential,country_US"));
        assert_eq!(p.password.as_deref(), Some("8198c6"));
    }

    #[test]
    fn test_socks5_proxy_address_from_backend_url() {
        // Production backend → api.proxybase.xyz:1082
        assert_eq!(
            socks5_proxy_address("https://api.proxybase.xyz"),
            "api.proxybase.xyz:1082"
        );
        // Trailing slash tolerated
        assert_eq!(
            socks5_proxy_address("https://api.proxybase.xyz/"),
            "api.proxybase.xyz:1082"
        );
        // Dev backend → localhost:1082
        assert_eq!(
            socks5_proxy_address("http://localhost:8080"),
            "localhost:1082"
        );
        // No scheme → treated as http://
        assert_eq!(
            socks5_proxy_address("api.proxybase.xyz"),
            "api.proxybase.xyz:1082"
        );
        // Garbage → safe default
        assert_eq!(socks5_proxy_address(""), "127.0.0.1:1082");
    }

    #[test]
    fn test_bridge_state_roundtrip() {
        let state = BridgeState {
            session_id: "abc-123".to_string(),
            port: 4321,
            pid: 999,
            backend_url: "https://api.proxybase.xyz".to_string(),
            upstream_addr: "api.proxybase.xyz:1082".to_string(),
        };
        let json = serde_json::to_string(&state).unwrap();
        let parsed: BridgeState = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.session_id, "abc-123");
        assert_eq!(parsed.port, 4321);
        assert_eq!(parsed.pid, 999);
        assert_eq!(parsed.upstream_addr, "api.proxybase.xyz:1082");
    }

    #[test]
    fn test_market_keepalive_parsing() {
        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "market",
            "keepalive",
            "--id",
            "session-1",
        ])
        .unwrap();
        match cli.command {
            Commands::Market { cmd: MarketCmd::Keepalive { id, loop_ } } => {
                assert_eq!(id, "session-1");
                assert!(!loop_);
            }
            _ => panic!("expected market keepalive"),
        }

        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "market",
            "keepalive",
            "--id",
            "session-1",
            "--loop",
        ])
        .unwrap();
        match cli.command {
            Commands::Market { cmd: MarketCmd::Keepalive { id, loop_ } } => {
                assert_eq!(id, "session-1");
                assert!(loop_);
            }
            _ => panic!("expected market keepalive --loop"),
        }
    }

    #[test]
    fn test_bridge_start_parsing() {
        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "bridge",
            "start",
            "session-1",
            "--port",
            "8080",
            "--foreground",
            "--upstream",
            "api.proxybase.xyz:1082",
        ])
        .unwrap();
        match cli.command {
            Commands::Bridge { cmd: BridgeCmd::Start { session_id, port, upstream, foreground } } => {
                assert_eq!(session_id, "session-1");
                assert_eq!(port, Some(8080));
                assert!(foreground);
                assert_eq!(upstream.as_deref(), Some("api.proxybase.xyz:1082"));
            }
            _ => panic!("expected bridge start"),
        }
    }

    #[test]
    fn test_market_buy_bridge_flag_parsing() {
        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "market",
            "buy",
            "--country",
            "US",
            "--network-type",
            "residential",
            "--bridge",
            "--bridge-port",
            "9000",
        ])
        .unwrap();
        match cli.command {
            Commands::Market { cmd: MarketCmd::Buy { bridge, bridge_port, .. } } => {
                assert!(bridge);
                assert_eq!(bridge_port, Some(9000));
            }
            _ => panic!("expected market buy"),
        }

        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "market",
            "buy",
            "--country",
            "US",
            "--network-type",
            "residential",
        ])
        .unwrap();
        match cli.command {
            Commands::Market { cmd: MarketCmd::Buy { bridge, bridge_port, .. } } => {
                assert!(!bridge);
                assert_eq!(bridge_port, None);
            }
            _ => panic!("expected market buy"),
        }
    }

    #[tokio::test]
    async fn test_keepalive_session_success_and_error() {
        // Minimal HTTP server: 200 for the alive session's keepalive endpoint,
        // 410 Gone for anything else.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = tokio::io::AsyncReadExt::read(&mut sock, &mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let first_line = req.lines().next().unwrap_or("");
                    let (status, body) =
                        if first_line.starts_with("POST /v2/sessions/alive-session/keepalive") {
                            ("200 OK", r#"{"session_id":"alive-session","status":"alive"}"#)
                        } else {
                            ("410 Gone", r#"{"error":"Session is no longer active"}"#)
                        };
                    let resp = format!(
                        "HTTP/1.1 {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        status,
                        body.len(),
                        body
                    );
                    let _ = tokio::io::AsyncWriteExt::write_all(&mut sock, resp.as_bytes()).await;
                });
            }
        });

        let client = BackendClient::new(&format!("http://{}", addr));

        // Success path
        let resp = client
            .keepalive_session("alive-session")
            .await
            .expect("keepalive must succeed");
        assert_eq!(resp.get("status").and_then(|v| v.as_str()), Some("alive"));

        // Error path: a 410 surfaces as an anyhow error carrying the status
        let err = client
            .keepalive_session("dead-session")
            .await
            .expect_err("keepalive of an inactive session must fail");
        assert!(err.to_string().contains("410"), "error was: {err}");

        server.abort();
    }

    #[test]
    fn test_base64_encode_decode_roundtrip() {
        let original = b"GET /v2/ip HTTP/1.1\r\nHost: api.proxybase.xyz\r\nConnection: close\r\n\r\n";
        let encoded = base64_encode(original);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_base64_decode_empty() {
        assert_eq!(base64_decode(""), Some(vec![]));
    }

    #[test]
    fn test_seller_config_path() {
        let path = seller_config_path();
        assert!(path.ends_with("seller_config.json"));
    }

    #[test]
    fn test_wallet_dir() {
        let dir = wallet_dir();
        assert!(dir.ends_with(".proxybase"));
    }

    #[test]
    fn test_wallet_import_hd_index_flag() {
        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "wallet",
            "import",
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
        ])
        .unwrap();
        match cli.command {
            Commands::Wallet { cmd: WalletCmd::Import { hd_index, .. } } => {
                // No flag → legacy raw-seed import (backward compatible)
                assert!(hd_index.is_none());
            }
            _ => panic!("expected wallet import"),
        }

        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "wallet",
            "import",
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            "--hd-index",
            "7",
        ])
        .unwrap();
        match cli.command {
            Commands::Wallet { cmd: WalletCmd::Import { hd_index, .. } } => {
                assert_eq!(hd_index, Some(7));
            }
            _ => panic!("expected wallet import"),
        }
    }

    #[test]
    fn test_wallet_sweep_parsing() {
        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "wallet",
            "sweep",
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            "--count",
            "10",
            "--target-tempo",
            "0x1234567890abcdef",
        ])
        .unwrap();
        match cli.command {
            Commands::Wallet { cmd: WalletCmd::Sweep { start_index, count, target_tempo, min_threshold, .. } } => {
                assert_eq!(start_index, 0);
                assert_eq!(count, 10);
                assert_eq!(target_tempo, "0x1234567890abcdef");
                assert_eq!(min_threshold, 1_000_000);
            }
            _ => panic!("expected wallet sweep"),
        }

        let cli = Cli::try_parse_from([
            "proxybase-cli",
            "wallet",
            "sweep",
            "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
            "--start-index",
            "5",
            "--count",
            "3",
            "--target-tempo",
            "0xabc",
            "--min-threshold",
            "500",
        ])
        .unwrap();
        match cli.command {
            Commands::Wallet { cmd: WalletCmd::Sweep { start_index, count, min_threshold, .. } } => {
                assert_eq!(start_index, 5);
                assert_eq!(count, 3);
                assert_eq!(min_threshold, 500);
            }
            _ => panic!("expected wallet sweep"),
        }
    }

    // ── Relay loop tests (CLOSE_WAIT regression) ──

    /// Core relay loop extracted for testing. Races TCP↔WS directions.
    /// Returns when either direction completes. Both tcp_r + tcp_w are dropped
    /// on return — no CLOSE_WAIT.
    async fn relay_loop(
        mut tcp_r: impl tokio::io::AsyncRead + Unpin,
        mut tcp_w: impl tokio::io::AsyncWrite + Unpin,
        relay_tx: &tokio::sync::mpsc::UnboundedSender<Message>,
        mut tcp_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
        sid: &str,
    ) {
        let tx2 = relay_tx.clone();
        let sid2 = sid.to_string();
        let tcp_to_ws = async {
            let mut buf = vec![0u8; 8192];
            loop {
                match tokio::io::AsyncReadExt::read(&mut tcp_r, &mut buf).await {
                    Ok(0) => { eprintln!("[RELAY {}] TCP closed", sid2); break; }
                    Ok(n) => {
                        let enc = base64_encode(&buf[..n]);
                        let m = serde_json::json!({"type":"relay_response","session_id":&sid2,"data":enc});
                        if tx2.send(Message::Text(serde_json::to_string(&m).unwrap_or_default())).is_err() {
                            break;
                        }
                    }
                    Err(e) => { eprintln!("[RELAY {}] Read error: {}", sid2, e); break; }
                }
            }
        };

        let sid3 = sid.to_string();
        let ws_to_tcp = async {
            while let Some(data) = tcp_rx.recv().await {
                if tokio::io::AsyncWriteExt::write_all(&mut tcp_w, &data).await.is_err() {
                    eprintln!("[RELAY {}] Write failed", sid3);
                    break;
                }
            }
        };

        let relay_task = async {
            tokio::select! {
                _ = tcp_to_ws => {}
                _ = ws_to_tcp => {}
            }
        };
        if tokio::time::timeout(tokio::time::Duration::from_secs(60), relay_task).await.is_err() {
            eprintln!("[RELAY {}] Inactivity timeout — closing", sid);
        }
        eprintln!("[RELAY {}] Closed", sid);
    }

    /// TCP EOF causes relay to exit — no hang, no CLOSE_WAIT.
    /// Uses a local TCP listener so EOF behavior matches production exactly.
    #[tokio::test]
    async fn test_relay_tcp_eof_exits_cleanly() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (relay_tx, mut relay_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let (_tcp_tx, tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        let handle = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (client_r, client_w) = tokio::io::split(stream);
            relay_loop(client_r, client_w, &relay_tx, tcp_rx, "test-eof").await;
        });

        // Accept and immediately close → client sees RST/EOF
        let (accepted, _) = listener.accept().await.unwrap();
        drop(accepted); // close server side → client sees EOF

        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("relay must not hang on TCP EOF")
            .expect("relay must not panic");

        assert!(relay_rx.recv().await.is_none());
    }

    /// Dropping tcp_tx (closing WS→TCP channel) causes relay to exit.
    #[tokio::test]
    async fn test_relay_channel_close_exits_cleanly() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (relay_tx, mut relay_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let (tcp_tx, tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        let handle = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (client_r, client_w) = tokio::io::split(stream);
            relay_loop(client_r, client_w, &relay_tx, tcp_rx, "test-channel").await;
        });

        // Accept connection but don't close it
        let _accepted = listener.accept().await.unwrap();
        // Close the WS→TCP channel → ws_to_tcp exits → select fires
        drop(tcp_tx);

        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("relay must not hang on channel close")
            .expect("relay must not panic");

        assert!(relay_rx.recv().await.is_none());
    }

    /// Data sent through tcp_tx arrives at the accepted TCP stream.
    #[tokio::test]
    async fn test_relay_data_flows_to_tcp() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (relay_tx, _relay_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let (tcp_tx, tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        let handle = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (client_r, client_w) = tokio::io::split(stream);
            relay_loop(client_r, client_w, &relay_tx, tcp_rx, "test-data").await;
        });

        let (mut accepted, _) = listener.accept().await.unwrap();

        tcp_tx.send(b"Hello, TCP!".to_vec()).unwrap();
        drop(tcp_tx); // close channel so relay exits

        let mut buf = vec![0u8; 64];
        let n = tokio::io::AsyncReadExt::read(&mut accepted, &mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"Hello, TCP!");

        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("relay hang")
            .expect("relay panic");
    }

    /// Both directions close simultaneously — relay must not deadlock.
    #[tokio::test]
    async fn test_relay_both_directions_close_together() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (relay_tx, mut relay_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let (tcp_tx, tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

        let handle = tokio::spawn(async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (client_r, client_w) = tokio::io::split(stream);
            relay_loop(client_r, client_w, &relay_tx, tcp_rx, "test-both").await;
        });

        let (accepted, _) = listener.accept().await.unwrap();
        drop(accepted); // close TCP → tcp_to_ws sees EOF
        drop(tcp_tx);   // close channel → ws_to_tcp sees None

        tokio::time::timeout(std::time::Duration::from_secs(3), handle)
            .await
            .expect("relay must not deadlock")
            .expect("relay must not panic");

        assert!(relay_rx.recv().await.is_none());
    }
    #[test]
    fn session_token_file_is_private() {
        let dir = std::env::temp_dir().join(format!(
            "proxybase-token-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        BackendClient::save_token_in(&dir, "secret-token");
        let path = dir.join("session_token");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "secret-token");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = |p: &std::path::Path| {
                std::fs::metadata(p).unwrap().permissions().mode() & 0o777
            };
            assert_eq!(mode(&path), 0o600);
            assert_eq!(mode(&dir), 0o700);

            // Self-heal: a leftover world-readable token gets tightened on load.
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            BackendClient::harden_token_file(&path);
            assert_eq!(mode(&path), 0o600);
        }

        std::fs::remove_dir_all(&dir).ok();
    }
}

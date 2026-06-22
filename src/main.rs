use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
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
    /// Backend health check
    Health,
    /// Print version
    Version,
}

#[derive(Subcommand)]
enum WalletCmd {
    /// Generate a new wallet
    Create,
    /// Import an existing mnemonic
    Import { phrase: String },
    /// Show wallet info
    Info,
}

#[derive(Subcommand)]
enum SellerCmd {
    /// Start selling bandwidth (daemonizes by default, use --foreground to keep in terminal).
    /// Add --upstream to resell external proxies simultaneously.
    Start {
        /// Upstream proxy host:port (repeatable, pairs with --upstream-user/--upstream-pass)
        #[arg(long = "upstream")]
        upstream_hosts: Vec<String>,
        /// Upstream proxy username (repeatable, pairs with --upstream)
        #[arg(long = "upstream-user")]
        upstream_users: Vec<String>,
        /// Upstream proxy password (repeatable, pairs with --upstream)
        #[arg(long = "upstream-pass")]
        upstream_passes: Vec<String>,
        /// Disable direct (own bandwidth). Only use --upstream proxies.
        #[arg(long)]
        no_direct: bool,
        /// Run in foreground (don't daemonize). Used internally by the service manager.
        #[arg(long)]
        foreground: bool,
    },
    /// Stop the background seller daemon
    Stop,
    /// Show seller status (daemon + backend stats)
    Status,
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
#[derive(Clone)]
struct UpstreamProxy {
    address: String,
    username: String,
    password: String,
    /// Parsed from --upstream-user (e.g. "type_residential" → "residential").
    country: Option<String>,
    proxy_category: Option<String>,
}

/// Parse country and proxy_category from an upstream username.
/// Format: `user_2930d5,type_residential,country_US,session_usresidential`
/// Extracts: country="US", proxy_category="residential"
fn parse_upstream_metadata(username: &str) -> (Option<String>, Option<String>) {
    let mut country = None;
    let mut category = None;
    for part in username.split(',') {
        if let Some(c) = part.strip_prefix("country_") {
            country = Some(c.to_uppercase());
        } else if let Some(net) = part.strip_prefix("type_") {
            category = Some(net.to_lowercase());
        }
    }
    (country, category)
}

// ---------------------------------------------------------------------------
// Seller config persistence (for daemon / reboot survival)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct SellerConfig {
    upstream_proxies: Vec<UpstreamProxyConfig>,
    no_direct: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct UpstreamProxyConfig {
    address: String,
    username: String,
    password: String,
    country: Option<String>,
    proxy_category: Option<String>,
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
    let content = std::fs::read_to_string(&path)
        .context("No saved seller config. Run 'seller start --upstream ...' first to save configuration.")?;
    Ok(serde_json::from_str(&content)?)
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
async fn run_seller(backend_url: &str, proxies: &[UpstreamProxy], include_direct: bool) {
    let client = BackendClient::new(backend_url);
    if !client.is_authenticated() {
        eprintln!("[seller] Not authenticated. Run 'proxybase-cli login' first.");
        return;
    }
    let _ = client.register_seller().await;

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
    },
    /// Close a session
    Close {
        session_id: String,
    },
    /// List active/past sessions
    Sessions,
    /// Get a single session's details
    SessionStatus {
        /// Session ID
        #[arg(long)]
        id: String,
    },
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
        dirs::home_dir()
            .unwrap_or_default()
            .join(".proxybase")
            .join("session_token")
    }

    fn load_token() -> Option<String> {
        std::fs::read_to_string(Self::token_path()).ok()
    }

    fn save_token(token: &str) {
        let path = Self::token_path();
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let _ = std::fs::write(&path, token);
    }

    fn bearer(&self) -> String {
        format!("Bearer {}", self.token.as_deref().unwrap_or(""))
    }

    fn is_authenticated(&self) -> bool {
        self.token.is_some()
    }

    pub fn token(&self) -> Option<&str> {
        self.token.as_deref()
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

    async fn register_seller(&self) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/v2/seller/register", self.base_url))
            .header("Authorization", self.bearer())
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

    async fn get_session(&self, session_id: &str) -> Result<serde_json::Value> {
        let resp = self.http.get(format!("{}/v2/sessions/{}", self.base_url, session_id))
            .header("Authorization", self.bearer()).send().await?;
        Ok(resp.json().await?)
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
            eprintln!("[RELAY {}] Using upstream proxy {} (user={})", sid, proxy.address, proxy.username);
            match fast_socks5::client::Socks5Stream::connect_with_password(
                &proxy.address,
                target_dest.to_string(),
                target_port,
                proxy.username.clone(),
                proxy.password.clone(),
                fast_socks5::client::Config::default(),
            ).await {
                Ok(stream) => {
                    let (r, w) = tokio::io::split(stream);
                    Ok((Box::new(r), Box::new(w)))
                }
                Err(e) => Err(anyhow::anyhow!("SOCKS5 upstream connect failed: {:?}", e)),
            }
        }
        None => {
            eprintln!("[RELAY {}] Direct connect (no upstream proxy)", sid);
            match tokio::net::TcpStream::connect(format!("{}:{}", target_ip, target_port)).await {
                Ok(tcp) => {
                    let (r, w) = tokio::io::split(tcp);
                    Ok((Box::new(r), Box::new(w)))
                }
                Err(e) => Err(anyhow::anyhow!("TCP connect failed: {}", e)),
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

    // TCP reads → WS relay_response
    let tx2 = relay_tx.clone();
    let sid2 = sid.clone();
    let tcp_to_ws = tokio::spawn(async move {
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
    });

    // WS relay_data → TCP writes
    while let Some(data) = tcp_rx.recv().await {
        if tokio::io::AsyncWriteExt::write_all(&mut tcp_w, &data).await.is_err() {
            eprintln!("[RELAY {}] Write failed", sid);
            break;
        }
    }
    tcp_to_ws.abort();
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
        let ws_url = format!(
            "{}/v2/ws/seller?token={}",
            backend_url.replace("https://", "wss://").replace("http://", "ws://"),
            current_token
        );

        eprintln!("[{}] Connecting (backoff={}s)...", path_id, backoff_secs);
        match try_single_path_connection(&ws_url, &current_token, &path_id, upstream.as_ref()).await {
            Ok(()) => {
                backoff_secs = 1;
                eprintln!("[{}] Disconnected. Reconnecting...", path_id);
            }
            Err(e) if e.to_string().contains("AUTH_EXPIRED") => {
                eprintln!("[{}] Session token expired. Re-authenticating...", path_id);
                match re_authenticate_single(backend_url).await {
                    Ok(new_token) => {
                        *token.lock().await = new_token;
                        eprintln!("[{}] Re-authenticated successfully.", path_id);
                        backoff_secs = 1;
                    }
                    Err(auth_err) => {
                        eprintln!("[{}] Re-auth failed: {}. Retrying in {}s...", path_id, auth_err, backoff_secs);
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(60);
                    }
                }
            }
            Err(e) => {
                eprintln!("[{}] Connection failed: {}. Retrying in {}s...", path_id, e, backoff_secs);
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
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
    let (ws, _resp) = connect_async(ws_url).await.context("Failed to connect WebSocket")?;
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

    let upstream = upstream.cloned();
    let mut ping_tick = interval(Duration::from_secs(30));
    let mut heartbeat_tick = interval(Duration::from_secs(60));
    let mut stream_count: u32 = 0;

    loop {
        tokio::select! {
            _ = ping_tick.tick() => { let _ = relay_tx.send(Message::Ping(vec![].into())); }
            _ = heartbeat_tick.tick() => {
                let hb = serde_json::json!({"type":"heartbeat","active_streams":stream_count,"version":"0.1.0","conn_id":conn_id});
                let _ = relay_tx.send(Message::Text(serde_json::to_string(&hb).unwrap_or_default()));
            }
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Ping(d))) => { let _ = relay_tx.send(Message::Pong(d)); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(p) = serde_json::from_str::<serde_json::Value>(&text) {
                            if p.get("error").and_then(|v| v.as_str()) == Some("invalid_token") {
                                return Err(anyhow::anyhow!("AUTH_EXPIRED"));
                            }
                            match p.get("type").and_then(|v| v.as_str()) {
                                Some("relay_data") => {
                                    if let Some(enc) = p.get("data").and_then(|v| v.as_str()) {
                                        if let Some(dec) = base64_decode(enc) {
                                            let streams = active.lock().await;
                                            let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                                            if let Some(s) = streams.get(sid) { let _ = s.send(dec); }
                                            else { for (_, s) in streams.iter() { let _ = s.send(dec.clone()); break; } }
                                        }
                                    }
                                }
                                Some("stream_open") => {
                                    let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("?").to_string();
                                    let tip = p.get("target_ip").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string();
                                    let tport = p.get("target_port").and_then(|v| v.as_u64()).unwrap_or(443) as u16;
                                    let thost = p.get("target_host").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    let dest = thost.unwrap_or_else(|| tip.clone());
                                    eprintln!("[{}] STREAM {} → {}:{} (direct_ip={})", path_id, sid, dest, tport, tip);

                                    let streams = active.clone();
                                    let tx = relay_tx.clone();
                                    let up = upstream.clone();
                                    stream_count += 1;

                                    let (tcp_tx, tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
                                    streams.lock().await.insert(sid.clone(), tcp_tx);

                                    tokio::spawn(async move {
                                        run_stream_relay(&dest, &tip, tport, up.as_ref(), &tx, tcp_rx, &sid).await;
                                        streams.lock().await.remove(&sid);
                                    });
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
    Ok(())
}

async fn run_seller_ws_loop(
    mut client: BackendClient,
    upstreams: Vec<UpstreamProxy>,
    include_direct: bool,
) -> Result<()> {
    let pool: Vec<Option<UpstreamProxy>> = {
        let mut v: Vec<Option<UpstreamProxy>> = Vec::new();
        if include_direct || upstreams.is_empty() { v.push(None); }
        for u in upstreams { v.push(Some(u)); }
        v
    };
    let pool = std::sync::Arc::new(pool);

    let mut backoff_secs = 1u64;
    loop {
        let token = client.token.as_deref().unwrap_or("").to_string();
        let ws_url = format!(
            "{}/v2/ws/seller?token={}",
            client.base_url.replace("http://", "ws://").replace("https://", "wss://"),
            token
        );

        println!("Connecting to {} (backoff={}s)...", ws_url, backoff_secs);
        match try_seller_connection(&ws_url, &token, pool.clone()).await {
            Ok(()) => {
                backoff_secs = 1;
                eprintln!("Disconnected. Reconnecting...");
            }
            Err(e) if e.to_string().contains("AUTH_EXPIRED") => {
                eprintln!("Session token expired or invalid. Re-authenticating...");
                match re_authenticate(&mut client).await {
                    Ok(()) => {
                        eprintln!("Re-authenticated successfully.");
                        backoff_secs = 1;
                    }
                    Err(auth_err) => {
                        eprintln!("Re-authentication failed: {}. Retrying in {}s...", auth_err, backoff_secs);
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                        backoff_secs = (backoff_secs * 2).min(60);
                    }
                }
            }
            Err(e) => {
                eprintln!("Connection failed: {}. Retrying in {}s...", e, backoff_secs);
                tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                backoff_secs = (backoff_secs * 2).min(60);
            }
        }
    }
}

async fn re_authenticate(client: &mut BackendClient) -> Result<()> {
    let wm = load_wallet()
        .context("No wallet found. Cannot re-authenticate.")?;
    let address = wm.address()
        .ok_or_else(|| anyhow::anyhow!("Wallet not loaded"))?;

    let challenge = client.auth_challenge(address).await?;
    let message = format!("{}:{}:{}", address, challenge.nonce, challenge.timestamp);
    let signature = wm.sign(message.as_bytes())?;
    let sig_hex = hex::encode(&signature);
    let public_key_hex = wm.public_key_hex()
        .ok_or_else(|| anyhow::anyhow!("Cannot get public key"))?;

    let auth = client.auth_verify(&public_key_hex, &challenge.nonce, &challenge.timestamp, &sig_hex).await?;
    BackendClient::save_token(&auth.session_token);
    client.token = Some(auth.session_token);
    Ok(())
}

async fn try_seller_connection(
    ws_url: &str,
    token: &str,
    pool: std::sync::Arc<Vec<Option<UpstreamProxy>>>,
) -> Result<()> {
    let (ws, _resp) = connect_async(ws_url).await.context("Failed to connect WebSocket")?;
    let conn_id = uuid::Uuid::new_v4().to_string();
    println!("Connected (conn={}). Press Ctrl+C to stop.", &conn_id[..8]);

    let (mut ws_sink, mut ws_stream) = ws.split();

    // Send auth token as the first message (required by standalone WS listener on port 1081).
    ws_sink
        .send(Message::Text(token.to_string()))
        .await
        .context("Failed to send auth token")?;

    let (relay_tx, mut relay_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
    let active: std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, tokio::sync::mpsc::UnboundedSender<Vec<u8>>>>> = Default::default();

    let relay_drain = tokio::spawn(async move {
        while let Some(msg) = relay_rx.recv().await {
            if ws_sink.send(msg).await.is_err() { break; }
        }
    });

    let mut ping_tick = interval(Duration::from_secs(30));
    let mut heartbeat_tick = interval(Duration::from_secs(60));
    let mut stream_count: u32 = 0;

    loop {
        tokio::select! {
            _ = ping_tick.tick() => { let _ = relay_tx.send(Message::Ping(vec![].into())); }
            _ = heartbeat_tick.tick() => {
                let hb = serde_json::json!({"type":"heartbeat","active_streams":stream_count,"version":"0.1.0","conn_id":conn_id});
                let _ = relay_tx.send(Message::Text(serde_json::to_string(&hb).unwrap_or_default()));
            }
            msg = ws_stream.next() => {
                match msg {
                    Some(Ok(Message::Ping(d))) => { let _ = relay_tx.send(Message::Pong(d)); }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        if let Ok(p) = serde_json::from_str::<serde_json::Value>(&text) {
                            // Detect auth-token rejection from the server
                            if p.get("error").and_then(|v| v.as_str()) == Some("invalid_token") {
                                return Err(anyhow::anyhow!("AUTH_EXPIRED"));
                            }
                            match p.get("type").and_then(|v| v.as_str()) {
                                Some("relay_data") => {
                                    if let Some(enc) = p.get("data").and_then(|v| v.as_str()) {
                                        if let Some(dec) = base64_decode(enc) {
                                            let streams = active.lock().await;
                                            let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("");
                                            if let Some(s) = streams.get(sid) { let _ = s.send(dec); }
                                            else { for (_, s) in streams.iter() { let _ = s.send(dec.clone()); break; } }
                                        }
                                    }
                                }
                                Some("stream_open") => {
                                    let sid = p.get("session_id").and_then(|v| v.as_str()).unwrap_or("?").to_string();
                                    let tip = p.get("target_ip").and_then(|v| v.as_str()).unwrap_or("127.0.0.1").to_string();
                                    let tport = p.get("target_port").and_then(|v| v.as_u64()).unwrap_or(443) as u16;
                                    let thost = p.get("target_host").and_then(|v| v.as_str()).map(|s| s.to_string());
                                    let dest = thost.unwrap_or_else(|| tip.clone());
                                    // Backend controls routing: use route_index if provided, else hash session_id
                                    let route_idx = p.get("route_index")
                                        .and_then(|v| v.as_u64())
                                        .map(|i| i as usize);
                                    eprintln!("[STREAM] {} → {}:{} (direct_ip={})", sid, dest, tport, tip);

                                    let streams = active.clone();
                                    let tx = relay_tx.clone();
                                    let idx = route_idx.unwrap_or_else(|| {
                                        let mut h: usize = 0;
                                        for b in sid.as_bytes() { h = h.wrapping_mul(31).wrapping_add(*b as usize); }
                                        h % pool.len()
                                    });
                                    let up = pool[idx].clone();
                                    stream_count += 1;

                                    let (tcp_tx, mut tcp_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
                                    streams.lock().await.insert(sid.clone(), tcp_tx);

                                    tokio::spawn(async move {
                                        let up_ref: Option<&UpstreamProxy> = up.as_ref();
                                        run_stream_relay(&dest, &tip, tport, up_ref, &tx, tcp_rx, &sid).await;
                                        streams.lock().await.remove(&sid);
                                    });
                                }
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => { eprintln!("Backend closed connection"); break; }
                    Some(Err(e)) => { eprintln!("WS error: {}", e); break; }
                    _ => {}
                }
            }
        }
    }
    relay_drain.abort();
    Ok(())
}

fn wallet_dir() -> std::path::PathBuf {
    dirs::home_dir()
        .unwrap_or_default()
        .join(".proxybase")
}

fn load_wallet() -> Result<libproxybase::WalletManager> {
    let mut wm = libproxybase::WalletManager::new(wallet_dir())?;
    wm.load("")?;
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
    println!("Session token saved.");

    Ok(auth.session_token)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = BackendClient::new(&cli.backend);

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
            WalletCmd::Import { phrase } => {
                let mut wm = libproxybase::WalletManager::new(wallet_dir())?;
                wm.import(&phrase, "")?;
                println!("Wallet imported successfully!");
                println!("Address: {}", wm.address().unwrap_or("unknown"));
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
            // Daemon-only commands (no auth required)
            match cmd {
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
                SellerCmd::Start { upstream_hosts, upstream_users, upstream_passes, no_direct, foreground } => {
                    // Build proxy list from args
                    let n = upstream_hosts.len().min(upstream_users.len()).min(upstream_passes.len());
                    let proxies: Vec<UpstreamProxy> = (0..n).map(|i| {
                        let (country, proxy_category) = parse_upstream_metadata(&upstream_users[i]);
                        UpstreamProxy {
                            address: upstream_hosts[i].clone(),
                            username: upstream_users[i].clone(),
                            password: upstream_passes[i].clone(),
                            country,
                            proxy_category,
                        }
                    }).collect();
                    if upstream_hosts.len() != n || upstream_users.len() != n || upstream_passes.len() != n {
                        eprintln!("Warning: --upstream, --upstream-user, --upstream-pass counts differ. Using {} proxy(s).", n);
                    }

                    // Save config if upstream args provided (so daemon can restart without args)
                    let has_upstream_args = !upstream_hosts.is_empty() || !upstream_users.is_empty() || !upstream_passes.is_empty();
                    if has_upstream_args {
                        let config = SellerConfig {
                            upstream_proxies: proxies.iter().map(|p| UpstreamProxyConfig {
                                address: p.address.clone(),
                                username: p.username.clone(),
                                password: p.password.clone(),
                                country: p.country.clone(),
                                proxy_category: p.proxy_category.clone(),
                            }).collect(),
                            no_direct,
                        };
                        save_seller_config(&config)?;
                    }

                    let (proxies, include_direct) = if foreground {
                        // Service manager flow: load saved config
                        let config = load_seller_config()?;
                        let p: Vec<UpstreamProxy> = config.upstream_proxies.iter().map(|u| UpstreamProxy {
                            address: u.address.clone(),
                            username: u.username.clone(),
                            password: u.password.clone(),
                            country: u.country.clone(),
                            proxy_category: u.proxy_category.clone(),
                        }).collect();
                        let include = !config.no_direct;
                        (p, include)
                    } else {
                        (proxies, !no_direct)
                    };

                    let total_paths = proxies.len() + if include_direct { 1 } else { 0 };
                    match (include_direct, proxies.len()) {
                        (true, 0) => println!("Selling own bandwidth (direct only)"),
                        (true, n) => println!("Selling direct + reselling via {} upstream(s) — {} total paths", n, total_paths),
                        (false, n) => println!("Reselling via {} upstream(s) only (no direct)", n),
                    }

                    if foreground {
                        // Write PID file so 'seller stop' can find us.
                        let pid_path = wallet_dir().join("proxybase-seller.pid");
                        if let Some(parent) = pid_path.parent() {
                            let _ = std::fs::create_dir_all(parent);
                        }
                        let _ = std::fs::write(&pid_path, std::process::id().to_string());

                        // Already inside a tokio runtime — run directly.
                        run_seller(&cli.backend, &proxies, include_direct).await;
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
                    match client.seller_status().await {
                        Ok(status) => println!("{}", serde_json::to_string_pretty(&status)?),
                        Err(e) => println!("Backend: unreachable ({e})"),
                    }
                }
                SellerCmd::Stop | SellerCmd::Install => {
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
                } => {
                    let session = client
                        .create_session(&country, &network_type, &session_type, None)
                        .await?;
                    println!("{}", serde_json::to_string_pretty(&session)?);
                    if let Some(sid) = session.get("session_id").and_then(|v| v.as_str()) {
                        let token = client.token.as_deref().unwrap_or("");
                        println!("Session {} opened.", sid);
                        println!("");
                        println!("SOCKS5 proxy: 127.0.0.1:1082");
                        println!("  Username: {}", sid);
                        println!("  Password: {}", token);
                        println!("");
                        println!("Example:");
                        println!("  curl --socks5 127.0.0.1:1082 --proxy-user {}:{} https://httpbin.org/ip", sid, token);
                    }
                }
                MarketCmd::Close { session_id } => {
                    let result = client.close_session(&session_id).await?;
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                MarketCmd::Sessions => {
                    let sessions = client.list_sessions().await?;
                    println!("{}", serde_json::to_string_pretty(&sessions)?);
                }
                MarketCmd::SessionStatus { id } => {
                    let session = client.get_session(&id).await?;
                    println!("{}", serde_json::to_string_pretty(&session)?);
                }
            }
        }

        Commands::Health => {
            let health = client.health().await?;
            println!("{}", serde_json::to_string_pretty(&health)?);
        }

        Commands::Version => {
            println!("proxybase-cli v{}", env!("CARGO_PKG_VERSION"));
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
            username: user.to_string(),
            password: pass.to_string(),
            country: None,
            proxy_category: None,
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
        assert_eq!(p.username, "user_2930d5,type_residential,country_US");
        assert_eq!(p.password, "8198c6");
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
}

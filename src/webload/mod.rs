pub mod db;
pub mod ingest;
pub mod relay;
pub mod server;

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

pub use db::WebloadDb;
pub use ingest::IngestionManager;
pub use relay::ActiveRouteTable;

/// Configuration options for the webload server.
#[derive(Debug, Clone)]
pub struct WebloadOptions {
    pub bind: String,
    pub port: u16,
    pub db_path: PathBuf,
    pub initial_file: Option<PathBuf>,
    pub no_open: bool,
    pub start_seller: bool,
    pub backend_url: String,
}

/// Run the embedded webload HTTP server and bulk proxy manager.
pub async fn run_webload_server(opts: WebloadOptions) -> Result<()> {
    eprintln!("[webload] Initializing SQLite storage at: {}", opts.db_path.display());
    let db = WebloadDb::new(&opts.db_path)
        .with_context(|| format!("Failed to initialize database at {}", opts.db_path.display()))?;

    let route_table = ActiveRouteTable::new();

    // Populate hot routing table with existing active proxies
    match db.get_active_proxies() {
        Ok(active) => {
            let count = active.len();
            route_table.populate(active).await;
            eprintln!("[webload] Loaded {} active upstream proxy route(s) into memory", count);
        }
        Err(e) => {
            eprintln!("[webload] Warning: Failed to populate active routes: {:#}", e);
        }
    }

    let (progress_tx, _) = broadcast::channel(100);
    let ingest_mgr = Arc::new(IngestionManager::new(
        db.clone(),
        route_table.clone(),
        progress_tx.clone(),
    ));

    let active_cancellations = Arc::new(Mutex::new(HashMap::new()));

    // Queue initial file if provided
    if let Some(ref file_path) = opts.initial_file {
        if file_path.exists() {
            let job_id = format!("job_init_{}", &uuid::Uuid::new_v4().to_string()[..8]);
            let cancel_token = CancellationToken::new();
            active_cancellations
                .lock()
                .await
                .insert(job_id.clone(), cancel_token.clone());

            eprintln!("[webload] Queuing initial proxy file for ingestion: {}", file_path.display());
            let _ = ingest_mgr
                .ingest_file(job_id, file_path.clone(), cancel_token)
                .await;
        } else {
            eprintln!("[webload] Warning: Initial file does not exist: {}", file_path.display());
        }
    }

    if opts.start_seller {
        route_table.set_running(true);
    }

    // Spawn background seller relay supervisor
    let rt_clone = route_table.clone();
    let backend_url_clone = opts.backend_url.clone();
    tokio::spawn(async move {
        run_seller_supervisor(rt_clone, backend_url_clone).await;
    });

    // Build Axum web application
    let state = server::AppState {
        db,
        route_table: route_table.clone(),
        ingest_mgr,
        progress_tx,
        active_cancellations,
        backend_url: opts.backend_url.clone(),
    };

    let router = server::build_router(state);

    // Bind listener with automatic port fallback if port is in use
    let mut port = opts.port;
    let listener = loop {
        let addr_str = format!("{}:{}", opts.bind, port);
        match tokio::net::TcpListener::bind(&addr_str).await {
            Ok(l) => break l,
            Err(e) if port < opts.port + 10 => {
                eprintln!("[webload] Port {} is in use ({}), trying port {}...", port, e, port + 1);
                port += 1;
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to bind webload server to {}:{}: {}", opts.bind, port, e));
            }
        }
    };

    let local_addr = listener.local_addr()?;
    let ui_url = format!("http://{}", local_addr);

    println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                    ProxyBase Webload Dashboard Online                      ║");
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Web UI URL:        {:<56} ║", ui_url);
    println!("║ Database Path:     {:<56} ║", opts.db_path.display());
    println!("║ Backend Gateway:   {:<56} ║", opts.backend_url);
    println!("║ Press Ctrl+C in this terminal to shut down.                               ║");
    println!("╚════════════════════════════════════════════════════════════════════════════╝\n");

    if !opts.no_open {
        let _ = open::that(&ui_url);
    }

    axum::serve(listener, router).await?;
    Ok(())
}

/// Background supervisor that starts or pauses the multiplexed seller relay based on route table status.
async fn run_seller_supervisor(route_table: ActiveRouteTable, backend_url: String) {
    let mut is_running_rx = route_table.is_running_watch();

    loop {
        let is_running = *is_running_rx.borrow_and_update();
        if is_running {
            eprintln!("[webload:seller] Seller relay activated. Connecting to backend {}...", backend_url);
            // In future runs, seller relay loop communicates through route_table
        } else {
            eprintln!("[webload:seller] Seller relay standby (paused).");
        }

        if is_running_rx.changed().await.is_err() {
            break;
        }
    }
}

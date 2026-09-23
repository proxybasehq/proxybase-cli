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

#[cfg(test)]
mod tests {
    use super::*;
    use db::ProxyQueryFilter;

    #[tokio::test]
    async fn test_webload_end_to_end_lifecycle() {
        let temp_dir = std::env::temp_dir().join(format!("webload_e2e_{}", uuid::Uuid::new_v4()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;
        let db_path = temp_dir.join("webload_e2e.db");
        let proxy_file = temp_dir.join("proxies_list.txt");

        // 1. Create a heterogeneous proxy input file
        let proxy_content = "
# US Proxies
192.168.10.1:1080:alice,country_US:pass1 # US Node 1
192.168.10.2:1080:bob,country_US:pass2 # US Node 2
# European proxies
socks5://carol:pass3@192.168.10.3:1080?country=DE&category=datacenter
dave:pass4:192.168.10.4:1080 # Inverted format
# Worldwide proxy without country
192.168.10.5:1080:eve:pass5
# Malformed line that should be skipped as warning
malformed_line_no_port
";
        tokio::fs::write(&proxy_file, proxy_content).await.unwrap();

        // 2. Initialize DB & route table
        let db = WebloadDb::new(&db_path).unwrap();
        let route_table = ActiveRouteTable::new();
        let (tx, mut rx) = broadcast::channel(100);

        let ingest_mgr = IngestionManager::new(db.clone(), route_table.clone(), tx);
        let cancel = CancellationToken::new();

        // 3. Ingest file and wait for completion
        ingest_mgr.ingest_file("e2e_job".to_string(), proxy_file, cancel).await.unwrap();

        let mut completed = false;
        let timeout_res = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            while let Ok(evt) = rx.recv().await {
                if evt.status == "completed" {
                    assert_eq!(evt.valid_count, 5);
                    assert_eq!(evt.warning_count, 1);
                    completed = true;
                    break;
                }
            }
        }).await;

        assert!(timeout_res.is_ok(), "Ingestion timed out");
        assert!(completed, "Ingestion did not emit completed status");

        // 4. Verify DB aggregate stats
        let stats = db.get_aggregate_stats().unwrap();
        assert_eq!(stats.total_proxies, 5);
        assert_eq!(stats.active_proxies, 5);
        assert_eq!(stats.paused_proxies, 0);

        // 5. Verify hot route table is populated
        let telem = route_table.get_telemetry().await;
        assert_eq!(telem.active_upstream_paths, 5);

        // 6. Test stopping an upstream proxy individually
        let page = db.query_proxies(&ProxyQueryFilter::default()).unwrap();
        let p1 = &page.items[0];
        let p1_id = p1.id;
        let p1_path = p1.path_id.clone();

        // Register a stream on p1
        let cancel_stream = CancellationToken::new();
        route_table.register_stream("session_e2e_1", &p1_path, cancel_stream.clone()).await;
        assert_eq!(route_table.get_telemetry().await.active_streams, 1);

        // Operator pauses p1 in DB and hot route table
        let (new_status, _) = db.toggle_proxy_status(p1_id).unwrap();
        assert_eq!(new_status, "paused");
        route_table.deactivate_proxy(&p1_path, true).await;

        // Verify p1 stream is immediately aborted and proxy removed from routing
        assert!(cancel_stream.is_cancelled());
        assert!(!route_table.is_active(&p1_path).await);
        assert!(route_table.get_upstream(&p1_path).await.is_none());

        // 7. Operator resumes p1
        let (resumed_status, _) = db.toggle_proxy_status(p1_id).unwrap();
        assert_eq!(resumed_status, "active");
        let active_list = db.get_active_proxies().unwrap();
        let (_, resumed_upstream) = active_list.into_iter().find(|(pid, _)| pid == &p1_path).unwrap();
        route_table.activate_proxy(&p1_path, resumed_upstream).await;
        assert!(route_table.is_active(&p1_path).await);

        // 8. Bulk pause by country (pause all 'US' proxies)
        let filter_us = ProxyQueryFilter {
            country: Some("US".to_string()),
            ..Default::default()
        };
        let paused_paths = db.bulk_update_by_filter(&filter_us, "paused").unwrap();
        for pid in paused_paths {
            route_table.deactivate_proxy(&pid, true).await;
        }

        let stats_after_us = db.get_aggregate_stats().unwrap();
        assert_eq!(stats_after_us.paused_proxies, 2);
        assert_eq!(stats_after_us.active_proxies, 3);

        // 9. Reopen DB from disk and verify persistence
        drop(db);
        let reopened_db = WebloadDb::new(&db_path).unwrap();
        let reopened_stats = reopened_db.get_aggregate_stats().unwrap();
        assert_eq!(reopened_stats.total_proxies, 5);
        assert_eq!(reopened_stats.paused_proxies, 2);
        assert_eq!(reopened_stats.active_proxies, 3);

        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }
}

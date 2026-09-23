use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use serde::{Deserialize, Serialize};

use crate::proxy_parser;
use super::db::WebloadDb;
use super::relay::ActiveRouteTable;

/// Progress event emitted during large file streaming ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestProgressEvent {
    pub job_id: String,
    pub status: String, // "running", "completed", "cancelled", "failed"
    pub lines_read: usize,
    pub total_estimated: usize,
    pub valid_count: usize,
    pub duplicate_count: usize,
    pub warning_count: usize,
    pub lines_per_second: usize,
    pub progress_percent: f32,
    pub error_message: Option<String>,
}

/// Result of a single proxy latency probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeResult {
    pub proxy_id: i64,
    pub address: String,
    pub latency_ms: Option<u64>,
    pub is_success: bool,
    pub error_message: Option<String>,
}

/// Manager for non-blocking file streaming ingestion.
pub struct IngestionManager {
    db: WebloadDb,
    route_table: ActiveRouteTable,
    progress_tx: broadcast::Sender<IngestProgressEvent>,
}

impl IngestionManager {
    pub fn new(
        db: WebloadDb,
        route_table: ActiveRouteTable,
        progress_tx: broadcast::Sender<IngestProgressEvent>,
    ) -> Self {
        Self {
            db,
            route_table,
            progress_tx,
        }
    }

    /// Run streaming ingestion of a local file in a background task.
    pub async fn ingest_file(
        &self,
        job_id: String,
        file_path: PathBuf,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let db = self.db.clone();
        let route_table = self.route_table.clone();
        let tx = self.progress_tx.clone();

        tokio::spawn(async move {
            let res = run_ingestion_loop(&job_id, &file_path, db, route_table, tx.clone(), cancel_token).await;
            if let Err(e) = res {
                let _ = tx.send(IngestProgressEvent {
                    job_id: job_id.clone(),
                    status: "failed".to_string(),
                    lines_read: 0,
                    total_estimated: 0,
                    valid_count: 0,
                    duplicate_count: 0,
                    warning_count: 0,
                    lines_per_second: 0,
                    progress_percent: 0.0,
                    error_message: Some(format!("{:#}", e)),
                });
            }
        });

        Ok(())
    }
}

async fn run_ingestion_loop(
    job_id: &str,
    file_path: &Path,
    db: WebloadDb,
    route_table: ActiveRouteTable,
    progress_tx: broadcast::Sender<IngestProgressEvent>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let file = tokio::fs::File::open(file_path)
        .await
        .with_context(|| format!("Failed to open proxy file: {}", file_path.display()))?;

    // Estimate line count by file size (~60 bytes per average proxy line)
    let metadata = file.metadata().await.ok();
    let file_size = metadata.map(|m| m.len()).unwrap_or(0);
    let total_estimated = (file_size / 60).max(1) as usize;

    let reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer
    let mut lines = reader.lines();

    let mut batch = Vec::with_capacity(10_000);
    let mut lines_read = 0usize;
    let mut valid_count = 0usize;
    let mut duplicate_count = 0usize;
    let mut warning_count = 0usize;

    let start_time = tokio::time::Instant::now();
    let mut last_progress_time = tokio::time::Instant::now();
    let mut lines_since_last_tick = 0usize;
    let mut lines_per_second = 0usize;

    while let Some(line) = lines.next_line().await? {
        if cancel_token.is_cancelled() {
            let _ = progress_tx.send(IngestProgressEvent {
                job_id: job_id.to_string(),
                status: "cancelled".to_string(),
                lines_read,
                total_estimated,
                valid_count,
                duplicate_count,
                warning_count,
                lines_per_second: 0,
                progress_percent: (lines_read as f32 / total_estimated as f32 * 100.0).min(100.0),
                error_message: None,
            });
            return Ok(());
        }

        lines_read += 1;
        lines_since_last_tick += 1;

        match proxy_parser::parse_proxy_line(&line) {
            Ok(Some(mut proxy)) => {
                proxy.source_line = Some(lines_read);
                batch.push(proxy);
            }
            Ok(None) => {
                // Empty line or comment
            }
            Err(_) => {
                warning_count += 1;
            }
        }

        // Commit batch every 10,000 parsed proxies
        if batch.len() >= 10_000 {
            let (inserted, dups) = db.insert_batch(&batch)?;
            valid_count += inserted;
            duplicate_count += dups;
            batch.clear();
        }

        // Emit progress update every 100ms or 5,000 lines
        if last_progress_time.elapsed() >= tokio::time::Duration::from_millis(150) || lines_read % 5000 == 0 {
            let elapsed_secs = last_progress_time.elapsed().as_secs_f64();
            if elapsed_secs > 0.0 {
                lines_per_second = (lines_since_last_tick as f64 / elapsed_secs) as usize;
            }
            lines_since_last_tick = 0;
            last_progress_time = tokio::time::Instant::now();

            let percent = if total_estimated > 0 {
                ((lines_read as f32 / total_estimated as f32) * 100.0).min(99.0)
            } else {
                0.0
            };

            let _ = progress_tx.send(IngestProgressEvent {
                job_id: job_id.to_string(),
                status: "running".to_string(),
                lines_read,
                total_estimated,
                valid_count,
                duplicate_count,
                warning_count,
                lines_per_second,
                progress_percent: percent,
                error_message: None,
            });
        }
    }

    // Flush any remaining proxies in final batch
    if !batch.is_empty() {
        let (inserted, dups) = db.insert_batch(&batch)?;
        valid_count += inserted;
        duplicate_count += dups;
    }

    // Update in-memory hot route table with newly active proxies
    if let Ok(active_proxies) = db.get_active_proxies() {
        route_table.populate(active_proxies).await;
    }

    let total_elapsed = start_time.elapsed().as_secs_f64().max(0.001);
    let avg_speed = (lines_read as f64 / total_elapsed) as usize;

    let _ = progress_tx.send(IngestProgressEvent {
        job_id: job_id.to_string(),
        status: "completed".to_string(),
        lines_read,
        total_estimated: lines_read,
        valid_count,
        duplicate_count,
        warning_count,
        lines_per_second: avg_speed,
        progress_percent: 100.0,
        error_message: None,
    });

    Ok(())
}

/// SOCKS5 probe worker to measure connection latency and handshake health.
pub async fn probe_proxy_endpoint(
    address: &str,
    username: Option<&str>,
    password: Option<&str>,
    timeout_secs: u64,
) -> (bool, Option<u64>, Option<String>) {
    let timeout_dur = tokio::time::Duration::from_secs(timeout_secs);
    let start = tokio::time::Instant::now();

    let res = match (username, password) {
        (Some(u), Some(p)) => {
            tokio::time::timeout(
                timeout_dur,
                fast_socks5::client::Socks5Stream::connect_with_password(
                    address,
                    "1.1.1.1".to_string(),
                    80,
                    u.to_string(),
                    p.to_string(),
                    fast_socks5::client::Config::default(),
                ),
            )
            .await
        }
        _ => {
            tokio::time::timeout(
                timeout_dur,
                fast_socks5::client::Socks5Stream::connect(
                    address,
                    "1.1.1.1".to_string(),
                    80,
                    fast_socks5::client::Config::default(),
                ),
            )
            .await
        }
    };

    let elapsed = start.elapsed().as_millis() as u64;

    match res {
        Ok(Ok(_stream)) => (true, Some(elapsed), None),
        Ok(Err(e)) => (false, None, Some(e.to_string())),
        Err(_) => (false, None, Some("Connection timed out".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_streaming_ingestion() {
        let temp_dir = std::env::temp_dir().join(format!("webload_test_{}", uuid::Uuid::new_v4()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;
        let test_file = temp_dir.join("proxies.txt");

        let content = "
192.168.1.1:1080:u1:p1
192.168.1.2:1080:u2:p2 # US
socks5://u3:p3@192.168.1.3:1080?country=DE
# comment line
malformed_proxy_line_without_port
192.168.1.4:1080:u4:p4
";
        tokio::fs::write(&test_file, content).await.unwrap();

        let db = WebloadDb::memory().unwrap();
        let route_table = ActiveRouteTable::new();
        let (tx, mut rx) = broadcast::channel(100);

        let mgr = IngestionManager::new(db.clone(), route_table.clone(), tx);
        let cancel = CancellationToken::new();

        mgr.ingest_file("job_1".to_string(), test_file.clone(), cancel).await.unwrap();

        // Wait for completed event
        let mut got_completed = false;
        while let Ok(evt) = rx.recv().await {
            if evt.status == "completed" {
                assert_eq!(evt.valid_count, 4);
                assert_eq!(evt.warning_count, 1);
                got_completed = true;
                break;
            }
        }

        assert!(got_completed);

        // Verify proxies in db
        let query_res = db.query_proxies(&crate::webload::db::ProxyQueryFilter::default()).unwrap();
        assert_eq!(query_res.total, 4);

        // Verify route table was populated
        assert_eq!(route_table.get_telemetry().await.active_upstream_paths, 4);

        // Clean up
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_streaming_ingestion_cancellation() {
        let temp_dir = std::env::temp_dir().join(format!("webload_cancel_{}", uuid::Uuid::new_v4()));
        let _ = tokio::fs::create_dir_all(&temp_dir).await;
        let test_file = temp_dir.join("many_proxies.txt");

        // Write a test file with proxies
        let mut content = String::new();
        for i in 1..=500 {
            content.push_str(&format!("10.10.{}.{}:1080:user:pass\n", i / 256, i % 256));
        }
        tokio::fs::write(&test_file, content).await.unwrap();

        let db = WebloadDb::memory().unwrap();
        let route_table = ActiveRouteTable::new();
        let (tx, mut rx) = broadcast::channel(100);

        let mgr = IngestionManager::new(db.clone(), route_table.clone(), tx);
        let cancel = CancellationToken::new();

        // Cancel token so it stops on first line
        cancel.cancel();

        mgr.ingest_file("job_cancel".to_string(), test_file.clone(), cancel.clone()).await.unwrap();

        let mut got_cancelled = false;
        let timeout_result = tokio::time::timeout(tokio::time::Duration::from_secs(3), async {
            while let Ok(evt) = rx.recv().await {
                if evt.status == "cancelled" {
                    got_cancelled = true;
                    break;
                } else if evt.status == "completed" || evt.status == "failed" {
                    break;
                }
            }
        }).await;

        assert!(timeout_result.is_ok(), "Timed out waiting for cancellation event");
        assert!(got_cancelled, "Expected cancellation event to be emitted");
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    }

    #[tokio::test]
    async fn test_streaming_ingestion_missing_file() {
        let db = WebloadDb::memory().unwrap();
        let route_table = ActiveRouteTable::new();
        let (tx, mut rx) = broadcast::channel(100);

        let mgr = IngestionManager::new(db, route_table, tx);
        let cancel = CancellationToken::new();

        let missing = PathBuf::from("/non/existent/path/proxies.txt");
        mgr.ingest_file("job_missing".to_string(), missing, cancel).await.unwrap();

        let mut got_failed = false;
        while let Ok(evt) = rx.recv().await {
            if evt.status == "failed" {
                assert!(evt.error_message.is_some());
                got_failed = true;
                break;
            }
        }
        assert!(got_failed, "Expected failed event for non-existent file");
    }

    #[tokio::test]
    async fn test_probe_proxy_endpoint_unreachable() {
        // Probe an unreachable local port
        let (success, lat, err) = probe_proxy_endpoint("127.0.0.1:59999", None, None, 1).await;
        assert!(!success);
        assert!(lat.is_none());
        assert!(err.is_some());
    }
}

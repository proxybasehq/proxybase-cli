use anyhow::Result;
use axum::{
    extract::{Path as AxumPath, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{delete, get, post},
    Json, Router,
};
use futures_util::stream::Stream;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

use super::db::{AggregateStats, PaginatedProxies, ProxyQueryFilter, WebloadDb};
use super::ingest::{probe_proxy_endpoint, IngestProgressEvent, IngestionManager};
use super::relay::ActiveRouteTable;

#[derive(RustEmbed)]
#[folder = "src/webload/static/"]
struct WebAssets;

/// Shared application state for all Axum route handlers.
#[derive(Clone)]
pub struct AppState {
    pub db: WebloadDb,
    pub route_table: ActiveRouteTable,
    pub ingest_mgr: Arc<IngestionManager>,
    pub progress_tx: broadcast::Sender<IngestProgressEvent>,
    pub active_cancellations: Arc<Mutex<HashMap<String, CancellationToken>>>,
    pub backend_url: String,
}

/// Build Axum Router with all API endpoints and embedded static assets.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // API routes
        .route("/api/proxies", get(handle_get_proxies))
        .route("/api/proxies/load", post(handle_load_proxies))
        .route("/api/proxies/:id/toggle", post(handle_toggle_proxy))
        .route("/api/proxies/:id/test", post(handle_test_proxy))
        .route("/api/proxies/:id", delete(handle_delete_proxy))
        .route("/api/proxies/bulk", post(handle_bulk_action))
        .route("/api/jobs/:id/cancel", post(handle_cancel_job))
        .route("/api/stats", get(handle_get_stats))
        .route("/api/seller/toggle", post(handle_toggle_seller))
        .route("/api/events", get(handle_sse_events))
        // Static assets
        .route("/", get(handle_index))
        .route("/assets/*path", get(handle_static_asset))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Static Asset Handlers (Embedded via rust-embed)
// ---------------------------------------------------------------------------

async fn handle_index() -> impl IntoResponse {
    match WebAssets::get("index.html") {
        Some(content) => {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, "text/html; charset=utf-8".parse().unwrap());
            (StatusCode::OK, headers, content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Index page not found").into_response(),
    }
}

async fn handle_static_asset(AxumPath(path): AxumPath<String>) -> impl IntoResponse {
    let clean_path = path.trim_start_matches('/');
    match WebAssets::get(clean_path) {
        Some(content) => {
            let mime = mime_guess::from_path(clean_path).first_or_octet_stream();
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, mime.as_ref().parse().unwrap());
            headers.insert(header::CACHE_CONTROL, "public, max-age=3600".parse().unwrap());
            (StatusCode::OK, headers, content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Asset not found").into_response(),
    }
}

// ---------------------------------------------------------------------------
// API Handlers
// ---------------------------------------------------------------------------

async fn handle_get_proxies(
    State(state): State<AppState>,
    Query(filter): Query<ProxyQueryFilter>,
) -> Result<Json<PaginatedProxies>, (StatusCode, String)> {
    state
        .db
        .query_proxies(&filter)
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Deserialize)]
struct LoadProxiesRequest {
    file_path: Option<String>,
    raw_content: Option<String>,
}

#[derive(Serialize)]
struct LoadProxiesResponse {
    job_id: String,
    status: String,
    message: String,
}

async fn handle_load_proxies(
    State(state): State<AppState>,
    Json(req): Json<LoadProxiesRequest>,
) -> Result<Json<LoadProxiesResponse>, (StatusCode, String)> {
    let job_id = format!("job_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let cancel_token = CancellationToken::new();

    let target_path: PathBuf = if let Some(ref path_str) = req.file_path {
        let p = PathBuf::from(path_str);
        if !p.exists() {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("File does not exist: {}", path_str),
            ));
        }
        p
    } else if let Some(ref raw_content) = req.raw_content {
        let temp_dir = std::env::temp_dir().join("proxybase_webload");
        let _ = tokio::fs::create_dir_all(&temp_dir).await;
        let temp_file = temp_dir.join(format!("{}.txt", &job_id));
        tokio::fs::write(&temp_file, raw_content)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        temp_file
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Either file_path or raw_content must be provided".to_string(),
        ));
    };

    state
        .active_cancellations
        .lock()
        .await
        .insert(job_id.clone(), cancel_token.clone());

    state
        .ingest_mgr
        .ingest_file(job_id.clone(), target_path, cancel_token)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(LoadProxiesResponse {
        job_id,
        status: "started".to_string(),
        message: "Streaming ingestion running in background".to_string(),
    }))
}

#[derive(Serialize)]
struct ToggleProxyResponse {
    id: i64,
    new_status: String,
    path_id: String,
}

async fn handle_toggle_proxy(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> Result<Json<ToggleProxyResponse>, (StatusCode, String)> {
    let (new_status, path_id) = state
        .db
        .toggle_proxy_status(id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if new_status == "active" {
        // Read full proxy to reactivate in route table
        if let Ok(active_list) = state.db.get_active_proxies() {
            if let Some((_, u)) = active_list.into_iter().find(|(pid, _)| pid == &path_id) {
                state.route_table.activate_proxy(&path_id, u).await;
            }
        }
    } else {
        // Stop proxy individually from serving upstream: immediately removes from active routing table
        state.route_table.deactivate_proxy(&path_id, true).await;
    }

    Ok(Json(ToggleProxyResponse {
        id,
        new_status,
        path_id,
    }))
}

#[derive(Serialize)]
struct TestProxyResponse {
    id: i64,
    is_success: bool,
    latency_ms: Option<u64>,
    error_message: Option<String>,
}

async fn handle_test_proxy(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> Result<Json<TestProxyResponse>, (StatusCode, String)> {
    let (address, username, password) = {
        let conn = state.db.clone();
        // Query single proxy info
        let filter = ProxyQueryFilter {
            page: Some(1),
            limit: Some(1),
            ..Default::default()
        };
        let page = conn
            .query_proxies(&filter)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let p = page
            .items
            .into_iter()
            .find(|item| item.id == id)
            .ok_or_else(|| (StatusCode::NOT_FOUND, "Proxy not found".to_string()))?;
        (p.address, p.username, p.password)
    };

    let (is_success, latency, err) =
        probe_proxy_endpoint(&address, username.as_deref(), password.as_deref(), 5).await;

    let _ = state.db.update_proxy_test_result(id, latency, is_success);

    Ok(Json(TestProxyResponse {
        id,
        is_success,
        latency_ms: latency,
        error_message: err,
    }))
}

#[derive(Deserialize)]
struct BulkActionRequest {
    action: String, // "pause", "resume", "delete"
    ids: Vec<i64>,
}

#[derive(Serialize)]
struct BulkActionResponse {
    affected: usize,
    action: String,
}

async fn handle_bulk_action(
    State(state): State<AppState>,
    Json(req): Json<BulkActionRequest>,
) -> Result<Json<BulkActionResponse>, (StatusCode, String)> {
    let affected = match req.action.as_str() {
        "pause" => {
            let path_ids = state
                .db
                .bulk_update_status(&req.ids, "paused")
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            for pid in path_ids {
                state.route_table.deactivate_proxy(&pid, true).await;
            }
            req.ids.len()
        }
        "resume" => {
            let path_ids = state
                .db
                .bulk_update_status(&req.ids, "active")
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            if let Ok(active_list) = state.db.get_active_proxies() {
                let map: HashMap<String, _> = active_list.into_iter().collect();
                for pid in path_ids {
                    if let Some(proxy) = map.get(&pid) {
                        state.route_table.activate_proxy(&pid, proxy.clone()).await;
                    }
                }
            }
            req.ids.len()
        }
        "delete" => {
            let mut count = 0;
            for id in req.ids {
                if let Ok(Some(pid)) = state.db.delete_proxy(id) {
                    state.route_table.deactivate_proxy(&pid, true).await;
                    count += 1;
                }
            }
            count
        }
        _ => return Err((StatusCode::BAD_REQUEST, "Unsupported bulk action".to_string())),
    };

    Ok(Json(BulkActionResponse {
        affected,
        action: req.action,
    }))
}

async fn handle_delete_proxy(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<i64>,
) -> Result<StatusCode, (StatusCode, String)> {
    if let Some(pid) = state
        .db
        .delete_proxy(id)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    {
        state.route_table.deactivate_proxy(&pid, true).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn handle_cancel_job(
    State(state): State<AppState>,
    AxumPath(job_id): AxumPath<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut map = state.active_cancellations.lock().await;
    if let Some(token) = map.remove(&job_id) {
        token.cancel();
        Ok(StatusCode::OK)
    } else {
        Err((StatusCode::NOT_FOUND, "Job not found or already ended".to_string()))
    }
}

async fn handle_get_stats(
    State(state): State<AppState>,
) -> Result<Json<AggregateStats>, (StatusCode, String)> {
    state
        .db
        .get_aggregate_stats()
        .map(Json)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Serialize)]
struct ToggleSellerResponse {
    is_running: bool,
}

async fn handle_toggle_seller(
    State(state): State<AppState>,
) -> Result<Json<ToggleSellerResponse>, (StatusCode, String)> {
    let rx = state.route_table.is_running_watch();
    let current = *rx.borrow();
    let new_state = !current;
    state.route_table.set_running(new_state);

    Ok(Json(ToggleSellerResponse {
        is_running: new_state,
    }))
}

// ---------------------------------------------------------------------------
// Server-Sent Events (SSE) Stream
// ---------------------------------------------------------------------------

async fn handle_sse_events(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, axum::Error>>> {
    let mut rx_ingest = state.progress_tx.subscribe();
    let route_table = state.route_table.clone();

    let stream = async_stream::stream! {
        let mut interval = tokio::time::interval(Duration::from_millis(1000));

        loop {
            tokio::select! {
                // Ingest progress events
                Ok(evt) = rx_ingest.recv() => {
                    if let Ok(json_str) = serde_json::to_string(&evt) {
                        yield Ok(Event::default().data(json_str));
                    }
                }
                // Periodic telemetry tick
                _ = interval.tick() => {
                    let mut telem = route_table.get_telemetry().await;
                    telem.bytes_per_sec = route_table.tick_throughput_rate();
                    let payload = serde_json::json!({
                        "type": "telemetry",
                        "active_streams": telem.active_streams,
                        "bytes_per_sec": telem.bytes_per_sec,
                        "total_bytes": telem.total_bytes_relayed,
                        "active_upstream_paths": telem.active_upstream_paths,
                        "is_relay_running": telem.is_relay_running,
                    });
                    if let Ok(json_str) = serde_json::to_string(&payload) {
                        yield Ok(Event::default().data(json_str));
                    }
                }
            }
        }
    };

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

use anyhow::Result;
use axum::{
    extract::{DefaultBodyLimit, Path as AxumPath, Query, State},
    http::{header, HeaderMap, StatusCode},
    middleware::{self, Next},
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
use std::collections::{HashMap, HashSet};
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
    pub auth_user: String,
    pub auth_pass: String,
    pub auth_tokens: Arc<Mutex<HashSet<String>>>,
}

/// Build Axum Router with all API endpoints and embedded static assets.
pub fn build_router(state: AppState) -> Router {
    Router::new()
        // Auth routes
        .route("/api/auth/login", post(handle_login))
        .route("/api/auth/status", get(handle_auth_status))
        .route("/api/auth/logout", post(handle_logout))
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
        .layer(middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(DefaultBodyLimit::max(500 * 1024 * 1024)) // 500 MB limit for massive proxy uploads
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
            headers.insert(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate".parse().unwrap());
            headers.insert(header::PRAGMA, "no-cache".parse().unwrap());
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
            headers.insert(header::CACHE_CONTROL, "no-cache, no-store, must-revalidate".parse().unwrap());
            headers.insert(header::PRAGMA, "no-cache".parse().unwrap());
            (StatusCode::OK, headers, content.data).into_response()
        }
        None => (StatusCode::NOT_FOUND, "Asset not found").into_response(),
    }
}

// ---------------------------------------------------------------------------
// Authentication Handlers & Middleware
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub username: String,
}

async fn handle_login(
    State(state): State<AppState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<serde_json::Value>)> {
    if payload.username == state.auth_user && payload.password == state.auth_pass {
        let token = format!("sess_{}", uuid::Uuid::new_v4().simple());
        let mut tokens = state.auth_tokens.lock().await;
        tokens.insert(token.clone());
        Ok(Json(LoginResponse {
            token,
            username: state.auth_user,
        }))
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid username or password" })),
        ))
    }
}

async fn handle_auth_status(
    State(state): State<AppState>,
) -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({
        "authenticated": true,
        "username": state.auth_user,
    })))
}

async fn handle_logout(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Some(auth_header) = headers.get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                let mut tokens = state.auth_tokens.lock().await;
                tokens.remove(token.trim());
            }
        }
    }
    (StatusCode::OK, Json(serde_json::json!({ "success": true })))
}

async fn auth_middleware(
    State(state): State<AppState>,
    req: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    // Allow index, static assets, and login endpoint without auth
    if path == "/" || path.starts_with("/assets/") || path == "/api/auth/login" {
        return next.run(req).await;
    }

    // Check token in Authorization header: Bearer <token>
    let mut token = None;
    if let Some(auth_header) = req.headers().get(header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(t) = auth_str.strip_prefix("Bearer ") {
                token = Some(t.trim().to_string());
            }
        }
    }

    // Also allow ?token=<token> query parameter (for EventSource SSE)
    if token.is_none() {
        if let Some(query) = req.uri().query() {
            for param in query.split('&') {
                if let Some((k, v)) = param.split_once('=') {
                    if k == "token" {
                        token = Some(v.to_string());
                        break;
                    }
                }
            }
        }
    }

    if let Some(ref t) = token {
        let tokens = state.auth_tokens.lock().await;
        if tokens.contains(t) {
            return next.run(req).await;
        }
    }

    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": "Unauthorized. Please authenticate with credentials displayed in terminal."
        })),
    ).into_response()
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
                    let mut val = serde_json::to_value(&evt).unwrap_or_default();
                    if let serde_json::Value::Object(ref mut map) = val {
                        map.insert("type".to_string(), serde_json::Value::String("ingest_progress".to_string()));
                    }
                    if let Ok(json_str) = serde_json::to_string(&val) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;
    use crate::proxy_parser::ParsedProxy;

    fn make_test_proxy(
        host: &str,
        port: u16,
        user: Option<&str>,
        country: Option<&str>,
        cat: Option<&str>,
    ) -> ParsedProxy {
        ParsedProxy {
            address: format!("{}:{}", host, port),
            host: host.to_string(),
            port,
            username: user.map(|s| s.to_string()),
            password: Some("p".to_string()),
            country: country.map(|s| s.to_string()),
            proxy_category: cat.map(|s| s.to_string()),
            label: None,
            source_line: None,
            raw_input: format!("{}:{}", host, port),
        }
    }

    fn auth_req(method: &str, uri: &str) -> axum::http::request::Builder {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("Authorization", "Bearer test_valid_token")
    }

    async fn setup_test_app() -> (Router, WebloadDb, ActiveRouteTable) {
        let db = WebloadDb::memory().unwrap();
        let route_table = ActiveRouteTable::new();
        let (progress_tx, _) = broadcast::channel(100);
        let ingest_mgr = Arc::new(IngestionManager::new(
            db.clone(),
            route_table.clone(),
            progress_tx.clone(),
        ));
        let active_cancellations = Arc::new(Mutex::new(HashMap::new()));
        let mut tokens = HashSet::new();
        tokens.insert("test_valid_token".to_string());
        let auth_tokens = Arc::new(Mutex::new(tokens));

        let state = AppState {
            db: db.clone(),
            route_table: route_table.clone(),
            ingest_mgr,
            progress_tx,
            active_cancellations,
            backend_url: "http://127.0.0.1:8080".to_string(),
            auth_user: "testadmin".to_string(),
            auth_pass: "testpass123".to_string(),
            auth_tokens,
        };

        (build_router(state), db, route_table)
    }

    #[tokio::test]
    async fn test_get_index_and_static_assets() {
        let (app, _, _) = setup_test_app().await;

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let html = String::from_utf8_lossy(&bytes);
        assert!(html.contains("ProxyBase Webload"));

        let req_css = Request::builder().uri("/assets/app.css").body(Body::empty()).unwrap();
        let res_css = app.clone().oneshot(req_css).await.unwrap();
        assert_eq!(res_css.status(), StatusCode::OK);

        let req_js = Request::builder().uri("/assets/app.js").body(Body::empty()).unwrap();
        let res_js = app.clone().oneshot(req_js).await.unwrap();
        assert_eq!(res_js.status(), StatusCode::OK);

        let req_404 = Request::builder().uri("/assets/non_existent.png").body(Body::empty()).unwrap();
        let res_404 = app.oneshot(req_404).await.unwrap();
        assert_eq!(res_404.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_api_auth_login_success() {
        let (app, _, _) = setup_test_app().await;

        let login_body = serde_json::json!({
            "username": "testadmin",
            "password": "testpass123"
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .body(Body::from(login_body.to_string()))
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["token"].as_str().unwrap().starts_with("sess_"));
        assert_eq!(json["username"], "testadmin");
    }

    #[tokio::test]
    async fn test_api_auth_login_invalid() {
        let (app, _, _) = setup_test_app().await;

        let login_body = serde_json::json!({
            "username": "testadmin",
            "password": "wrong_password"
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/auth/login")
            .header("Content-Type", "application/json")
            .body(Body::from(login_body.to_string()))
            .unwrap();

        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_api_auth_middleware_blocks_unauthorized() {
        let (app, _, _) = setup_test_app().await;

        // Missing token -> 401
        let req_no_token = Request::builder().uri("/api/proxies").body(Body::empty()).unwrap();
        let res_no_token = app.clone().oneshot(req_no_token).await.unwrap();
        assert_eq!(res_no_token.status(), StatusCode::UNAUTHORIZED);

        // Invalid token -> 401
        let req_invalid = Request::builder()
            .uri("/api/proxies")
            .header("Authorization", "Bearer invalid_token_xyz")
            .body(Body::empty())
            .unwrap();
        let res_invalid = app.oneshot(req_invalid).await.unwrap();
        assert_eq!(res_invalid.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_api_auth_logout() {
        let (app, _, _) = setup_test_app().await;

        // 1. Verify access works with valid token
        let req1 = auth_req("GET", "/api/auth/status").body(Body::empty()).unwrap();
        let res1 = app.clone().oneshot(req1).await.unwrap();
        assert_eq!(res1.status(), StatusCode::OK);

        // 2. Perform logout
        let req_logout = auth_req("POST", "/api/auth/logout").body(Body::empty()).unwrap();
        let res_logout = app.clone().oneshot(req_logout).await.unwrap();
        assert_eq!(res_logout.status(), StatusCode::OK);

        // 3. Verify access is now rejected
        let req2 = auth_req("GET", "/api/auth/status").body(Body::empty()).unwrap();
        let res2 = app.oneshot(req2).await.unwrap();
        assert_eq!(res2.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_api_auth_sse_query_token() {
        let (app, _, _) = setup_test_app().await;

        // Valid query token
        let req = Request::builder().uri("/api/events?token=test_valid_token").body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        // Invalid query token
        let req_bad = Request::builder().uri("/api/events?token=bad_token").body(Body::empty()).unwrap();
        let res_bad = app.oneshot(req_bad).await.unwrap();
        assert_eq!(res_bad.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_api_proxies_pagination_and_filter() {
        let (app, db, _) = setup_test_app().await;

        // Seed 30 proxies
        let mut proxies = Vec::new();
        for i in 1..=30 {
            let cc = if i <= 10 { Some("US") } else if i <= 20 { Some("DE") } else { None };
            let cat = if i % 2 == 0 { Some("residential") } else { Some("datacenter") };
            proxies.push(make_test_proxy(&format!("10.0.0.{}", i), 1080, Some(&format!("u_{}", i)), cc, cat));
        }
        db.insert_batch(&proxies).unwrap();

        // 1. Pagination: page 1 with limit 10
        let req = auth_req("GET", "/api/proxies?page=1&limit=10").body(Body::empty()).unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["items"].as_array().unwrap().len(), 10);
        assert_eq!(json["total"], 30);
        assert_eq!(json["total_pages"], 3);

        // 2. Filter by country: US
        let req_us = auth_req("GET", "/api/proxies?country=US&limit=50").body(Body::empty()).unwrap();
        let res_us = app.clone().oneshot(req_us).await.unwrap();
        let body_us = axum::body::to_bytes(res_us.into_body(), usize::MAX).await.unwrap();
        let json_us: serde_json::Value = serde_json::from_slice(&body_us).unwrap();
        assert_eq!(json_us["filtered"], 10);

        // 3. Filter by Worldwide (null country)
        let req_ww = auth_req("GET", "/api/proxies?country=WW&limit=50").body(Body::empty()).unwrap();
        let res_ww = app.clone().oneshot(req_ww).await.unwrap();
        let body_ww = axum::body::to_bytes(res_ww.into_body(), usize::MAX).await.unwrap();
        let json_ww: serde_json::Value = serde_json::from_slice(&body_ww).unwrap();
        assert_eq!(json_ww["filtered"], 10);

        // 4. Search query
        let req_search = auth_req("GET", "/api/proxies?search=10.0.0.15").body(Body::empty()).unwrap();
        let res_search = app.oneshot(req_search).await.unwrap();
        let body_search = axum::body::to_bytes(res_search.into_body(), usize::MAX).await.unwrap();
        let json_search: serde_json::Value = serde_json::from_slice(&body_search).unwrap();
        assert_eq!(json_search["items"].as_array().unwrap().len(), 1);
        assert_eq!(json_search["items"][0]["address"], "10.0.0.15:1080");
    }

    #[tokio::test]
    async fn test_api_toggle_and_bulk_actions() {
        let (app, db, route_table) = setup_test_app().await;

        let proxies = vec![
            make_test_proxy("192.168.1.1", 1080, Some("u1"), Some("US"), None),
            make_test_proxy("192.168.1.2", 1080, Some("u2"), Some("US"), None),
            make_test_proxy("192.168.1.3", 1080, Some("u3"), Some("US"), None),
        ];
        db.insert_batch(&proxies).unwrap();
        let active = db.get_active_proxies().unwrap();
        route_table.populate(active).await;

        let page = db.query_proxies(&ProxyQueryFilter::default()).unwrap();
        let id1 = page.items[0].id;
        let path1 = page.items[0].path_id.clone();

        // 1. Toggle proxy 1 to paused
        let req_toggle = auth_req("POST", &format!("/api/proxies/{}/toggle", id1))
            .body(Body::empty())
            .unwrap();
        let res_toggle = app.clone().oneshot(req_toggle).await.unwrap();
        assert_eq!(res_toggle.status(), StatusCode::OK);
        let body_toggle = axum::body::to_bytes(res_toggle.into_body(), usize::MAX).await.unwrap();
        let json_toggle: serde_json::Value = serde_json::from_slice(&body_toggle).unwrap();
        assert_eq!(json_toggle["new_status"], "paused");
        assert!(!route_table.is_active(&path1).await);

        // 2. Bulk pause remaining
        let id2 = page.items[1].id;
        let id3 = page.items[2].id;
        let bulk_body = serde_json::json!({
            "action": "pause",
            "ids": [id2, id3]
        });
        let req_bulk = auth_req("POST", "/api/proxies/bulk")
            .header("Content-Type", "application/json")
            .body(Body::from(bulk_body.to_string()))
            .unwrap();
        let res_bulk = app.clone().oneshot(req_bulk).await.unwrap();
        assert_eq!(res_bulk.status(), StatusCode::OK);

        let stats = db.get_aggregate_stats().unwrap();
        assert_eq!(stats.active_proxies, 0);
        assert_eq!(stats.paused_proxies, 3);

        // 3. Bulk resume
        let bulk_resume_body = serde_json::json!({
            "action": "resume",
            "ids": [id1, id2, id3]
        });
        let req_resume = auth_req("POST", "/api/proxies/bulk")
            .header("Content-Type", "application/json")
            .body(Body::from(bulk_resume_body.to_string()))
            .unwrap();
        let res_resume = app.clone().oneshot(req_resume).await.unwrap();
        assert_eq!(res_resume.status(), StatusCode::OK);
        assert!(route_table.is_active(&path1).await);

        // 4. Delete proxy 1
        let req_del = auth_req("DELETE", &format!("/api/proxies/{}", id1))
            .body(Body::empty())
            .unwrap();
        let res_del = app.oneshot(req_del).await.unwrap();
        assert_eq!(res_del.status(), StatusCode::NO_CONTENT);
        assert!(!route_table.is_active(&path1).await);
    }

    #[tokio::test]
    async fn test_api_stats_and_seller_toggle() {
        let (app, db, route_table) = setup_test_app().await;

        let proxies = vec![
            make_test_proxy("5.5.5.1", 1080, Some("u1"), Some("JP"), None),
            make_test_proxy("5.5.5.2", 1080, Some("u2"), None, None),
        ];
        db.insert_batch(&proxies).unwrap();

        // Stats
        let req_stats = auth_req("GET", "/api/stats").body(Body::empty()).unwrap();
        let res_stats = app.clone().oneshot(req_stats).await.unwrap();
        assert_eq!(res_stats.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res_stats.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["total_proxies"], 2);
        assert_eq!(json["active_proxies"], 2);

        // Seller toggle
        assert!(!*route_table.is_running_watch().borrow());
        let req_toggle = auth_req("POST", "/api/seller/toggle").body(Body::empty()).unwrap();
        let res_toggle = app.clone().oneshot(req_toggle).await.unwrap();
        assert_eq!(res_toggle.status(), StatusCode::OK);
        assert!(*route_table.is_running_watch().borrow());
    }

    #[tokio::test]
    async fn test_api_load_proxies_direct_content() {
        let (app, db, _) = setup_test_app().await;

        let payload = serde_json::json!({
            "raw_content": "100.64.0.1:1080:user:pass\n100.64.0.2:1080:user:pass # US\n100.64.0.3:1080:user:pass"
        });

        let req = auth_req("POST", "/api/proxies/load")
            .header("Content-Type", "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "started");
        assert!(json["job_id"].as_str().is_some());

        // Wait brief moment for background ingestion task to complete
        for _ in 0..50 {
            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
            let stats = db.get_aggregate_stats().unwrap();
            if stats.total_proxies == 3 {
                break;
            }
        }

        let stats = db.get_aggregate_stats().unwrap();
        assert_eq!(stats.total_proxies, 3);
    }

    #[tokio::test]
    async fn test_api_cancel_job() {
        let (app, _, _) = setup_test_app().await;

        // 1. Cancel non-existent job -> 404
        let req_missing = auth_req("POST", "/api/jobs/job_unknown/cancel")
            .body(Body::empty())
            .unwrap();
        let res_missing = app.clone().oneshot(req_missing).await.unwrap();
        assert_eq!(res_missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_api_bulk_delete() {
        let (app, db, route_table) = setup_test_app().await;

        let proxies = vec![
            make_test_proxy("8.8.8.1", 1080, Some("u1"), Some("US"), None),
            make_test_proxy("8.8.8.2", 1080, Some("u2"), Some("US"), None),
        ];
        db.insert_batch(&proxies).unwrap();
        let active = db.get_active_proxies().unwrap();
        route_table.populate(active).await;

        let page = db.query_proxies(&ProxyQueryFilter::default()).unwrap();
        let ids: Vec<i64> = page.items.iter().map(|p| p.id).collect();

        let bulk_body = serde_json::json!({
            "action": "delete",
            "ids": ids
        });

        let req = auth_req("POST", "/api/proxies/bulk")
            .header("Content-Type", "application/json")
            .body(Body::from(bulk_body.to_string()))
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let stats = db.get_aggregate_stats().unwrap();
        assert_eq!(stats.total_proxies, 0);
        assert_eq!(route_table.get_telemetry().await.active_upstream_paths, 0);
    }

    #[tokio::test]
    async fn test_api_test_proxy_endpoint() {
        let (app, db, _) = setup_test_app().await;

        let proxies = vec![make_test_proxy("127.0.0.1", 59998, Some("u"), None, None)];
        db.insert_batch(&proxies).unwrap();
        let page = db.query_proxies(&ProxyQueryFilter::default()).unwrap();
        let id = page.items[0].id;

        let req = auth_req("POST", &format!("/api/proxies/{}/test", id))
            .body(Body::empty())
            .unwrap();

        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let body = axum::body::to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], id);
        assert_eq!(json["is_success"], false);
        assert!(json["error_message"].as_str().is_some());
    }

    #[tokio::test]
    async fn test_api_sse_events_header() {
        let (app, _, _) = setup_test_app().await;

        let req = auth_req("GET", "/api/events").body(Body::empty()).unwrap();
        let res = app.oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let content_type = res.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(content_type.contains("text/event-stream"));
    }

    #[tokio::test]
    async fn test_api_proxies_sorting() {
        let (app, db, _) = setup_test_app().await;

        let proxies = vec![
            make_test_proxy("1.1.1.1", 1080, None, None, None),
            make_test_proxy("9.9.9.9", 1080, None, None, None),
        ];
        db.insert_batch(&proxies).unwrap();

        // Sort by host DESC
        let req_desc = auth_req("GET", "/api/proxies?sort_by=host&sort_dir=desc").body(Body::empty()).unwrap();
        let res_desc = app.clone().oneshot(req_desc).await.unwrap();
        assert_eq!(res_desc.status(), StatusCode::OK);
        let body = axum::body::to_bytes(res_desc.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["items"][0]["host"], "9.9.9.9");
        assert_eq!(json["items"][1]["host"], "1.1.1.1");

        // Sort by host ASC
        let req_asc = auth_req("GET", "/api/proxies?sort_by=host&sort_dir=asc").body(Body::empty()).unwrap();
        let res_asc = app.oneshot(req_asc).await.unwrap();
        let body_asc = axum::body::to_bytes(res_asc.into_body(), usize::MAX).await.unwrap();
        let json_asc: serde_json::Value = serde_json::from_slice(&body_asc).unwrap();
        assert_eq!(json_asc["items"][0]["host"], "1.1.1.1");
        assert_eq!(json_asc["items"][1]["host"], "9.9.9.9");
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use serde::{Deserialize, Serialize};
use crate::UpstreamProxy;

/// Telemetry snapshot for seller relay performance.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RelayTelemetry {
    pub active_streams: u32,
    pub total_streams_relayed: u64,
    pub total_bytes_relayed: u64,
    pub bytes_per_sec: u64,
    pub active_upstream_paths: usize,
    pub is_relay_running: bool,
}

/// In-memory hot route table for sub-microsecond stream routing and individual upstream control.
#[derive(Clone)]
pub struct ActiveRouteTable {
    /// O(1) hot lookup: path_id -> UpstreamProxy info
    routes: Arc<RwLock<HashMap<String, Arc<UpstreamProxy>>>>,
    /// Fast set of active path_ids
    active_paths: Arc<RwLock<HashSet<String>>>,
    /// Active streams: session_id -> (path_id, cancellation_token)
    streams: Arc<RwLock<HashMap<String, (String, CancellationToken)>>>,
    /// Global seller relay running flag
    is_running: Arc<tokio::sync::watch::Sender<bool>>,
    /// Telemetry counters
    active_streams_count: Arc<AtomicU32>,
    total_streams_count: Arc<AtomicU64>,
    total_bytes_count: Arc<AtomicU64>,
    current_bps: Arc<AtomicU64>,
}

impl ActiveRouteTable {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::watch::channel(false);
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
            active_paths: Arc::new(RwLock::new(HashSet::new())),
            streams: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(tx),
            active_streams_count: Arc::new(AtomicU32::new(0)),
            total_streams_count: Arc::new(AtomicU64::new(0)),
            total_bytes_count: Arc::new(AtomicU64::new(0)),
            current_bps: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Watcher for whether the seller relay is running.
    pub fn is_running_watch(&self) -> tokio::sync::watch::Receiver<bool> {
        self.is_running.subscribe()
    }

    /// Set running status
    pub fn set_running(&self, running: bool) {
        let _ = self.is_running.send(running);
    }

    /// Bulk initialize the hot route table from active database records.
    pub async fn populate(&self, entries: Vec<(String, UpstreamProxy)>) {
        let mut routes = self.routes.write().await;
        let mut active = self.active_paths.write().await;
        routes.clear();
        active.clear();

        for (path_id, proxy) in entries {
            routes.insert(path_id.clone(), Arc::new(proxy));
            active.insert(path_id);
        }
    }

    /// Activate or resume an upstream proxy.
    pub async fn activate_proxy(&self, path_id: &str, proxy: UpstreamProxy) {
        let mut routes = self.routes.write().await;
        let mut active = self.active_paths.write().await;
        routes.insert(path_id.to_string(), Arc::new(proxy));
        active.insert(path_id.to_string());
    }

    /// Deactivate or stop an individual upstream proxy.
    /// If `abort_streams` is true, immediately cancels any active streams flowing through this proxy.
    pub async fn deactivate_proxy(&self, path_id: &str, abort_streams: bool) {
        {
            let mut routes = self.routes.write().await;
            let mut active = self.active_paths.write().await;
            routes.remove(path_id);
            active.remove(path_id);
        }

        if abort_streams {
            let streams = self.streams.read().await;
            for (_sid, (pid, token)) in streams.iter() {
                if pid == path_id {
                    token.cancel();
                }
            }
        }
    }

    /// Instant check: is this path_id active?
    pub async fn is_active(&self, path_id: &str) -> bool {
        self.active_paths.read().await.contains(path_id)
    }

    /// Sub-microsecond upstream proxy lookup.
    pub async fn get_upstream(&self, path_id: &str) -> Option<Arc<UpstreamProxy>> {
        self.routes.read().await.get(path_id).cloned()
    }

    /// Register an active relayed stream session.
    pub async fn register_stream(&self, session_id: &str, path_id: &str, token: CancellationToken) {
        let mut streams = self.streams.write().await;
        streams.insert(session_id.to_string(), (path_id.to_string(), token));
        self.active_streams_count.fetch_add(1, Ordering::Relaxed);
        self.total_streams_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Unregister a finished stream session.
    pub async fn unregister_stream(&self, session_id: &str) {
        let mut streams = self.streams.write().await;
        if streams.remove(session_id).is_some() {
            self.active_streams_count.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record relayed byte count.
    pub fn record_bytes(&self, bytes: u64) {
        self.total_bytes_count.fetch_add(bytes, Ordering::Relaxed);
        self.current_bps.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Reset window throughput (e.g. called every 1s by background telemetry loop).
    pub fn tick_throughput_rate(&self) -> u64 {
        self.current_bps.swap(0, Ordering::Relaxed)
    }

    /// Get current telemetry snapshot.
    pub async fn get_telemetry(&self) -> RelayTelemetry {
        let active_upstream_paths = self.active_paths.read().await.len();
        RelayTelemetry {
            active_streams: self.active_streams_count.load(Ordering::Relaxed),
            total_streams_relayed: self.total_streams_count.load(Ordering::Relaxed),
            total_bytes_relayed: self.total_bytes_count.load(Ordering::Relaxed),
            bytes_per_sec: self.current_bps.load(Ordering::Relaxed),
            active_upstream_paths,
            is_relay_running: *self.is_running.borrow(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_proxy(addr: &str) -> UpstreamProxy {
        UpstreamProxy {
            address: addr.to_string(),
            username: Some("u".to_string()),
            password: Some("p".to_string()),
            country: Some("US".to_string()),
            proxy_category: Some("residential".to_string()),
            label: None,
        }
    }

    #[tokio::test]
    async fn test_active_route_table_toggles() {
        let table = ActiveRouteTable::new();

        // 1. Populate
        table.populate(vec![
            ("p_1".to_string(), make_test_proxy("1.1.1.1:1080")),
            ("p_2".to_string(), make_test_proxy("1.1.1.2:1080")),
        ]).await;

        assert!(table.is_active("p_1").await);
        assert!(table.is_active("p_2").await);
        assert!(!table.is_active("p_3").await);

        assert_eq!(table.get_upstream("p_1").await.unwrap().address, "1.1.1.1:1080");

        // 2. Deactivate individual proxy (stop from upstream)
        table.deactivate_proxy("p_1", false).await;
        assert!(!table.is_active("p_1").await);
        assert!(table.get_upstream("p_1").await.is_none());
        assert!(table.is_active("p_2").await);

        // 3. Reactivate
        table.activate_proxy("p_1", make_test_proxy("1.1.1.1:1080")).await;
        assert!(table.is_active("p_1").await);
    }

    #[tokio::test]
    async fn test_stream_registration_and_abort() {
        let table = ActiveRouteTable::new();
        let cancel = CancellationToken::new();

        table.register_stream("s_100", "p_1", cancel.clone()).await;
        let telem = table.get_telemetry().await;
        assert_eq!(telem.active_streams, 1);
        assert_eq!(telem.total_streams_relayed, 1);

        // Stop proxy with abort_streams = true
        table.deactivate_proxy("p_1", true).await;
        assert!(cancel.is_cancelled());

        // Unregister finished stream
        table.unregister_stream("s_100").await;
        let telem2 = table.get_telemetry().await;
        assert_eq!(telem2.active_streams, 0);
    }
}

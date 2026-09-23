use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use sha2::Digest;
use crate::proxy_parser::ParsedProxy;

/// Database record representing an upstream proxy in the webload system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyRecord {
    pub id: i64,
    pub path_id: String,
    pub address: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    pub country: Option<String>,
    pub category: Option<String>,
    pub label: Option<String>,
    pub raw_input: String,
    pub status: String, // 'active', 'paused', 'error', 'testing'
    pub last_latency_ms: Option<u64>,
    pub last_tested_at: Option<i64>,
    pub consecutive_failures: u32,
    pub total_streams: u64,
    pub total_bytes_relayed: u64,
    pub created_at: i64,
}

/// Query filter options for server-side pagination, searching, and filtering.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProxyQueryFilter {
    pub page: Option<usize>,
    pub limit: Option<usize>,
    pub search: Option<String>,
    pub status: Option<String>,
    pub country: Option<String>,
    pub category: Option<String>,
    pub max_latency: Option<u64>,
    pub sort_by: Option<String>,
    pub sort_dir: Option<String>,
}

/// Paginated result response returned to the web UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedProxies {
    pub items: Vec<ProxyRecord>,
    pub total: usize,
    pub filtered: usize,
    pub page: usize,
    pub limit: usize,
    pub total_pages: usize,
}

/// Aggregated statistics for the telemetry header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateStats {
    pub total_proxies: usize,
    pub active_proxies: usize,
    pub paused_proxies: usize,
    pub error_proxies: usize,
    pub tested_proxies: usize,
    pub avg_latency_ms: Option<u64>,
    pub countries_count: usize,
    pub top_countries: Vec<(String, usize)>,
}

/// Thread-safe SQLite database manager for webload.
#[derive(Clone)]
pub struct WebloadDb {
    conn: Arc<Mutex<Connection>>,
    db_path: PathBuf,
}

impl WebloadDb {
    /// Initialize the SQLite database at the given path with WAL mode and indexes.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create db dir: {}", parent.display()))?;
        }

        let conn = Connection::open(&db_path)
            .with_context(|| format!("Failed to open SQLite db: {}", db_path.display()))?;

        // Configure SQLite performance PRAGMAs
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -64000; -- 64MB cache
            PRAGMA mmap_size = 268435456; -- 256MB mmap
            PRAGMA temp_store = MEMORY;
            ",
        )?;

        // Initialize schema
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path_id TEXT UNIQUE NOT NULL,
                address TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                username TEXT,
                password TEXT,
                country TEXT,
                category TEXT,
                label TEXT,
                raw_input TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                last_latency_ms INTEGER,
                last_tested_at INTEGER,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                total_streams INTEGER NOT NULL DEFAULT 0,
                total_bytes_relayed INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_proxies_status ON proxies(status);
            CREATE INDEX IF NOT EXISTS idx_proxies_country ON proxies(country);
            CREATE INDEX IF NOT EXISTS idx_proxies_category ON proxies(category);
            CREATE INDEX IF NOT EXISTS idx_proxies_latency ON proxies(last_latency_ms);
            CREATE INDEX IF NOT EXISTS idx_proxies_host ON proxies(host);
            CREATE INDEX IF NOT EXISTS idx_proxies_created ON proxies(created_at);
            ",
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path,
        })
    }

    /// Open an in-memory database (useful for automated testing).
    #[allow(dead_code)]
    pub fn memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(
            "
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path_id TEXT UNIQUE NOT NULL,
                address TEXT NOT NULL,
                host TEXT NOT NULL,
                port INTEGER NOT NULL,
                username TEXT,
                password TEXT,
                country TEXT,
                category TEXT,
                label TEXT,
                raw_input TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                last_latency_ms INTEGER,
                last_tested_at INTEGER,
                consecutive_failures INTEGER NOT NULL DEFAULT 0,
                total_streams INTEGER NOT NULL DEFAULT 0,
                total_bytes_relayed INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_proxies_status ON proxies(status);
            CREATE INDEX IF NOT EXISTS idx_proxies_country ON proxies(country);
            CREATE INDEX IF NOT EXISTS idx_proxies_category ON proxies(category);
            CREATE INDEX IF NOT EXISTS idx_proxies_latency ON proxies(last_latency_ms);
            ",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path: PathBuf::from(":memory:"),
        })
    }

    /// Return the database file path.
    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// Insert a batch of parsed proxies in a single transaction.
    /// Deduplicates against existing proxies by address + username.
    pub fn insert_batch(&self, proxies: &[ParsedProxy]) -> Result<(usize, usize)> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut inserted = 0;
        let mut duplicates = 0;

        {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO proxies (
                    path_id, address, host, port, username, password,
                    country, category, label, raw_input, status, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'active', ?11)
                ON CONFLICT(path_id) DO NOTHING"
            )?;

            for p in proxies {
                // Generate a deterministic path_id based on address + username to prevent duplicate paths
                let dedup_key = format!("{}|{}", p.address.to_lowercase(), p.username.as_deref().unwrap_or(""));
                let hash = sha2::Sha256::digest(dedup_key.as_bytes());
                let path_id = format!("p_{}", hex::encode(&hash[..8]));

                let rows = stmt.execute(params![
                    path_id,
                    p.address,
                    p.host,
                    p.port,
                    p.username,
                    p.password,
                    p.country,
                    p.proxy_category,
                    p.label,
                    p.raw_input,
                    now,
                ])?;

                if rows > 0 {
                    inserted += 1;
                } else {
                    duplicates += 1;
                }
            }
        }

        tx.commit()?;
        Ok((inserted, duplicates))
    }

    /// Query proxies with server-side pagination, searching, and filtering.
    pub fn query_proxies(&self, filter: &ProxyQueryFilter) -> Result<PaginatedProxies> {
        let conn = self.conn.lock().unwrap();

        let page = filter.page.unwrap_or(1).max(1);
        let limit = filter.limit.unwrap_or(50).clamp(1, 1000);
        let offset = (page - 1) * limit;

        // Total count in database
        let total: usize = conn.query_row("SELECT COUNT(*) FROM proxies", [], |r| r.get(0))?;

        // Build dynamic WHERE clause
        let mut conditions = Vec::new();
        let mut sql_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(ref search) = filter.search {
            let trimmed = search.trim();
            if !trimmed.is_empty() {
                let pattern = format!("%{}%", trimmed);
                sql_params.push(Box::new(pattern.clone()));
                sql_params.push(Box::new(pattern.clone()));
                sql_params.push(Box::new(pattern));
                conditions.push("(address LIKE ? OR username LIKE ? OR label LIKE ?)");
            }
        }

        if let Some(ref status) = filter.status {
            if status != "all" && !status.is_empty() {
                sql_params.push(Box::new(status.clone()));
                conditions.push("status = ?");
            }
        }

        if let Some(ref country) = filter.country {
            if country != "all" && !country.is_empty() {
                if country == "WW" || country == "worldwide" || country == "none" {
                    conditions.push("(country IS NULL OR country = 'worldwide' OR country = 'WW')");
                } else {
                    sql_params.push(Box::new(country.clone()));
                    conditions.push("country = ?");
                }
            }
        }

        if let Some(ref category) = filter.category {
            if category != "all" && !category.is_empty() {
                sql_params.push(Box::new(category.clone()));
                conditions.push("category = ?");
            }
        }

        if let Some(max_lat) = filter.max_latency {
            sql_params.push(Box::new(max_lat as i64));
            conditions.push("last_latency_ms <= ?");
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        // Filtered count query
        let count_sql = format!("SELECT COUNT(*) FROM proxies {}", where_clause);
        let mut count_stmt = conn.prepare(&count_sql)?;
        let count_params: Vec<&dyn rusqlite::ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();
        let filtered: usize = count_stmt.query_row(count_params.as_slice(), |r| r.get(0))?;

        // Determine sort order
        let sort_col = match filter.sort_by.as_deref() {
            Some("address") => "address",
            Some("host") => "host",
            Some("country") => "country",
            Some("category") => "category",
            Some("status") => "status",
            Some("latency") | Some("last_latency_ms") => "last_latency_ms",
            Some("streams") | Some("total_streams") => "total_streams",
            Some("created_at") => "created_at",
            _ => "id",
        };

        let sort_dir = match filter.sort_dir.as_deref() {
            Some("desc") | Some("DESC") => "DESC",
            _ => "ASC",
        };

        // Fetch paginated rows
        let select_sql = format!(
            "SELECT id, path_id, address, host, port, username, password,
                    country, category, label, raw_input, status,
                    last_latency_ms, last_tested_at, consecutive_failures,
                    total_streams, total_bytes_relayed, created_at
             FROM proxies {}
             ORDER BY {} {}
             LIMIT ? OFFSET ?",
            where_clause, sort_col, sort_dir
        );

        let mut select_stmt = conn.prepare(&select_sql)?;
        sql_params.push(Box::new(limit as i64));
        sql_params.push(Box::new(offset as i64));
        let full_params: Vec<&dyn rusqlite::ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

        let items_iter = select_stmt.query_map(full_params.as_slice(), |r| {
            let lat_opt: Option<i64> = r.get(12)?;
            Ok(ProxyRecord {
                id: r.get(0)?,
                path_id: r.get(1)?,
                address: r.get(2)?,
                host: r.get(3)?,
                port: r.get(4)?,
                username: r.get(5)?,
                password: r.get(6)?,
                country: r.get(7)?,
                category: r.get(8)?,
                label: r.get(9)?,
                raw_input: r.get(10)?,
                status: r.get(11)?,
                last_latency_ms: lat_opt.map(|l| l as u64),
                last_tested_at: r.get(13)?,
                consecutive_failures: r.get::<_, i64>(14)? as u32,
                total_streams: r.get::<_, i64>(15)? as u64,
                total_bytes_relayed: r.get::<_, i64>(16)? as u64,
                created_at: r.get(17)?,
            })
        })?;

        let mut items = Vec::new();
        for item in items_iter {
            items.push(item?);
        }

        let total_pages = (filtered + limit - 1) / limit;

        Ok(PaginatedProxies {
            items,
            total,
            filtered,
            page,
            limit,
            total_pages: total_pages.max(1),
        })
    }

    /// Toggle status of an individual proxy by ID (between 'active' and 'paused').
    /// Returns the updated status and the proxy record.
    pub fn toggle_proxy_status(&self, id: i64) -> Result<(String, String)> {
        let conn = self.conn.lock().unwrap();
        let (current_status, path_id): (String, String) = conn.query_row(
            "SELECT status, path_id FROM proxies WHERE id = ?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;

        let new_status = if current_status == "active" {
            "paused"
        } else {
            "active"
        };

        conn.execute(
            "UPDATE proxies SET status = ? WHERE id = ?",
            params![new_status, id],
        )?;

        Ok((new_status.to_string(), path_id))
    }

    /// Set status of an individual proxy by ID explicitly.
    pub fn set_proxy_status(&self, id: i64, status: &str) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        let path_id: String = conn.query_row(
            "SELECT path_id FROM proxies WHERE id = ?",
            [id],
            |r| r.get(0),
        )?;

        conn.execute(
            "UPDATE proxies SET status = ? WHERE id = ?",
            params![status, id],
        )?;

        Ok(path_id)
    }

    /// Bulk update status for an explicit list of proxy IDs.
    /// Returns the updated path_ids.
    pub fn bulk_update_status(&self, ids: &[i64], status: &str) -> Result<Vec<String>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut path_ids = Vec::new();
        {
            let mut select_stmt = tx.prepare_cached("SELECT path_id FROM proxies WHERE id = ?")?;
            let mut update_stmt = tx.prepare_cached("UPDATE proxies SET status = ? WHERE id = ?")?;

            for &id in ids {
                if let Ok(pid) = select_stmt.query_row([id], |r| r.get::<_, String>(0)) {
                    update_stmt.execute(params![status, id])?;
                    path_ids.push(pid);
                }
            }
        }

        tx.commit()?;
        Ok(path_ids)
    }

    /// Bulk update status for all proxies matching a query filter.
    pub fn bulk_update_by_filter(&self, filter: &ProxyQueryFilter, status: &str) -> Result<Vec<String>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut conditions = Vec::new();
        let mut sql_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(ref search) = filter.search {
            let trimmed = search.trim();
            if !trimmed.is_empty() {
                let pattern = format!("%{}%", trimmed);
                sql_params.push(Box::new(pattern.clone()));
                sql_params.push(Box::new(pattern.clone()));
                sql_params.push(Box::new(pattern));
                conditions.push("(address LIKE ? OR username LIKE ? OR label LIKE ?)");
            }
        }

        if let Some(ref s) = filter.status {
            if s != "all" && !s.is_empty() {
                sql_params.push(Box::new(s.clone()));
                conditions.push("status = ?");
            }
        }

        if let Some(ref country) = filter.country {
            if country != "all" && !country.is_empty() {
                if country == "WW" || country == "worldwide" || country == "none" {
                    conditions.push("(country IS NULL OR country = 'worldwide' OR country = 'WW')");
                } else {
                    sql_params.push(Box::new(country.clone()));
                    conditions.push("country = ?");
                }
            }
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let mut path_ids = Vec::new();
        {
            let select_sql = format!("SELECT id, path_id FROM proxies {}", where_clause);
            let mut select_stmt = tx.prepare(&select_sql)?;
            let query_params: Vec<&dyn rusqlite::ToSql> = sql_params.iter().map(|b| b.as_ref()).collect();

            let rows = select_stmt.query_map(query_params.as_slice(), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            })?;

            let mut matched = Vec::new();
            for r in rows {
                matched.push(r?);
            }

            let mut update_stmt = tx.prepare_cached("UPDATE proxies SET status = ? WHERE id = ?")?;
            for (id, pid) in matched {
                update_stmt.execute(params![status, id])?;
                path_ids.push(pid);
            }
        }

        tx.commit()?;
        Ok(path_ids)
    }

    /// Delete a proxy by ID.
    pub fn delete_proxy(&self, id: i64) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let path_id: Option<String> = conn
            .query_row("SELECT path_id FROM proxies WHERE id = ?", [id], |r| r.get(0))
            .ok();

        if path_id.is_some() {
            conn.execute("DELETE FROM proxies WHERE id = ?", [id])?;
        }

        Ok(path_id)
    }

    /// Record the result of a latency and handshake probe.
    pub fn update_proxy_test_result(
        &self,
        id: i64,
        latency_ms: Option<u64>,
        is_success: bool,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if is_success {
            conn.execute(
                "UPDATE proxies SET
                    last_latency_ms = ?1,
                    last_tested_at = ?2,
                    consecutive_failures = 0,
                    status = CASE WHEN status = 'error' THEN 'active' ELSE status END
                 WHERE id = ?3",
                params![latency_ms.map(|l| l as i64), now, id],
            )?;
        } else {
            conn.execute(
                "UPDATE proxies SET
                    last_tested_at = ?1,
                    consecutive_failures = consecutive_failures + 1,
                    status = CASE WHEN consecutive_failures + 1 >= 3 THEN 'error' ELSE status END
                 WHERE id = ?2",
                params![now, id],
            )?;
        }

        Ok(())
    }

    /// Fetch all active proxies for populating the hot seller relay routing table.
    pub fn get_active_proxies(&self) -> Result<Vec<(String, crate::UpstreamProxy)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT path_id, address, username, password, country, category, label
             FROM proxies WHERE status = 'active'"
        )?;

        let iter = stmt.query_map([], |r| {
            let path_id: String = r.get(0)?;
            let address: String = r.get(1)?;
            let username: Option<String> = r.get(2)?;
            let password: Option<String> = r.get(3)?;
            let country: Option<String> = r.get(4)?;
            let proxy_category: Option<String> = r.get(5)?;
            let label: Option<String> = r.get(6)?;

            Ok((
                path_id,
                crate::UpstreamProxy {
                    address,
                    username,
                    password,
                    country,
                    proxy_category,
                    label,
                },
            ))
        })?;

        let mut results = Vec::new();
        for item in iter {
            results.push(item?);
        }
        Ok(results)
    }

    /// Get aggregated metrics for dashboard summary cards.
    pub fn get_aggregate_stats(&self) -> Result<AggregateStats> {
        let conn = self.conn.lock().unwrap();

        let total_proxies: usize = conn.query_row("SELECT COUNT(*) FROM proxies", [], |r| r.get(0))?;
        let active_proxies: usize = conn.query_row("SELECT COUNT(*) FROM proxies WHERE status = 'active'", [], |r| r.get(0))?;
        let paused_proxies: usize = conn.query_row("SELECT COUNT(*) FROM proxies WHERE status = 'paused'", [], |r| r.get(0))?;
        let error_proxies: usize = conn.query_row("SELECT COUNT(*) FROM proxies WHERE status = 'error'", [], |r| r.get(0))?;
        let tested_proxies: usize = conn.query_row("SELECT COUNT(*) FROM proxies WHERE last_tested_at IS NOT NULL", [], |r| r.get(0))?;

        let avg_latency: Option<f64> = conn.query_row(
            "SELECT AVG(last_latency_ms) FROM proxies WHERE last_latency_ms IS NOT NULL",
            [],
            |r| r.get(0),
        ).unwrap_or(None);

        let countries_count: usize = conn.query_row(
            "SELECT COUNT(DISTINCT country) FROM proxies WHERE country IS NOT NULL",
            [],
            |r| r.get(0),
        )?;

        // Top 10 countries
        let mut top_stmt = conn.prepare(
            "SELECT COALESCE(country, 'Worldwide'), COUNT(*) as c
             FROM proxies
             GROUP BY country
             ORDER BY c DESC
             LIMIT 10"
        )?;

        let top_iter = top_stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, usize>(1)?))
        })?;

        let mut top_countries = Vec::new();
        for item in top_iter {
            top_countries.push(item?);
        }

        Ok(AggregateStats {
            total_proxies,
            active_proxies,
            paused_proxies,
            error_proxies,
            tested_proxies,
            avg_latency_ms: avg_latency.map(|l| l.round() as u64),
            countries_count,
            top_countries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_proxy(host: &str, port: u16, user: Option<&str>, country: Option<&str>) -> ParsedProxy {
        ParsedProxy {
            address: format!("{}:{}", host, port),
            host: host.to_string(),
            port,
            username: user.map(|s| s.to_string()),
            password: Some("secret123".to_string()),
            country: country.map(|s| s.to_string()),
            proxy_category: Some("residential".to_string()),
            label: Some("test_node".to_string()),
            source_line: Some(1),
            raw_input: format!("{}:{}:{}:secret123", host, port, user.unwrap_or("")),
        }
    }

    #[test]
    fn test_batch_insert_and_dedup() {
        let db = WebloadDb::memory().unwrap();

        let proxies = vec![
            make_test_proxy("1.1.1.1", 1080, Some("user1"), Some("US")),
            make_test_proxy("1.1.1.2", 1080, Some("user2"), Some("DE")),
            make_test_proxy("1.1.1.1", 1080, Some("user1"), Some("US")), // duplicate
        ];

        let (inserted, dups) = db.insert_batch(&proxies).unwrap();
        assert_eq!(inserted, 2);
        assert_eq!(dups, 1);

        let stats = db.get_aggregate_stats().unwrap();
        assert_eq!(stats.total_proxies, 2);
        assert_eq!(stats.active_proxies, 2);
    }

    #[test]
    fn test_pagination_and_filtering() {
        let db = WebloadDb::memory().unwrap();

        let mut proxies = Vec::new();
        for i in 1..=25 {
            let cc = if i <= 15 { "US" } else { "DE" };
            proxies.push(make_test_proxy(&format!("10.0.0.{}", i), 1080, Some(&format!("user_{}", i)), Some(cc)));
        }
        db.insert_batch(&proxies).unwrap();

        // Page 1 with limit 10
        let filter1 = ProxyQueryFilter {
            page: Some(1),
            limit: Some(10),
            ..Default::default()
        };
        let res1 = db.query_proxies(&filter1).unwrap();
        assert_eq!(res1.items.len(), 10);
        assert_eq!(res1.total, 25);
        assert_eq!(res1.total_pages, 3);

        // Filter by country US
        let filter_us = ProxyQueryFilter {
            country: Some("US".to_string()),
            limit: Some(50),
            ..Default::default()
        };
        let res_us = db.query_proxies(&filter_us).unwrap();
        assert_eq!(res_us.items.len(), 15);
        assert_eq!(res_us.filtered, 15);

        // Search by IP
        let filter_search = ProxyQueryFilter {
            search: Some("10.0.0.12".to_string()),
            ..Default::default()
        };
        let res_search = db.query_proxies(&filter_search).unwrap();
        assert_eq!(res_search.items.len(), 1);
        assert_eq!(res_search.items[0].address, "10.0.0.12:1080");
    }

    #[test]
    fn test_individual_toggle_and_bulk() {
        let db = WebloadDb::memory().unwrap();

        let proxies = vec![
            make_test_proxy("2.2.2.1", 1080, Some("u1"), Some("US")),
            make_test_proxy("2.2.2.2", 1080, Some("u2"), Some("US")),
        ];
        db.insert_batch(&proxies).unwrap();

        let list = db.query_proxies(&ProxyQueryFilter::default()).unwrap();
        let id1 = list.items[0].id;
        let id2 = list.items[1].id;

        // Toggle proxy 1 to paused
        let (new_status, _) = db.toggle_proxy_status(id1).unwrap();
        assert_eq!(new_status, "paused");

        let stats = db.get_aggregate_stats().unwrap();
        assert_eq!(stats.active_proxies, 1);
        assert_eq!(stats.paused_proxies, 1);

        // Toggle proxy 1 back to active
        let (resumed_status, _) = db.toggle_proxy_status(id1).unwrap();
        assert_eq!(resumed_status, "active");

        // Bulk update both to paused
        db.bulk_update_status(&[id1, id2], "paused").unwrap();
        let stats_paused = db.get_aggregate_stats().unwrap();
        assert_eq!(stats_paused.paused_proxies, 2);
        assert_eq!(stats_paused.active_proxies, 0);
    }
}

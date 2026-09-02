use std::io::BufRead;
use std::path::Path;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Fully parsed and validated upstream SOCKS5 proxy configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedProxy {
    /// Normalized SOCKS5 socket address "host:port" or "[ipv6]:port"
    pub address: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Inferred or explicit country code (e.g., "US", "DE")
    pub country: Option<String>,
    /// Inferred or explicit category (e.g., "residential", "datacenter", "mobile")
    pub proxy_category: Option<String>,
    /// Optional human-readable tag or path label
    pub label: Option<String>,
    /// Line number in source file (if parsed from file)
    pub source_line: Option<usize>,
    /// Original raw line string
    pub raw_input: String,
}

/// Report generated when loading proxies from a file or stream.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ProxyLoadReport {
    pub proxies: Vec<ParsedProxy>,
    pub total_lines: usize,
    pub parsed_count: usize,
    pub skipped_empty_or_comments: usize,
    pub warnings: Vec<ProxyParseWarning>,
    pub duplicates_deduplicated: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProxyParseWarning {
    pub line_number: usize,
    pub raw_line: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyParseError {
    InvalidHost(String),
    InvalidPort(String),
    UnsupportedScheme(String),
    Malformed(String),
}

impl std::fmt::Display for ProxyParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProxyParseError::InvalidHost(h) => write!(f, "Invalid or missing host: {}", h),
            ProxyParseError::InvalidPort(p) => write!(f, "Invalid port: {}", p),
            ProxyParseError::UnsupportedScheme(s) => {
                write!(f, "Unsupported proxy scheme '{}' (only SOCKS5 is supported)", s)
            }
            ProxyParseError::Malformed(m) => write!(f, "Malformed proxy connection string: {}", m),
        }
    }
}

impl std::error::Error for ProxyParseError {}

/// Parse a single line into a `ParsedProxy`.
/// Returns `Ok(None)` if the line is empty or a comment.
pub fn parse_proxy_line(raw_line: &str) -> Result<Option<ParsedProxy>, ProxyParseError> {
    // 1. Sanitize line: strip BOM, trim whitespace
    let trimmed = raw_line.trim_start_matches('\u{feff}').trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    // 2. Whole-line comments (# or //)
    if trimmed.starts_with('#') || trimmed.starts_with("//") {
        return Ok(None);
    }

    // 3. Extract trailing comment or label (e.g. "host:port:user:pass # label" or "... // comment")
    let (line_without_comment, inline_label) = split_inline_comment_or_label(trimmed);
    let line = line_without_comment.trim();
    if line.is_empty() {
        return Ok(None);
    }

    // 4. Try URI Scheme (socks5://, socks5h://, socks://)
    if let Some(parsed) = try_parse_uri(line, inline_label.as_deref(), raw_line)? {
        return Ok(Some(parsed));
    }

    // 5. Try '@' Delimited (user:pass@host:port or host:port@user:pass)
    if line.contains('@') {
        if let Some(parsed) = try_parse_at_delimited(line, inline_label.as_deref(), raw_line)? {
            return Ok(Some(parsed));
        }
    }

    // 6. Try Delimited Tuple (:, ;, |, \t, ,)
    if let Some(parsed) = try_parse_delimited_tuple(line, inline_label.as_deref(), raw_line)? {
        return Ok(Some(parsed));
    }

    Err(ProxyParseError::Malformed(format!(
        "Unable to recognize format for '{}'",
        raw_line
    )))
}

/// Parse upstream proxies from any `BufRead` reader (file, stdin, cursor).
pub fn parse_proxy_reader<R: BufRead>(reader: R) -> Result<ProxyLoadReport> {
    let mut report = ProxyLoadReport::default();
    let mut seen_keys = std::collections::HashSet::new();

    for (idx, line_res) in reader.lines().enumerate() {
        let line_num = idx + 1;
        report.total_lines += 1;
        let line = line_res.context(format!("Failed to read line {}", line_num))?;

        match parse_proxy_line(&line) {
            Ok(Some(mut proxy)) => {
                proxy.source_line = Some(line_num);
                // Deduplicate by address + username
                let dedup_key = format!(
                    "{}|{}",
                    proxy.address.to_lowercase(),
                    proxy.username.as_deref().unwrap_or("")
                );
                if seen_keys.insert(dedup_key) {
                    report.proxies.push(proxy);
                    report.parsed_count += 1;
                } else {
                    report.duplicates_deduplicated += 1;
                }
            }
            Ok(None) => {
                report.skipped_empty_or_comments += 1;
            }
            Err(e) => {
                report.warnings.push(ProxyParseWarning {
                    line_number: line_num,
                    raw_line: line,
                    reason: e.to_string(),
                });
            }
        }
    }

    Ok(report)
}

/// Parse upstream proxies from a file path.
pub fn parse_proxy_file<P: AsRef<Path>>(path: P) -> Result<ProxyLoadReport> {
    let file = std::fs::File::open(path.as_ref())
        .with_context(|| format!("Failed to open proxy file: {}", path.as_ref().display()))?;
    let reader = std::io::BufReader::new(file);
    parse_proxy_reader(reader)
}

/// Parse upstream proxies from standard input.
pub fn parse_proxy_stdin() -> Result<ProxyLoadReport> {
    let stdin = std::io::stdin();
    let reader = stdin.lock();
    parse_proxy_reader(reader)
}

// ---------------------------------------------------------------------------
// Internal parsing helpers
// ---------------------------------------------------------------------------

/// Split trailing inline comments or labels (`# tag` or `// comment`).
/// Ignores `#` if it's inside a URI query or auth.
fn split_inline_comment_or_label(s: &str) -> (&str, Option<String>) {
    // If it's a URI, URL parser handles fragments/query params directly
    if s.contains("://") {
        return (s, None);
    }

    // Check for inline '#'
    if let Some(pos) = s.find('#') {
        let content = &s[..pos];
        let label = s[pos + 1..].trim();
        let label_opt = if !label.is_empty() {
            Some(label.to_string())
        } else {
            None
        };
        return (content, label_opt);
    }

    // Check for inline '//' with preceding whitespace
    if let Some(pos) = s.find(" //") {
        let content = &s[..pos];
        let label = s[pos + 3..].trim();
        let label_opt = if !label.is_empty() {
            Some(label.to_string())
        } else {
            None
        };
        return (content, label_opt);
    }

    (s, None)
}

/// Parse URI schemes (socks5://, socks5h://, socks://)
fn try_parse_uri(
    s: &str,
    inline_label: Option<&str>,
    raw_input: &str,
) -> Result<Option<ParsedProxy>, ProxyParseError> {
    if !s.contains("://") {
        return Ok(None);
    }

    let url = url::Url::parse(s).map_err(|e| ProxyParseError::Malformed(format!("URL parse error: {}", e)))?;
    let scheme = url.scheme().to_lowercase();

    match scheme.as_str() {
        "socks5" | "socks5h" | "socks" => {}
        "http" | "https" | "socks4" | "socks4a" => {
            return Err(ProxyParseError::UnsupportedScheme(scheme));
        }
        _ => return Ok(None),
    }

    let host = url
        .host_str()
        .ok_or_else(|| ProxyParseError::InvalidHost("Missing host in URL".to_string()))?
        .to_string();

    let port = url.port().unwrap_or(1080);

    let username = if url.username().is_empty() {
        None
    } else {
        Some(percent_decode(url.username()))
    };

    let password = url.password().map(percent_decode);

    // Metadata extraction from query params and username
    let mut country = None;
    let mut proxy_category = None;
    let mut label = inline_label.map(|l| l.to_string()).or_else(|| url.fragment().map(|f| f.to_string()));

    for (k, v) in url.query_pairs() {
        match k.to_lowercase().as_str() {
            "country" | "cc" | "geo" => country = Some(v.to_uppercase()),
            "type" | "category" | "net" => proxy_category = Some(v.to_lowercase()),
            "label" | "name" | "tag" if label.is_none() => label = Some(v.to_string()),
            _ => {}
        }
    }

    if let Some(ref u) = username {
        let (meta_country, meta_cat) = parse_upstream_metadata(u);
        if country.is_none() {
            country = meta_country;
        }
        if proxy_category.is_none() {
            proxy_category = meta_cat;
        }
    }

    let address = format_address(&host, port);

    Ok(Some(ParsedProxy {
        address,
        host,
        port,
        username,
        password,
        country,
        proxy_category,
        label,
        source_line: None,
        raw_input: raw_input.to_string(),
    }))
}

/// Parse formats containing `@`:
/// `user:pass@host:port` OR `host:port@user:pass`
fn try_parse_at_delimited(
    s: &str,
    inline_label: Option<&str>,
    raw_input: &str,
) -> Result<Option<ParsedProxy>, ProxyParseError> {
    let parts: Vec<&str> = s.splitn(2, '@').collect();
    if parts.len() != 2 {
        return Ok(None);
    }

    let left = parts[0].trim();
    let right = parts[1].trim();

    // Case 1: Standard user:pass@host:port (right side is host:port)
    if let Some((host, port)) = parse_host_port(right) {
        let (username, password) = parse_user_pass(left);
        let (country, proxy_category) = username
            .as_deref()
            .map(parse_upstream_metadata)
            .unwrap_or((None, None));
        let address = format_address(&host, port);
        return Ok(Some(ParsedProxy {
            address,
            host,
            port,
            username,
            password,
            country,
            proxy_category,
            label: inline_label.map(|l| l.to_string()),
            source_line: None,
            raw_input: raw_input.to_string(),
        }));
    }

    // Case 2: Inverted host:port@user:pass (left side is host:port)
    if let Some((host, port)) = parse_host_port(left) {
        let (username, password) = parse_user_pass(right);
        let (country, proxy_category) = username
            .as_deref()
            .map(parse_upstream_metadata)
            .unwrap_or((None, None));
        let address = format_address(&host, port);
        return Ok(Some(ParsedProxy {
            address,
            host,
            port,
            username,
            password,
            country,
            proxy_category,
            label: inline_label.map(|l| l.to_string()),
            source_line: None,
            raw_input: raw_input.to_string(),
        }));
    }

    Ok(None)
}

/// Parse delimited tuples:
/// - 2 parts: `host:port`
/// - 4 parts: `host:port:user:pass` OR `user:pass:host:port`
/// - Delimiters: `:`, `;`, `|`, `\t`, `,`
fn try_parse_delimited_tuple(
    s: &str,
    inline_label: Option<&str>,
    raw_input: &str,
) -> Result<Option<ParsedProxy>, ProxyParseError> {
    // Check for IPv6 host with bracket: e.g. `[2001:db8::1]:1080:user:pass` or `[::1]:1080`
    if s.starts_with('[') {
        if let Some(close_bracket) = s.find(']') {
            let host = &s[1..close_bracket];
            let remainder = &s[close_bracket + 1..];
            if remainder.starts_with(':') {
                let rest = &remainder[1..];
                let subparts: Vec<&str> = rest.split(':').collect();
                if !subparts.is_empty() {
                    let port = subparts[0]
                        .parse::<u16>()
                        .map_err(|_| ProxyParseError::InvalidPort(subparts[0].to_string()))?;
                    let (username, password) = match subparts.len() {
                        1 => (None, None),
                        2 => (Some(subparts[1].to_string()), None),
                        _ => {
                            let u = subparts[1].to_string();
                            let p = subparts[2..].join(":");
                            (Some(u), Some(p))
                        }
                    };
                    let (country, proxy_category) = username
                        .as_deref()
                        .map(parse_upstream_metadata)
                        .unwrap_or((None, None));
                    let address = format_address(host, port);
                    return Ok(Some(ParsedProxy {
                        address,
                        host: host.to_string(),
                        port,
                        username,
                        password,
                        country,
                        proxy_category,
                        label: inline_label.map(|l| l.to_string()),
                        source_line: None,
                        raw_input: raw_input.to_string(),
                    }));
                }
            }
        }
    }

    // Determine active delimiter if alternative delimiter used
    let delimiter = if s.contains(';') && s.matches(';').count() >= 3 {
        ';'
    } else if s.contains('|') && s.matches('|').count() >= 3 {
        '|'
    } else if s.contains('\t') && s.matches('\t').count() >= 1 {
        '\t'
    } else if s.contains(',') && s.matches(',').count() >= 3 && !s.contains("country_") {
        ','
    } else {
        ':'
    };

    let parts: Vec<&str> = if delimiter == ':' {
        // For colon delimiter, split into parts
        s.split(':').collect()
    } else {
        s.split(delimiter).collect()
    };

    if parts.len() < 2 {
        return Ok(None);
    }

    // 2-part: host:port
    if parts.len() == 2 {
        let host = parts[0].trim();
        let port_str = parts[1].trim();
        let port = port_str
            .parse::<u16>()
            .map_err(|_| ProxyParseError::InvalidPort(port_str.to_string()))?;
        if host.is_empty() {
            return Err(ProxyParseError::InvalidHost("Empty host".to_string()));
        }
        let address = format_address(host, port);
        return Ok(Some(ParsedProxy {
            address,
            host: host.to_string(),
            port,
            username: None,
            password: None,
            country: None,
            proxy_category: None,
            label: inline_label.map(|l| l.to_string()),
            source_line: None,
            raw_input: raw_input.to_string(),
        }));
    }

    // 3-part: host:port:user
    if parts.len() == 3 {
        let host = parts[0].trim();
        let port_str = parts[1].trim();
        if let Ok(port) = port_str.parse::<u16>() {
            let user = parts[2].trim().to_string();
            let (country, proxy_category) = parse_upstream_metadata(&user);
            let address = format_address(host, port);
            return Ok(Some(ParsedProxy {
                address,
                host: host.to_string(),
                port,
                username: Some(user),
                password: None,
                country,
                proxy_category,
                label: inline_label.map(|l| l.to_string()),
                source_line: None,
                raw_input: raw_input.to_string(),
            }));
        }
    }

    // 4 or more parts:
    // Disambiguate host:port:user:pass vs user:pass:host:port
    let p0 = parts[0].trim();
    let p1 = parts[1].trim();
    let p2 = parts[2].trim();
    let p3 = parts[3].trim();

    let p1_port = p1.parse::<u16>().ok().filter(|&p| p > 0);
    let p3_port = p3.parse::<u16>().ok().filter(|&p| p > 0);

    let is_host_first = match (p1_port, p3_port) {
        (Some(_), None) => true,
        (None, Some(_)) => false,
        (Some(_), Some(_)) => {
            // Both are numbers! Apply heuristic:
            // Is p0 IP/domain? Is p2 IP/domain?
            let p0_is_host = is_ip_or_domain(p0);
            let p2_is_host = is_ip_or_domain(p2);
            if p0_is_host && !p2_is_host {
                true
            } else if !p0_is_host && p2_is_host {
                false
            } else {
                // Default to standard industry format (host:port:user:pass)
                true
            }
        }
        (None, None) => {
            return Err(ProxyParseError::InvalidPort(format!(
                "Neither '{}' nor '{}' is a valid port number",
                p1, p3
            )));
        }
    };

    if is_host_first {
        let host = p0;
        let port = p1
            .parse::<u16>()
            .map_err(|_| ProxyParseError::InvalidPort(p1.to_string()))?;
        let user = p2.to_string();
        // If there were more than 4 colon parts, rejoin remaining parts as password
        let pass = if parts.len() > 4 && delimiter == ':' {
            parts[3..].join(":")
        } else {
            p3.to_string()
        };

        let (country, proxy_category) = parse_upstream_metadata(&user);
        let address = format_address(host, port);
        return Ok(Some(ParsedProxy {
            address,
            host: host.to_string(),
            port,
            username: Some(user),
            password: Some(pass),
            country,
            proxy_category,
            label: inline_label.map(|l| l.to_string()),
            source_line: None,
            raw_input: raw_input.to_string(),
        }));
    } else {
        // user:pass:host:port
        let user = p0.to_string();
        let pass = p1.to_string();
        let host = p2;
        let port = p3
            .parse::<u16>()
            .map_err(|_| ProxyParseError::InvalidPort(p3.to_string()))?;

        let (country, proxy_category) = parse_upstream_metadata(&user);
        let address = format_address(host, port);
        return Ok(Some(ParsedProxy {
            address,
            host: host.to_string(),
            port,
            username: Some(user),
            password: Some(pass),
            country,
            proxy_category,
            label: inline_label.map(|l| l.to_string()),
            source_line: None,
            raw_input: raw_input.to_string(),
        }));
    }
}

/// Helper to parse `host:port` or `[ipv6]:port`.
fn parse_host_port(s: &str) -> Option<(String, u16)> {
    let s = s.trim();
    if s.starts_with('[') {
        let close = s.find(']')?;
        let host = &s[1..close];
        let rest = &s[close + 1..];
        if let Some(port_str) = rest.strip_prefix(':') {
            let port = port_str.parse::<u16>().ok().filter(|&p| p > 0)?;
            return Some((host.to_string(), port));
        }
        return None;
    }

    let last_colon = s.rfind(':')?;
    let host = &s[..last_colon];
    let port_str = &s[last_colon + 1..];
    let port = port_str.parse::<u16>().ok().filter(|&p| p > 0)?;
    if host.is_empty() {
        return None;
    }
    Some((host.to_string(), port))
}

/// Helper to parse `username:password` or `username`.
fn parse_user_pass(s: &str) -> (Option<String>, Option<String>) {
    let s = s.trim();
    if s.is_empty() {
        return (None, None);
    }
    if let Some(pos) = s.find(':') {
        let u = &s[..pos];
        let p = &s[pos + 1..];
        (Some(u.to_string()), Some(p.to_string()))
    } else {
        (Some(s.to_string()), None)
    }
}

/// Format host and port, enclosing IPv6 in brackets if needed.
fn format_address(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

/// Heuristic check if string looks like an IP address or domain name.
fn is_ip_or_domain(s: &str) -> bool {
    // IPv4 check
    if s.chars().all(|c| c.is_ascii_digit() || c == '.') && s.matches('.').count() == 3 {
        return true;
    }
    // IPv6 check
    if s.contains(':') {
        return true;
    }
    // Domain check (has dot, ends with common TLD or alphanumeric)
    if s.contains('.') && !s.starts_with('.') && !s.ends_with('.') {
        return true;
    }
    // Localhost
    if s.eq_ignore_ascii_case("localhost") {
        return true;
    }
    false
}

/// Percent-decode a URL component.
fn percent_decode(s: &str) -> String {
    url::form_urlencoded::parse(s.as_bytes())
        .map(|(k, _)| k.to_string())
        .next()
        .unwrap_or_else(|| s.to_string())
}

/// Parse country and proxy_category from an upstream username.
/// Format: `user_2930d5,type_residential,country_US,session_usresidential`
/// Extracts: country="US", proxy_category="residential"
pub fn parse_upstream_metadata(username: &str) -> (Option<String>, Option<String>) {
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
// Unit Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uri_standard_socks5() {
        let line = "socks5://myuser:mypass@proxy.example.com:1080";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "proxy.example.com:1080");
        assert_eq!(p.host, "proxy.example.com");
        assert_eq!(p.port, 1080);
        assert_eq!(p.username.as_deref(), Some("myuser"));
        assert_eq!(p.password.as_deref(), Some("mypass"));
    }

    #[test]
    fn test_uri_socks5h_unauthenticated() {
        let line = "socks5h://127.0.0.1:9050";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "127.0.0.1:9050");
        assert_eq!(p.host, "127.0.0.1");
        assert_eq!(p.port, 9050);
        assert_eq!(p.username, None);
        assert_eq!(p.password, None);
    }

    #[test]
    fn test_uri_with_fragment_and_query() {
        let line = "socks5://user_abc,country_US,type_residential:pass123@gate.vendor.com:8000?type=datacenter#node_01";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "gate.vendor.com:8000");
        assert_eq!(p.country.as_deref(), Some("US"));
        // Query param overrides username metadata
        assert_eq!(p.proxy_category.as_deref(), Some("datacenter"));
        assert_eq!(p.label.as_deref(), Some("node_01"));
    }

    #[test]
    fn test_uri_percent_encoded() {
        let line = "socks5://user%40mail.com:p%40ss%3Aword@1.2.3.4:1080";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.username.as_deref(), Some("user@mail.com"));
        assert_eq!(p.password.as_deref(), Some("p@ss:word"));
    }

    #[test]
    fn test_standard_delimited_host_port_user_pass() {
        let line = "192.168.1.100:8000:my_user:my_pass";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "192.168.1.100:8000");
        assert_eq!(p.host, "192.168.1.100");
        assert_eq!(p.port, 8000);
        assert_eq!(p.username.as_deref(), Some("my_user"));
        assert_eq!(p.password.as_deref(), Some("my_pass"));
    }

    #[test]
    fn test_standard_delimited_with_colon_password() {
        let line = "proxy.vendor.com:1080:admin:pass:with:multiple:colons";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "proxy.vendor.com:1080");
        assert_eq!(p.username.as_deref(), Some("admin"));
        assert_eq!(p.password.as_deref(), Some("pass:with:multiple:colons"));
    }

    #[test]
    fn test_auth_first_at_delimited() {
        let line = "john:secret@10.0.0.1:1080";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "10.0.0.1:1080");
        assert_eq!(p.username.as_deref(), Some("john"));
        assert_eq!(p.password.as_deref(), Some("secret"));
    }

    #[test]
    fn test_inverted_delimited_user_pass_host_port() {
        let line = "user123:pass456:residential.proxy.com:8000";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "residential.proxy.com:8000");
        assert_eq!(p.username.as_deref(), Some("user123"));
        assert_eq!(p.password.as_deref(), Some("pass456"));
    }

    #[test]
    fn test_unauthenticated_host_port() {
        let line = "127.0.0.1:9050";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "127.0.0.1:9050");
        assert_eq!(p.username, None);
        assert_eq!(p.password, None);
    }

    #[test]
    fn test_ipv6_proxy() {
        let line = "[2001:db8::1]:1080:myuser:mypass";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.address, "[2001:db8::1]:1080");
        assert_eq!(p.host, "2001:db8::1");
        assert_eq!(p.port, 1080);
        assert_eq!(p.username.as_deref(), Some("myuser"));
        assert_eq!(p.password.as_deref(), Some("mypass"));
    }

    #[test]
    fn test_alternative_delimiters() {
        let semicolon = "1.2.3.4;1080;user;pass";
        let p1 = parse_proxy_line(semicolon).unwrap().expect("Should parse");
        assert_eq!(p1.address, "1.2.3.4:1080");
        assert_eq!(p1.username.as_deref(), Some("user"));

        let pipe = "1.2.3.4|1080|user|pass";
        let p2 = parse_proxy_line(pipe).unwrap().expect("Should parse");
        assert_eq!(p2.address, "1.2.3.4:1080");

        let tsv = "1.2.3.4\t1080\tuser\tpass";
        let p3 = parse_proxy_line(tsv).unwrap().expect("Should parse");
        assert_eq!(p3.address, "1.2.3.4:1080");
    }

    #[test]
    fn test_comments_and_blank_lines() {
        assert!(parse_proxy_line("").unwrap().is_none());
        assert!(parse_proxy_line("   \r\n").unwrap().is_none());
        assert!(parse_proxy_line("# This is a comment").unwrap().is_none());
        assert!(parse_proxy_line("// Another comment").unwrap().is_none());

        let with_inline = "1.2.3.4:1080:usr:pwd # primary US proxy";
        let p = parse_proxy_line(with_inline).unwrap().expect("Should parse");
        assert_eq!(p.address, "1.2.3.4:1080");
        assert_eq!(p.label.as_deref(), Some("primary US proxy"));
    }

    #[test]
    fn test_reader_batch_and_deduplication() {
        let content = r#"
        # Proxy list
        1.1.1.1:1080:u1:p1
        2.2.2.2:1080:u2:p2

        # Duplicate of first proxy
        socks5://u1:p1@1.1.1.1:1080

        // Third proxy
        socks5://3.3.3.3:9050
        "#;

        let report = parse_proxy_reader(content.as_bytes()).unwrap();
        assert_eq!(report.parsed_count, 3);
        assert_eq!(report.duplicates_deduplicated, 1);
        assert_eq!(report.proxies.len(), 3);
        assert_eq!(report.proxies[0].address, "1.1.1.1:1080");
        assert_eq!(report.proxies[1].address, "2.2.2.2:1080");
        assert_eq!(report.proxies[2].address, "3.3.3.3:9050");
    }

    #[test]
    fn test_unsupported_scheme_error() {
        let http_line = "http://user:pass@1.2.3.4:8080";
        let err = parse_proxy_line(http_line).unwrap_err();
        match err {
            ProxyParseError::UnsupportedScheme(s) => assert_eq!(s, "http"),
            _ => panic!("Expected UnsupportedScheme error"),
        }
    }

    #[test]
    fn test_invalid_port_error() {
        let invalid = "1.2.3.4:999999:user:pass";
        let err = parse_proxy_line(invalid).unwrap_err();
        match err {
            ProxyParseError::InvalidPort(_) => {}
            _ => panic!("Expected InvalidPort error"),
        }
    }

    #[test]
    fn test_bom_stripping() {
        let line_with_bom = "\u{feff}socks5://1.2.3.4:1080";
        let p = parse_proxy_line(line_with_bom).unwrap().expect("Should parse with BOM");
        assert_eq!(p.address, "1.2.3.4:1080");
    }

    #[test]
    fn test_query_params_cc_and_geo() {
        let line = "socks5://1.2.3.4:1080?cc=de&net=residential&name=Frankfurt-Node";
        let p = parse_proxy_line(line).unwrap().expect("Should parse");
        assert_eq!(p.country.as_deref(), Some("DE"));
        assert_eq!(p.proxy_category.as_deref(), Some("residential"));
        assert_eq!(p.label.as_deref(), Some("Frankfurt-Node"));
    }
}

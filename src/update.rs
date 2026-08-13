//! Self-update: check GitHub Releases, download the matching raw binary,
//! verify its SHA256 checksum, and atomically replace the running executable.
//!
//! Release profile uses `panic = "abort"` — nothing in this module may panic
//! outside of unit tests.

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const CHECK_INTERVAL_SECS: u64 = 86_400;
const CHECKSUM_FILE: &str = "SHA256SUMS";

#[derive(serde::Deserialize)]
struct GhRelease {
    tag_name: String,
    #[serde(default)]
    assets: Vec<GhAsset>,
}

#[derive(serde::Deserialize)]
struct GhAsset {
    name: String,
    browser_download_url: String,
}

fn split_repo(s: &str) -> Option<(String, String)> {
    let mut parts = s.trim().trim_end_matches('/').rsplit('/').take(2);
    let repo_name = parts.next()?.to_string();
    let owner = parts.next()?.to_string();
    if owner.is_empty() || repo_name.is_empty() {
        return None;
    }
    Some((owner, repo_name))
}

fn repo_override() -> Option<(String, String)> {
    let val = std::env::var("PROXYBASE_UPDATE_REPO").ok()?;
    split_repo(&val)
}

fn repo() -> (String, String) {
    repo_override().unwrap_or_else(|| {
        split_repo(env!("CARGO_PKG_REPOSITORY")).unwrap_or_else(|| {
            ("proxybasehq".to_string(), "proxybase-cli".to_string())
        })
    })
}

fn current_version() -> semver::Version {
    // Compile-time constant from Cargo — always valid semver.
    semver::Version::parse(env!("CARGO_PKG_VERSION")).expect("CARGO_PKG_VERSION must be valid semver")
}

fn parse_tag(tag: &str) -> Option<semver::Version> {
    let v = tag
        .strip_prefix("proxybase-cli-v")
        .or_else(|| tag.strip_prefix('v'))?;
    semver::Version::parse(v).ok()
}

fn is_newer(latest: &semver::Version) -> bool {
    latest.pre.is_empty() && *latest > current_version()
}

fn asset_for(arch: &str, os: &str) -> Option<&'static str> {
    match (arch, os) {
        ("x86_64", "linux") => Some("proxybase-cli-x86_64-unknown-linux-gnu"),
        ("x86_64", "windows") => Some("proxybase-cli-x86_64-pc-windows-msvc.exe"),
        ("aarch64", "macos") => Some("proxybase-cli-aarch64-apple-darwin"),
        _ => None,
    }
}

fn target_asset() -> Option<&'static str> {
    asset_for(std::env::consts::ARCH, std::env::consts::OS)
}

fn parse_sha256sums(text: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for line in text.lines() {
        let mut parts = line.split_whitespace();
        if let (Some(hash), Some(name)) = (parts.next(), parts.next()) {
            if hash.len() == 64 && hash.chars().all(|c| c.is_ascii_hexdigit()) {
                map.insert(name.to_string(), hash.to_lowercase());
            }
        }
    }
    map
}

fn sha256_file(path: &Path) -> Result<String> {
    use sha2::Digest;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        use std::io::Read;
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Remove a leftover `.old` binary from a previous Windows self-replace.
pub fn cleanup_stale() {
    if let Ok(exe) = std::env::current_exe() {
        let old = exe.with_extension("old");
        if old.exists() {
            let _ = std::fs::remove_file(&old);
        }
    }
}

fn http_client() -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .user_agent(format!("proxybase-cli/{}", env!("CARGO_PKG_VERSION")));
    let token = std::env::var("GITHUB_TOKEN")
        .ok()
        .or_else(|| std::env::var("GH_TOKEN").ok());
    if let Some(t) = token {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Ok(v) = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", t)) {
            headers.insert(reqwest::header::AUTHORIZATION, v);
        }
        builder = builder.default_headers(headers);
    }
    Ok(builder.build()?)
}

async fn fetch_latest(client: &reqwest::Client) -> Result<GhRelease> {
    let (owner, repo) = repo();
    let url = format!("https://api.github.com/repos/{}/{}/releases/latest", owner, repo);
    let resp = client
        .get(&url)
        .send()
        .await
        .with_context(|| format!("Failed to query GitHub for latest release of {}/{}", owner, repo))?
        .error_for_status()?;
    Ok(resp.json::<GhRelease>().await?)
}

/// Locate the binary asset for this platform plus the checksum file.
fn find_assets(release: &GhRelease, asset_name: &str) -> Option<(String, String)> {
    let mut asset_url = None;
    let mut sha_url = None;
    for a in &release.assets {
        if a.name == asset_name {
            asset_url = Some(a.browser_download_url.clone());
        } else if a.name == CHECKSUM_FILE {
            sha_url = Some(a.browser_download_url.clone());
        }
    }
    Some((asset_url?, sha_url?))
}

async fn download_asset(client: &reqwest::Client, url: &str, dest: &Path) -> Result<()> {
    let resp = client.get(url).send().await?.error_for_status()?;
    let bytes = resp.bytes().await?;
    std::fs::write(dest, bytes)?;
    Ok(())
}

#[cfg(unix)]
fn copy_permissions(from: &Path, to: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(from) {
        let _ = std::fs::set_permissions(to, std::fs::Permissions::from_mode(meta.permissions().mode()));
    }
}

#[cfg(not(unix))]
fn copy_permissions(_from: &Path, _to: &Path) {}

/// Unix/macOS: rename over the running binary is safe — the running process
/// keeps the old inode.
#[cfg(unix)]
fn replace_binary(temp: &Path, exe: &Path) -> Result<()> {
    std::fs::rename(temp, exe).with_context(|| {
        format!(
            "Failed to replace {} — is the install directory writable? Try running from an elevated shell.",
            exe.display()
        )
    })
}

/// Windows: a running .exe cannot be overwritten. Rename it aside, move the new
/// binary in, and leave the `.old` for cleanup on next startup.
#[cfg(windows)]
fn replace_binary(temp: &Path, exe: &Path) -> Result<()> {
    let old = exe.with_extension("old");
    std::fs::rename(exe, &old).with_context(|| {
        format!(
            "Failed to rename {} — antivirus may be locking the binary. Try running from an elevated shell.",
            exe.display()
        )
    })?;
    if let Err(e) = std::fs::rename(temp, exe) {
        let _ = std::fs::rename(&old, exe);
        return Err(e).with_context(|| "Failed to install the new binary (original restored)");
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn replace_binary(temp: &Path, exe: &Path) -> Result<()> {
    std::fs::rename(temp, exe).with_context(|| "Failed to replace the current binary")
}

/// `proxybase-cli update` — install the latest release (or only check with `--check`).
pub async fn run_update(check_only: bool) -> Result<()> {
    let asset_name = match target_asset() {
        Some(n) => n.to_string(),
        None => {
            println!(
                "No release for your platform ({} {}). Supported: linux x86_64, windows x86_64, macos aarch64 (Apple Silicon).",
                std::env::consts::OS,
                std::env::consts::ARCH
            );
            return Ok(());
        }
    };

    let client = http_client()?;
    let release = fetch_latest(&client).await?;

    let latest = match parse_tag(&release.tag_name) {
        Some(v) if is_newer(&v) => v,
        _ => {
            println!(
                "proxybase-cli v{} is already the latest version.",
                env!("CARGO_PKG_VERSION")
            );
            return Ok(());
        }
    };

    let (asset_url, sha_url) = find_assets(&release, &asset_name).with_context(|| {
        format!(
            "Release {} has no '{}' asset or '{}' file",
            release.tag_name, asset_name, CHECKSUM_FILE
        )
    })?;

    if check_only {
        println!("New version v{} available — run 'proxybase-cli update'", latest);
        return Ok(());
    }

    let sums = parse_sha256sums(&fetch_checksums(&client, &sha_url).await?);
    let expected = sums
        .get(&asset_name)
        .with_context(|| format!("{} contains no checksum for '{}'", CHECKSUM_FILE, asset_name))?;

    let exe = std::env::current_exe().context("Cannot determine current executable path")?;
    let temp: PathBuf = exe.with_extension(format!("new.{}", std::process::id()));
    download_asset(&client, &asset_url, &temp).await?;

    let actual = sha256_file(&temp)?;
    if &actual != expected {
        let _ = std::fs::remove_file(&temp);
        anyhow::bail!("Checksum mismatch for downloaded binary — aborting update");
    }

    if Path::new("/.dockerenv").exists() {
        eprintln!(
            "warning: running inside a Docker container — this update is lost when the container restarts."
        );
    }

    copy_permissions(&exe, &temp);
    replace_binary(&temp, &exe)?;

    println!(
        "Updated proxybase-cli {} -> v{}",
        env!("CARGO_PKG_VERSION"),
        latest
    );
    Ok(())
}

async fn fetch_checksums(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?.error_for_status()?;
    Ok(resp.text().await?)
}

/// Background check: at most once per day, print a notice when a newer
/// version exists. Never fails the caller and never panics.
pub async fn check_and_notify() {
    if Path::new("/.dockerenv").exists() {
        // Image upgrades come from `docker pull` — a notice here is daily noise.
        return;
    }

    let dir = crate::wallet_dir();
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let cache = dir.join("update_check.json");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if let Ok(content) = std::fs::read_to_string(&cache) {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) {
            if let Some(last) = v.get("last_check_secs").and_then(|x| x.as_u64()) {
                if now.saturating_sub(last) < CHECK_INTERVAL_SECS {
                    return;
                }
            }
        }
    }

    // Write the timestamp before the network call so an offline machine
    // doesn't retry (and time out) on every single invocation.
    let json = serde_json::json!({ "last_check_secs": now });
    if std::fs::write(&cache, json.to_string()).is_err() {
        return;
    }

    let client = match http_client() {
        Ok(c) => c,
        Err(_) => return,
    };
    let release = match fetch_latest(&client).await {
        Ok(r) => r,
        Err(_) => return,
    };
    let asset_name = match target_asset() {
        Some(n) => n,
        None => return,
    };
    if find_assets(&release, asset_name).is_none() {
        return;
    }
    if let Some(latest) = parse_tag(&release.tag_name) {
        if is_newer(&latest) {
            eprintln!(
                "New version v{} available — run 'proxybase-cli update'",
                latest
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tag() {
        assert_eq!(parse_tag("proxybase-cli-v1.2.3"), semver::Version::parse("1.2.3").ok());
        assert_eq!(parse_tag("v0.9.0"), semver::Version::parse("0.9.0").ok());
        assert_eq!(parse_tag("v1.0.0-rc.1"), semver::Version::parse("1.0.0-rc.1").ok());
        assert_eq!(parse_tag("garbage"), None);
        assert_eq!(parse_tag("release-1.0.0"), None);
        assert_eq!(parse_tag(""), None);
    }

    #[test]
    fn test_is_newer() {
        assert!(is_newer(&semver::Version::parse("0.2.0").unwrap()));
        assert!(!is_newer(&current_version()));
        assert!(!is_newer(&semver::Version::parse("0.0.1").unwrap()));
        assert!(!is_newer(&semver::Version::parse("0.2.0-rc.1").unwrap()));
    }

    #[test]
    fn test_split_repo() {
        assert_eq!(
            split_repo("https://github.com/proxybasehq/proxybase-cli"),
            Some(("proxybasehq".to_string(), "proxybase-cli".to_string()))
        );
        assert_eq!(
            split_repo("https://github.com/owner/repo/"),
            Some(("owner".to_string(), "repo".to_string()))
        );
        assert_eq!(split_repo("owner/repo"), Some(("owner".to_string(), "repo".to_string())));
        assert_eq!(split_repo("owner"), None);
        assert_eq!(split_repo(""), None);
    }

    #[test]
    fn test_default_repo() {
        assert_eq!(repo(), ("proxybasehq".to_string(), "proxybase-cli".to_string()));
    }

    #[test]
    fn test_parse_sha256sums() {
        let text = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824  proxybase-cli-x86_64-unknown-linux-gnu\nnot-a-real-line\n";
        let map = parse_sha256sums(text);
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.get("proxybase-cli-x86_64-unknown-linux-gnu").unwrap(),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert!(map.get("nope").is_none());
    }

    #[test]
    fn test_sha256_file() {
        let path = std::env::temp_dir().join(format!("proxybase-cli-sha-test-{}", std::process::id()));
        std::fs::write(&path, b"hello").unwrap();
        let hash = sha256_file(&path).unwrap();
        std::fs::remove_file(&path).ok();
        assert_eq!(hash, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824");
    }

    #[test]
    fn test_asset_for() {
        assert_eq!(asset_for("x86_64", "linux"), Some("proxybase-cli-x86_64-unknown-linux-gnu"));
        assert_eq!(asset_for("x86_64", "windows"), Some("proxybase-cli-x86_64-pc-windows-msvc.exe"));
        assert_eq!(asset_for("aarch64", "macos"), Some("proxybase-cli-aarch64-apple-darwin"));
        assert_eq!(asset_for("x86_64", "macos"), None, "Intel macOS is not published");
        assert_eq!(asset_for("aarch64", "linux"), None);
        assert_eq!(asset_for("x86_64", "freebsd"), None);
    }

    #[test]
    fn test_find_assets() {
        let release = GhRelease {
            tag_name: "proxybase-cli-v0.2.0".to_string(),
            assets: vec![
                GhAsset {
                    name: "proxybase-cli-x86_64-unknown-linux-gnu".to_string(),
                    browser_download_url: "https://example.com/bin".to_string(),
                },
                GhAsset {
                    name: "SHA256SUMS".to_string(),
                    browser_download_url: "https://example.com/sums".to_string(),
                },
                GhAsset {
                    name: "proxybase-cli-x86_64-unknown-linux-gnu.tar.gz".to_string(),
                    browser_download_url: "https://example.com/archive".to_string(),
                },
            ],
        };
        let (bin, sha) = find_assets(&release, "proxybase-cli-x86_64-unknown-linux-gnu").unwrap();
        assert_eq!(bin, "https://example.com/bin");
        assert_eq!(sha, "https://example.com/sums");
        assert!(find_assets(&release, "proxybase-cli-aarch64-apple-darwin").is_none());
    }
}

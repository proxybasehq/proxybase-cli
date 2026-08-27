//! Local unauthenticated SOCKS5 bridge: accepts NO_AUTH SOCKS5 connections on
//! 127.0.0.1:<port> and relays them through the authenticated remote SOCKS5
//! gateway (username = session id, password = session token). Lets apps that
//! do not support SOCKS5 username/password auth (Chrome, some scrapers, curl
//! without credentials) use a buyer session transparently.
//!
//! Ported from proxybase-gui/src-tauri/src/bridge.rs, minus the in-process
//! registry/stop — the CLI runs one bridge per dedicated process, so a bridge
//! is stopped by killing its process ('bridge stop' sends SIGTERM) and the OS
//! releases the listener socket.

use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::BackendClient;

/// How often the bridge touches the session on the backend so the 1h idle
/// timeout does not kill it while a long-lived app holds the bridge open.
/// Matches the GUI keepalive cadence.
const KEEPALIVE_INTERVAL: tokio::time::Duration = tokio::time::Duration::from_secs(300);

/// Start a local unauthenticated SOCKS5 bridge for a session. Spawns the
/// accept loop + keepalive task and returns the local port the bridge is
/// listening on. The caller must keep the tokio runtime alive (foreground
/// mode blocks) — the bridge runs until the process is killed.
pub async fn start_bridge(
    session_id: &str,
    backend_url: &str,
    upstream_addr: &str,
    preferred_port: Option<u16>,
) -> Result<u16, String> {
    let sid = session_id.to_string();

    // One bridge process per session, so port conflicts come from OTHER
    // processes. If the preferred port is taken, fall back to an ephemeral
    // port instead of failing the start.
    let mut listener = None;
    if let Some(port) = preferred_port {
        match bind_listener(port) {
            Ok(l) => listener = Some(l),
            Err(e) => eprintln!(
                "[bridge {}] Port {} unavailable ({}), falling back to an ephemeral port",
                sid, port, e
            ),
        }
    }
    let listener = match listener {
        Some(l) => l,
        None => bind_listener(0)?,
    };
    let local_port = listener
        .local_addr()
        .map_err(|e| format!("Failed to get local addr: {}", e))?
        .port();

    let backend_url = backend_url.to_string();
    let upstream_addr = upstream_addr.to_string();
    tokio::spawn(async move {
        eprintln!("[bridge {}] Started on port {}", sid, local_port);

        // Keep the session alive on the backend while the bridge runs.
        // The token is re-read from disk on every call so re-auth'd
        // tokens propagate without a bridge restart.
        {
            let kp_sid = sid.clone();
            let kp_backend = backend_url.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(KEEPALIVE_INTERVAL);
                loop {
                    tick.tick().await;
                    let client = BackendClient::new(&kp_backend);
                    if let Err(e) = client.keepalive_session(&kp_sid).await {
                        eprintln!("[bridge {}] keepalive failed: {}", kp_sid, e);
                    }
                }
            });
        }

        loop {
            match listener.accept().await {
                Ok((client_stream, client_addr)) => {
                    let up_addr = upstream_addr.clone();
                    let up_sid = sid.clone();
                    // Dynamically reload token on every connection so
                    // re-auth'd tokens propagate to the bridge instantly.
                    let up_pass = BackendClient::load_token()
                        .unwrap_or_default();
                    eprintln!("[bridge {}] Accepted client {}", sid, client_addr);
                    tokio::spawn(async move {
                        relay_through_upstream(
                            client_stream,
                            &up_addr,
                            &up_sid,
                            &up_pass,
                        ).await;
                    });
                }
                Err(e) => {
                    eprintln!("[bridge {}] Accept error: {}", sid, e);
                }
            }
        }
    });

    Ok(local_port)
}

/// SO_REUSEADDR is required on macOS/BSD to rebind a stable port while old
/// relay connections are still in TIME_WAIT. Without it, a restart fails
/// with EADDRINUSE and the bridge port silently disappears.
fn bind_listener(port: u16) -> Result<tokio::net::TcpListener, String> {
    let socket = tokio::net::TcpSocket::new_v4()
        .map_err(|e| format!("Failed to create bridge socket: {}", e))?;
    socket
        .set_reuseaddr(true)
        .map_err(|e| format!("Failed to set SO_REUSEADDR: {}", e))?;
    let bind_addr = format!("127.0.0.1:{}", port);
    socket
        .bind(
            bind_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("Invalid bridge bind addr {}: {}", bind_addr, e))?,
        )
        .map_err(|e| format!("Failed to bind bridge on {}: {}", bind_addr, e))?;
    socket
        .listen(1024)
        .map_err(|e| format!("Failed to listen on {}: {}", bind_addr, e))
}

/// Relay a client connection through the authenticated upstream SOCKS5 proxy.
async fn relay_through_upstream(
    mut client: tokio::net::TcpStream,
    upstream_addr: &str,
    upstream_username: &str,
    upstream_password: &str,
) {
    // Bound the client SOCKS5 handshake so a stalled client cannot hold a
    // socket (FD) forever.
    let target = match tokio::time::timeout(
        std::time::Duration::from_secs(15),
        accept_socks5_noauth(&mut client),
    )
    .await
    {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            eprintln!("Bridge SOCKS5 handshake failed: {}", e);
            return;
        }
        Err(_) => {
            eprintln!("Bridge SOCKS5 handshake timed out");
            return;
        }
    };

    // Connect to upstream with auth. fast-socks5 has no built-in connect
    // timeout (Config::default() leaves connect_timeout as None), so bound the
    // whole upstream handshake to keep dead proxies from leaking sockets.
    let mut cfg = fast_socks5::client::Config::default();
    cfg.set_skip_auth(false);
    cfg.set_connect_timeout(std::time::Duration::from_secs(10));
    let upstream = match tokio::time::timeout(
        std::time::Duration::from_secs(15),
        fast_socks5::client::Socks5Stream::connect_with_password(
            upstream_addr,
            target.0,
            target.1,
            upstream_username.to_string(),
            upstream_password.to_string(),
            cfg,
        ),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            eprintln!("Bridge upstream connect failed: {:?}", e);
            return;
        }
        Err(_) => {
            eprintln!("Bridge upstream connect timed out");
            return;
        }
    };

    let (mut up_r, mut up_w) = tokio::io::split(upstream);
    let (mut cl_r, mut cl_w) = tokio::io::split(client);

    // Idle timeout: traffic in either direction resets the clock. Abandoned
    // tunnels close after 60s (matches the seller relay's idle timeout) so a
    // client that never disconnects cannot hold sockets forever.
    const IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
    let deadline = Arc::new(std::sync::Mutex::new(
        tokio::time::Instant::now() + IDLE_TIMEOUT,
    ));

    // Bidirectional relay
    let mut up_to_cl = {
        let deadline = deadline.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match up_r.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        *deadline.lock().unwrap() =
                            tokio::time::Instant::now() + IDLE_TIMEOUT;
                        if cl_w.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        })
    };

    let mut cl_to_up = {
        let deadline = deadline.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match cl_r.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        *deadline.lock().unwrap() =
                            tokio::time::Instant::now() + IDLE_TIMEOUT;
                        if up_w.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        })
    };

    // Sleep until the current idle deadline, re-checking it on each wake so
    // any traffic keeps the tunnel alive indefinitely.
    let mut idle_task = tokio::spawn({
        let deadline = deadline.clone();
        async move {
            loop {
                let next = *deadline.lock().unwrap();
                if tokio::time::Instant::now() >= next {
                    break;
                }
                tokio::time::sleep_until(next).await;
            }
        }
    });

    tokio::select! {
        _ = &mut up_to_cl => {}
        _ = &mut cl_to_up => {}
        _ = &mut idle_task => {
            eprintln!("Bridge relay idle timeout — closing");
        }
    }
    // Abort whichever direction is still running so BOTH socket halves close
    // immediately (previously the other task kept running, leaking sockets).
    up_to_cl.abort();
    cl_to_up.abort();
    idle_task.abort();
}

/// Minimal SOCKS5 connect accept (no auth, only CONNECT command).
async fn accept_socks5_noauth(
    client: &mut tokio::net::TcpStream,
) -> Result<(String, u16), String> {
    let mut greeting_hdr = [0u8; 2];
    client
        .read_exact(&mut greeting_hdr)
        .await
        .map_err(|e| format!("read greeting header: {}", e))?;

    if greeting_hdr[0] != 0x05 {
        return Err("not SOCKS5".to_string());
    }
    let nmethods = greeting_hdr[1] as usize;

    let mut methods = vec![0u8; nmethods];
    if nmethods > 0 {
        client
            .read_exact(&mut methods)
            .await
            .map_err(|e| format!("read methods: {}", e))?;
    }

    // Reply: no auth
    client
        .write_all(&[0x05, 0x00])
        .await
        .map_err(|e| format!("write auth reply: {}", e))?;

    // Read connect request
    let mut hdr = [0u8; 4];
    client
        .read_exact(&mut hdr)
        .await
        .map_err(|e| format!("read connect hdr: {}", e))?;

    if hdr[0] != 0x05 || hdr[1] != 0x01 {
        return Err("not CONNECT".to_string());
    }

    let host = match hdr[3] {
        0x01 => {
            // IPv4
            let mut ip = [0u8; 4];
            client
                .read_exact(&mut ip)
                .await
                .map_err(|e| format!("read ipv4: {}", e))?;
            std::net::Ipv4Addr::from(ip).to_string()
        }
        0x03 => {
            // Domain name
            let mut len = [0u8; 1];
            client
                .read_exact(&mut len)
                .await
                .map_err(|e| format!("read domain len: {}", e))?;
            let mut domain = vec![0u8; len[0] as usize];
            client
                .read_exact(&mut domain)
                .await
                .map_err(|e| format!("read domain: {}", e))?;
            String::from_utf8_lossy(&domain).to_string()
        }
        0x04 => {
            // IPv6
            let mut ip = [0u8; 16];
            client
                .read_exact(&mut ip)
                .await
                .map_err(|e| format!("read ipv6: {}", e))?;
            std::net::Ipv6Addr::from(ip).to_string()
        }
        _ => return Err("unsupported address type".to_string()),
    };

    // Read port
    let mut port_bytes = [0u8; 2];
    client
        .read_exact(&mut port_bytes)
        .await
        .map_err(|e| format!("read port: {}", e))?;
    let port = u16::from_be_bytes(port_bytes);

    // Send success reply
    let reply = [
        0x05, 0x00, 0x00, 0x01, // VER, REP, RSV, ATYP
        0x00, 0x00, 0x00, 0x00, // BND.ADDR (0.0.0.0)
        (port >> 8) as u8,       // BND.PORT hi
        (port & 0xFF) as u8,     // BND.PORT lo
    ];
    client
        .write_all(&reply)
        .await
        .map_err(|e| format!("write reply: {}", e))?;

    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fast_socks5::server::{run_tcp_proxy, DnsResolveHelper as _, Socks5ServerProtocol};
    use fast_socks5::Socks5Command;
    use std::time::Duration;

    /// TCP echo server: returns every byte it receives. Bound dual-stack
    /// (`[::]:0`) so both `127.0.0.1` and `localhost`/`::1` clients reach it.
    async fn spawn_echo_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("[::]:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { continue };
                tokio::spawn(async move {
                    let mut buf = [0u8; 8192];
                    loop {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => {
                                if sock.write_all(&buf[..n]).await.is_err() {
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        });
        (addr, handle)
    }

    /// Password-auth SOCKS5 upstream (accepts any credentials) that proxies
    /// to real targets — the same tokio-native fast-socks5 server the backend
    /// gateway uses.
    async fn spawn_socks5_upstream() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { continue };
                tokio::spawn(async move {
                    let proto = match Socks5ServerProtocol::accept_password_auth(socket, |_u, _p| true).await {
                        Ok((proto, _)) => proto,
                        Err(_) => return,
                    };
                    let (proto, cmd, target_addr) = match proto.read_command().await {
                        Ok(t) => t,
                        Err(_) => return,
                    };
                    let (proto, cmd, target_addr) = match (proto, cmd, target_addr).resolve_dns().await {
                        Ok(t) => t,
                        Err(_) => return,
                    };
                    if matches!(cmd, Socks5Command::TCPConnect) {
                        let _ = run_tcp_proxy(proto, &target_addr, Duration::from_secs(10), false).await;
                    }
                });
            }
        });
        (addr, handle)
    }

    /// Full no-auth SOCKS5 client handshake against the bridge: greeting,
    /// CONNECT, success reply. Returns the connected stream.
    async fn socks5_client_handshake(
        bridge_port: u16,
        connect_payload: Vec<u8>,
    ) -> tokio::net::TcpStream {
        let mut client = tokio::net::TcpStream::connect(("127.0.0.1", bridge_port))
            .await
            .expect("bridge must accept connections");
        client
            .write_all(&[0x05, 0x01, 0x00])
            .await
            .expect("write greeting");
        let mut reply = [0u8; 2];
        client.read_exact(&mut reply).await.expect("read auth reply");
        assert_eq!(reply, [0x05, 0x00], "bridge must accept NO_AUTH");
        client
            .write_all(&connect_payload)
            .await
            .expect("write connect request");
        let mut reply = [0u8; 10];
        client.read_exact(&mut reply).await.expect("read connect reply");
        assert_eq!(reply[1], 0x00, "bridge must grant CONNECT");
        client
    }

    /// Start the standard test rig: echo server + auth upstream + bridge.
    async fn start_test_rig(
        session_id: &str,
    ) -> (
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
        std::net::SocketAddr,
        tokio::task::JoinHandle<()>,
        u16,
    ) {
        let (echo_addr, echo_handle) = spawn_echo_server().await;
        let (socks_addr, socks_handle) = spawn_socks5_upstream().await;
        // Unreachable backend URL keeps the bridge's keepalive task quiet.
        let bridge_port = start_bridge(
            session_id,
            "http://127.0.0.1:1",
            &format!("127.0.0.1:{}", socks_addr.port()),
            None,
        )
        .await
        .expect("bridge must start");
        (echo_addr, echo_handle, socks_addr, socks_handle, bridge_port)
    }

    #[tokio::test]
    async fn test_bridge_relays_ipv4_through_authenticated_upstream() {
        let (echo_addr, echo_handle, _socks_addr, socks_handle, bridge_port) =
            start_test_rig("test-relay-v4").await;

        let mut connect_req = vec![0x05, 0x01, 0x00, 0x01];
        connect_req.extend_from_slice(&[127, 0, 0, 1]);
        connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
        let mut client = tokio::time::timeout(
            Duration::from_secs(10),
            socks5_client_handshake(bridge_port, connect_req),
        )
        .await
        .expect("handshake timed out");

        client.write_all(b"hello through bridge").await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(10), client.read(&mut buf))
            .await
            .expect("echo timed out")
            .expect("read failed");
        assert_eq!(&buf[..n], b"hello through bridge");

        echo_handle.abort();
        socks_handle.abort();
    }

    #[tokio::test]
    async fn test_bridge_relays_domain_through_authenticated_upstream() {
        let (echo_addr, echo_handle, _socks_addr, socks_handle, bridge_port) =
            start_test_rig("test-relay-domain").await;

        // Domain-name CONNECT (ATYP 0x03) exercises the domain parse branch
        // in accept_socks5_noauth.
        let mut connect_req = vec![0x05, 0x01, 0x00, 0x03, 9];
        connect_req.extend_from_slice(b"localhost");
        connect_req.extend_from_slice(&echo_addr.port().to_be_bytes());
        let mut client = tokio::time::timeout(
            Duration::from_secs(10),
            socks5_client_handshake(bridge_port, connect_req),
        )
        .await
        .expect("handshake timed out");

        client.write_all(b"domain echo").await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(10), client.read(&mut buf))
            .await
            .expect("echo timed out")
            .expect("read failed");
        assert_eq!(&buf[..n], b"domain echo");

        echo_handle.abort();
        socks_handle.abort();
    }

    #[tokio::test]
    async fn test_bridge_preferred_port_and_fallback() {
        let (_echo_addr, echo_handle, socks_addr, socks_handle, bridge_port) =
            start_test_rig("test-ports").await;
        assert_ne!(bridge_port, 0, "bridge must report its bound port");

        let upstream = format!("127.0.0.1:{}", socks_addr.port());

        // Preferred port that is free must be used.
        let probe = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let free_port = probe.local_addr().unwrap().port();
        drop(probe);
        let p = start_bridge("test-ports-2", "http://127.0.0.1:1", &upstream, Some(free_port))
            .await
            .expect("bridge with preferred port must start");
        assert_eq!(p, free_port);

        // Preferred port already bound by another bridge's live listener →
        // must fall back to an ephemeral port instead of failing the start.
        let p2 = start_bridge("test-ports-3", "http://127.0.0.1:1", &upstream, Some(bridge_port))
            .await
            .expect("bridge must fall back instead of failing");
        assert_ne!(p2, bridge_port, "conflicting preferred port must fall back");

        echo_handle.abort();
        socks_handle.abort();
    }
}

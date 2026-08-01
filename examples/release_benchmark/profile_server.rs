//! Serves deterministic Profile fixtures over loopback HTTP.

use std::error::Error;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::FileTypeExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::WorkloadScale;
use super::reporting::invalid;

pub(super) struct ProfileServer {
    base_url: String,
    shutdown: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl ProfileServer {
    pub(super) fn start(
        manifest_path: &Path,
        scale: WorkloadScale,
    ) -> Result<Self, Box<dyn Error>> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let active_body = Arc::new(release_profile_document(manifest_path, scale)?);
        let inactive_body = Arc::new(
            concat!(
                "proxies:\n",
                "  - name: benchmark-node\n",
                "    type: ss\n",
                "    server: 127.0.0.1\n",
                "    port: 443\n",
                "    cipher: aes-128-gcm\n",
                "    password: fixture-password\n",
                "proxy-groups:\n",
                "  - name: Main\n",
                "    type: select\n",
                "    proxies: [benchmark-node, DIRECT]\n",
                "rules:\n",
                "  - MATCH,Main\n"
            )
            .as_bytes()
            .to_vec(),
        );
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);
        let thread = thread::spawn(move || {
            while !thread_shutdown.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = serve_profile(stream, &active_body, &inactive_body);
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => return,
                }
            }
        });
        Ok(Self {
            base_url: format!("http://{address}"),
            shutdown,
            thread: Some(thread),
        })
    }

    pub(super) fn url(&self, index: u64) -> String {
        format!("{}/profile-{index:03}.yaml", self.base_url)
    }
}

impl Drop for ProfileServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn release_profile_document(
    manifest_path: &Path,
    scale: WorkloadScale,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let root = manifest_path
        .parent()
        .ok_or_else(|| invalid("workload manifest must have a parent directory"))?;
    let nodes = read_ndjson(&root.join("active-nodes.ndjson"))?;
    if nodes.len() != usize::try_from(scale.active_nodes)? {
        return Err(invalid(
            "release Profile document Active Node count differs from its workload",
        ));
    }
    let mut body = String::with_capacity(
        usize::try_from(scale.active_nodes.saturating_mul(180))?
            .saturating_add(usize::try_from(scale.local_rules.saturating_mul(64))?),
    );
    body.push_str("proxies:\n");
    for node in &nodes {
        let name = node["name"]
            .as_str()
            .ok_or_else(|| invalid("workload Node name must be a string"))?;
        writeln!(body, "  - name: {name}")?;
        body.push_str(
            "    type: ss\n    server: 127.0.0.1\n    port: 443\n    cipher: aes-128-gcm\n    password: fixture-password\n",
        );
    }
    body.push_str("proxy-groups:\n  - name: PROXY\n    type: select\n    proxies:\n");
    for node in &nodes {
        let name = node["name"]
            .as_str()
            .ok_or_else(|| invalid("workload Node name must be a string"))?;
        writeln!(body, "      - {name}")?;
    }
    body.push_str("      - DIRECT\n");
    let rules = fs::read_to_string(root.join("rules.yaml"))?;
    if rules.lines().count() != usize::try_from(scale.local_rules.saturating_add(1))? {
        return Err(invalid(
            "release Profile document Local Rule count differs from its workload",
        ));
    }
    body.push_str(&rules);
    Ok(body.into_bytes())
}

fn serve_profile(
    mut stream: TcpStream,
    active_body: &[u8],
    inactive_body: &[u8],
) -> Result<(), io::Error> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut request = [0_u8; 4 * 1_024];
    let read = stream.read(&mut request)?;
    let active = String::from_utf8_lossy(&request[..read]).starts_with("GET /profile-000.yaml ");
    let body = if active { active_body } else { inactive_body };
    let header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/yaml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

pub(super) fn wait_for_socket(path: &Path, timeout: Duration) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => return Ok(()),
            Ok(_) => return Err(invalid("fixture Core service endpoint is not a socket")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(Box::new(error)),
        }
        if Instant::now() >= deadline {
            return Err(invalid("fixture Core service did not become ready"));
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn read_ndjson(path: &Path) -> Result<Vec<Value>, Box<dyn Error>> {
    BufReader::new(File::open(path)?)
        .lines()
        .map(|line| Ok(serde_json::from_str(&line?)?))
        .collect()
}

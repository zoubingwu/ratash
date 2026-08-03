//! Provides deterministic Core and privileged-service fixture processes.

use std::env;
use std::error::Error;
use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use serde_json::{Map, Value, json};

use ratash::config::ConfigCompiler;
use ratash::core::{CoreControlEndpoint, OwnerSessionRequest};
use ratash::core_service_ipc::{CoreServiceServer, CoreServiceServerConfig};
use ratash::lifecycle::{ProcessInspector, PsProcessInspector};
use ratash::process_controller::{
    NativeCoreProcessConfig, NativeCoreProcessController, UnixCoreControlClient,
};
use ratash::service::{
    CallerCredentialValidator, PrivilegedCoreRuntimeService, PrivilegedServiceConfig,
    PrivilegedServiceDependencies, ProcessIdentityProbe, RuntimeConfigurationPolicy,
    RuntimeManifestFileV1, SecretGenerator, ServicePlatformError, ServicePlatformErrorKind,
    TunCapabilityPreflight,
};
use ratash::tui_runtime::{ProcessSignalSource, ShutdownSignal};

use super::CORE_FIXTURE_WORKER_COUNT;
use super::reporting::sha256_file;
use super::support::invalid;

#[derive(Default)]
struct FixtureOwnerIdentity {
    pid: AtomicU64,
    value: Mutex<String>,
}

struct FixtureCredentials {
    owner_uid: u32,
    identity: Arc<FixtureOwnerIdentity>,
}

impl CallerCredentialValidator for FixtureCredentials {
    fn validate(&self, request: &OwnerSessionRequest) -> Result<(), ServicePlatformError> {
        if request.owner_uid != self.owner_uid {
            return Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ));
        }
        self.identity
            .pid
            .store(u64::from(request.supervisor_pid), Ordering::Release);
        *self
            .identity
            .value
            .lock()
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Credential))? =
            request.supervisor_start_identity.clone();
        Ok(())
    }
}

struct FixtureIdentityProbe(Arc<FixtureOwnerIdentity>);

impl ProcessIdentityProbe for FixtureIdentityProbe {
    fn start_identity(&self, pid: u32) -> Result<Option<String>, ServicePlatformError> {
        if self.0.pid.load(Ordering::Acquire) == u64::from(pid) {
            self.0
                .value
                .lock()
                .map(|value| Some(value.clone()))
                .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::ProcessInspection))
        } else {
            PsProcessInspector
                .identity(pid)
                .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::ProcessInspection))
        }
    }
}

struct FixtureTun;

impl TunCapabilityPreflight for FixtureTun {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        Ok(())
    }
}

struct AllowConfigurationPolicy;

impl RuntimeConfigurationPolicy for AllowConfigurationPolicy {
    fn validate(
        &self,
        _configuration: &[u8],
        _endpoint: &CoreControlEndpoint,
        _provider_files: &[RuntimeManifestFileV1],
    ) -> Result<(), ServicePlatformError> {
        Ok(())
    }
}

#[derive(Default)]
struct FixtureSecrets(AtomicU64);

impl SecretGenerator for FixtureSecrets {
    fn generate(&self) -> Result<String, ServicePlatformError> {
        Ok(format!(
            "benchmark-secret-{}",
            self.0.fetch_add(1, Ordering::Relaxed)
        ))
    }
}

pub(super) fn run_fixture_core_service(
    socket: &Path,
    runtime_root: &Path,
    owner_uid: u32,
    mihomo: &Path,
    guardian: &Path,
) -> Result<(), Box<dyn Error>> {
    let compiler = ConfigCompiler::bundled()?;
    let identity = Arc::new(FixtureOwnerIdentity::default());
    let processes = NativeCoreProcessController::new_guarded(
        NativeCoreProcessConfig::default(),
        Arc::new(UnixCoreControlClient::default()),
        Arc::new(PsProcessInspector),
        guardian.to_owned(),
    )?;
    let runtime = Arc::new(PrivilegedCoreRuntimeService::new(
        PrivilegedServiceConfig::product_defaults(
            runtime_root.to_owned(),
            compiler.compiler_policy_sha256().to_owned(),
            sha256_file(mihomo)?,
        ),
        PrivilegedServiceDependencies {
            credentials: Box::new(FixtureCredentials {
                owner_uid,
                identity: Arc::clone(&identity),
            }),
            identities: Box::new(FixtureIdentityProbe(identity)),
            tun: Box::new(FixtureTun),
            configuration_policy: Box::new(AllowConfigurationPolicy),
            secrets: Box::new(FixtureSecrets::default()),
            processes: Box::new(processes),
        },
    )?);
    let _server = CoreServiceServer::start(
        socket,
        runtime,
        CoreServiceServerConfig::new(runtime_root, owner_uid),
    )?;
    let signal = ProcessSignalSource::new()
        .map_err(|_| invalid("fixture Core service signal handling could not start"))?;
    while !signal.shutdown_requested() {
        thread::sleep(Duration::from_millis(25));
    }
    Ok(())
}

pub(super) fn argument_value<'a>(arguments: &'a [String], name: &str) -> Option<&'a str> {
    arguments
        .windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].as_str())
}

pub(super) fn run_fixture_core(socket: &Path) -> Result<(), Box<dyn Error>> {
    let active_nodes = env::var("RATASH_BENCHMARK_ACTIVE_NODES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    let proxies = Arc::new(fixture_proxy_response(active_nodes)?);
    let listener = UnixListener::bind(socket)?;
    fs::set_permissions(socket, fs::Permissions::from_mode(0o600))?;
    let (sender, receiver) = mpsc::sync_channel(CORE_FIXTURE_WORKER_COUNT);
    let receiver = Arc::new(Mutex::new(receiver));
    let _workers = (0..CORE_FIXTURE_WORKER_COUNT)
        .map(|_| {
            let receiver = Arc::clone(&receiver);
            let proxies = Arc::clone(&proxies);
            thread::spawn(move || {
                loop {
                    let stream = receiver
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .recv();
                    let Ok(stream) = stream else {
                        return;
                    };
                    let _ = serve_core_request(stream, &proxies);
                }
            })
        })
        .collect::<Vec<_>>();
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                sender
                    .send(stream)
                    .map_err(|_| invalid("fixture Core worker pool stopped"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(Box::new(error)),
        }
    }
}

fn fixture_proxy_response(active_nodes: u64) -> Result<Vec<u8>, Box<dyn Error>> {
    let names = (0..active_nodes)
        .map(|index| Value::from(format!("Release Node {index:05}")))
        .collect::<Vec<_>>();
    let mut proxies = Map::new();
    proxies.insert(
        "GLOBAL".to_owned(),
        json!({
            "alive": true, "all": ["PROXY", "DIRECT"], "history": [],
            "name": "GLOBAL", "now": "PROXY", "type": "Selector",
            "udp": true, "xudp": false
        }),
    );
    proxies.insert(
        "PROXY".to_owned(),
        json!({
            "alive": true, "all": names, "history": [],
            "name": "PROXY", "now": "Release Node 00000", "type": "Selector",
            "udp": true, "xudp": false
        }),
    );
    proxies.insert(
        "DIRECT".to_owned(),
        json!({
            "alive": true, "history": [], "name": "DIRECT",
            "type": "Direct", "udp": true, "xudp": false
        }),
    );
    for index in 0..active_nodes {
        let name = format!("Release Node {index:05}");
        proxies.insert(
            name.clone(),
            json!({
                "alive": true, "history": [], "name": name,
                "type": "Shadowsocks", "udp": true, "xudp": false
            }),
        );
    }
    Ok(serde_json::to_vec(&json!({ "proxies": proxies }))?)
}

fn serve_core_request(mut stream: UnixStream, proxies: &[u8]) -> Result<(), Box<dyn Error>> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut request = [0_u8; 8 * 1_024];
    let mut request_bytes = 0_usize;
    let request_length = loop {
        if request_bytes == request.len() {
            return Err(invalid("fixture Core request exceeds its bounded buffer"));
        }
        let read = stream.read(&mut request[request_bytes..])?;
        if read == 0 {
            return Err(invalid("fixture Core request closed before completion"));
        }
        request_bytes += read;
        let Some(header_start) = request[..request_bytes]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        else {
            continue;
        };
        let header_end = header_start + 4;
        let headers = std::str::from_utf8(&request[..header_start])?;
        let content_length = headers
            .split("\r\n")
            .skip(1)
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then_some(value.trim())
            })
            .map(str::parse::<usize>)
            .transpose()?
            .unwrap_or(0);
        let complete = header_end
            .checked_add(content_length)
            .ok_or_else(|| invalid("fixture Core request length overflowed"))?;
        if complete > request.len() {
            return Err(invalid(
                "fixture Core request body exceeds its bounded buffer",
            ));
        }
        if request_bytes >= complete {
            break complete;
        }
    };
    let request = String::from_utf8_lossy(&request[..request_length]);
    let (status, body): (&str, &[u8]) = if request.starts_with("PUT /configs?force=true ") {
        ("204 No Content", b"")
    } else if request.starts_with("GET /providers/proxies ") {
        ("200 OK", br#"{"providers":{}}"#)
    } else if request.starts_with("GET /proxies ") {
        ("200 OK", proxies)
    } else if request.contains("/delay?") {
        thread::sleep(Duration::from_millis(5));
        ("200 OK", br#"{"delay":5}"#)
    } else if request.starts_with("GET /connections ") {
        (
            "200 OK",
            br#"{"downloadTotal":0,"uploadTotal":0,"connections":[]}"#,
        )
    } else {
        (
            "200 OK",
            br#"{"meta":true,"premium":false,"version":"v1.19.28"}"#,
        )
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()?;
    thread::sleep(Duration::from_millis(1));
    Ok(())
}

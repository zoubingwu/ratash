use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use hopash::core::{
    ConnectionSummary, CoreControlEndpoint, DelayProbeRequest, DelayTarget, MihomoAdapter,
    MihomoErrorKind, MihomoLogFrame, MihomoLogLevel, MihomoReadiness, MihomoVersion, NodeSelection,
    ProviderState, TrafficFrame,
};
use hopash::domain::{CoreInstanceGeneration, NodeRecordId};
use hopash::mihomo::{MihomoAdapterConfig, UnixMihomoAdapter};
use tungstenite::handshake::derive_accept_key;

const PROXIES: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/proxies.json");
const PROVIDERS: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/providers.json");
const VERSION: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/version.json");
const DELAY: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/delay.json");
const TRAFFIC: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/traffic.json");
const CONNECTIONS: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/connections.json");
const LOG: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/log.json");
const SECRET: &str = "fixture-bearer-secret";

type Handler = Box<dyn FnOnce(UnixStream) + Send + 'static>;

struct UnixFixture {
    root: PathBuf,
    socket_path: PathBuf,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl UnixFixture {
    fn new(label: &str, handlers: Vec<Handler>) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "hopash-mihomo-{label}-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).expect("fixture directory should be created");
        let socket_path = root.join("mihomo.sock");
        let listener = UnixListener::bind(&socket_path).expect("fixture socket should bind");

        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            let mut handlers = VecDeque::from(handlers);
            while !handlers.is_empty() && !worker_stop.load(Ordering::Acquire) {
                let (stream, _) = listener.accept().expect("fixture accept should succeed");
                if worker_stop.load(Ordering::Acquire) {
                    return;
                }
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .expect("fixture read timeout should be set");
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .expect("fixture write timeout should be set");
                handlers.pop_front().expect("fixture handler should exist")(stream);
            }
            assert!(
                handlers.is_empty() || worker_stop.load(Ordering::Acquire),
                "fixture server stopped with pending handlers"
            );
        });

        Self {
            root,
            socket_path,
            stop,
            worker: Some(worker),
        }
    }

    fn endpoint(&self, secret: &str) -> CoreControlEndpoint {
        CoreControlEndpoint::new(&self.socket_path, secret)
    }

    fn finish(mut self) {
        self.worker
            .take()
            .expect("fixture worker should exist")
            .join()
            .expect("fixture worker should succeed");
    }
}

impl Drop for UnixFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = UnixStream::connect(&self.socket_path);
            let _ = worker.join();
        }
        if self.root.exists() {
            fs::remove_dir_all(&self.root).expect("fixture directory should be removed");
        }
    }
}

#[derive(Debug)]
struct Request {
    method: String,
    target: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn read_request(mut stream: &UnixStream) -> Request {
    let mut bytes = Vec::new();
    let header_end = loop {
        let mut chunk = [0_u8; 1024];
        let read = stream
            .read(&mut chunk)
            .expect("fixture request should be readable");
        assert!(read > 0, "fixture request closed before its headers");
        bytes.extend_from_slice(&chunk[..read]);
        assert!(
            bytes.len() <= 64 * 1024,
            "fixture request headers are bounded"
        );
        if let Some(position) = find_bytes(&bytes, b"\r\n\r\n") {
            break position + 4;
        }
    };

    let head = std::str::from_utf8(&bytes[..header_end - 4])
        .expect("fixture request headers should be UTF-8");
    let mut lines = head.split("\r\n");
    let request_line = lines.next().expect("request line should exist");
    let mut request_parts = request_line.split(' ');
    let method = request_parts
        .next()
        .expect("request method should exist")
        .to_owned();
    let target = request_parts
        .next()
        .expect("request target should exist")
        .to_owned();
    assert_eq!(request_parts.next(), Some("HTTP/1.1"));
    assert!(request_parts.next().is_none());

    let mut headers = BTreeMap::new();
    for line in lines {
        let (name, value) = line
            .split_once(':')
            .expect("fixture request header should have a value");
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>().expect("valid content length"))
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let mut chunk = [0_u8; 1024];
        let read = stream
            .read(&mut chunk)
            .expect("fixture request body should be readable");
        assert!(read > 0, "fixture request closed before its body");
        bytes.extend_from_slice(&chunk[..read]);
    }

    Request {
        method,
        target,
        headers,
        body: bytes[header_end..header_end + content_length].to_vec(),
    }
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn assert_request(request: &Request, method: &str, target: &str, secret: &str) {
    let authorization = format!("Bearer {secret}");
    assert_eq!(request.method, method);
    assert_eq!(request.target, target);
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some(authorization.as_str())
    );
    assert_eq!(
        request.headers.get("host").map(String::as_str),
        Some("localhost")
    );
}

fn handler(
    method: &'static str,
    target: &'static str,
    secret: &'static str,
    response: Vec<u8>,
) -> Handler {
    Box::new(move |mut stream| {
        let request = read_request(&stream);
        assert_request(&request, method, target, secret);
        stream
            .write_all(&response)
            .expect("fixture response should be written");
        finish_http_response(&mut stream);
    })
}

fn finish_http_response(stream: &mut UnixStream) {
    stream
        .shutdown(Shutdown::Write)
        .expect("fixture response should finish cleanly");
    wait_for_client_shutdown(stream);
}

fn response(status: &str, content_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut head = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    if let Some(content_type) = content_type {
        head.push_str(&format!("Content-Type: {content_type}\r\n"));
    }
    head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    let mut output = head.into_bytes();
    output.extend_from_slice(body);
    output
}

fn json_response(status: &str, body: &[u8]) -> Vec<u8> {
    response(status, Some("application/json; charset=utf-8"), body)
}

fn adapter() -> UnixMihomoAdapter {
    UnixMihomoAdapter::new(MihomoAdapterConfig::default())
        .expect("default Mihomo adapter configuration should be valid")
}

#[test]
fn configuration_reload_uses_the_pinned_force_endpoint_and_strict_statuses() {
    let configuration_path = "/tmp/hopash-runtime/config.yaml";
    let success: Handler = Box::new(move |mut stream| {
        let request = read_request(&stream);
        assert_request(&request, "PUT", "/configs?force=true", SECRET);
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .expect("reload request body should be JSON"),
            serde_json::json!({"path": configuration_path})
        );
        stream
            .write_all(&response("204 No Content", None, b""))
            .expect("reload response should be written");
        finish_http_response(&mut stream);
    });
    let fixture = UnixFixture::new(
        "reload",
        vec![
            success,
            handler(
                "PUT",
                "/configs?force=true",
                SECRET,
                json_response("400 Bad Request", b"{}"),
            ),
            handler(
                "PUT",
                "/configs?force=true",
                SECRET,
                json_response("503 Service Unavailable", b"{}"),
            ),
        ],
    );
    let endpoint = fixture.endpoint(SECRET);
    let adapter = adapter();

    adapter
        .reload_configuration(&endpoint, Path::new(configuration_path))
        .expect("configuration reload should succeed");
    assert_eq!(
        adapter
            .reload_configuration(&endpoint, Path::new(configuration_path))
            .expect_err("a rejected reload should fail")
            .kind,
        MihomoErrorKind::InvalidResponse
    );
    assert_eq!(
        adapter
            .reload_configuration(&endpoint, Path::new(configuration_path))
            .expect_err("an unavailable reload endpoint should fail")
            .kind,
        MihomoErrorKind::Unavailable
    );
    fixture.finish();

    assert_eq!(
        adapter
            .reload_configuration(
                &CoreControlEndpoint::new("/tmp/missing.sock", SECRET),
                Path::new("relative.yaml"),
            )
            .expect_err("a relative configuration path should fail before I/O")
            .kind,
        MihomoErrorKind::InvalidResponse
    );
}

#[test]
fn version_and_readiness_use_bearer_authorization_and_strict_statuses() {
    let fixture = UnixFixture::new(
        "version-readiness",
        vec![
            handler("GET", "/version", SECRET, json_response("200 OK", VERSION)),
            handler("GET", "/version", SECRET, json_response("200 OK", VERSION)),
            handler(
                "GET",
                "/version",
                SECRET,
                json_response("503 Service Unavailable", b"{}"),
            ),
            handler(
                "GET",
                "/version",
                SECRET,
                json_response("401 Unauthorized", b"{}"),
            ),
        ],
    );
    let endpoint = fixture.endpoint(SECRET);
    let adapter = adapter();

    assert_eq!(
        adapter.version(&endpoint).expect("version should decode"),
        MihomoVersion {
            version: "v1.19.28".to_owned(),
            meta: true,
        }
    );
    assert_eq!(
        adapter.readiness(&endpoint).expect("Core should be ready"),
        MihomoReadiness::Ready
    );
    assert_eq!(
        adapter
            .readiness(&endpoint)
            .expect("503 readiness should mean starting"),
        MihomoReadiness::Starting
    );
    assert_eq!(
        adapter
            .version(&endpoint)
            .expect_err("unauthorized version should fail")
            .kind,
        MihomoErrorKind::Unauthorized
    );
    fixture.finish();
}

#[test]
fn proxy_projection_distinguishes_provider_outages_from_invalid_responses() {
    let fixture = UnixFixture::new(
        "proxy-provider",
        vec![
            handler("GET", "/proxies", SECRET, json_response("200 OK", PROXIES)),
            handler(
                "GET",
                "/providers/proxies",
                SECRET,
                json_response("200 OK", PROVIDERS),
            ),
            handler("GET", "/proxies", SECRET, json_response("200 OK", PROXIES)),
            handler(
                "GET",
                "/providers/proxies",
                SECRET,
                json_response("503 Service Unavailable", b"{}"),
            ),
            handler("GET", "/proxies", SECRET, json_response("200 OK", PROXIES)),
            handler(
                "GET",
                "/providers/proxies",
                SECRET,
                json_response("200 OK", b"{"),
            ),
            handler("GET", "/proxies", SECRET, json_response("200 OK", PROXIES)),
            handler(
                "GET",
                "/providers/proxies",
                SECRET,
                json_response("403 Forbidden", b"{}"),
            ),
        ],
    );
    let endpoint = fixture.endpoint(SECRET);
    let adapter = adapter();
    let order = ["GLOBAL", "Manual", "Automatic", "Nested"]
        .map(str::to_owned)
        .to_vec();

    let ready = adapter
        .proxy_view(&endpoint, &order)
        .expect("provider projection should succeed");
    assert_eq!(ready.provider_state, ProviderState::Ready);
    assert!(
        ready
            .nodes
            .contains_key(&NodeRecordId::for_provider("alpha", "provider-only"))
    );

    let unavailable = adapter
        .proxy_view(&endpoint, &order)
        .expect("provider outage should preserve the Core proxy view");
    assert_eq!(unavailable.provider_state, ProviderState::Unavailable);

    assert_eq!(
        adapter
            .proxy_view(&endpoint, &order)
            .expect_err("malformed provider data should fail")
            .kind,
        MihomoErrorKind::InvalidResponse
    );
    assert_eq!(
        adapter
            .proxy_view(&endpoint, &order)
            .expect_err("provider authorization should fail")
            .kind,
        MihomoErrorKind::Unauthorized
    );
    fixture.finish();
}

#[test]
fn selection_and_delay_targets_encode_paths_queries_and_json_exactly() {
    let group_name = "Main / 東京";
    let node_name = "Node/\"Quoted\"";
    let core_proxy = "Core/東京";
    let provider_name = "Provider A";
    let provider_proxy = "Node/1";
    let test_url = "https://example.test/generate_204?x=1&name=two words";
    let selection_handler: Handler = Box::new(move |mut stream| {
        let request = read_request(&stream);
        assert_request(
            &request,
            "PUT",
            "/proxies/Main%20%2F%20%E6%9D%B1%E4%BA%AC/",
            SECRET,
        );
        assert_eq!(
            request.headers.get("content-type").map(String::as_str),
            Some("application/json")
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .expect("selection body should be JSON"),
            serde_json::json!({"name": "Node/\"Quoted\""})
        );
        stream
            .write_all(&response("204 No Content", None, b""))
            .expect("selection response should be written");
        finish_http_response(&mut stream);
    });
    let rejected_selection = handler(
        "PUT",
        "/proxies/Main%20%2F%20%E6%9D%B1%E4%BA%AC/",
        SECRET,
        json_response("400 Bad Request", b"{\"message\":\"rejected\"}"),
    );
    let core_delay = delay_handler("/proxies/Core%2F%E6%9D%B1%E4%BA%AC/delay", test_url, 2500);
    let provider_delay = delay_handler(
        "/providers/proxies/Provider%20A/Node%2F1/healthcheck",
        test_url,
        2500,
    );
    let rejected_delay = handler(
        "GET",
        "/providers/proxies/Provider%20A/Node%2F1/healthcheck?url=https%3A%2F%2Fexample.test%2Fgenerate_204%3Fx%3D1%26name%3Dtwo+words&timeout=2500",
        SECRET,
        json_response("504 Gateway Timeout", b"{}"),
    );
    let fixture = UnixFixture::new(
        "selection-delay",
        vec![
            selection_handler,
            rejected_selection,
            core_delay,
            provider_delay,
            rejected_delay,
        ],
    );
    let endpoint = fixture.endpoint(SECRET);
    let adapter = adapter();
    let selection = NodeSelection {
        group_name: group_name.to_owned(),
        node_name: node_name.to_owned(),
        record_id: NodeRecordId::for_core(node_name),
    };

    adapter
        .select_node(&endpoint, &selection)
        .expect("selection should succeed");
    assert_eq!(
        adapter
            .select_node(&endpoint, &selection)
            .expect_err("rejected selection should fail")
            .kind,
        MihomoErrorKind::SelectionRejected
    );

    let core_result = adapter
        .probe_delay(
            &endpoint,
            &DelayProbeRequest {
                record_id: NodeRecordId::for_core(core_proxy),
                target: DelayTarget::CoreProxy {
                    proxy_name: core_proxy.to_owned(),
                },
                test_url: test_url.to_owned(),
                timeout_ms: 2500,
            },
        )
        .expect("Core proxy delay should succeed");
    assert_eq!(core_result.delay_ms, 42);

    let provider_result = adapter
        .probe_delay(
            &endpoint,
            &DelayProbeRequest {
                record_id: NodeRecordId::for_provider(provider_name, provider_proxy),
                target: DelayTarget::ProviderProxy {
                    provider_name: provider_name.to_owned(),
                    proxy_name: provider_proxy.to_owned(),
                },
                test_url: test_url.to_owned(),
                timeout_ms: 2500,
            },
        )
        .expect("provider proxy delay should succeed");
    assert_eq!(provider_result.delay_ms, 42);
    assert_eq!(
        adapter
            .probe_delay(
                &endpoint,
                &DelayProbeRequest {
                    record_id: NodeRecordId::for_provider(provider_name, provider_proxy),
                    target: DelayTarget::ProviderProxy {
                        provider_name: provider_name.to_owned(),
                        proxy_name: provider_proxy.to_owned(),
                    },
                    test_url: test_url.to_owned(),
                    timeout_ms: 2500,
                },
            )
            .expect_err("rejected provider delay should fail")
            .kind,
        MihomoErrorKind::ProbeFailed
    );
    assert_eq!(
        adapter
            .probe_delay(
                &endpoint,
                &DelayProbeRequest {
                    record_id: NodeRecordId::for_provider("different", core_proxy),
                    target: DelayTarget::CoreProxy {
                        proxy_name: core_proxy.to_owned(),
                    },
                    test_url: test_url.to_owned(),
                    timeout_ms: 2500,
                },
            )
            .expect_err("source-aware Node identity mismatch should fail")
            .kind,
        MihomoErrorKind::ProbeFailed
    );
    assert_eq!(
        adapter
            .probe_delay(
                &endpoint,
                &DelayProbeRequest {
                    record_id: NodeRecordId::for_core(core_proxy),
                    target: DelayTarget::CoreProxy {
                        proxy_name: core_proxy.to_owned(),
                    },
                    test_url: test_url.to_owned(),
                    timeout_ms: 32_768,
                },
            )
            .expect_err("v1.19.28 signed timeout overflow should fail")
            .kind,
        MihomoErrorKind::ProbeFailed
    );
    fixture.finish();
}

fn delay_handler(expected_path: &'static str, test_url: &'static str, timeout_ms: u64) -> Handler {
    Box::new(move |mut stream| {
        let request = read_request(&stream);
        let authorization = format!("Bearer {SECRET}");
        assert_eq!(request.method, "GET");
        assert_eq!(
            request.headers.get("authorization").map(String::as_str),
            Some(authorization.as_str())
        );
        let (path, query) = request
            .target
            .split_once('?')
            .expect("Delay Probe request should have a query");
        assert_eq!(path, expected_path);
        assert!(
            query.contains(
                "url=https%3A%2F%2Fexample.test%2Fgenerate_204%3Fx%3D1%26name%3Dtwo+words"
            )
        );
        let values = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect::<BTreeMap<_, _>>();
        let expected_timeout = timeout_ms.to_string();
        assert_eq!(values.len(), 2);
        assert_eq!(values.get("url").map(String::as_str), Some(test_url));
        assert_eq!(
            values.get("timeout").map(String::as_str),
            Some(expected_timeout.as_str())
        );
        stream
            .write_all(&json_response("200 OK", DELAY))
            .expect("delay response should be written");
        finish_http_response(&mut stream);
    })
}

#[test]
fn connection_summary_uses_the_bounded_rest_projection() {
    let fixture = UnixFixture::new(
        "connections",
        vec![handler(
            "GET",
            "/connections",
            SECRET,
            json_response("200 OK", CONNECTIONS),
        )],
    );
    let endpoint = fixture.endpoint(SECRET);

    assert_eq!(
        adapter()
            .connection_summary(&endpoint)
            .expect("connection summary should decode"),
        ConnectionSummary {
            active_connections: 2,
            upload_total_bytes: 4096,
            download_total_bytes: 8192,
            memory_bytes: Some(1_048_576),
        }
    );
    fixture.finish();
}

#[test]
fn websocket_streams_authorize_decode_tag_and_cancel_each_event_type() {
    let fixture = UnixFixture::new(
        "websocket-streams",
        vec![
            websocket_handler("/traffic", TRAFFIC),
            websocket_handler("/connections", CONNECTIONS),
            websocket_handler("/logs?level=debug", LOG),
        ],
    );
    let endpoint = fixture.endpoint(SECRET);
    let adapter = adapter();

    let mut traffic = adapter
        .open_traffic_stream(&endpoint, CoreInstanceGeneration(11))
        .expect("traffic stream should open");
    let traffic_event = traffic
        .next_event()
        .expect("traffic event should decode")
        .expect("traffic event should exist");
    assert_eq!(
        traffic_event.instance_generation,
        CoreInstanceGeneration(11)
    );
    assert_eq!(
        traffic_event.payload,
        TrafficFrame {
            upload_bytes_per_second: 1024,
            download_bytes_per_second: 2048,
        }
    );
    traffic.cancel();
    assert!(
        traffic
            .next_event()
            .expect("cancel should be clean")
            .is_none()
    );

    let mut connections = adapter
        .open_connection_stream(&endpoint, CoreInstanceGeneration(12))
        .expect("connection stream should open");
    let connection_event = connections
        .next_event()
        .expect("connection event should decode")
        .expect("connection event should exist");
    assert_eq!(
        connection_event.instance_generation,
        CoreInstanceGeneration(12)
    );
    assert_eq!(connection_event.payload.active_connections, 2);
    connections.cancel();
    assert!(
        connections
            .next_event()
            .expect("cancel should be clean")
            .is_none()
    );

    let mut logs = adapter
        .open_log_stream(&endpoint, CoreInstanceGeneration(13))
        .expect("log stream should open");
    let log_event = logs
        .next_event()
        .expect("log event should decode")
        .expect("log event should exist");
    assert_eq!(log_event.instance_generation, CoreInstanceGeneration(13));
    assert_eq!(
        log_event.payload,
        MihomoLogFrame {
            level: MihomoLogLevel::Info,
            message: "[TCP] fixture connection established".to_owned(),
        }
    );
    logs.cancel();
    assert!(logs.next_event().expect("cancel should be clean").is_none());
    fixture.finish();
}

fn websocket_handler(target: &'static str, payload: &'static [u8]) -> Handler {
    Box::new(move |mut stream| {
        let request = read_request(&stream);
        assert_request(&request, "GET", target, SECRET);
        assert_header_token(&request, "connection", "upgrade");
        assert_header_token(&request, "upgrade", "websocket");
        assert_eq!(
            request
                .headers
                .get("sec-websocket-version")
                .map(String::as_str),
            Some("13")
        );
        let key = request
            .headers
            .get("sec-websocket-key")
            .expect("WebSocket key should exist");
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        stream
            .write_all(response.as_bytes())
            .expect("WebSocket handshake should be written");
        stream
            .write_all(&websocket_text_frame(payload))
            .expect("WebSocket fixture frame should be written");
        wait_for_client_shutdown(&mut stream);
    })
}

fn wait_for_client_shutdown(stream: &mut UnixStream) {
    let mut byte = [0_u8; 1];
    match stream.read(&mut byte) {
        Ok(0) => {}
        Ok(_) => panic!("fixture WebSocket client sent an unexpected frame"),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset
                    | io::ErrorKind::TimedOut
                    | io::ErrorKind::WouldBlock
            ) => {}
        Err(error) => panic!("fixture WebSocket shutdown should be readable: {error}"),
    }
}

fn assert_header_token(request: &Request, name: &str, expected: &str) {
    assert!(
        request
            .headers
            .get(name)
            .expect("required header should exist")
            .split(',')
            .any(|token| token.trim().eq_ignore_ascii_case(expected))
    );
}

fn websocket_text_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x81];
    match payload.len() {
        length @ 0..=125 => frame.push(length as u8),
        length @ 126..=65_535 => {
            frame.push(126);
            frame.extend_from_slice(&(length as u16).to_be_bytes());
        }
        length => {
            frame.push(127);
            frame.extend_from_slice(&(length as u64).to_be_bytes());
        }
    }
    frame.extend_from_slice(payload);
    frame
}

#[test]
fn response_and_frame_limits_content_type_and_read_deadline_are_enforced() {
    let oversized_header = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Filler: {}\r\nContent-Length: 2\r\n\r\n{{}}",
        "x".repeat(256)
    )
    .into_bytes();
    let fixture = UnixFixture::new(
        "limits",
        vec![
            handler(
                "GET",
                "/version",
                SECRET,
                response("200 OK", Some("text/plain"), VERSION),
            ),
            handler("GET", "/version", SECRET, oversized_header),
            handler(
                "GET",
                "/version",
                SECRET,
                json_response("200 OK", br#"{"version":"v1.19.28"}"#),
            ),
            Box::new(|stream| {
                let request = read_request(&stream);
                assert_request(&request, "GET", "/version", SECRET);
                thread::sleep(Duration::from_millis(100));
            }),
            oversized_websocket_handler(),
        ],
    );
    let endpoint = fixture.endpoint(SECRET);

    assert_eq!(
        adapter()
            .version(&endpoint)
            .expect_err("wrong content type should fail")
            .kind,
        MihomoErrorKind::InvalidResponse
    );

    let header_config = MihomoAdapterConfig {
        max_response_header_bytes: 128,
        ..MihomoAdapterConfig::default()
    };
    assert_eq!(
        UnixMihomoAdapter::new(header_config)
            .expect("header limit should be valid")
            .version(&endpoint)
            .expect_err("oversized header should fail")
            .kind,
        MihomoErrorKind::InvalidResponse
    );

    let body_config = MihomoAdapterConfig {
        max_response_body_bytes: 4,
        ..MihomoAdapterConfig::default()
    };
    assert_eq!(
        UnixMihomoAdapter::new(body_config)
            .expect("body limit should be valid")
            .version(&endpoint)
            .expect_err("oversized body should fail")
            .kind,
        MihomoErrorKind::InvalidResponse
    );

    let deadline_config = MihomoAdapterConfig {
        read_timeout: Duration::from_millis(20),
        ..MihomoAdapterConfig::default()
    };
    let started = Instant::now();
    assert_eq!(
        UnixMihomoAdapter::new(deadline_config)
            .expect("read deadline should be valid")
            .version(&endpoint)
            .expect_err("silent response should time out")
            .kind,
        MihomoErrorKind::Unavailable
    );
    assert!(started.elapsed() < Duration::from_secs(1));

    let frame_config = MihomoAdapterConfig {
        max_websocket_frame_bytes: 8,
        ..MihomoAdapterConfig::default()
    };
    let mut stream = UnixMihomoAdapter::new(frame_config)
        .expect("frame limit should be valid")
        .open_traffic_stream(&endpoint, CoreInstanceGeneration(21))
        .expect("limited traffic stream should open");
    assert_eq!(
        stream
            .next_event()
            .expect_err("oversized frame should fail")
            .kind,
        MihomoErrorKind::InvalidResponse
    );
    stream.cancel();
    fixture.finish();
}

fn oversized_websocket_handler() -> Handler {
    Box::new(|mut stream| {
        let request = read_request(&stream);
        assert_request(&request, "GET", "/traffic", SECRET);
        let key = request
            .headers
            .get("sec-websocket-key")
            .expect("WebSocket key should exist");
        let accept = derive_accept_key(key.as_bytes());
        stream
            .write_all(
                format!(
                    "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
                )
                .as_bytes(),
            )
            .expect("WebSocket handshake should be written");
        stream
            .write_all(&websocket_text_frame(TRAFFIC))
            .expect("oversized WebSocket frame should be written");
        wait_for_client_shutdown(&mut stream);
    })
}

#[test]
fn configuration_requires_positive_timeouts_and_limits() {
    let mutations: [fn(&mut MihomoAdapterConfig); 6] = [
        |config: &mut MihomoAdapterConfig| config.connect_timeout = Duration::ZERO,
        |config: &mut MihomoAdapterConfig| config.read_timeout = Duration::ZERO,
        |config: &mut MihomoAdapterConfig| config.write_timeout = Duration::ZERO,
        |config: &mut MihomoAdapterConfig| config.max_response_header_bytes = 0,
        |config: &mut MihomoAdapterConfig| config.max_response_body_bytes = 0,
        |config: &mut MihomoAdapterConfig| config.max_websocket_frame_bytes = 0,
    ];
    for mutate in mutations {
        let mut config = MihomoAdapterConfig::default();
        mutate(&mut config);
        assert!(UnixMihomoAdapter::new(config).is_err());
    }
}

#[test]
fn diagnostics_redact_bearer_socket_path_and_response_body() {
    let response_secret = "response-body-secret";
    let fixture = UnixFixture::new(
        "path-secret",
        vec![handler(
            "GET",
            "/version",
            "auth-secret",
            json_response("500 Internal Server Error", response_secret.as_bytes()),
        )],
    );
    let endpoint = fixture.endpoint("auth-secret");
    let adapter = adapter();
    let error = adapter
        .version(&endpoint)
        .expect_err("server failure should be reported");

    assert_eq!(error.kind, MihomoErrorKind::Unavailable);
    for rendered in [
        format!("{adapter:?}"),
        format!("{error:?}"),
        error.to_string(),
    ] {
        assert!(!rendered.contains("auth-secret"));
        assert!(!rendered.contains("path-secret"));
        assert!(!rendered.contains(response_secret));
    }
    fixture.finish();

    let missing_path = unique_missing_socket("missing-path-secret");
    let missing_endpoint = CoreControlEndpoint::new(&missing_path, "missing-auth-secret");
    let started = Instant::now();
    let error = adapter
        .version(&missing_endpoint)
        .expect_err("missing Unix socket should fail");
    assert_eq!(error.kind, MihomoErrorKind::Unavailable);
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(!format!("{error:?}").contains("missing-path-secret"));
    assert!(!error.to_string().contains("missing-auth-secret"));
}

fn unique_missing_socket(label: &str) -> PathBuf {
    static NEXT_ID: AtomicU64 = AtomicU64::new(10_000);
    std::env::temp_dir().join(format!(
        "hopash-{label}-{}-{}.sock",
        std::process::id(),
        NEXT_ID.fetch_add(1, Ordering::Relaxed)
    ))
}

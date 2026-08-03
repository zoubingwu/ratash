use ratash::domain::SubscriptionUrl;
use ratash::profile::{ProfileSnapshot, RefreshStage, SnapshotLimits};
use ratash::profile_source::{
    DownloadErrorKind, ProfileSource, ProfileSourcePolicy, ReqwestProfileSource,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

enum ServerStep {
    Write(Vec<u8>),
    Wait(Duration),
}

struct TestServer {
    authority: String,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn start(responses: Vec<Vec<ServerStep>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the loopback fixture should bind");
        let address = listener
            .local_addr()
            .expect("the loopback fixture should have an address");
        let task = tokio::spawn(async move {
            for response in responses {
                let (mut socket, _) = listener
                    .accept()
                    .await
                    .expect("the fixture should accept a request");
                let _ = read_request_headers(&mut socket).await;
                for step in response {
                    match step {
                        ServerStep::Write(bytes) => {
                            if socket.write_all(&bytes).await.is_err() {
                                break;
                            }
                        }
                        ServerStep::Wait(duration) => tokio::time::sleep(duration).await,
                    }
                }
                let _ = socket.shutdown().await;
            }
        });
        Self {
            authority: address.to_string(),
            task,
        }
    }

    fn url(&self, path_and_query: &str) -> SubscriptionUrl {
        SubscriptionUrl::parse(&format!("http://{}{path_and_query}", self.authority))
            .expect("the loopback fixture URL should be valid")
    }

    fn credential_url(&self, path_and_query: &str) -> SubscriptionUrl {
        SubscriptionUrl::parse(&format!(
            "http://alice:password@{}{path_and_query}",
            self.authority
        ))
        .expect("the loopback fixture URL should be valid")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request_headers(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buffer = [0_u8; 1_024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = socket
            .read(&mut buffer)
            .await
            .expect("the fixture request should be readable");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        assert!(
            request.len() <= 16 * 1_024,
            "fixture request headers grew too large"
        );
    }
    request
}

fn write(bytes: impl Into<Vec<u8>>) -> ServerStep {
    ServerStep::Write(bytes.into())
}

fn policy(max_body_bytes: usize) -> ProfileSourcePolicy {
    ProfileSourcePolicy {
        connect_timeout: Duration::from_millis(200),
        request_timeout: Duration::from_millis(200),
        total_timeout: Duration::from_secs(1),
        max_redirects: 3,
        max_body_bytes,
        max_metadata_name_bytes: 80,
    }
}

fn fixed_response(status: &str, headers: &str, body: &[u8]) -> Vec<ServerStep> {
    vec![write(
        format!(
            "HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n",
            body.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body.iter().copied())
        .collect::<Vec<_>>(),
    )]
}

#[test]
fn rejects_zero_duration_and_capacity_limits() {
    let base = policy(1_024);
    let invalid_policies = [
        ProfileSourcePolicy {
            connect_timeout: Duration::ZERO,
            ..base
        },
        ProfileSourcePolicy {
            request_timeout: Duration::ZERO,
            ..base
        },
        ProfileSourcePolicy {
            total_timeout: Duration::ZERO,
            ..base
        },
        ProfileSourcePolicy {
            max_body_bytes: 0,
            ..base
        },
        ProfileSourcePolicy {
            max_metadata_name_bytes: 0,
            ..base
        },
    ];

    for limits in invalid_policies {
        let error = ReqwestProfileSource::new(limits)
            .err()
            .expect("zero limits should be rejected");
        assert_eq!(error.kind(), DownloadErrorKind::InvalidPolicy);
        assert_eq!(error.stage(), RefreshStage::Download);
        assert!(!error.retryable());
    }
}

#[tokio::test]
async fn downloads_profile_with_bounded_metadata_and_a_sanitized_final_url() {
    let body = b"proxies: []\nrules: []\n";
    let server = TestServer::start(vec![fixed_response(
        "200 OK",
        "Profile-Title: Primary Team\r\n",
        body,
    )])
    .await;
    let source = ReqwestProfileSource::new(policy(1_024)).expect("the source should initialize");
    let source: Box<dyn ProfileSource> = Box::new(source);
    let url = server.credential_url("/token-private-value.yaml?credential=query-secret");

    let download = source
        .download(&url)
        .await
        .expect("the fixture profile should download");

    assert_eq!(download.body(), body);
    assert_eq!(download.metadata_name(), Some("Primary Team"));
    assert_eq!(
        download.safe_final_url(),
        format!(
            "http://{}/[redacted]?[redacted]=[redacted]",
            server.authority
        )
    );
    let debug = format!("{download:?}");
    for secret in ["alice", "password", "private-value", "query-secret"] {
        assert!(!debug.contains(secret), "{secret} leaked in {debug}");
    }
}

#[tokio::test]
async fn downloads_yaml_from_a_user_agent_negotiated_subscription() {
    const YAML_PROFILE: &[u8] = b"proxies: []\nrules: []\n";
    const GENERIC_SUBSCRIPTION: &[u8] = b"c3M6Ly9maXh0dXJlCg==";

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the content negotiation fixture should bind");
    let address = listener
        .local_addr()
        .expect("the content negotiation fixture should have an address");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .expect("the fixture should accept a request");
        let request = read_request_headers(&mut socket).await;
        let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
        let expected_user_agent = format!(
            "\r\nuser-agent: clash-verge/v{}\r\n",
            env!("CARGO_PKG_VERSION")
        );
        let body = if request.contains(&expected_user_agent) {
            YAML_PROFILE
        } else {
            GENERIC_SUBSCRIPTION
        };
        let response = fixed_response("200 OK", "", body);
        for step in response {
            if let ServerStep::Write(bytes) = step {
                socket
                    .write_all(&bytes)
                    .await
                    .expect("the fixture response should write");
            }
        }
    });
    let source = ReqwestProfileSource::new(policy(1_024)).expect("the source should initialize");
    let url = SubscriptionUrl::parse(&format!("http://{address}/subscription"))
        .expect("the loopback subscription URL should be valid");

    let download = source
        .download(&url)
        .await
        .expect("the negotiated Profile should download");
    let snapshot = ProfileSnapshot::parse(download.body(), SnapshotLimits::new(1_024, 16))
        .expect("the negotiated response should be a YAML Profile");

    assert_eq!(snapshot.raw(), YAML_PROFILE);
    server.await.expect("the fixture should stop cleanly");
}

#[tokio::test]
async fn extracts_utf8_content_disposition_after_rejecting_oversized_title() {
    let server = TestServer::start(vec![fixed_response(
        "200 OK",
        concat!(
            "Profile-Title: title-is-too-long\r\n",
            "Content-Disposition: attachment; filename*=UTF-8''Team%20One.yaml\r\n"
        ),
        b"rules: []\n",
    )])
    .await;
    let mut limits = policy(1_024);
    limits.max_metadata_name_bytes = 12;
    let source = ReqwestProfileSource::new(limits).expect("the source should initialize");

    let download = source
        .download(&server.url("/profile"))
        .await
        .expect("the fixture profile should download");

    assert_eq!(download.metadata_name(), Some("Team One"));
}

#[tokio::test]
async fn ignores_metadata_names_with_control_characters() {
    let server = TestServer::start(vec![fixed_response(
        "200 OK",
        "Profile-Title: invalid\tname\r\n",
        b"rules: []\n",
    )])
    .await;
    let source = ReqwestProfileSource::new(policy(1_024)).expect("the source should initialize");

    let download = source
        .download(&server.url("/profile"))
        .await
        .expect("the fixture profile should download");

    assert_eq!(download.metadata_name(), None);
}

#[tokio::test]
async fn rejects_oversized_content_length_before_reading_the_body() {
    let response = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Connection: close\r\n",
        "Content-Length: 1024\r\n",
        "\r\n"
    );
    let server = TestServer::start(vec![vec![write(response.as_bytes())]]).await;
    let source = ReqwestProfileSource::new(policy(8)).expect("the source should initialize");

    let error = source
        .download(&server.url("/profile"))
        .await
        .expect_err("the declared body size should be rejected");

    assert_eq!(error.stage(), RefreshStage::Download);
    assert_eq!(error.kind(), DownloadErrorKind::BodyTooLarge { limit: 8 });
    assert!(!error.retryable());
}

#[tokio::test]
async fn rejects_chunked_bodies_when_the_stream_crosses_the_limit() {
    let response = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Connection: close\r\n",
        "Transfer-Encoding: chunked\r\n",
        "\r\n",
        "6\r\n123456\r\n",
        "6\r\n789abc\r\n",
        "0\r\n\r\n"
    );
    let server = TestServer::start(vec![vec![write(response.as_bytes())]]).await;
    let source = ReqwestProfileSource::new(policy(10)).expect("the source should initialize");

    let error = source
        .download(&server.url("/profile"))
        .await
        .expect_err("the streamed body size should be rejected");

    assert_eq!(error.kind(), DownloadErrorKind::BodyTooLarge { limit: 10 });
}

#[tokio::test]
async fn rejects_redirects_to_unsupported_schemes() {
    let response = concat!(
        "HTTP/1.1 302 Found\r\n",
        "Connection: close\r\n",
        "Location: file:///tmp/private-token\r\n",
        "Content-Length: 0\r\n",
        "\r\n"
    );
    let server = TestServer::start(vec![vec![write(response.as_bytes())]]).await;
    let source = ReqwestProfileSource::new(policy(1_024)).expect("the source should initialize");

    let error = source
        .download(&server.url("/profile"))
        .await
        .expect_err("the redirect scheme should be rejected");

    assert_eq!(error.kind(), DownloadErrorKind::RedirectRejected);
    assert!(!format!("{error:?} {error}").contains("private-token"));
}

#[tokio::test]
async fn enforces_the_redirect_hop_limit() {
    let response = concat!(
        "HTTP/1.1 302 Found\r\n",
        "Connection: close\r\n",
        "Location: /second\r\n",
        "Content-Length: 0\r\n",
        "\r\n"
    );
    let server = TestServer::start(vec![vec![write(response.as_bytes())]]).await;
    let mut limits = policy(1_024);
    limits.max_redirects = 0;
    let source = ReqwestProfileSource::new(limits).expect("the source should initialize");

    let error = source
        .download(&server.url("/first"))
        .await
        .expect_err("the first redirect should exceed the zero-hop limit");

    assert_eq!(error.kind(), DownloadErrorKind::RedirectRejected);
}

#[tokio::test]
async fn enforces_the_response_idle_timeout() {
    let server = TestServer::start(vec![vec![
        ServerStep::Wait(Duration::from_millis(100)),
        write(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"),
    ]])
    .await;
    let mut limits = policy(1_024);
    limits.request_timeout = Duration::from_millis(20);
    let source = ReqwestProfileSource::new(limits).expect("the source should initialize");

    let error = source
        .download(&server.url("/profile"))
        .await
        .expect_err("the response wait should time out");

    assert_eq!(error.kind(), DownloadErrorKind::RequestTimeout);
    assert!(error.retryable());
}

#[tokio::test]
async fn enforces_the_total_download_timeout_across_active_chunks() {
    let headers = concat!(
        "HTTP/1.1 200 OK\r\n",
        "Connection: close\r\n",
        "Transfer-Encoding: chunked\r\n",
        "\r\n"
    );
    let server = TestServer::start(vec![vec![
        write(headers.as_bytes()),
        ServerStep::Wait(Duration::from_millis(30)),
        write(b"1\r\na\r\n"),
        ServerStep::Wait(Duration::from_millis(30)),
        write(b"1\r\nb\r\n0\r\n\r\n"),
    ]])
    .await;
    let mut limits = policy(1_024);
    limits.request_timeout = Duration::from_millis(100);
    limits.total_timeout = Duration::from_millis(45);
    let source = ReqwestProfileSource::new(limits).expect("the source should initialize");

    let error = source
        .download(&server.url("/profile"))
        .await
        .expect_err("the total download should time out");

    assert_eq!(error.kind(), DownloadErrorKind::TotalTimeout);
}

#[tokio::test]
async fn cancellation_releases_a_stalled_response_body_immediately() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the loopback fixture should bind");
    let address = listener
        .local_addr()
        .expect("the loopback fixture should have an address");
    let (body_stalled_sender, body_stalled_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener
            .accept()
            .await
            .expect("the fixture should accept a request");
        read_request_headers(&mut socket).await;
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 1024\r\n\r\n")
            .await
            .expect("the fixture response headers should write");
        let _ = body_stalled_sender.send(());
        tokio::time::sleep(Duration::from_secs(30)).await;
    });
    let mut limits = policy(2_048);
    limits.request_timeout = Duration::from_secs(30);
    limits.total_timeout = Duration::from_secs(30);
    let source: Arc<dyn ProfileSource> =
        Arc::new(ReqwestProfileSource::new(limits).expect("the source should initialize"));
    let url = SubscriptionUrl::parse(&format!("http://{address}/profile"))
        .expect("the loopback fixture URL should be valid");
    let download_source = Arc::clone(&source);
    let download = tokio::spawn(async move { download_source.download(&url).await });

    body_stalled_receiver
        .await
        .expect("the fixture should stall after its response headers");
    assert!(!download.is_finished());
    source.cancel_pending();
    let error = tokio::time::timeout(Duration::from_millis(250), download)
        .await
        .expect("cancellation should release the stalled download")
        .expect("the download task should join")
        .expect_err("the stalled download should be cancelled");

    assert_eq!(error.kind(), DownloadErrorKind::Cancelled);
    assert!(!error.retryable());
    server.abort();
}

#[tokio::test]
async fn error_display_and_debug_never_include_subscription_credentials() {
    let server = TestServer::start(vec![fixed_response(
        "503 Service Unavailable",
        "",
        b"secret response",
    )])
    .await;
    let source = ReqwestProfileSource::new(policy(1_024)).expect("the source should initialize");
    let url = server.credential_url("/token-path-secret?credential=query-secret");

    let error = source
        .download(&url)
        .await
        .expect_err("the fixture status should fail");
    let diagnostic = format!("{error:?} {error}");

    assert_eq!(error.kind(), DownloadErrorKind::HttpStatus { status: 503 });
    assert!(error.retryable());
    for secret in [
        "alice",
        "password",
        "path-secret",
        "query-secret",
        "secret response",
    ] {
        assert!(
            !diagnostic.contains(secret),
            "{secret} leaked in {diagnostic}"
        );
    }
}

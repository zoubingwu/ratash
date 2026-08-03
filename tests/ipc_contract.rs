use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;

use ratash::application::{ApplicationOperation, RulePlacement as ApplicationRulePlacement};
use ratash::constants::{
    CORE_LOG_LINE_MAX_BYTES, IPC_FRAME_MAX_BYTES, LOG_SUBSCRIBER_CAPACITY, LOG_SUBSCRIBER_MAX_BYTES,
};
use ratash::domain::SubscriptionUrl;
use ratash::ipc::{
    AcceptError, CorrelationError, EmptyPayload, FrameError, IPC_PROTOCOL_VERSION, IpcError,
    IpcRequest, IpcResponse, LogStreamItem, LogSubscriber, LogSubscriptionPayload, LogTailPayload,
    LogTailV1, NodeSelectorPayload, PeerAuthorizationError, PeerAuthorizer, ProfileAddPayload,
    ProfileListPagePayload, ProfileSelectorPayload, ProxyListPagePayload, ProxyListPayload,
    ProxySelectPayload, RequestId, RequestOperation, RuleAddPayload, RuleListPagePayload,
    RulePlacement, RuleReplacePayload, RuleSelectorPayload, StatusStreamItem, StatusSubscriber,
    StatusSubscriptionPayload, SubscriberPublishStatus, accept_authorized, bind_private_listener,
    read_frame, write_frame,
};
use ratash::telemetry::{CoreLogRecord, LogBuffer, LogLevel, LogSource};

#[test]
fn request_dtos_round_trip_every_remote_cli_operation() {
    let subscription_url = SubscriptionUrl::parse(
        "https://user:password@example.com/subscription.yaml?token=secret-value",
    )
    .expect("fixture URL should be valid");
    let operations = vec![
        RequestOperation::Start(empty()),
        RequestOperation::Stop(empty()),
        RequestOperation::Restart(empty()),
        RequestOperation::GetStatus(empty()),
        RequestOperation::SubscribeStatus(StatusSubscriptionPayload {
            after_sequence: Some(4),
        }),
        RequestOperation::ProfileAdd(ProfileAddPayload::new(&subscription_url)),
        RequestOperation::ProfileList(empty()),
        RequestOperation::ProfileListPage(ProfileListPagePayload { offset: 128 }),
        RequestOperation::ProfileUse(ProfileSelectorPayload {
            profile: "work".to_owned(),
        }),
        RequestOperation::ProfileRemove(ProfileSelectorPayload {
            profile: "archive".to_owned(),
        }),
        RequestOperation::ProxyList(ProxyListPayload {
            group: "Automatic".to_owned(),
        }),
        RequestOperation::ProxyListPage(ProxyListPagePayload {
            group: "Automatic".to_owned(),
            groups_offset: 128,
            nodes_offset: 256,
        }),
        RequestOperation::ProxySelect(ProxySelectPayload {
            group: "Automatic".to_owned(),
            node: "Tokyo".to_owned(),
        }),
        RequestOperation::LatencyList(empty()),
        RequestOperation::LatencyShow(NodeSelectorPayload {
            node: "Tokyo".to_owned(),
        }),
        RequestOperation::RuleList(empty()),
        RequestOperation::RuleListPage(RuleListPagePayload { offset: 128 }),
        RequestOperation::RuleAdd(RuleAddPayload {
            rule: "DOMAIN,example.com,DIRECT".to_owned(),
            placement: RulePlacement::Before("MATCH,Proxy".to_owned()),
        }),
        RequestOperation::RuleReplace(RuleReplacePayload {
            old_rule: "MATCH,Proxy".to_owned(),
            new_rule: "MATCH,DIRECT".to_owned(),
        }),
        RequestOperation::RuleRemove(RuleSelectorPayload {
            rule: "MATCH,DIRECT".to_owned(),
        }),
        RequestOperation::FollowLogs(LogSubscriptionPayload {
            after_sequence: Some(90),
        }),
        RequestOperation::LogTail(LogTailPayload {
            after_sequence: Some(80),
        }),
    ];

    for (index, operation) in operations.into_iter().enumerate() {
        let request = IpcRequest::new(RequestId(index as u64 + 1), operation);
        let encoded = serde_json::to_value(&request).expect("request should serialize");
        let decoded: IpcRequest =
            serde_json::from_value(encoded.clone()).expect("request should deserialize");

        assert_eq!(decoded, request);
        assert_eq!(encoded["protocol_version"], IPC_PROTOCOL_VERSION);
        assert!(encoded.get("operation").is_some());
        assert!(encoded.get("payload").is_some());
    }
}

#[test]
fn request_operations_convert_to_the_shared_application_contract() {
    let url = SubscriptionUrl::parse("https://example.com/profile.yaml")
        .expect("fixture URL should be valid");

    assert_eq!(
        RequestOperation::ProfileAdd(ProfileAddPayload::new(&url))
            .into_application_operation()
            .expect("Profile add should map to an application operation"),
        ApplicationOperation::ProfileAdd {
            subscription_url: url,
        }
    );
    assert_eq!(
        RequestOperation::RuleAdd(RuleAddPayload {
            rule: "MATCH,DIRECT".to_owned(),
            placement: RulePlacement::After("DOMAIN,example.com,Proxy".to_owned()),
        })
        .into_application_operation()
        .expect("Rule add should map to an application operation"),
        ApplicationOperation::RuleAdd {
            rule: "MATCH,DIRECT".to_owned(),
            placement: ApplicationRulePlacement::After("DOMAIN,example.com,Proxy".to_owned()),
        }
    );
}

#[test]
fn profile_add_debug_output_redacts_the_complete_subscription_url() {
    let secret_url = "https://user:password@example.com/secret-path?token=secret-query";
    let url = SubscriptionUrl::parse(secret_url).expect("fixture URL should be valid");
    let request = IpcRequest::new(
        RequestId(7),
        RequestOperation::ProfileAdd(ProfileAddPayload::new(&url)),
    );

    let debug = format!("{request:?}");
    let wire = serde_json::to_string(&request).expect("request should serialize");

    assert!(!debug.contains("password"));
    assert!(!debug.contains("secret-path"));
    assert!(!debug.contains("secret-query"));
    assert!(debug.contains("[REDACTED]"));
    assert!(wire.contains("secret-query"));
}

#[test]
fn protocol_mismatch_returns_a_correlated_structured_error() {
    let mut request = IpcRequest::new(RequestId(42), RequestOperation::GetStatus(empty()));
    request.protocol_version = IPC_PROTOCOL_VERSION + 1;

    let response = request
        .validate_protocol()
        .expect_err("mismatched protocol should be rejected");

    assert_eq!(response.request_id, RequestId(42));
    assert_eq!(
        response.error().expect("error should exist").code,
        "protocol_mismatch"
    );
    assert_eq!(
        response
            .error()
            .expect("error should exist")
            .details
            .as_ref()
            .expect("details should exist")["actual"],
        IPC_PROTOCOL_VERSION + 1
    );
}

#[test]
fn responses_enforce_protocol_and_request_id_correlation() {
    let response = IpcResponse::success(RequestId(12), serde_json::json!({ "ready": true }));
    assert_eq!(response.ensure_correlated(RequestId(12)), Ok(()));
    assert_eq!(
        response.ensure_correlated(RequestId(11)),
        Err(CorrelationError::RequestIdMismatch {
            expected: RequestId(11),
            actual: RequestId(12),
        })
    );

    let mut incompatible = response;
    incompatible.protocol_version += 1;
    assert_eq!(
        incompatible.ensure_correlated(RequestId(12)),
        Err(CorrelationError::ProtocolMismatch {
            expected: IPC_PROTOCOL_VERSION,
            actual: IPC_PROTOCOL_VERSION + 1,
        })
    );
}

#[test]
fn success_and_error_response_envelopes_round_trip() {
    let success = IpcResponse::success(RequestId(1), serde_json::json!({ "state": "ready" }));
    let failure = IpcResponse::failure(
        RequestId(2),
        IpcError {
            code: "profile_not_found".to_owned(),
            message: "The Profile does not exist".to_owned(),
            retryable: false,
            details: None,
        },
    );

    let success_value = serde_json::to_value(&success).expect("response should serialize");
    let failure_value = serde_json::to_value(&failure).expect("response should serialize");
    let success_round_trip: IpcResponse =
        serde_json::from_value(success_value.clone()).expect("response should deserialize");
    let failure_round_trip: IpcResponse =
        serde_json::from_value(failure_value.clone()).expect("response should deserialize");

    assert_eq!(success_round_trip, success);
    assert_eq!(failure_round_trip, failure);
    assert!(success_value.get("data").is_some());
    assert!(success_value.get("error").is_none());
    assert!(failure_value.get("data").is_none());
    assert!(failure_value.get("error").is_some());
}

#[test]
fn error_debug_output_omits_message_and_details() {
    let error = IpcError::new(
        ratash::error::ErrorCode::ExternalOperationFailed,
        "credential=message-secret",
        true,
    )
    .with_details(serde_json::json!({ "token": "details-secret" }));

    let debug = format!("{error:?}");

    assert!(!debug.contains("message-secret"));
    assert!(!debug.contains("details-secret"));
    assert!(debug.contains("external_operation_failed"));
    assert!(debug.contains("has_details: true"));
}

#[test]
fn frame_round_trip_handles_partial_reads_and_writes() {
    let request = IpcRequest::new(
        RequestId(99),
        RequestOperation::ProxySelect(ProxySelectPayload {
            group: "Automatic".to_owned(),
            node: "Tokyo".to_owned(),
        }),
    );
    let mut writer = ChunkWriter::new(3);

    write_frame(&mut writer, &request).expect("partial writer should receive the full frame");

    let bytes = writer.into_inner();
    let encoded_length = u32::from_be_bytes(bytes[0..4].try_into().expect("header should exist"));
    assert_eq!(encoded_length as usize, bytes.len() - 4);
    let mut reader = ChunkReader::new(bytes, 2);
    let decoded: IpcRequest =
        read_frame(&mut reader).expect("partial reader should return the full frame");
    assert_eq!(decoded, request);
}

#[test]
fn frame_reader_rejects_oversized_headers_before_reading_a_body() {
    let oversized = u32::try_from(IPC_FRAME_MAX_BYTES + 1)
        .expect("configured frame limit should fit a u32")
        .to_be_bytes();
    let mut reader = ChunkReader::new(oversized.to_vec(), 1);

    let error = read_frame::<_, serde_json::Value>(&mut reader)
        .expect_err("oversized frame should be rejected");

    assert!(matches!(
        error,
        FrameError::FrameTooLarge {
            limit: IPC_FRAME_MAX_BYTES,
            actual
        } if actual == IPC_FRAME_MAX_BYTES + 1
    ));
}

#[test]
fn frame_writer_rejects_oversized_json() {
    let value = "x".repeat(IPC_FRAME_MAX_BYTES);
    let mut writer = Vec::new();

    let error =
        write_frame(&mut writer, &value).expect_err("JSON overhead exceeds the frame limit");

    assert!(matches!(error, FrameError::FrameTooLarge { .. }));
    assert!(writer.is_empty());
}

#[test]
fn frame_reader_reports_truncated_payloads() {
    let mut bytes = 5_u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(b"{});");

    let error = read_frame::<_, serde_json::Value>(&mut bytes.as_slice())
        .expect_err("truncated payload should be rejected");

    assert!(matches!(
        error,
        FrameError::Io(ref source) if source.kind() == io::ErrorKind::UnexpectedEof
    ));
}

#[test]
fn socket_binding_sets_private_parent_and_endpoint_permissions() {
    let fixture = TempFixture::new("permissions");
    let socket_path = fixture.path().join("ipc/ratash.sock");

    let listener = bind_private_listener(&socket_path).expect("private listener should bind");

    assert_eq!(
        mode(socket_path.parent().expect("parent should exist")),
        0o700
    );
    assert_eq!(mode(&socket_path), 0o600);
    drop(listener);
}

#[test]
fn peer_authorization_is_an_object_safe_accept_boundary() {
    let fixture = TempFixture::new("peer-auth");
    let socket_path = fixture.path().join("ipc/ratash.sock");
    let listener = bind_private_listener(&socket_path).expect("private listener should bind");
    let client_path = socket_path;
    let client = thread::spawn(move || {
        UnixStream::connect(client_path).expect("fixture client should connect")
    });
    let authorizer: Box<dyn PeerAuthorizer> = Box::new(RejectPeer);

    let result = accept_authorized(&listener, authorizer.as_ref());

    assert!(matches!(result, Err(AcceptError::Unauthorized(_))));
    drop(client.join().expect("fixture client thread should finish"));
}

#[test]
fn status_subscription_starts_with_a_snapshot_then_contiguous_events() {
    let mut subscriber =
        StatusSubscriber::new(3, 10, 100, serde_json::json!({ "lifecycle": "ready" }))
            .expect("capacity should be valid");

    assert!(matches!(
        subscriber.pop_front(),
        Some(StatusStreamItem::Snapshot { sequence: 10, .. })
    ));
    assert_eq!(
        subscriber.publish(11, 101, serde_json::json!({ "connections": 1 })),
        SubscriberPublishStatus::Queued
    );
    assert!(matches!(
        subscriber.pop_front(),
        Some(StatusStreamItem::Event { sequence: 11, .. })
    ));
}

#[test]
fn status_queue_overflow_collapses_to_one_resync_marker() {
    let mut subscriber = StatusSubscriber::new(2, 5, 100, serde_json::json!({ "full": true }))
        .expect("capacity should be valid");
    assert_eq!(
        subscriber.publish(6, 101, serde_json::json!({ "event": 1 })),
        SubscriberPublishStatus::Queued
    );

    assert_eq!(
        subscriber.publish(7, 102, serde_json::json!({ "event": 2 })),
        SubscriberPublishStatus::ResyncRequired
    );
    assert_eq!(
        subscriber.publish(8, 103, serde_json::json!({ "event": 3 })),
        SubscriberPublishStatus::AwaitingResync
    );
    assert_eq!(subscriber.len(), 1);
    assert!(subscriber.requires_resync());
    assert_eq!(
        subscriber.pop_front(),
        Some(StatusStreamItem::ResyncRequired {
            expected_sequence: 7,
            observed_sequence: 8,
        })
    );

    subscriber.resync(8, 104, serde_json::json!({ "full": "replacement" }));
    assert!(!subscriber.requires_resync());
    assert!(matches!(
        subscriber.pop_front(),
        Some(StatusStreamItem::Snapshot { sequence: 8, .. })
    ));
}

#[test]
fn status_sequence_gap_requires_a_full_snapshot() {
    let mut subscriber =
        StatusSubscriber::new(4, 20, 100, serde_json::json!({})).expect("capacity should be valid");
    subscriber.pop_front();

    assert_eq!(
        subscriber.publish(22, 101, serde_json::json!({ "gap": true })),
        SubscriberPublishStatus::ResyncRequired
    );
    assert_eq!(
        subscriber.pop_front(),
        Some(StatusStreamItem::ResyncRequired {
            expected_sequence: 21,
            observed_sequence: 22,
        })
    );
}

#[test]
fn exhausted_stream_sequences_require_resynchronization() {
    let mut status = StatusSubscriber::new(2, u64::MAX, 100, serde_json::json!({}))
        .expect("capacity should be valid");
    status.pop_front();
    let mut logs = LogSubscriber::new(2, Some(u64::MAX)).expect("capacity should be valid");

    assert_eq!(
        status.publish(u64::MAX, 101, serde_json::json!({})),
        SubscriberPublishStatus::ResyncRequired
    );
    assert_eq!(
        logs.publish(&log_record(u64::MAX, "exhausted")),
        SubscriberPublishStatus::ResyncRequired
    );
}

#[test]
fn log_queue_overflow_emits_one_gap_marker_with_the_delivery_anchor() {
    let mut subscriber = LogSubscriber::new(2, None).expect("capacity should be valid");
    subscriber.publish(&log_record(1, "first"));
    let delivered = subscriber.pop_front().expect("first record should exist");
    assert!(matches!(delivered, LogStreamItem::Record { .. }));
    subscriber.publish(&log_record(2, "second"));
    subscriber.publish(&log_record(3, "third"));

    assert_eq!(
        subscriber.publish(&log_record(4, "fourth")),
        SubscriberPublishStatus::ResyncRequired
    );
    assert_eq!(
        subscriber.publish(&log_record(5, "fifth")),
        SubscriberPublishStatus::AwaitingResync
    );
    assert_eq!(subscriber.len(), 1);
    assert_eq!(subscriber.gap_count(), 1);
    assert_eq!(
        subscriber.pop_front(),
        Some(LogStreamItem::Gap {
            after_sequence: Some(1),
            latest_sequence: 5,
        })
    );
}

#[test]
fn log_queue_byte_overflow_emits_a_resync_marker() {
    let mut subscriber =
        LogSubscriber::new(LOG_SUBSCRIBER_CAPACITY, None).expect("capacity should be valid");
    let message = "x".repeat(CORE_LOG_LINE_MAX_BYTES);
    let retained_records = LOG_SUBSCRIBER_MAX_BYTES / CORE_LOG_LINE_MAX_BYTES;
    for sequence in 1..=retained_records {
        assert_eq!(
            subscriber.publish(&log_record(sequence as u64, &message)),
            SubscriberPublishStatus::Queued
        );
    }

    assert_eq!(
        subscriber.publish(&log_record((retained_records + 1) as u64, &message)),
        SubscriberPublishStatus::ResyncRequired
    );
    assert_eq!(subscriber.len(), 1);
    assert_eq!(subscriber.gap_count(), 1);
}

#[test]
fn log_gap_recovers_through_the_retained_tail() {
    let mut buffer = LogBuffer::new(3, 128).expect("fixture limits should be valid");
    for index in 1..=5 {
        buffer
            .push(
                index * 10,
                LogLevel::Info,
                LogSource::CoreApi,
                format!("log-{index}"),
            )
            .expect("fixture log should fit");
    }
    let tail = buffer.tail_after(Some(0));
    let projected = LogTailV1::from(tail.clone());
    let mut subscriber = LogSubscriber::new(2, Some(0)).expect("capacity should be valid");

    assert_eq!(
        subscriber.publish(&tail.records[0]),
        SubscriberPublishStatus::ResyncRequired
    );
    assert!(subscriber.awaiting_tail());
    assert!(projected.gap);
    assert_eq!(
        projected
            .records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        [3, 4, 5]
    );

    subscriber.mark_tail_sent(tail.latest_sequence);
    assert!(!subscriber.awaiting_tail());
    assert_eq!(
        subscriber.publish(&log_record(6, "next")),
        SubscriberPublishStatus::Queued
    );
}

#[test]
fn log_record_debug_output_omits_message_content() {
    let record = LogTailV1::from(ratash::telemetry::LogTail {
        records: vec![log_record(1, "credential=secret")],
        dropped_total: 0,
        gap: false,
        earliest_sequence: Some(1),
        latest_sequence: Some(1),
        sequence_horizon: Some(1),
    })
    .records
    .remove(0);

    let debug = format!("{record:?}");

    assert!(!debug.contains("credential"));
    assert!(!debug.contains("secret"));
    assert!(debug.contains("message_bytes"));
}

fn empty() -> EmptyPayload {
    EmptyPayload {}
}

fn log_record(sequence: u64, message: &str) -> CoreLogRecord {
    CoreLogRecord::new(
        sequence,
        sequence.saturating_mul(10),
        LogLevel::Info,
        LogSource::CoreApi,
        message,
    )
}

struct RejectPeer;

impl PeerAuthorizer for RejectPeer {
    fn authorize(&self, _peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
        Err(PeerAuthorizationError::new("peer UID is not authorized"))
    }
}

struct ChunkWriter {
    bytes: Vec<u8>,
    chunk_size: usize,
}

impl ChunkWriter {
    fn new(chunk_size: usize) -> Self {
        Self {
            bytes: Vec::new(),
            chunk_size,
        }
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = bytes.len().min(self.chunk_size);
        self.bytes.extend_from_slice(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct ChunkReader {
    bytes: Vec<u8>,
    position: usize,
    chunk_size: usize,
}

impl ChunkReader {
    fn new(bytes: Vec<u8>, chunk_size: usize) -> Self {
        Self {
            bytes,
            position: 0,
            chunk_size,
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let remaining = self.bytes.len().saturating_sub(self.position);
        let read = remaining.min(output.len()).min(self.chunk_size);
        output[..read].copy_from_slice(&self.bytes[self.position..self.position + read]);
        self.position += read;
        Ok(read)
    }
}

struct TempFixture {
    path: PathBuf,
}

impl TempFixture {
    fn new(label: &str) -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let path = std::env::temp_dir().join(format!(
            "ratash-ipc-{label}-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("fixture directory should be created");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempFixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("fixture directory should be removed");
    }
}

fn mode(path: &Path) -> u32 {
    fs::symlink_metadata(path)
        .expect("fixture path should exist")
        .permissions()
        .mode()
        & 0o777
}

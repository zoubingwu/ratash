use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;

use ratash::core::{
    ApplyCandidateResult, ApplyDisposition, ConnectionSummary, CoreControlEndpoint, CoreEvent,
    CoreEventStream, CoreRuntime, CoreRuntimeError, CoreRuntimeErrorKind, CoreRuntimeStatus,
    DelayProbeRequest, DelayProbeResult, ForwardedCoreLog, ForwardedCoreLogBatch, LatencyFreshness,
    ManagedCoreHandle, MihomoAdapter, MihomoError, MihomoErrorKind, MihomoJsonCodec,
    MihomoLogFrame, MihomoLogLevel, MihomoReadiness, MihomoVersion, NodeRowMemberV1, NodeSelection,
    NodeSource, OwnerSession, OwnerSessionProof, OwnerSessionRequest, ProbeObservation,
    ProbeStatus, ProjectionErrorKind, ProviderState, ProxyMember, ProxyView, ProxyViewOrderSource,
    RuntimeBundle, SelectionError, StopCoreResult, TrafficFrame, UnresolvedMemberReason,
    project_proxy_view,
};
use ratash::domain::{
    CoreInstanceGeneration, LatencySample, NodeRecordId, ProbeGeneration, ProxyGroupId,
    RuntimeGeneration, SampleState,
};

const PROXIES: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/proxies.json");
const PROVIDERS: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/providers.json");
const VERSION: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/version.json");
const DELAY: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/delay.json");
const TRAFFIC: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/traffic.json");
const CONNECTIONS: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/connections.json");
const LOG: &[u8] = include_bytes!("../fixtures/mihomo/v1.19.28/log.json");

fn group_order() -> Vec<String> {
    ["GLOBAL", "Manual", "Automatic", "Nested"]
        .map(str::to_owned)
        .to_vec()
}

fn fixture_view() -> ProxyView {
    project_proxy_view(PROXIES, Some(PROVIDERS), &group_order()).expect("fixture projection")
}

#[test]
fn v1_19_28_projection_preserves_groups_and_source_aware_nodes() {
    let view = fixture_view();

    assert_eq!(view.schema_version, 1);
    assert_eq!(
        view.order_source,
        ProxyViewOrderSource::EffectiveConfiguration
    );
    assert_eq!(view.provider_state, ProviderState::Ready);
    assert_eq!(
        view.groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>(),
        ["GLOBAL", "Manual", "Automatic", "Nested"]
    );
    assert_eq!(
        view.primary_group().map(|group| group.name.as_str()),
        Some("Manual")
    );
    assert_eq!(view.groups[1].id, ProxyGroupId::for_name("Manual"));
    assert!(view.groups[0].core_internal);
    assert!(view.groups[0].selectable);
    assert!(!view.groups[2].selectable);

    let core_id = NodeRecordId::for_core("core-a");
    let provider_id = NodeRecordId::for_provider("alpha", "provider-only");
    assert_ne!(core_id, provider_id);
    assert!(matches!(
        view.nodes[&core_id].source,
        NodeSource::Core { ref proxy_name } if proxy_name == "core-a"
    ));
    assert!(matches!(
        view.nodes[&provider_id].source,
        NodeSource::Provider {
            ref provider_name,
            ref proxy_name,
        } if provider_name == "alpha" && proxy_name == "provider-only"
    ));

    let manual = &view.groups[1];
    assert!(matches!(manual.members[0], ProxyMember::Node { ref name, .. } if name == "core-a"));
    assert!(
        matches!(manual.members[1], ProxyMember::Node { ref name, .. } if name == "provider-only")
    );
    assert!(matches!(
        manual.members[2],
        ProxyMember::Unresolved {
            reason: UnresolvedMemberReason::Ambiguous,
            ref candidate_ids,
            ..
        } if candidate_ids.len() == 2
    ));
    assert!(matches!(
        manual.members[3],
        ProxyMember::Unresolved {
            reason: UnresolvedMemberReason::Missing,
            ..
        }
    ));
    assert!(matches!(manual.members[4], ProxyMember::Group { ref name } if name == "Nested"));
}

#[test]
fn exact_selection_reports_each_resolution_state() {
    let view = fixture_view();

    let selection = view
        .resolve_exact_selection("Manual", "provider-only")
        .expect("unique provider node");
    assert_eq!(
        selection.record_id,
        NodeRecordId::for_provider("alpha", "provider-only")
    );
    assert!(matches!(
        view.resolve_exact_selection("Manual", "shared"),
        Err(SelectionError::NodeAmbiguous {
            ref candidate_ids,
            ..
        }) if candidate_ids.len() == 2
    ));
    assert_eq!(
        view.resolve_exact_selection("Manual", "missing"),
        Err(SelectionError::NodeMissing("missing".to_owned()))
    );
    assert_eq!(
        view.resolve_exact_selection("Manual", "Nested"),
        Err(SelectionError::TargetIsGroup("Nested".to_owned()))
    );
    assert_eq!(
        view.resolve_exact_selection("Automatic", "core-a"),
        Err(SelectionError::GroupNotSelectable("Automatic".to_owned()))
    );
    assert_eq!(
        view.resolve_exact_selection("Unknown", "core-a"),
        Err(SelectionError::GroupMissing("Unknown".to_owned()))
    );
}

#[test]
fn unavailable_provider_response_stays_distinct_from_missing_members() {
    let view = project_proxy_view(PROXIES, None, &group_order()).expect("proxies remain usable");
    let manual = &view.groups[1];

    assert_eq!(view.provider_state, ProviderState::Unavailable);
    assert!(matches!(
        manual.members[1],
        ProxyMember::Unresolved {
            reason: UnresolvedMemberReason::ProviderUnavailable,
            ..
        }
    ));
    assert_eq!(
        view.resolve_exact_selection("Manual", "provider-only"),
        Err(SelectionError::ProviderUnavailable(
            "provider-only".to_owned()
        ))
    );
}

#[test]
fn node_rows_preserve_every_member_state_and_merge_latency() {
    let view = fixture_view();
    let provider_id = NodeRecordId::for_provider("alpha", "provider-only");
    let observations = BTreeMap::from([(
        provider_id.clone(),
        ProbeObservation {
            sample: Some(LatencySample {
                node_id: provider_id.clone(),
                delay_ms: Some(42),
                sampled_at_unix_ms: Some(100),
                state: SampleState::Fresh,
                probe_generation: ProbeGeneration(7),
            }),
            status: ProbeStatus::Succeeded,
        },
    )]);

    let rows = view
        .node_rows("Manual", &observations)
        .expect("known group");

    assert_eq!(
        rows.iter().map(|row| row.name.as_str()).collect::<Vec<_>>(),
        ["core-a", "provider-only", "shared", "missing", "Nested"]
    );
    assert!(matches!(rows[0].member, NodeRowMemberV1::Node { .. }));
    assert_eq!(rows[0].freshness, LatencyFreshness::NotSampled);
    assert_eq!(rows[0].probe_status, ProbeStatus::NotSampled);
    assert!(matches!(
        rows[1].member,
        NodeRowMemberV1::Node { ref record_id, .. } if record_id == &provider_id
    ));
    assert_eq!(rows[1].delay_ms, Some(42));
    assert_eq!(rows[1].sampled_at_unix_ms, Some(100));
    assert_eq!(rows[1].freshness, LatencyFreshness::Fresh);
    assert_eq!(rows[1].probe_status, ProbeStatus::Succeeded);
    assert!(rows[1].selected);
    assert!(matches!(
        rows[2].member,
        NodeRowMemberV1::Unresolved {
            reason: UnresolvedMemberReason::Ambiguous,
            ref candidate_ids,
        } if candidate_ids.len() == 2
    ));
    assert!(matches!(
        rows[3].member,
        NodeRowMemberV1::Unresolved {
            reason: UnresolvedMemberReason::Missing,
            ref candidate_ids,
        } if candidate_ids.is_empty()
    ));
    assert_eq!(rows[4].member, NodeRowMemberV1::Group);
    assert_eq!(rows[4].proxy_type.as_deref(), Some("Selector"));
}

#[test]
fn node_rows_preserve_provider_unavailable_members() {
    let view = project_proxy_view(PROXIES, None, &group_order()).expect("proxies remain usable");
    let rows = view
        .node_rows("Manual", &BTreeMap::new())
        .expect("known group");

    assert!(matches!(
        rows[1].member,
        NodeRowMemberV1::Unresolved {
            reason: UnresolvedMemberReason::ProviderUnavailable,
            ref candidate_ids,
        } if candidate_ids.is_empty()
    ));
}

#[test]
fn codecs_match_v1_19_28_snapshot_and_stream_shapes() {
    assert_eq!(
        MihomoJsonCodec::version(VERSION).expect("version"),
        MihomoVersion {
            version: "v1.19.28".to_owned(),
            meta: true,
        }
    );
    assert_eq!(
        MihomoJsonCodec::delay(DELAY).expect("delay"),
        DelayProbeResult { delay_ms: 42 }
    );
    assert_eq!(
        MihomoJsonCodec::traffic(TRAFFIC).expect("traffic"),
        TrafficFrame {
            upload_bytes_per_second: 1024,
            download_bytes_per_second: 2048,
        }
    );
    assert_eq!(
        MihomoJsonCodec::connections(CONNECTIONS).expect("connections"),
        ConnectionSummary {
            active_connections: 2,
            upload_total_bytes: 4096,
            download_total_bytes: 8192,
            memory_bytes: Some(1_048_576),
        }
    );
    assert_eq!(
        MihomoJsonCodec::log(LOG).expect("log"),
        MihomoLogFrame {
            level: MihomoLogLevel::Info,
            message: "[TCP] fixture connection established".to_owned(),
        }
    );
    let error = project_proxy_view(PROXIES, Some(b"{}"), &group_order())
        .expect_err("providers map is required");
    assert_eq!(error.kind, ProjectionErrorKind::Providers);
}

#[test]
fn fixed_codecs_reject_unknown_fields_and_oversized_numbers() {
    assert_eq!(
        MihomoJsonCodec::traffic(br#"{"up":1,"down":2,"upTotal":3,"downTotal":4}"#)
            .expect("v1.19.28 traffic totals are accepted"),
        TrafficFrame {
            upload_bytes_per_second: 1,
            download_bytes_per_second: 2,
        }
    );

    for error in [
        MihomoJsonCodec::version(
            br#"{"meta":true,"premium":false,"version":"v1.19.28","secret-field":1}"#,
        )
        .expect_err("unknown version field"),
        MihomoJsonCodec::delay(br#"{"delay":42,"secret-field":1}"#)
            .expect_err("unknown delay field"),
        MihomoJsonCodec::traffic(br#"{"up":1,"down":2,"secret-field":1}"#)
            .expect_err("unknown traffic field"),
        MihomoJsonCodec::connections(
            br#"{"connections":[],"downloadTotal":1,"uploadTotal":2,"secret-field":1}"#,
        )
        .expect_err("unknown connections field"),
        MihomoJsonCodec::log(br#"{"type":"info","payload":"safe","secret-field":1}"#)
            .expect_err("unknown log field"),
    ] {
        assert!(!format!("{error:?}").contains("secret-field"));
        assert!(!error.to_string().contains("secret-field"));
    }

    assert!(MihomoJsonCodec::delay(br#"{"delay":65536}"#).is_err());
    assert!(MihomoJsonCodec::traffic(br#"{"up":18446744073709551616,"down":0}"#).is_err());
    assert!(
        MihomoJsonCodec::connections(
            br#"{"connections":[],"downloadTotal":18446744073709551616,"uploadTotal":0}"#
        )
        .is_err()
    );
}

#[test]
fn log_and_error_formatting_omit_untrusted_diagnostics() {
    let process_log = ForwardedCoreLog {
        sequence: 1,
        timestamp_unix_ms: 2,
        source: ratash::core::ProcessOutputSource::Stderr,
        message: "process-log-secret".to_owned(),
        instance_generation: CoreInstanceGeneration(3),
    };
    let mihomo_log = MihomoLogFrame {
        level: MihomoLogLevel::Error,
        message: "mihomo-log-secret".to_owned(),
    };
    assert!(!format!("{process_log:?}").contains("process-log-secret"));
    assert!(!format!("{mihomo_log:?}").contains("mihomo-log-secret"));

    let runtime_error =
        CoreRuntimeError::new(CoreRuntimeErrorKind::Apply, "runtime-adapter-secret");
    let mihomo_error = MihomoError::new(MihomoErrorKind::InvalidResponse, "mihomo-adapter-secret");
    let projection_error =
        MihomoJsonCodec::delay(b"projection-parser-secret").expect_err("invalid JSON response");
    for (debug, display, secret) in [
        (
            format!("{runtime_error:?}"),
            runtime_error.to_string(),
            "runtime-adapter-secret",
        ),
        (
            format!("{mihomo_error:?}"),
            mihomo_error.to_string(),
            "mihomo-adapter-secret",
        ),
        (
            format!("{projection_error:?}"),
            projection_error.to_string(),
            "projection-parser-secret",
        ),
    ] {
        assert!(!debug.contains(secret));
        assert!(!display.contains(secret));
    }
}

#[test]
fn core_runtime_port_requires_owner_proof_and_redacts_credentials() {
    let runtime = FakeCoreRuntime;
    assert_core_runtime_object_safe(&runtime);

    let request = OwnerSessionRequest {
        owner_uid: 501,
        supervisor_pid: 100,
        supervisor_start_identity: "start-identity".to_owned(),
        instance_token: "instance-secret".to_owned(),
        protocol_version: 1,
    };
    let session = runtime.open_owner_session(&request).expect("session");
    let bundle = RuntimeBundle {
        generation: RuntimeGeneration(3),
        generation_root: PathBuf::from("/fixture/generation-3"),
        manifest_sha256: "manifest".to_owned(),
        compiler_policy_sha256: "policy".to_owned(),
        mihomo_binary_sha256: "binary".to_owned(),
    };
    let applied = runtime
        .apply_candidate(&session.proof, &bundle)
        .expect("apply");

    assert_eq!(applied.disposition, ApplyDisposition::Spawned);
    assert_eq!(
        applied.managed_core.runtime_generation,
        RuntimeGeneration(3)
    );
    assert!(!format!("{request:?}").contains("instance-secret"));
    assert!(!format!("{:?}", session.proof).contains("session-secret"));
    assert!(!format!("{:?}", applied.managed_core.endpoint).contains("core-secret"));
}

#[test]
fn mihomo_port_and_stream_contracts_are_object_safe_and_generation_scoped() {
    let adapter = FakeMihomoAdapter;
    assert_mihomo_object_safe(&adapter);
    let endpoint = CoreControlEndpoint::new("/fixture/core.sock", "secret");
    let mut stream = adapter
        .open_traffic_stream(&endpoint, CoreInstanceGeneration(9))
        .expect("stream");
    let event = stream.next_event().expect("stream result").expect("event");

    assert_eq!(event.instance_generation, CoreInstanceGeneration(9));
    assert_eq!(event.payload.upload_bytes_per_second, 1024);
    stream.cancel();
    assert!(stream.next_event().expect("cancelled stream").is_none());
}

fn assert_core_runtime_object_safe(_: &dyn CoreRuntime) {}

fn assert_mihomo_object_safe(_: &dyn MihomoAdapter) {}

struct FakeCoreRuntime;

impl FakeCoreRuntime {
    fn handle(runtime_generation: RuntimeGeneration) -> ManagedCoreHandle {
        ManagedCoreHandle {
            pid: 200,
            process_start_identity: "core-start".to_owned(),
            endpoint: CoreControlEndpoint::new("/fixture/core.sock", "core-secret"),
            instance_generation: CoreInstanceGeneration(4),
            runtime_generation,
        }
    }
}

impl CoreRuntime for FakeCoreRuntime {
    fn open_owner_session(
        &self,
        request: &OwnerSessionRequest,
    ) -> Result<OwnerSession, CoreRuntimeError> {
        Ok(OwnerSession {
            proof: OwnerSessionProof::new(
                format!("session-{}", request.supervisor_pid),
                "session-secret",
            ),
            protocol_version: request.protocol_version,
            owner_generation: 1,
            endpoint: CoreControlEndpoint::new("/fixture/core.sock", "core-secret"),
        })
    }

    fn apply_candidate(
        &self,
        _owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, CoreRuntimeError> {
        Ok(ApplyCandidateResult {
            disposition: ApplyDisposition::Spawned,
            managed_core: Self::handle(bundle.generation),
        })
    }

    fn status(&self, _owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        Ok(CoreRuntimeStatus::from_managed_core(Some(Self::handle(
            RuntimeGeneration(3),
        ))))
    }

    fn logs(
        &self,
        _owner: &OwnerSessionProof,
        _after_sequence: Option<u64>,
        _limit: usize,
    ) -> Result<ForwardedCoreLogBatch, CoreRuntimeError> {
        Ok(ForwardedCoreLogBatch {
            records: Vec::new(),
            next_sequence: None,
            dropped_before: 0,
            dropped_since_after: 0,
        })
    }

    fn stop(&self, _owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError> {
        Ok(StopCoreResult {
            stopped: true,
            instance_generation: Some(CoreInstanceGeneration(4)),
        })
    }

    fn close_owner_session(&self, _owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        Ok(())
    }
}

struct FakeMihomoAdapter;

impl MihomoAdapter for FakeMihomoAdapter {
    fn version(&self, _endpoint: &CoreControlEndpoint) -> Result<MihomoVersion, MihomoError> {
        Ok(MihomoJsonCodec::version(VERSION).expect("fixture version"))
    }

    fn readiness(&self, _endpoint: &CoreControlEndpoint) -> Result<MihomoReadiness, MihomoError> {
        Ok(MihomoReadiness::Ready)
    }

    fn proxy_view(
        &self,
        _endpoint: &CoreControlEndpoint,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        Ok(
            project_proxy_view(PROXIES, Some(PROVIDERS), effective_group_order)
                .expect("fixture proxy view"),
        )
    }

    fn select_node(
        &self,
        _endpoint: &CoreControlEndpoint,
        _selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        Ok(())
    }

    fn probe_delay(
        &self,
        _endpoint: &CoreControlEndpoint,
        _request: &DelayProbeRequest,
    ) -> Result<DelayProbeResult, MihomoError> {
        Ok(DelayProbeResult { delay_ms: 42 })
    }

    fn connection_summary(
        &self,
        _endpoint: &CoreControlEndpoint,
    ) -> Result<ConnectionSummary, MihomoError> {
        Ok(MihomoJsonCodec::connections(CONNECTIONS).expect("fixture connections"))
    }

    fn open_traffic_stream(
        &self,
        _endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError> {
        Ok(Box::new(FakeStream::one(CoreEvent {
            instance_generation: generation,
            payload: MihomoJsonCodec::traffic(TRAFFIC).expect("fixture traffic"),
        })))
    }

    fn open_connection_stream(
        &self,
        _endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError> {
        Ok(Box::new(FakeStream::one(CoreEvent {
            instance_generation: generation,
            payload: MihomoJsonCodec::connections(CONNECTIONS).expect("fixture connections"),
        })))
    }

    fn open_log_stream(
        &self,
        _endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError> {
        Ok(Box::new(FakeStream::one(CoreEvent {
            instance_generation: generation,
            payload: MihomoJsonCodec::log(LOG).expect("fixture log"),
        })))
    }
}

struct FakeStream<T> {
    events: VecDeque<CoreEvent<T>>,
    cancelled: bool,
}

impl<T> FakeStream<T> {
    fn one(event: CoreEvent<T>) -> Self {
        Self {
            events: VecDeque::from([event]),
            cancelled: false,
        }
    }
}

impl<T: Send> CoreEventStream<T> for FakeStream<T> {
    fn next_event(&mut self) -> Result<Option<CoreEvent<T>>, MihomoError> {
        if self.cancelled {
            return Ok(None);
        }
        Ok(self.events.pop_front())
    }

    fn cancel(&mut self) {
        self.cancelled = true;
        self.events.clear();
    }
}

use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use hopash::config::{AuthoritativeConfig, ConfigCompiler};
use hopash::core::{
    ConnectionSummary, CoreControlEndpoint, CoreEventStream, CoreRuntimeError,
    CoreRuntimeErrorKind, DelayProbeRequest, DelayProbeResult, ManagedCoreHandle, MihomoAdapter,
    MihomoError, MihomoLogFrame, MihomoReadiness, MihomoVersion, NodeSelection, ProxyView,
    TrafficFrame,
};
use hopash::domain::{CoreInstanceGeneration, RuntimeGeneration};
use hopash::persistence::{ObjectId, TransactionBundle};
use hopash::profile::{ProfileSnapshot, SnapshotLimits};
use hopash::runtime_adapters::{
    MihomoRuntimeHealthProbe, StagedRuntimeBundleResolver, classify_runtime_apply_error,
};
use hopash::runtime_bundle::RuntimeBundleStager;
use hopash::transaction::{RuntimeApplyFailure, RuntimeBundleResolver, RuntimeHealthProbe};

struct FixtureMihomo {
    readiness: Mutex<VecDeque<Result<MihomoReadiness, MihomoError>>>,
    version: Mutex<VecDeque<Result<MihomoVersion, MihomoError>>>,
}

impl MihomoAdapter for FixtureMihomo {
    fn version(&self, _endpoint: &CoreControlEndpoint) -> Result<MihomoVersion, MihomoError> {
        self.version.lock().unwrap().pop_front().unwrap()
    }

    fn readiness(&self, _endpoint: &CoreControlEndpoint) -> Result<MihomoReadiness, MihomoError> {
        self.readiness.lock().unwrap().pop_front().unwrap()
    }

    fn proxy_view(
        &self,
        _endpoint: &CoreControlEndpoint,
        _effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        unreachable!()
    }

    fn select_node(
        &self,
        _endpoint: &CoreControlEndpoint,
        _selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        unreachable!()
    }

    fn probe_delay(
        &self,
        _endpoint: &CoreControlEndpoint,
        _request: &DelayProbeRequest,
    ) -> Result<DelayProbeResult, MihomoError> {
        unreachable!()
    }

    fn connection_summary(
        &self,
        _endpoint: &CoreControlEndpoint,
    ) -> Result<ConnectionSummary, MihomoError> {
        unreachable!()
    }

    fn open_traffic_stream(
        &self,
        _endpoint: &CoreControlEndpoint,
        _generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError> {
        unreachable!()
    }

    fn open_connection_stream(
        &self,
        _endpoint: &CoreControlEndpoint,
        _generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError> {
        unreachable!()
    }

    fn open_log_stream(
        &self,
        _endpoint: &CoreControlEndpoint,
        _generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError> {
        unreachable!()
    }
}

#[test]
fn health_probe_requires_ready_pinned_meta_core() {
    let mihomo = Arc::new(FixtureMihomo {
        readiness: Mutex::new(VecDeque::from([Ok(MihomoReadiness::Ready)])),
        version: Mutex::new(VecDeque::from([Ok(MihomoVersion {
            version: "v1.19.28".to_owned(),
            meta: true,
        })])),
    });
    let probe = MihomoRuntimeHealthProbe::bundled(mihomo);
    probe
        .confirm_ready(&managed_core())
        .expect("the pinned ready Core should pass health confirmation");

    let starting = Arc::new(FixtureMihomo {
        readiness: Mutex::new(VecDeque::from([Ok(MihomoReadiness::Starting)])),
        version: Mutex::new(VecDeque::new()),
    });
    assert!(
        MihomoRuntimeHealthProbe::bundled(starting)
            .confirm_ready(&managed_core())
            .is_err()
    );
}

#[test]
fn resolver_reconstructs_only_the_matching_staged_generation() {
    let root = temporary_root("resolver");
    let binary = root.join("fixture-mihomo");
    fs::write(&binary, b"fixture binary").unwrap();
    let mut permissions = fs::metadata(&binary).unwrap().permissions();
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o700);
    fs::set_permissions(&binary, permissions).unwrap();
    let binary_sha256 = sha256_hex(b"fixture binary");
    let compiler = ConfigCompiler::bundled().unwrap();
    let stage_root = root.join("runtime");
    let stager = RuntimeBundleStager::new(
        &stage_root,
        &binary,
        &binary_sha256,
        compiler.compiler_policy_sha256(),
    )
    .unwrap();
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let snapshot = ProfileSnapshot::parse(
        b"proxies: []\nproxy-groups: []\nrules: []\n",
        SnapshotLimits::new(1024, 16),
    )
    .unwrap();
    let configuration = compiler
        .compile(
            &snapshot,
            &[],
            &AuthoritativeConfig::new("/tmp/hopash-core.sock", "secret"),
            &workspace,
        )
        .unwrap();
    let staged = stager.stage(RuntimeGeneration(7), &configuration).unwrap();
    let resolver = StagedRuntimeBundleResolver::new(
        &stage_root,
        compiler.compiler_policy_sha256(),
        &binary_sha256,
    )
    .unwrap();
    let resolved = resolver
        .resolve(&transaction(RuntimeGeneration(7)))
        .unwrap();
    assert_eq!(resolved, staged);
    assert!(
        resolver
            .resolve(&transaction(RuntimeGeneration(8)))
            .is_err()
    );

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn apply_transport_uncertainty_is_classified_as_indeterminate() {
    for kind in [
        CoreRuntimeErrorKind::ReloadTimeout,
        CoreRuntimeErrorKind::Unavailable,
    ] {
        assert_eq!(
            classify_runtime_apply_error(&CoreRuntimeError::new(kind, "fixture")),
            RuntimeApplyFailure::Indeterminate
        );
    }
    assert_eq!(
        classify_runtime_apply_error(&CoreRuntimeError::new(
            CoreRuntimeErrorKind::TunPermissionDenied,
            "fixture"
        )),
        RuntimeApplyFailure::TunPermissionDenied
    );
}

fn managed_core() -> ManagedCoreHandle {
    ManagedCoreHandle {
        pid: 42,
        process_start_identity: "fixture".to_owned(),
        endpoint: CoreControlEndpoint::new("/tmp/hopash-core.sock", "secret"),
        instance_generation: CoreInstanceGeneration(3),
        runtime_generation: RuntimeGeneration(7),
    }
}

fn transaction(generation: RuntimeGeneration) -> TransactionBundle {
    let id = ObjectId::parse(&sha256_hex(b"fixture")).unwrap();
    TransactionBundle {
        supervisor_state: id.clone(),
        profile_snapshot: id.clone(),
        local_rule_set: id.clone(),
        effective_configuration: id,
        profile_revision: hopash::profile::ProfileRevision(1),
        local_rule_set_revision: hopash::domain::LocalRuleSetRevision(1),
        active_profile_id: hopash::domain::ProfileId::new(),
        runtime_generation: generation,
    }
}

fn temporary_root(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "hopash-runtime-adapter-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&path).unwrap();
    path
}

fn sha256_hex(content: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(content);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

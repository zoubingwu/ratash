use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hopash::config::{AuthoritativeConfig, ConfigCompiler, EffectiveConfiguration};
use hopash::core::{
    ApplyDisposition, CoreControlEndpoint, CoreRuntime, MihomoReadiness, OwnerSessionRequest,
};
use hopash::domain::RuntimeGeneration;
use hopash::lifecycle::{ProcessInspector, PsProcessInspector};
use hopash::process_controller::{
    CoreControlClient, NativeCoreProcessConfig, NativeCoreProcessController,
    SystemProcessIdentityProbe,
};
use hopash::profile::{ProfileSnapshot, SnapshotLimits};
use hopash::runtime_bundle::RuntimeBundleStager;
use hopash::service::{
    CORE_RUNTIME_PROTOCOL_VERSION, CallerCredentialValidator, PrivilegedCoreRuntimeService,
    PrivilegedServiceConfig, PrivilegedServiceDependencies, ServicePlatformError,
    TunCapabilityPreflight, UuidSecretGenerator,
};
use sha2::{Digest, Sha256};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "hopash-process-controller-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&path).expect("fixture directory should be created");
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct FakeControl {
    reloads: Mutex<Vec<PathBuf>>,
}

impl CoreControlClient for FakeControl {
    fn readiness(
        &self,
        _endpoint: &CoreControlEndpoint,
    ) -> Result<MihomoReadiness, ServicePlatformError> {
        Ok(MihomoReadiness::Ready)
    }

    fn reload(
        &self,
        _endpoint: &CoreControlEndpoint,
        configuration_path: &Path,
    ) -> Result<(), ServicePlatformError> {
        self.reloads
            .lock()
            .expect("reload fixture lock should remain available")
            .push(configuration_path.to_path_buf());
        Ok(())
    }
}

struct AllowCaller;

impl CallerCredentialValidator for AllowCaller {
    fn validate(&self, _request: &OwnerSessionRequest) -> Result<(), ServicePlatformError> {
        Ok(())
    }
}

struct AllowTun;

impl TunCapabilityPreflight for AllowTun {
    fn check(&self, _owner_uid: u32) -> Result<(), ServicePlatformError> {
        Ok(())
    }
}

#[test]
fn verified_runtime_spawns_reloads_forwards_bounded_logs_and_stops() {
    let directory = TestDirectory::new("lifecycle");
    let executable = directory.0.join("fixture-core");
    fs::write(
        &executable,
        b"#!/bin/sh\nprintf 'core stdout\\n'\nprintf 'core stderr\\n' >&2\nprintf '0123456789abcdef\\n'\nexec /bin/sleep 30\n",
    )
    .expect("fixture executable should be written");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
        .expect("fixture executable should be executable");

    let effective = effective_configuration(&directory.0);
    let binary_sha256 = sha256(&fs::read(&executable).expect("fixture binary should be readable"));
    let runtime_root = directory.0.join("service-runtime");
    let stager = RuntimeBundleStager::new(
        &runtime_root,
        &executable,
        &binary_sha256,
        effective.compiler_policy_sha256(),
    )
    .expect("runtime stager should be configured");
    let first_bundle = stager
        .stage(RuntimeGeneration(1), &effective)
        .expect("first Runtime Generation should be staged");
    let second_bundle = stager
        .stage(RuntimeGeneration(2), &effective)
        .expect("second Runtime Generation should be staged");

    let control = Arc::new(FakeControl::default());
    let process_controller = NativeCoreProcessController::new(
        NativeCoreProcessConfig {
            readiness_timeout: Duration::from_secs(1),
            readiness_poll_interval: Duration::from_millis(5),
            stop_timeout: Duration::from_secs(1),
            log_capacity: 3,
            max_log_line_bytes: 8,
        },
        control.clone(),
        Arc::new(PsProcessInspector),
    )
    .expect("process controller should be configured");
    let service = PrivilegedCoreRuntimeService::new(
        PrivilegedServiceConfig::product_defaults(
            runtime_root,
            effective.compiler_policy_sha256().to_owned(),
            binary_sha256,
        ),
        PrivilegedServiceDependencies {
            credentials: Box::new(AllowCaller),
            identities: Box::new(SystemProcessIdentityProbe),
            tun: Box::new(AllowTun),
            secrets: Box::new(UuidSecretGenerator),
            processes: Box::new(process_controller),
        },
    )
    .expect("privileged runtime service should start");
    let start_identity = PsProcessInspector
        .identity(std::process::id())
        .expect("Supervisor identity lookup should succeed")
        .expect("Supervisor identity should exist");
    let session = service
        .open_owner_session(&OwnerSessionRequest {
            owner_uid: nix::unistd::Uid::current().as_raw(),
            supervisor_pid: std::process::id(),
            supervisor_start_identity: start_identity,
            instance_token: "fixture-instance".to_owned(),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        })
        .expect("owner session should open");

    let first = service
        .apply_candidate(&session.proof, &first_bundle)
        .expect("first Runtime Apply should spawn the fixture Core");
    assert_eq!(first.disposition, ApplyDisposition::Spawned);
    assert_eq!(first.managed_core.runtime_generation, RuntimeGeneration(1));

    let logs = wait_for_logs(&service, &session.proof, 3);
    assert!(
        logs.records
            .iter()
            .any(|record| record.message == "core std")
    );
    assert!(
        logs.records
            .iter()
            .any(|record| record.message == "01234567")
    );
    assert!(logs.records.len() <= 3);

    let second = service
        .apply_candidate(&session.proof, &second_bundle)
        .expect("second Runtime Apply should reload the fixture Core");
    assert_eq!(second.disposition, ApplyDisposition::Reloaded);
    assert_eq!(second.managed_core.pid, first.managed_core.pid);
    assert_eq!(second.managed_core.runtime_generation, RuntimeGeneration(2));
    let reloads = control
        .reloads
        .lock()
        .expect("reload fixture lock should remain available");
    assert_eq!(
        reloads.as_slice(),
        &[second_bundle.generation_root.join("config.yaml")]
    );
    drop(reloads);

    let stopped = service
        .stop(&session.proof)
        .expect("owned fixture Core should stop");
    assert!(stopped.stopped);
    assert_eq!(
        stopped.instance_generation,
        Some(first.managed_core.instance_generation)
    );
    service
        .close_owner_session(&session.proof)
        .expect("owner session should close");
}

#[test]
fn controller_configuration_rejects_zero_deadlines_and_capacities() {
    let valid = NativeCoreProcessConfig::default();
    let controls: Arc<dyn CoreControlClient> = Arc::new(FakeControl::default());
    let inspector: Arc<dyn ProcessInspector> = Arc::new(PsProcessInspector);

    for invalid in [
        NativeCoreProcessConfig {
            readiness_timeout: Duration::ZERO,
            ..valid
        },
        NativeCoreProcessConfig {
            readiness_poll_interval: Duration::ZERO,
            ..valid
        },
        NativeCoreProcessConfig {
            stop_timeout: Duration::ZERO,
            ..valid
        },
        NativeCoreProcessConfig {
            log_capacity: 0,
            ..valid
        },
        NativeCoreProcessConfig {
            max_log_line_bytes: 0,
            ..valid
        },
    ] {
        assert!(
            NativeCoreProcessController::new(invalid, controls.clone(), inspector.clone()).is_err()
        );
    }
}

fn wait_for_logs(
    service: &PrivilegedCoreRuntimeService,
    proof: &hopash::core::OwnerSessionProof,
    expected: usize,
) -> hopash::core::ForwardedCoreLogBatch {
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    loop {
        let batch = service
            .logs(proof, None, expected)
            .expect("fixture Core logs should be readable");
        if batch.records.len() >= expected || std::time::Instant::now() >= deadline {
            return batch;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn effective_configuration(root: &Path) -> EffectiveConfiguration {
    let profile_root = root.join("profile");
    fs::create_dir_all(&profile_root).expect("profile fixture root should be created");
    let snapshot = ProfileSnapshot::parse(
        br#"
proxy-groups:
  - name: Main
    type: select
    proxies: [DIRECT]
rules:
  - MATCH,DIRECT
"#,
        SnapshotLimits::new(128 * 1_024, 32),
    )
    .expect("Profile fixture should parse");
    ConfigCompiler::bundled()
        .expect("bundled compiler should load")
        .compile(
            &snapshot,
            &[],
            &AuthoritativeConfig::new(root.join("core.sock").to_string_lossy(), "fixture-secret"),
            &profile_root,
        )
        .expect("Profile fixture should compile")
}

fn sha256(content: &[u8]) -> String {
    Sha256::digest(content)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

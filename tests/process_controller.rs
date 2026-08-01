use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hopash::config::{AuthoritativeConfig, ConfigCompiler, EffectiveConfiguration};
use hopash::constants::CORE_RESTART_INITIAL_BACKOFF;
use hopash::core::{
    ApplyDisposition, CoreControlEndpoint, CoreRuntime, CoreRuntimeLifecycle, MihomoReadiness,
    OwnerSessionRequest,
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
    CORE_RUNTIME_PROTOCOL_VERSION, CallerCredentialValidator, CoreProcessController,
    PrivilegedCoreRuntimeService, PrivilegedServiceConfig, PrivilegedServiceDependencies,
    ServiceMaintenanceOutcome, ServicePlatformError, TunCapabilityPreflight, UnexpectedExitOutcome,
    UuidSecretGenerator,
};
use sha2::{Digest, Sha256};

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let path = Path::new("/private/tmp").join(format!(
            "hpc-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
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
    fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o711))
        .expect("the service runtime root should permit owner traversal");

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
    let endpoint_listener = UnixListener::bind(&session.endpoint.socket_path)
        .expect("fixture Core control endpoint should bind");

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

    let kill_status = Command::new("/bin/kill")
        .args(["-KILL", &second.managed_core.pid.to_string()])
        .status()
        .expect("fixture Core kill command should run");
    assert!(kill_status.success());
    let deadline = Instant::now() + Duration::from_secs(2);
    let exited_status = loop {
        match service.status(&session.proof) {
            Ok(status) if status.lifecycle == CoreRuntimeLifecycle::RestartPending => break status,
            Ok(_) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("status should observe the fixture child exit: {error:?}"),
            Ok(_) => panic!("status should observe the fixture child exit before the deadline"),
        }
    };
    assert!(exited_status.managed_core.is_none());
    assert!(exited_status.restart.pending);
    assert_eq!(exited_status.restart.attempts, 0);
    assert_eq!(
        exited_status.restart.backoff,
        Some(CORE_RESTART_INITIAL_BACKOFF)
    );
    let scheduled = service
        .maintenance_step(Duration::ZERO)
        .expect("maintenance should inspect the exit already observed by status");
    assert_eq!(
        scheduled.outcome,
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Pending {
            attempts: 0,
            next_attempt_at: CORE_RESTART_INITIAL_BACKOFF,
        })
    );
    drop(endpoint_listener);
    let endpoint_listener = UnixListener::bind(&session.endpoint.socket_path)
        .expect("the restarted fixture Core control endpoint should bind");
    let restarted = match service
        .maintenance_step(CORE_RESTART_INITIAL_BACKOFF)
        .expect("due maintenance should restart the fixture child")
        .outcome
    {
        ServiceMaintenanceOutcome::UnexpectedExit(UnexpectedExitOutcome::Restarted {
            managed_core,
            ..
        }) => managed_core,
        outcome => panic!("fixture Core should restart after its child exit: {outcome:?}"),
    };
    assert_eq!(restarted.runtime_generation, RuntimeGeneration(2));
    assert_eq!(
        restarted.instance_generation,
        hopash::domain::CoreInstanceGeneration(first.managed_core.instance_generation.0 + 1)
    );

    let stopped = service
        .stop(&session.proof)
        .expect("restarted fixture Core should stop");
    assert!(stopped.stopped);
    assert_eq!(
        stopped.instance_generation,
        Some(restarted.instance_generation)
    );
    drop(endpoint_listener);
    service
        .close_owner_session(&session.proof)
        .expect("owner session should close");
}

#[test]
fn guarded_controller_stop_and_drop_contain_the_exact_core() {
    let directory = TestDirectory::new("guardian");
    let executable = directory.0.join("fixture-core");
    let script = b"#!/bin/sh\ntrap 'printf guardian-final-stdout; printf guardian-final-stderr >&2; exit 0' TERM\nprintf 'guardian stdout\\n'\nprintf 'guardian stderr\\n' >&2\nwhile :; do /bin/sleep 0.05; done\n";
    fs::write(&executable, script).expect("fixture executable should be written");
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
        .expect("fixture executable should be executable");
    let effective = effective_configuration(&directory.0);
    let binary_sha256 = sha256(script);
    let runtime_root = directory.0.join("guardian-runtime");
    let bundle = RuntimeBundleStager::new(
        &runtime_root,
        &executable,
        &binary_sha256,
        effective.compiler_policy_sha256(),
    )
    .expect("runtime stager should be configured")
    .stage(RuntimeGeneration(1), &effective)
    .expect("the Runtime Generation should be staged");
    fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o711))
        .expect("the service runtime root should permit owner traversal");
    let process_controller = NativeCoreProcessController::new_guarded(
        NativeCoreProcessConfig {
            readiness_timeout: Duration::from_secs(2),
            readiness_poll_interval: Duration::from_millis(5),
            stop_timeout: Duration::from_secs(1),
            log_capacity: 8,
            max_log_line_bytes: 64,
        },
        Arc::new(FakeControl::default()),
        Arc::new(PsProcessInspector),
        PathBuf::from(env!("CARGO_BIN_EXE_hopash")),
    )
    .expect("guarded process controller should be configured");
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
    let supervisor_identity = PsProcessInspector
        .identity(std::process::id())
        .expect("Supervisor identity lookup should succeed")
        .expect("Supervisor identity should exist");
    let session = service
        .open_owner_session(&OwnerSessionRequest {
            owner_uid: nix::unistd::Uid::current().as_raw(),
            supervisor_pid: std::process::id(),
            supervisor_start_identity: supervisor_identity,
            instance_token: "guardian-fixture-instance".to_owned(),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        })
        .expect("owner session should open");
    let endpoint_listener = UnixListener::bind(&session.endpoint.socket_path)
        .expect("fixture Core control endpoint should bind");
    let mut unrelated = Command::new("/bin/sleep")
        .arg("30")
        .spawn()
        .expect("the unrelated fixture should start");

    let first = service
        .apply_candidate(&session.proof, &bundle)
        .expect("the guarded Core should start");
    let first_identity = first.managed_core.process_start_identity.clone();
    let logs = wait_for_logs(&service, &session.proof, 2);
    assert!(
        logs.records
            .iter()
            .any(|record| record.message == "guardian stdout")
    );
    assert!(
        logs.records
            .iter()
            .any(|record| record.message == "guardian stderr")
    );
    service
        .stop(&session.proof)
        .expect("normal service stop should close the guardian control pipe");
    wait_for_identity_gone(first.managed_core.pid, &first_identity);
    let stopped_logs = service
        .logs(&session.proof, None, usize::MAX)
        .expect("stopped Core logs should remain available");
    assert!(
        stopped_logs
            .records
            .iter()
            .any(|record| record.message == "guardian-final-stdout")
    );
    assert!(
        stopped_logs
            .records
            .iter()
            .any(|record| record.message == "guardian-final-stderr")
    );
    assert!(
        unrelated
            .try_wait()
            .expect("the unrelated fixture should be inspectable")
            .is_none()
    );

    drop(endpoint_listener);
    let endpoint_listener = UnixListener::bind(&session.endpoint.socket_path)
        .expect("the restarted fixture Core control endpoint should bind");
    let second = service
        .apply_candidate(&session.proof, &bundle)
        .expect("the guarded Core should restart");
    let second_identity = second.managed_core.process_start_identity.clone();
    drop(service);
    wait_for_identity_gone(second.managed_core.pid, &second_identity);
    assert!(
        unrelated
            .try_wait()
            .expect("the unrelated fixture should be inspectable")
            .is_none()
    );

    unrelated.kill().expect("the unrelated fixture should stop");
    unrelated
        .wait()
        .expect("the unrelated fixture should be reaped");
    drop(endpoint_listener);
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

#[test]
fn core_control_endpoint_is_private_to_the_owner_and_service() {
    let directory = TestDirectory::new("endpoint");
    let service_root = directory.0.join("r");
    let control_root = service_root.join("c");
    let generation_root = service_root.join("g1");
    fs::create_dir_all(&control_root).expect("control root should be created");
    fs::create_dir(&generation_root).expect("generation root should be created");
    fs::set_permissions(&service_root, fs::Permissions::from_mode(0o711))
        .expect("service root access should be configured");
    fs::set_permissions(&control_root, fs::Permissions::from_mode(0o711))
        .expect("control root access should be configured");
    fs::set_permissions(&generation_root, fs::Permissions::from_mode(0o700))
        .expect("generation root should stay private");
    let endpoint = CoreControlEndpoint::new(control_root.join("s"), "secret");
    let listener = UnixListener::bind(&endpoint.socket_path)
        .expect("fixture Core control endpoint should bind");
    fs::set_permissions(&endpoint.socket_path, fs::Permissions::from_mode(0o777))
        .expect("fixture endpoint permissions should be widened before hardening");
    let controller = NativeCoreProcessController::new(
        NativeCoreProcessConfig::default(),
        Arc::new(FakeControl::default()),
        Arc::new(PsProcessInspector),
    )
    .expect("process controller should be configured");

    controller
        .grant_endpoint_access(&endpoint, nix::unistd::Uid::effective().as_raw())
        .expect("endpoint access should be restricted to the owner and service");

    let endpoint_metadata =
        fs::symlink_metadata(&endpoint.socket_path).expect("endpoint metadata should load");
    let generation_metadata =
        fs::symlink_metadata(&generation_root).expect("generation metadata should load");
    assert!(endpoint_metadata.file_type().is_socket());
    assert_eq!(
        endpoint_metadata.uid(),
        nix::unistd::Uid::effective().as_raw()
    );
    assert_eq!(endpoint_metadata.mode() & 0o777, 0o600);
    assert_eq!(generation_metadata.mode() & 0o777, 0o700);
    drop(listener);
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

fn wait_for_identity_gone(pid: u32, identity: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let current = PsProcessInspector
            .identity(pid)
            .expect("fixture process identity should be inspectable");
        if current.as_deref() != Some(identity) {
            return;
        }
        assert!(Instant::now() < deadline, "the guarded Core should exit");
        std::thread::sleep(Duration::from_millis(10));
    }
}

//! Production process composition for the public CLI and hidden service modes.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fmt;
use std::fs;
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
#[cfg(test)]
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[cfg(target_os = "macos")]
use core_foundation::base::TCFType;
#[cfg(target_os = "macos")]
use core_foundation::data::CFData;
#[cfg(target_os = "macos")]
use core_foundation::url::CFURL;
#[cfg(target_os = "macos")]
use security_framework::os::macos::code_signing::{
    Flags as CodeSigningFlags, GuestAttributes, SecCode, SecRequirement, SecStaticCode,
};

#[cfg(test)]
use crate::application::ApplicationService;
use crate::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput, Clock,
    SystemClock,
};
use crate::background::{BackgroundApplication, BackgroundRuntime, MihomoBackgroundCorePort};
use crate::config::{
    AuthoritativeConfig, BUNDLED_CORE_VERSION, ConfigCompiler, CoreConfigValidator,
};
use crate::constants::{
    CORE_SERVICE_LIVENESS_INTERVAL, DAEMON_SHUTDOWN_TIMEOUT, IPC_REQUEST_TIMEOUT,
    MIHOMO_BINARY_MAX_BYTES, STATUS_SAMPLE_INTERVAL,
};
use crate::core::{
    CoreRuntime, MihomoAdapter, OwnerSessionProof, OwnerSessionRequest, ProcessOutputSource,
};
use crate::core_service_ipc::{
    CoreServiceClient, CoreServicePeerAuthorizer, CoreServicePeerIdentity, CoreServiceServer,
    CoreServiceServerConfig,
};
#[cfg(test)]
use crate::daemon::ShutdownPort;
use crate::daemon::{
    DaemonError, InternalSupervisorInvocation, ReadinessFailure, ShutdownAcknowledgement,
    ShutdownIntent, StartupFailureCategory, StartupStage, SupervisorOwnership,
};
use crate::domain::{CoreLifecycle, LocalRuleSetRevision, SupervisorLifecycle};
use crate::error::ErrorCode;
use crate::geodata::GeoDataCatalog;
#[cfg(test)]
use crate::ipc_runtime::IpcClient;
use crate::ipc_runtime::{IpcServer, IpcServerConfig, IpcStreamBroker, SameUserPeerAuthorizer};
use crate::lifecycle::{
    ManagedCoreIdentityRecord, ProcessIdentity, PsProcessInspector, StatePaths,
    current_process_identity,
};
use crate::mihomo::{MihomoAdapterConfig, UnixMihomoAdapter};
use crate::persistence::PersistenceStore;
use crate::process_controller::{
    MacOsTunCapabilityPreflight, NativeCoreProcessController, SystemProcessIdentityProbe,
};
use crate::profile::{ActiveProfileRevision, ProfileRevision};
use crate::profile_source::{ProfileSourcePolicy, ReqwestProfileSource};
use crate::runtime_adapters::{
    MihomoRuntimeHealthProbe, StagedRuntimeBundleResolver, classify_runtime_apply_error,
};
use crate::runtime_bundle::RuntimeBundleStager;
use crate::service::{
    CORE_RUNTIME_PROTOCOL_VERSION, CallerCredentialValidator, PrivilegedCoreRuntimeService,
    PrivilegedServiceConfig, PrivilegedServiceDependencies, RuntimeConfigurationPolicy,
    RuntimeManifestFileV1, ServicePlatformError, ServicePlatformErrorKind, UuidSecretGenerator,
};
#[cfg(test)]
use crate::shutdown_ipc::request_shutdown as request_shutdown_over_control;
use crate::shutdown_ipc::{ShutdownControlError, ShutdownControlHandler, ShutdownIpcServer};
use crate::state::AuthoritativeStateStore;
use crate::supervisor::{
    BlockingProfileFetchPort, CoordinatedSupervisorTransactions, DirectSupervisorCorePort,
    Supervisor, SupervisorDependencies, SupervisorRevisionAuthority, SupervisorTransactionPort,
};
use crate::telemetry::{LogLevel, LogSource};
use crate::transaction::{
    CandidateRevisionSource, CandidateRevisions, ConfigTransactionCoordinator,
    ConfigTransactionDependencies, CoreRuntimeApplyAdapter, RuntimeApplyPort,
    RuntimeBundleResolver, TransactionStore,
};
use crate::tui_runtime::{
    MonotonicClock, ProcessSignalSource, RuntimeClock, RuntimeWaiter, RuntimeWaker,
    ShutdownSignal as ProcessShutdownSignal,
};
use crate::validator::MihomoCommandValidator;

mod foreground;

pub use foreground::{IpcForegroundRunner, ProductionApplicationClient, run_public_invocation};

#[cfg(test)]
use foreground::ShutdownControlPort;
use foreground::expect_status;

pub const INTERNAL_CORE_SERVICE_MODE: &str = "__core-service";
pub const CORE_SERVICE_SOCKET_PATH: &str = "/var/run/hopash-rs/core-service.sock";
pub const BUNDLED_MIHOMO_PATH: &str = "/Library/Application Support/Hopash RS/bin/mihomo";
pub const BUNDLED_GEODATA_PATH: &str = "/Library/Application Support/Hopash RS/share/geodata";

const INSTALLED_HOPASH_PATH: &str = "/usr/local/bin/hopash";
pub const HOPASH_CODE_IDENTIFIER: &str = "hopash";

#[cfg(debug_assertions)]
const DEBUG_CORE_SERVICE_SOCKET_ENV: &str = "HOPASH_CORE_SERVICE_SOCKET";

const OBSERVER_LOG_BATCH: usize = 256;

// -----------------------------------------------------------------------------
// Hidden privileged service mode
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreServiceInvocation {
    pub owner_uid: u32,
    pub socket_path: PathBuf,
    pub runtime_root: PathBuf,
    pub mihomo_binary: PathBuf,
}

impl CoreServiceInvocation {
    pub fn parse_process_arguments(args: &[OsString]) -> io::Result<Option<Self>> {
        if args.get(1).and_then(|value| value.to_str()) != Some(INTERNAL_CORE_SERVICE_MODE) {
            return Ok(None);
        }
        if args.len() != 10
            || args.get(2).and_then(|value| value.to_str()) != Some("--owner-uid")
            || args.get(4).and_then(|value| value.to_str()) != Some("--socket")
            || args.get(6).and_then(|value| value.to_str()) != Some("--runtime-root")
            || args.get(8).and_then(|value| value.to_str()) != Some("--mihomo")
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the internal Core service invocation is invalid",
            ));
        }
        let owner_uid = args[3]
            .to_str()
            .and_then(|value| value.parse::<u32>().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the internal Core service owner is invalid",
                )
            })?;
        let invocation = Self {
            owner_uid,
            socket_path: PathBuf::from(&args[5]),
            runtime_root: PathBuf::from(&args[7]),
            mihomo_binary: PathBuf::from(&args[9]),
        };
        if !invocation.socket_path.is_absolute()
            || !invocation.runtime_root.is_absolute()
            || !invocation.mihomo_binary.is_absolute()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the internal Core service paths must be absolute",
            ));
        }
        Ok(Some(invocation))
    }
}

#[derive(Clone, Debug)]
struct InstalledHopashPeerAuthorizer {
    executable: PathBuf,
    identity: InstalledFileIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InstalledFileIdentity {
    device: u64,
    inode: u64,
}

struct BundledConfigurationPolicy {
    compiler: ConfigCompiler,
}

impl RuntimeConfigurationPolicy for BundledConfigurationPolicy {
    fn validate(
        &self,
        configuration: &[u8],
        endpoint: &crate::core::CoreControlEndpoint,
        provider_files: &[RuntimeManifestFileV1],
    ) -> Result<(), ServicePlatformError> {
        let controller_unix = endpoint.socket_path.to_str().ok_or_else(|| {
            ServicePlatformError::new(ServicePlatformErrorKind::ConfigurationPolicy)
        })?;
        let authoritative = AuthoritativeConfig::new(controller_unix, endpoint.secret());
        let provider_files = provider_files
            .iter()
            .map(|file| file.path.clone())
            .collect::<BTreeSet<_>>();
        self.compiler
            .validate_privileged_candidate(configuration, &authoritative, &provider_files)
            .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::ConfigurationPolicy))
    }
}

impl InstalledHopashPeerAuthorizer {
    #[cfg(target_os = "macos")]
    fn new() -> io::Result<Self> {
        let expected = PathBuf::from(INSTALLED_HOPASH_PATH);
        let metadata = fs::symlink_metadata(&expected).map_err(peer_signature_error)?;
        if !metadata.file_type().is_file()
            || metadata.uid() != 0
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the installed Hopash executable has unsafe ownership or permissions",
            ));
        }
        let executable = fs::canonicalize(&expected).map_err(peer_signature_error)?;
        if executable != expected {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the installed Hopash executable path is not canonical",
            ));
        }
        #[cfg(feature = "local-unsigned")]
        validate_local_install_ancestors(&executable)?;

        let requirement = installed_code_requirement()?;
        let url = CFURL::from_path(&executable, false).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "the installed Hopash executable path is invalid",
            )
        })?;
        let code =
            SecStaticCode::from_path(&url, CodeSigningFlags::NONE).map_err(peer_signature_error)?;
        code.check_validity(static_code_validation_flags(), &requirement)
            .map_err(peer_signature_error)?;

        Ok(Self {
            executable,
            identity: InstalledFileIdentity {
                device: metadata.dev(),
                inode: metadata.ino(),
            },
        })
    }

    #[cfg(not(target_os = "macos"))]
    fn new() -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the privileged Core service is supported only on macOS",
        ))
    }
}

impl CoreServicePeerAuthorizer for InstalledHopashPeerAuthorizer {
    #[cfg(target_os = "macos")]
    fn authorize(&self, peer: &CoreServicePeerIdentity) -> io::Result<()> {
        let token = peer.audit_token().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the Core service peer has no audit token",
            )
        })?;
        let pid = i32::try_from(peer.pid()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the Core service peer PID is invalid",
            )
        })?;
        let token_bytes = token
            .iter()
            .flat_map(|word| word.to_ne_bytes())
            .collect::<Vec<_>>();
        let token_data = CFData::from_buffer(&token_bytes);
        let mut attributes = GuestAttributes::new();
        attributes.set_pid(pid);
        attributes.set_audit_token(token_data.as_concrete_TypeRef());

        let requirement = installed_code_requirement()?;
        let code = SecCode::copy_guest_with_attribues(None, &attributes, CodeSigningFlags::NONE)
            .map_err(peer_signature_error)?;
        code.check_validity(dynamic_code_validation_flags(), &requirement)
            .map_err(peer_signature_error)?;
        let executable = code
            .path(CodeSigningFlags::NONE)
            .map_err(peer_signature_error)?
            .to_path()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "the Core service peer executable path is unavailable",
                )
            })?;
        if executable != self.executable {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the Core service peer executable path is unauthorized",
            ));
        }
        let metadata = fs::symlink_metadata(&self.executable).map_err(peer_signature_error)?;
        if !metadata.file_type().is_file()
            || metadata.uid() != 0
            || metadata.permissions().mode() & 0o022 != 0
            || metadata.dev() != self.identity.device
            || metadata.ino() != self.identity.inode
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the installed Hopash executable changed after service startup",
            ));
        }
        Ok(())
    }

    #[cfg(not(target_os = "macos"))]
    fn authorize(&self, _peer: &CoreServicePeerIdentity) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the privileged Core service is supported only on macOS",
        ))
    }
}

#[cfg(all(target_os = "macos", feature = "local-unsigned"))]
fn validate_local_install_ancestors(executable: &Path) -> io::Result<()> {
    for ancestor in executable.ancestors().skip(1) {
        let metadata = fs::symlink_metadata(ancestor).map_err(peer_signature_error)?;
        if !metadata.is_dir() || metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the installed Hopash path has unsafe ancestor permissions",
            ));
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn dynamic_code_validation_flags() -> CodeSigningFlags {
    let flags = CodeSigningFlags::STRICT_VALIDATE | CodeSigningFlags::NO_NETWORK_ACCESS;
    #[cfg(feature = "local-unsigned")]
    {
        flags
    }
    #[cfg(not(feature = "local-unsigned"))]
    {
        flags | CodeSigningFlags::CHECK_TRUSTED_ANCHORS
    }
}

#[cfg(target_os = "macos")]
fn static_code_validation_flags() -> CodeSigningFlags {
    dynamic_code_validation_flags() | CodeSigningFlags::CHECK_ALL_ARCHITECTURES
}

#[cfg(all(target_os = "macos", feature = "local-unsigned"))]
fn installed_code_requirement() -> io::Result<SecRequirement> {
    format!("identifier \"{HOPASH_CODE_IDENTIFIER}\"")
        .parse()
        .map_err(peer_signature_error)
}

#[cfg(all(target_os = "macos", not(feature = "local-unsigned")))]
fn installed_code_requirement() -> io::Result<SecRequirement> {
    format!(
        "anchor apple generic and certificate leaf[field.1.2.840.113635.100.6.1.13] exists and certificate 1[field.1.2.840.113635.100.6.2.6] exists and identifier \"{HOPASH_CODE_IDENTIFIER}\""
    )
        .parse()
        .map_err(peer_signature_error)
}

#[cfg(target_os = "macos")]
fn peer_signature_error(error: impl fmt::Debug) -> io::Error {
    let _ = error;
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "the installed Hopash signature is invalid",
    )
}

struct ExactCallerCredentials {
    owner_uid: u32,
}

impl CallerCredentialValidator for ExactCallerCredentials {
    fn validate(&self, request: &OwnerSessionRequest) -> Result<(), ServicePlatformError> {
        if request.owner_uid != self.owner_uid
            || request.supervisor_pid == 0
            || request.supervisor_start_identity.is_empty()
            || request.instance_token.is_empty()
            || request.protocol_version != CORE_RUNTIME_PROTOCOL_VERSION
        {
            return Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ));
        }
        let actual = crate::lifecycle::ProcessInspector::identity(
            &PsProcessInspector,
            request.supervisor_pid,
        )
        .map_err(|_| ServicePlatformError::new(ServicePlatformErrorKind::Credential))?;
        if actual.as_deref() == Some(request.supervisor_start_identity.as_str()) {
            Ok(())
        } else {
            Err(ServicePlatformError::new(
                ServicePlatformErrorKind::Credential,
            ))
        }
    }
}

pub fn run_core_service(invocation: CoreServiceInvocation) -> io::Result<()> {
    if !nix::unistd::Uid::effective().is_root() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "the privileged Core service requires root privileges",
        ));
    }
    let peer_authorizer = Arc::new(InstalledHopashPeerAuthorizer::new()?);
    let compiler = ConfigCompiler::bundled().map_err(invalid_product_configuration)?;
    let geodata = GeoDataCatalog::bundled().map_err(invalid_product_configuration)?;
    let compiler_policy_sha256 = compiler.compiler_policy_sha256().to_owned();
    let mihomo_binary_sha256 = verified_binary_sha256(&invocation.mihomo_binary)?;
    let runtime = Arc::new(
        PrivilegedCoreRuntimeService::new(
            PrivilegedServiceConfig::product_defaults(
                invocation.runtime_root.clone(),
                compiler_policy_sha256,
                mihomo_binary_sha256,
            ),
            PrivilegedServiceDependencies {
                credentials: Box::new(ExactCallerCredentials {
                    owner_uid: invocation.owner_uid,
                }),
                identities: Box::new(SystemProcessIdentityProbe),
                tun: Box::new(MacOsTunCapabilityPreflight),
                configuration_policy: Box::new(BundledConfigurationPolicy { compiler }),
                secrets: Box::new(UuidSecretGenerator),
                processes: Box::new(
                    NativeCoreProcessController::product_defaults()
                        .map_err(core_process_controller_startup_error)?,
                ),
            },
        )
        .map_err(|_| io::Error::other("the privileged Core service could not initialize"))?,
    );
    let server_config = CoreServiceServerConfig::new(invocation.runtime_root, invocation.owner_uid)
        .with_installed_geo_data(BUNDLED_GEODATA_PATH, 0, geodata);
    let mut server = CoreServiceServer::start_with_peer_authorizer(
        &invocation.socket_path,
        Arc::clone(&runtime),
        server_config,
        peer_authorizer,
    )?;
    let signal = ProcessSignalSource::new()
        .map_err(|_| io::Error::other("the Core service signal listener could not start"))?;
    let clock = MonotonicClock::default();
    let wake = RuntimeWaker::default();
    run_core_service_maintenance_loop(&signal, &clock, &wake, wake.clone(), |now| {
        runtime
            .maintenance_step(now)
            .map(|step| step.next_deadline)
            .unwrap_or_else(|_| now.saturating_add(CORE_SERVICE_LIVENESS_INTERVAL))
    });
    let server_result = server.shutdown();
    let service_result = runtime
        .shutdown_service()
        .map_err(|_| io::Error::other("the Core service shutdown failed"));
    server_result.and(service_result)
}

fn run_core_service_maintenance_loop(
    signal: &dyn ProcessShutdownSignal,
    clock: &dyn RuntimeClock,
    waiter: &dyn RuntimeWaiter,
    waker: RuntimeWaker,
    mut maintenance: impl FnMut(Duration) -> Duration,
) {
    signal.install_waker(waker);
    loop {
        let checkpoint = waiter.checkpoint();
        if signal.shutdown_requested() {
            break;
        }
        let next_deadline = maintenance(clock.now());
        if signal.shutdown_requested() {
            break;
        }
        let timeout = next_deadline.saturating_sub(clock.now());
        waiter.wait(checkpoint, Some(timeout));
    }
}

// -----------------------------------------------------------------------------
// Hidden Supervisor mode
// -----------------------------------------------------------------------------

pub fn run_internal_supervisor(invocation: InternalSupervisorInvocation) -> io::Result<()> {
    let readiness = invocation
        .readiness_channel()
        .map_err(|_| io::Error::other("the Supervisor readiness channel is invalid"))?;
    let inspector = PsProcessInspector;
    let process = current_process_identity(&inspector)
        .map_err(|_| io::Error::other("the Supervisor process identity is unavailable"))?;
    let paths = StatePaths::for_root(invocation.state_root);
    let started_at_unix_ms = SystemClock.now_unix_ms();
    let ownership = match SupervisorOwnership::acquire(
        paths.clone(),
        process.clone(),
        started_at_unix_ms,
        &inspector,
    ) {
        Ok(ownership) => ownership,
        Err(error) => {
            let failure = readiness_failure_from_daemon(&error);
            let _ = readiness.publish_failure(process, failure);
            return Err(io::Error::other(error.to_string()));
        }
    };
    let ownership = Arc::new(Mutex::new(Some(ownership)));
    let result = run_owned_supervisor(paths, process.clone(), Arc::clone(&ownership), &readiness);
    if let Err(error) = &result {
        let _ = readiness.publish_failure(process, error.failure.clone());
    }
    release_ownership(&ownership)?;
    result.map_err(|error| io::Error::other(error.failure.message))
}

#[derive(Clone, Debug)]
struct StartupError {
    failure: ReadinessFailure,
}

impl StartupError {
    fn new(
        stage: StartupStage,
        category: StartupFailureCategory,
        message: impl Into<String>,
    ) -> Self {
        Self {
            failure: ReadinessFailure {
                stage,
                category,
                message: message.into(),
            },
        }
    }
}

fn run_owned_supervisor(
    paths: StatePaths,
    process: ProcessIdentity,
    ownership: Arc<Mutex<Option<SupervisorOwnership>>>,
    readiness: &crate::daemon::ReadinessChannel,
) -> Result<(), StartupError> {
    let compiler = ConfigCompiler::bundled().map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The bundled configuration policy is invalid",
        )
    })?;
    let compiler_policy_sha256 = compiler.compiler_policy_sha256().to_owned();
    let mihomo_binary = bundled_mihomo_path().map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The bundled Mihomo path is unavailable",
        )
    })?;
    let mihomo_binary_sha256 = verified_binary_sha256(&mihomo_binary).map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The bundled Mihomo executable is unavailable or invalid",
        )
    })?;

    let core_runtime: Arc<dyn CoreRuntime> = Arc::new(core_service_client().map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The Core service endpoint configuration is invalid",
        )
    })?);
    let instance_token = ownership
        .lock()
        .map_err(|_| startup_internal("The Supervisor ownership state is unavailable"))?
        .as_ref()
        .map(|ownership| ownership.record().instance_token().to_owned())
        .ok_or_else(|| startup_internal("The Supervisor ownership state is unavailable"))?;
    let session = core_runtime
        .open_owner_session(&OwnerSessionRequest {
            owner_uid: nix::unistd::Uid::effective().as_raw(),
            supervisor_pid: process.pid,
            supervisor_start_identity: process.start_identity.clone(),
            instance_token: instance_token.clone(),
            protocol_version: CORE_RUNTIME_PROTOCOL_VERSION,
        })
        .map_err(map_core_session_startup)?;
    let owner = session.proof;
    let owner_guard = OwnerSessionGuard::new(Arc::clone(&core_runtime), owner.clone());
    let authoritative = AuthoritativeConfig::new(
        session.endpoint.socket_path.to_string_lossy().into_owned(),
        session.endpoint.secret().to_owned(),
    );

    let validator: Arc<dyn CoreConfigValidator + Send + Sync> = Arc::new(
        MihomoCommandValidator::bundled(
            &mihomo_binary,
            &mihomo_binary_sha256,
            BUNDLED_GEODATA_PATH,
        )
        .map_err(|_| {
            StartupError::new(
                StartupStage::SupervisorInitialization,
                StartupFailureCategory::Configuration,
                "The Mihomo validation policy is invalid",
            )
        })?,
    );
    let bundle_root = paths.runtime.join("generations");
    let stager = Arc::new(
        RuntimeBundleStager::new(
            &bundle_root,
            &mihomo_binary,
            &mihomo_binary_sha256,
            &compiler_policy_sha256,
        )
        .map_err(|_| startup_internal("The Runtime Bundle staging root is unavailable"))?,
    );
    let resolver: Arc<dyn RuntimeBundleResolver> = Arc::new(
        StagedRuntimeBundleResolver::new(
            &bundle_root,
            &compiler_policy_sha256,
            &mihomo_binary_sha256,
        )
        .map_err(|_| startup_internal("The Runtime Bundle resolver is unavailable"))?,
    );
    let state_store = Arc::new(
        AuthoritativeStateStore::open(&paths.persistence)
            .map_err(|_| startup_internal("The authoritative state store is unavailable"))?,
    );
    let transaction_store: Arc<dyn TransactionStore> = Arc::new(
        PersistenceStore::open(&paths.persistence)
            .map_err(|_| startup_internal("The transaction store is unavailable"))?,
    );
    let revisions = Arc::new(SupervisorRevisionAuthority::new(CandidateRevisions {
        profile: ProfileRevision(0),
        active_profile: ActiveProfileRevision(0),
        local_rule_set: LocalRuleSetRevision(0),
        compiler_policy_sha256,
        core_version: BUNDLED_CORE_VERSION.to_owned(),
    }));
    let revision_source: Arc<dyn CandidateRevisionSource> = revisions.clone();
    let runtime_apply: Arc<dyn RuntimeApplyPort> = Arc::new(CoreRuntimeApplyAdapter::new(
        Arc::clone(&core_runtime),
        classify_runtime_apply_error,
    ));
    let mihomo_port: Arc<dyn MihomoAdapter> = Arc::new(
        UnixMihomoAdapter::new(MihomoAdapterConfig::default())
            .map_err(|_| startup_internal("The Mihomo adapter policy is invalid"))?,
    );
    let lifecycle_lock = Arc::new(Mutex::new(()));
    let coordinator = Arc::new(ConfigTransactionCoordinator::new(
        ConfigTransactionDependencies {
            store: transaction_store,
            runtime: runtime_apply,
            validator: Arc::clone(&validator),
            health: Arc::new(MihomoRuntimeHealthProbe::bundled(Arc::clone(&mihomo_port))),
            revisions: revision_source,
            bundles: resolver,
            lifecycle_lock: Arc::clone(&lifecycle_lock),
        },
        owner.clone(),
    ));
    let transactions: Arc<dyn SupervisorTransactionPort> =
        Arc::new(CoordinatedSupervisorTransactions::new(
            Arc::clone(&state_store),
            coordinator,
            stager,
            revisions,
        ));
    let source = ReqwestProfileSource::new(ProfileSourcePolicy::product()).map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Configuration,
            "The Profile source policy is invalid",
        )
    })?;
    let source = Arc::new(
        BlockingProfileFetchPort::new(Arc::new(source))
            .map_err(|_| startup_internal("The Profile source runtime could not start"))?,
    );
    let core = Arc::new(DirectSupervisorCorePort::new(
        Arc::clone(&core_runtime),
        Arc::clone(&mihomo_port),
        owner.clone(),
    ));
    let supervisor = Arc::new(
        Supervisor::open(SupervisorDependencies {
            clock: Arc::new(SystemClock),
            source,
            compiler,
            validator,
            transactions,
            state_store,
            core,
            authoritative,
            staging_root: paths.runtime.join("profiles"),
        })
        .map_err(|_| startup_internal("The Supervisor state could not be opened"))?,
    );
    let initial_status = supervisor
        .execute(ApplicationOperation::GetStatus)
        .and_then(expect_status)
        .map_err(|_| {
            StartupError::new(
                StartupStage::CoreReadiness,
                StartupFailureCategory::Readiness,
                "The initial Supervisor status is unavailable",
            )
        })?;
    update_instance_record(
        &ownership,
        &initial_status,
        core_runtime.status(&owner).ok(),
    )
    .map_err(|_| startup_internal("The Supervisor instance record could not be updated"))?;

    let broker = Arc::new(
        IpcStreamBroker::new(0, SystemClock.now_unix_ms(), initial_status)
            .map_err(|_| startup_internal("The IPC stream broker could not start"))?,
    );
    let drain = Arc::new(DrainController::default());
    let shutdown_supervisor = Arc::clone(&supervisor);
    drain.install_cancellation(Arc::new(move || {
        shutdown_supervisor.cancel_pending_mutations();
    }));
    let application = Arc::new(SupervisorApplication {
        supervisor: Arc::clone(&supervisor),
        drain: Arc::clone(&drain),
    });
    let mut ipc_server = IpcServer::start_with_streams(
        &paths.ipc_socket,
        application,
        Arc::new(SameUserPeerAuthorizer::current()),
        Arc::clone(&broker),
        IpcServerConfig::default(),
    )
    .map_err(|_| startup_internal("The Supervisor IPC server could not start"))?;
    let shutdown_handler = Arc::new(ProductionShutdownHandler {
        process,
        instance_token,
        drain: Arc::clone(&drain),
    });
    let mut shutdown_server = ShutdownIpcServer::start(
        &paths.shutdown_socket,
        shutdown_handler,
        Arc::new(SameUserPeerAuthorizer::current()),
        IPC_REQUEST_TIMEOUT,
    )
    .map_err(|_| startup_internal("The Supervisor shutdown IPC server could not start"))?;
    let background_application: Arc<dyn BackgroundApplication> = supervisor.clone();
    let background_mihomo: Arc<dyn MihomoAdapter> = Arc::new(
        UnixMihomoAdapter::new(MihomoAdapterConfig::default())
            .map_err(|_| startup_internal("The background Mihomo adapter policy is invalid"))?,
    );
    let background_core = Arc::new(MihomoBackgroundCorePort::new(background_mihomo));
    let mut background = BackgroundRuntime::start(
        background_application,
        background_core,
        Arc::new(SystemClock),
    )
    .map_err(|_| startup_internal("The Supervisor background runtime could not start"))?;
    let mut observer = SupervisorObserver::start(ObserverDependencies {
        supervisor: Arc::clone(&supervisor),
        core_runtime: Arc::clone(&core_runtime),
        owner: owner.clone(),
        broker,
        ownership: Arc::clone(&ownership),
    })
    .map_err(|_| startup_internal("The Supervisor observer could not start"))?;

    {
        let guard = ownership
            .lock()
            .map_err(|_| startup_internal("The Supervisor ownership state is unavailable"))?;
        guard
            .as_ref()
            .ok_or_else(|| startup_internal("The Supervisor ownership state is unavailable"))?
            .publish_ready(readiness)
            .map_err(|_| startup_internal("The Supervisor readiness signal could not publish"))?;
    }

    let process_signal = ProcessSignalSource::new()
        .map_err(|_| startup_internal("The Supervisor signal listener could not start"))?;
    let shutdown_waker = RuntimeWaker::default();
    wait_for_supervisor_shutdown(
        drain.as_ref(),
        &process_signal,
        &shutdown_waker,
        shutdown_waker.clone(),
    );
    let shutdown_deadline = Instant::now()
        .checked_add(DAEMON_SHUTDOWN_TIMEOUT)
        .ok_or_else(|| startup_internal("The Supervisor shutdown deadline is unavailable"))?;
    let cancel_result = remaining_shutdown_budget(shutdown_deadline).and_then(|remaining| {
        core_runtime
            .cancel_pending_apply_with_timeout(&owner, remaining)
            .map_err(|_| io::Error::other("the pending Runtime Apply could not be cancelled"))
    });

    run_shutdown_sequence(
        || background.shutdown_until(shutdown_deadline),
        || observer.shutdown_until(shutdown_deadline),
        || {
            let remaining = remaining_shutdown_budget(shutdown_deadline)?;
            let drain_result = run_after_mutation_drain(drain.as_ref(), remaining, || {
                let stop_result = lock_lifecycle_until(&lifecycle_lock, shutdown_deadline)
                    .and_then(|_guard| {
                        let remaining = remaining_shutdown_budget(shutdown_deadline)?;
                        core_runtime
                            .stop_with_timeout(&owner, remaining)
                            .map(|_| ())
                            .map_err(|_| io::Error::other("the Managed Core could not stop"))
                    });
                let remaining = remaining_shutdown_budget(shutdown_deadline)?;
                let close_result = owner_guard.close_with_timeout(remaining);
                stop_result.and(close_result)
            });
            let result = cancel_result.and(drain_result);
            if result.is_err() {
                owner_guard.defer_cleanup_to_service();
            }
            result
        },
        || ipc_server.shutdown_until(shutdown_deadline),
        || shutdown_server.shutdown_until(shutdown_deadline),
    )
    .map_err(|_| {
        StartupError::new(
            StartupStage::SupervisorInitialization,
            StartupFailureCategory::Internal,
            "The Supervisor shutdown did not complete cleanly",
        )
    })
}

fn wait_for_supervisor_shutdown(
    drain: &DrainController,
    signal: &dyn ProcessShutdownSignal,
    waiter: &dyn RuntimeWaiter,
    waker: RuntimeWaker,
) {
    signal.install_waker(waker.clone());
    drain.install_waker(waker);
    loop {
        let checkpoint = waiter.checkpoint();
        if signal.shutdown_requested() {
            drain.request();
        }
        if drain.is_requested() {
            return;
        }
        waiter.wait(checkpoint, None);
    }
}

struct OwnerSessionGuard {
    runtime: Arc<dyn CoreRuntime>,
    owner: OwnerSessionProof,
    cleanup_state: AtomicU8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum OwnerCleanupState {
    Active = 0,
    Closed = 1,
    Deferred = 2,
}

impl OwnerSessionGuard {
    fn new(runtime: Arc<dyn CoreRuntime>, owner: OwnerSessionProof) -> Self {
        Self {
            runtime,
            owner,
            cleanup_state: AtomicU8::new(OwnerCleanupState::Active as u8),
        }
    }

    fn close_with_timeout(&self, timeout: Duration) -> io::Result<()> {
        if self.cleanup_state.load(Ordering::Acquire) != OwnerCleanupState::Active as u8 {
            return Ok(());
        }
        let result = self
            .runtime
            .close_owner_session_with_timeout(&self.owner, timeout)
            .map_err(|_| io::Error::other("the Core owner session could not close"));
        self.cleanup_state.store(
            if result.is_ok() {
                OwnerCleanupState::Closed
            } else {
                OwnerCleanupState::Deferred
            } as u8,
            Ordering::Release,
        );
        result
    }

    fn defer_cleanup_to_service(&self) {
        let _ = self.cleanup_state.compare_exchange(
            OwnerCleanupState::Active as u8,
            OwnerCleanupState::Deferred as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

impl Drop for OwnerSessionGuard {
    fn drop(&mut self) {
        if self
            .cleanup_state
            .compare_exchange(
                OwnerCleanupState::Active as u8,
                OwnerCleanupState::Deferred as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            let closed = self.runtime.close_owner_session(&self.owner).is_ok();
            if closed {
                self.cleanup_state
                    .store(OwnerCleanupState::Closed as u8, Ordering::Release);
            }
        }
    }
}

fn remaining_shutdown_budget(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "The Supervisor shutdown deadline expired",
        ))
    } else {
        Ok(remaining)
    }
}

fn lock_lifecycle_until(
    lifecycle_lock: &Mutex<()>,
    deadline: Instant,
) -> io::Result<std::sync::MutexGuard<'_, ()>> {
    loop {
        match lifecycle_lock.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(io::Error::other("the Core lifecycle lock is unavailable"));
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                let remaining = remaining_shutdown_budget(deadline)?;
                thread::sleep(remaining.min(Duration::from_millis(1)));
            }
        }
    }
}

fn run_shutdown_sequence<B, O, C, M, S>(
    background: B,
    observer: O,
    core_and_owner: C,
    main_ipc: M,
    control_ipc: S,
) -> io::Result<()>
where
    B: FnOnce() -> io::Result<()>,
    O: FnOnce() -> io::Result<()>,
    C: FnOnce() -> io::Result<()>,
    M: FnOnce() -> io::Result<()>,
    S: FnOnce() -> io::Result<()>,
{
    let background_result = background();
    let observer_result = observer();
    let core_result = core_and_owner();
    let main_ipc_result = main_ipc();
    let control_ipc_result = control_ipc();
    background_result
        .and(observer_result)
        .and(core_result)
        .and(main_ipc_result)
        .and(control_ipc_result)
}

fn run_after_mutation_drain<T>(
    drain: &DrainController,
    timeout: Duration,
    teardown: T,
) -> io::Result<()>
where
    T: FnOnce() -> io::Result<()>,
{
    if !drain.wait_for_mutations(timeout) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "The Supervisor mutation drain deadline expired",
        ));
    }
    teardown()
}

#[derive(Default)]
struct DrainState {
    requested: bool,
    active_mutations: usize,
}

struct DrainController {
    state: Mutex<DrainState>,
    ready: Condvar,
    waker: Mutex<Option<RuntimeWaker>>,
    cancellation: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Default for DrainController {
    fn default() -> Self {
        Self {
            state: Mutex::new(DrainState::default()),
            ready: Condvar::new(),
            waker: Mutex::new(None),
            cancellation: Mutex::new(None),
        }
    }
}

impl DrainController {
    fn request(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let first_request = !state.requested;
        state.requested = true;
        self.ready.notify_all();
        drop(state);
        if first_request
            && let Some(cancellation) = self
                .cancellation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        {
            cancellation();
        }
        if let Some(waker) = self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            waker.wake();
        }
    }

    fn install_waker(&self, waker: RuntimeWaker) {
        *self
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waker.clone());
        if self.is_requested() {
            waker.wake();
        }
    }

    fn install_cancellation(&self, cancellation: Arc<dyn Fn() + Send + Sync>) {
        *self
            .cancellation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&cancellation));
        if self.is_requested() {
            cancellation();
        }
    }

    fn is_requested(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .requested
    }

    fn begin_mutation(self: &Arc<Self>) -> Option<MutationGuard> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.requested {
            return None;
        }
        state.active_mutations += 1;
        Some(MutationGuard {
            drain: Arc::clone(self),
        })
    }

    fn wait_for_mutations(&self, timeout: Duration) -> bool {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.active_mutations > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timed_out) = self
                .ready
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if timed_out.timed_out() && state.active_mutations > 0 {
                return false;
            }
        }
        true
    }
}

struct MutationGuard {
    drain: Arc<DrainController>,
}

impl Drop for MutationGuard {
    fn drop(&mut self) {
        let mut state = self
            .drain
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_mutations -= 1;
        if state.active_mutations == 0 {
            self.drain.ready.notify_all();
        }
    }
}

struct ProductionShutdownHandler {
    process: ProcessIdentity,
    instance_token: String,
    drain: Arc<DrainController>,
}

impl ShutdownControlHandler for ProductionShutdownHandler {
    fn request_shutdown(
        &self,
        intent: &ShutdownIntent,
    ) -> Result<ShutdownAcknowledgement, ShutdownControlError> {
        if intent.protocol_version != crate::ipc::IPC_PROTOCOL_VERSION
            || intent.process != self.process
            || intent.instance_token != self.instance_token
        {
            return Err(ShutdownControlError::Rejected);
        }
        self.drain.request();
        Ok(ShutdownAcknowledgement {
            process: self.process.clone(),
            instance_token: self.instance_token.clone(),
        })
    }
}

struct SupervisorApplication<A> {
    supervisor: Arc<A>,
    drain: Arc<DrainController>,
}

impl<A: ApplicationClient> ApplicationClient for SupervisorApplication<A> {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        if is_lifecycle_operation(&operation) {
            return Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "Supervisor lifecycle operations require the dedicated control channel",
                false,
            ));
        }
        let _mutation = if is_mutation(&operation) {
            Some(self.drain.begin_mutation().ok_or_else(|| {
                ApplicationError::new(
                    ErrorCode::OperationUnavailable,
                    "The Supervisor is shutting down",
                    true,
                )
            })?)
        } else {
            None
        };
        let output = self.supervisor.execute(operation)?;
        Ok(if self.drain.is_requested() {
            stopping_output(output)
        } else {
            output
        })
    }
}

fn is_lifecycle_operation(operation: &ApplicationOperation) -> bool {
    matches!(
        operation,
        ApplicationOperation::Start | ApplicationOperation::Stop | ApplicationOperation::Restart
    )
}

fn is_mutation(operation: &ApplicationOperation) -> bool {
    matches!(
        operation,
        ApplicationOperation::ProfileAdd { .. }
            | ApplicationOperation::ProfileUse { .. }
            | ApplicationOperation::ProfileRemove { .. }
            | ApplicationOperation::ProxySelect { .. }
            | ApplicationOperation::RuleAdd { .. }
            | ApplicationOperation::RuleReplace { .. }
            | ApplicationOperation::RuleRemove { .. }
    )
}

fn stopping_output(output: ApplicationOutput) -> ApplicationOutput {
    match output {
        ApplicationOutput::Status(mut status) => {
            status.supervisor.lifecycle = SupervisorLifecycle::Stopping;
            if status.core.lifecycle != CoreLifecycle::Unconfigured
                && status.core.lifecycle != CoreLifecycle::Stopped
            {
                status.core.lifecycle = CoreLifecycle::Stopping;
            }
            ApplicationOutput::Status(status)
        }
        output => output,
    }
}

#[derive(Default)]
struct WakeSignal {
    requested: AtomicBool,
    mutex: Mutex<()>,
    ready: Condvar,
}

impl WakeSignal {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.ready.notify_all();
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    fn wait(&self, timeout: Duration) {
        if self.is_requested() {
            return;
        }
        let guard = self
            .mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = self.ready.wait_timeout(guard, timeout);
    }
}

struct ObserverDependencies {
    supervisor: Arc<Supervisor>,
    core_runtime: Arc<dyn CoreRuntime>,
    owner: OwnerSessionProof,
    broker: Arc<IpcStreamBroker>,
    ownership: Arc<Mutex<Option<SupervisorOwnership>>>,
}

struct SupervisorObserver {
    shutdown: Arc<WakeSignal>,
    thread: Option<JoinHandle<()>>,
}

impl SupervisorObserver {
    fn start(dependencies: ObserverDependencies) -> io::Result<Self> {
        let shutdown = Arc::new(WakeSignal::default());
        let worker_shutdown = Arc::clone(&shutdown);
        let thread = thread::Builder::new()
            .name("hopash-observer".to_owned())
            .spawn(move || observer_loop(dependencies, worker_shutdown))?;
        Ok(Self {
            shutdown,
            thread: Some(thread),
        })
    }

    fn shutdown(&mut self) -> io::Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        self.shutdown.request();
        thread
            .join()
            .map_err(|_| io::Error::other("the Supervisor observer panicked"))
    }

    fn shutdown_until(&mut self, deadline: Instant) -> io::Result<()> {
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        self.shutdown.request();
        if !wait_until_thread_finished(&thread, deadline) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "The Supervisor observer exceeded the shutdown deadline",
            ));
        }
        thread
            .join()
            .map_err(|_| io::Error::other("the Supervisor observer panicked"))
    }
}

impl Drop for SupervisorObserver {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn wait_until_thread_finished<T>(thread: &JoinHandle<T>, deadline: Instant) -> bool {
    while !thread.is_finished() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(Duration::from_millis(1)));
    }
    true
}

fn observer_loop(dependencies: ObserverDependencies, shutdown: Arc<WakeSignal>) {
    let mut status_sequence = 0_u64;
    let mut previous_status = None;
    let mut supervisor_log_sequence = None;
    let mut broker_logs_seeded = false;
    let mut service_log_sequence = None;
    let mut pending_service_drops = 0_u64;

    while !shutdown.is_requested() {
        if let Ok(batch) = dependencies.core_runtime.logs(
            &dependencies.owner,
            service_log_sequence,
            OBSERVER_LOG_BATCH,
        ) {
            let current_core = dependencies
                .core_runtime
                .status(&dependencies.owner)
                .ok()
                .and_then(|status| status.managed_core);
            if !batch.records.is_empty() {
                let _ = dependencies
                    .supervisor
                    .execute(ApplicationOperation::GetStatus);
            }
            pending_service_drops = pending_service_drops.saturating_add(batch.dropped_since_after);
            if pending_service_drops > 0
                && let Some(core) = &current_core
                && dependencies
                    .supervisor
                    .record_core_log_drop(core.instance_generation, pending_service_drops)
                    .is_ok_and(|accepted| accepted)
            {
                pending_service_drops = 0;
                let _ = dependencies.supervisor.publish_core_log(
                    core.instance_generation,
                    SystemClock.now_unix_ms(),
                    LogLevel::Warn,
                    LogSource::Stderr,
                    "Privileged Core output was dropped before forwarding",
                );
            }
            let mut consumed_batch = true;
            for record in batch.records {
                if current_core
                    .as_ref()
                    .is_none_or(|core| core.instance_generation != record.instance_generation)
                {
                    service_log_sequence = Some(record.sequence);
                    continue;
                }
                let level = match record.source {
                    ProcessOutputSource::Stdout => LogLevel::Info,
                    ProcessOutputSource::Stderr => LogLevel::Warn,
                };
                let source = match record.source {
                    ProcessOutputSource::Stdout => LogSource::Stdout,
                    ProcessOutputSource::Stderr => LogSource::Stderr,
                };
                match dependencies.supervisor.publish_core_log(
                    record.instance_generation,
                    record.timestamp_unix_ms,
                    level,
                    source,
                    record.message,
                ) {
                    Ok(true) => service_log_sequence = Some(record.sequence),
                    Ok(false) | Err(_) => {
                        consumed_batch = false;
                        break;
                    }
                }
            }
            if consumed_batch {
                service_log_sequence = batch.next_sequence.or(service_log_sequence);
            }
        }

        if let Ok(tail) = dependencies
            .supervisor
            .core_log_tail(supervisor_log_sequence)
        {
            if !broker_logs_seeded || tail.gap {
                let sequence_horizon = tail.sequence_horizon;
                if dependencies.broker.synchronize_log_tail(tail).is_ok() {
                    broker_logs_seeded = true;
                    supervisor_log_sequence = sequence_horizon.or(supervisor_log_sequence);
                }
            } else {
                let sequence_horizon = tail.sequence_horizon;
                let mut synchronized = true;
                for record in tail.records {
                    let sequence = record.sequence();
                    if dependencies.broker.publish_log(record).is_ok() {
                        supervisor_log_sequence = Some(sequence);
                    } else {
                        synchronized = false;
                        broker_logs_seeded = false;
                        break;
                    }
                }
                if synchronized {
                    supervisor_log_sequence = sequence_horizon.or(supervisor_log_sequence);
                }
            }
        }

        if let Ok(status) = dependencies
            .supervisor
            .execute(ApplicationOperation::GetStatus)
            .and_then(expect_status)
        {
            let core_status = dependencies.core_runtime.status(&dependencies.owner).ok();
            let _ = update_instance_record(&dependencies.ownership, &status, core_status);
            if previous_status.as_ref() != Some(&status)
                && let Some(next) = status_sequence.checked_add(1)
                && dependencies
                    .broker
                    .publish_status(next, SystemClock.now_unix_ms(), status.clone())
                    .is_ok()
            {
                status_sequence = next;
                previous_status = Some(status);
            }
        }
        shutdown.wait(STATUS_SAMPLE_INTERVAL);
    }
}

fn update_instance_record(
    ownership: &Mutex<Option<SupervisorOwnership>>,
    status: &crate::domain::StatusSnapshot,
    core_status: Option<crate::core::CoreRuntimeStatus>,
) -> Result<(), ()> {
    let active_profile_id = status
        .active_profile
        .as_ref()
        .map(|profile| profile.id.to_string());
    let managed_core = core_status
        .and_then(|status| status.managed_core)
        .map(|core| ManagedCoreIdentityRecord {
            pid: core.pid,
            process_start_identity: core.process_start_identity,
            control_endpoint: core.endpoint.socket_path,
            runtime_generation: core.runtime_generation.0,
            core_instance_generation: core.instance_generation.0,
        });
    let mut ownership = ownership.lock().map_err(|_| ())?;
    let ownership = ownership.as_mut().ok_or(())?;
    if ownership.record().active_profile_id == active_profile_id
        && ownership.record().managed_core == managed_core
    {
        return Ok(());
    }
    ownership
        .update_record(|record| {
            record.active_profile_id = active_profile_id;
            record.managed_core = managed_core;
        })
        .map_err(|_| ())
}

fn release_ownership(ownership: &Mutex<Option<SupervisorOwnership>>) -> io::Result<()> {
    let ownership = ownership
        .lock()
        .map_err(|_| io::Error::other("the Supervisor ownership state is unavailable"))?
        .take();
    ownership.map_or(Ok(()), |ownership| {
        ownership
            .release()
            .map_err(|_| io::Error::other("the Supervisor ownership state could not release"))
    })
}

fn map_core_session_startup(error: crate::core::CoreRuntimeError) -> StartupError {
    let (category, message) = match error.kind {
        crate::core::CoreRuntimeErrorKind::TunPermissionDenied => (
            StartupFailureCategory::Permission,
            "TUN capability is unavailable",
        ),
        crate::core::CoreRuntimeErrorKind::TunUnsupported => (
            StartupFailureCategory::Unsupported,
            "TUN is unsupported on this platform",
        ),
        crate::core::CoreRuntimeErrorKind::ProtocolMismatch => (
            StartupFailureCategory::Configuration,
            "The Core service protocol is incompatible",
        ),
        crate::core::CoreRuntimeErrorKind::Authentication => (
            StartupFailureCategory::Permission,
            "The Core service rejected the Supervisor identity",
        ),
        _ => (
            StartupFailureCategory::Readiness,
            "The privileged Core service is unavailable",
        ),
    };
    StartupError::new(StartupStage::CoreReadiness, category, message)
}

fn startup_internal(message: &'static str) -> StartupError {
    StartupError::new(
        StartupStage::SupervisorInitialization,
        StartupFailureCategory::Internal,
        message,
    )
}

fn readiness_failure_from_daemon(error: &DaemonError) -> ReadinessFailure {
    ReadinessFailure {
        stage: error.stage().unwrap_or(StartupStage::SingletonOwnership),
        category: error.category().unwrap_or(StartupFailureCategory::Internal),
        message: error.to_string(),
    }
}

fn bundled_mihomo_path() -> io::Result<PathBuf> {
    let path = std::env::var_os("HOPASH_MIHOMO_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(BUNDLED_MIHOMO_PATH));
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the Mihomo executable path must be absolute",
        ))
    }
}

fn core_service_client() -> io::Result<CoreServiceClient> {
    #[cfg(debug_assertions)]
    if let Some(socket_path) = std::env::var_os(DEBUG_CORE_SERVICE_SOCKET_ENV) {
        let socket_path = PathBuf::from(socket_path);
        if !socket_path.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the Core service socket path must be absolute",
            ));
        }
        return Ok(CoreServiceClient::for_service_uid(
            socket_path,
            nix::unistd::Uid::effective().as_raw(),
        ));
    }
    Ok(CoreServiceClient::new(CORE_SERVICE_SOCKET_PATH))
}

fn verified_binary_sha256(path: &Path) -> io::Result<String> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the Mihomo path must be absolute",
        ));
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o111 == 0
        || metadata.len() > MIHOMO_BINARY_MAX_BYTES as u64
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the Mihomo executable is invalid",
        ));
    }
    let file = fs::File::open(path)?;
    let opened = file.metadata()?;
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the Mihomo executable changed while opening",
        ));
    }
    let mut content = Vec::with_capacity(metadata.len() as usize);
    file.take((MIHOMO_BINARY_MAX_BYTES as u64).saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() > MIHOMO_BINARY_MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the Mihomo executable is too large",
        ));
    }
    Ok(crate::digest::sha256_hex(&content))
}

fn invalid_product_configuration(_error: impl fmt::Display) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "the bundled product configuration is invalid",
    )
}

fn core_process_controller_startup_error(error: io::Error) -> io::Error {
    io::Error::new(
        error.kind(),
        "the Core process controller could not initialize",
    )
}

#[cfg(test)]
mod tests;

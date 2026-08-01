use hopash::config::{
    AuthoritativeConfig, ConfigCompiler, CoreConfigValidator, CoreValidationError,
    EffectiveConfiguration,
};
use hopash::core::{
    ApplyCandidateResult, ApplyDisposition, CoreControlEndpoint, CoreRuntimeError,
    CoreRuntimeStatus, ManagedCoreHandle, OwnerSessionProof, RuntimeBundle,
};
use hopash::domain::{CoreInstanceGeneration, LocalRuleSetRevision, ProfileId, RuntimeGeneration};
use hopash::persistence::{
    ObjectId, PersistenceStore, PreparedTransaction, RecoveryState, TransactionBundle,
    TransactionId,
};
use hopash::profile::{ActiveProfileRevision, ProfileRevision, ProfileSnapshot, SnapshotLimits};
use hopash::runtime_bundle::{
    RuntimeGenerationPruneResult, RuntimeGenerationRetention, prune_runtime_generations,
};
use hopash::transaction::{
    ApplyPath, CandidateRevisionSource, CandidateRevisions, ConfigTransactionCandidate,
    ConfigTransactionCoordinator, ConfigTransactionDependencies, ConfigTransactionErrorKind,
    RecoveryOutcome, RuntimeApplyFailure, RuntimeApplyPort, RuntimeBundleResolveError,
    RuntimeBundleResolver, RuntimeHealthError, RuntimeHealthProbe, TransactionStore,
};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hopash-config-transaction-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("the test directory should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("the test directory should be removed");
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreFailure {
    PrepareAfterWrite,
    CommitBeforeWrite,
    CommitAfterWrite,
    Clear,
    Recover,
}

struct FaultStore {
    inner: Arc<PersistenceStore>,
    failure: Mutex<Option<StoreFailure>>,
    events: Arc<Mutex<Vec<String>>>,
}

impl FaultStore {
    fn fail_next(&self, failure: StoreFailure) {
        *self.failure.lock().expect("failure lock") = Some(failure);
    }

    fn take_failure(&self, expected: StoreFailure) -> bool {
        let mut failure = self.failure.lock().expect("failure lock");
        if *failure == Some(expected) {
            *failure = None;
            true
        } else {
            false
        }
    }

    fn event(&self, event: &str) {
        self.events
            .lock()
            .expect("event lock")
            .push(event.to_owned());
    }
}

impl TransactionStore for FaultStore {
    fn read_object_limited(&self, id: &ObjectId, limit: usize) -> io::Result<Vec<u8>> {
        self.event("read_object");
        self.inner.read_object_limited(id, limit)
    }

    fn prepare(&self, bundle: &TransactionBundle) -> io::Result<PreparedTransaction> {
        self.event("prepare");
        let prepared = self.inner.prepare(bundle)?;
        if self.take_failure(StoreFailure::PrepareAfterWrite) {
            Err(io::Error::other("injected prepare failure"))
        } else {
            Ok(prepared)
        }
    }

    fn recover(&self) -> io::Result<RecoveryState> {
        self.event("recover");
        if self.take_failure(StoreFailure::Recover) {
            Err(io::Error::other("injected recovery failure"))
        } else {
            self.inner.recover()
        }
    }

    fn commit_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        self.event("commit");
        if self.take_failure(StoreFailure::CommitBeforeWrite) {
            return Err(io::Error::other("injected commit failure"));
        }
        self.inner.commit_prepared(prepared)?;
        if self.take_failure(StoreFailure::CommitAfterWrite) {
            Err(io::Error::other("injected post-commit failure"))
        } else {
            Ok(())
        }
    }

    fn clear_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        self.event("clear");
        if self.take_failure(StoreFailure::Clear) {
            Err(io::Error::other("injected cleanup failure"))
        } else {
            self.inner.clear_prepared(prepared)
        }
    }

    fn load_transaction(&self, id: &TransactionId) -> io::Result<TransactionBundle> {
        self.event("load_transaction");
        self.inner.load_transaction(id)
    }
}

struct MutableRevisions {
    current: Mutex<CandidateRevisions>,
}

impl MutableRevisions {
    fn set(&self, revisions: CandidateRevisions) {
        *self.current.lock().expect("revision lock") = revisions;
    }
}

impl CandidateRevisionSource for MutableRevisions {
    fn current(&self) -> CandidateRevisions {
        self.current.lock().expect("revision lock").clone()
    }
}

struct BlockPoint {
    entered: Mutex<Option<mpsc::Sender<()>>>,
    released: Mutex<bool>,
    release_changed: Condvar,
}

impl BlockPoint {
    fn new() -> (Arc<Self>, mpsc::Receiver<()>) {
        let (sender, receiver) = mpsc::channel();
        (
            Arc::new(Self {
                entered: Mutex::new(Some(sender)),
                released: Mutex::new(false),
                release_changed: Condvar::new(),
            }),
            receiver,
        )
    }

    fn wait(&self) {
        if let Some(sender) = self.entered.lock().expect("entered lock").take() {
            sender.send(()).expect("the test should observe validation");
        }
        let mut released = self.released.lock().expect("release lock");
        while !*released {
            released = self.release_changed.wait(released).expect("release wait");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("release lock") = true;
        self.release_changed.notify_all();
    }
}

struct FakeValidator {
    fail_next: AtomicBool,
    block_next: Mutex<Option<Arc<BlockPoint>>>,
    active_block: Mutex<Option<Arc<BlockPoint>>>,
    events: Arc<Mutex<Vec<String>>>,
}

impl FakeValidator {
    fn fail_next(&self) {
        self.fail_next.store(true, Ordering::Release);
    }

    fn block_next(&self, point: Arc<BlockPoint>) {
        *self.block_next.lock().expect("block lock") = Some(point);
    }
}

impl CoreConfigValidator for FakeValidator {
    fn validate(
        &self,
        _configuration: &EffectiveConfiguration,
        _staging_root: &std::path::Path,
    ) -> Result<(), CoreValidationError> {
        self.events
            .lock()
            .expect("event lock")
            .push("validate".to_owned());
        if let Some(point) = self.block_next.lock().expect("block lock").take() {
            *self.active_block.lock().expect("active block lock") = Some(Arc::clone(&point));
            point.wait();
            self.active_block.lock().expect("active block lock").take();
        }
        if self.fail_next.swap(false, Ordering::AcqRel) {
            Err(CoreValidationError::new("injected validation failure"))
        } else {
            Ok(())
        }
    }

    fn cancel_pending(&self) {
        if let Some(point) = self
            .active_block
            .lock()
            .expect("active block lock")
            .as_ref()
        {
            point.release();
        }
    }
}

struct FakeHealth {
    fail_next: AtomicBool,
    events: Arc<Mutex<Vec<String>>>,
}

impl FakeHealth {
    fn fail_next(&self) {
        self.fail_next.store(true, Ordering::Release);
    }
}

impl RuntimeHealthProbe for FakeHealth {
    fn confirm_ready(&self, managed_core: &ManagedCoreHandle) -> Result<(), RuntimeHealthError> {
        self.events
            .lock()
            .expect("event lock")
            .push(format!("health:{}", managed_core.runtime_generation.0));
        if self.fail_next.swap(false, Ordering::AcqRel) {
            Err(RuntimeHealthError)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyScript {
    Success,
    DefiniteFailure,
    IndeterminateFailure,
    TunPermissionDenied,
    TunUnsupported,
}

struct FakeRuntimeState {
    managed_core: Option<ManagedCoreHandle>,
    scripts: VecDeque<ApplyScript>,
    next_instance_generation: u64,
    mismatch_status_once: bool,
    mismatch_endpoint_once: bool,
}

struct FakeRuntime {
    state: Mutex<FakeRuntimeState>,
    events: Arc<Mutex<Vec<String>>>,
    block_next: Mutex<Option<Arc<BlockPoint>>>,
    active_block: Mutex<Option<Arc<BlockPoint>>>,
}

impl FakeRuntime {
    fn script(&self, scripts: impl IntoIterator<Item = ApplyScript>) {
        self.state
            .lock()
            .expect("runtime lock")
            .scripts
            .extend(scripts);
    }

    fn mismatch_status_once(&self) {
        self.state
            .lock()
            .expect("runtime lock")
            .mismatch_status_once = true;
    }

    fn mismatch_endpoint_once(&self) {
        self.state
            .lock()
            .expect("runtime lock")
            .mismatch_endpoint_once = true;
    }

    fn force_generation(&self, generation: RuntimeGeneration) {
        let mut state = self.state.lock().expect("runtime lock");
        state.managed_core = Some(core_handle(generation, 900));
    }

    fn generation(&self) -> Option<RuntimeGeneration> {
        self.state
            .lock()
            .expect("runtime lock")
            .managed_core
            .as_ref()
            .map(|core| core.runtime_generation)
    }

    fn event(&self, event: String) {
        self.events.lock().expect("event lock").push(event);
    }

    fn block_next(&self, point: Arc<BlockPoint>) {
        *self.block_next.lock().expect("runtime block lock") = Some(point);
    }
}

impl RuntimeApplyPort for FakeRuntime {
    fn apply_candidate(
        &self,
        _owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, RuntimeApplyFailure> {
        self.event(format!("apply:{}", bundle.generation.0));
        if let Some(point) = self.block_next.lock().expect("runtime block lock").take() {
            *self.active_block.lock().expect("active block lock") = Some(Arc::clone(&point));
            point.wait();
            self.active_block.lock().expect("active block lock").take();
        }
        let mut state = self.state.lock().expect("runtime lock");
        let script = state.scripts.pop_front().unwrap_or(ApplyScript::Success);
        match script {
            ApplyScript::Success => {
                state.next_instance_generation += 1;
                let managed_core = core_handle(bundle.generation, state.next_instance_generation);
                state.managed_core = Some(managed_core.clone());
                Ok(ApplyCandidateResult {
                    disposition: ApplyDisposition::Reloaded,
                    managed_core,
                })
            }
            ApplyScript::DefiniteFailure => Err(RuntimeApplyFailure::Definite),
            ApplyScript::TunPermissionDenied => Err(RuntimeApplyFailure::TunPermissionDenied),
            ApplyScript::TunUnsupported => Err(RuntimeApplyFailure::TunUnsupported),
            ApplyScript::IndeterminateFailure => {
                state.next_instance_generation += 1;
                state.managed_core = Some(core_handle(
                    bundle.generation,
                    state.next_instance_generation,
                ));
                Err(RuntimeApplyFailure::Indeterminate)
            }
        }
    }

    fn status(&self, _owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError> {
        self.event("status".to_owned());
        let mut state = self.state.lock().expect("runtime lock");
        let mut managed_core = state.managed_core.clone();
        if state.mismatch_status_once {
            state.mismatch_status_once = false;
            if let Some(core) = managed_core.as_mut() {
                core.pid += 1;
            }
        }
        if state.mismatch_endpoint_once {
            state.mismatch_endpoint_once = false;
            if let Some(core) = managed_core.as_mut() {
                core.endpoint =
                    CoreControlEndpoint::new("/fixture/unexpected-core.sock", "unexpected-secret");
            }
        }
        Ok(CoreRuntimeStatus::from_managed_core(managed_core))
    }

    fn stop(&self, _owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError> {
        self.event("stop".to_owned());
        self.state.lock().expect("runtime lock").managed_core = None;
        Ok(())
    }

    fn cancel_pending_apply(&self) {
        if let Some(point) = self
            .active_block
            .lock()
            .expect("active block lock")
            .as_ref()
        {
            point.release();
        }
    }
}

fn core_handle(
    runtime_generation: RuntimeGeneration,
    instance_generation: u64,
) -> ManagedCoreHandle {
    ManagedCoreHandle {
        pid: 1_000 + u32::try_from(instance_generation).expect("fixture generation should fit"),
        process_start_identity: format!("start-{instance_generation}"),
        endpoint: CoreControlEndpoint::new(
            format!("/fixture/core-{instance_generation}.sock"),
            "fixture-secret",
        ),
        instance_generation: CoreInstanceGeneration(instance_generation),
        runtime_generation,
    }
}

struct FakeBundleResolver {
    bundles: Mutex<BTreeMap<RuntimeGeneration, RuntimeBundle>>,
    runtime_root: PathBuf,
}

impl FakeBundleResolver {
    fn register(&self, bundle: RuntimeBundle) {
        let generation_root = self
            .runtime_root
            .join(format!("generation-{:020}", bundle.generation.0));
        fs::create_dir_all(&generation_root)
            .expect("the retained Runtime Generation should be created");
        fs::set_permissions(&generation_root, fs::Permissions::from_mode(0o700))
            .expect("the retained Runtime Generation should be private");
        self.bundles
            .lock()
            .expect("bundle lock")
            .insert(bundle.generation, bundle);
    }
}

impl RuntimeBundleResolver for FakeBundleResolver {
    fn resolve(
        &self,
        transaction: &TransactionBundle,
    ) -> Result<RuntimeBundle, RuntimeBundleResolveError> {
        self.bundles
            .lock()
            .expect("bundle lock")
            .get(&transaction.runtime_generation)
            .cloned()
            .ok_or(RuntimeBundleResolveError)
    }

    fn prune_generations(
        &self,
        retention: RuntimeGenerationRetention,
    ) -> Result<RuntimeGenerationPruneResult, RuntimeBundleResolveError> {
        prune_runtime_generations(&self.runtime_root, retention)
            .map_err(|_| RuntimeBundleResolveError)
    }
}

struct Harness {
    directory: TestDirectory,
    persistence: Arc<PersistenceStore>,
    store: Arc<FaultStore>,
    runtime: Arc<FakeRuntime>,
    validator: Arc<FakeValidator>,
    health: Arc<FakeHealth>,
    revisions: Arc<MutableRevisions>,
    bundles: Arc<FakeBundleResolver>,
    coordinator: Arc<ConfigTransactionCoordinator>,
    events: Arc<Mutex<Vec<String>>>,
}

impl Harness {
    fn new() -> Self {
        let directory = TestDirectory::new();
        let persistence = Arc::new(
            PersistenceStore::open(directory.path.join("state"))
                .expect("the persistence store should open"),
        );
        let events = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(FaultStore {
            inner: persistence.clone(),
            failure: Mutex::new(None),
            events: events.clone(),
        });
        let runtime = Arc::new(FakeRuntime {
            state: Mutex::new(FakeRuntimeState {
                managed_core: None,
                scripts: VecDeque::new(),
                next_instance_generation: 0,
                mismatch_status_once: false,
                mismatch_endpoint_once: false,
            }),
            events: events.clone(),
            block_next: Mutex::new(None),
            active_block: Mutex::new(None),
        });
        let validator = Arc::new(FakeValidator {
            fail_next: AtomicBool::new(false),
            block_next: Mutex::new(None),
            active_block: Mutex::new(None),
            events: events.clone(),
        });
        let health = Arc::new(FakeHealth {
            fail_next: AtomicBool::new(false),
            events: events.clone(),
        });
        let initial_revisions = CandidateRevisions {
            profile: ProfileRevision(0),
            active_profile: ActiveProfileRevision(0),
            local_rule_set: LocalRuleSetRevision(0),
            compiler_policy_sha256: String::new(),
            core_version: String::new(),
        };
        let revisions = Arc::new(MutableRevisions {
            current: Mutex::new(initial_revisions),
        });
        let runtime_root = directory.path.join("retained-runtime");
        fs::create_dir(&runtime_root).expect("the retained runtime root should be created");
        fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))
            .expect("the retained runtime root should be private");
        let bundles = Arc::new(FakeBundleResolver {
            bundles: Mutex::new(BTreeMap::new()),
            runtime_root,
        });
        let coordinator = Arc::new(ConfigTransactionCoordinator::new(
            ConfigTransactionDependencies {
                store: store.clone(),
                runtime: runtime.clone(),
                validator: validator.clone(),
                health: health.clone(),
                revisions: revisions.clone(),
                bundles: bundles.clone(),
                lifecycle_lock: Arc::new(Mutex::new(())),
            },
            OwnerSessionProof::new("owner", "owner-secret"),
        ));
        Self {
            directory,
            persistence,
            store,
            runtime,
            validator,
            health,
            revisions,
            bundles,
            coordinator,
            events,
        }
    }

    fn candidate(&self, generation: u64) -> ConfigTransactionCandidate {
        let generation_root = self.directory.path.join(format!("runtime-{generation}"));
        fs::create_dir(&generation_root).expect("the runtime root should be created");
        let snapshot = ProfileSnapshot::parse(
            format!(
                concat!(
                    "proxies:\n",
                    "  - name: node-{generation}\n",
                    "    type: ss\n",
                    "    server: 127.0.0.1\n",
                    "    port: 443\n",
                    "    cipher: aes-128-gcm\n",
                    "    password: fixture-password\n",
                    "proxy-groups:\n",
                    "  - name: Main\n",
                    "    type: select\n",
                    "    proxies: [node-{generation}, DIRECT]\n",
                    "rules: []\n"
                ),
                generation = generation
            )
            .as_bytes(),
            SnapshotLimits::new(64 * 1_024, 32),
        )
        .expect("the fixture Profile Snapshot should parse");
        let compiler = ConfigCompiler::bundled().expect("the bundled compiler should load");
        let configuration = compiler
            .compile(
                &snapshot,
                &["MATCH,Main".to_owned()],
                &AuthoritativeConfig::new(
                    generation_root.join("core.sock").display().to_string(),
                    "core-secret",
                ),
                &generation_root,
            )
            .expect("the fixture configuration should compile");
        let revisions = CandidateRevisions {
            profile: ProfileRevision(generation * 10),
            active_profile: ActiveProfileRevision(generation * 20),
            local_rule_set: LocalRuleSetRevision(generation * 30),
            compiler_policy_sha256: configuration.compiler_policy_sha256().to_owned(),
            core_version: configuration.core_version().to_owned(),
        };
        let runtime = RuntimeBundle {
            generation: RuntimeGeneration(generation),
            generation_root,
            manifest_sha256: format!("manifest-{generation}"),
            compiler_policy_sha256: revisions.compiler_policy_sha256.clone(),
            mihomo_binary_sha256: "mihomo-binary".to_owned(),
        };
        let transaction = TransactionBundle {
            supervisor_state: self.put_object("state", generation),
            profile_snapshot: self.put_object("snapshot", generation),
            local_rule_set: self.put_object("rules", generation),
            effective_configuration: self
                .persistence
                .put_object(configuration.yaml().as_bytes())
                .expect("the configuration should be stored"),
            profile_revision: revisions.profile,
            local_rule_set_revision: revisions.local_rule_set,
            active_profile_id: ProfileId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8")
                .expect("the fixture Profile ID should parse"),
            runtime_generation: RuntimeGeneration(generation),
        };
        self.bundles.register(runtime.clone());
        self.revisions.set(revisions.clone());
        ConfigTransactionCandidate {
            transaction,
            runtime,
            configuration,
            revisions,
        }
    }

    fn put_object(&self, label: &str, generation: u64) -> ObjectId {
        self.persistence
            .put_object(format!("{label}-{generation}").as_bytes())
            .expect("the fixture object should be stored")
    }

    fn commit_initial(&self) {
        let candidate = self.candidate(1);
        self.coordinator
            .execute(&candidate)
            .expect("the initial transaction should commit");
        self.events.lock().expect("event lock").clear();
    }

    fn committed_generation(&self) -> Option<RuntimeGeneration> {
        let recovery = self.persistence.recover().expect("state should recover");
        recovery.committed.map(|manifest| {
            self.persistence
                .load_transaction(&manifest.current)
                .expect("the committed transaction should load")
                .runtime_generation
        })
    }

    fn has_prepared(&self) -> bool {
        self.persistence
            .recover()
            .expect("state should recover")
            .prepared
            .is_some()
    }

    fn events(&self) -> Vec<String> {
        self.events.lock().expect("event lock").clone()
    }

    fn retained_generations(&self) -> Vec<u64> {
        let mut generations = fs::read_dir(&self.bundles.runtime_root)
            .expect("the retained runtime root should be readable")
            .filter_map(Result::ok)
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| name.strip_prefix("generation-"))
                    .and_then(|generation| generation.parse::<u64>().ok())
            })
            .collect::<Vec<_>>();
        generations.sort_unstable();
        generations
    }
}

#[test]
fn successful_transaction_orders_prepare_validation_apply_health_commit_and_cleanup() {
    let harness = Harness::new();
    let candidate = harness.candidate(1);

    let result = harness
        .coordinator
        .execute(&candidate)
        .expect("the transaction should commit");

    assert_eq!(result.candidate_generation, RuntimeGeneration(1));
    assert_eq!(result.committed_generation, RuntimeGeneration(1));
    assert_eq!(result.apply_path, ApplyPath::Direct);
    assert_eq!(result.recovery, RecoveryOutcome::NotRequired);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
    assert_event_order(
        &harness.events(),
        &[
            "prepare", "validate", "apply:1", "status", "health:1", "commit", "clear",
        ],
    );
}

#[test]
fn sequential_commits_retain_only_current_and_previous_runtime_generations() {
    let harness = Harness::new();
    for generation in 1..=5 {
        let candidate = harness.candidate(generation);
        harness
            .coordinator
            .execute(&candidate)
            .expect("the transaction should commit");
    }

    assert_eq!(harness.retained_generations(), vec![4, 5]);
}

#[test]
fn validation_keeps_current_previous_and_prepared_runtime_generations() {
    let harness = Harness::new();
    harness.commit_initial();
    let second = harness.candidate(2);
    harness
        .coordinator
        .execute(&second)
        .expect("the second transaction should commit");
    let third = harness.candidate(3);
    let (point, entered) = BlockPoint::new();
    harness.validator.block_next(point.clone());
    let coordinator = Arc::clone(&harness.coordinator);
    let worker = std::thread::spawn(move || coordinator.execute(&third));

    entered
        .recv_timeout(Duration::from_secs(1))
        .expect("validation should start");
    assert_eq!(harness.retained_generations(), vec![1, 2, 3]);
    point.release();
    worker
        .join()
        .expect("the transaction thread should finish")
        .expect("the prepared transaction should commit");
    assert_eq!(harness.retained_generations(), vec![2, 3]);
}

#[test]
fn failed_apply_removes_the_candidate_runtime_generation_after_rollback() {
    let harness = Harness::new();
    harness.commit_initial();
    let second = harness.candidate(2);
    harness
        .coordinator
        .execute(&second)
        .expect("the second transaction should commit");
    let third = harness.candidate(3);
    harness.runtime.script([ApplyScript::DefiniteFailure]);

    let error = harness
        .coordinator
        .execute(&third)
        .expect_err("the third Runtime Apply should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Apply);
    assert_eq!(harness.retained_generations(), vec![1, 2]);
}

#[test]
fn unsafe_runtime_entry_blocks_apply_and_preserves_all_entries() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    let unknown = harness.bundles.runtime_root.join("unexpected-entry");
    fs::write(&unknown, b"preserve").expect("the unknown entry should be written");

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("unsafe runtime cleanup should block apply");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Cleanup);
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(unknown.exists());
    assert_eq!(harness.retained_generations(), vec![1, 2]);
}

#[test]
fn validation_failure_clears_the_prepared_journal_without_applying() {
    let harness = Harness::new();
    let candidate = harness.candidate(1);
    harness.validator.fail_next();

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("validation should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Validation);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Converged { generation: None }
    );
    assert_eq!(harness.committed_generation(), None);
    assert_eq!(harness.runtime.generation(), None);
    assert!(!harness.has_prepared());
    assert!(
        !harness
            .events()
            .iter()
            .any(|event| event.starts_with("apply:"))
    );
}

#[test]
fn revision_change_during_validation_discards_the_stale_candidate() {
    let harness = Harness::new();
    let candidate = harness.candidate(1);
    let changed = CandidateRevisions {
        profile: ProfileRevision(999),
        ..candidate.revisions.clone()
    };
    let (point, entered) = BlockPoint::new();
    harness.validator.block_next(point.clone());
    let coordinator = harness.coordinator.clone();
    let transaction = candidate;
    let worker = std::thread::spawn(move || coordinator.execute(&transaction));
    entered
        .recv_timeout(Duration::from_secs(1))
        .expect("validation should start");
    harness.revisions.set(changed);
    point.release();

    let error = worker
        .join()
        .expect("the worker should finish")
        .expect_err("the candidate should become stale");

    assert_eq!(error.kind, ConfigTransactionErrorKind::StaleCandidate);
    assert_eq!(harness.runtime.generation(), None);
    assert!(!harness.has_prepared());
}

#[test]
fn blocking_producers_serialize_and_rule_acquire_reports_busy() {
    let harness = Harness::new();
    let candidate = harness.candidate(1);
    let (point, entered) = BlockPoint::new();
    harness.validator.block_next(point.clone());
    let first_coordinator = harness.coordinator.clone();
    let first_candidate = candidate.clone();
    let first = std::thread::spawn(move || first_coordinator.execute(&first_candidate));
    entered
        .recv_timeout(Duration::from_secs(1))
        .expect("the first producer should hold the coordinator");

    let rule_error = harness
        .coordinator
        .try_execute_rule(&candidate)
        .expect_err("the rule producer should report busy");
    assert_eq!(rule_error.kind, ConfigTransactionErrorKind::Busy);

    let second_coordinator = harness.coordinator;
    let second_candidate = candidate;
    let (finished_sender, finished_receiver) = mpsc::channel();
    let second = std::thread::spawn(move || {
        finished_sender
            .send(second_coordinator.execute(&second_candidate))
            .expect("the second result should be observed");
    });
    assert!(
        finished_receiver
            .recv_timeout(Duration::from_millis(30))
            .is_err(),
        "the second normal producer should wait"
    );

    point.release();
    first
        .join()
        .expect("the first producer should finish")
        .expect("the first producer should commit");
    let second_error = finished_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("the second producer should resume")
        .expect_err("the reused generation should be invalid after serialization");
    assert_eq!(
        second_error.kind,
        ConfigTransactionErrorKind::InvalidCandidate
    );
    second.join().expect("the second producer should finish");
}

#[test]
fn definite_apply_failure_rolls_back_to_the_previous_generation() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.runtime.script([ApplyScript::DefiniteFailure]);

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the candidate apply should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Apply);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Converged {
            generation: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn shutdown_cancels_validation_and_preserves_the_prepared_journal() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    let (point, entered) = BlockPoint::new();
    harness.validator.block_next(point);
    let coordinator = Arc::clone(&harness.coordinator);
    let worker = std::thread::spawn(move || coordinator.execute(&candidate));
    entered
        .recv_timeout(Duration::from_secs(1))
        .expect("validation should reach the cancellable fixture");

    harness.coordinator.request_shutdown();

    let error = worker
        .join()
        .expect("the validation worker should finish")
        .expect_err("shutdown should interrupt validation before Runtime Apply");
    assert_eq!(error.kind, ConfigTransactionErrorKind::Shutdown);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Pending {
            target: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert!(harness.has_prepared());
    assert!(!harness.events().iter().any(|event| event == "apply:2"));
    assert!(!harness.events().iter().any(|event| event == "commit"));
}

#[test]
fn shutdown_cancels_runtime_apply_and_preserves_the_prepared_journal() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    let (point, entered) = BlockPoint::new();
    harness.runtime.block_next(point);
    let coordinator = Arc::clone(&harness.coordinator);
    let worker = std::thread::spawn(move || coordinator.execute(&candidate));
    entered
        .recv_timeout(Duration::from_secs(1))
        .expect("Runtime Apply should reach the cancellable fixture");

    harness.coordinator.request_shutdown();

    let error = worker
        .join()
        .expect("the Runtime Apply worker should finish")
        .expect_err("shutdown should interrupt Runtime Apply before commit");
    assert_eq!(error.kind, ConfigTransactionErrorKind::Shutdown);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Pending {
            target: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert!(harness.has_prepared());
    assert!(!harness.events().iter().any(|event| event == "commit"));
}

#[test]
fn tun_preflight_failure_retains_its_transaction_category() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.runtime.script([ApplyScript::TunPermissionDenied]);

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the TUN preflight should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::TunPermissionDenied);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Converged {
            generation: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn unsupported_tun_retains_its_transaction_category() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.runtime.script([ApplyScript::TunUnsupported]);

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the unsupported TUN platform should reject the candidate");

    assert_eq!(error.kind, ConfigTransactionErrorKind::TunUnsupported);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Converged {
            generation: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn indeterminate_apply_restarts_and_confirms_the_candidate_before_commit() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness
        .runtime
        .script([ApplyScript::IndeterminateFailure, ApplyScript::Success]);

    let result = harness
        .coordinator
        .execute(&candidate)
        .expect("candidate restart should confirm the transaction");

    assert_eq!(result.apply_path, ApplyPath::CandidateRestart);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(2)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(2)));
    assert_event_order(
        &harness.events(),
        &["apply:2", "stop", "apply:2", "status", "health:2", "commit"],
    );
}

#[test]
fn failed_candidate_restart_restores_the_previous_generation() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.runtime.script([
        ApplyScript::IndeterminateFailure,
        ApplyScript::DefiniteFailure,
        ApplyScript::Success,
    ]);

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the failed candidate restart should roll back");

    assert_eq!(error.kind, ConfigTransactionErrorKind::IndeterminateApply);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Converged {
            generation: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
    assert_event_order(
        &harness.events(),
        &[
            "apply:2", "stop", "apply:2", "stop", "apply:1", "status", "health:1", "clear",
        ],
    );
}

#[test]
fn health_failure_rolls_back_the_candidate() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.health.fail_next();

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("candidate health should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Health);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn mismatched_core_identity_blocks_commit_and_rolls_back() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.runtime.mismatch_status_once();

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the mismatched process identity should fail confirmation");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Health);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
}

#[test]
fn mismatched_core_endpoint_blocks_commit_and_rolls_back() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.runtime.mismatch_endpoint_once();

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the mismatched control endpoint should fail confirmation");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Health);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
}

#[test]
fn commit_failure_rolls_back_to_the_previous_pointer_and_core() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.store.fail_next(StoreFailure::CommitBeforeWrite);

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("the pointer update should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Commit);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn indeterminate_commit_acknowledgement_observes_the_durable_candidate_pointer() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.store.fail_next(StoreFailure::CommitAfterWrite);

    let result = harness
        .coordinator
        .execute(&candidate)
        .expect("the durable candidate should be reported as committed");

    assert_eq!(result.committed_generation, RuntimeGeneration(2));
    assert_eq!(
        result.recovery,
        RecoveryOutcome::Converged {
            generation: Some(RuntimeGeneration(2))
        }
    );
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(2)));
    assert!(!harness.has_prepared());
}

#[test]
fn cleanup_failure_leaves_a_recoverable_journal_after_commit() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.store.fail_next(StoreFailure::Clear);

    let result = harness
        .coordinator
        .execute(&candidate)
        .expect("the durable candidate should remain committed");

    assert_eq!(result.committed_generation, RuntimeGeneration(2));
    assert_eq!(
        result.recovery,
        RecoveryOutcome::Pending {
            target: Some(RuntimeGeneration(2))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(2)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(2)));
    assert!(harness.has_prepared());

    let recovered = harness
        .coordinator
        .recover_startup()
        .expect("startup recovery should clear the committed journal");
    assert_eq!(recovered.committed_generation, Some(RuntimeGeneration(2)));
    assert!(recovered.cleared_prepared_journal);
    assert!(!harness.has_prepared());
}

#[test]
fn the_next_transaction_retries_committed_journal_cleanup_online() {
    let harness = Harness::new();
    harness.commit_initial();
    harness.store.fail_next(StoreFailure::Clear);

    let pending = harness
        .coordinator
        .execute(&harness.candidate(2))
        .expect("the durable candidate should remain committed");
    assert_eq!(
        pending.recovery,
        RecoveryOutcome::Pending {
            target: Some(RuntimeGeneration(2))
        }
    );
    assert!(harness.has_prepared());

    let recovered = harness
        .coordinator
        .execute(&harness.candidate(3))
        .expect("the next transaction should clear the committed journal and proceed");

    assert_eq!(recovered.committed_generation, RuntimeGeneration(3));
    assert_eq!(recovered.recovery, RecoveryOutcome::NotRequired);
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(3)));
    assert!(!harness.has_prepared());
}

#[test]
fn prepare_acknowledgement_failure_clears_the_durable_journal() {
    let harness = Harness::new();
    harness.commit_initial();
    let candidate = harness.candidate(2);
    harness.store.fail_next(StoreFailure::PrepareAfterWrite);

    let error = harness
        .coordinator
        .execute(&candidate)
        .expect_err("journal preparation should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Prepare);
    assert_eq!(
        error.recovery,
        RecoveryOutcome::Converged {
            generation: Some(RuntimeGeneration(1))
        }
    );
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn startup_recovery_converges_an_interrupted_candidate_to_the_committed_pointer() {
    let harness = Harness::new();
    harness.commit_initial();
    let interrupted = harness.candidate(2);
    harness
        .persistence
        .prepare(&interrupted.transaction)
        .expect("the interrupted journal should be prepared");
    harness.runtime.force_generation(RuntimeGeneration(2));
    harness.store.fail_next(StoreFailure::Recover);

    let error = harness
        .coordinator
        .recover_startup()
        .expect_err("the injected recovery read should fail");
    assert_eq!(error.kind, ConfigTransactionErrorKind::Recovery);
    assert!(harness.has_prepared());

    let recovered = harness
        .coordinator
        .recover_startup()
        .expect("the next startup recovery should converge");

    assert_eq!(recovered.committed_generation, Some(RuntimeGeneration(1)));
    assert!(recovered.cleared_prepared_journal);
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(1)));
    assert!(!harness.has_prepared());
}

#[test]
fn startup_reapply_discards_stale_prepared_state_without_replaying_an_old_runtime() {
    let harness = Harness::new();
    harness.commit_initial();
    let interrupted = harness.candidate(2);
    harness
        .persistence
        .prepare(&interrupted.transaction)
        .expect("the interrupted journal should be prepared");
    harness.runtime.force_generation(RuntimeGeneration(2));

    let recovery = harness
        .coordinator
        .prepare_startup_reapply(Some(RuntimeGeneration(1)))
        .expect("startup should prepare a fresh session-specific transaction");

    assert_eq!(recovery.committed_generation, Some(RuntimeGeneration(1)));
    assert_eq!(recovery.candidate_generation, RuntimeGeneration(3));
    assert!(recovery.cleared_prepared_journal);
    assert_eq!(harness.runtime.generation(), Some(RuntimeGeneration(2)));
    assert!(!harness.has_prepared());

    let current_session = harness.candidate(3);
    harness
        .coordinator
        .execute(&current_session)
        .expect("the fresh session transaction should allow a skipped generation");
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(3)));
}

#[test]
fn failed_startup_reapply_stops_the_candidate_without_replaying_the_old_endpoint() {
    let harness = Harness::new();
    harness.commit_initial();
    let current_session = harness.candidate(2);
    harness.runtime.script([ApplyScript::DefiniteFailure]);

    let error = harness
        .coordinator
        .execute_startup_reapply(&current_session)
        .expect_err("the startup candidate should fail");

    assert_eq!(error.kind, ConfigTransactionErrorKind::Apply);
    assert_eq!(harness.committed_generation(), Some(RuntimeGeneration(1)));
    assert_eq!(harness.runtime.generation(), None);
    assert!(!harness.has_prepared());
    assert!(!harness.events().iter().any(|event| event == "apply:1"));
}

fn assert_event_order(events: &[String], expected: &[&str]) {
    let mut next = 0;
    for event in events {
        if expected.get(next).is_some_and(|expected| event == expected) {
            next += 1;
        }
    }
    assert_eq!(
        next,
        expected.len(),
        "expected ordered events {expected:?}, actual events {events:?}"
    );
}

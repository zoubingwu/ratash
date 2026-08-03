use ratash::application::{ApplicationClient, ApplicationOperation, ApplicationOutput, Clock};
use ratash::config::{
    AuthoritativeConfig, ConfigCompiler, CoreConfigValidator, CoreValidationError,
};
use ratash::constants::LATENCY_FRESHNESS;
use ratash::core::{
    Availability, CoreControlEndpoint, CoreRuntimeDiagnosticCategory, CoreRuntimeLifecycle,
    CoreRuntimeRestartStatus, CoreRuntimeStatus, CoreRuntimeTunReason, CoreRuntimeTunStatus,
    ManagedCoreHandle, MihomoError, NodeSelection, NodeSource, ProviderState, ProxyGroup,
    ProxyMember, ProxyNode, ProxyView, ProxyViewOrderSource,
};
use ratash::diagnostics::{WrapperDiagnosticCategory, WrapperDiagnosticState};
use ratash::domain::{
    ApplyState, CoreInstanceGeneration, NodeRecordId, ProxyGroupId, RuntimeApplyPhase,
    RuntimeGeneration, RuntimeRecoveryStatus, StreamState, SubscriptionUrl, SupervisorHealthReason,
};
use ratash::state::{AuthoritativeState, AuthoritativeStateStore};
use ratash::supervisor::{
    FetchedProfile, ProfileFetchError, ProfileFetchPort, Supervisor, SupervisorCorePort,
    SupervisorDependencies, SupervisorRuleTransactionReservation, SupervisorTransactionFailure,
    SupervisorTransactionPort, SupervisorTransactionRequest, TelemetryStream,
};
use ratash::transaction::{
    CandidateRevisions, ConfigTransactionSuccess, RecoveryOutcome as TransactionRecoveryOutcome,
};
use serde_yaml_ng::Value;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

#[path = "support/configuration.rs"]
mod configuration_support;
use configuration_support::{canonicalize_configuration, remove_v5_domain_recovery};

#[derive(Debug)]
struct FixedClock;

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> u64 {
        10_000
    }
}

#[derive(Default)]
struct WorkGate {
    state: Mutex<WorkGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct WorkGateState {
    entered: bool,
    released: bool,
}

impl WorkGate {
    fn reset(&self) {
        let mut state = self.state.lock().expect("the work gate lock");
        *state = WorkGateState::default();
    }

    fn enter_and_wait(&self) {
        let mut state = self.state.lock().expect("the work gate lock");
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("the work gate wait");
        }
    }

    fn wait_until_entered(&self) {
        let mut state = self.state.lock().expect("the work gate lock");
        while !state.entered {
            state = self.changed.wait(state).expect("the work gate wait");
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("the work gate lock");
        state.released = true;
        self.changed.notify_all();
    }
}

struct MutableClock {
    now: AtomicU64,
}

impl MutableClock {
    fn new(now: u64) -> Self {
        Self {
            now: AtomicU64::new(now),
        }
    }

    fn set(&self, now: u64) {
        self.now.store(now, Ordering::Relaxed);
    }
}

impl Clock for MutableClock {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::Relaxed)
    }
}

struct UnusedProfileSource;

impl ProfileFetchPort for UnusedProfileSource {
    fn fetch(
        &self,
        _url: &ratash::domain::SubscriptionUrl,
    ) -> Result<FetchedProfile, ProfileFetchError> {
        panic!("the zero-Profile status path must not download a Profile")
    }
}

struct AcceptingValidator;

impl CoreConfigValidator for AcceptingValidator {
    fn validate(
        &self,
        _configuration: &ratash::config::EffectiveConfiguration,
        _staging_root: &std::path::Path,
    ) -> Result<(), CoreValidationError> {
        Ok(())
    }
}

struct ToggleValidator {
    fail_next: AtomicBool,
}

impl CoreConfigValidator for ToggleValidator {
    fn validate(
        &self,
        _configuration: &ratash::config::EffectiveConfiguration,
        _staging_root: &std::path::Path,
    ) -> Result<(), CoreValidationError> {
        if self.fail_next.swap(false, Ordering::Relaxed) {
            Err(CoreValidationError::new("injected validation failure"))
        } else {
            Ok(())
        }
    }
}

struct QueueProfileSource {
    results: Mutex<VecDeque<Result<FetchedProfile, ProfileFetchError>>>,
}

impl QueueProfileSource {
    fn push(&self, result: Result<FetchedProfile, ProfileFetchError>) {
        self.results
            .lock()
            .expect("the source lock should be available")
            .push_back(result);
    }
}

impl ProfileFetchPort for QueueProfileSource {
    fn fetch(&self, _url: &SubscriptionUrl) -> Result<FetchedProfile, ProfileFetchError> {
        self.results
            .lock()
            .expect("the source lock should be available")
            .pop_front()
            .expect("the test should provide a Profile download")
    }
}

struct BlockingProfileSource {
    entered: Mutex<bool>,
    entered_changed: Condvar,
    released: Mutex<bool>,
    released_changed: Condvar,
    result: Mutex<Option<Result<FetchedProfile, ProfileFetchError>>>,
}

impl BlockingProfileSource {
    fn new(result: Result<FetchedProfile, ProfileFetchError>) -> Self {
        Self {
            entered: Mutex::new(false),
            entered_changed: Condvar::new(),
            released: Mutex::new(false),
            released_changed: Condvar::new(),
            result: Mutex::new(Some(result)),
        }
    }

    fn wait_until_entered(&self) {
        let mut entered = self.entered.lock().expect("the entered lock");
        while !*entered {
            entered = self
                .entered_changed
                .wait(entered)
                .expect("the entered lock should remain available");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("the released lock") = true;
        self.released_changed.notify_all();
    }
}

impl ProfileFetchPort for BlockingProfileSource {
    fn fetch(&self, _url: &SubscriptionUrl) -> Result<FetchedProfile, ProfileFetchError> {
        *self.entered.lock().expect("the entered lock") = true;
        self.entered_changed.notify_all();
        let mut released = self.released.lock().expect("the released lock");
        while !*released {
            released = self
                .released_changed
                .wait(released)
                .expect("the released lock should remain available");
        }
        self.result
            .lock()
            .expect("the result lock")
            .take()
            .expect("the blocked source should have one result")
    }
}

struct UnusedTransactions;

impl SupervisorTransactionPort for UnusedTransactions {
    fn try_reserve_rule(
        &self,
    ) -> Result<Box<dyn SupervisorRuleTransactionReservation + '_>, SupervisorTransactionFailure>
    {
        panic!("the zero-Profile status path must not reserve a transaction")
    }

    fn apply(
        &self,
        _request: SupervisorTransactionRequest<'_>,
        _fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        panic!("the zero-Profile status path must not apply a transaction")
    }

    fn persist_metadata(
        &self,
        _request: SupervisorTransactionRequest<'_>,
    ) -> Result<(), SupervisorTransactionFailure> {
        panic!("the zero-Profile status path must not persist metadata")
    }

    fn set_current_revisions(&self, _revisions: CandidateRevisions) {}
}

struct UnusedCore;

impl SupervisorCorePort for UnusedCore {
    fn runtime_status(&self) -> Result<CoreRuntimeStatus, MihomoError> {
        panic!("the zero-Profile status path must not contact the Core")
    }

    fn proxy_view(
        &self,
        _core: &ManagedCoreHandle,
        _effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        panic!("the zero-Profile status path must not query proxies")
    }

    fn select_node(
        &self,
        _core: &ManagedCoreHandle,
        _selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        panic!("the zero-Profile status path must not select a Node")
    }
}

struct FakeCoreState {
    managed_core: Option<ManagedCoreHandle>,
    status_override: Option<CoreRuntimeStatus>,
    fail_status: bool,
    view: ProxyView,
    selections: Vec<NodeSelection>,
    fail_next_selection: bool,
    fail_selection_call: Option<usize>,
    fail_proxy_view_attempts: usize,
    next_instance_generation: u64,
}

struct FakeCore {
    state: Mutex<FakeCoreState>,
}

impl FakeCore {
    fn applied(&self, generation: RuntimeGeneration) {
        let mut state = self
            .state
            .lock()
            .expect("the Core lock should be available");
        state.next_instance_generation += 1;
        let instance_generation = state.next_instance_generation;
        state.managed_core = Some(ManagedCoreHandle {
            pid: 4_000 + u32::try_from(instance_generation).expect("generation should fit"),
            process_start_identity: format!("fixture-start-{instance_generation}"),
            endpoint: CoreControlEndpoint::new("/fixture/core.sock", "fixture-core-secret"),
            instance_generation: CoreInstanceGeneration(instance_generation),
            runtime_generation: generation,
        });
    }

    fn set_runtime_status(&self, status: CoreRuntimeStatus) {
        self.state
            .lock()
            .expect("the Core lock should be available")
            .status_override = Some(status);
    }

    fn fail_runtime_status(&self) {
        self.state
            .lock()
            .expect("the Core lock should be available")
            .fail_status = true;
    }
}

impl SupervisorCorePort for FakeCore {
    fn runtime_status(&self) -> Result<CoreRuntimeStatus, MihomoError> {
        let state = self
            .state
            .lock()
            .expect("the Core lock should be available");
        if state.fail_status {
            return Err(MihomoError::new(
                ratash::core::MihomoErrorKind::Unavailable,
                "injected runtime status failure",
            ));
        }
        Ok(state
            .status_override
            .clone()
            .unwrap_or_else(|| CoreRuntimeStatus::from_managed_core(state.managed_core.clone())))
    }

    fn proxy_view(
        &self,
        _core: &ManagedCoreHandle,
        _effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        let mut state = self
            .state
            .lock()
            .expect("the Core lock should be available");
        if state.fail_proxy_view_attempts > 0 {
            state.fail_proxy_view_attempts -= 1;
            return Err(MihomoError::new(
                ratash::core::MihomoErrorKind::Unavailable,
                "the fixture provider is warming up",
            ));
        }
        Ok(state.view.clone())
    }

    fn select_node(
        &self,
        _core: &ManagedCoreHandle,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        let mut state = self
            .state
            .lock()
            .expect("the Core lock should be available");
        state.selections.push(selection.clone());
        let call_index = state.selections.len();
        if state.fail_next_selection || state.fail_selection_call == Some(call_index) {
            state.fail_next_selection = false;
            state.fail_selection_call = None;
            return Err(MihomoError::new(
                ratash::core::MihomoErrorKind::SelectionRejected,
                "injected selection failure",
            ));
        }
        if let Some(group) = state
            .view
            .groups
            .iter_mut()
            .find(|group| group.name == selection.group_name)
        {
            group.selected_name = Some(selection.node_name.clone());
        }
        Ok(())
    }
}

struct PersistingTransactions {
    store: Arc<AuthoritativeStateStore>,
    core: Arc<FakeCore>,
    apply_count: AtomicU64,
    metadata_count: AtomicU64,
    fail_next_apply: AtomicBool,
    fail_next_validation: AtomicBool,
    fail_next_metadata: AtomicBool,
    busy_next_rule: AtomicBool,
    next_success_recovery: Mutex<Option<TransactionRecoveryOutcome>>,
    next_failure_recovery: Mutex<Option<TransactionRecoveryOutcome>>,
    block_next_apply: AtomicBool,
    apply_gate: WorkGate,
}

impl PersistingTransactions {
    fn block_next_apply(&self) {
        self.apply_gate.reset();
        self.block_next_apply.store(true, Ordering::Release);
    }

    fn wait_for_blocked_apply(&self) {
        self.apply_gate.wait_until_entered();
    }

    fn release_blocked_apply(&self) {
        self.apply_gate.release();
    }

    fn commit(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<(), SupervisorTransactionFailure> {
        let bundle = self
            .store
            .stage_candidate(AuthoritativeState {
                profiles: request.profiles,
                local_rules: request.local_rules,
                effective_configuration: request.configuration.yaml().as_bytes(),
                runtime_generation: request.generation,
            })
            .map_err(|_| {
                SupervisorTransactionFailure::new(
                    ratash::supervisor::SupervisorTransactionFailureKind::State,
                )
            })?;
        let prepared = self.store.persistence().prepare(&bundle).map_err(|_| {
            SupervisorTransactionFailure::new(
                ratash::supervisor::SupervisorTransactionFailureKind::State,
            )
        })?;
        self.store
            .persistence()
            .commit_prepared(&prepared)
            .map_err(|_| {
                SupervisorTransactionFailure::new(
                    ratash::supervisor::SupervisorTransactionFailureKind::State,
                )
            })?;
        self.store
            .persistence()
            .clear_prepared(&prepared)
            .map_err(|_| {
                SupervisorTransactionFailure::new(
                    ratash::supervisor::SupervisorTransactionFailureKind::State,
                )
            })
    }
}

struct PersistingRuleTransactionReservation<'a> {
    transactions: &'a PersistingTransactions,
}

impl SupervisorRuleTransactionReservation for PersistingRuleTransactionReservation<'_> {
    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        self.transactions.apply(request, false)
    }
}

impl SupervisorTransactionPort for PersistingTransactions {
    fn try_reserve_rule(
        &self,
    ) -> Result<Box<dyn SupervisorRuleTransactionReservation + '_>, SupervisorTransactionFailure>
    {
        if self.busy_next_rule.swap(false, Ordering::Relaxed) {
            return Err(SupervisorTransactionFailure::new(
                ratash::supervisor::SupervisorTransactionFailureKind::Busy,
            ));
        }
        Ok(Box::new(PersistingRuleTransactionReservation {
            transactions: self,
        }))
    }

    fn apply(
        &self,
        request: SupervisorTransactionRequest<'_>,
        fail_fast: bool,
    ) -> Result<ConfigTransactionSuccess, SupervisorTransactionFailure> {
        self.apply_count.fetch_add(1, Ordering::Relaxed);
        if fail_fast && self.busy_next_rule.swap(false, Ordering::Relaxed) {
            return Err(SupervisorTransactionFailure::new(
                ratash::supervisor::SupervisorTransactionFailureKind::Busy,
            ));
        }
        if self.fail_next_apply.swap(false, Ordering::Relaxed) {
            let mut failure = SupervisorTransactionFailure::new(
                ratash::supervisor::SupervisorTransactionFailureKind::Coordinator(
                    ratash::transaction::ConfigTransactionErrorKind::Apply,
                ),
            );
            failure.candidate_generation = Some(request.generation);
            failure.committed_generation = request
                .generation
                .0
                .checked_sub(1)
                .filter(|generation| *generation > 0)
                .map(RuntimeGeneration);
            failure.recovery = self
                .next_failure_recovery
                .lock()
                .expect("the failure recovery lock")
                .take()
                .unwrap_or(TransactionRecoveryOutcome::NotRequired);
            return Err(failure);
        }
        if self.fail_next_validation.swap(false, Ordering::Relaxed) {
            return Err(SupervisorTransactionFailure::new(
                ratash::supervisor::SupervisorTransactionFailureKind::Coordinator(
                    ratash::transaction::ConfigTransactionErrorKind::Validation,
                ),
            ));
        }
        if self.block_next_apply.swap(false, Ordering::AcqRel) {
            self.apply_gate.enter_and_wait();
        }
        let generation = request.generation;
        self.commit(request)?;
        self.core.applied(generation);
        let recovery = self
            .next_success_recovery
            .lock()
            .expect("the success recovery lock")
            .take()
            .unwrap_or(TransactionRecoveryOutcome::NotRequired);
        Ok(ConfigTransactionSuccess {
            candidate_generation: generation,
            committed_generation: generation,
            apply_path: ratash::transaction::ApplyPath::Direct,
            recovery,
        })
    }

    fn persist_metadata(
        &self,
        request: SupervisorTransactionRequest<'_>,
    ) -> Result<(), SupervisorTransactionFailure> {
        self.metadata_count.fetch_add(1, Ordering::Relaxed);
        if self.fail_next_metadata.swap(false, Ordering::Relaxed) {
            return Err(SupervisorTransactionFailure::new(
                ratash::supervisor::SupervisorTransactionFailureKind::State,
            ));
        }
        self.commit(request)
    }

    fn set_current_revisions(&self, _revisions: CandidateRevisions) {}
}

struct Harness {
    directory: TestDirectory,
    clock: Arc<MutableClock>,
    source: Arc<QueueProfileSource>,
    core: Arc<FakeCore>,
    transactions: Arc<PersistingTransactions>,
    state_store: Arc<AuthoritativeStateStore>,
    validator: Arc<ToggleValidator>,
}

impl Harness {
    fn new(label: &str) -> Self {
        let directory = TestDirectory::new(label);
        let state_store = Arc::new(
            AuthoritativeStateStore::open(directory.path.join("state"))
                .expect("the state store should open"),
        );
        let core = Arc::new(FakeCore {
            state: Mutex::new(FakeCoreState {
                managed_core: None,
                status_override: None,
                fail_status: false,
                view: fixture_proxy_view(),
                selections: Vec::new(),
                fail_next_selection: false,
                fail_selection_call: None,
                fail_proxy_view_attempts: 0,
                next_instance_generation: 0,
            }),
        });
        let transactions = Arc::new(PersistingTransactions {
            store: state_store.clone(),
            core: core.clone(),
            apply_count: AtomicU64::new(0),
            metadata_count: AtomicU64::new(0),
            fail_next_apply: AtomicBool::new(false),
            fail_next_validation: AtomicBool::new(false),
            fail_next_metadata: AtomicBool::new(false),
            busy_next_rule: AtomicBool::new(false),
            next_success_recovery: Mutex::new(None),
            next_failure_recovery: Mutex::new(None),
            block_next_apply: AtomicBool::new(false),
            apply_gate: WorkGate::default(),
        });
        Self {
            directory,
            clock: Arc::new(MutableClock::new(10_000)),
            source: Arc::new(QueueProfileSource {
                results: Mutex::new(VecDeque::new()),
            }),
            core,
            transactions,
            state_store,
            validator: Arc::new(ToggleValidator {
                fail_next: AtomicBool::new(false),
            }),
        }
    }

    fn open(&self) -> Supervisor {
        self.open_for_session(
            self.source.clone(),
            self.directory.path.join("core.sock"),
            "fixture-secret",
        )
    }

    fn open_with_source(&self, source: Arc<dyn ProfileFetchPort>) -> Supervisor {
        self.open_for_session(
            source,
            self.directory.path.join("core.sock"),
            "fixture-secret",
        )
    }

    fn open_for_session(
        &self,
        source: Arc<dyn ProfileFetchPort>,
        core_socket: PathBuf,
        core_secret: &str,
    ) -> Supervisor {
        Supervisor::open(SupervisorDependencies {
            clock: self.clock.clone(),
            source,
            compiler: ConfigCompiler::bundled().expect("the compiler should load"),
            validator: self.validator.clone(),
            transactions: self.transactions.clone(),
            state_store: self.state_store.clone(),
            core: self.core.clone(),
            authoritative: AuthoritativeConfig::new(core_socket.display().to_string(), core_secret),
            staging_root: self.directory.path.join("staging"),
        })
        .expect("the Supervisor should open")
    }

    fn queue_profile(&self, name: &str, node_name: &str) {
        self.source.push(Ok(FetchedProfile {
            body: fixture_profile(node_name),
            metadata_name: Some(name.to_owned()),
        }));
    }
}

fn fixture_profile(node_name: &str) -> Vec<u8> {
    format!(
        concat!(
            "proxies:\n",
            "  - name: {node_name}\n",
            "    type: ss\n",
            "    server: 127.0.0.1\n",
            "    port: 443\n",
            "    cipher: aes-128-gcm\n",
            "    password: fixture-password\n",
            "proxy-groups:\n",
            "  - name: Main\n",
            "    type: select\n",
            "    proxies: [{node_name}, DIRECT]\n",
            "rules:\n",
            "  - MATCH,Main\n"
        ),
        node_name = node_name,
    )
    .into_bytes()
}

fn fixture_proxy_view() -> ProxyView {
    let node_id = NodeRecordId::for_core("node-a");
    ProxyView {
        schema_version: 1,
        order_source: ProxyViewOrderSource::EffectiveConfiguration,
        provider_state: ProviderState::Ready,
        groups: vec![ProxyGroup {
            id: ProxyGroupId::for_name("Main"),
            name: "Main".to_owned(),
            proxy_type: "Selector".to_owned(),
            availability: Availability::Available,
            selectable: true,
            core_internal: false,
            selected_name: Some("node-a".to_owned()),
            members: vec![ProxyMember::Node {
                name: "node-a".to_owned(),
                record_id: node_id.clone(),
                availability: Availability::Available,
            }],
        }],
        nodes: BTreeMap::from([(
            node_id.clone(),
            ProxyNode {
                record_id: node_id,
                name: "node-a".to_owned(),
                proxy_type: "Shadowsocks".to_owned(),
                availability: Availability::Available,
                core_internal: false,
                source: NodeSource::Core {
                    proxy_name: "node-a".to_owned(),
                },
            },
        )]),
        providers: Vec::new(),
    }
}

fn two_node_proxy_view() -> ProxyView {
    let node_a = NodeRecordId::for_core("node-a");
    let node_b = NodeRecordId::for_core("node-b");
    let nodes = [
        (
            node_a.clone(),
            ProxyNode {
                record_id: node_a.clone(),
                name: "node-a".to_owned(),
                proxy_type: "Shadowsocks".to_owned(),
                availability: Availability::Available,
                core_internal: false,
                source: NodeSource::Core {
                    proxy_name: "node-a".to_owned(),
                },
            },
        ),
        (
            node_b.clone(),
            ProxyNode {
                record_id: node_b.clone(),
                name: "node-b".to_owned(),
                proxy_type: "Shadowsocks".to_owned(),
                availability: Availability::Available,
                core_internal: false,
                source: NodeSource::Core {
                    proxy_name: "node-b".to_owned(),
                },
            },
        ),
    ];
    ProxyView {
        schema_version: 1,
        order_source: ProxyViewOrderSource::EffectiveConfiguration,
        provider_state: ProviderState::Ready,
        groups: vec![ProxyGroup {
            id: ProxyGroupId::for_name("Main"),
            name: "Main".to_owned(),
            proxy_type: "Selector".to_owned(),
            availability: Availability::Available,
            selectable: true,
            core_internal: false,
            selected_name: Some("node-a".to_owned()),
            members: vec![
                ProxyMember::Node {
                    name: "node-a".to_owned(),
                    record_id: node_a,
                    availability: Availability::Available,
                },
                ProxyMember::Node {
                    name: "node-b".to_owned(),
                    record_id: node_b,
                    availability: Availability::Available,
                },
            ],
        }],
        nodes: BTreeMap::from(nodes),
        providers: Vec::new(),
    }
}

fn oversized_proxy_view() -> ProxyView {
    let nodes = (0..=ratash::constants::MAX_ACTIVE_NODES)
        .map(|index| {
            let name = format!("node-{index}");
            let record_id = NodeRecordId::for_core(&name);
            (
                record_id.clone(),
                ProxyNode {
                    record_id,
                    name: name.clone(),
                    proxy_type: "Shadowsocks".to_owned(),
                    availability: Availability::Available,
                    core_internal: false,
                    source: NodeSource::Core { proxy_name: name },
                },
            )
        })
        .collect();
    ProxyView {
        schema_version: 1,
        order_source: ProxyViewOrderSource::EffectiveConfiguration,
        provider_state: ProviderState::Ready,
        groups: vec![ProxyGroup {
            id: ProxyGroupId::for_name("Main"),
            name: "Main".to_owned(),
            proxy_type: "Selector".to_owned(),
            availability: Availability::Available,
            selectable: true,
            core_internal: false,
            selected_name: None,
            members: Vec::new(),
        }],
        nodes,
        providers: Vec::new(),
    }
}

fn duplicate_node_name_proxy_view() -> ProxyView {
    let first = NodeRecordId::for_provider("provider-a", "shared");
    let second = NodeRecordId::for_provider("provider-b", "shared");
    let node = |record_id: NodeRecordId, provider_name: &str| ProxyNode {
        record_id,
        name: "shared".to_owned(),
        proxy_type: "Shadowsocks".to_owned(),
        availability: Availability::Available,
        core_internal: false,
        source: NodeSource::Provider {
            provider_name: provider_name.to_owned(),
            proxy_name: "shared".to_owned(),
        },
    };
    ProxyView {
        schema_version: 1,
        order_source: ProxyViewOrderSource::EffectiveConfiguration,
        provider_state: ProviderState::Ready,
        groups: vec![ProxyGroup {
            id: ProxyGroupId::for_name("Main"),
            name: "Main".to_owned(),
            proxy_type: "Selector".to_owned(),
            availability: Availability::Available,
            selectable: true,
            core_internal: false,
            selected_name: Some("shared".to_owned()),
            members: vec![
                ProxyMember::Node {
                    name: "shared".to_owned(),
                    record_id: first.clone(),
                    availability: Availability::Available,
                },
                ProxyMember::Node {
                    name: "shared".to_owned(),
                    record_id: second.clone(),
                    availability: Availability::Available,
                },
            ],
        }],
        nodes: BTreeMap::from([
            (first.clone(), node(first, "provider-a")),
            (second.clone(), node(second, "provider-b")),
        ]),
        providers: Vec::new(),
    }
}

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new(label: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ratash-supervisor-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&path).expect("the test directory should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).expect("the test directory should be removed");
    }
}

fn directory_snapshot(root: &std::path::Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(
        root: &std::path::Path,
        directory: &std::path::Path,
        files: &mut BTreeMap<PathBuf, Vec<u8>>,
    ) {
        for entry in std::fs::read_dir(directory).expect("the fixture directory should be readable")
        {
            let entry = entry.expect("the fixture entry should be readable");
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root)
                        .expect("the fixture path should remain inside its root")
                        .to_path_buf(),
                    std::fs::read(path).expect("the fixture file should be readable"),
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

#[test]
fn zero_profile_supervisor_is_ready_without_contacting_the_core() {
    let directory = TestDirectory::new("empty");
    let state_store = Arc::new(
        AuthoritativeStateStore::open(directory.path.join("state"))
            .expect("the state store should open"),
    );
    let supervisor = Supervisor::open(SupervisorDependencies {
        clock: Arc::new(FixedClock),
        source: Arc::new(UnusedProfileSource),
        compiler: ConfigCompiler::bundled().expect("the compiler should load"),
        validator: Arc::new(AcceptingValidator),
        transactions: Arc::new(UnusedTransactions),
        state_store,
        core: Arc::new(UnusedCore),
        authoritative: AuthoritativeConfig::new(
            directory.path.join("core.sock").display().to_string(),
            "fixture-secret",
        ),
        staging_root: directory.path.join("staging"),
    })
    .expect("the Supervisor should open");

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should succeed")
    else {
        panic!("status should return a Status output")
    };

    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Ready
    );
    assert_eq!(
        status.core.lifecycle,
        ratash::domain::CoreLifecycle::Unconfigured
    );
    assert_eq!(
        status.tun.reason,
        Some(ratash::domain::TunReason::NoActiveProfile)
    );
    assert!(status.active_profile.is_none());
    assert!(status.runtime_generation.is_none());
    assert_eq!(status.apply_state, ApplyState::Idle);
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Idle);
    assert!(status.runtime_apply.candidate_generation.is_none());
    assert!(status.runtime_apply.committed_generation.is_none());
    assert_eq!(
        status.runtime_apply.recovery.status,
        RuntimeRecoveryStatus::NotRequired
    );
}

#[test]
fn status_projects_core_restart_degradation_and_tun_capability() {
    let harness = Harness::new("core-health-status");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    harness.core.set_runtime_status(CoreRuntimeStatus {
        managed_core: None,
        lifecycle: CoreRuntimeLifecycle::RestartPending,
        restart: CoreRuntimeRestartStatus {
            pending: true,
            attempts: 1,
            backoff: Some(Duration::from_secs(2)),
            diagnostic: None,
        },
        tun: CoreRuntimeTunStatus::available(),
    });
    let pending = get_status(&supervisor);
    assert_eq!(
        pending.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Ready
    );
    assert_eq!(
        pending.core.lifecycle,
        ratash::domain::CoreLifecycle::Starting
    );
    assert_eq!(
        pending.core.restart,
        ratash::domain::CoreRestartStatus {
            pending: true,
            attempts: 1,
            backoff_ms: Some(2_000),
            diagnostic: None,
        }
    );
    assert!(pending.tun.capable);
    assert!(!pending.tun.effective);
    assert_eq!(
        pending.tun.reason,
        Some(ratash::domain::TunReason::CoreUnavailable)
    );

    harness.core.set_runtime_status(CoreRuntimeStatus {
        managed_core: None,
        lifecycle: CoreRuntimeLifecycle::Degraded,
        restart: CoreRuntimeRestartStatus {
            pending: false,
            attempts: 3,
            backoff: None,
            diagnostic: Some(CoreRuntimeDiagnosticCategory::CoreRestartLimitReached),
        },
        tun: CoreRuntimeTunStatus::available(),
    });
    let degraded = get_status(&supervisor);
    assert_eq!(
        degraded.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert!(degraded.supervisor.health_reasons.is_empty());
    assert_eq!(
        degraded.core.lifecycle,
        ratash::domain::CoreLifecycle::Degraded
    );
    assert_eq!(
        degraded.core.restart.diagnostic,
        Some(ratash::domain::CoreDiagnosticCategory::RestartLimitReached)
    );

    let managed_core = harness
        .core
        .state
        .lock()
        .expect("the Core lock should be available")
        .managed_core
        .clone()
        .expect("the applied Core should be available");
    for (reason, expected) in [
        (
            CoreRuntimeTunReason::PermissionDenied,
            ratash::domain::TunReason::PermissionDenied,
        ),
        (
            CoreRuntimeTunReason::Unsupported,
            ratash::domain::TunReason::Unsupported,
        ),
    ] {
        harness.core.set_runtime_status(CoreRuntimeStatus {
            managed_core: Some(managed_core.clone()),
            lifecycle: CoreRuntimeLifecycle::Running,
            restart: CoreRuntimeRestartStatus::inactive(),
            tun: CoreRuntimeTunStatus {
                capable: false,
                reason: Some(reason),
            },
        });
        let status = get_status(&supervisor);
        assert_eq!(status.core.lifecycle, ratash::domain::CoreLifecycle::Ready);
        assert!(!status.tun.capable);
        assert!(!status.tun.effective);
        assert_eq!(status.tun.reason, Some(expected));
    }
}

#[test]
fn runtime_status_failure_is_publicly_degraded() {
    let harness = Harness::new("core-status-failure");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    harness.core.fail_runtime_status();

    let status = get_status(&supervisor);

    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert!(status.supervisor.health_reasons.is_empty());
    assert_eq!(
        status.core.lifecycle,
        ratash::domain::CoreLifecycle::Degraded
    );
    assert_eq!(
        status.core.restart,
        ratash::domain::CoreRestartStatus::default()
    );
    assert!(!status.tun.capable);
    assert!(!status.tun.effective);
    assert_eq!(
        status.tun.reason,
        Some(ratash::domain::TunReason::CoreUnavailable)
    );
}

#[test]
fn first_profile_add_commits_rules_runtime_probes_and_reopens_from_persistence() {
    let harness = Harness::new("first-profile");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();

    let ApplicationOutput::ProfileMutation(added) = supervisor
        .execute(ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/primary.yaml")
                .expect("the URL should be valid"),
        })
        .expect("the first Profile should be added")
    else {
        panic!("Profile add should return a mutation output")
    };

    assert_eq!(
        added.action,
        ratash::application::ProfileMutationAction::Added
    );
    assert!(added.profile.active);
    assert_eq!(
        added
            .runtime_apply
            .as_ref()
            .and_then(|apply| apply.committed_generation),
        Some(RuntimeGeneration(1))
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 1);

    let ApplicationOutput::Rules(rules) = supervisor
        .execute(ApplicationOperation::RuleList)
        .expect("rules should be available")
    else {
        panic!("Rule list should return Rules")
    };
    assert!(rules.initialized);
    assert_eq!(
        rules.revision,
        Some(ratash::domain::LocalRuleSetRevision(1))
    );
    assert_eq!(rules.rules[0].rule_string, "MATCH,Main");

    let ApplicationOutput::ProfilePage(profile_page) = supervisor
        .execute(ApplicationOperation::ProfileListPage { offset: 0 })
        .expect("the Profile page should be available")
    else {
        panic!("Profile page should return ProfilePage")
    };
    assert_eq!(profile_page.total, 1);
    assert_eq!(profile_page.profiles.len(), 1);

    let ApplicationOutput::RulePage(rule_page) = supervisor
        .execute(ApplicationOperation::RuleListPage { offset: 0 })
        .expect("the Rule page should be available")
    else {
        panic!("Rule page should return RulePage")
    };
    assert_eq!(rule_page.total, 1);
    assert_eq!(rule_page.rules.len(), 1);

    let ApplicationOutput::Latencies(latencies) = supervisor
        .execute(ApplicationOperation::LatencyList)
        .expect("latencies should be available")
    else {
        panic!("Latency list should return Latencies")
    };
    assert_eq!(latencies.samples.len(), 1);
    assert_eq!(latencies.samples[0].node_name, "node-a");
    assert_eq!(
        latencies.samples[0].probe_generation,
        ratash::domain::ProbeGeneration(1)
    );
    assert_eq!(
        latencies.samples[0].probe_status,
        ratash::application::LatencyProbeStatus::Queued
    );

    drop(supervisor);
    let reopened = harness.open();
    let ApplicationOutput::Profiles(profiles) = reopened
        .execute(ApplicationOperation::ProfileList)
        .expect("persisted Profiles should load")
    else {
        panic!("Profile list should return Profiles")
    };
    assert_eq!(profiles.profiles.len(), 1);
    assert_eq!(profiles.profiles[0].id, added.profile.id);
    assert!(profiles.profiles[0].active);
    let ApplicationOutput::Status(status) = reopened
        .execute(ApplicationOperation::GetStatus)
        .expect("the restarted status should load")
    else {
        panic!("status should return a Status output")
    };
    assert_eq!(status.runtime_generation, Some(RuntimeGeneration(2)));
    assert_eq!(status.apply_state, ApplyState::Idle);
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Succeeded);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 2);
}

#[test]
fn startup_apply_exposes_pending_runtime_recovery_health() {
    let harness = Harness::new("startup-recovery-health");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    drop(supervisor);
    *harness
        .transactions
        .next_success_recovery
        .lock()
        .expect("the success recovery lock") = Some(TransactionRecoveryOutcome::Pending {
        target: Some(RuntimeGeneration(1)),
    });

    let reopened = harness.open();
    let status = get_status(&reopened);

    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert_eq!(
        status.supervisor.health_reasons,
        [SupervisorHealthReason::RuntimeRecovery]
    );
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Recovering);
    let diagnostics = reopened
        .wrapper_diagnostic_tail(None)
        .expect("the startup diagnostic tail should remain available");
    assert_eq!(diagnostics.records.len(), 1);
    assert_eq!(
        diagnostics.records[0].category,
        WrapperDiagnosticCategory::RuntimeRecovery
    );
    assert_eq!(diagnostics.records[0].state, WrapperDiagnosticState::Raised);
    assert_eq!(diagnostics.records[0].timestamp_unix_ms, 10_000);
}

#[test]
fn profile_add_accepts_core_owned_fields_for_runtime_apply() {
    let harness = Harness::new("core-owned-profile-field");
    let mut body = fixture_profile("node-a");
    body.extend_from_slice(b"clash-for-android:\n  append-system-dns: true\n");
    harness.source.push(Ok(FetchedProfile {
        body,
        metadata_name: Some("Core Owned".to_owned()),
    }));
    let supervisor = harness.open();

    let ApplicationOutput::ProfileMutation(added) = supervisor
        .execute(ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse(
                "https://example.test/core-owned-profile.yaml",
            )
            .expect("the URL should be valid"),
        })
        .expect("the Core-owned Profile field should reach Runtime Apply")
    else {
        panic!("Profile add should return a mutation output")
    };

    assert_eq!(
        added.action,
        ratash::application::ProfileMutationAction::Added
    );
    assert!(added.profile.active);
    assert_eq!(
        added
            .runtime_apply
            .as_ref()
            .and_then(|apply| apply.committed_generation),
        Some(RuntimeGeneration(1))
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 1);
    assert_eq!(profile_list(&supervisor).len(), 1);
}

#[test]
fn status_publishes_probe_queue_overload_metrics() {
    let harness = Harness::new("probe-queue-status");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    let freshness_ms: u64 = LATENCY_FRESHNESS
        .as_millis()
        .try_into()
        .expect("the freshness threshold should fit");
    harness
        .clock
        .now
        .store(10_001_u64.saturating_add(freshness_ms), Ordering::Relaxed);

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should remain available under probe load")
    else {
        panic!("status should return Status")
    };

    assert_eq!(status.probe_queue.active_node_count, 1);
    assert_eq!(status.probe_queue.queue_depth, 1);
    assert_eq!(status.probe_queue.in_flight_count, 0);
    assert!(status.probe_queue.overloaded);
    assert_eq!(status.probe_queue.oldest_due_age_ms, Some(freshness_ms + 1));
    assert_eq!(status.probe_queue.stale_node_count, 1);
    assert_eq!(status.probe_queue.stale_ratio(), 1.0);
}

#[test]
fn startup_migrates_the_committed_v3_geo_policy_through_a_new_runtime_generation() {
    let harness = Harness::new("startup-v3-geo-policy");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    drop(supervisor);

    let limits = ratash::profile::SnapshotLimits::new(
        ratash::constants::PROFILE_RESPONSE_MAX_BYTES,
        ratash::constants::YAML_MAX_DEPTH,
    );
    let hydrated = harness
        .state_store
        .load_committed(limits, ratash::rule::RuleSetLimits::product())
        .expect("the current state should load")
        .expect("the current state should be committed");
    let active_profile_id = hydrated
        .profiles
        .active_profile_id()
        .expect("the Active Profile should remain selected");
    let mut legacy_configuration: Value =
        serde_yaml_ng::from_slice(&hydrated.effective_configuration)
            .expect("the Effective Configuration should parse");
    remove_v5_domain_recovery(&mut legacy_configuration);
    let legacy_mapping = legacy_configuration
        .as_mapping_mut()
        .expect("the Effective Configuration should be a mapping");
    legacy_mapping.remove("geo-auto-update");
    let legacy_configuration =
        serde_yaml_ng::to_string(&canonicalize_configuration(legacy_configuration))
            .expect("the legacy Effective Configuration should serialize");
    let legacy = harness
        .state_store
        .stage_candidate(AuthoritativeState {
            profiles: &hydrated.profiles,
            local_rules: &hydrated.local_rules,
            effective_configuration: legacy_configuration.as_bytes(),
            runtime_generation: hydrated.runtime_generation,
        })
        .expect("the v3 state should stage");
    let prepared = harness
        .state_store
        .persistence()
        .prepare(&legacy)
        .expect("the v3 state should prepare");
    harness
        .state_store
        .persistence()
        .commit_prepared(&prepared)
        .expect("the v3 state should commit");
    harness
        .state_store
        .persistence()
        .clear_prepared(&prepared)
        .expect("the v3 journal should clear");

    let restarted = harness.open();
    let migrated = harness
        .state_store
        .load_committed(limits, ratash::rule::RuleSetLimits::product())
        .expect("the migrated state should load")
        .expect("the migrated state should be committed");

    assert_eq!(
        migrated.profiles.active_profile_id(),
        Some(active_profile_id)
    );
    assert_eq!(migrated.local_rules, hydrated.local_rules);
    assert_eq!(migrated.runtime_generation, RuntimeGeneration(2));
    assert!(
        String::from_utf8(migrated.effective_configuration)
            .expect("the migrated Effective Configuration should be UTF-8")
            .contains("geo-auto-update: false")
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        get_status(&restarted).runtime_generation,
        Some(RuntimeGeneration(2))
    );
}

#[test]
fn startup_migrates_the_committed_v4_domain_policy_through_a_new_runtime_generation() {
    let harness = Harness::new("startup-v4-domain-policy");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    drop(supervisor);

    let limits = ratash::profile::SnapshotLimits::new(
        ratash::constants::PROFILE_RESPONSE_MAX_BYTES,
        ratash::constants::YAML_MAX_DEPTH,
    );
    let hydrated = harness
        .state_store
        .load_committed(limits, ratash::rule::RuleSetLimits::product())
        .expect("the current state should load")
        .expect("the current state should be committed");
    let mut legacy_configuration: Value =
        serde_yaml_ng::from_slice(&hydrated.effective_configuration)
            .expect("the Effective Configuration should parse");
    remove_v5_domain_recovery(&mut legacy_configuration);
    let legacy_configuration =
        serde_yaml_ng::to_string(&canonicalize_configuration(legacy_configuration))
            .expect("the legacy Effective Configuration should serialize");
    let legacy = harness
        .state_store
        .stage_candidate(AuthoritativeState {
            profiles: &hydrated.profiles,
            local_rules: &hydrated.local_rules,
            effective_configuration: legacy_configuration.as_bytes(),
            runtime_generation: hydrated.runtime_generation,
        })
        .expect("the v4 state should stage");
    let prepared = harness
        .state_store
        .persistence()
        .prepare(&legacy)
        .expect("the v4 state should prepare");
    harness
        .state_store
        .persistence()
        .commit_prepared(&prepared)
        .expect("the v4 state should commit");
    harness
        .state_store
        .persistence()
        .clear_prepared(&prepared)
        .expect("the v4 journal should clear");

    let restarted = harness.open();
    let migrated = harness
        .state_store
        .load_committed(limits, ratash::rule::RuleSetLimits::product())
        .expect("the migrated state should load")
        .expect("the migrated state should be committed");
    let configuration = String::from_utf8(migrated.effective_configuration)
        .expect("the migrated Effective Configuration should be UTF-8");

    assert_eq!(migrated.runtime_generation, RuntimeGeneration(2));
    assert!(configuration.contains("enhanced-mode: fake-ip"));
    assert!(configuration.contains("sniffer:"));
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        get_status(&restarted).runtime_generation,
        Some(RuntimeGeneration(2))
    );
}

#[test]
fn startup_migrates_the_committed_v5_sniffer_policy_through_a_new_runtime_generation() {
    let harness = Harness::new("startup-v5-sniffer-policy");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    drop(supervisor);

    let limits = ratash::profile::SnapshotLimits::new(
        ratash::constants::PROFILE_RESPONSE_MAX_BYTES,
        ratash::constants::YAML_MAX_DEPTH,
    );
    let hydrated = harness
        .state_store
        .load_committed(limits, ratash::rule::RuleSetLimits::product())
        .expect("the current state should load")
        .expect("the current state should be committed");
    let mut legacy_configuration: Value =
        serde_yaml_ng::from_slice(&hydrated.effective_configuration)
            .expect("the Effective Configuration should parse");
    legacy_configuration["sniffer"]
        .as_mapping_mut()
        .expect("sniffer should be a mapping")
        .insert("override-destination".into(), false.into());
    let legacy_configuration =
        serde_yaml_ng::to_string(&canonicalize_configuration(legacy_configuration))
            .expect("the legacy Effective Configuration should serialize");
    let legacy = harness
        .state_store
        .stage_candidate(AuthoritativeState {
            profiles: &hydrated.profiles,
            local_rules: &hydrated.local_rules,
            effective_configuration: legacy_configuration.as_bytes(),
            runtime_generation: hydrated.runtime_generation,
        })
        .expect("the v5 state should stage");
    let prepared = harness
        .state_store
        .persistence()
        .prepare(&legacy)
        .expect("the v5 state should prepare");
    harness
        .state_store
        .persistence()
        .commit_prepared(&prepared)
        .expect("the v5 state should commit");
    harness
        .state_store
        .persistence()
        .clear_prepared(&prepared)
        .expect("the v5 journal should clear");

    let restarted = harness.open();
    let migrated = harness
        .state_store
        .load_committed(limits, ratash::rule::RuleSetLimits::product())
        .expect("the migrated state should load")
        .expect("the migrated state should be committed");
    let configuration: Value = serde_yaml_ng::from_slice(&migrated.effective_configuration)
        .expect("the migrated Effective Configuration should parse");

    assert_eq!(migrated.runtime_generation, RuntimeGeneration(2));
    assert_eq!(
        configuration["sniffer"]["override-destination"],
        Value::Bool(true)
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        get_status(&restarted).runtime_generation,
        Some(RuntimeGeneration(2))
    );
}

#[test]
fn restart_recompiles_for_the_new_core_session_and_restores_runtime_state() {
    let harness = Harness::new("restart-session");
    harness.core.state.lock().expect("the Core lock").view = two_node_proxy_view();
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-b").as_str().to_owned(),
        })
        .expect("the saved selection should be committed");
    drop(supervisor);
    {
        let mut core = harness.core.state.lock().expect("the Core lock");
        core.selections.clear();
        core.view.groups[0].selected_name = Some("node-a".to_owned());
    }

    let restarted = harness.open_for_session(
        harness.source.clone(),
        harness.directory.path.join("rotated-core.sock"),
        "rotated-session-secret",
    );

    let hydrated = harness
        .state_store
        .load_committed(
            ratash::profile::SnapshotLimits::new(
                ratash::constants::PROFILE_RESPONSE_MAX_BYTES,
                ratash::constants::YAML_MAX_DEPTH,
            ),
            ratash::rule::RuleSetLimits::product(),
        )
        .expect("the restarted state should load")
        .expect("the restarted state should remain committed");
    let persisted = String::from_utf8(hydrated.effective_configuration)
        .expect("the Effective Configuration should be UTF-8");
    assert!(persisted.contains("rotated-core.sock"));
    assert!(persisted.contains("rotated-session-secret"));
    assert_eq!(hydrated.runtime_generation, RuntimeGeneration(2));
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 2);
    assert_eq!(
        harness
            .core
            .state
            .lock()
            .expect("the Core lock")
            .selections
            .last()
            .expect("the selection should be restored")
            .node_name,
        "node-b"
    );
    assert_eq!(
        restarted
            .take_due_probes()
            .expect("the restarted probe queue should be readable")
            .len(),
        2
    );
}

#[test]
fn failed_first_profile_apply_preserves_the_zero_profile_state() {
    let harness = Harness::new("failed-first-profile");
    harness.queue_profile("Primary", "node-a");
    harness
        .transactions
        .fail_next_apply
        .store(true, Ordering::Relaxed);
    let supervisor = harness.open();

    let error = supervisor
        .execute(ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/primary.yaml")
                .expect("the URL should be valid"),
        })
        .expect_err("the injected Runtime Apply should fail");
    assert_eq!(
        error.code,
        ratash::error::ErrorCode::ExternalOperationFailed
    );
    assert_eq!(
        error.details,
        Some(
            ratash::application::ApplicationErrorDetails::RuntimeApplyFailure(Box::new(
                ratash::application::RuntimeApplyFailureDetails {
                    candidate_generation: Some(RuntimeGeneration(1)),
                    committed_generation: None,
                    stage: ratash::application::RuntimeApplyFailureStage::Apply,
                    recovery: ratash::application::RecoveryOutcome {
                        status: ratash::application::RecoveryStatus::NotRequired,
                        restored_generation: None,
                        message: None,
                    },
                },
            ))
        )
    );

    let ApplicationOutput::Profiles(profiles) = supervisor
        .execute(ApplicationOperation::ProfileList)
        .expect("Profile list should remain available")
    else {
        panic!("Profile list should return Profiles")
    };
    assert!(profiles.profiles.is_empty());

    let ApplicationOutput::Rules(rules) = supervisor
        .execute(ApplicationOperation::RuleList)
        .expect("Rule list should remain available")
    else {
        panic!("Rule list should return Rules")
    };
    assert!(!rules.initialized);

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should remain available")
    else {
        panic!("status should return Status")
    };
    assert_eq!(
        status.core.lifecycle,
        ratash::domain::CoreLifecycle::Unconfigured
    );
    assert!(status.runtime_generation.is_none());
    assert_eq!(status.apply_state, ApplyState::Failed);
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Failed);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(1))
    );
    assert!(status.runtime_apply.committed_generation.is_none());
    assert_eq!(
        status.runtime_apply.recovery.status,
        RuntimeRecoveryStatus::NotRequired
    );
}

#[test]
fn committed_apply_with_pending_cleanup_swaps_state_and_reports_degraded_recovery() {
    let harness = Harness::new("pending-commit-cleanup");
    harness.queue_profile("Primary", "node-a");
    *harness
        .transactions
        .next_success_recovery
        .lock()
        .expect("the success recovery lock") = Some(TransactionRecoveryOutcome::Pending {
        target: Some(RuntimeGeneration(1)),
    });
    let supervisor = harness.open();

    let ApplicationOutput::ProfileMutation(added) = supervisor
        .execute(ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/primary.yaml")
                .expect("the URL should be valid"),
        })
        .expect("the durably committed Profile should be adopted")
    else {
        panic!("Profile add should return a mutation output")
    };
    let apply = added
        .runtime_apply
        .expect("the first Profile should report Runtime Apply");
    assert_eq!(
        apply.status,
        ratash::application::RuntimeApplyStatus::Applied
    );
    assert_eq!(
        apply.recovery.status,
        ratash::application::RecoveryStatus::Pending
    );
    assert_eq!(
        apply.recovery.restored_generation,
        Some(RuntimeGeneration(1))
    );

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should remain available")
    else {
        panic!("status should return Status")
    };
    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert_eq!(
        status.supervisor.health_reasons,
        [SupervisorHealthReason::RuntimeRecovery]
    );
    assert_eq!(status.runtime_generation, Some(RuntimeGeneration(1)));
    assert_eq!(status.apply_state, ApplyState::Recovering);
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Recovering);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(1))
    );
    assert_eq!(
        status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(1))
    );
    assert_eq!(
        status.runtime_apply.recovery.status,
        RuntimeRecoveryStatus::Pending
    );
    assert_eq!(
        status.runtime_apply.recovery.restored_generation,
        Some(RuntimeGeneration(1))
    );
    assert_eq!(
        status.runtime_apply.recovery.message.as_deref(),
        Some("Committed Runtime Generation cleanup is pending")
    );
    assert!(added.profile.active);
}

#[test]
fn failed_runtime_recovery_marks_the_supervisor_degraded_and_retains_rules() {
    let harness = Harness::new("failed-runtime-recovery");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    harness
        .transactions
        .fail_next_apply
        .store(true, Ordering::Relaxed);
    *harness
        .transactions
        .next_failure_recovery
        .lock()
        .expect("the failure recovery lock") = Some(TransactionRecoveryOutcome::Failed {
        target: Some(RuntimeGeneration(1)),
    });

    supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: "DOMAIN,example.com,DIRECT".to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
        .expect_err("the injected recovery failure should fail the mutation");
    assert_eq!(rule_strings_from_application(&supervisor), ["MATCH,Main"]);

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should remain available")
    else {
        panic!("status should return Status")
    };
    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert_eq!(
        status.supervisor.health_reasons,
        [SupervisorHealthReason::RuntimeRecovery]
    );
    assert_eq!(status.core.lifecycle, ratash::domain::CoreLifecycle::Ready);
    assert!(status.tun.effective);
    assert_eq!(status.runtime_generation, Some(RuntimeGeneration(1)));
    assert_eq!(status.apply_state, ApplyState::Failed);
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Failed);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(1))
    );
    assert_eq!(
        status.runtime_apply.recovery.status,
        RuntimeRecoveryStatus::Failed
    );
    assert_eq!(
        status.runtime_apply.recovery.restored_generation,
        Some(RuntimeGeneration(1))
    );

    supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: "DOMAIN,example.org,DIRECT".to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
        .expect("a later Runtime Apply should succeed");
    let recovered_status = get_status(&supervisor);
    assert_eq!(recovered_status.apply_state, ApplyState::Idle);
    assert_eq!(
        recovered_status.runtime_apply.phase,
        RuntimeApplyPhase::Succeeded
    );
    assert_eq!(
        recovered_status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        recovered_status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        recovered_status.runtime_apply.recovery.status,
        RuntimeRecoveryStatus::NotRequired
    );
    assert_eq!(
        recovered_status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Ready
    );
    assert!(recovered_status.supervisor.health_reasons.is_empty());
}

#[test]
fn wrapper_diagnostics_record_health_transitions_once_without_untrusted_strings() {
    const SECRET_MARKER: &str = "private-subscription-token";

    let harness = Harness::new("wrapper-diagnostic-transitions");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(
        &supervisor,
        &format!("https://example.test/primary.yaml?token={SECRET_MARKER}"),
    );

    for timestamp in [11_000, 12_000] {
        harness.clock.set(timestamp);
        harness
            .transactions
            .fail_next_apply
            .store(true, Ordering::Relaxed);
        *harness
            .transactions
            .next_failure_recovery
            .lock()
            .expect("the failure recovery lock") = Some(TransactionRecoveryOutcome::Failed {
            target: Some(RuntimeGeneration(1)),
        });
        supervisor
            .execute(ApplicationOperation::RuleAdd {
                rule: format!("DOMAIN,failed-{timestamp}.example,DIRECT"),
                placement: ratash::application::RulePlacement::Prepend,
            })
            .expect_err("the injected Runtime Recovery failure should fail the mutation");
    }

    harness.clock.set(13_000);
    supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: "DOMAIN,recovered.example,DIRECT".to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
        .expect("the later Runtime Apply should clear recovery health");
    harness.clock.set(14_000);
    supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: "DOMAIN,still-healthy.example,DIRECT".to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
        .expect("the healthy Runtime Apply should remain healthy");

    let diagnostics = supervisor
        .wrapper_diagnostic_tail(None)
        .expect("the diagnostic tail should remain available");
    assert_eq!(diagnostics.records.len(), 2);
    assert_eq!(
        diagnostics
            .records
            .iter()
            .map(|record| (record.timestamp_unix_ms, record.category, record.state))
            .collect::<Vec<_>>(),
        [
            (
                11_000,
                WrapperDiagnosticCategory::RuntimeRecovery,
                WrapperDiagnosticState::Raised,
            ),
            (
                13_000,
                WrapperDiagnosticCategory::RuntimeRecovery,
                WrapperDiagnosticState::Cleared,
            ),
        ]
    );
    assert!(!format!("{diagnostics:?}").contains(SECRET_MARKER));
}

#[test]
fn supervisor_wrapper_diagnostics_retain_the_bounded_latest_tail_and_report_a_gap() {
    let harness = Harness::new("bounded-wrapper-diagnostics");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let transitions = ratash::constants::WRAPPER_DIAGNOSTIC_CAPACITY + 2;
    for index in 0..(transitions / 2) {
        harness
            .transactions
            .fail_next_apply
            .store(true, Ordering::Relaxed);
        *harness
            .transactions
            .next_failure_recovery
            .lock()
            .expect("the failure recovery lock") = Some(TransactionRecoveryOutcome::Failed {
            target: Some(RuntimeGeneration(index as u64 + 1)),
        });
        let rule = format!("DOMAIN,diagnostic-{index}.example,DIRECT");
        supervisor
            .execute(ApplicationOperation::RuleAdd {
                rule: rule.clone(),
                placement: ratash::application::RulePlacement::Prepend,
            })
            .expect_err("the injected Runtime Recovery failure should raise health");
        supervisor
            .execute(ApplicationOperation::RuleAdd {
                rule,
                placement: ratash::application::RulePlacement::Prepend,
            })
            .expect("the succeeding Runtime Apply should clear health");
    }

    let diagnostics = supervisor
        .wrapper_diagnostic_tail(Some(0))
        .expect("the bounded diagnostic tail should remain available");
    assert_eq!(
        diagnostics.records.len(),
        ratash::constants::WRAPPER_DIAGNOSTIC_CAPACITY
    );
    assert_eq!(diagnostics.evicted_total, 2);
    assert!(diagnostics.gap);
    assert_eq!(diagnostics.earliest_sequence, Some(3));
    assert_eq!(diagnostics.latest_sequence, Some(transitions as u64));
}

#[test]
fn status_reports_applying_while_a_transaction_owns_authoritative_state() {
    let harness = Harness::new("observable-apply-state");
    harness.queue_profile("Primary", "node-a");
    let supervisor = Arc::new(harness.open());
    add_profile(supervisor.as_ref(), "https://example.test/primary.yaml");
    supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("the initial status should be cached");
    harness.transactions.block_next_apply();

    let mutation_supervisor = Arc::clone(&supervisor);
    let mutation = thread::spawn(move || {
        mutation_supervisor.execute(ApplicationOperation::RuleAdd {
            rule: "DOMAIN,example.com,DIRECT".to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
    });
    harness.transactions.wait_for_blocked_apply();

    let (status_sender, status_receiver) = mpsc::channel();
    let status_supervisor = Arc::clone(&supervisor);
    let status_thread = thread::spawn(move || {
        let _ = status_sender.send(status_supervisor.execute(ApplicationOperation::GetStatus));
    });
    let observed = status_receiver.recv_timeout(Duration::from_millis(500));
    harness.transactions.release_blocked_apply();
    mutation
        .join()
        .expect("the mutation thread should finish")
        .expect("the released mutation should commit");
    status_thread
        .join()
        .expect("the status thread should finish");
    let observed = observed
        .expect("status should remain responsive while Runtime Apply is blocked")
        .expect("status should remain available");

    let ApplicationOutput::Status(status) = observed else {
        panic!("status should return Status")
    };
    assert_eq!(status.apply_state, ApplyState::Applying);
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Applying);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(1))
    );
}

#[test]
fn oversized_active_node_set_deactivates_probes_and_marks_degraded_state() {
    let harness = Harness::new("oversized-probe-set");
    harness.queue_profile("Primary", "node-a");
    harness.core.state.lock().expect("the Core lock").view = oversized_proxy_view();
    let supervisor = harness.open();

    add_profile(&supervisor, "https://example.test/primary.yaml");
    assert!(
        supervisor
            .take_due_probes()
            .expect("the disabled Probe Scheduler should remain available")
            .is_empty()
    );
    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should remain available")
    else {
        panic!("status should return Status")
    };
    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert_eq!(
        status.supervisor.health_reasons,
        [SupervisorHealthReason::ProbeScheduler]
    );

    harness.core.state.lock().expect("the Core lock").view = fixture_proxy_view();
    supervisor
        .reconcile_runtime_state()
        .expect("a valid Node set should reseed probes");
    let recovered = get_status(&supervisor);
    assert_eq!(
        recovered.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Ready
    );
    assert!(recovered.supervisor.health_reasons.is_empty());
    assert_eq!(recovered.probe_queue.active_node_count, 1);
}

#[test]
fn activation_executes_one_current_and_one_latest_pending_target() {
    let harness = Harness::new("pending-activation");
    harness.queue_profile("Primary", "node-a");
    harness.queue_profile("Secondary", "node-b");
    harness.queue_profile("Tertiary", "node-c");
    let supervisor = Arc::new(harness.open());
    add_profile(supervisor.as_ref(), "https://example.test/primary.yaml");
    let secondary = add_profile(supervisor.as_ref(), "https://example.test/secondary.yaml");
    let tertiary = add_profile(supervisor.as_ref(), "https://example.test/tertiary.yaml");
    harness.transactions.block_next_apply();

    let current_supervisor = Arc::clone(&supervisor);
    let current_id = secondary.id.to_string();
    let current = thread::spawn(move || {
        current_supervisor.execute(ApplicationOperation::ProfileUse {
            profile: current_id,
        })
    });
    harness.transactions.wait_for_blocked_apply();

    let pending_supervisor = Arc::clone(&supervisor);
    let pending_id = tertiary.id.to_string();
    let pending = thread::spawn(move || {
        pending_supervisor.execute(ApplicationOperation::ProfileUse {
            profile: pending_id,
        })
    });
    thread::sleep(Duration::from_millis(20));
    harness.transactions.release_blocked_apply();
    current
        .join()
        .expect("the current activation thread should finish")
        .expect("the current activation should commit");
    pending
        .join()
        .expect("the pending activation thread should finish")
        .expect("the latest pending activation should commit");

    let active = profile_list(supervisor.as_ref())
        .into_iter()
        .find(|profile| profile.active)
        .expect("one Profile should remain active");
    assert_eq!(active.id, tertiary.id);
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 3);
}

#[test]
fn profile_activation_and_removal_swap_authority_only_after_commit() {
    let harness = Harness::new("profile-activation");
    harness.queue_profile("Primary", "node-a");
    harness.queue_profile("Secondary", "node-b");
    let supervisor = harness.open();
    let primary = add_profile(&supervisor, "https://example.test/primary.yaml");
    let secondary = add_profile(&supervisor, "https://example.test/secondary.yaml");
    assert!(primary.active);
    assert!(!secondary.active);
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 1);
    assert_eq!(
        harness.transactions.metadata_count.load(Ordering::Relaxed),
        1
    );

    let active_remove_error = supervisor
        .execute(ApplicationOperation::ProfileRemove {
            profile: primary.id.to_string(),
        })
        .expect_err("the Active Profile should be protected");
    assert_eq!(
        active_remove_error.code,
        ratash::error::ErrorCode::ProfileActive
    );

    harness
        .transactions
        .fail_next_apply
        .store(true, Ordering::Relaxed);
    supervisor
        .execute(ApplicationOperation::ProfileUse {
            profile: secondary.id.to_string(),
        })
        .expect_err("the injected activation should fail");
    let profiles = profile_list(&supervisor);
    assert!(
        profiles
            .iter()
            .find(|profile| profile.id == primary.id)
            .expect("the primary Profile should remain")
            .active
    );
    assert!(
        !profiles
            .iter()
            .find(|profile| profile.id == secondary.id)
            .expect("the secondary Profile should remain")
            .active
    );

    let ApplicationOutput::ProfileMutation(activated) = supervisor
        .execute(ApplicationOperation::ProfileUse {
            profile: secondary.id.to_string(),
        })
        .expect("the second activation should succeed")
    else {
        panic!("Profile use should return a mutation")
    };
    assert!(activated.profile.active);
    assert_eq!(
        activated
            .runtime_apply
            .and_then(|apply| apply.committed_generation),
        Some(RuntimeGeneration(2))
    );
    let activation_status = get_status(&supervisor);
    assert_eq!(
        activation_status.runtime_apply.phase,
        RuntimeApplyPhase::Succeeded
    );
    assert_eq!(
        activation_status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        activation_status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(2))
    );

    supervisor
        .execute(ApplicationOperation::ProfileRemove {
            profile: primary.id.to_string(),
        })
        .expect("the inactive Profile should be removed");
    let profiles = profile_list(&supervisor);
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].id, secondary.id);
    assert!(profiles[0].active);
}

#[test]
fn profile_activation_restores_its_saved_proxy_group_selection() {
    let harness = Harness::new("selection-restore");
    harness.core.state.lock().expect("the Core lock").view = two_node_proxy_view();
    harness.queue_profile("Primary", "node-a");
    harness.queue_profile("Secondary", "node-b");
    let supervisor = harness.open();
    let primary = add_profile(&supervisor, "https://example.test/primary.yaml");
    let secondary = add_profile(&supervisor, "https://example.test/secondary.yaml");

    supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-b").as_str().to_owned(),
        })
        .expect("the primary Profile selection should persist");
    supervisor
        .execute(ApplicationOperation::ProfileUse {
            profile: secondary.id.to_string(),
        })
        .expect("the secondary Profile should activate");
    {
        let mut core = harness.core.state.lock().expect("the Core lock");
        core.selections.clear();
        core.view.groups[0].selected_name = Some("node-a".to_owned());
    }

    supervisor
        .execute(ApplicationOperation::ProfileUse {
            profile: primary.id.to_string(),
        })
        .expect("the primary Profile should reactivate");
    assert_eq!(
        harness
            .core
            .state
            .lock()
            .expect("the Core lock")
            .selections
            .last()
            .expect("selection restoration should call the Core")
            .node_name,
        "node-b"
    );
    assert!(
        supervisor
            .retry_selection_restore()
            .expect("selection restoration state should be readable")
    );
}

#[test]
fn core_replacement_retries_provider_warmup_then_restores_selections_and_probes() {
    let harness = Harness::new("core-replacement-restore");
    harness.core.state.lock().expect("the Core lock").view = two_node_proxy_view();
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-b").as_str().to_owned(),
        })
        .expect("the selection should persist");
    let _ = supervisor
        .take_due_probes()
        .expect("the first Probe Generation should be readable");
    harness.core.applied(RuntimeGeneration(1));
    {
        let mut core = harness.core.state.lock().expect("the Core lock");
        core.selections.clear();
        core.view.groups[0].selected_name = Some("node-a".to_owned());
        core.fail_proxy_view_attempts = 2;
    }

    supervisor
        .reconcile_runtime_state()
        .expect("the first warm-up attempt should remain recoverable");
    supervisor
        .reconcile_runtime_state()
        .expect("the second warm-up attempt should remain recoverable");
    assert!(
        harness
            .core
            .state
            .lock()
            .expect("the Core lock")
            .selections
            .is_empty()
    );

    supervisor
        .reconcile_runtime_state()
        .expect("the ready provider view should reconcile");
    assert_eq!(
        harness
            .core
            .state
            .lock()
            .expect("the Core lock")
            .selections
            .last()
            .expect("the saved selection should be restored")
            .node_name,
        "node-b"
    );
    let probes = supervisor
        .take_due_probes()
        .expect("the replacement Probe Generation should be readable");
    assert_eq!(probes.len(), 2);
    assert_eq!(
        probes[0].task.generation,
        ratash::domain::ProbeGeneration(2)
    );
    assert!(
        supervisor
            .retry_selection_restore()
            .expect("selection restoration should be complete")
    );
}

#[test]
fn unresolved_selection_restoration_recovers_when_the_provider_view_stabilizes() {
    let harness = Harness::new("selection-restore-limit");
    harness.core.state.lock().expect("the Core lock").view = two_node_proxy_view();
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-b").as_str().to_owned(),
        })
        .expect("the selection should persist");
    harness.core.applied(RuntimeGeneration(1));
    harness.core.state.lock().expect("the Core lock").view = fixture_proxy_view();

    assert!(
        !supervisor
            .retry_selection_restore()
            .expect("the first restore attempt should remain pending")
    );
    let ApplicationOutput::Status(pending_status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("pending restoration status should remain available")
    else {
        panic!("status should return a Status output")
    };
    assert!(pending_status.selection_restore_pending);

    let mut complete = false;
    for _ in 1..ratash::constants::SELECTION_RESTORE_ATTEMPT_LIMIT {
        complete = supervisor
            .retry_selection_restore()
            .expect("a bounded restore attempt should complete");
    }
    assert!(!complete);

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("degraded status should remain available")
    else {
        panic!("status should return a Status output")
    };
    assert_eq!(
        status.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert_eq!(
        status.supervisor.health_reasons,
        [SupervisorHealthReason::SelectionRestoration]
    );
    assert!(!status.selection_restore_pending);

    harness.core.state.lock().expect("the Core lock").view = two_node_proxy_view();
    supervisor
        .reconcile_runtime_state()
        .expect("background reconciliation should restore the saved selection");
    let recovered = get_status(&supervisor);
    assert_eq!(
        recovered.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Ready
    );
    assert!(recovered.supervisor.health_reasons.is_empty());
}

#[test]
fn duplicate_profile_names_return_typed_candidates() {
    let harness = Harness::new("profile-ambiguity");
    harness.queue_profile("Shared", "node-a");
    harness.queue_profile("Shared", "node-b");
    let supervisor = harness.open();
    let first = add_profile(&supervisor, "https://one.example.test/profile.yaml");
    let second = add_profile(&supervisor, "https://two.example.test/profile.yaml");

    let error = supervisor
        .execute(ApplicationOperation::ProfileUse {
            profile: "Shared".to_owned(),
        })
        .expect_err("the duplicate display name should be ambiguous");
    assert_eq!(error.code, ratash::error::ErrorCode::ProfileAmbiguous);
    let candidates = error
        .selector_candidates
        .expect("ambiguity should include typed candidates");
    assert_eq!(
        candidates.selector,
        ratash::application::SelectorKind::Profile
    );
    assert_eq!(
        candidates
            .candidates
            .into_iter()
            .map(|candidate| candidate.id)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([first.id.to_string(), second.id.to_string()])
    );
}

#[test]
fn proxy_selection_persists_after_core_success_and_compensates_on_failure() {
    let harness = Harness::new("proxy-selection");
    harness.core.state.lock().expect("the Core lock").view = two_node_proxy_view();
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    let group_id = ProxyGroupId::for_name("Main");

    let ApplicationOutput::Proxies(proxies) = supervisor
        .execute(ApplicationOperation::ProxyList {
            group: group_id.as_str().to_owned(),
        })
        .expect("Proxy list by opaque group ID should succeed")
    else {
        panic!("Proxy list should return Proxies")
    };
    assert_eq!(proxies.group.id, group_id);
    assert_eq!(proxies.nodes.len(), 2);
    let node_b = proxies
        .nodes
        .iter()
        .find(|node| node.name == "node-b")
        .and_then(|node| node.id.clone())
        .expect("node-b should have an opaque ID");

    let ApplicationOutput::ProxySelection(selected) = supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: group_id.as_str().to_owned(),
            node: node_b.as_str().to_owned(),
        })
        .expect("selection by opaque group and Node IDs should succeed")
    else {
        panic!("Proxy select should return ProxySelection")
    };
    assert_eq!(selected.group_id, group_id);
    assert_eq!(selected.group, "Main");
    assert_eq!(selected.selected_node.id, node_b.as_str());
    assert_eq!(
        selected.previous_node.expect("previous selection").name,
        "node-a"
    );
    assert!(selected.persisted);

    harness
        .transactions
        .fail_next_metadata
        .store(true, Ordering::Relaxed);
    let error = supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-a").as_str().to_owned(),
        })
        .expect_err("the injected metadata commit should fail");
    assert_eq!(
        error.code,
        ratash::error::ErrorCode::ExternalOperationFailed
    );
    assert!(error.message.contains("restored"));
    let core = harness.core.state.lock().expect("the Core lock");
    assert_eq!(core.view.groups[0].selected_name.as_deref(), Some("node-b"));
    assert_eq!(
        core.selections
            .iter()
            .rev()
            .take(2)
            .map(|selection| selection.node_name.as_str())
            .collect::<Vec<_>>(),
        vec!["node-b", "node-a"]
    );
    drop(core);
    assert!(get_status(&supervisor).supervisor.health_reasons.is_empty());

    {
        let mut core = harness.core.state.lock().expect("the Core lock");
        core.fail_selection_call = Some(core.selections.len() + 2);
    }
    harness
        .transactions
        .fail_next_metadata
        .store(true, Ordering::Relaxed);
    supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-a").as_str().to_owned(),
        })
        .expect_err("failed persistence compensation should surface an error");
    let degraded = get_status(&supervisor);
    assert_eq!(
        degraded.supervisor.lifecycle,
        ratash::domain::SupervisorLifecycle::Degraded
    );
    assert_eq!(
        degraded.supervisor.health_reasons,
        [SupervisorHealthReason::SelectionCompensation]
    );

    supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: NodeRecordId::for_core("node-b").as_str().to_owned(),
        })
        .expect("a persisted selection should settle compensation health");
    assert!(get_status(&supervisor).supervisor.health_reasons.is_empty());
}

#[test]
fn latency_show_prefers_opaque_id_and_reports_name_ambiguity() {
    let harness = Harness::new("latency-ambiguity");
    harness.core.state.lock().expect("the Core lock").view = duplicate_node_name_proxy_view();
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let ApplicationOutput::Latencies(latencies) = supervisor
        .execute(ApplicationOperation::LatencyList)
        .expect("Latency list should succeed")
    else {
        panic!("Latency list should return Latencies")
    };
    assert_eq!(latencies.samples.len(), 2);
    assert!(
        latencies
            .samples
            .iter()
            .all(|sample| sample.probe_status == ratash::application::LatencyProbeStatus::Queued)
    );

    let target = latencies.samples[0].node_id.clone();
    let ApplicationOutput::Latency(shown) = supervisor
        .execute(ApplicationOperation::LatencyShow {
            node: target.as_str().to_owned(),
        })
        .expect("opaque ID lookup should succeed")
    else {
        panic!("Latency show should return Latency")
    };
    assert_eq!(shown.sample.node_id, target);

    let error = supervisor
        .execute(ApplicationOperation::LatencyShow {
            node: "shared".to_owned(),
        })
        .expect_err("the duplicate Node name should be ambiguous");
    assert_eq!(error.code, ratash::error::ErrorCode::NodeAmbiguous);
    let candidates = error
        .selector_candidates
        .expect("Node ambiguity should include candidates");
    assert_eq!(candidates.selector, ratash::application::SelectorKind::Node);
    assert_eq!(candidates.candidates.len(), 2);
}

#[test]
fn probe_surfaces_share_the_active_profile_node_boundary() {
    let harness = Harness::new("probe-node-boundary");
    let mut view = fixture_proxy_view();
    for name in ["DIRECT", "REJECT"] {
        let record_id = NodeRecordId::for_core(name);
        view.groups[0].members.push(ProxyMember::Node {
            name: name.to_owned(),
            record_id: record_id.clone(),
            availability: Availability::Available,
        });
        view.nodes.insert(
            record_id.clone(),
            ProxyNode {
                record_id,
                name: name.to_owned(),
                proxy_type: name.to_owned(),
                availability: Availability::Available,
                core_internal: true,
                source: NodeSource::Core {
                    proxy_name: name.to_owned(),
                },
            },
        );
    }
    harness.core.state.lock().expect("the Core lock").view = view;
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let probes = supervisor
        .take_due_probes()
        .expect("due probes should remain available");
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].task.node_id, NodeRecordId::for_core("node-a"));

    let ApplicationOutput::Latencies(latencies) = supervisor
        .execute(ApplicationOperation::LatencyList)
        .expect("Latency list should remain available")
    else {
        panic!("Latency list should return Latencies")
    };
    assert_eq!(latencies.samples.len(), 1);
    assert_eq!(latencies.samples[0].node_name, "node-a");

    for selector in [
        "DIRECT".to_owned(),
        NodeRecordId::for_core("REJECT").as_str().to_owned(),
    ] {
        let error = supervisor
            .execute(ApplicationOperation::LatencyShow { node: selector })
            .expect_err("Core-internal targets should stay outside Delay Probe surfaces");
        assert_eq!(error.code, ratash::error::ErrorCode::NodeNotFound);
    }
}

#[test]
fn proxy_and_rule_ambiguity_use_their_stable_error_codes() {
    let proxy_harness = Harness::new("proxy-ambiguity-code");
    proxy_harness.core.state.lock().expect("the Core lock").view = duplicate_node_name_proxy_view();
    proxy_harness.queue_profile("Primary", "node-a");
    let proxy_supervisor = proxy_harness.open();
    add_profile(
        &proxy_supervisor,
        "https://example.test/proxy-ambiguity.yaml",
    );

    let proxy_error = proxy_supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Main".to_owned(),
            node: "shared".to_owned(),
        })
        .expect_err("an ambiguous Node selector should fail");
    assert_eq!(proxy_error.code, ratash::error::ErrorCode::NodeAmbiguous);

    let rule_harness = Harness::new("rule-ambiguity-code");
    let duplicated_rules = String::from_utf8(fixture_profile("node-a"))
        .expect("the Profile fixture should be UTF-8")
        .replace("  - MATCH,Main\n", "  - MATCH,Main\n  - MATCH,Main\n");
    rule_harness.source.push(Ok(FetchedProfile {
        body: duplicated_rules.into_bytes(),
        metadata_name: Some("Primary".to_owned()),
    }));
    let rule_supervisor = rule_harness.open();
    add_profile(&rule_supervisor, "https://example.test/rule-ambiguity.yaml");

    let rule_error = rule_supervisor
        .execute(ApplicationOperation::RuleRemove {
            rule: "MATCH,Main".to_owned(),
        })
        .expect_err("an ambiguous Rule String should fail");
    assert_eq!(rule_error.code, ratash::error::ErrorCode::RuleAmbiguous);
}

#[test]
fn proxy_group_and_node_misses_have_selector_specific_codes() {
    let harness = Harness::new("selector-specific-misses");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let group_error = supervisor
        .execute(ApplicationOperation::ProxyList {
            group: "Missing Group".to_owned(),
        })
        .expect_err("the missing Proxy Group should fail");
    assert_eq!(
        group_error.code,
        ratash::error::ErrorCode::ProxyGroupNotFound
    );

    let node_error = supervisor
        .execute(ApplicationOperation::LatencyShow {
            node: "missing-node".to_owned(),
        })
        .expect_err("the missing Node should fail");
    assert_eq!(node_error.code, ratash::error::ErrorCode::NodeNotFound);
}

#[test]
fn proxy_rows_preserve_group_and_unresolved_member_states() {
    let harness = Harness::new("proxy-unresolved");
    let mut view = fixture_proxy_view();
    let candidates = vec![
        NodeRecordId::for_provider("provider-a", "shared"),
        NodeRecordId::for_provider("provider-b", "shared"),
    ];
    view.groups[0].members.extend([
        ProxyMember::Group {
            name: "Nested".to_owned(),
        },
        ProxyMember::Unresolved {
            name: "missing".to_owned(),
            reason: ratash::core::UnresolvedMemberReason::Missing,
            candidate_ids: Vec::new(),
        },
        ProxyMember::Unresolved {
            name: "shared".to_owned(),
            reason: ratash::core::UnresolvedMemberReason::Ambiguous,
            candidate_ids: candidates.clone(),
        },
        ProxyMember::Unresolved {
            name: "warming".to_owned(),
            reason: ratash::core::UnresolvedMemberReason::ProviderUnavailable,
            candidate_ids: Vec::new(),
        },
    ]);
    view.groups.push(ProxyGroup {
        id: ProxyGroupId::for_name("Nested"),
        name: "Nested".to_owned(),
        proxy_type: "Selector".to_owned(),
        availability: Availability::Available,
        selectable: true,
        core_internal: false,
        selected_name: None,
        members: Vec::new(),
    });
    view.groups.extend([
        ProxyGroup {
            id: ProxyGroupId::for_name("GLOBAL"),
            name: "GLOBAL".to_owned(),
            proxy_type: "Selector".to_owned(),
            availability: Availability::Available,
            selectable: true,
            core_internal: true,
            selected_name: None,
            members: Vec::new(),
        },
        ProxyGroup {
            id: ProxyGroupId::for_name("Fallback"),
            name: "Fallback".to_owned(),
            proxy_type: "Fallback".to_owned(),
            availability: Availability::Available,
            selectable: false,
            core_internal: false,
            selected_name: None,
            members: vec![ProxyMember::Node {
                name: "node-a".to_owned(),
                record_id: NodeRecordId::for_core("node-a"),
                availability: Availability::Available,
            }],
        },
    ]);
    harness.core.state.lock().expect("the Core lock").view = view;
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let ApplicationOutput::Proxies(proxies) = supervisor
        .execute(ApplicationOperation::ProxyList {
            group: "Main".to_owned(),
        })
        .expect("Proxy list should succeed")
    else {
        panic!("Proxy list should return Proxies")
    };
    let kinds = proxies
        .nodes
        .iter()
        .map(|row| (row.name.as_str(), row.member_kind))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        proxies
            .groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Main", "Nested", "GLOBAL", "Fallback"]
    );
    assert_eq!(kinds["Nested"], ratash::application::ProxyMemberKind::Group);
    assert_eq!(
        kinds["missing"],
        ratash::application::ProxyMemberKind::Missing
    );
    assert_eq!(
        kinds["shared"],
        ratash::application::ProxyMemberKind::Ambiguous
    );
    assert_eq!(
        kinds["warming"],
        ratash::application::ProxyMemberKind::ProviderUnavailable
    );
    let ambiguous = proxies
        .nodes
        .iter()
        .find(|row| row.name == "shared")
        .expect("the ambiguous row should remain");
    assert_eq!(ambiguous.candidate_ids, candidates);

    let ApplicationOutput::ProxyPage(page) = supervisor
        .execute(ApplicationOperation::ProxyListPage {
            group: "Main".to_owned(),
            groups_offset: 0,
            nodes_offset: 0,
        })
        .expect("Proxy page should succeed")
    else {
        panic!("Proxy page should return ProxyPage")
    };
    assert_eq!(page.groups_total, proxies.groups.len());
    assert_eq!(page.nodes_total, proxies.nodes.len());
    assert_eq!(page.groups, proxies.groups);
    assert_eq!(page.nodes, proxies.nodes);

    let selections_before = harness
        .core
        .state
        .lock()
        .expect("the Core lock")
        .selections
        .len();
    let error = supervisor
        .execute(ApplicationOperation::ProxySelect {
            group: "Fallback".to_owned(),
            node: NodeRecordId::for_core("node-a").as_str().to_owned(),
        })
        .expect_err("a non-selectable Proxy Group should reject selection");
    assert_eq!(error.code, ratash::error::ErrorCode::CoreUnavailable);
    assert_eq!(
        harness
            .core
            .state
            .lock()
            .expect("the Core lock")
            .selections
            .len(),
        selections_before
    );
    assert_eq!(
        get_status(&supervisor).primary_proxy_group.as_deref(),
        Some("Main")
    );
}

#[test]
fn rule_mutations_use_exact_strings_and_keep_authority_on_busy_or_apply_failure() {
    let harness = Harness::new("rule-mutations");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let added_rule = "DOMAIN,example.com,DIRECT";
    let ApplicationOutput::RuleMutation(added) = supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: added_rule.to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
        .expect("Rule add should succeed")
    else {
        panic!("Rule add should return RuleMutation")
    };
    assert_eq!(added.action, ratash::application::RuleMutationAction::Added);
    assert_eq!(added.resulting_position, Some(0));
    assert_eq!(
        added.runtime_apply.committed_generation,
        Some(RuntimeGeneration(2))
    );

    let replacement = "DOMAIN-SUFFIX,example.com,DIRECT";
    let ApplicationOutput::RuleMutation(replaced) = supervisor
        .execute(ApplicationOperation::RuleReplace {
            old_rule: added_rule.to_owned(),
            new_rule: replacement.to_owned(),
        })
        .expect("Rule replace should succeed")
    else {
        panic!("Rule replace should return RuleMutation")
    };
    assert_eq!(replaced.previous_rule.as_deref(), Some(added_rule));
    assert_eq!(replaced.changed_rule, replacement);
    assert_eq!(replaced.resulting_position, Some(0));

    let files_before = directory_snapshot(&harness.directory.path);
    let apply_count_before = harness.transactions.apply_count.load(Ordering::Relaxed);
    harness
        .transactions
        .busy_next_rule
        .store(true, Ordering::Relaxed);
    let busy = supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: "invalid".to_owned(),
            placement: ratash::application::RulePlacement::Prepend,
        })
        .expect_err("reservation should report busy before validating the Rule String");
    assert_eq!(busy.code, ratash::error::ErrorCode::RuleBusy);
    assert_eq!(
        harness.transactions.apply_count.load(Ordering::Relaxed),
        apply_count_before
    );
    assert_eq!(directory_snapshot(&harness.directory.path), files_before);
    assert_eq!(rule_strings_from_application(&supervisor)[0], replacement);

    harness
        .transactions
        .fail_next_apply
        .store(true, Ordering::Relaxed);
    supervisor
        .execute(ApplicationOperation::RuleRemove {
            rule: replacement.to_owned(),
        })
        .expect_err("the injected Runtime Apply should fail");
    let after_failure = rule_strings_from_application(&supervisor);
    assert_eq!(after_failure, vec![replacement, "MATCH,Main"]);

    let ApplicationOutput::RuleMutation(removed) = supervisor
        .execute(ApplicationOperation::RuleRemove {
            rule: replacement.to_owned(),
        })
        .expect("Rule remove should succeed")
    else {
        panic!("Rule remove should return RuleMutation")
    };
    assert_eq!(
        removed.action,
        ratash::application::RuleMutationAction::Removed
    );
    assert_eq!(removed.resulting_position, None);
    assert_eq!(
        rule_strings_from_application(&supervisor),
        vec!["MATCH,Main"]
    );

    let policy_error = supervisor
        .execute(ApplicationOperation::RuleAdd {
            rule: "DOMAIN,invalid.example,MissingPolicy".to_owned(),
            placement: ratash::application::RulePlacement::Append,
        })
        .expect_err("an unavailable Policy Target should fail validation");
    assert_eq!(
        policy_error.code,
        ratash::error::ErrorCode::PolicyTargetNotFound
    );
    assert_eq!(
        rule_strings_from_application(&supervisor),
        vec!["MATCH,Main"]
    );
}

#[test]
fn inactive_and_active_refreshes_follow_distinct_commit_paths_and_record_apply_failure() {
    let harness = Harness::new("profile-refresh");
    harness.queue_profile("Primary", "node-a");
    harness.queue_profile("Secondary", "node-b");
    let supervisor = harness.open();
    let primary = add_profile(&supervisor, "https://example.test/primary.yaml");
    let secondary = add_profile(&supervisor, "https://example.test/secondary.yaml");

    harness.clock.now.store(20_000, Ordering::Relaxed);
    harness.queue_profile("Secondary", "node-c");
    assert_eq!(
        supervisor
            .refresh_profile(secondary.id)
            .expect("the inactive refresh should succeed"),
        ratash::supervisor::ProfileRefreshDisposition::InactiveStored
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 1);
    let refreshed_secondary = profile_list(&supervisor)
        .into_iter()
        .find(|profile| profile.id == secondary.id)
        .expect("the secondary Profile should remain");
    assert_eq!(refreshed_secondary.last_success_at_unix_ms, 20_000);
    assert_eq!(
        refreshed_secondary.refresh_state,
        ratash::application::ProfileRefreshState::Fresh
    );

    harness.clock.now.store(25_000, Ordering::Relaxed);
    harness.queue_profile("Secondary", "node-c");
    harness.validator.fail_next.store(true, Ordering::Relaxed);
    supervisor
        .refresh_profile(secondary.id)
        .expect_err("the injected inactive validation should fail");
    let secondary_after_failure = profile_list(&supervisor)
        .into_iter()
        .find(|profile| profile.id == secondary.id)
        .expect("the secondary Profile should remain");
    assert_eq!(secondary_after_failure.last_success_at_unix_ms, 20_000);
    assert_eq!(
        secondary_after_failure
            .last_error
            .expect("the validation stage should be retained")
            .stage,
        ratash::application::ProfileRefreshStage::Validate
    );

    harness.clock.now.store(30_000, Ordering::Relaxed);
    harness.queue_profile("Primary", "node-a");
    assert_eq!(
        supervisor
            .refresh_profile(primary.id)
            .expect("the active refresh should succeed"),
        ratash::supervisor::ProfileRefreshDisposition::ActiveApplied
    );
    assert_eq!(harness.transactions.apply_count.load(Ordering::Relaxed), 2);
    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should succeed")
    else {
        panic!("status should return Status")
    };
    assert_eq!(status.runtime_generation, Some(RuntimeGeneration(2)));
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Succeeded);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(2))
    );
    assert_eq!(
        status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(2))
    );

    harness.clock.now.store(35_000, Ordering::Relaxed);
    harness.queue_profile("Primary", "node-a");
    harness
        .transactions
        .fail_next_validation
        .store(true, Ordering::Relaxed);
    supervisor
        .refresh_profile(primary.id)
        .expect_err("the injected active validation should fail");
    let primary_after_validation = profile_list(&supervisor)
        .into_iter()
        .find(|profile| profile.id == primary.id)
        .expect("the primary Profile should remain");
    assert_eq!(primary_after_validation.last_success_at_unix_ms, 30_000);
    assert_eq!(
        primary_after_validation
            .last_error
            .expect("the validation stage should be retained")
            .stage,
        ratash::application::ProfileRefreshStage::Validate
    );

    harness.clock.now.store(40_000, Ordering::Relaxed);
    harness.queue_profile("Primary", "node-a");
    harness
        .transactions
        .fail_next_apply
        .store(true, Ordering::Relaxed);
    supervisor
        .refresh_profile(primary.id)
        .expect_err("the injected active refresh apply should fail");
    let primary_after_failure = profile_list(&supervisor)
        .into_iter()
        .find(|profile| profile.id == primary.id)
        .expect("the primary Profile should remain");
    assert_eq!(primary_after_failure.last_success_at_unix_ms, 30_000);
    assert_eq!(
        primary_after_failure
            .last_error
            .expect("the failure stage should be retained")
            .stage,
        ratash::application::ProfileRefreshStage::Apply
    );
    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should succeed")
    else {
        panic!("status should return Status")
    };
    assert_eq!(status.runtime_generation, Some(RuntimeGeneration(2)));
    assert_eq!(status.runtime_apply.phase, RuntimeApplyPhase::Failed);
    assert_eq!(
        status.runtime_apply.candidate_generation,
        Some(RuntimeGeneration(3))
    );
    assert_eq!(
        status.runtime_apply.committed_generation,
        Some(RuntimeGeneration(2))
    );
}

#[test]
fn refresh_completion_discards_a_profile_removed_during_download() {
    let harness = Harness::new("stale-refresh");
    harness.queue_profile("Primary", "node-a");
    harness.queue_profile("Secondary", "node-b");
    let initial = harness.open();
    add_profile(&initial, "https://example.test/primary.yaml");
    let secondary = add_profile(&initial, "https://example.test/secondary.yaml");
    drop(initial);

    let blocked_source = Arc::new(BlockingProfileSource::new(Ok(FetchedProfile {
        body: fixture_profile("node-c"),
        metadata_name: Some("Secondary".to_owned()),
    })));
    let supervisor = Arc::new(harness.open_with_source(blocked_source.clone()));
    let refreshing = {
        let supervisor = supervisor.clone();
        std::thread::spawn(move || supervisor.refresh_profile(secondary.id))
    };
    blocked_source.wait_until_entered();
    supervisor
        .execute(ApplicationOperation::ProfileRemove {
            profile: secondary.id.to_string(),
        })
        .expect("the inactive Profile should be removed while download is in flight");
    blocked_source.release();

    assert_eq!(
        refreshing
            .join()
            .expect("the refresh thread should finish")
            .expect("stale completion should be handled"),
        ratash::supervisor::ProfileRefreshDisposition::Discarded
    );
    assert_eq!(profile_list(&supervisor).len(), 1);
}

#[test]
fn bounded_background_seams_drive_due_refresh_probe_completion_and_core_logs() {
    let harness = Harness::new("background-seams");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    let profile = add_profile(&supervisor, "https://example.test/primary.yaml");

    let probe_tasks = supervisor
        .take_due_probes()
        .expect("due probes should be available");
    assert_eq!(probe_tasks.len(), 1);
    let probe = probe_tasks[0].clone();
    assert!(probe.request.is_some());
    assert_eq!(
        supervisor
            .complete_probe(ratash::scheduler::ProbeCompletion {
                task: probe.task.clone(),
                outcome: ratash::scheduler::ProbeOutcome::Success { delay_ms: 37 },
                completed_at_unix_ms: 11_000,
            })
            .expect("probe completion should be accepted"),
        ratash::scheduler::ProbeCompletionStatus::Rescheduled {
            next_probe_at_unix_ms: 311_000,
        }
    );
    let ApplicationOutput::Latency(latency) = supervisor
        .execute(ApplicationOperation::LatencyShow {
            node: probe.task.node_id.as_str().to_owned(),
        })
        .expect("the completed sample should be visible")
    else {
        panic!("Latency show should return Latency")
    };
    assert_eq!(latency.sample.delay_ms, Some(37));

    let refresh_due_at = 10_000
        + u64::try_from(ratash::constants::PROFILE_REFRESH_INTERVAL.as_millis())
            .expect("the interval should fit");
    harness.clock.now.store(refresh_due_at, Ordering::Relaxed);
    let refresh_tasks = supervisor
        .take_due_refreshes()
        .expect("due refreshes should be available");
    assert_eq!(refresh_tasks.len(), 1);
    assert_eq!(refresh_tasks[0].profile_id, profile.id);
    harness.queue_profile("Primary", "node-a");
    assert_eq!(
        supervisor
            .execute_refresh_task(refresh_tasks[0])
            .expect("the due refresh should complete"),
        ratash::supervisor::ProfileRefreshDisposition::ActiveApplied
    );
    assert!(
        supervisor
            .take_due_refreshes()
            .expect("the refresh scheduler should remain available")
            .is_empty()
    );

    supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should initialize telemetry");
    let core_generation = harness
        .core
        .state
        .lock()
        .expect("the Core lock")
        .managed_core
        .as_ref()
        .expect("the Managed Core should exist")
        .instance_generation;
    assert!(
        supervisor
            .publish_core_log(
                core_generation,
                refresh_due_at,
                ratash::telemetry::LogLevel::Info,
                ratash::telemetry::LogSource::CoreApi,
                "fixture log",
            )
            .expect("Core Log publication should succeed")
    );
    let tail = supervisor
        .core_log_tail(None)
        .expect("the bounded Core Log tail should be available");
    assert_eq!(tail.records.len(), 1);
    assert_eq!(tail.records[0].message(), "fixture log");
}

#[test]
fn core_instance_generation_change_clears_latest_telemetry() {
    let harness = Harness::new("telemetry-generation");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");
    supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should initialize telemetry");
    let first_generation = harness
        .core
        .state
        .lock()
        .expect("the Core lock")
        .managed_core
        .as_ref()
        .expect("the Managed Core should exist")
        .instance_generation;
    assert!(
        supervisor
            .publish_traffic(
                first_generation,
                ratash::domain::TrafficSample {
                    upload_bytes_per_second: 5,
                    download_bytes_per_second: 8,
                    sampled_at_unix_ms: Some(11_000),
                    state: ratash::domain::SampleState::Fresh,
                },
            )
            .expect("traffic publication should succeed")
    );

    harness.core.applied(RuntimeGeneration(1));
    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should observe Core replacement")
    else {
        panic!("status should return Status")
    };
    assert_eq!(
        status.traffic.state,
        ratash::domain::SampleState::Unavailable
    );
    assert_eq!(status.traffic.upload_bytes_per_second, 0);
    assert!(
        !supervisor
            .publish_core_log(
                first_generation,
                12_000,
                ratash::telemetry::LogLevel::Info,
                ratash::telemetry::LogSource::CoreApi,
                "late log",
            )
            .expect("late generation should be rejected")
    );
}

#[test]
fn stream_health_is_projected_and_scoped_to_the_core_instance_generation() {
    let harness = Harness::new("stream-health-generation");
    harness.queue_profile("Primary", "node-a");
    let supervisor = harness.open();
    add_profile(&supervisor, "https://example.test/primary.yaml");

    let first_core = supervisor
        .managed_core()
        .expect("Managed Core status should be available")
        .expect("the Managed Core should exist");
    assert!(
        supervisor
            .set_stream_state(
                first_core.instance_generation,
                TelemetryStream::Traffic,
                StreamState::Healthy,
            )
            .expect("traffic health should be accepted")
    );
    assert!(
        supervisor
            .set_stream_state(
                first_core.instance_generation,
                TelemetryStream::Connections,
                StreamState::Stale,
            )
            .expect("connection health should be accepted")
    );
    assert!(
        supervisor
            .set_stream_state(
                first_core.instance_generation,
                TelemetryStream::Logs,
                StreamState::Connecting,
            )
            .expect("log health should be accepted")
    );

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should project stream health")
    else {
        panic!("status should return Status")
    };
    assert_eq!(status.stream_health.traffic, StreamState::Healthy);
    assert_eq!(status.stream_health.connections, StreamState::Stale);
    assert_eq!(status.stream_health.logs, StreamState::Connecting);

    harness.core.applied(RuntimeGeneration(1));
    let second_core = supervisor
        .managed_core()
        .expect("replacement Core status should be available")
        .expect("the replacement Managed Core should exist");
    assert_ne!(
        first_core.instance_generation,
        second_core.instance_generation
    );
    assert!(
        !supervisor
            .set_stream_state(
                first_core.instance_generation,
                TelemetryStream::Logs,
                StreamState::Healthy,
            )
            .expect("late stream health should be rejected")
    );

    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should project reset stream health")
    else {
        panic!("status should return Status")
    };
    assert_eq!(
        status.stream_health,
        ratash::domain::StreamHealthSet {
            traffic: StreamState::Disconnected,
            connections: StreamState::Disconnected,
            logs: StreamState::Disconnected,
        }
    );
}

fn rule_strings_from_application(supervisor: &Supervisor) -> Vec<String> {
    let ApplicationOutput::Rules(outcome) = supervisor
        .execute(ApplicationOperation::RuleList)
        .expect("Rule list should succeed")
    else {
        panic!("Rule list should return Rules")
    };
    outcome
        .rules
        .into_iter()
        .map(|rule| rule.rule_string)
        .collect()
}

fn get_status(supervisor: &Supervisor) -> ratash::domain::StatusSnapshot {
    let ApplicationOutput::Status(status) = supervisor
        .execute(ApplicationOperation::GetStatus)
        .expect("status should succeed")
    else {
        panic!("status should return Status")
    };
    status
}

fn add_profile(supervisor: &Supervisor, url: &str) -> ratash::application::ProfileSummary {
    let ApplicationOutput::ProfileMutation(outcome) = supervisor
        .execute(ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse(url).expect("the URL should be valid"),
        })
        .expect("the Profile should be added")
    else {
        panic!("Profile add should return a mutation")
    };
    outcome.profile
}

fn profile_list(supervisor: &Supervisor) -> Vec<ratash::application::ProfileSummary> {
    let ApplicationOutput::Profiles(outcome) = supervisor
        .execute(ApplicationOperation::ProfileList)
        .expect("Profile list should succeed")
    else {
        panic!("Profile list should return Profiles")
    };
    outcome.profiles
}

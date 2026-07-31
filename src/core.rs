use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::PathBuf;

use serde::Deserialize;

use crate::domain::{
    CoreInstanceGeneration, LatencySample, NodeRecordId, RuntimeGeneration, SampleState,
};

pub const PROXY_VIEW_SCHEMA_VERSION: u8 = 1;

// -----------------------------------------------------------------------------
// Privileged Core runtime boundary
// -----------------------------------------------------------------------------

#[derive(Clone, Eq, PartialEq)]
pub struct OwnerSessionRequest {
    pub owner_uid: u32,
    pub supervisor_pid: u32,
    pub supervisor_start_identity: String,
    pub instance_token: String,
    pub protocol_version: u16,
}

impl fmt::Debug for OwnerSessionRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionRequest")
            .field("owner_uid", &self.owner_uid)
            .field("supervisor_pid", &self.supervisor_pid)
            .field("supervisor_start_identity", &self.supervisor_start_identity)
            .field("instance_token", &"[REDACTED]")
            .field("protocol_version", &self.protocol_version)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct OwnerSessionProof {
    session_id: String,
    session_token: String,
}

impl OwnerSessionProof {
    #[must_use]
    pub fn new(session_id: impl Into<String>, session_token: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            session_token: session_token.into(),
        }
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    #[must_use]
    pub fn session_token(&self) -> &str {
        &self.session_token
    }
}

impl fmt::Debug for OwnerSessionProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OwnerSessionProof")
            .field("session_id", &self.session_id)
            .field("session_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnerSession {
    pub proof: OwnerSessionProof,
    pub protocol_version: u16,
    pub owner_generation: u64,
    pub endpoint: CoreControlEndpoint,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeBundle {
    pub generation: RuntimeGeneration,
    pub generation_root: PathBuf,
    pub manifest_sha256: String,
    pub compiler_policy_sha256: String,
    pub mihomo_binary_sha256: String,
}

#[derive(Clone, Eq, PartialEq)]
pub struct CoreControlEndpoint {
    pub socket_path: PathBuf,
    secret: String,
}

impl CoreControlEndpoint {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>, secret: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            secret: secret.into(),
        }
    }

    #[must_use]
    pub fn secret(&self) -> &str {
        &self.secret
    }
}

impl fmt::Debug for CoreControlEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreControlEndpoint")
            .field("socket_path", &self.socket_path)
            .field("secret", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedCoreHandle {
    pub pid: u32,
    pub process_start_identity: String,
    pub endpoint: CoreControlEndpoint,
    pub instance_generation: CoreInstanceGeneration,
    pub runtime_generation: RuntimeGeneration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyDisposition {
    Spawned,
    Reloaded,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyCandidateResult {
    pub disposition: ApplyDisposition,
    pub managed_core: ManagedCoreHandle,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreRuntimeStatus {
    pub managed_core: Option<ManagedCoreHandle>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopCoreResult {
    pub stopped: bool,
    pub instance_generation: Option<CoreInstanceGeneration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessOutputSource {
    Stdout,
    Stderr,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ForwardedCoreLog {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub source: ProcessOutputSource,
    pub message: String,
    pub instance_generation: CoreInstanceGeneration,
}

impl fmt::Debug for ForwardedCoreLog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForwardedCoreLog")
            .field("sequence", &self.sequence)
            .field("timestamp_unix_ms", &self.timestamp_unix_ms)
            .field("source", &self.source)
            .field("message_bytes", &self.message.len())
            .field("instance_generation", &self.instance_generation)
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForwardedCoreLogBatch {
    pub records: Vec<ForwardedCoreLog>,
    pub next_sequence: Option<u64>,
    pub dropped_before: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreRuntimeErrorKind {
    Authentication,
    ProtocolMismatch,
    TunPermissionDenied,
    InvalidBundle,
    ProcessIdentityMismatch,
    Apply,
    ReloadTimeout,
    Readiness,
    Unavailable,
}

#[derive(Clone, Eq, PartialEq)]
pub struct CoreRuntimeError {
    pub kind: CoreRuntimeErrorKind,
    diagnostic: String,
}

impl CoreRuntimeError {
    #[must_use]
    pub fn new(kind: CoreRuntimeErrorKind, diagnostic: impl Into<String>) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
        }
    }
}

impl fmt::Debug for CoreRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreRuntimeError")
            .field("kind", &self.kind)
            .field("diagnostic_bytes", &self.diagnostic.len())
            .finish()
    }
}

impl fmt::Display for CoreRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            CoreRuntimeErrorKind::Authentication => "Core runtime authentication failed",
            CoreRuntimeErrorKind::ProtocolMismatch => "Core runtime protocol mismatch",
            CoreRuntimeErrorKind::TunPermissionDenied => {
                "TUN capability is unavailable for the Managed Core"
            }
            CoreRuntimeErrorKind::InvalidBundle => "Core runtime bundle is invalid",
            CoreRuntimeErrorKind::ProcessIdentityMismatch => {
                "Managed Core process identity mismatch"
            }
            CoreRuntimeErrorKind::Apply => "Core Runtime Apply failed",
            CoreRuntimeErrorKind::ReloadTimeout => "Managed Core reload timed out",
            CoreRuntimeErrorKind::Readiness => "Managed Core readiness check failed",
            CoreRuntimeErrorKind::Unavailable => "Core runtime service is unavailable",
        })
    }
}

impl std::error::Error for CoreRuntimeError {}

pub trait CoreRuntime: Send + Sync {
    fn open_owner_session(
        &self,
        request: &OwnerSessionRequest,
    ) -> Result<OwnerSession, CoreRuntimeError>;

    fn apply_candidate(
        &self,
        owner: &OwnerSessionProof,
        bundle: &RuntimeBundle,
    ) -> Result<ApplyCandidateResult, CoreRuntimeError>;

    fn status(&self, owner: &OwnerSessionProof) -> Result<CoreRuntimeStatus, CoreRuntimeError>;

    fn logs(
        &self,
        owner: &OwnerSessionProof,
        after_sequence: Option<u64>,
        limit: usize,
    ) -> Result<ForwardedCoreLogBatch, CoreRuntimeError>;

    fn stop(&self, owner: &OwnerSessionProof) -> Result<StopCoreResult, CoreRuntimeError>;

    fn close_owner_session(&self, owner: &OwnerSessionProof) -> Result<(), CoreRuntimeError>;
}

// -----------------------------------------------------------------------------
// Mihomo projection
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Availability {
    Available,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProxyViewOrderSource {
    EffectiveConfiguration,
    StableFallback,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderState {
    Ready,
    Unavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeSource {
    Core {
        proxy_name: String,
    },
    Provider {
        provider_name: String,
        proxy_name: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnresolvedMemberReason {
    Missing,
    Ambiguous,
    ProviderUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProxyMember {
    Group {
        name: String,
    },
    Node {
        name: String,
        record_id: NodeRecordId,
        availability: Availability,
    },
    Unresolved {
        name: String,
        reason: UnresolvedMemberReason,
        candidate_ids: Vec<NodeRecordId>,
    },
}

impl ProxyMember {
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Group { name } | Self::Node { name, .. } | Self::Unresolved { name, .. } => name,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyGroup {
    pub name: String,
    pub proxy_type: String,
    pub availability: Availability,
    pub selectable: bool,
    pub core_internal: bool,
    pub selected_name: Option<String>,
    pub members: Vec<ProxyMember>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyNode {
    pub record_id: NodeRecordId,
    pub name: String,
    pub proxy_type: String,
    pub availability: Availability,
    pub core_internal: bool,
    pub source: NodeSource,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderVehicle {
    Http,
    File,
    Inline,
    Compatible,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyProvider {
    pub name: String,
    pub vehicle: ProviderVehicle,
    pub node_ids: Vec<NodeRecordId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyView {
    pub schema_version: u8,
    pub order_source: ProxyViewOrderSource,
    pub provider_state: ProviderState,
    pub groups: Vec<ProxyGroup>,
    pub nodes: BTreeMap<NodeRecordId, ProxyNode>,
    pub providers: Vec<ProxyProvider>,
}

impl ProxyView {
    #[must_use]
    pub fn primary_group(&self) -> Option<&ProxyGroup> {
        self.groups
            .iter()
            .find(|group| group.selectable && group.name != "GLOBAL")
    }

    pub fn resolve_exact_selection(
        &self,
        group_name: &str,
        node_name: &str,
    ) -> Result<NodeSelection, SelectionError> {
        let group = self
            .groups
            .iter()
            .find(|group| group.name == group_name)
            .ok_or_else(|| SelectionError::GroupMissing(group_name.to_owned()))?;
        if !group.selectable {
            return Err(SelectionError::GroupNotSelectable(group_name.to_owned()));
        }

        let mut candidates = BTreeSet::new();
        let mut unresolved = None;
        let mut saw_nested_group = false;
        for member in group
            .members
            .iter()
            .filter(|member| member.name() == node_name)
        {
            match member {
                ProxyMember::Node {
                    record_id,
                    availability,
                    ..
                } => {
                    if *availability == Availability::Unavailable {
                        return Err(SelectionError::NodeUnavailable(node_name.to_owned()));
                    }
                    candidates.insert(record_id.clone());
                }
                ProxyMember::Unresolved {
                    reason,
                    candidate_ids,
                    ..
                } => {
                    candidates.extend(candidate_ids.iter().cloned());
                    unresolved = Some(*reason);
                }
                ProxyMember::Group { .. } => saw_nested_group = true,
            }
        }

        if candidates.len() > 1 || unresolved == Some(UnresolvedMemberReason::Ambiguous) {
            return Err(SelectionError::NodeAmbiguous {
                name: node_name.to_owned(),
                candidate_ids: candidates.into_iter().collect(),
            });
        }
        if let Some(record_id) = candidates.into_iter().next() {
            return Ok(NodeSelection {
                group_name: group_name.to_owned(),
                node_name: node_name.to_owned(),
                record_id,
            });
        }
        if unresolved == Some(UnresolvedMemberReason::ProviderUnavailable) {
            return Err(SelectionError::ProviderUnavailable(node_name.to_owned()));
        }
        if saw_nested_group {
            return Err(SelectionError::TargetIsGroup(node_name.to_owned()));
        }
        Err(SelectionError::NodeMissing(node_name.to_owned()))
    }

    pub fn node_rows(
        &self,
        group_name: &str,
        probe_views: &BTreeMap<NodeRecordId, ProbeObservation>,
    ) -> Result<Vec<NodeRowV1>, SelectionError> {
        let group = self
            .groups
            .iter()
            .find(|group| group.name == group_name)
            .ok_or_else(|| SelectionError::GroupMissing(group_name.to_owned()))?;
        let mut rows = Vec::new();
        for member in &group.members {
            rows.push(self.node_row(group, member, probe_views));
        }
        Ok(rows)
    }

    fn node_row(
        &self,
        parent: &ProxyGroup,
        member: &ProxyMember,
        probe_views: &BTreeMap<NodeRecordId, ProbeObservation>,
    ) -> NodeRowV1 {
        let selected = parent.selected_name.as_deref() == Some(member.name());
        match member {
            ProxyMember::Node {
                name,
                record_id,
                availability,
            } => {
                let node = self.nodes.get(record_id);
                let observation = probe_views.get(record_id);
                let sample = observation.and_then(|observation| observation.sample.as_ref());
                NodeRowV1 {
                    schema_version: 1,
                    name: name.clone(),
                    member: node.map_or_else(
                        || NodeRowMemberV1::Unresolved {
                            reason: UnresolvedMemberReason::Missing,
                            candidate_ids: Vec::new(),
                        },
                        |node| NodeRowMemberV1::Node {
                            record_id: record_id.clone(),
                            source: node.source.clone(),
                        },
                    ),
                    proxy_type: node.map(|node| node.proxy_type.clone()),
                    availability: node.map_or(Availability::Unavailable, |_| *availability),
                    selected,
                    delay_ms: sample.and_then(|sample| sample.delay_ms),
                    sampled_at_unix_ms: sample.and_then(|sample| sample.sampled_at_unix_ms),
                    freshness: sample.map_or(LatencyFreshness::NotSampled, |sample| {
                        match sample.state {
                            SampleState::Fresh => LatencyFreshness::Fresh,
                            SampleState::Stale => LatencyFreshness::Stale,
                            SampleState::Unavailable => LatencyFreshness::Unavailable,
                        }
                    }),
                    probe_status: observation.map_or(ProbeStatus::NotSampled, |view| view.status),
                }
            }
            ProxyMember::Group { name } => {
                let nested = self.groups.iter().find(|group| group.name == *name);
                NodeRowV1 {
                    schema_version: 1,
                    name: name.clone(),
                    member: NodeRowMemberV1::Group,
                    proxy_type: nested.map(|group| group.proxy_type.clone()),
                    availability: nested
                        .map_or(Availability::Unavailable, |group| group.availability),
                    selected,
                    delay_ms: None,
                    sampled_at_unix_ms: None,
                    freshness: LatencyFreshness::NotSampled,
                    probe_status: ProbeStatus::NotSampled,
                }
            }
            ProxyMember::Unresolved {
                name,
                reason,
                candidate_ids,
            } => NodeRowV1 {
                schema_version: 1,
                name: name.clone(),
                member: NodeRowMemberV1::Unresolved {
                    reason: *reason,
                    candidate_ids: candidate_ids.clone(),
                },
                proxy_type: None,
                availability: Availability::Unavailable,
                selected,
                delay_ms: None,
                sampled_at_unix_ms: None,
                freshness: LatencyFreshness::Unavailable,
                probe_status: ProbeStatus::NotSampled,
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeSelection {
    pub group_name: String,
    pub node_name: String,
    pub record_id: NodeRecordId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SelectionError {
    GroupMissing(String),
    GroupNotSelectable(String),
    NodeMissing(String),
    NodeAmbiguous {
        name: String,
        candidate_ids: Vec<NodeRecordId>,
    },
    NodeUnavailable(String),
    ProviderUnavailable(String),
    TargetIsGroup(String),
}

impl fmt::Display for SelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GroupMissing(name) => write!(formatter, "Proxy Group '{name}' was not found"),
            Self::GroupNotSelectable(name) => {
                write!(formatter, "Proxy Group '{name}' is not selectable")
            }
            Self::NodeMissing(name) => write!(formatter, "Node '{name}' was not found"),
            Self::NodeAmbiguous { name, .. } => write!(formatter, "Node '{name}' is ambiguous"),
            Self::NodeUnavailable(name) => write!(formatter, "Node '{name}' is unavailable"),
            Self::ProviderUnavailable(name) => {
                write!(formatter, "provider data for Node '{name}' is unavailable")
            }
            Self::TargetIsGroup(name) => write!(formatter, "'{name}' identifies a Proxy Group"),
        }
    }
}

impl std::error::Error for SelectionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LatencyFreshness {
    NotSampled,
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeStatus {
    NotSampled,
    Queued,
    InFlight,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeObservation {
    pub sample: Option<LatencySample>,
    pub status: ProbeStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeRowV1 {
    pub schema_version: u8,
    pub name: String,
    pub member: NodeRowMemberV1,
    pub proxy_type: Option<String>,
    pub availability: Availability,
    pub selected: bool,
    pub delay_ms: Option<u64>,
    pub sampled_at_unix_ms: Option<u64>,
    pub freshness: LatencyFreshness,
    pub probe_status: ProbeStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeRowMemberV1 {
    Group,
    Node {
        record_id: NodeRecordId,
        source: NodeSource,
    },
    Unresolved {
        reason: UnresolvedMemberReason,
        candidate_ids: Vec<NodeRecordId>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionErrorKind {
    Proxies,
    Providers,
    Version,
    Delay,
    Traffic,
    Connections,
    Log,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProjectionError {
    pub kind: ProjectionErrorKind,
    diagnostic: String,
}

impl ProjectionError {
    fn invalid(kind: ProjectionErrorKind, diagnostic: impl Into<String>) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
        }
    }
}

impl fmt::Debug for ProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionError")
            .field("kind", &self.kind)
            .field("diagnostic_bytes", &self.diagnostic.len())
            .finish()
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid Mihomo {} response",
            match self.kind {
                ProjectionErrorKind::Proxies => "proxies",
                ProjectionErrorKind::Providers => "providers",
                ProjectionErrorKind::Version => "version",
                ProjectionErrorKind::Delay => "delay",
                ProjectionErrorKind::Traffic => "traffic",
                ProjectionErrorKind::Connections => "connections",
                ProjectionErrorKind::Log => "log",
            }
        )
    }
}

impl std::error::Error for ProjectionError {}

pub fn project_proxy_view(
    proxies_json: &[u8],
    providers_json: Option<&[u8]>,
    effective_group_order: &[String],
) -> Result<ProxyView, ProjectionError> {
    let proxies: RawProxies = serde_json::from_slice(proxies_json).map_err(|error| {
        ProjectionError::invalid(
            ProjectionErrorKind::Proxies,
            format!("invalid Mihomo proxies response: {error}"),
        )
    })?;
    let providers = providers_json
        .map(|json| {
            serde_json::from_slice::<RawProviders>(json).map_err(|error| {
                ProjectionError::invalid(
                    ProjectionErrorKind::Providers,
                    format!("invalid Mihomo providers response: {error}"),
                )
            })
        })
        .transpose()?;
    Ok(build_proxy_view(proxies, providers, effective_group_order))
}

fn build_proxy_view(
    proxies: RawProxies,
    providers: Option<RawProviders>,
    effective_group_order: &[String],
) -> ProxyView {
    let provider_state = if providers.is_some() {
        ProviderState::Ready
    } else {
        ProviderState::Unavailable
    };
    let mut nodes = BTreeMap::new();
    let mut provider_candidates: BTreeMap<String, BTreeSet<NodeRecordId>> = BTreeMap::new();
    let mut provider_views = Vec::new();

    for (provider_name, provider) in providers
        .map(|providers| providers.providers)
        .unwrap_or_default()
    {
        let mut node_ids = Vec::new();
        for proxy in provider.proxies {
            let record_id = NodeRecordId::for_provider(&provider_name, &proxy.name);
            provider_candidates
                .entry(proxy.name.clone())
                .or_default()
                .insert(record_id.clone());
            nodes.entry(record_id.clone()).or_insert_with(|| ProxyNode {
                record_id: record_id.clone(),
                name: proxy.name.clone(),
                proxy_type: proxy.proxy_type,
                availability: availability(proxy.alive),
                core_internal: false,
                source: NodeSource::Provider {
                    provider_name: provider_name.clone(),
                    proxy_name: proxy.name,
                },
            });
            if !node_ids.contains(&record_id) {
                node_ids.push(record_id);
            }
        }
        provider_views.push(ProxyProvider {
            name: provider_name,
            vehicle: provider_vehicle(&provider.vehicle_type),
            node_ids,
        });
    }

    let mut raw_groups = BTreeMap::new();
    let mut core_node_ids = BTreeMap::new();
    for (name, proxy) in proxies.proxies {
        if proxy.all.is_some() {
            raw_groups.insert(name, proxy);
            continue;
        }
        let (record_id, source) = proxy.provider.as_ref().map_or_else(
            || {
                (
                    NodeRecordId::for_core(&name),
                    NodeSource::Core {
                        proxy_name: name.clone(),
                    },
                )
            },
            |provider_name| {
                (
                    NodeRecordId::for_provider(provider_name, &name),
                    NodeSource::Provider {
                        provider_name: provider_name.clone(),
                        proxy_name: name.clone(),
                    },
                )
            },
        );
        if matches!(source, NodeSource::Core { .. }) {
            core_node_ids.insert(name.clone(), record_id.clone());
        } else {
            provider_candidates
                .entry(name.clone())
                .or_default()
                .insert(record_id.clone());
        }
        nodes.entry(record_id.clone()).or_insert(ProxyNode {
            record_id,
            name: name.clone(),
            proxy_type: proxy.proxy_type,
            availability: availability(proxy.alive),
            core_internal: is_core_internal(&name),
            source,
        });
    }

    let group_names = raw_groups.keys().cloned().collect::<BTreeSet<_>>();
    let mut ordered_names = Vec::new();
    let mut selected = BTreeSet::new();
    for name in effective_group_order {
        if raw_groups.contains_key(name) && selected.insert(name.clone()) {
            ordered_names.push(name.clone());
        }
    }
    let order_source = if ordered_names.is_empty() {
        ProxyViewOrderSource::StableFallback
    } else {
        ProxyViewOrderSource::EffectiveConfiguration
    };
    ordered_names.extend(
        raw_groups
            .keys()
            .filter(|name| !selected.contains(*name))
            .cloned(),
    );

    let groups = ordered_names
        .into_iter()
        .filter_map(|name| {
            raw_groups.remove(&name).map(|proxy| {
                let members = proxy
                    .all
                    .unwrap_or_default()
                    .into_iter()
                    .map(|member| {
                        resolve_member(
                            member,
                            &group_names,
                            &core_node_ids,
                            &provider_candidates,
                            &nodes,
                            provider_state,
                        )
                    })
                    .collect();
                ProxyGroup {
                    core_internal: is_core_internal(&name),
                    name,
                    selectable: proxy.proxy_type.eq_ignore_ascii_case("selector"),
                    proxy_type: proxy.proxy_type,
                    availability: availability(proxy.alive),
                    selected_name: proxy.now,
                    members,
                }
            })
        })
        .collect();

    ProxyView {
        schema_version: PROXY_VIEW_SCHEMA_VERSION,
        order_source,
        provider_state,
        groups,
        nodes,
        providers: provider_views,
    }
}

fn resolve_member(
    name: String,
    group_names: &BTreeSet<String>,
    core_node_ids: &BTreeMap<String, NodeRecordId>,
    provider_candidates: &BTreeMap<String, BTreeSet<NodeRecordId>>,
    nodes: &BTreeMap<NodeRecordId, ProxyNode>,
    provider_state: ProviderState,
) -> ProxyMember {
    if group_names.contains(&name) {
        return ProxyMember::Group { name };
    }
    if let Some(record_id) = core_node_ids.get(&name) {
        return ProxyMember::Node {
            name,
            availability: nodes
                .get(record_id)
                .map_or(Availability::Unavailable, |node| node.availability),
            record_id: record_id.clone(),
        };
    }
    if provider_state == ProviderState::Unavailable {
        return ProxyMember::Unresolved {
            name,
            reason: UnresolvedMemberReason::ProviderUnavailable,
            candidate_ids: Vec::new(),
        };
    }
    match provider_candidates.get(&name) {
        None => ProxyMember::Unresolved {
            name,
            reason: UnresolvedMemberReason::Missing,
            candidate_ids: Vec::new(),
        },
        Some(candidates) if candidates.len() == 1 => {
            let record_id = candidates.iter().next().expect("one candidate").clone();
            ProxyMember::Node {
                name,
                availability: nodes
                    .get(&record_id)
                    .map_or(Availability::Unavailable, |node| node.availability),
                record_id,
            }
        }
        Some(candidates) => ProxyMember::Unresolved {
            name,
            reason: UnresolvedMemberReason::Ambiguous,
            candidate_ids: candidates.iter().cloned().collect(),
        },
    }
}

fn availability(alive: bool) -> Availability {
    if alive {
        Availability::Available
    } else {
        Availability::Unavailable
    }
}

fn is_core_internal(name: &str) -> bool {
    matches!(
        name,
        "GLOBAL" | "DIRECT" | "REJECT" | "REJECT-DROP" | "PASS" | "COMPATIBLE"
    )
}

fn provider_vehicle(value: &str) -> ProviderVehicle {
    match value.to_ascii_lowercase().as_str() {
        "http" => ProviderVehicle::Http,
        "file" => ProviderVehicle::File,
        "inline" => ProviderVehicle::Inline,
        "compatible" => ProviderVehicle::Compatible,
        _ => ProviderVehicle::Unknown,
    }
}

#[derive(Deserialize)]
struct RawProxies {
    proxies: BTreeMap<String, RawProxy>,
}

#[derive(Deserialize)]
struct RawProxy {
    #[serde(rename = "type")]
    proxy_type: String,
    #[serde(default)]
    alive: bool,
    #[serde(default)]
    all: Option<Vec<String>>,
    #[serde(default)]
    now: Option<String>,
    #[serde(default)]
    provider: Option<String>,
}

#[derive(Deserialize)]
struct RawProviders {
    providers: BTreeMap<String, RawProvider>,
}

#[derive(Deserialize)]
struct RawProvider {
    #[serde(rename = "vehicleType")]
    vehicle_type: String,
    #[serde(default)]
    proxies: Vec<RawProviderProxy>,
}

#[derive(Deserialize)]
struct RawProviderProxy {
    name: String,
    #[serde(rename = "type")]
    proxy_type: String,
    #[serde(default)]
    alive: bool,
}

// -----------------------------------------------------------------------------
// Mihomo adapter and event contracts
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MihomoVersion {
    pub version: String,
    pub meta: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MihomoReadiness {
    Ready,
    Starting,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelayProbeRequest {
    pub record_id: NodeRecordId,
    pub target: DelayTarget,
    pub test_url: String,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DelayTarget {
    CoreProxy {
        proxy_name: String,
    },
    ProviderProxy {
        provider_name: String,
        proxy_name: String,
    },
}

impl DelayTarget {
    #[must_use]
    pub fn from_node(node: &ProxyNode) -> Self {
        match &node.source {
            NodeSource::Core { proxy_name } => Self::CoreProxy {
                proxy_name: proxy_name.clone(),
            },
            NodeSource::Provider {
                provider_name,
                proxy_name,
            } => Self::ProviderProxy {
                provider_name: provider_name.clone(),
                proxy_name: proxy_name.clone(),
            },
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DelayProbeResult {
    pub delay_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrafficFrame {
    pub upload_bytes_per_second: u64,
    pub download_bytes_per_second: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectionSummary {
    pub active_connections: u64,
    pub upload_total_bytes: u64,
    pub download_total_bytes: u64,
    pub memory_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MihomoLogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Eq, PartialEq)]
pub struct MihomoLogFrame {
    pub level: MihomoLogLevel,
    pub message: String,
}

impl fmt::Debug for MihomoLogFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MihomoLogFrame")
            .field("level", &self.level)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreEvent<T> {
    pub instance_generation: CoreInstanceGeneration,
    pub payload: T,
}

pub trait CoreEventStream<T>: Send {
    fn next_event(&mut self) -> Result<Option<CoreEvent<T>>, MihomoError>;

    fn cancel(&mut self);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MihomoErrorKind {
    Unavailable,
    Unauthorized,
    InvalidResponse,
    SelectionRejected,
    ProbeFailed,
    StreamClosed,
}

#[derive(Clone, Eq, PartialEq)]
pub struct MihomoError {
    pub kind: MihomoErrorKind,
    diagnostic: String,
}

impl MihomoError {
    #[must_use]
    pub fn new(kind: MihomoErrorKind, diagnostic: impl Into<String>) -> Self {
        Self {
            kind,
            diagnostic: diagnostic.into(),
        }
    }
}

impl fmt::Debug for MihomoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MihomoError")
            .field("kind", &self.kind)
            .field("diagnostic_bytes", &self.diagnostic.len())
            .finish()
    }
}

impl fmt::Display for MihomoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            MihomoErrorKind::Unavailable => "Mihomo API is unavailable",
            MihomoErrorKind::Unauthorized => "Mihomo API authorization failed",
            MihomoErrorKind::InvalidResponse => "Mihomo API returned an invalid response",
            MihomoErrorKind::SelectionRejected => "Mihomo rejected the Node selection",
            MihomoErrorKind::ProbeFailed => "Mihomo Delay Probe failed",
            MihomoErrorKind::StreamClosed => "Mihomo event stream closed",
        })
    }
}

impl std::error::Error for MihomoError {}

pub trait MihomoAdapter: Send + Sync {
    fn version(&self, endpoint: &CoreControlEndpoint) -> Result<MihomoVersion, MihomoError>;

    fn readiness(&self, endpoint: &CoreControlEndpoint) -> Result<MihomoReadiness, MihomoError>;

    fn proxy_view(
        &self,
        endpoint: &CoreControlEndpoint,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError>;

    fn select_node(
        &self,
        endpoint: &CoreControlEndpoint,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError>;

    fn probe_delay(
        &self,
        endpoint: &CoreControlEndpoint,
        request: &DelayProbeRequest,
    ) -> Result<DelayProbeResult, MihomoError>;

    fn connection_summary(
        &self,
        endpoint: &CoreControlEndpoint,
    ) -> Result<ConnectionSummary, MihomoError>;

    fn open_traffic_stream(
        &self,
        endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<TrafficFrame>>, MihomoError>;

    fn open_connection_stream(
        &self,
        endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<ConnectionSummary>>, MihomoError>;

    fn open_log_stream(
        &self,
        endpoint: &CoreControlEndpoint,
        generation: CoreInstanceGeneration,
    ) -> Result<Box<dyn CoreEventStream<MihomoLogFrame>>, MihomoError>;
}

pub struct MihomoJsonCodec;

impl MihomoJsonCodec {
    pub fn version(json: &[u8]) -> Result<MihomoVersion, ProjectionError> {
        let raw: RawVersion = parse_response(json, ProjectionErrorKind::Version, "version")?;
        Ok(MihomoVersion {
            version: raw.version,
            meta: raw.meta,
        })
    }

    pub fn delay(json: &[u8]) -> Result<DelayProbeResult, ProjectionError> {
        let raw: RawDelay = parse_response(json, ProjectionErrorKind::Delay, "delay")?;
        let delay_ms = u16::try_from(raw.delay).map_err(|_| {
            ProjectionError::invalid(
                ProjectionErrorKind::Delay,
                "Mihomo delay exceeds the v1.19.28 unsigned 16-bit range",
            )
        })?;
        Ok(DelayProbeResult {
            delay_ms: u64::from(delay_ms),
        })
    }

    pub fn traffic(json: &[u8]) -> Result<TrafficFrame, ProjectionError> {
        let raw: RawTraffic = parse_response(json, ProjectionErrorKind::Traffic, "traffic")?;
        Ok(TrafficFrame {
            upload_bytes_per_second: raw.up,
            download_bytes_per_second: raw.down,
        })
    }

    pub fn connections(json: &[u8]) -> Result<ConnectionSummary, ProjectionError> {
        let raw: RawConnections =
            parse_response(json, ProjectionErrorKind::Connections, "connections")?;
        let active_connections = u64::try_from(raw.connections.len()).map_err(|_| {
            ProjectionError::invalid(
                ProjectionErrorKind::Connections,
                "Mihomo connection count exceeds the projection range",
            )
        })?;
        Ok(ConnectionSummary {
            active_connections,
            upload_total_bytes: raw.upload_total,
            download_total_bytes: raw.download_total,
            memory_bytes: raw.memory,
        })
    }

    pub fn log(json: &[u8]) -> Result<MihomoLogFrame, ProjectionError> {
        let raw: RawLog = parse_response(json, ProjectionErrorKind::Log, "log")?;
        let level = match raw.kind.to_ascii_lowercase().as_str() {
            "debug" => MihomoLogLevel::Debug,
            "info" => MihomoLogLevel::Info,
            "warning" | "warn" => MihomoLogLevel::Warn,
            "error" => MihomoLogLevel::Error,
            _ => {
                return Err(ProjectionError::invalid(
                    ProjectionErrorKind::Log,
                    "invalid Mihomo log response: unknown level",
                ));
            }
        };
        Ok(MihomoLogFrame {
            level,
            message: raw.payload,
        })
    }
}

fn parse_response<'de, T: Deserialize<'de>>(
    json: &'de [u8],
    kind: ProjectionErrorKind,
    name: &str,
) -> Result<T, ProjectionError> {
    serde_json::from_slice(json).map_err(|error| {
        ProjectionError::invalid(kind, format!("invalid Mihomo {name} response: {error}"))
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawVersion {
    version: String,
    #[serde(default)]
    meta: bool,
    #[serde(default, rename = "premium")]
    _premium: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDelay {
    delay: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawTraffic {
    up: u64,
    down: u64,
    #[serde(default, rename = "upTotal")]
    _up_total: u64,
    #[serde(default, rename = "downTotal")]
    _down_total: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConnections {
    #[serde(rename = "uploadTotal")]
    upload_total: u64,
    #[serde(rename = "downloadTotal")]
    download_total: u64,
    #[serde(default)]
    memory: Option<u64>,
    connections: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLog {
    #[serde(rename = "type")]
    kind: String,
    payload: String,
}

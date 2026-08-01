//! Projects authoritative Supervisor state into application and status DTOs.

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::application::{
    ApplicationError, LatencyFreshness as ApplicationLatencyFreshness,
    LatencyProbeStatus as ApplicationLatencyProbeStatus, LatencySummary, PolicyTargetValidation,
    ProfileRefreshFailure, ProfileRefreshStage, ProfileRefreshState, ProfileSummary,
    ProxyAvailability, ProxyGroupSummary, ProxyMemberKind, ProxyNodeRow, ProxyNodeSource,
    RuleSummary, SelectorCandidate, SelectorIdentity, SelectorKind,
};
use crate::constants::{CORE_LOG_LINE_MAX_BYTES, LOG_CAPACITY, TRAFFIC_SERIES_CAPACITY};
use crate::core::{
    Availability, CoreRuntimeDiagnosticCategory as RuntimeDiagnosticCategory, CoreRuntimeLifecycle,
    CoreRuntimeStatus, CoreRuntimeTunReason, ManagedCoreHandle, NodeRowMemberV1, NodeSelection,
    NodeSource, ProbeObservation, ProbeStatus as CoreProbeStatus, ProxyView, SelectionError,
};
use crate::diagnostics::WrapperDiagnosticContext;
use crate::domain::{
    ActiveProfileSummary, CoreDiagnosticCategory, CoreInstanceGeneration, CoreLifecycle,
    CoreRestartStatus, CoreStatus, NodeRecordId, ProbeGeneration, ProbeQueueStatus, ProfileId,
    ProxyGroupId, RuntimeApplySnapshot, RuntimeGeneration, SampleState, SelectedNodeSummary,
    StatusSnapshot, StreamHealthSet, StreamState, SupervisorHealthReason, SupervisorLifecycle,
    SupervisorStatus, TrafficSample, TunReason, TunStatus,
};
use crate::error::ErrorCode;
use crate::profile::{Profile, ProfileCatalog, RefreshContext, RefreshStage};
use crate::scheduler::ProbeStatus;
use crate::telemetry::{LogTail, TelemetryStore};

use super::SupervisorState;
use super::errors::{internal_error, no_active_profile, selector_not_found};

pub(super) fn profile_summary(profile: &Profile, active: Option<ProfileId>) -> ProfileSummary {
    ProfileSummary {
        id: profile.id,
        name: profile.name.clone(),
        subscription_url: profile.subscription_url.clone(),
        active: active == Some(profile.id),
        refresh_state: if profile.last_error.is_some() {
            ProfileRefreshState::Error
        } else {
            ProfileRefreshState::Fresh
        },
        last_success_at_unix_ms: profile.last_success_at_unix_ms,
        next_refresh_at_unix_ms: profile.next_refresh_at_unix_ms,
        last_error: profile
            .last_error
            .as_ref()
            .map(|failure| ProfileRefreshFailure {
                stage: match failure.stage {
                    RefreshStage::Download => ProfileRefreshStage::Download,
                    RefreshStage::Parse => ProfileRefreshStage::Parse,
                    RefreshStage::Validate => ProfileRefreshStage::Validate,
                    RefreshStage::Apply => ProfileRefreshStage::Apply,
                },
                message: failure.safe_message.clone(),
            }),
    }
}

pub(super) fn profile_list_snapshot_id(profiles: &ProfileCatalog) -> u64 {
    let mut hasher = DefaultHasher::new();
    profiles.len().hash(&mut hasher);
    profiles.active_profile_id().hash(&mut hasher);
    for profile in profiles.profiles() {
        profile.id.hash(&mut hasher);
        profile.name.hash(&mut hasher);
        profile.subscription_url.expose().as_str().hash(&mut hasher);
        profile.revision.0.hash(&mut hasher);
        profile.last_success_at_unix_ms.hash(&mut hasher);
        profile.next_refresh_at_unix_ms.hash(&mut hasher);
        match &profile.last_error {
            Some(failure) => {
                1_u8.hash(&mut hasher);
                std::mem::discriminant(&failure.stage).hash(&mut hasher);
                failure.safe_message.hash(&mut hasher);
            }
            None => 0_u8.hash(&mut hasher),
        }
    }
    hasher.finish()
}

pub(super) fn effective_group_order(
    profiles: &ProfileCatalog,
) -> Result<Vec<String>, ApplicationError> {
    let active = profiles
        .active_profile_id()
        .and_then(|id| profiles.get(id))
        .ok_or_else(no_active_profile)?;
    let Some(serde_yaml_ng::Value::Sequence(groups)) =
        active.snapshot.document().get("proxy-groups")
    else {
        return Ok(Vec::new());
    };
    Ok(groups
        .iter()
        .filter_map(serde_yaml_ng::Value::as_mapping)
        .filter_map(|group| group.get("name"))
        .filter_map(serde_yaml_ng::Value::as_str)
        .map(str::to_owned)
        .collect())
}

pub(super) fn wrapper_diagnostic_context(
    state: &SupervisorState,
    reason: SupervisorHealthReason,
) -> WrapperDiagnosticContext {
    match reason {
        SupervisorHealthReason::RuntimeRecovery
        | SupervisorHealthReason::ConfigurationProjection => WrapperDiagnosticContext {
            runtime_generation: state.runtime_generation,
            ..WrapperDiagnosticContext::default()
        },
        SupervisorHealthReason::SelectionCompensation
        | SupervisorHealthReason::SelectionRestoration => WrapperDiagnosticContext {
            runtime_generation: state.runtime_generation,
            core_generation: state
                .observed_core_generation
                .or(state.probe_core_generation),
            revision: None,
        },
        SupervisorHealthReason::ProbeScheduler => WrapperDiagnosticContext {
            runtime_generation: state.runtime_generation,
            core_generation: state
                .probe_core_generation
                .or(state.observed_core_generation),
            revision: (state.next_probe_generation > 0).then_some(state.next_probe_generation),
        },
    }
}

pub(super) fn resolve_proxy_group<'a>(
    view: &'a ProxyView,
    selector: &str,
) -> Result<&'a crate::core::ProxyGroup, ApplicationError> {
    if let Ok(id) = ProxyGroupId::parse(selector) {
        return view
            .groups
            .iter()
            .find(|group| group.id == id)
            .ok_or_else(|| selector_not_found(SelectorKind::ProxyGroup, "Proxy Group"));
    }
    view.groups
        .iter()
        .find(|group| group.name == selector)
        .ok_or_else(|| selector_not_found(SelectorKind::ProxyGroup, "Proxy Group"))
}

pub(super) fn selection_by_selector(
    view: &ProxyView,
    group_name: &str,
    selector: &str,
) -> Result<NodeSelection, SelectionError> {
    let group = view
        .groups
        .iter()
        .find(|group| group.name == group_name)
        .ok_or_else(|| SelectionError::GroupMissing(group_name.to_owned()))?;
    if !group.selectable {
        return Err(SelectionError::GroupNotSelectable(group_name.to_owned()));
    }
    if let Ok(record_id) = NodeRecordId::parse(selector) {
        let node = group.members.iter().find_map(|member| match member {
            crate::core::ProxyMember::Node {
                name,
                record_id: candidate,
                availability,
            } if *candidate == record_id => Some((name, availability)),
            _ => None,
        });
        return match node {
            Some((name, Availability::Available)) => Ok(NodeSelection {
                group_name: group_name.to_owned(),
                node_name: name.clone(),
                record_id,
            }),
            Some((name, Availability::Unavailable)) => {
                Err(SelectionError::NodeUnavailable(name.clone()))
            }
            None => Err(SelectionError::NodeMissing(selector.to_owned())),
        };
    }
    view.resolve_exact_selection(group_name, selector)
}

pub(super) fn probe_observations(
    state: &SupervisorState,
    view: &ProxyView,
    now_unix_ms: u64,
) -> BTreeMap<NodeRecordId, ProbeObservation> {
    view.nodes
        .keys()
        .filter_map(|node_id| {
            state
                .probes
                .node_snapshot(node_id, now_unix_ms)
                .map(|snapshot| {
                    (
                        node_id.clone(),
                        ProbeObservation {
                            sample: snapshot.sample,
                            status: match snapshot.status {
                                ProbeStatus::NotSampled => CoreProbeStatus::NotSampled,
                                ProbeStatus::Queued => CoreProbeStatus::Queued,
                                ProbeStatus::InFlight => CoreProbeStatus::InFlight,
                                ProbeStatus::Available => CoreProbeStatus::Succeeded,
                                ProbeStatus::TimedOut | ProbeStatus::Unavailable => {
                                    CoreProbeStatus::Failed
                                }
                            },
                        },
                    )
                })
        })
        .collect()
}

pub(super) fn probe_observations_page(
    state: &SupervisorState,
    group: &crate::core::ProxyGroup,
    offset: usize,
    limit: usize,
    now_unix_ms: u64,
) -> BTreeMap<NodeRecordId, ProbeObservation> {
    let end = offset.saturating_add(limit).min(group.members.len());
    group
        .members
        .get(offset..end)
        .unwrap_or_default()
        .iter()
        .filter_map(|member| match member {
            crate::core::ProxyMember::Node { record_id, .. } => state
                .probes
                .node_snapshot(record_id, now_unix_ms)
                .map(|snapshot| {
                    (
                        record_id.clone(),
                        ProbeObservation {
                            sample: snapshot.sample,
                            status: match snapshot.status {
                                ProbeStatus::NotSampled => CoreProbeStatus::NotSampled,
                                ProbeStatus::Queued => CoreProbeStatus::Queued,
                                ProbeStatus::InFlight => CoreProbeStatus::InFlight,
                                ProbeStatus::Available => CoreProbeStatus::Succeeded,
                                ProbeStatus::TimedOut | ProbeStatus::Unavailable => {
                                    CoreProbeStatus::Failed
                                }
                            },
                        },
                    )
                }),
            _ => None,
        })
        .collect()
}

pub(super) fn proxy_list_snapshot_id(view: &ProxyView, generation: CoreInstanceGeneration) -> u64 {
    let mut hasher = DefaultHasher::new();
    generation.hash(&mut hasher);
    view.schema_version.hash(&mut hasher);
    std::mem::discriminant(&view.order_source).hash(&mut hasher);
    std::mem::discriminant(&view.provider_state).hash(&mut hasher);
    for group in &view.groups {
        group.id.hash(&mut hasher);
        group.name.hash(&mut hasher);
        group.proxy_type.hash(&mut hasher);
        std::mem::discriminant(&group.availability).hash(&mut hasher);
        group.selectable.hash(&mut hasher);
        group.core_internal.hash(&mut hasher);
        group.selected_name.hash(&mut hasher);
        for member in &group.members {
            std::mem::discriminant(member).hash(&mut hasher);
            match member {
                crate::core::ProxyMember::Group { name } => name.hash(&mut hasher),
                crate::core::ProxyMember::Node {
                    name,
                    record_id,
                    availability,
                } => {
                    name.hash(&mut hasher);
                    record_id.hash(&mut hasher);
                    std::mem::discriminant(availability).hash(&mut hasher);
                }
                crate::core::ProxyMember::Unresolved {
                    name,
                    reason,
                    candidate_ids,
                } => {
                    name.hash(&mut hasher);
                    std::mem::discriminant(reason).hash(&mut hasher);
                    candidate_ids.hash(&mut hasher);
                }
            }
        }
    }
    for (record_id, node) in &view.nodes {
        record_id.hash(&mut hasher);
        node.name.hash(&mut hasher);
        node.proxy_type.hash(&mut hasher);
        std::mem::discriminant(&node.availability).hash(&mut hasher);
        node.core_internal.hash(&mut hasher);
        std::mem::discriminant(&node.source).hash(&mut hasher);
        match &node.source {
            crate::core::NodeSource::Core { proxy_name } => proxy_name.hash(&mut hasher),
            crate::core::NodeSource::Provider {
                provider_name,
                proxy_name,
            } => {
                provider_name.hash(&mut hasher);
                proxy_name.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

pub(super) fn proxy_group_summary(
    view: &ProxyView,
    group: &crate::core::ProxyGroup,
) -> ProxyGroupSummary {
    let selected_node = group
        .selected_name
        .as_deref()
        .and_then(|name| selection_by_selector(view, &group.name, name).ok())
        .map(|selection| SelectorIdentity {
            id: selection.record_id.as_str().to_owned(),
            name: selection.node_name,
        });
    ProxyGroupSummary {
        id: group.id.clone(),
        name: group.name.clone(),
        proxy_type: group.proxy_type.clone(),
        selectable: group.selectable,
        selected_node,
    }
}

pub(super) fn proxy_row(row: crate::core::NodeRowV1) -> ProxyNodeRow {
    let (id, member_kind, source, candidate_ids) = match row.member {
        NodeRowMemberV1::Group => (None, ProxyMemberKind::Group, None, Vec::new()),
        NodeRowMemberV1::Node { record_id, source } => {
            let source = match source {
                NodeSource::Core { .. } => ProxyNodeSource::Core,
                NodeSource::Provider { provider_name, .. } => {
                    ProxyNodeSource::Provider { provider_name }
                }
            };
            (
                Some(record_id),
                ProxyMemberKind::Node,
                Some(source),
                Vec::new(),
            )
        }
        NodeRowMemberV1::Unresolved {
            reason,
            candidate_ids,
        } => {
            let kind = match reason {
                crate::core::UnresolvedMemberReason::Missing => ProxyMemberKind::Missing,
                crate::core::UnresolvedMemberReason::Ambiguous => ProxyMemberKind::Ambiguous,
                crate::core::UnresolvedMemberReason::ProviderUnavailable => {
                    ProxyMemberKind::ProviderUnavailable
                }
            };
            (None, kind, None, candidate_ids)
        }
    };
    ProxyNodeRow {
        id,
        name: row.name,
        member_kind,
        source,
        candidate_ids,
        proxy_type: row.proxy_type,
        availability: match row.availability {
            Availability::Available => ProxyAvailability::Available,
            Availability::Unavailable => ProxyAvailability::Unavailable,
        },
        selected: row.selected,
        delay_ms: row.delay_ms,
        sampled_at_unix_ms: row.sampled_at_unix_ms,
        freshness: match row.freshness {
            crate::core::LatencyFreshness::NotSampled => ApplicationLatencyFreshness::NotSampled,
            crate::core::LatencyFreshness::Fresh => ApplicationLatencyFreshness::Fresh,
            crate::core::LatencyFreshness::Stale => ApplicationLatencyFreshness::Stale,
            crate::core::LatencyFreshness::Unavailable => ApplicationLatencyFreshness::Unavailable,
        },
        probe_status: match row.probe_status {
            CoreProbeStatus::NotSampled => ApplicationLatencyProbeStatus::NotSampled,
            CoreProbeStatus::Queued => ApplicationLatencyProbeStatus::Queued,
            CoreProbeStatus::InFlight => ApplicationLatencyProbeStatus::InFlight,
            CoreProbeStatus::Succeeded => ApplicationLatencyProbeStatus::Succeeded,
            CoreProbeStatus::Failed => ApplicationLatencyProbeStatus::Failed,
        },
    }
}

pub(super) fn latency_summary(
    state: &SupervisorState,
    node_id: NodeRecordId,
    node_name: &str,
    generation: ProbeGeneration,
    now_unix_ms: u64,
) -> LatencySummary {
    let snapshot = state.probes.node_snapshot(&node_id, now_unix_ms);
    let sample = snapshot
        .as_ref()
        .and_then(|snapshot| snapshot.sample.as_ref());
    LatencySummary {
        node_id,
        node_name: node_name.to_owned(),
        delay_ms: sample.and_then(|sample| sample.delay_ms),
        sampled_at_unix_ms: sample.and_then(|sample| sample.sampled_at_unix_ms),
        freshness: match sample.map(|sample| sample.state) {
            None => ApplicationLatencyFreshness::NotSampled,
            Some(SampleState::Fresh) => ApplicationLatencyFreshness::Fresh,
            Some(SampleState::Stale) => ApplicationLatencyFreshness::Stale,
            Some(SampleState::Unavailable) => ApplicationLatencyFreshness::Unavailable,
        },
        probe_status: match snapshot.map(|snapshot| snapshot.status) {
            None | Some(ProbeStatus::NotSampled) => ApplicationLatencyProbeStatus::NotSampled,
            Some(ProbeStatus::Queued) => ApplicationLatencyProbeStatus::Queued,
            Some(ProbeStatus::InFlight) => ApplicationLatencyProbeStatus::InFlight,
            Some(ProbeStatus::Available) => ApplicationLatencyProbeStatus::Succeeded,
            Some(ProbeStatus::TimedOut | ProbeStatus::Unavailable) => {
                ApplicationLatencyProbeStatus::Failed
            }
        },
        probe_generation: generation,
    }
}

pub(super) fn rule_summary(entry: crate::rule::RuleListEntry<'_>) -> RuleSummary {
    RuleSummary {
        index: entry.index,
        rule_string: entry.rule.as_str().to_owned(),
        rule_type: entry.parsed.rule_type.as_str().to_owned(),
        payload: entry.parsed.payload.map(str::to_owned),
        policy_target: entry.parsed.policy_target.to_owned(),
        params: entry.parsed.params.into_iter().map(str::to_owned).collect(),
        policy_target_validation: PolicyTargetValidation::Valid,
    }
}

pub(super) fn resolve_latency_node<'a>(
    view: &'a ProxyView,
    selector: &str,
) -> Result<&'a crate::core::ProxyNode, ApplicationError> {
    if let Ok(id) = NodeRecordId::parse(selector) {
        return view
            .nodes
            .get(&id)
            .filter(|node| probe_eligible_node(node))
            .ok_or_else(|| selector_not_found(SelectorKind::Node, "Node"));
    }
    let candidates = view
        .nodes
        .values()
        .filter(|node| probe_eligible_node(node) && node.name == selector)
        .collect::<Vec<_>>();
    match candidates.as_slice() {
        [] => Err(selector_not_found(SelectorKind::Node, "Node")),
        [node] => Ok(*node),
        _ => Err(ApplicationError::new(
            ErrorCode::NodeAmbiguous,
            "The Node selector is ambiguous",
            false,
        )
        .with_selector_candidates(
            SelectorKind::Node,
            candidates
                .into_iter()
                .map(|node| SelectorCandidate::new(node.record_id.as_str(), &node.name))
                .collect(),
        )),
    }
}

pub(super) fn probe_eligible_node(node: &crate::core::ProxyNode) -> bool {
    !node.core_internal
}

pub(super) struct CoreHealthProjection {
    pub(super) managed_core: Option<ManagedCoreHandle>,
    pub(super) core: CoreStatus,
    pub(super) tun: TunStatus,
    pub(super) degraded: bool,
}

impl CoreHealthProjection {
    pub(super) fn unconfigured() -> Self {
        Self {
            managed_core: None,
            core: CoreStatus {
                lifecycle: CoreLifecycle::Unconfigured,
                pid: None,
                instance_generation: None,
                restart: CoreRestartStatus::default(),
            },
            tun: TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::NoActiveProfile),
            },
            degraded: false,
        }
    }

    pub(super) fn unavailable() -> Self {
        Self {
            managed_core: None,
            core: CoreStatus {
                lifecycle: CoreLifecycle::Degraded,
                pid: None,
                instance_generation: None,
                restart: CoreRestartStatus::default(),
            },
            tun: TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::CoreUnavailable),
            },
            degraded: true,
        }
    }

    pub(super) fn from_runtime(status: CoreRuntimeStatus) -> Self {
        let (lifecycle, managed_core, degraded) = match (status.lifecycle, status.managed_core) {
            (CoreRuntimeLifecycle::Owned, None) => (CoreLifecycle::Stopped, None, false),
            (CoreRuntimeLifecycle::Running, Some(core)) => {
                (CoreLifecycle::Ready, Some(core), false)
            }
            (CoreRuntimeLifecycle::RestartPending, _) => (CoreLifecycle::Starting, None, false),
            (CoreRuntimeLifecycle::Degraded, _) => (CoreLifecycle::Degraded, None, true),
            (CoreRuntimeLifecycle::Owned | CoreRuntimeLifecycle::Running, _) => {
                (CoreLifecycle::Degraded, None, true)
            }
        };
        let capable = status.tun.capable;
        let effective = lifecycle == CoreLifecycle::Ready && capable;
        let runtime_tun_reason = status.tun.reason.map(|reason| match reason {
            CoreRuntimeTunReason::PermissionDenied => TunReason::PermissionDenied,
            CoreRuntimeTunReason::Unsupported => TunReason::Unsupported,
        });
        let reason = if effective {
            None
        } else {
            runtime_tun_reason.or(Some(TunReason::CoreUnavailable))
        };
        let restart = CoreRestartStatus {
            pending: status.restart.pending,
            attempts: u64::try_from(status.restart.attempts).unwrap_or(u64::MAX),
            backoff_ms: status
                .restart
                .backoff
                .map(|backoff| u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX)),
            diagnostic: status.restart.diagnostic.map(|category| match category {
                RuntimeDiagnosticCategory::CoreRestartLimitReached => {
                    CoreDiagnosticCategory::RestartLimitReached
                }
            }),
        };
        Self {
            core: CoreStatus {
                lifecycle,
                pid: managed_core.as_ref().map(|core| core.pid),
                instance_generation: managed_core.as_ref().map(|core| core.instance_generation),
                restart,
            },
            managed_core,
            tun: TunStatus {
                requested: true,
                capable,
                effective,
                reason,
            },
            degraded,
        }
    }
}

pub(super) fn status_proxy_fields(
    state: &SupervisorState,
    now_unix_ms: u64,
) -> (
    Option<String>,
    Option<SelectedNodeSummary>,
    Option<crate::domain::LatencySample>,
) {
    let Some(view) = &state.cached_proxy_view else {
        return (None, None, None);
    };
    let Some(group) = view.primary_group() else {
        return (None, None, None);
    };
    let selection = group
        .selected_name
        .as_deref()
        .and_then(|name| selection_by_selector(view, &group.name, name).ok());
    let selected = selection.as_ref().map(|selection| SelectedNodeSummary {
        id: selection.record_id.clone(),
        name: selection.node_name.clone(),
    });
    let latency = selection.and_then(|selection| {
        state
            .probes
            .node_snapshot(&selection.record_id, now_unix_ms)
            .and_then(|snapshot| snapshot.sample)
    });
    (Some(group.name.clone()), selected, latency)
}

pub(super) fn ensure_telemetry(
    state: &mut SupervisorState,
    generation: CoreInstanceGeneration,
) -> Result<(), ApplicationError> {
    let replaced = match (state.telemetry.as_mut(), state.telemetry_generation) {
        (Some(telemetry), Some(current)) if current != generation => {
            telemetry.replace_core(generation);
            state.telemetry_generation = Some(generation);
            true
        }
        (Some(_), Some(_)) => false,
        (Some(telemetry), None) => {
            telemetry.replace_core(generation);
            state.telemetry_generation = Some(generation);
            true
        }
        (None, _) => {
            state.telemetry = Some(
                TelemetryStore::new(
                    generation,
                    LOG_CAPACITY,
                    CORE_LOG_LINE_MAX_BYTES,
                    TRAFFIC_SERIES_CAPACITY,
                )
                .map_err(|_| internal_error())?,
            );
            state.telemetry_generation = Some(generation);
            true
        }
    };
    if replaced {
        state.stream_health = disconnected_stream_health();
    }
    Ok(())
}

pub(super) fn disconnected_stream_health() -> StreamHealthSet {
    StreamHealthSet {
        traffic: StreamState::Disconnected,
        connections: StreamState::Disconnected,
        logs: StreamState::Disconnected,
    }
}

pub(super) fn refresh_is_stale(
    profiles: &ProfileCatalog,
    profile_id: ProfileId,
    context: RefreshContext,
) -> bool {
    let Ok(current) = profiles.refresh_context(profile_id) else {
        return true;
    };
    current.profile_revision != context.profile_revision
        || (profiles.active_profile_id() == Some(profile_id)
            && current.active_revision != context.active_revision)
}

pub(super) fn initial_status_snapshot(
    started_at_unix_ms: u64,
    profiles: &ProfileCatalog,
    runtime_generation: Option<RuntimeGeneration>,
    runtime_apply: RuntimeApplySnapshot,
    health_reasons: Vec<SupervisorHealthReason>,
) -> StatusSnapshot {
    let active_profile = profiles
        .active_profile_id()
        .and_then(|id| profiles.get(id))
        .map(|profile| ActiveProfileSummary {
            id: profile.id,
            name: profile.name.clone(),
        });
    let unconfigured = active_profile.is_none();
    StatusSnapshot {
        supervisor: SupervisorStatus {
            lifecycle: if health_reasons.is_empty() {
                SupervisorLifecycle::Ready
            } else {
                SupervisorLifecycle::Degraded
            },
            started_at_unix_ms,
            uptime_seconds: 0,
            health_reasons,
        },
        core: CoreStatus {
            lifecycle: if unconfigured {
                CoreLifecycle::Unconfigured
            } else {
                CoreLifecycle::Stopped
            },
            pid: None,
            instance_generation: None,
            restart: CoreRestartStatus::default(),
        },
        tun: TunStatus {
            requested: true,
            capable: false,
            effective: false,
            reason: Some(if unconfigured {
                TunReason::NoActiveProfile
            } else {
                TunReason::CoreUnavailable
            }),
        },
        active_profile,
        primary_proxy_group: None,
        selected_node: None,
        latency: None,
        traffic: unavailable_traffic(),
        connection_count: 0,
        runtime_generation,
        apply_state: runtime_apply.phase.compatibility_state(),
        runtime_apply,
        selection_restore_pending: false,
        probe_queue: ProbeQueueStatus::default(),
        stream_health: disconnected_stream_health(),
    }
}

pub(super) fn probe_queue_status(metrics: crate::scheduler::ProbeMetrics) -> ProbeQueueStatus {
    ProbeQueueStatus {
        active_node_count: metrics.active_node_count.try_into().unwrap_or(u64::MAX),
        queue_depth: metrics.queue_depth.try_into().unwrap_or(u64::MAX),
        in_flight_count: metrics.in_flight_count.try_into().unwrap_or(u64::MAX),
        overloaded: metrics.overloaded,
        oldest_due_age_ms: metrics.oldest_due_age_ms,
        estimated_full_pass_duration_ms: metrics.estimated_full_pass_duration_ms,
        stale_node_count: metrics.stale_node_count.try_into().unwrap_or(u64::MAX),
    }
}

pub(super) fn unavailable_traffic() -> TrafficSample {
    TrafficSample {
        upload_bytes_per_second: 0,
        download_bytes_per_second: 0,
        sampled_at_unix_ms: None,
        state: SampleState::Unavailable,
    }
}

pub(super) fn empty_log_tail() -> LogTail {
    LogTail {
        records: Vec::new(),
        dropped_total: 0,
        gap: false,
        earliest_sequence: None,
        latest_sequence: None,
        sequence_horizon: None,
    }
}

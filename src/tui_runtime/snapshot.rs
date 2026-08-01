//! Application snapshot loading and view projection.

use std::sync::Arc;

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput,
    ProfileRefreshState, ProxyAvailability, ProxyGroupSummary, ProxyListOutcome, ProxyMemberKind,
};
use crate::cancellation::CancellationToken;
use crate::constants::MAX_ACTIVE_NODES;
use crate::tui::{FullViewSnapshot, ProfileRow, ProxyGroupRow, ProxyGroupSnapshot, ProxyRow};

use super::{
    LogTail, StatusInterfaceError, StatusInterfaceErrorKind, StatusLogEventSource,
    bounded_log_records,
};

pub trait FullSnapshotSource: Send + Sync {
    fn fetch_full_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError>;

    fn refresh_view_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.fetch_full_snapshot(connection_generation, cancellation)
    }

    fn fetch_proxy_group(
        &self,
        _group: &str,
        _connection_generation: u64,
        _cancellation: &CancellationToken,
    ) -> Result<ProxyGroupSnapshot, StatusInterfaceError> {
        Err(StatusInterfaceError::new(
            StatusInterfaceErrorKind::InvalidConfiguration,
            "The snapshot source does not support Proxy Group loading",
        ))
    }
}

pub struct ApplicationSnapshotSource<C: ?Sized, E: ?Sized> {
    client: Arc<C>,
    events: Arc<E>,
}

impl<C: ?Sized, E: ?Sized> ApplicationSnapshotSource<C, E> {
    #[must_use]
    pub fn new(client: Arc<C>, events: Arc<E>) -> Self {
        Self { client, events }
    }
}

impl<C, E> ApplicationSnapshotSource<C, E>
where
    C: ApplicationClient + Send + Sync + ?Sized,
    E: StatusLogEventSource + ?Sized,
{
    fn fetch_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
        include_logs: bool,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        check_snapshot_cancellation(cancellation)?;
        let status = match self
            .client
            .execute_cancellable(ApplicationOperation::GetStatus, cancellation)
            .map_err(snapshot_application_error)?
        {
            ApplicationOutput::Status(status) => status,
            _ => return Err(unexpected_snapshot_output("status")),
        };

        check_snapshot_cancellation(cancellation)?;
        let profiles = match self
            .client
            .execute_cancellable(ApplicationOperation::ProfileList, cancellation)
            .map_err(snapshot_application_error)?
        {
            ApplicationOutput::Profiles(outcome) => outcome
                .profiles
                .into_iter()
                .map(|profile| ProfileRow {
                    id: profile.id,
                    name: profile.name,
                    active: profile.active,
                    fresh: profile.refresh_state == ProfileRefreshState::Fresh,
                    last_success_at_unix_ms: profile.last_success_at_unix_ms,
                    next_refresh_at_unix_ms: profile.next_refresh_at_unix_ms,
                    error: profile.last_error.map(|error| error.message),
                })
                .collect(),
            _ => return Err(unexpected_snapshot_output("Profile list")),
        };

        check_snapshot_cancellation(cancellation)?;
        let (proxy_groups, proxies) = if let Some(group) = status.primary_proxy_group.clone() {
            match self
                .client
                .execute_cancellable(ApplicationOperation::ProxyList { group }, cancellation)
                .map_err(snapshot_application_error)?
            {
                ApplicationOutput::Proxies(outcome) => {
                    let snapshot = proxy_group_snapshot(outcome);
                    (snapshot.groups, snapshot.proxies)
                }
                _ => return Err(unexpected_snapshot_output("Proxy list")),
            }
        } else {
            (Vec::new(), Vec::new())
        };

        let tail = if include_logs {
            check_snapshot_cancellation(cancellation)?;
            self.events
                .fetch_log_tail(connection_generation, None, cancellation)?
        } else {
            LogTail {
                records: Vec::new(),
                gap: false,
                dropped_total: 0,
            }
        };
        Ok(FullViewSnapshot {
            status,
            proxy_groups,
            proxies,
            profiles,
            logs: bounded_log_records(tail.records),
            dropped_logs: tail.dropped_total,
        })
    }
}

impl<C, E> FullSnapshotSource for ApplicationSnapshotSource<C, E>
where
    C: ApplicationClient + Send + Sync + ?Sized,
    E: StatusLogEventSource + ?Sized,
{
    fn fetch_full_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.fetch_snapshot(connection_generation, cancellation, true)
    }

    fn refresh_view_snapshot(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<FullViewSnapshot, StatusInterfaceError> {
        self.fetch_snapshot(connection_generation, cancellation, false)
    }

    fn fetch_proxy_group(
        &self,
        group: &str,
        _connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<ProxyGroupSnapshot, StatusInterfaceError> {
        check_snapshot_cancellation(cancellation)?;
        match self
            .client
            .execute_cancellable(
                ApplicationOperation::ProxyList {
                    group: group.to_owned(),
                },
                cancellation,
            )
            .map_err(snapshot_application_error)?
        {
            ApplicationOutput::Proxies(outcome) => Ok(proxy_group_snapshot(outcome)),
            _ => Err(unexpected_snapshot_output("Proxy list")),
        }
    }
}

fn proxy_group_snapshot(outcome: ProxyListOutcome) -> ProxyGroupSnapshot {
    let ProxyListOutcome {
        group,
        groups,
        nodes,
    } = outcome;
    let group_id = group.id.clone();
    let group_name = group.name.clone();
    let current_group = proxy_group_row(group);
    let mut group_rows = groups
        .into_iter()
        .filter(|group| group.selectable)
        .take(MAX_ACTIVE_NODES)
        .map(proxy_group_row)
        .collect::<Vec<_>>();
    if group_rows.is_empty() {
        group_rows.push(current_group.clone());
    }
    let proxies = nodes
        .into_iter()
        .take(MAX_ACTIVE_NODES)
        .map(|node| ProxyRow {
            group_id: group_id.clone(),
            group: group_name.clone(),
            node_id: node.id,
            name: node.name,
            node_type: node
                .proxy_type
                .unwrap_or_else(|| proxy_member_kind_title(node.member_kind).to_owned()),
            available: node.availability == ProxyAvailability::Available,
            selected: node.selected,
            delay_ms: node.delay_ms,
            sampled_at_unix_ms: node.sampled_at_unix_ms,
            freshness: node.freshness,
            probe_status: node.probe_status,
        })
        .collect();
    ProxyGroupSnapshot {
        group: current_group,
        groups: group_rows,
        proxies,
    }
}

fn proxy_group_row(group: ProxyGroupSummary) -> ProxyGroupRow {
    ProxyGroupRow {
        id: group.id,
        name: group.name,
        proxy_type: group.proxy_type,
        selected_node: group.selected_node.map(|node| node.name),
    }
}

fn proxy_member_kind_title(kind: ProxyMemberKind) -> &'static str {
    match kind {
        ProxyMemberKind::Node => "node",
        ProxyMemberKind::Group => "group",
        ProxyMemberKind::Missing => "missing",
        ProxyMemberKind::Ambiguous => "ambiguous",
        ProxyMemberKind::ProviderUnavailable => "provider_unavailable",
    }
}

fn check_snapshot_cancellation(
    cancellation: &CancellationToken,
) -> Result<(), StatusInterfaceError> {
    if cancellation.is_cancelled() {
        Err(StatusInterfaceError::new(
            StatusInterfaceErrorKind::Snapshot,
            "The snapshot request was cancelled",
        ))
    } else {
        Ok(())
    }
}

fn snapshot_application_error(error: ApplicationError) -> StatusInterfaceError {
    StatusInterfaceError::new(StatusInterfaceErrorKind::Snapshot, error.message)
}

fn unexpected_snapshot_output(resource: &str) -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Snapshot,
        format!("The application returned an unexpected {resource} result"),
    )
}

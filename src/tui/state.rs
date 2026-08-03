//! View state and bounded projection helpers for the Status Interface.

use std::collections::VecDeque;
use std::fmt;

use crate::application::PolicyTargetValidation;
use crate::constants::{
    LOG_CAPACITY, LOG_RETENTION_MAX_BYTES, MAX_ACTIVE_NODES, MINIMUM_TERMINAL_HEIGHT,
    MINIMUM_TERMINAL_WIDTH,
};
use crate::domain::{
    LocalRuleSetRevision, NodeRecordId, ProfileId, ProxyGroupId, RuntimeGeneration, StatusSnapshot,
};
use crate::ipc::RequestId;
use crate::telemetry::{CoreLogRecord, LogLevel, LogSource};

use super::PROFILE_VIEW_CAPACITY;
use super::input::InteractionMap;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Page {
    #[default]
    Proxies,
    Connections,
    Rules,
    Logs,
}

impl Page {
    pub(in crate::tui) const ALL: [Self; 4] =
        [Self::Proxies, Self::Connections, Self::Rules, Self::Logs];

    pub(in crate::tui) fn index(self) -> usize {
        match self {
            Self::Proxies => 0,
            Self::Connections => 1,
            Self::Rules => 2,
            Self::Logs => 3,
        }
    }

    pub(in crate::tui) fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    pub(in crate::tui) fn previous(self) -> Self {
        Self::ALL[(self.index() + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    pub(in crate::tui) fn title(self) -> &'static str {
        match self {
            Self::Proxies => "Proxies",
            Self::Connections => "Connections",
            Self::Rules => "Rules",
            Self::Logs => "Logs",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Focus {
    Tabs,
    ProxyGroups,
    #[default]
    Content,
    Search,
    FooterHelp,
    FooterQuit,
    Modal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionState {
    pub status: ConnectionStatus,
    pub generation: u64,
    pub snapshot_stale: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyRow {
    pub group_id: ProxyGroupId,
    pub group: String,
    pub node_id: Option<NodeRecordId>,
    pub name: String,
    pub node_type: String,
    pub available: bool,
    pub selected: bool,
    pub delay_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyGroupRow {
    pub id: ProxyGroupId,
    pub name: String,
    pub proxy_type: String,
    pub selectable: bool,
    pub selected_node: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProxySort {
    #[default]
    Original,
    Name,
    Delay,
}

impl ProxySort {
    pub(in crate::tui) const ALL: [Self; 3] = [Self::Original, Self::Name, Self::Delay];

    pub(in crate::tui) fn title(self) -> &'static str {
        match self {
            Self::Original => "Original",
            Self::Name => "Name",
            Self::Delay => "Delay",
        }
    }

    pub(in crate::tui) fn next(self) -> Self {
        match self {
            Self::Original => Self::Name,
            Self::Name => Self::Delay,
            Self::Delay => Self::Original,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileRow {
    pub id: ProfileId,
    pub name: String,
    pub active: bool,
    pub fresh: bool,
    pub last_success_at_unix_ms: u64,
    pub next_refresh_at_unix_ms: u64,
    pub error: Option<String>,
}

#[derive(Clone, Eq, PartialEq)]
pub struct ViewLogRecord {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
    pub level: LogLevel,
    pub source: LogSource,
    pub message: String,
}

impl From<&CoreLogRecord> for ViewLogRecord {
    fn from(record: &CoreLogRecord) -> Self {
        Self {
            sequence: record.sequence(),
            timestamp_unix_ms: record.timestamp_unix_ms(),
            level: record.level(),
            source: record.source(),
            message: record.message().to_owned(),
        }
    }
}

impl fmt::Debug for ViewLogRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ViewLogRecord")
            .field("sequence", &self.sequence)
            .field("timestamp_unix_ms", &self.timestamp_unix_ms)
            .field("level", &self.level)
            .field("source", &self.source)
            .field("message_bytes", &self.message.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LogLevelFilter {
    #[default]
    All,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevelFilter {
    pub(in crate::tui) const ALL: [Self; 5] =
        [Self::All, Self::Debug, Self::Info, Self::Warn, Self::Error];

    pub(in crate::tui) fn title(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Debug => "Debug",
            Self::Info => "Info",
            Self::Warn => "Warn",
            Self::Error => "Error",
        }
    }

    fn matches(self, level: LogLevel) -> bool {
        matches!(self, Self::All)
            || matches!(
                (self, level),
                (Self::Debug, LogLevel::Debug)
                    | (Self::Info, LogLevel::Info)
                    | (Self::Warn, LogLevel::Warn)
                    | (Self::Error, LogLevel::Error)
            )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProxiesState {
    pub groups: Vec<ProxyGroupRow>,
    pub group_cursor: usize,
    pub rows: Vec<ProxyRow>,
    pub selected_group: Option<String>,
    pub group_load_pending: Option<PendingProxyGroupLoad>,
    pub selection_pending: Option<(ProxyGroupId, NodeRecordId)>,
    pub selected: usize,
    pub scroll: usize,
    pub filter: String,
    pub sort: ProxySort,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConnectionsState {
    pub selected: usize,
    pub scroll: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingProxyGroupLoad {
    pub request_id: RequestId,
    pub connection_generation: u64,
    pub group_id: ProxyGroupId,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProfilesState {
    pub rows: Vec<ProfileRow>,
    pub selected: usize,
    pub scroll: usize,
    pub filter: String,
    pub activation_pending: Option<ProfileId>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandPaletteAction {
    Profiles,
    RestartSupervisor,
    StopSupervisor,
}

impl CommandPaletteAction {
    pub(in crate::tui) const ALL: [Self; 3] = [
        Self::Profiles,
        Self::RestartSupervisor,
        Self::StopSupervisor,
    ];

    pub(in crate::tui) fn label(self) -> &'static str {
        match self {
            Self::Profiles => "profile switch",
            Self::RestartSupervisor => "restart",
            Self::StopSupervisor => "stop",
        }
    }

    pub(in crate::tui) fn description(self) -> &'static str {
        match self {
            Self::Profiles => "Activate a saved Profile",
            Self::RestartSupervisor => "Restart the Supervisor and restore committed runtime",
            Self::StopSupervisor => "Stop the Supervisor and Managed Core",
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommandPaletteState {
    pub query: String,
    pub selected: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuleRow {
    pub index: usize,
    pub rule_string: String,
    pub rule_type: String,
    pub payload: Option<String>,
    pub policy_target: String,
    pub policy_target_validation: PolicyTargetValidation,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RulesState {
    pub rows: Vec<RuleRow>,
    pub initialized: bool,
    pub revision: Option<LocalRuleSetRevision>,
    pub selected: usize,
    pub scroll: usize,
    pub filter: String,
    pub loaded_connection_generation: Option<u64>,
    pub loaded_runtime_generation: Option<RuntimeGeneration>,
    pub load_pending: Option<PendingRuleLoad>,
    pub load_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingRuleLoad {
    pub request_id: RequestId,
    pub connection_generation: u64,
    pub runtime_generation: Option<RuntimeGeneration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogsState {
    pub records: VecDeque<ViewLogRecord>,
    pub level_filter: LogLevelFilter,
    pub search: String,
    pub since_unix_ms: Option<u64>,
    pub until_unix_ms: Option<u64>,
    pub scroll: usize,
    pub viewport_start: usize,
    pub follow: bool,
    pub paused: bool,
    pub paused_anchor: Option<u64>,
    pub gap: bool,
    pub dropped_total: u64,
    pub evicted_total: u64,
    pub retained_bytes: usize,
}

impl Default for LogsState {
    fn default() -> Self {
        Self {
            records: VecDeque::with_capacity(LOG_CAPACITY),
            level_filter: LogLevelFilter::Info,
            search: String::new(),
            since_unix_ms: None,
            until_unix_ms: None,
            scroll: 0,
            viewport_start: 0,
            follow: true,
            paused: false,
            paused_anchor: None,
            gap: false,
            dropped_total: 0,
            evicted_total: 0,
            retained_bytes: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Modal {
    Help,
    CommandPalette,
    Profiles,
    RuleEditor {
        original: Option<String>,
        value: String,
    },
    RuleRemovalConfirmation {
        rule: String,
    },
    LifecycleConfirmation {
        action: CommandPaletteAction,
    },
    Message {
        title: String,
        body: String,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingOperationKind {
    ActivateProfile,
    SelectNode,
    AddRule,
    ReplaceRule,
    RemoveRule,
    RestartSupervisor,
    StopSupervisor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingOperation {
    pub request_id: RequestId,
    pub connection_generation: u64,
    pub kind: PendingOperationKind,
}

#[derive(Clone, Debug)]
pub struct FullViewSnapshot {
    pub status: StatusSnapshot,
    pub proxy_groups: Vec<ProxyGroupRow>,
    pub proxies: Vec<ProxyRow>,
    pub profiles: Vec<ProfileRow>,
    pub logs: Vec<ViewLogRecord>,
    pub dropped_logs: u64,
}

#[derive(Clone, Debug)]
pub struct MutationSuccess {
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct ProxyGroupSnapshot {
    pub group: ProxyGroupRow,
    pub groups: Vec<ProxyGroupRow>,
    pub proxies: Vec<ProxyRow>,
}

#[derive(Clone, Debug)]
pub struct RuleListSnapshot {
    pub initialized: bool,
    pub revision: Option<LocalRuleSetRevision>,
    pub rows: Vec<RuleRow>,
}

#[derive(Clone, Debug)]
pub struct AppState {
    pub page: Page,
    pub focus: Focus,
    pub connection: ConnectionState,
    pub status: Option<StatusSnapshot>,
    pub proxies: ProxiesState,
    pub connections: ConnectionsState,
    pub profiles: ProfilesState,
    pub command_palette: CommandPaletteState,
    pub rules: RulesState,
    pub logs: LogsState,
    pub zoomed_focus: bool,
    pub modal: Option<Modal>,
    pub toast: Option<String>,
    pub pending: Option<PendingOperation>,
    pub should_quit: bool,
    pub terminal_width: u16,
    pub terminal_height: u16,
    pub render_dirty: bool,
    pub(in crate::tui) interaction_map: Option<InteractionMap>,
    next_request_id: u64,
    pub(in crate::tui) next_frame_revision: u64,
    view_revision: u64,
    pub(in crate::tui) status_revision: u64,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            page: Page::Proxies,
            focus: Focus::Content,
            connection: ConnectionState {
                status: ConnectionStatus::Connecting,
                generation: 0,
                snapshot_stale: false,
            },
            status: None,
            proxies: ProxiesState::default(),
            connections: ConnectionsState::default(),
            profiles: ProfilesState::default(),
            command_palette: CommandPaletteState::default(),
            rules: RulesState::default(),
            logs: LogsState::default(),
            zoomed_focus: false,
            modal: None,
            toast: None,
            pending: None,
            should_quit: false,
            terminal_width: MINIMUM_TERMINAL_WIDTH,
            terminal_height: MINIMUM_TERMINAL_HEIGHT,
            render_dirty: true,
            interaction_map: None,
            next_request_id: 1,
            next_frame_revision: 1,
            view_revision: 1,
            status_revision: 1,
        }
    }

    pub fn publish_interaction_map(&mut self, map: InteractionMap) {
        self.next_frame_revision = map.frame_revision.wrapping_add(1).max(1);
        self.interaction_map = Some(map);
        self.render_dirty = false;
    }

    #[must_use]
    pub fn interaction_map(&self) -> Option<&InteractionMap> {
        self.interaction_map.as_ref()
    }

    #[must_use]
    pub fn frame_revision(&self) -> u64 {
        self.interaction_map
            .as_ref()
            .map_or(0, |map| map.frame_revision)
    }

    pub(in crate::tui) fn take_request_id(&mut self) -> RequestId {
        let id = RequestId(self.next_request_id);
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        id
    }

    pub(in crate::tui) fn bump_view_revision(&mut self) {
        self.view_revision = self.view_revision.wrapping_add(1).max(1);
    }

    #[must_use]
    #[doc(hidden)]
    pub fn view_revision(&self) -> u64 {
        self.view_revision
    }

    #[must_use]
    #[doc(hidden)]
    pub fn status_revision(&self) -> u64 {
        self.status_revision
    }

    #[must_use]
    pub(crate) fn accepts_snapshot_refresh(
        &self,
        generation: u64,
        base_view_revision: u64,
    ) -> bool {
        generation == self.connection.generation
            && self.connection.status == ConnectionStatus::Connected
            && self.pending.is_none()
            && base_view_revision == self.view_revision
    }

    pub(in crate::tui) fn replace_snapshot(&mut self, generation: u64, snapshot: FullViewSnapshot) {
        self.bump_view_revision();
        self.status_revision = self.status_revision.wrapping_add(1).max(1);
        self.pending = None;
        self.profiles.activation_pending = None;
        self.proxies.group_load_pending = None;
        self.proxies.selection_pending = None;
        self.connections = ConnectionsState::default();
        self.rules.load_pending = None;
        self.rules.loaded_connection_generation = None;
        self.rules.loaded_runtime_generation = None;
        self.rules.load_error = None;
        self.connection = ConnectionState {
            status: ConnectionStatus::Connected,
            generation,
            snapshot_stale: false,
        };
        self.proxies.groups = snapshot
            .proxy_groups
            .into_iter()
            .take(MAX_ACTIVE_NODES)
            .collect();
        self.proxies.rows = snapshot
            .proxies
            .into_iter()
            .take(MAX_ACTIVE_NODES)
            .collect();
        self.profiles.rows = snapshot
            .profiles
            .into_iter()
            .take(PROFILE_VIEW_CAPACITY)
            .collect();
        self.logs.records.clear();
        self.logs.retained_bytes = 0;
        self.logs.dropped_total = self.logs.dropped_total.max(snapshot.dropped_logs);
        self.logs.gap = false;
        for record in snapshot.logs {
            push_view_log(&mut self.logs, record);
        }
        self.status = Some(snapshot.status);
        self.proxies.selected_group = self.proxies.rows.first().map(|row| row.group.clone());
        self.proxies.group_cursor = self
            .proxies
            .selected_group
            .as_ref()
            .and_then(|selected| {
                self.proxies
                    .groups
                    .iter()
                    .position(|group| &group.name == selected)
            })
            .unwrap_or(0);
        self.clamp_selections();
        self.render_dirty = true;
    }

    pub(in crate::tui) fn refresh_snapshot(
        &mut self,
        generation: u64,
        base_view_revision: u64,
        base_status_revision: u64,
        snapshot: FullViewSnapshot,
    ) {
        if !self.accepts_snapshot_refresh(generation, base_view_revision) {
            return;
        }
        let proxies = snapshot
            .proxies
            .into_iter()
            .take(MAX_ACTIVE_NODES)
            .collect::<Vec<_>>();
        let proxy_groups = snapshot
            .proxy_groups
            .into_iter()
            .take(MAX_ACTIVE_NODES)
            .collect::<Vec<_>>();
        let incoming_group = proxies.first().map(|row| row.group.as_str());
        let replace_proxies = self
            .proxies
            .selected_group
            .as_deref()
            .is_none_or(|selected| Some(selected) == incoming_group);
        let profiles = snapshot
            .profiles
            .into_iter()
            .take(PROFILE_VIEW_CAPACITY)
            .collect::<Vec<_>>();
        let replace_status = base_status_revision == self.status_revision;
        if (!replace_status || self.status.as_ref() == Some(&snapshot.status))
            && (!replace_proxies || self.proxies.rows == proxies)
            && self.proxies.groups == proxy_groups
            && self.profiles.rows == profiles
            && !self.connection.snapshot_stale
        {
            return;
        }
        let selected_proxy = filtered_proxies(&self.proxies)
            .get(self.proxies.selected)
            .and_then(|row| row.node_id.clone());
        let selected_profile = filtered_profiles(&self.profiles)
            .get(self.profiles.selected)
            .map(|row| row.id);
        let focused_group = self
            .proxies
            .groups
            .get(self.proxies.group_cursor)
            .map(|group| group.name.clone());
        if replace_proxies {
            self.proxies.rows = proxies;
        }
        self.proxies.groups = proxy_groups;
        self.profiles.rows = profiles;
        if replace_proxies
            && let Some(selected) = selected_proxy
            && let Some(index) = filtered_proxies(&self.proxies)
                .iter()
                .position(|row| row.node_id.as_ref() == Some(&selected))
        {
            self.proxies.selected = index;
        }
        if let Some(selected) = selected_profile
            && let Some(index) = filtered_profiles(&self.profiles)
                .iter()
                .position(|row| row.id == selected)
        {
            self.profiles.selected = index;
        }
        if replace_status {
            self.set_status_preserving_connection_selection(snapshot.status);
            self.status_revision = self.status_revision.wrapping_add(1).max(1);
        }
        self.connection.snapshot_stale = false;
        if replace_proxies {
            self.proxies.selected_group = self.proxies.rows.first().map(|row| row.group.clone());
        }
        self.proxies.group_cursor = self
            .proxies
            .groups
            .iter()
            .position(|group| focused_group.as_ref() == Some(&group.name))
            .or_else(|| {
                self.proxies.selected_group.as_ref().and_then(|selected| {
                    self.proxies
                        .groups
                        .iter()
                        .position(|group| &group.name == selected)
                })
            })
            .unwrap_or(0);
        self.clamp_selections();
        self.bump_view_revision();
        self.render_dirty = true;
    }

    pub(in crate::tui) fn set_status_preserving_connection_selection(
        &mut self,
        status: StatusSnapshot,
    ) {
        let selected_connection = self
            .status
            .as_ref()
            .and_then(|current| current.connections.get(self.connections.selected))
            .map(|connection| connection.id.as_str())
            .filter(|id| !id.is_empty())
            .map(str::to_owned);
        self.status = Some(status);
        if let Some(selected_connection) = selected_connection
            && let Some(index) = self.status.as_ref().and_then(|current| {
                current
                    .connections
                    .iter()
                    .position(|connection| connection.id == selected_connection)
            })
        {
            self.connections.selected = index;
        }
        self.connections.selected =
            clamp_index(self.connections.selected, self.connection_record_count());
        self.connections.scroll = self.connections.scroll.min(self.connections.selected);
    }

    pub(in crate::tui) fn clamp_selections(&mut self) {
        self.proxies.group_cursor =
            clamp_index(self.proxies.group_cursor, self.proxies.groups.len());
        self.proxies.selected = clamp_index(self.proxies.selected, self.filtered_proxy_count());
        self.profiles.selected = clamp_index(self.profiles.selected, self.filtered_profile_count());
        self.rules.selected = clamp_index(self.rules.selected, self.filtered_rule_count());
        self.connections.selected =
            clamp_index(self.connections.selected, self.connection_record_count());
        self.connections.scroll = self.connections.scroll.min(self.connections.selected);
        if self.logs.follow {
            self.logs.scroll = 0;
        } else {
            self.logs.scroll = clamp_index(self.logs.scroll, self.filtered_log_count());
        }
        self.logs.viewport_start = self
            .logs
            .viewport_start
            .min(self.filtered_log_count().saturating_sub(1));
    }

    pub(in crate::tui) fn filtered_proxy_count(&self) -> usize {
        filtered_proxies(&self.proxies).len()
    }

    pub(in crate::tui) fn filtered_profile_count(&self) -> usize {
        filtered_profiles(&self.profiles).len()
    }

    pub(in crate::tui) fn filtered_rule_count(&self) -> usize {
        filtered_rules(&self.rules).len()
    }

    pub(in crate::tui) fn filtered_log_count(&self) -> usize {
        filtered_log_indices(&self.logs).len()
    }

    pub(in crate::tui) fn connection_record_count(&self) -> usize {
        self.status
            .as_ref()
            .map_or(0, |status| status.connections.len())
    }

    pub(in crate::tui) fn rules_projection_ready(&self) -> bool {
        self.connection.status == ConnectionStatus::Connected
            && self.rules.initialized
            && self.rules.load_pending.is_none()
            && self.rules.load_error.is_none()
            && self.rules.loaded_connection_generation == Some(self.connection.generation)
            && self.status.as_ref().is_some_and(|status| {
                self.rules.loaded_runtime_generation == status.runtime_generation
            })
    }

    pub(in crate::tui) fn modal_action_pending(&self) -> bool {
        let Some(pending) = self.pending.as_ref() else {
            return false;
        };
        matches!(
            (self.modal.as_ref(), pending.kind),
            (
                Some(Modal::RuleEditor { original: None, .. }),
                PendingOperationKind::AddRule
            ) | (
                Some(Modal::RuleEditor {
                    original: Some(_),
                    ..
                }),
                PendingOperationKind::ReplaceRule
            ) | (
                Some(Modal::RuleRemovalConfirmation { .. }),
                PendingOperationKind::RemoveRule
            ) | (
                Some(Modal::LifecycleConfirmation {
                    action: CommandPaletteAction::RestartSupervisor
                }),
                PendingOperationKind::RestartSupervisor
            ) | (
                Some(Modal::LifecycleConfirmation {
                    action: CommandPaletteAction::StopSupervisor
                }),
                PendingOperationKind::StopSupervisor
            )
        )
    }
}

pub(in crate::tui) fn filtered_proxies(state: &ProxiesState) -> Vec<&ProxyRow> {
    filtered_proxy_indices(state)
        .into_iter()
        .map(|index| &state.rows[index])
        .collect()
}

pub(in crate::tui) fn filtered_proxy_indices(state: &ProxiesState) -> Vec<usize> {
    let needle = state.filter.to_lowercase();
    let mut rows = state
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| needle.is_empty() || row.name.to_lowercase().contains(&needle))
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    match state.sort {
        ProxySort::Original => {}
        ProxySort::Name => {
            rows.sort_by_cached_key(|index| state.rows[*index].name.to_lowercase());
        }
        ProxySort::Delay => rows.sort_by_key(|index| {
            let row = &state.rows[*index];
            (row.delay_ms.is_none(), row.delay_ms)
        }),
    }
    rows
}

pub(in crate::tui) fn filtered_profiles(state: &ProfilesState) -> Vec<&ProfileRow> {
    let needle = state.filter.to_lowercase();
    state
        .rows
        .iter()
        .filter(|row| needle.is_empty() || row.name.to_lowercase().contains(&needle))
        .collect()
}

pub(in crate::tui) fn filtered_rules(state: &RulesState) -> Vec<&RuleRow> {
    filtered_rule_indices(state)
        .into_iter()
        .map(|index| &state.rows[index])
        .collect()
}

pub(in crate::tui) fn filtered_rule_indices(state: &RulesState) -> Vec<usize> {
    let needle = state.filter.to_lowercase();
    state
        .rows
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            needle.is_empty()
                || row.rule_string.to_lowercase().contains(&needle)
                || row.policy_target.to_lowercase().contains(&needle)
        })
        .map(|(index, _)| index)
        .collect()
}

pub(in crate::tui) fn filtered_log_indices(state: &LogsState) -> Vec<usize> {
    let query = parse_log_query(&state.search);
    let since = match (state.since_unix_ms, query.since_unix_ms) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    };
    let until = match (state.until_unix_ms, query.until_unix_ms) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (left, right) => left.or(right),
    };
    state
        .records
        .iter()
        .enumerate()
        .filter(|(_, record)| record.source == LogSource::CoreApi)
        .filter(|(_, record)| query.level.is_some() || state.level_filter.matches(record.level))
        .filter(|(_, record)| query.level.is_none_or(|level| level.matches(record.level)))
        .filter(|(_, record)| since.is_none_or(|since| record.timestamp_unix_ms >= since))
        .filter(|(_, record)| until.is_none_or(|until| record.timestamp_unix_ms <= until))
        .filter(|(_, record)| {
            let message = record.message.to_lowercase();
            query
                .content_terms
                .iter()
                .all(|term| message.contains(term))
        })
        .map(|(index, _)| index)
        .collect()
}

pub(in crate::tui) fn selected_log_position(
    state: &LogsState,
    filtered_count: usize,
) -> Option<usize> {
    if state.follow {
        return None;
    }
    filtered_count.checked_sub(state.scroll.saturating_add(1))
}

pub(in crate::tui) fn visible_log_start(
    state: &LogsState,
    filtered_count: usize,
    visible_count: usize,
) -> usize {
    selected_log_position(state, filtered_count).map_or_else(
        || filtered_count.saturating_sub(visible_count),
        |selected| {
            visible_window_start(
                state.viewport_start,
                selected,
                visible_count,
                filtered_count,
            )
        },
    )
}

pub(in crate::tui) fn visible_window_start(
    current: usize,
    selected: usize,
    visible_count: usize,
    total: usize,
) -> usize {
    if total == 0 || visible_count >= total {
        return 0;
    }
    if visible_count == 0 {
        return selected.min(total.saturating_sub(1));
    }
    let current = current.min(total.saturating_sub(visible_count));
    if selected < current {
        selected
    } else if selected >= current.saturating_add(visible_count) {
        selected
            .saturating_add(1)
            .saturating_sub(visible_count)
            .min(total.saturating_sub(visible_count))
    } else {
        current
    }
}

#[derive(Debug, Default)]
struct LogQuery {
    since_unix_ms: Option<u64>,
    until_unix_ms: Option<u64>,
    level: Option<LogLevelFilter>,
    content_terms: Vec<String>,
}

fn parse_log_query(input: &str) -> LogQuery {
    let mut query = LogQuery::default();
    for token in input.split_whitespace() {
        let lower = token.to_lowercase();
        if let Some(value) = lower.strip_prefix("since:")
            && let Ok(value) = value.parse()
        {
            query.since_unix_ms = Some(value);
        } else if let Some(value) = lower.strip_prefix("until:")
            && let Ok(value) = value.parse()
        {
            query.until_unix_ms = Some(value);
        } else if let Some(value) = lower.strip_prefix("level:")
            && let Some(level) = parse_log_level_filter(value)
        {
            query.level = Some(level);
        } else if let Some(value) = lower.strip_prefix("content:") {
            if !value.is_empty() {
                query.content_terms.push(value.to_owned());
            }
        } else {
            query.content_terms.push(lower);
        }
    }
    query
}

fn parse_log_level_filter(value: &str) -> Option<LogLevelFilter> {
    match value {
        "all" => Some(LogLevelFilter::All),
        "debug" => Some(LogLevelFilter::Debug),
        "info" => Some(LogLevelFilter::Info),
        "warn" | "warning" => Some(LogLevelFilter::Warn),
        "error" => Some(LogLevelFilter::Error),
        _ => None,
    }
}

pub(in crate::tui) fn clamp_index(index: usize, length: usize) -> usize {
    index.min(length.saturating_sub(1))
}

pub(in crate::tui) fn moved_index(index: usize, length: usize, delta: isize) -> usize {
    if length == 0 {
        return 0;
    }
    if delta < 0 {
        index.saturating_sub(delta.unsigned_abs())
    } else {
        index.saturating_add(delta as usize).min(length - 1)
    }
}

pub(in crate::tui) fn push_view_log(logs: &mut LogsState, mut record: ViewLogRecord) {
    record.message = record.message.into_boxed_str().into_string();
    while logs.records.len() == LOG_CAPACITY
        || logs.retained_bytes.saturating_add(record.message.len()) > LOG_RETENTION_MAX_BYTES
    {
        let Some(evicted) = logs.records.pop_front() else {
            break;
        };
        logs.retained_bytes = logs.retained_bytes.saturating_sub(evicted.message.len());
        logs.evicted_total = logs.evicted_total.saturating_add(1);
    }
    if logs.retained_bytes.saturating_add(record.message.len()) > LOG_RETENTION_MAX_BYTES {
        logs.evicted_total = logs.evicted_total.saturating_add(1);
        logs.gap = true;
        return;
    }
    logs.retained_bytes = logs.retained_bytes.saturating_add(record.message.len());
    logs.records.push_back(record);
}

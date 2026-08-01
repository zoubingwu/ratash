//! State, reducer, and input contracts for the Ratatui Status Interface.

use std::collections::VecDeque;
use std::fmt;

use crossterm::event::{
    Event as CrosstermEvent, KeyCode, KeyEventKind, KeyModifiers,
    MouseButton as CrosstermMouseButton, MouseEventKind,
};
use ratatui::layout::Rect;

use crate::application::{LatencyFreshness, LatencyProbeStatus};
use crate::constants::{
    LOG_CAPACITY, LOG_RETENTION_MAX_BYTES, MAX_ACTIVE_NODES, MINIMUM_TERMINAL_HEIGHT,
    MINIMUM_TERMINAL_WIDTH, TRAFFIC_SERIES_CAPACITY, TUI_SEARCH_MAX_BYTES,
    TUI_SEARCH_MAX_CHARACTERS,
};
use crate::domain::{NodeRecordId, ProfileId, ProxyGroupId, StatusSnapshot};
use crate::ipc::RequestId;
use crate::telemetry::{CoreLogRecord, LogLevel, LogSource};

mod event_inbox;
mod render;
mod terminal;

pub use event_inbox::{EventBudgets, EventInboxError, EventSource, FairEventInbox};
pub use render::{LayoutRegions, compute_layout, render, render_buffer};
pub use terminal::{
    CrosstermControl, TerminalAction, TerminalControl, TerminalSession, TerminalSessionError,
};

pub const PROFILE_VIEW_CAPACITY: usize = 100;
pub const EVENT_SOURCE_CAPACITY: usize = 256;

// -----------------------------------------------------------------------------
// State and reducer contract
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Page {
    #[default]
    Overview,
    Proxies,
    Profiles,
    Logs,
}

impl Page {
    const ALL: [Self; 4] = [Self::Overview, Self::Proxies, Self::Profiles, Self::Logs];

    fn index(self) -> usize {
        match self {
            Self::Overview => 0,
            Self::Proxies => 1,
            Self::Profiles => 2,
            Self::Logs => 3,
        }
    }

    fn next(self) -> Self {
        Self::ALL[(self.index() + 1) % Self::ALL.len()]
    }

    fn previous(self) -> Self {
        Self::ALL[(self.index() + Self::ALL.len() - 1) % Self::ALL.len()]
    }

    fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Proxies => "Proxies",
            Self::Profiles => "Profiles",
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

impl Focus {
    fn next(self) -> Self {
        match self {
            Self::Tabs => Self::ProxyGroups,
            Self::ProxyGroups => Self::Content,
            Self::Content => Self::Search,
            Self::Search => Self::FooterHelp,
            Self::FooterHelp => Self::FooterQuit,
            Self::FooterQuit | Self::Modal => Self::Tabs,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Tabs => Self::FooterQuit,
            Self::ProxyGroups => Self::Tabs,
            Self::Content => Self::ProxyGroups,
            Self::Search => Self::Content,
            Self::FooterHelp => Self::Search,
            Self::FooterQuit => Self::FooterHelp,
            Self::Modal => Self::Modal,
        }
    }
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
    pub sampled_at_unix_ms: Option<u64>,
    pub freshness: LatencyFreshness,
    pub probe_status: LatencyProbeStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProxyGroupRow {
    pub id: ProxyGroupId,
    pub name: String,
    pub proxy_type: String,
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
    const ALL: [Self; 3] = [Self::Original, Self::Name, Self::Delay];

    fn title(self) -> &'static str {
        match self {
            Self::Original => "Original",
            Self::Name => "Name",
            Self::Delay => "Delay",
        }
    }

    fn next(self) -> Self {
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
    const ALL: [Self; 5] = [Self::All, Self::Debug, Self::Info, Self::Warn, Self::Error];

    fn title(self) -> &'static str {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogsState {
    pub records: VecDeque<ViewLogRecord>,
    pub level_filter: LogLevelFilter,
    pub search: String,
    pub since_unix_ms: Option<u64>,
    pub until_unix_ms: Option<u64>,
    pub scroll: usize,
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
            level_filter: LogLevelFilter::All,
            search: String::new(),
            since_unix_ms: None,
            until_unix_ms: None,
            scroll: 0,
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
    Message { title: String, body: String },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingOperationKind {
    ActivateProfile,
    SelectNode,
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
pub struct AppState {
    pub page: Page,
    pub focus: Focus,
    pub connection: ConnectionState,
    pub status: Option<StatusSnapshot>,
    pub proxies: ProxiesState,
    pub profiles: ProfilesState,
    pub logs: LogsState,
    pub upload_series: VecDeque<u64>,
    pub download_series: VecDeque<u64>,
    pub modal: Option<Modal>,
    pub toast: Option<String>,
    pub pending: Option<PendingOperation>,
    pub should_quit: bool,
    pub terminal_width: u16,
    pub terminal_height: u16,
    pub render_dirty: bool,
    interaction_map: Option<InteractionMap>,
    next_request_id: u64,
    next_frame_revision: u64,
    view_revision: u64,
    status_revision: u64,
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
            page: Page::Overview,
            focus: Focus::Content,
            connection: ConnectionState {
                status: ConnectionStatus::Connecting,
                generation: 0,
                snapshot_stale: false,
            },
            status: None,
            proxies: ProxiesState::default(),
            profiles: ProfilesState::default(),
            logs: LogsState::default(),
            upload_series: VecDeque::with_capacity(TRAFFIC_SERIES_CAPACITY),
            download_series: VecDeque::with_capacity(TRAFFIC_SERIES_CAPACITY),
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

    fn take_request_id(&mut self) -> RequestId {
        let id = RequestId(self.next_request_id);
        self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
        id
    }

    fn bump_view_revision(&mut self) {
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

    fn replace_snapshot(&mut self, generation: u64, snapshot: FullViewSnapshot) {
        self.bump_view_revision();
        self.status_revision = self.status_revision.wrapping_add(1).max(1);
        self.pending = None;
        self.profiles.activation_pending = None;
        self.proxies.group_load_pending = None;
        self.proxies.selection_pending = None;
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
        self.push_traffic_sample();
        self.clamp_selections();
        self.render_dirty = true;
    }

    fn refresh_snapshot(
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
            self.status = Some(snapshot.status);
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

    fn push_traffic_sample(&mut self) {
        let Some(status) = &self.status else {
            return;
        };
        push_bounded(
            &mut self.upload_series,
            status.traffic.upload_bytes_per_second,
            TRAFFIC_SERIES_CAPACITY,
        );
        push_bounded(
            &mut self.download_series,
            status.traffic.download_bytes_per_second,
            TRAFFIC_SERIES_CAPACITY,
        );
    }

    fn clamp_selections(&mut self) {
        self.proxies.group_cursor =
            clamp_index(self.proxies.group_cursor, self.proxies.groups.len());
        self.proxies.selected = clamp_index(self.proxies.selected, self.filtered_proxy_count());
        self.profiles.selected = clamp_index(self.profiles.selected, self.filtered_profile_count());
    }

    fn filtered_proxy_count(&self) -> usize {
        filtered_proxies(&self.proxies).len()
    }

    fn filtered_profile_count(&self) -> usize {
        filtered_profiles(&self.profiles).len()
    }
}

#[derive(Clone, Debug)]
pub enum UiEvent {
    Terminal(TerminalInput),
    Intent(UiIntent),
    Connected {
        connection_generation: u64,
        snapshot: FullViewSnapshot,
    },
    Disconnected {
        connection_generation: u64,
    },
    StatusSnapshot {
        connection_generation: u64,
        status: StatusSnapshot,
    },
    SnapshotRefreshed {
        connection_generation: u64,
        base_view_revision: u64,
        base_status_revision: u64,
        snapshot: FullViewSnapshot,
    },
    SnapshotRefreshFailed {
        connection_generation: u64,
        base_view_revision: u64,
    },
    ProxyGroupLoaded {
        request_id: RequestId,
        connection_generation: u64,
        result: Result<ProxyGroupSnapshot, String>,
    },
    LogBatch {
        connection_generation: u64,
        records: Vec<ViewLogRecord>,
        gap: bool,
        dropped_total: u64,
    },
    CommandResult {
        request_id: RequestId,
        connection_generation: u64,
        result: Result<MutationSuccess, String>,
    },
    ReconnectDeadline {
        connection_generation: u64,
    },
    Resize {
        width: u16,
        height: u16,
    },
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UiIntent {
    SwitchPage(Page),
    NextPage,
    PreviousPage,
    FocusNext,
    FocusPrevious,
    FocusSearch,
    PreviousProxyGroup,
    NextProxyGroup,
    ShowProxyGroup(ProxyGroupId),
    InputCharacter(char),
    Backspace,
    Escape,
    MoveUp,
    MoveDown,
    ActivateSelected,
    ActivateProfile(ProfileId),
    SelectNode {
        group_id: ProxyGroupId,
        node_id: NodeRecordId,
    },
    ScrollUp,
    ScrollDown,
    SetProxySort(ProxySort),
    SetLogLevel(LogLevelFilter),
    ToggleLogPause,
    FollowLogs,
    ToggleHelp,
    CloseModal,
    Quit,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Connect {
        connection_generation: u64,
    },
    ScheduleReconnect {
        connection_generation: u64,
    },
    ActivateProfile {
        request_id: RequestId,
        connection_generation: u64,
        profile_id: ProfileId,
    },
    SelectNode {
        request_id: RequestId,
        connection_generation: u64,
        group_id: ProxyGroupId,
        node_id: NodeRecordId,
    },
    FetchProxyGroup {
        request_id: RequestId,
        connection_generation: u64,
        group_id: ProxyGroupId,
    },
    FetchLogTail {
        connection_generation: u64,
        after_sequence: Option<u64>,
    },
    RefreshSnapshot {
        connection_generation: u64,
        base_view_revision: u64,
        base_status_revision: u64,
    },
    Cancel {
        request_id: RequestId,
    },
}

pub fn update(state: &mut AppState, event: UiEvent) -> Vec<Command> {
    match event {
        UiEvent::Terminal(input) => input_to_intent(state, input)
            .map_or_else(Vec::new, |intent| apply_intent(state, intent)),
        UiEvent::Intent(intent) => apply_intent(state, intent),
        UiEvent::Connected {
            connection_generation,
            snapshot,
        } => {
            if connection_generation >= state.connection.generation {
                state.replace_snapshot(connection_generation, snapshot);
                state.toast = Some("Connected".to_owned());
            }
            Vec::new()
        }
        UiEvent::Disconnected {
            connection_generation,
        } => {
            if connection_generation == state.connection.generation {
                let mut commands = cancel_pending(state);
                state.connection.status = ConnectionStatus::Disconnected;
                state.connection.snapshot_stale = state.status.is_some();
                state.render_dirty = true;
                commands.push(Command::ScheduleReconnect {
                    connection_generation,
                });
                commands
            } else {
                Vec::new()
            }
        }
        UiEvent::StatusSnapshot {
            connection_generation,
            status,
        } => {
            if connection_generation == state.connection.generation
                && state.connection.status == ConnectionStatus::Connected
                && state.status.as_ref() != Some(&status)
            {
                let collection_changed = state
                    .status
                    .as_ref()
                    .is_some_and(|previous| status_requires_snapshot_refresh(previous, &status));
                state.status = Some(status);
                state.status_revision = state.status_revision.wrapping_add(1).max(1);
                if collection_changed {
                    state.bump_view_revision();
                }
                state.push_traffic_sample();
                state.render_dirty = true;
            }
            Vec::new()
        }
        UiEvent::SnapshotRefreshed {
            connection_generation,
            base_view_revision,
            base_status_revision,
            snapshot,
        } => {
            state.refresh_snapshot(
                connection_generation,
                base_view_revision,
                base_status_revision,
                snapshot,
            );
            Vec::new()
        }
        UiEvent::SnapshotRefreshFailed {
            connection_generation,
            base_view_revision,
        } => {
            if state.accepts_snapshot_refresh(connection_generation, base_view_revision) {
                state.connection.snapshot_stale = state.status.is_some();
                state.render_dirty = true;
            }
            Vec::new()
        }
        UiEvent::ProxyGroupLoaded {
            request_id,
            connection_generation,
            result,
        } => {
            let current = state
                .proxies
                .group_load_pending
                .as_ref()
                .is_some_and(|pending| {
                    pending.request_id == request_id
                        && pending.connection_generation == connection_generation
                        && connection_generation == state.connection.generation
                });
            if current {
                state.proxies.group_load_pending = None;
                match result {
                    Ok(snapshot) => {
                        state.proxies.groups =
                            snapshot.groups.into_iter().take(MAX_ACTIVE_NODES).collect();
                        state.proxies.rows = snapshot
                            .proxies
                            .into_iter()
                            .take(MAX_ACTIVE_NODES)
                            .collect();
                        state.proxies.selected_group = Some(snapshot.group.name.clone());
                        state.proxies.group_cursor = state
                            .proxies
                            .groups
                            .iter()
                            .position(|group| group.name == snapshot.group.name)
                            .unwrap_or(0);
                        state.proxies.selected = 0;
                        state.proxies.scroll = 0;
                        state.bump_view_revision();
                        state.toast = Some(format!(
                            "Success: Loaded Proxy Group {}",
                            snapshot.group.name
                        ));
                    }
                    Err(message) => state.toast = Some(format!("Error: {message}")),
                }
                state.clamp_selections();
                state.render_dirty = true;
            }
            Vec::new()
        }
        UiEvent::LogBatch {
            connection_generation,
            records,
            gap,
            dropped_total,
        } => {
            if connection_generation == state.connection.generation && !state.logs.paused {
                for record in records {
                    push_view_log(&mut state.logs, record);
                }
                state.logs.gap |= gap;
                state.logs.dropped_total = state.logs.dropped_total.max(dropped_total);
                if state.logs.follow {
                    state.logs.scroll = 0;
                }
                state.render_dirty = true;
            }
            Vec::new()
        }
        UiEvent::CommandResult {
            request_id,
            connection_generation,
            result,
        } => {
            let current = state.pending.as_ref().is_some_and(|pending| {
                pending.request_id == request_id
                    && pending.connection_generation == connection_generation
                    && connection_generation == state.connection.generation
            });
            if current {
                match result {
                    Ok(success) => {
                        if state.pending.as_ref().is_some_and(|pending| {
                            pending.kind == PendingOperationKind::ActivateProfile
                        }) {
                            state.profiles.activation_pending = None;
                        }
                        state.pending = None;
                        state.proxies.selection_pending = None;
                        state.connection.snapshot_stale = state.status.is_some();
                        state.bump_view_revision();
                        state.toast = Some(format!("Success: {}", success.message));
                        state.render_dirty = true;
                    }
                    Err(message) => {
                        if state.pending.as_ref().is_some_and(|pending| {
                            pending.kind == PendingOperationKind::ActivateProfile
                        }) {
                            state.profiles.activation_pending = None;
                        }
                        state.pending = None;
                        state.proxies.selection_pending = None;
                        state.toast = Some(format!("Error: {message}"));
                        state.render_dirty = true;
                    }
                }
            }
            Vec::new()
        }
        UiEvent::ReconnectDeadline {
            connection_generation,
        } => {
            if state.connection.status == ConnectionStatus::Disconnected
                && connection_generation == state.connection.generation
            {
                let next_generation = connection_generation.wrapping_add(1);
                state.connection.status = ConnectionStatus::Connecting;
                state.connection.generation = next_generation;
                state.render_dirty = true;
                vec![Command::Connect {
                    connection_generation: next_generation,
                }]
            } else {
                Vec::new()
            }
        }
        UiEvent::Resize { width, height } => {
            state.terminal_width = width;
            state.terminal_height = height;
            state.interaction_map = None;
            state.render_dirty = true;
            Vec::new()
        }
        UiEvent::Shutdown => {
            state.should_quit = true;
            state.render_dirty = true;
            Vec::new()
        }
    }
}

pub(crate) fn status_requires_snapshot_refresh(
    previous: &StatusSnapshot,
    next: &StatusSnapshot,
) -> bool {
    previous.active_profile != next.active_profile
        || previous.runtime_generation != next.runtime_generation
        || previous.core.instance_generation != next.core.instance_generation
        || previous.primary_proxy_group != next.primary_proxy_group
        || previous.selected_node != next.selected_node
        || previous.latency != next.latency
}

fn apply_intent(state: &mut AppState, intent: UiIntent) -> Vec<Command> {
    let commands = match intent {
        UiIntent::SwitchPage(page) => {
            state.page = page;
            state.focus = Focus::Content;
            Vec::new()
        }
        UiIntent::NextPage => {
            state.page = state.page.next();
            state.focus = Focus::Content;
            Vec::new()
        }
        UiIntent::PreviousPage => {
            state.page = state.page.previous();
            state.focus = Focus::Content;
            Vec::new()
        }
        UiIntent::FocusNext => {
            let mut focus = state.focus.next();
            while !focus_available(state.page, focus) {
                focus = focus.next();
            }
            state.focus = focus;
            Vec::new()
        }
        UiIntent::FocusPrevious => {
            let mut focus = state.focus.previous();
            while !focus_available(state.page, focus) {
                focus = focus.previous();
            }
            state.focus = focus;
            Vec::new()
        }
        UiIntent::FocusSearch => {
            if state.page != Page::Overview {
                state.focus = Focus::Search;
            }
            Vec::new()
        }
        UiIntent::PreviousProxyGroup => move_proxy_group(state, -1),
        UiIntent::NextProxyGroup => move_proxy_group(state, 1),
        UiIntent::ShowProxyGroup(group_id) => issue_proxy_group_load(state, group_id),
        UiIntent::InputCharacter(character) => {
            append_search(state, character);
            Vec::new()
        }
        UiIntent::Backspace => {
            if let Some(search) = current_search_mut(state) {
                search.pop();
            }
            state.clamp_selections();
            Vec::new()
        }
        UiIntent::Escape => {
            state.modal = None;
            state.focus = Focus::Content;
            Vec::new()
        }
        UiIntent::MoveUp | UiIntent::ScrollUp => {
            move_selection(state, -1);
            Vec::new()
        }
        UiIntent::MoveDown | UiIntent::ScrollDown => {
            move_selection(state, 1);
            Vec::new()
        }
        UiIntent::ActivateSelected => activate_selected(state),
        UiIntent::ActivateProfile(profile_id) => issue_profile_activation(state, profile_id),
        UiIntent::SelectNode { group_id, node_id } => {
            issue_node_selection(state, group_id, node_id)
        }
        UiIntent::SetProxySort(sort) => {
            state.proxies.sort = sort;
            state.proxies.selected = 0;
            state.proxies.scroll = 0;
            Vec::new()
        }
        UiIntent::SetLogLevel(level) => {
            state.logs.level_filter = level;
            state.logs.scroll = 0;
            Vec::new()
        }
        UiIntent::ToggleLogPause => {
            if state.logs.paused {
                state.logs.paused = false;
                state.logs.follow = true;
                vec![Command::FetchLogTail {
                    connection_generation: state.connection.generation,
                    after_sequence: state.logs.paused_anchor.take(),
                }]
            } else {
                state.logs.paused = true;
                state.logs.follow = false;
                state.logs.paused_anchor = state.logs.records.back().map(|record| record.sequence);
                Vec::new()
            }
        }
        UiIntent::FollowLogs => {
            state.logs.follow = true;
            state.logs.scroll = 0;
            Vec::new()
        }
        UiIntent::ToggleHelp => {
            if state.modal == Some(Modal::Help) {
                state.modal = None;
                state.focus = Focus::Content;
            } else {
                state.modal = Some(Modal::Help);
                state.focus = Focus::Modal;
            }
            Vec::new()
        }
        UiIntent::CloseModal => {
            state.modal = None;
            state.focus = Focus::Content;
            Vec::new()
        }
        UiIntent::Quit => {
            state.should_quit = true;
            Vec::new()
        }
    };
    state.render_dirty = true;
    commands
}

fn append_search(state: &mut AppState, character: char) {
    if !character.is_control()
        && let Some(search) = current_search_mut(state)
        && search.chars().count() < TUI_SEARCH_MAX_CHARACTERS
        && search.len().saturating_add(character.len_utf8()) <= TUI_SEARCH_MAX_BYTES
    {
        search.push(character);
        state.clamp_selections();
    }
}

fn focus_available(page: Page, focus: Focus) -> bool {
    match focus {
        Focus::ProxyGroups => page == Page::Proxies,
        Focus::Search => page != Page::Overview,
        Focus::Modal => false,
        Focus::Tabs | Focus::Content | Focus::FooterHelp | Focus::FooterQuit => true,
    }
}

fn move_proxy_group(state: &mut AppState, delta: isize) -> Vec<Command> {
    state.proxies.group_cursor = moved_index(
        state.proxies.group_cursor,
        state.proxies.groups.len(),
        delta,
    );
    Vec::new()
}

fn issue_proxy_group_load(state: &mut AppState, group_id: ProxyGroupId) -> Vec<Command> {
    let Some((index, group_name)) = state
        .proxies
        .groups
        .iter()
        .enumerate()
        .find(|(_, candidate)| candidate.id == group_id)
        .map(|(index, group)| (index, group.name.clone()))
    else {
        return Vec::new();
    };
    state.proxies.group_cursor = index;
    if state.proxies.selected_group.as_deref() == Some(group_name.as_str())
        && state.proxies.group_load_pending.is_none()
    {
        return Vec::new();
    }
    let request_id = state.take_request_id();
    let mut commands = cancel_group_load(state);
    state.proxies.group_load_pending = Some(PendingProxyGroupLoad {
        request_id,
        connection_generation: state.connection.generation,
        group_id: group_id.clone(),
    });
    commands.push(Command::FetchProxyGroup {
        request_id,
        connection_generation: state.connection.generation,
        group_id,
    });
    commands
}

fn current_search_mut(state: &mut AppState) -> Option<&mut String> {
    match state.page {
        Page::Overview => None,
        Page::Proxies => Some(&mut state.proxies.filter),
        Page::Profiles => Some(&mut state.profiles.filter),
        Page::Logs => Some(&mut state.logs.search),
    }
}

fn move_selection(state: &mut AppState, delta: isize) {
    match state.page {
        Page::Overview => {}
        Page::Proxies => {
            state.proxies.selected =
                moved_index(state.proxies.selected, state.filtered_proxy_count(), delta);
            state.proxies.scroll = state.proxies.selected;
        }
        Page::Profiles => {
            state.profiles.selected = moved_index(
                state.profiles.selected,
                state.filtered_profile_count(),
                delta,
            );
            state.profiles.scroll = state.profiles.selected;
        }
        Page::Logs => {
            state.logs.scroll = if delta < 0 {
                state.logs.follow = false;
                state.logs.scroll.saturating_add(1)
            } else {
                let scroll = state.logs.scroll.saturating_sub(1);
                if scroll == 0 {
                    state.logs.follow = true;
                }
                scroll
            };
        }
    }
}

fn activate_selected(state: &mut AppState) -> Vec<Command> {
    match state.page {
        Page::Proxies => filtered_proxies(&state.proxies)
            .get(state.proxies.selected)
            .and_then(|row| {
                row.node_id
                    .clone()
                    .map(|node_id| (row.group_id.clone(), node_id))
            })
            .map_or_else(Vec::new, |(group_id, node_id)| {
                issue_node_selection(state, group_id, node_id)
            }),
        Page::Profiles => filtered_profiles(&state.profiles)
            .get(state.profiles.selected)
            .map(|row| row.id)
            .map_or_else(Vec::new, |profile_id| {
                issue_profile_activation(state, profile_id)
            }),
        Page::Logs => apply_intent(state, UiIntent::ToggleLogPause),
        Page::Overview => Vec::new(),
    }
}

fn issue_profile_activation(state: &mut AppState, profile_id: ProfileId) -> Vec<Command> {
    let request_id = state.take_request_id();
    let mut commands = cancel_pending(state);
    state.profiles.activation_pending = Some(profile_id);
    state.pending = Some(PendingOperation {
        request_id,
        connection_generation: state.connection.generation,
        kind: PendingOperationKind::ActivateProfile,
    });
    state.bump_view_revision();
    commands.push(Command::ActivateProfile {
        request_id,
        connection_generation: state.connection.generation,
        profile_id,
    });
    commands
}

fn issue_node_selection(
    state: &mut AppState,
    group_id: ProxyGroupId,
    node_id: NodeRecordId,
) -> Vec<Command> {
    let request_id = state.take_request_id();
    let mut commands = cancel_pending(state);
    state.pending = Some(PendingOperation {
        request_id,
        connection_generation: state.connection.generation,
        kind: PendingOperationKind::SelectNode,
    });
    if let Some(group) = state
        .proxies
        .groups
        .iter()
        .find(|group| group.id == group_id)
    {
        state.proxies.selected_group = Some(group.name.clone());
    }
    state.proxies.selection_pending = Some((group_id.clone(), node_id.clone()));
    state.bump_view_revision();
    commands.push(Command::SelectNode {
        request_id,
        connection_generation: state.connection.generation,
        group_id,
        node_id,
    });
    commands
}

fn cancel_pending(state: &mut AppState) -> Vec<Command> {
    let mut commands = cancel_mutation(state);
    commands.extend(cancel_group_load(state));
    commands
}

fn cancel_mutation(state: &mut AppState) -> Vec<Command> {
    state.pending.take().map_or_else(Vec::new, |pending| {
        if pending.kind == PendingOperationKind::ActivateProfile {
            state.profiles.activation_pending = None;
        }
        state.proxies.selection_pending = None;
        vec![Command::Cancel {
            request_id: pending.request_id,
        }]
    })
}

fn cancel_group_load(state: &mut AppState) -> Vec<Command> {
    state
        .proxies
        .group_load_pending
        .take()
        .map_or_else(Vec::new, |pending| {
            vec![Command::Cancel {
                request_id: pending.request_id,
            }]
        })
}

fn filtered_proxies(state: &ProxiesState) -> Vec<&ProxyRow> {
    let needle = state.filter.to_lowercase();
    let mut rows = state
        .rows
        .iter()
        .filter(|row| needle.is_empty() || row.name.to_lowercase().contains(&needle))
        .collect::<Vec<_>>();
    match state.sort {
        ProxySort::Original => {}
        ProxySort::Name => rows.sort_by_key(|row| row.name.to_lowercase()),
        ProxySort::Delay => rows.sort_by_key(|row| (row.delay_ms.is_none(), row.delay_ms)),
    }
    rows
}

fn filtered_profiles(state: &ProfilesState) -> Vec<&ProfileRow> {
    let needle = state.filter.to_lowercase();
    state
        .rows
        .iter()
        .filter(|row| needle.is_empty() || row.name.to_lowercase().contains(&needle))
        .collect()
}

fn filtered_logs(state: &LogsState) -> Vec<&ViewLogRecord> {
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
        .filter(|record| state.level_filter.matches(record.level))
        .filter(|record| query.level.is_none_or(|level| level.matches(record.level)))
        .filter(|record| since.is_none_or(|since| record.timestamp_unix_ms >= since))
        .filter(|record| until.is_none_or(|until| record.timestamp_unix_ms <= until))
        .filter(|record| {
            let message = record.message.to_lowercase();
            query
                .content_terms
                .iter()
                .all(|term| message.contains(term))
        })
        .collect()
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

fn clamp_index(index: usize, length: usize) -> usize {
    index.min(length.saturating_sub(1))
}

fn moved_index(index: usize, length: usize, delta: isize) -> usize {
    if length == 0 {
        return 0;
    }
    if delta < 0 {
        index.saturating_sub(delta.unsigned_abs())
    } else {
        index.saturating_add(delta as usize).min(length - 1)
    }
}

fn push_view_log(logs: &mut LogsState, mut record: ViewLogRecord) {
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

fn push_bounded(values: &mut VecDeque<u64>, value: u64, capacity: usize) {
    if values.len() == capacity {
        values.pop_front();
    }
    values.push_back(value);
}

// -----------------------------------------------------------------------------
// Input mapping and shared interaction map
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyInput {
    Character(char),
    Enter,
    Escape,
    Tab,
    BackTab,
    Up,
    Down,
    Left,
    Right,
    Backspace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MouseInputKind {
    LeftClick,
    ScrollUp,
    ScrollDown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MouseInput {
    pub kind: MouseInputKind,
    pub column: u16,
    pub row: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalInput {
    Key(KeyInput),
    Mouse(MouseInput),
}

pub fn from_crossterm_event(event: CrosstermEvent) -> Option<UiEvent> {
    match event {
        CrosstermEvent::Key(event) if event.kind == KeyEventKind::Press => {
            let key = match event.code {
                KeyCode::Char(character) => KeyInput::Character(character),
                KeyCode::Enter => KeyInput::Enter,
                KeyCode::Esc => KeyInput::Escape,
                KeyCode::Tab => KeyInput::Tab,
                KeyCode::BackTab => KeyInput::BackTab,
                KeyCode::Up => KeyInput::Up,
                KeyCode::Down => KeyInput::Down,
                KeyCode::Left => KeyInput::Left,
                KeyCode::Right => KeyInput::Right,
                KeyCode::Backspace => KeyInput::Backspace,
                _ => return None,
            };
            let key = if event.modifiers.contains(KeyModifiers::SHIFT) && key == KeyInput::Tab {
                KeyInput::BackTab
            } else {
                key
            };
            Some(UiEvent::Terminal(TerminalInput::Key(key)))
        }
        CrosstermEvent::Mouse(event) => {
            let kind = match event.kind {
                MouseEventKind::Down(CrosstermMouseButton::Left) => MouseInputKind::LeftClick,
                MouseEventKind::ScrollUp => MouseInputKind::ScrollUp,
                MouseEventKind::ScrollDown => MouseInputKind::ScrollDown,
                _ => return None,
            };
            Some(UiEvent::Terminal(TerminalInput::Mouse(MouseInput {
                kind,
                column: event.column,
                row: event.row,
            })))
        }
        CrosstermEvent::Resize(width, height) => Some(UiEvent::Resize { width, height }),
        _ => None,
    }
}

pub fn input_to_intent(state: &AppState, input: TerminalInput) -> Option<UiIntent> {
    if let TerminalInput::Mouse(mouse) = input {
        return state
            .interaction_map
            .as_ref()
            .and_then(|map| map.intent_for(mouse));
    }
    let TerminalInput::Key(key) = input else {
        return None;
    };

    if state.modal.is_some() {
        return match key {
            KeyInput::Escape | KeyInput::Enter | KeyInput::Character('q') => {
                Some(UiIntent::CloseModal)
            }
            _ => None,
        };
    }
    if state.focus == Focus::Search {
        return match key {
            KeyInput::Escape | KeyInput::Enter => Some(UiIntent::Escape),
            KeyInput::Backspace => Some(UiIntent::Backspace),
            KeyInput::Tab => Some(UiIntent::FocusNext),
            KeyInput::BackTab => Some(UiIntent::FocusPrevious),
            KeyInput::Character(character) => Some(UiIntent::InputCharacter(character)),
            _ => None,
        };
    }

    match key {
        KeyInput::Character('1') => Some(UiIntent::SwitchPage(Page::Overview)),
        KeyInput::Character('2') => Some(UiIntent::SwitchPage(Page::Proxies)),
        KeyInput::Character('3') => Some(UiIntent::SwitchPage(Page::Profiles)),
        KeyInput::Character('4') => Some(UiIntent::SwitchPage(Page::Logs)),
        KeyInput::Character('j') | KeyInput::Down if state.focus == Focus::ProxyGroups => {
            Some(UiIntent::NextProxyGroup)
        }
        KeyInput::Character('k') | KeyInput::Up if state.focus == Focus::ProxyGroups => {
            Some(UiIntent::PreviousProxyGroup)
        }
        KeyInput::Character('j') | KeyInput::Down => Some(UiIntent::MoveDown),
        KeyInput::Character('k') | KeyInput::Up => Some(UiIntent::MoveUp),
        KeyInput::Enter => selected_intent(state),
        KeyInput::Left if state.focus == Focus::ProxyGroups => Some(UiIntent::PreviousProxyGroup),
        KeyInput::Right if state.focus == Focus::ProxyGroups => Some(UiIntent::NextProxyGroup),
        KeyInput::Left => Some(UiIntent::PreviousPage),
        KeyInput::Right => Some(UiIntent::NextPage),
        KeyInput::Tab => Some(UiIntent::FocusNext),
        KeyInput::BackTab => Some(UiIntent::FocusPrevious),
        KeyInput::Character('/') => Some(UiIntent::FocusSearch),
        KeyInput::Character('?') => Some(UiIntent::ToggleHelp),
        KeyInput::Character('q') => Some(UiIntent::Quit),
        KeyInput::Escape => Some(UiIntent::Escape),
        KeyInput::Character('s') if state.page == Page::Proxies => {
            Some(UiIntent::SetProxySort(state.proxies.sort.next()))
        }
        KeyInput::Character('a') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::All))
        }
        KeyInput::Character('d') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Debug))
        }
        KeyInput::Character('i') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Info))
        }
        KeyInput::Character('w') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Warn))
        }
        KeyInput::Character('e') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Error))
        }
        KeyInput::Character('p') if state.page == Page::Logs => Some(UiIntent::ToggleLogPause),
        KeyInput::Character('f') if state.page == Page::Logs => Some(UiIntent::FollowLogs),
        _ => None,
    }
}

fn selected_intent(state: &AppState) -> Option<UiIntent> {
    match state.focus {
        Focus::Tabs => return Some(UiIntent::SwitchPage(state.page)),
        Focus::ProxyGroups => {
            return state
                .proxies
                .groups
                .get(state.proxies.group_cursor)
                .map(|group| UiIntent::ShowProxyGroup(group.id.clone()));
        }
        Focus::FooterHelp => return Some(UiIntent::ToggleHelp),
        Focus::FooterQuit => return Some(UiIntent::Quit),
        Focus::Search | Focus::Modal => return None,
        Focus::Content => {}
    }
    match state.page {
        Page::Proxies => filtered_proxies(&state.proxies)
            .get(state.proxies.selected)
            .and_then(|row| {
                row.node_id.clone().map(|node_id| UiIntent::SelectNode {
                    group_id: row.group_id.clone(),
                    node_id,
                })
            }),
        Page::Profiles => filtered_profiles(&state.profiles)
            .get(state.profiles.selected)
            .map(|row| UiIntent::ActivateProfile(row.id)),
        Page::Logs => Some(UiIntent::ToggleLogPause),
        Page::Overview => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Interaction {
    pub area: Rect,
    pub intent: UiIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScrollInteraction {
    pub area: Rect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InteractionMap {
    pub frame_revision: u64,
    pub interactions: Vec<Interaction>,
    pub scroll_regions: Vec<ScrollInteraction>,
}

impl InteractionMap {
    #[must_use]
    pub fn intent_for(&self, mouse: MouseInput) -> Option<UiIntent> {
        match mouse.kind {
            MouseInputKind::LeftClick => self
                .interactions
                .iter()
                .rev()
                .find(|interaction| contains(interaction.area, mouse.column, mouse.row))
                .map(|interaction| interaction.intent.clone()),
            MouseInputKind::ScrollUp | MouseInputKind::ScrollDown => self
                .scroll_regions
                .iter()
                .any(|interaction| contains(interaction.area, mouse.column, mouse.row))
                .then_some(if mouse.kind == MouseInputKind::ScrollUp {
                    UiIntent::ScrollUp
                } else {
                    UiIntent::ScrollDown
                }),
        }
    }
}

fn contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && column < area.x.saturating_add(area.width)
        && row >= area.y
        && row < area.y.saturating_add(area.height)
}

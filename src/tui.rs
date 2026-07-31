use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Write};

use crossterm::cursor::{Hide, Show};
use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture, Event as CrosstermEvent, KeyCode, KeyEventKind,
    KeyModifiers, MouseButton as CrosstermMouseButton, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Sparkline, Tabs, Widget};

use crate::application::{LatencyFreshness, LatencyProbeStatus};
use crate::constants::{
    LOG_CAPACITY, MAX_ACTIVE_NODES, MINIMUM_TERMINAL_HEIGHT, MINIMUM_TERMINAL_WIDTH,
    TRAFFIC_SERIES_CAPACITY,
};
use crate::domain::{NodeRecordId, ProfileId, SampleState, StatusSnapshot};
use crate::ipc::RequestId;
use crate::telemetry::{CoreLogRecord, LogLevel, LogSource};

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
    pub selection_pending: Option<(String, NodeRecordId)>,
    pub selected: usize,
    pub scroll: usize,
    pub filter: String,
    pub sort: ProxySort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingProxyGroupLoad {
    pub request_id: RequestId,
    pub connection_generation: u64,
    pub group: String,
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
    pub snapshot: FullViewSnapshot,
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

    fn replace_snapshot(&mut self, generation: u64, snapshot: FullViewSnapshot) {
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
        let log_count = snapshot.logs.len();
        self.logs.records.extend(
            snapshot
                .logs
                .into_iter()
                .skip(log_count.saturating_sub(LOG_CAPACITY)),
        );
        self.logs.dropped_total = snapshot.dropped_logs;
        self.logs.evicted_total = 0;
        self.logs.gap = false;
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

    fn refresh_snapshot(&mut self, generation: u64, snapshot: FullViewSnapshot) {
        if generation != self.connection.generation
            || self.connection.status != ConnectionStatus::Connected
            || self.pending.is_some()
        {
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
        if self.status.as_ref() == Some(&snapshot.status)
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
        self.status = Some(snapshot.status);
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
        snapshot: FullViewSnapshot,
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
    ShowProxyGroup(String),
    InputCharacter(char),
    Backspace,
    Escape,
    MoveUp,
    MoveDown,
    ActivateSelected,
    ActivateProfile(ProfileId),
    SelectNode {
        group: String,
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
        group: String,
        node_id: NodeRecordId,
    },
    FetchProxyGroup {
        request_id: RequestId,
        connection_generation: u64,
        group: String,
    },
    FetchLogTail {
        connection_generation: u64,
        after_sequence: Option<u64>,
    },
    RefreshSnapshot {
        connection_generation: u64,
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
                state.status = Some(status);
                state.push_traffic_sample();
                state.render_dirty = true;
            }
            Vec::new()
        }
        UiEvent::SnapshotRefreshed {
            connection_generation,
            snapshot,
        } => {
            state.refresh_snapshot(connection_generation, snapshot);
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
                    if state.logs.records.len() == LOG_CAPACITY {
                        state.logs.records.pop_front();
                        state.logs.evicted_total = state.logs.evicted_total.saturating_add(1);
                    }
                    state.logs.records.push_back(record);
                }
                state.logs.gap |= gap;
                state.logs.dropped_total = dropped_total;
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
                        state.replace_snapshot(connection_generation, success.snapshot);
                        state.toast = Some(format!("Success: {}", success.message));
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
        UiIntent::ShowProxyGroup(group) => issue_proxy_group_load(state, group),
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
        UiIntent::SelectNode { group, node_id } => issue_node_selection(state, group, node_id),
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
    if !character.is_control() {
        if let Some(search) = current_search_mut(state) {
            search.push(character);
            state.clamp_selections();
        }
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

fn issue_proxy_group_load(state: &mut AppState, group: String) -> Vec<Command> {
    if let Some(index) = state
        .proxies
        .groups
        .iter()
        .position(|candidate| candidate.name == group)
    {
        state.proxies.group_cursor = index;
    }
    if state.proxies.selected_group.as_deref() == Some(group.as_str())
        && state.proxies.group_load_pending.is_none()
    {
        return Vec::new();
    }
    let request_id = state.take_request_id();
    let mut commands = cancel_group_load(state);
    state.proxies.group_load_pending = Some(PendingProxyGroupLoad {
        request_id,
        connection_generation: state.connection.generation,
        group: group.clone(),
    });
    commands.push(Command::FetchProxyGroup {
        request_id,
        connection_generation: state.connection.generation,
        group,
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
                    .map(|node_id| (row.group.clone(), node_id))
            })
            .map_or_else(Vec::new, |(group, node_id)| {
                issue_node_selection(state, group, node_id)
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
    commands.push(Command::ActivateProfile {
        request_id,
        connection_generation: state.connection.generation,
        profile_id,
    });
    commands
}

fn issue_node_selection(
    state: &mut AppState,
    group: String,
    node_id: NodeRecordId,
) -> Vec<Command> {
    let request_id = state.take_request_id();
    let mut commands = cancel_pending(state);
    state.pending = Some(PendingOperation {
        request_id,
        connection_generation: state.connection.generation,
        kind: PendingOperationKind::SelectNode,
    });
    state.proxies.selected_group = Some(group.clone());
    state.proxies.selection_pending = Some((group.clone(), node_id.clone()));
    commands.push(Command::SelectNode {
        request_id,
        connection_generation: state.connection.generation,
        group,
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
                .map(|group| UiIntent::ShowProxyGroup(group.name.clone()));
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
                    group: row.group.clone(),
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

// -----------------------------------------------------------------------------
// Pure layout and rendering
// -----------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutRegions {
    pub area: Rect,
    pub navigation: Rect,
    pub content: Rect,
    pub footer: Rect,
    pub proxy_groups: Option<Rect>,
    pub search: Option<Rect>,
    pub list: Option<Rect>,
    pub modal: Option<Rect>,
    pub minimum_size: bool,
}

pub fn compute_layout(
    state: &AppState,
    area: Rect,
    frame_revision: u64,
) -> (LayoutRegions, InteractionMap) {
    if area.width < MINIMUM_TERMINAL_WIDTH || area.height < MINIMUM_TERMINAL_HEIGHT {
        return (
            LayoutRegions {
                area,
                navigation: Rect::default(),
                content: area,
                footer: Rect::default(),
                proxy_groups: None,
                search: None,
                list: None,
                modal: None,
                minimum_size: true,
            },
            InteractionMap {
                frame_revision,
                interactions: Vec::new(),
                scroll_regions: Vec::new(),
            },
        );
    }

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(1),
            Constraint::Length(1),
        ])
        .split(area);
    let navigation = vertical[0];
    let content = vertical[1];
    let footer = vertical[2];
    let (proxy_groups, search, list) = match state.page {
        Page::Overview => (None, None, None),
        Page::Proxies => {
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Min(1),
                ])
                .split(content);
            (Some(rows[0]), Some(rows[1]), Some(rows[2]))
        }
        Page::Profiles => {
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(3), Constraint::Min(1)])
                .split(content);
            (None, Some(rows[0]), Some(rows[1]))
        }
        Page::Logs => {
            let rows = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(3),
                    Constraint::Min(1),
                ])
                .split(content);
            (None, Some(rows[1]), Some(rows[2]))
        }
    };
    let modal = state.modal.as_ref().map(|_| centered_rect(64, 14, area));
    let regions = LayoutRegions {
        area,
        navigation,
        content,
        footer,
        proxy_groups,
        search,
        list,
        modal,
        minimum_size: false,
    };
    let map = interaction_map(state, &regions, frame_revision);
    (regions, map)
}

pub fn render(frame: &mut Frame<'_>, state: &AppState) -> InteractionMap {
    render_buffer(state, frame.area(), frame.buffer_mut())
}

pub fn render_buffer(state: &AppState, area: Rect, buffer: &mut Buffer) -> InteractionMap {
    let (regions, map) = compute_layout(state, area, state.next_frame_revision);
    if regions.minimum_size {
        render_minimum_size(area, buffer);
        return map;
    }

    render_navigation(state, regions.navigation, buffer);
    match state.page {
        Page::Overview => render_overview(state, regions.content, buffer),
        Page::Proxies => render_proxies(state, &regions, buffer),
        Page::Profiles => render_profiles(state, &regions, buffer),
        Page::Logs => render_logs(state, &regions, buffer),
    }
    render_footer(state, regions.footer, buffer);
    if let (Some(modal), Some(area)) = (&state.modal, regions.modal) {
        render_modal(modal, area, buffer);
    }
    map
}

fn interaction_map(
    state: &AppState,
    regions: &LayoutRegions,
    frame_revision: u64,
) -> InteractionMap {
    let mut interactions = Vec::new();
    let mut scroll_regions = Vec::new();

    for (index, page) in Page::ALL.iter().copied().enumerate() {
        interactions.push(Interaction {
            area: Rect::new(
                regions.navigation.x.saturating_add(1 + index as u16 * 12),
                regions.navigation.y.saturating_add(1),
                11,
                1,
            ),
            intent: UiIntent::SwitchPage(page),
        });
    }
    let help_width = 8_u16.min(regions.footer.width);
    interactions.push(Interaction {
        area: Rect::new(regions.footer.x, regions.footer.y, help_width, 1),
        intent: UiIntent::ToggleHelp,
    });
    let quit_width = 8_u16.min(regions.footer.width);
    interactions.push(Interaction {
        area: Rect::new(
            regions
                .footer
                .x
                .saturating_add(regions.footer.width.saturating_sub(quit_width)),
            regions.footer.y,
            quit_width,
            1,
        ),
        intent: UiIntent::Quit,
    });
    if let Some(search) = regions.search {
        interactions.push(Interaction {
            area: search,
            intent: UiIntent::FocusSearch,
        });
        if state.page == Page::Proxies {
            let sort_start = search.x.saturating_add(search.width.saturating_sub(31));
            for (index, sort) in ProxySort::ALL.iter().copied().enumerate() {
                interactions.push(Interaction {
                    area: Rect::new(
                        sort_start.saturating_add(index as u16 * 10),
                        search.y.saturating_add(1),
                        10,
                        1,
                    ),
                    intent: UiIntent::SetProxySort(sort),
                });
            }
        }
    }
    if let Some(group_area) = regions.proxy_groups {
        interactions.push(Interaction {
            area: Rect::new(
                group_area.x.saturating_add(1),
                group_area.y.saturating_add(1),
                3,
                1,
            ),
            intent: UiIntent::PreviousProxyGroup,
        });
        interactions.push(Interaction {
            area: Rect::new(
                group_area
                    .x
                    .saturating_add(group_area.width.saturating_sub(4)),
                group_area.y.saturating_add(1),
                3,
                1,
            ),
            intent: UiIntent::NextProxyGroup,
        });
        for (_, group, area) in visible_proxy_groups(&state.proxies, group_area) {
            interactions.push(Interaction {
                area,
                intent: UiIntent::ShowProxyGroup(group.name.clone()),
            });
        }
    }
    if let Some(list) = regions.list {
        scroll_regions.push(ScrollInteraction { area: list });
        let available_rows = list.height.saturating_sub(2) as usize;
        match state.page {
            Page::Proxies => {
                for (row_index, row) in filtered_proxies(&state.proxies)
                    .into_iter()
                    .skip(state.proxies.scroll.min(state.proxies.selected))
                    .take(available_rows)
                    .enumerate()
                {
                    let Some(node_id) = row.node_id.clone() else {
                        continue;
                    };
                    interactions.push(Interaction {
                        area: Rect::new(
                            list.x.saturating_add(1),
                            list.y.saturating_add(1 + row_index as u16),
                            list.width.saturating_sub(2),
                            1,
                        ),
                        intent: UiIntent::SelectNode {
                            group: row.group.clone(),
                            node_id,
                        },
                    });
                }
            }
            Page::Profiles => {
                for (row_index, row) in filtered_profiles(&state.profiles)
                    .into_iter()
                    .skip(state.profiles.scroll.min(state.profiles.selected))
                    .take(available_rows)
                    .enumerate()
                {
                    interactions.push(Interaction {
                        area: Rect::new(
                            list.x.saturating_add(1),
                            list.y.saturating_add(1 + row_index as u16),
                            list.width.saturating_sub(2),
                            1,
                        ),
                        intent: UiIntent::ActivateProfile(row.id),
                    });
                }
            }
            Page::Overview | Page::Logs => {}
        }
    }
    if state.page == Page::Logs {
        let controls = Rect::new(
            regions.content.x,
            regions.content.y,
            regions.content.width,
            3,
        );
        for (index, level) in LogLevelFilter::ALL.iter().copied().enumerate() {
            interactions.push(Interaction {
                area: Rect::new(
                    controls.x.saturating_add(1 + index as u16 * 8),
                    controls.y.saturating_add(1),
                    7,
                    1,
                ),
                intent: UiIntent::SetLogLevel(level),
            });
        }
        interactions.push(Interaction {
            area: Rect::new(
                controls.x.saturating_add(43),
                controls.y.saturating_add(1),
                8,
                1,
            ),
            intent: UiIntent::ToggleLogPause,
        });
        interactions.push(Interaction {
            area: Rect::new(
                controls.x.saturating_add(51),
                controls.y.saturating_add(1),
                9,
                1,
            ),
            intent: UiIntent::FollowLogs,
        });
    }

    if let Some(modal) = regions.modal {
        interactions.clear();
        scroll_regions.clear();
        interactions.push(Interaction {
            area: Rect::new(
                modal.x.saturating_add(modal.width.saturating_sub(10)),
                modal.y.saturating_add(modal.height.saturating_sub(2)),
                8,
                1,
            ),
            intent: UiIntent::CloseModal,
        });
    }

    InteractionMap {
        frame_revision,
        interactions,
        scroll_regions,
    }
}

fn render_navigation(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let titles = Page::ALL
        .iter()
        .map(|page| Line::from(format!(" {} [{}] ", page.title(), page.index() + 1)))
        .collect::<Vec<_>>();
    Tabs::new(titles)
        .select(state.page.index())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(if state.focus == Focus::Tabs {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default()
                })
                .title("Hopash RS"),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .render(area, buffer);
}

fn render_overview(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);
    let connection = connection_label(state.connection);
    let body = if let Some(status) = &state.status {
        let active_profile = status.active_profile.as_ref().map_or_else(
            || Cow::Borrowed("-"),
            |profile| terminal_safe(&profile.name),
        );
        let primary_group = status
            .primary_proxy_group
            .as_deref()
            .map_or_else(|| Cow::Borrowed("-"), terminal_safe);
        let current_node = status
            .selected_node
            .as_ref()
            .map_or_else(|| Cow::Borrowed("-"), |node| terminal_safe(&node.name));
        let selected_probe_status = status.selected_node.as_ref().and_then(|selected| {
            state
                .proxies
                .rows
                .iter()
                .find(|row| row.node_id.as_ref() == Some(&selected.id))
                .map(|row| latency_probe_status_title(row.probe_status))
        });
        let (delay, sampled_at, freshness, probe_status, probe_generation) =
            status.latency.as_ref().map_or_else(
                || {
                    (
                        "not_sampled".to_owned(),
                        "-".to_owned(),
                        selected_probe_status.unwrap_or("not_sampled"),
                        "not_sampled",
                        "-".to_owned(),
                    )
                },
                |sample| {
                    (
                        sample.delay_ms.map_or_else(
                            || "not_sampled".to_owned(),
                            |delay| format!("{delay} ms"),
                        ),
                        sample
                            .sampled_at_unix_ms
                            .map_or_else(|| "-".to_owned(), |sampled_at| sampled_at.to_string()),
                        sample_state_title(sample.state),
                        selected_probe_status.unwrap_or_else(|| {
                            latency_sample_probe_status(sample.state, sample.delay_ms)
                        }),
                        sample.probe_generation.0.to_string(),
                    )
                },
            );
        format!(
            "Connection: {connection}\nSupervisor: {:?}\nCore: {:?}\nTUN: {}\nActive Profile: {}\nPrimary Group: {}\nCurrent Node: {}\nLatency: {delay}\nSampled At: {sampled_at}\nFreshness: {freshness}\nProbe: {probe_status} (generation {probe_generation})\nConnections: {}\nUptime: {}s",
            status.supervisor.lifecycle,
            status.core.lifecycle,
            if status.tun.effective {
                "effective"
            } else {
                "inactive"
            },
            active_profile,
            primary_group,
            current_node,
            status.connection_count,
            status.supervisor.uptime_seconds,
        )
    } else {
        format!("Connection: {connection}\nWaiting for the first status snapshot")
    };
    Paragraph::new(body)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(focus_border(state.focus == Focus::Content))
                .title("Status"),
        )
        .render(columns[0], buffer);

    let traffic = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(columns[1]);
    let upload = state.upload_series.iter().copied().collect::<Vec<_>>();
    let download = state.download_series.iter().copied().collect::<Vec<_>>();
    Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title("Upload B/s"))
        .data(&upload)
        .style(Style::default().fg(Color::Yellow))
        .render(traffic[0], buffer);
    Sparkline::default()
        .block(Block::default().borders(Borders::ALL).title("Download B/s"))
        .data(&download)
        .style(Style::default().fg(Color::Green))
        .render(traffic[1], buffer);
}

fn visible_proxy_groups(state: &ProxiesState, area: Rect) -> Vec<(usize, &ProxyGroupRow, Rect)> {
    let mut visible = Vec::new();
    let mut x = area.x.saturating_add(4);
    let end = area.x.saturating_add(area.width.saturating_sub(4));
    for (index, group) in state
        .groups
        .iter()
        .enumerate()
        .skip(state.group_cursor.min(state.groups.len().saturating_sub(1)))
    {
        let width = proxy_group_chip_width(state, group).min(end.saturating_sub(x));
        if width == 0 {
            break;
        }
        visible.push((
            index,
            group,
            Rect::new(x, area.y.saturating_add(1), width, 1),
        ));
        x = x.saturating_add(width);
        if x >= end {
            break;
        }
    }
    visible
}

fn proxy_group_chip_width(state: &ProxiesState, group: &ProxyGroupRow) -> u16 {
    proxy_group_chip_label(state, group)
        .chars()
        .count()
        .clamp(5, 24)
        .try_into()
        .unwrap_or(24)
}

fn proxy_group_chip_label(state: &ProxiesState, group: &ProxyGroupRow) -> String {
    let marker = if state
        .group_load_pending
        .as_ref()
        .is_some_and(|pending| pending.group == group.name)
    {
        "[pending]"
    } else if state.selected_group.as_deref() == Some(group.name.as_str()) {
        "[current]"
    } else {
        ""
    };
    format!("{marker} {} ", terminal_safe(&group.name))
}

fn render_proxy_groups(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let focused_group = state.proxies.groups.get(state.proxies.group_cursor);
    let focused = focused_group.map_or_else(
        || "unconfigured".to_owned(),
        |group| {
            format!(
                "{} -> {}",
                terminal_safe(&group.name),
                group
                    .selected_node
                    .as_deref()
                    .map_or_else(|| Cow::Borrowed("-"), terminal_safe)
            )
        },
    );
    Block::default()
        .borders(Borders::ALL)
        .border_style(focus_border(state.focus == Focus::ProxyGroups))
        .title(format!(
            "Proxy Groups ({}) · focus {focused} · Enter switches",
            state.proxies.groups.len()
        ))
        .render(area, buffer);
    Paragraph::new(" < ")
        .style(Style::default().fg(Color::Yellow))
        .render(
            Rect::new(area.x.saturating_add(1), area.y.saturating_add(1), 3, 1),
            buffer,
        );
    Paragraph::new(" > ")
        .style(Style::default().fg(Color::Yellow))
        .render(
            Rect::new(
                area.x.saturating_add(area.width.saturating_sub(4)),
                area.y.saturating_add(1),
                3,
                1,
            ),
            buffer,
        );
    for (index, group, chip_area) in visible_proxy_groups(&state.proxies, area) {
        let current = state.proxies.selected_group.as_deref() == Some(group.name.as_str());
        let pending = state
            .proxies
            .group_load_pending
            .as_ref()
            .is_some_and(|load| load.group == group.name);
        let focused = index == state.proxies.group_cursor;
        let style = if pending {
            Style::default()
                .fg(Color::Magenta)
                .add_modifier(Modifier::BOLD)
        } else if focused && state.focus == Focus::ProxyGroups {
            Style::default().fg(Color::Black).bg(Color::Yellow)
        } else if current {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else if focused {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };
        Paragraph::new(proxy_group_chip_label(&state.proxies, group))
            .style(style)
            .render(chip_area, buffer);
    }
}

fn render_proxies(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    render_proxy_groups(
        state,
        regions
            .proxy_groups
            .expect("Proxies layout includes Proxy Groups"),
        buffer,
    );
    let search_area = regions.search.expect("Proxies layout includes search");
    let search_style = if state.focus == Focus::Search {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    let search_width = search_area.width.saturating_sub(32) as usize;
    let search_text = format!("/{}", terminal_safe(&state.proxies.filter))
        .chars()
        .take(search_width)
        .collect::<String>();
    let mut spans = vec![Span::styled(
        format!("{search_text:<search_width$}"),
        search_style,
    )];
    spans.extend(ProxySort::ALL.iter().copied().map(|sort| {
        let style = if sort == state.proxies.sort {
            Style::default().fg(Color::Black).bg(Color::Cyan)
        } else {
            Style::default()
        };
        Span::styled(format!(" {:<9}", sort.title()), style)
    }));
    Paragraph::new(Line::from(spans))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Node search · Sort"),
        )
        .render(search_area, buffer);
    let rows = filtered_proxies(&state.proxies);
    let offset = state.proxies.scroll.min(state.proxies.selected);
    let items = rows
        .into_iter()
        .skip(offset)
        .enumerate()
        .map(|(index, row)| {
            let pending =
                state
                    .proxies
                    .selection_pending
                    .as_ref()
                    .is_some_and(|(group, node_id)| {
                        group == &row.group && row.node_id.as_ref() == Some(node_id)
                    });
            let marker = match (row.selected, pending) {
                (true, true) => "[current][pending]",
                (true, false) => "[current]         ",
                (false, true) => "[pending]         ",
                (false, false) => "                  ",
            };
            let availability = if row.available {
                "ready"
            } else {
                "unavailable"
            };
            let delay = row
                .delay_ms
                .map_or_else(|| "not_sampled".to_owned(), |delay| format!("{delay}ms"));
            let sampled_at = row
                .sampled_at_unix_ms
                .map_or_else(|| "-".to_owned(), |sampled_at| sampled_at.to_string());
            let item = ListItem::new(format!(
                "{marker} {:<20} {:<12} {:<11} {:<11} sampled={:<13} freshness={:<11} probe={}",
                terminal_safe(&row.name),
                terminal_safe(&row.node_type),
                availability,
                delay,
                sampled_at,
                latency_freshness_title(row.freshness),
                latency_probe_status_title(row.probe_status),
            ));
            if offset + index == state.proxies.selected {
                item.style(selected_style(state.focus == Focus::Content))
            } else {
                item
            }
        })
        .collect::<Vec<_>>();
    List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(focus_border(state.focus == Focus::Content))
                .title(format!(
                    "Nodes ({}) · {} · {:?}",
                    state.proxies.rows.len(),
                    state
                        .proxies
                        .selected_group
                        .as_deref()
                        .unwrap_or("unconfigured"),
                    state.proxies.sort
                )),
        )
        .render(regions.list.expect("Proxies layout includes list"), buffer);
}

fn focus_border(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default()
    }
}

fn sample_state_title(state: SampleState) -> &'static str {
    match state {
        SampleState::Fresh => "fresh",
        SampleState::Stale => "stale",
        SampleState::Unavailable => "unavailable",
    }
}

fn latency_sample_probe_status(state: SampleState, delay_ms: Option<u64>) -> &'static str {
    match (state, delay_ms) {
        (SampleState::Unavailable, _) => "failed",
        (SampleState::Fresh | SampleState::Stale, Some(_)) => "succeeded",
        (SampleState::Fresh | SampleState::Stale, None) => "not_sampled",
    }
}

fn latency_freshness_title(freshness: LatencyFreshness) -> &'static str {
    match freshness {
        LatencyFreshness::NotSampled => "not_sampled",
        LatencyFreshness::Fresh => "fresh",
        LatencyFreshness::Stale => "stale",
        LatencyFreshness::Unavailable => "unavailable",
    }
}

fn latency_probe_status_title(status: LatencyProbeStatus) -> &'static str {
    match status {
        LatencyProbeStatus::NotSampled => "not_sampled",
        LatencyProbeStatus::Queued => "queued",
        LatencyProbeStatus::InFlight => "in_flight",
        LatencyProbeStatus::Succeeded => "succeeded",
        LatencyProbeStatus::Failed => "failed",
    }
}

fn render_profiles(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    render_search(
        "Profile search",
        &state.profiles.filter,
        state.focus == Focus::Search,
        regions.search.expect("Profiles layout includes search"),
        buffer,
    );
    let rows = filtered_profiles(&state.profiles);
    let offset = state.profiles.scroll.min(state.profiles.selected);
    let items = rows
        .into_iter()
        .skip(offset)
        .enumerate()
        .map(|(index, row)| {
            let marker = match (
                row.active,
                state.profiles.activation_pending == Some(row.id),
            ) {
                (true, true) => "[active][pending]",
                (true, false) => "[active]         ",
                (false, true) => "[pending]        ",
                (false, false) => "                 ",
            };
            let freshness = if row.fresh { "fresh" } else { "stale" };
            let error = row
                .error
                .as_deref()
                .map_or_else(|| Cow::Borrowed("-"), terminal_safe);
            let item = ListItem::new(format!(
                "{marker} {:<24} {:<6} next={} error={error}",
                terminal_safe(&row.name),
                freshness,
                row.next_refresh_at_unix_ms
            ));
            if offset + index == state.profiles.selected {
                item.style(selected_style(state.focus == Focus::Content))
            } else {
                item
            }
        })
        .collect::<Vec<_>>();
    List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(focus_border(state.focus == Focus::Content))
                .title(format!("Profiles ({})", state.profiles.rows.len())),
        )
        .render(regions.list.expect("Profiles layout includes list"), buffer);
}

fn render_logs(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    let controls = Rect::new(
        regions.content.x,
        regions.content.y,
        regions.content.width,
        3,
    );
    let filter_spans = LogLevelFilter::ALL
        .iter()
        .copied()
        .flat_map(|level| {
            let style = if level == state.logs.level_filter {
                Style::default().fg(Color::Black).bg(Color::Cyan)
            } else {
                Style::default()
            };
            [Span::styled(format!(" {:<7}", level.title()), style)]
        })
        .chain([
            Span::raw("  "),
            Span::styled(
                if state.logs.paused {
                    " Resume "
                } else {
                    " Pause  "
                },
                if state.logs.paused {
                    Style::default().fg(Color::Black).bg(Color::Yellow)
                } else {
                    Style::default().fg(Color::Yellow)
                },
            ),
            Span::styled(
                " Follow  ",
                if state.logs.follow {
                    Style::default().fg(Color::Black).bg(Color::Green)
                } else {
                    Style::default().fg(Color::Green)
                },
            ),
        ])
        .collect::<Vec<_>>();
    Paragraph::new(Line::from(filter_spans))
        .block(Block::default().borders(Borders::ALL).title("Log controls"))
        .render(controls, buffer);
    render_search(
        "Log query · since:<ms> until:<ms> level:<name> content:<text>",
        &state.logs.search,
        state.focus == Focus::Search,
        regions.search.expect("Logs layout includes search"),
        buffer,
    );

    let rows = filtered_logs(&state.logs);
    let visible = regions
        .list
        .expect("Logs layout includes list")
        .height
        .saturating_sub(2) as usize;
    let start = if state.logs.follow {
        rows.len().saturating_sub(visible)
    } else {
        rows.len()
            .saturating_sub(visible)
            .saturating_sub(state.logs.scroll)
    };
    let items = rows
        .into_iter()
        .skip(start)
        .take(visible)
        .map(|record| {
            ListItem::new(format!(
                "{} {:<5} {:<8} {}",
                record.timestamp_unix_ms,
                log_level_title(record.level),
                log_source_title(record.source),
                terminal_safe(&record.message)
            ))
        })
        .collect::<Vec<_>>();
    let state_label = if state.logs.paused { "paused" } else { "live" };
    let follow_label = if state.logs.follow {
        "following"
    } else {
        "manual"
    };
    List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(focus_border(state.focus == Focus::Content))
                .title(format!(
                    "Core Logs · {state_label} · {follow_label} · dropped={} evicted={}{}",
                    state.logs.dropped_total,
                    state.logs.evicted_total,
                    if state.logs.gap { " · gap" } else { "" }
                )),
        )
        .render(regions.list.expect("Logs layout includes list"), buffer);
}

fn render_search(title: &str, value: &str, focused: bool, area: Rect, buffer: &mut Buffer) {
    let style = if focused {
        Style::default()
            .fg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Paragraph::new(format!("/{}", terminal_safe(value)))
        .style(style)
        .block(Block::default().borders(Borders::ALL).title(title))
        .render(area, buffer);
}

fn render_footer(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let stale = if state.connection.snapshot_stale {
        " · snapshot stale"
    } else {
        ""
    };
    let toast = state.toast.as_ref().map_or_else(String::new, |message| {
        format!(" · {}", terminal_safe(message))
    });
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(8),
            Constraint::Min(1),
            Constraint::Length(8),
        ])
        .split(area);
    Paragraph::new(Span::styled(
        "[?] Help",
        if state.focus == Focus::FooterHelp {
            Style::default().fg(Color::Black).bg(Color::Cyan)
        } else {
            Style::default().fg(Color::Cyan)
        },
    ))
    .render(columns[0], buffer);
    Paragraph::new(format!(
        "{}{}{}",
        connection_label(state.connection),
        stale,
        toast
    ))
    .render(columns[1], buffer);
    Paragraph::new(Span::styled(
        "[q] Quit",
        if state.focus == Focus::FooterQuit {
            Style::default().fg(Color::Black).bg(Color::Yellow)
        } else {
            Style::default().fg(Color::Yellow)
        },
    ))
    .render(columns[2], buffer);
}

fn render_modal(modal: &Modal, area: Rect, buffer: &mut Buffer) {
    Clear.render(area, buffer);
    let (title, body) = match modal {
        Modal::Help => (
            Cow::Borrowed("Keyboard and mouse help"),
            Cow::Borrowed(
                "1-4 pages · Tab/Shift+Tab focus · arrows or j/k move\nEnter activates · / searches · s sorts nodes · p pauses logs · f follows logs\nLogs: a/d/i/w/e selects All/Debug/Info/Warn/Error\nEsc closes · q quits when no input or modal owns focus\n\nMouse: click tabs, controls, search, Profile, or Node; wheel scrolls.",
            ),
        ),
        Modal::Message { title, body } => (terminal_safe(title), terminal_safe_multiline(body)),
    };
    Paragraph::new(body.as_ref())
        .block(Block::default().borders(Borders::ALL).title(title.as_ref()))
        .render(area, buffer);
    let close = Rect::new(
        area.x.saturating_add(area.width.saturating_sub(10)),
        area.y.saturating_add(area.height.saturating_sub(2)),
        8,
        1,
    );
    Paragraph::new("[Close]")
        .style(Style::default().fg(Color::Yellow))
        .render(close, buffer);
}

fn render_minimum_size(area: Rect, buffer: &mut Buffer) {
    Paragraph::new(format!(
        "Terminal too small\nRequired: {}x{}\nCurrent: {}x{}",
        MINIMUM_TERMINAL_WIDTH, MINIMUM_TERMINAL_HEIGHT, area.width, area.height
    ))
    .block(Block::default().borders(Borders::ALL).title("Hopash RS"))
    .render(area, buffer);
}

fn selected_style(focused: bool) -> Style {
    if focused {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Cyan)
    }
}

fn terminal_safe(value: &str) -> Cow<'_, str> {
    terminal_safe_with_newlines(value, false)
}

fn terminal_safe_multiline(value: &str) -> Cow<'_, str> {
    terminal_safe_with_newlines(value, true)
}

fn terminal_safe_with_newlines(value: &str, allow_newlines: bool) -> Cow<'_, str> {
    if value
        .chars()
        .all(|character| !character.is_control() || (allow_newlines && character == '\n'))
    {
        return Cow::Borrowed(value);
    }
    Cow::Owned(
        value
            .chars()
            .map(|character| {
                if character.is_control() && !(allow_newlines && character == '\n') {
                    '?'
                } else {
                    character
                }
            })
            .collect(),
    )
}

fn connection_label(connection: ConnectionState) -> &'static str {
    match connection.status {
        ConnectionStatus::Connecting => "connecting",
        ConnectionStatus::Connected => "connected",
        ConnectionStatus::Disconnected => "disconnected",
    }
}

fn log_level_title(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}

fn log_source_title(source: LogSource) -> &'static str {
    match source {
        LogSource::CoreApi => "core",
        LogSource::Stdout => "stdout",
        LogSource::Stderr => "stderr",
    }
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width.saturating_sub(2));
    let height = height.min(area.height.saturating_sub(2));
    Rect::new(
        area.x.saturating_add(area.width.saturating_sub(width) / 2),
        area.y
            .saturating_add(area.height.saturating_sub(height) / 2),
        width,
        height,
    )
}

// -----------------------------------------------------------------------------
// Fair bounded event inbox
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventSource {
    Terminal,
    CommandResult,
    Deadline,
    Telemetry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventBudgets {
    pub terminal: usize,
    pub command_result: usize,
    pub deadline: usize,
    pub telemetry: usize,
}

impl Default for EventBudgets {
    fn default() -> Self {
        Self {
            terminal: 8,
            command_result: 8,
            deadline: 2,
            telemetry: 64,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventInboxError {
    InvalidCapacity,
    InvalidBudget,
}

#[derive(Debug)]
pub struct FairEventInbox {
    capacity_per_source: usize,
    budgets: EventBudgets,
    shutdown: Option<UiEvent>,
    terminal: VecDeque<UiEvent>,
    command_results: VecDeque<UiEvent>,
    deadlines: VecDeque<UiEvent>,
    telemetry: VecDeque<UiEvent>,
    dropped: [u64; 4],
}

impl FairEventInbox {
    pub fn new(capacity_per_source: usize, budgets: EventBudgets) -> Result<Self, EventInboxError> {
        if capacity_per_source == 0 {
            return Err(EventInboxError::InvalidCapacity);
        }
        if budgets.terminal == 0
            || budgets.command_result == 0
            || budgets.deadline == 0
            || budgets.telemetry == 0
        {
            return Err(EventInboxError::InvalidBudget);
        }
        Ok(Self {
            capacity_per_source,
            budgets,
            shutdown: None,
            terminal: VecDeque::with_capacity(capacity_per_source),
            command_results: VecDeque::with_capacity(capacity_per_source),
            deadlines: VecDeque::with_capacity(capacity_per_source),
            telemetry: VecDeque::with_capacity(capacity_per_source),
            dropped: [0; 4],
        })
    }

    pub fn product() -> Self {
        Self::new(EVENT_SOURCE_CAPACITY, EventBudgets::default())
            .expect("product event inbox constants are non-zero")
    }

    pub fn push(&mut self, source: EventSource, event: UiEvent) {
        if matches!(event, UiEvent::Shutdown) {
            self.shutdown = Some(event);
            return;
        }
        if source == EventSource::Telemetry && matches!(event, UiEvent::StatusSnapshot { .. }) {
            if let Some(position) = self
                .telemetry
                .iter()
                .rposition(|queued| matches!(queued, UiEvent::StatusSnapshot { .. }))
            {
                self.telemetry[position] = event;
                return;
            }
        }
        let index = source_index(source);
        let queue = match source {
            EventSource::Terminal => &mut self.terminal,
            EventSource::CommandResult => &mut self.command_results,
            EventSource::Deadline => &mut self.deadlines,
            EventSource::Telemetry => &mut self.telemetry,
        };
        if queue.len() == self.capacity_per_source {
            queue.pop_front();
            self.dropped[index] = self.dropped[index].saturating_add(1);
        }
        queue.push_back(event);
    }

    pub fn drain_round(&mut self) -> Vec<UiEvent> {
        if let Some(shutdown) = self.shutdown.take() {
            return vec![shutdown];
        }
        let total_budget = self
            .budgets
            .terminal
            .saturating_add(self.budgets.command_result)
            .saturating_add(self.budgets.deadline)
            .saturating_add(self.budgets.telemetry);
        let mut events = Vec::with_capacity(total_budget);
        drain_source(&mut self.terminal, self.budgets.terminal, &mut events);
        drain_source(
            &mut self.command_results,
            self.budgets.command_result,
            &mut events,
        );
        drain_source(&mut self.deadlines, self.budgets.deadline, &mut events);
        drain_source(&mut self.telemetry, self.budgets.telemetry, &mut events);
        events
    }

    #[must_use]
    pub fn dropped(&self, source: EventSource) -> u64 {
        self.dropped[source_index(source)]
    }

    #[must_use]
    pub fn len(&self, source: EventSource) -> usize {
        match source {
            EventSource::Terminal => self.terminal.len(),
            EventSource::CommandResult => self.command_results.len(),
            EventSource::Deadline => self.deadlines.len(),
            EventSource::Telemetry => self.telemetry.len(),
        }
    }
}

fn source_index(source: EventSource) -> usize {
    match source {
        EventSource::Terminal => 0,
        EventSource::CommandResult => 1,
        EventSource::Deadline => 2,
        EventSource::Telemetry => 3,
    }
}

fn drain_source(queue: &mut VecDeque<UiEvent>, budget: usize, output: &mut Vec<UiEvent>) {
    for _ in 0..budget {
        let Some(event) = queue.pop_front() else {
            break;
        };
        output.push(event);
    }
}

// -----------------------------------------------------------------------------
// Idempotent terminal cleanup seam and Crossterm adapter
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalAction {
    EnableRawMode,
    DisableRawMode,
    EnterAlternateScreen,
    LeaveAlternateScreen,
    EnableMouseCapture,
    DisableMouseCapture,
    EnableFocusReporting,
    DisableFocusReporting,
    EnableBracketedPaste,
    DisableBracketedPaste,
    HideCursor,
    ShowCursor,
}

pub trait TerminalControl {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()>;
}

pub struct TerminalSession<'a> {
    control: &'a mut dyn TerminalControl,
    cleanup_actions: Vec<TerminalAction>,
    cleaned: bool,
}

impl<'a> TerminalSession<'a> {
    pub fn enter(control: &'a mut dyn TerminalControl) -> Result<Self, TerminalSessionError> {
        let mut session = Self {
            control,
            cleanup_actions: Vec::with_capacity(6),
            cleaned: false,
        };
        for (enable, cleanup) in [
            (
                TerminalAction::EnableRawMode,
                TerminalAction::DisableRawMode,
            ),
            (
                TerminalAction::EnterAlternateScreen,
                TerminalAction::LeaveAlternateScreen,
            ),
            (
                TerminalAction::EnableMouseCapture,
                TerminalAction::DisableMouseCapture,
            ),
            (
                TerminalAction::EnableFocusReporting,
                TerminalAction::DisableFocusReporting,
            ),
            (
                TerminalAction::EnableBracketedPaste,
                TerminalAction::DisableBracketedPaste,
            ),
            (TerminalAction::HideCursor, TerminalAction::ShowCursor),
        ] {
            session.cleanup_actions.push(cleanup);
            if let Err(source) = session.control.apply(enable) {
                let cleanup_error = session.cleanup().err();
                return Err(TerminalSessionError {
                    failed_action: enable,
                    source,
                    cleanup_error,
                });
            }
        }
        Ok(session)
    }

    pub fn cleanup(&mut self) -> io::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        self.cleaned = true;
        let mut first_error = None;
        while let Some(action) = self.cleanup_actions.pop() {
            if let Err(error) = self.control.apply(action) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[must_use]
    pub fn is_cleaned(&self) -> bool {
        self.cleaned
    }
}

impl Drop for TerminalSession<'_> {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

#[derive(Debug)]
pub struct TerminalSessionError {
    pub failed_action: TerminalAction,
    pub source: io::Error,
    pub cleanup_error: Option<io::Error>,
}

impl fmt::Display for TerminalSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "terminal initialization failed during {:?}: {}",
            self.failed_action, self.source
        )
    }
}

impl std::error::Error for TerminalSessionError {}

pub struct CrosstermControl<W> {
    writer: W,
}

impl<W> CrosstermControl<W> {
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> TerminalControl for CrosstermControl<W> {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()> {
        match action {
            TerminalAction::EnableRawMode => enable_raw_mode(),
            TerminalAction::DisableRawMode => disable_raw_mode(),
            TerminalAction::EnterAlternateScreen => execute!(self.writer, EnterAlternateScreen),
            TerminalAction::LeaveAlternateScreen => execute!(self.writer, LeaveAlternateScreen),
            TerminalAction::EnableMouseCapture => execute!(self.writer, EnableMouseCapture),
            TerminalAction::DisableMouseCapture => execute!(self.writer, DisableMouseCapture),
            TerminalAction::EnableFocusReporting => execute!(self.writer, EnableFocusChange),
            TerminalAction::DisableFocusReporting => execute!(self.writer, DisableFocusChange),
            TerminalAction::EnableBracketedPaste => execute!(self.writer, EnableBracketedPaste),
            TerminalAction::DisableBracketedPaste => execute!(self.writer, DisableBracketedPaste),
            TerminalAction::HideCursor => execute!(self.writer, Hide),
            TerminalAction::ShowCursor => execute!(self.writer, Show),
        }
    }
}

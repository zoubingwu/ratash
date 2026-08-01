//! Reducer, commands, and state transition helpers for the Status Interface.

use crate::constants::{
    LOCAL_RULE_COUNT_MAX, MAX_ACTIVE_NODES, RULE_STRING_MAX_BYTES, TUI_SEARCH_MAX_BYTES,
    TUI_SEARCH_MAX_CHARACTERS,
};
use crate::domain::{NodeRecordId, ProfileId, ProxyGroupId, StatusSnapshot};
use crate::ipc::RequestId;

use super::input::{TerminalInput, input_to_intent};
use super::state::*;

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
    RulesLoaded {
        request_id: RequestId,
        connection_generation: u64,
        result: Result<RuleListSnapshot, String>,
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
    FocusLeft,
    FocusRight,
    FocusSearch,
    OpenCommandPalette,
    OpenProfiles,
    RunPaletteAction(CommandPaletteAction),
    ConfirmLifecycleAction,
    LoadRules,
    OpenRuleAdd,
    OpenSelectedRuleEditor,
    RequestSelectedRuleRemoval,
    SubmitRuleEditor,
    ConfirmRuleRemoval,
    SelectRule(usize),
    SelectLog {
        tail_offset: usize,
    },
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
    ToggleProxyDetail,
    ToggleZoom,
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
    FetchRules {
        request_id: RequestId,
        connection_generation: u64,
    },
    AddRule {
        request_id: RequestId,
        connection_generation: u64,
        rule: String,
    },
    ReplaceRule {
        request_id: RequestId,
        connection_generation: u64,
        old_rule: String,
        new_rule: String,
    },
    RemoveRule {
        request_id: RequestId,
        connection_generation: u64,
        rule: String,
    },
    RestartSupervisor {
        request_id: RequestId,
        connection_generation: u64,
    },
    StopSupervisor {
        request_id: RequestId,
        connection_generation: u64,
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
                if state.page == Page::Rules {
                    return ensure_rules_loaded(state);
                }
            }
            Vec::new()
        }
        UiEvent::Disconnected {
            connection_generation,
        } => {
            if connection_generation == state.connection.generation {
                let stop_pending = state
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.kind == PendingOperationKind::StopSupervisor);
                let mut commands = cancel_pending(state);
                state.connection.status = ConnectionStatus::Disconnected;
                state.connection.snapshot_stale = state.status.is_some();
                state.render_dirty = true;
                if stop_pending {
                    state.should_quit = true;
                } else {
                    commands.push(Command::ScheduleReconnect {
                        connection_generation,
                    });
                }
                commands
            } else {
                Vec::new()
            }
        }
        UiEvent::StatusSnapshot {
            connection_generation,
            status,
        } => {
            let mut commands = Vec::new();
            if connection_generation == state.connection.generation
                && state.connection.status == ConnectionStatus::Connected
                && state.status.as_ref() != Some(&status)
            {
                let runtime_changed = state.status.as_ref().is_some_and(|previous| {
                    previous.runtime_generation != status.runtime_generation
                });
                let collection_changed = state
                    .status
                    .as_ref()
                    .is_some_and(|previous| status_requires_snapshot_refresh(previous, &status));
                if runtime_changed {
                    commands.extend(invalidate_rule_cache(state));
                }
                state.status = Some(status);
                state.status_revision = state.status_revision.wrapping_add(1).max(1);
                if collection_changed {
                    state.bump_view_revision();
                }
                state.push_traffic_sample();
                state.render_dirty = true;
                if runtime_changed && state.page == Page::Rules {
                    commands.extend(ensure_rules_loaded(state));
                }
            }
            commands
        }
        UiEvent::SnapshotRefreshed {
            connection_generation,
            base_view_revision,
            base_status_revision,
            snapshot,
        } => {
            let previous_runtime_generation = state
                .status
                .as_ref()
                .and_then(|status| status.runtime_generation);
            state.refresh_snapshot(
                connection_generation,
                base_view_revision,
                base_status_revision,
                snapshot,
            );
            let runtime_changed = state
                .status
                .as_ref()
                .and_then(|status| status.runtime_generation)
                != previous_runtime_generation;
            if runtime_changed {
                let mut commands = invalidate_rule_cache(state);
                if state.page == Page::Rules {
                    commands.extend(ensure_rules_loaded(state));
                }
                commands
            } else {
                Vec::new()
            }
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
                        if state.terminal_width < 90 {
                            state.focus = Focus::Content;
                        }
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
        UiEvent::RulesLoaded {
            request_id,
            connection_generation,
            result,
        } => {
            let current_runtime_generation = state
                .status
                .as_ref()
                .and_then(|status| status.runtime_generation);
            let accepted_runtime_generation = state
                .rules
                .load_pending
                .as_ref()
                .filter(|pending| {
                    pending.request_id == request_id
                        && pending.connection_generation == connection_generation
                        && connection_generation == state.connection.generation
                        && pending.runtime_generation == current_runtime_generation
                })
                .map(|pending| pending.runtime_generation);
            if let Some(runtime_generation) = accepted_runtime_generation {
                state.rules.load_pending = None;
                match result {
                    Ok(snapshot) => {
                        state.rules.rows = snapshot
                            .rows
                            .into_iter()
                            .take(LOCAL_RULE_COUNT_MAX)
                            .collect();
                        state.rules.initialized = snapshot.initialized;
                        state.rules.revision = snapshot.revision;
                        state.rules.loaded_connection_generation = Some(connection_generation);
                        state.rules.loaded_runtime_generation = runtime_generation;
                        state.rules.load_error = None;
                        state.rules.selected = 0;
                        state.rules.scroll = 0;
                        state.bump_view_revision();
                    }
                    Err(message) => {
                        state.rules.loaded_connection_generation = None;
                        state.rules.loaded_runtime_generation = None;
                        state.rules.load_error = Some(message.clone());
                        state.toast = Some(format!("Error: {message}"));
                    }
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
                let pinned_sequence = selected_log_sequence(state);
                for record in records {
                    push_view_log(&mut state.logs, record);
                }
                state.logs.gap |= gap;
                state.logs.dropped_total = state.logs.dropped_total.max(dropped_total);
                if state.logs.follow {
                    state.logs.scroll = 0;
                } else if let Some(sequence) = pinned_sequence {
                    restore_log_selection(state, sequence);
                } else {
                    state.logs.scroll = clamp_index(state.logs.scroll, state.filtered_log_count());
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
            let mut commands = Vec::new();
            let current = state.pending.as_ref().is_some_and(|pending| {
                pending.request_id == request_id
                    && pending.connection_generation == connection_generation
                    && connection_generation == state.connection.generation
            });
            if current {
                let pending_kind = state.pending.as_ref().map(|pending| pending.kind);
                let profile_operation = pending_kind == Some(PendingOperationKind::ActivateProfile);
                let rule_operation = pending_kind.is_some_and(|kind| {
                    matches!(
                        kind,
                        PendingOperationKind::AddRule
                            | PendingOperationKind::ReplaceRule
                            | PendingOperationKind::RemoveRule
                    )
                });
                let lifecycle_operation = pending_kind.is_some_and(|kind| {
                    matches!(
                        kind,
                        PendingOperationKind::RestartSupervisor
                            | PendingOperationKind::StopSupervisor
                    )
                });
                let stop_operation = pending_kind == Some(PendingOperationKind::StopSupervisor);
                match result {
                    Ok(success) => {
                        if profile_operation {
                            state.profiles.activation_pending = None;
                            state.modal = None;
                            state.focus = Focus::Content;
                        }
                        if rule_operation {
                            state.modal = None;
                            state.focus = Focus::Content;
                        }
                        if lifecycle_operation {
                            state.modal = None;
                            state.focus = Focus::Content;
                            state.should_quit |= stop_operation;
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
                if profile_operation && state.page == Page::Rules {
                    commands.extend(ensure_rules_loaded(state));
                }
                if rule_operation {
                    commands.extend(invalidate_rule_cache(state));
                    if state.page == Page::Rules {
                        commands.extend(ensure_rules_loaded(state));
                    }
                }
            }
            commands
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
            normalize_focus(state);
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
        UiIntent::SwitchPage(page) => switch_page(state, page),
        UiIntent::NextPage => {
            let page = state.page.next();
            switch_page(state, page)
        }
        UiIntent::PreviousPage => {
            let page = state.page.previous();
            switch_page(state, page)
        }
        UiIntent::FocusNext => move_focus(state, 1),
        UiIntent::FocusPrevious => move_focus(state, -1),
        UiIntent::FocusLeft => move_horizontal_focus(state, -1),
        UiIntent::FocusRight => move_horizontal_focus(state, 1),
        UiIntent::FocusSearch => {
            if search_available(state) {
                state.focus = Focus::Search;
            }
            Vec::new()
        }
        UiIntent::OpenCommandPalette => {
            state.modal = Some(Modal::CommandPalette);
            state.focus = Focus::Modal;
            state.command_palette.selected = clamp_index(
                state.command_palette.selected,
                filtered_palette_actions(&state.command_palette).len(),
            );
            Vec::new()
        }
        UiIntent::OpenProfiles => {
            state.modal = Some(Modal::Profiles);
            state.focus = Focus::Content;
            state.profiles.selected =
                clamp_index(state.profiles.selected, state.filtered_profile_count());
            Vec::new()
        }
        UiIntent::RunPaletteAction(action) => run_palette_action(state, action),
        UiIntent::ConfirmLifecycleAction => confirm_lifecycle_action(state),
        UiIntent::LoadRules => issue_rule_load(state),
        UiIntent::OpenRuleAdd => {
            if state.rules_projection_ready() {
                open_rule_editor(state, None);
            }
            Vec::new()
        }
        UiIntent::OpenSelectedRuleEditor => {
            if state.rules_projection_ready() {
                open_selected_rule_editor(state);
            }
            Vec::new()
        }
        UiIntent::RequestSelectedRuleRemoval => {
            if state.rules_projection_ready() {
                request_selected_rule_removal(state);
            }
            Vec::new()
        }
        UiIntent::SubmitRuleEditor => submit_rule_editor(state),
        UiIntent::ConfirmRuleRemoval => confirm_rule_removal(state),
        UiIntent::SelectRule(index) => {
            if state.rules_projection_ready() {
                state.rules.selected = clamp_index(index, state.filtered_rule_count());
                state.rules.scroll = state.rules.selected;
                state.focus = Focus::Content;
            }
            Vec::new()
        }
        UiIntent::SelectLog { tail_offset } => {
            let filtered_count = state.filtered_log_count();
            if state.page == Page::Logs && state.modal.is_none() && tail_offset < filtered_count {
                state.logs.follow = false;
                state.logs.scroll = tail_offset;
                state.focus = Focus::Content;
            }
            Vec::new()
        }
        UiIntent::PreviousProxyGroup => move_proxy_group(state, -1),
        UiIntent::NextProxyGroup => move_proxy_group(state, 1),
        UiIntent::ShowProxyGroup(group_id) => issue_proxy_group_load(state, group_id),
        UiIntent::InputCharacter(character) => {
            append_input(state, character);
            Vec::new()
        }
        UiIntent::Backspace => {
            backspace_input(state);
            Vec::new()
        }
        UiIntent::Escape => escape(state),
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
                state.logs.scroll = 0;
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
        UiIntent::ToggleProxyDetail => {
            if state.page == Page::Proxies && state.focus == Focus::Content {
                state.proxy_detail_open = !state.proxy_detail_open;
                state.zoomed_focus = false;
            }
            Vec::new()
        }
        UiIntent::ToggleZoom => {
            if state.modal.is_none() && state.page == Page::Proxies {
                state.zoomed_focus = !state.zoomed_focus;
            }
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

fn append_input(state: &mut AppState, character: char) {
    if character.is_control() {
        return;
    }
    if state.modal == Some(Modal::CommandPalette) {
        if state.command_palette.query.chars().count() < TUI_SEARCH_MAX_CHARACTERS
            && state
                .command_palette
                .query
                .len()
                .saturating_add(character.len_utf8())
                <= TUI_SEARCH_MAX_BYTES
        {
            state.command_palette.query.push(character);
            state.command_palette.selected = clamp_index(
                state.command_palette.selected,
                filtered_palette_actions(&state.command_palette).len(),
            );
        }
        return;
    }
    if let Some(Modal::RuleEditor { value, .. }) = state.modal.as_mut() {
        if value.len().saturating_add(character.len_utf8()) <= RULE_STRING_MAX_BYTES {
            value.push(character);
        }
        return;
    }
    if let Some(search) = current_search_mut(state)
        && search.chars().count() < TUI_SEARCH_MAX_CHARACTERS
        && search.len().saturating_add(character.len_utf8()) <= TUI_SEARCH_MAX_BYTES
    {
        search.push(character);
        state.clamp_selections();
    }
}

fn backspace_input(state: &mut AppState) {
    if state.modal == Some(Modal::CommandPalette) {
        state.command_palette.query.pop();
        state.command_palette.selected = clamp_index(
            state.command_palette.selected,
            filtered_palette_actions(&state.command_palette).len(),
        );
        return;
    }
    if let Some(Modal::RuleEditor { value, .. }) = state.modal.as_mut() {
        value.pop();
        return;
    }
    if let Some(search) = current_search_mut(state) {
        search.pop();
    }
    state.clamp_selections();
}

fn switch_page(state: &mut AppState, page: Page) -> Vec<Command> {
    state.page = page;
    state.modal = None;
    state.zoomed_focus = false;
    state.focus = Focus::Content;
    if page == Page::Rules {
        ensure_rules_loaded(state)
    } else {
        Vec::new()
    }
}

fn available_focuses(state: &AppState) -> &'static [Focus] {
    const OVERVIEW: &[Focus] = &[
        Focus::Tabs,
        Focus::Content,
        Focus::FooterHelp,
        Focus::FooterQuit,
    ];
    const PROXIES: &[Focus] = &[
        Focus::Tabs,
        Focus::ProxyGroups,
        Focus::Content,
        Focus::Search,
        Focus::FooterHelp,
        Focus::FooterQuit,
    ];
    const CONNECTIONS: &[Focus] = OVERVIEW;
    const SEARCHABLE: &[Focus] = &[
        Focus::Tabs,
        Focus::Content,
        Focus::Search,
        Focus::FooterHelp,
        Focus::FooterQuit,
    ];
    const PROFILES: &[Focus] = &[Focus::Content, Focus::Search];
    const MODAL: &[Focus] = &[Focus::Modal];

    match state.modal.as_ref() {
        Some(Modal::Profiles) => PROFILES,
        Some(
            Modal::Help
            | Modal::CommandPalette
            | Modal::RuleEditor { .. }
            | Modal::RuleRemovalConfirmation { .. }
            | Modal::LifecycleConfirmation { .. }
            | Modal::Message { .. },
        ) => MODAL,
        None => match state.page {
            Page::Overview => OVERVIEW,
            Page::Proxies => PROXIES,
            Page::Connections => CONNECTIONS,
            Page::Rules | Page::Logs => SEARCHABLE,
        },
    }
}

fn move_focus(state: &mut AppState, delta: isize) -> Vec<Command> {
    let focuses = available_focuses(state);
    let index = focuses
        .iter()
        .position(|candidate| *candidate == state.focus)
        .unwrap_or(0);
    let next = if delta < 0 {
        index.checked_sub(1).unwrap_or(focuses.len() - 1)
    } else {
        (index + 1) % focuses.len()
    };
    state.focus = focuses[next];
    Vec::new()
}

fn move_horizontal_focus(state: &mut AppState, delta: isize) -> Vec<Command> {
    if state.modal.is_none() && state.page == Page::Proxies {
        match (state.focus, delta < 0) {
            (Focus::Content, true) | (Focus::Search, true) => {
                state.focus = Focus::ProxyGroups;
            }
            (Focus::ProxyGroups, false) => state.focus = Focus::Content,
            _ => {}
        }
    }
    Vec::new()
}

fn normalize_focus(state: &mut AppState) {
    let focuses = available_focuses(state);
    if !focuses.contains(&state.focus) {
        state.focus = focuses[0];
    }
}

fn search_available(state: &AppState) -> bool {
    state.modal == Some(Modal::Profiles)
        || state.modal.is_none() && matches!(state.page, Page::Proxies | Page::Rules | Page::Logs)
}

fn escape(state: &mut AppState) -> Vec<Command> {
    if state.modal == Some(Modal::Profiles) && state.focus == Focus::Search {
        state.focus = Focus::Content;
    } else if state.modal.is_some() {
        state.modal = None;
        state.focus = Focus::Content;
    } else if state.focus == Focus::Search {
        state.focus = Focus::Content;
    } else if state.page == Page::Proxies && state.proxy_detail_open {
        state.proxy_detail_open = false;
    } else if state.zoomed_focus {
        state.zoomed_focus = false;
    } else if state.page == Page::Logs && !state.logs.follow {
        state.logs.follow = true;
        state.logs.scroll = 0;
    } else if state.page == Page::Proxies
        && state.focus == Focus::Content
        && state.terminal_width < 90
    {
        state.focus = Focus::ProxyGroups;
    } else {
        state.focus = Focus::Content;
    }
    Vec::new()
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
        if state.terminal_width < 90 {
            state.focus = Focus::Content;
        }
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

fn ensure_rules_loaded(state: &mut AppState) -> Vec<Command> {
    let connection_generation = state.connection.generation;
    let runtime_generation = state
        .status
        .as_ref()
        .and_then(|status| status.runtime_generation);
    if state.connection.status != ConnectionStatus::Connected
        || state.rules.loaded_connection_generation == Some(connection_generation)
            && state.rules.loaded_runtime_generation == runtime_generation
        || state.rules.load_pending.as_ref().is_some_and(|pending| {
            pending.connection_generation == connection_generation
                && pending.runtime_generation == runtime_generation
        })
    {
        Vec::new()
    } else {
        issue_rule_load(state)
    }
}

fn issue_rule_load(state: &mut AppState) -> Vec<Command> {
    if state.connection.status != ConnectionStatus::Connected {
        return Vec::new();
    }
    let request_id = state.take_request_id();
    let mut commands = cancel_rule_load(state);
    state.rules.load_error = None;
    state.rules.load_pending = Some(PendingRuleLoad {
        request_id,
        connection_generation: state.connection.generation,
        runtime_generation: state
            .status
            .as_ref()
            .and_then(|status| status.runtime_generation),
    });
    commands.push(Command::FetchRules {
        request_id,
        connection_generation: state.connection.generation,
    });
    commands
}

fn invalidate_rule_cache(state: &mut AppState) -> Vec<Command> {
    let commands = cancel_rule_load(state);
    state.rules.loaded_connection_generation = None;
    state.rules.loaded_runtime_generation = None;
    state.rules.load_error = None;
    commands
}

pub(in crate::tui) fn filtered_palette_actions(
    state: &CommandPaletteState,
) -> Vec<CommandPaletteAction> {
    let needle = state.query.to_lowercase();
    CommandPaletteAction::ALL
        .into_iter()
        .filter(|action| {
            needle.is_empty()
                || action.label().contains(&needle)
                || action.description().to_lowercase().contains(&needle)
        })
        .collect()
}

fn run_palette_action(state: &mut AppState, action: CommandPaletteAction) -> Vec<Command> {
    match action {
        CommandPaletteAction::Profiles => {
            state.modal = Some(Modal::Profiles);
            state.focus = Focus::Content;
            state.profiles.selected =
                clamp_index(state.profiles.selected, state.filtered_profile_count());
            Vec::new()
        }
        CommandPaletteAction::RestartSupervisor => issue_lifecycle_action(state, action),
        CommandPaletteAction::StopSupervisor => {
            state.modal = Some(Modal::LifecycleConfirmation { action });
            state.focus = Focus::Modal;
            Vec::new()
        }
    }
}

fn confirm_lifecycle_action(state: &mut AppState) -> Vec<Command> {
    let Some(Modal::LifecycleConfirmation { action }) = state.modal.as_ref() else {
        return Vec::new();
    };
    if state.modal_action_pending() {
        return Vec::new();
    }
    issue_lifecycle_action(state, *action)
}

fn issue_lifecycle_action(state: &mut AppState, action: CommandPaletteAction) -> Vec<Command> {
    let request_id = state.take_request_id();
    let mut commands = cancel_pending(state);
    let connection_generation = state.connection.generation;
    let (kind, command) = match action {
        CommandPaletteAction::RestartSupervisor => (
            PendingOperationKind::RestartSupervisor,
            Command::RestartSupervisor {
                request_id,
                connection_generation,
            },
        ),
        CommandPaletteAction::StopSupervisor => (
            PendingOperationKind::StopSupervisor,
            Command::StopSupervisor {
                request_id,
                connection_generation,
            },
        ),
        CommandPaletteAction::Profiles => return Vec::new(),
    };
    state.pending = Some(PendingOperation {
        request_id,
        connection_generation,
        kind,
    });
    state.bump_view_revision();
    commands.push(command);
    commands
}

fn open_rule_editor(state: &mut AppState, original: Option<String>) {
    let value = original.clone().unwrap_or_default();
    state.modal = Some(Modal::RuleEditor { original, value });
    state.focus = Focus::Modal;
}

fn open_selected_rule_editor(state: &mut AppState) {
    let rule = filtered_rules(&state.rules)
        .get(state.rules.selected)
        .map(|row| row.rule_string.clone());
    if let Some(rule) = rule {
        open_rule_editor(state, Some(rule));
    }
}

fn request_selected_rule_removal(state: &mut AppState) {
    let rule = filtered_rules(&state.rules)
        .get(state.rules.selected)
        .map(|row| row.rule_string.clone());
    if let Some(rule) = rule {
        state.modal = Some(Modal::RuleRemovalConfirmation { rule });
        state.focus = Focus::Modal;
    }
}

fn submit_rule_editor(state: &mut AppState) -> Vec<Command> {
    if !state.rules_projection_ready() || state.modal_action_pending() {
        return Vec::new();
    }
    let Some(Modal::RuleEditor { original, value }) = state.modal.clone() else {
        return Vec::new();
    };
    if value.is_empty() {
        state.toast = Some("Error: Rule String is required".to_owned());
        return Vec::new();
    }
    if original.as_deref() == Some(value.as_str()) {
        state.toast = Some("Rule String is unchanged".to_owned());
        return Vec::new();
    }

    let request_id = state.take_request_id();
    let mut commands = cancel_pending(state);
    let connection_generation = state.connection.generation;
    let (kind, command) = match original {
        Some(old_rule) => (
            PendingOperationKind::ReplaceRule,
            Command::ReplaceRule {
                request_id,
                connection_generation,
                old_rule,
                new_rule: value,
            },
        ),
        None => (
            PendingOperationKind::AddRule,
            Command::AddRule {
                request_id,
                connection_generation,
                rule: value,
            },
        ),
    };
    state.pending = Some(PendingOperation {
        request_id,
        connection_generation,
        kind,
    });
    state.bump_view_revision();
    commands.push(command);
    commands
}

fn confirm_rule_removal(state: &mut AppState) -> Vec<Command> {
    if !state.rules_projection_ready() || state.modal_action_pending() {
        return Vec::new();
    }
    let Some(Modal::RuleRemovalConfirmation { rule }) = state.modal.clone() else {
        return Vec::new();
    };
    let request_id = state.take_request_id();
    let mut commands = cancel_pending(state);
    let connection_generation = state.connection.generation;
    state.pending = Some(PendingOperation {
        request_id,
        connection_generation,
        kind: PendingOperationKind::RemoveRule,
    });
    state.bump_view_revision();
    commands.push(Command::RemoveRule {
        request_id,
        connection_generation,
        rule,
    });
    commands
}

fn current_search_mut(state: &mut AppState) -> Option<&mut String> {
    if state.modal == Some(Modal::Profiles) {
        return Some(&mut state.profiles.filter);
    }
    match state.page {
        Page::Overview => None,
        Page::Proxies => Some(&mut state.proxies.filter),
        Page::Connections => None,
        Page::Rules => Some(&mut state.rules.filter),
        Page::Logs => Some(&mut state.logs.search),
    }
}

fn move_selection(state: &mut AppState, delta: isize) {
    if state.modal == Some(Modal::CommandPalette) {
        state.command_palette.selected = moved_index(
            state.command_palette.selected,
            filtered_palette_actions(&state.command_palette).len(),
            delta,
        );
        return;
    }
    if state.modal == Some(Modal::Profiles) {
        state.profiles.selected = moved_index(
            state.profiles.selected,
            state.filtered_profile_count(),
            delta,
        );
        state.profiles.scroll = state.profiles.selected;
        return;
    }
    match state.page {
        Page::Overview => {}
        Page::Proxies => {
            state.proxies.selected =
                moved_index(state.proxies.selected, state.filtered_proxy_count(), delta);
            state.proxies.scroll = state.proxies.selected;
        }
        Page::Connections => {}
        Page::Rules if state.rules_projection_ready() => {
            state.rules.selected =
                moved_index(state.rules.selected, state.filtered_rule_count(), delta);
            state.rules.scroll = state.rules.selected;
        }
        Page::Rules => {}
        Page::Logs => {
            let filtered_count = state.filtered_log_count();
            if filtered_count > 0 {
                state.logs.follow = false;
                state.logs.scroll = if delta < 0 {
                    state.logs.scroll.saturating_add(1).min(filtered_count - 1)
                } else {
                    state.logs.scroll.saturating_sub(1)
                };
            }
        }
    }
}

fn selected_log_sequence(state: &AppState) -> Option<u64> {
    let row_indices = filtered_log_indices(&state.logs);
    selected_log_position(&state.logs, row_indices.len())
        .and_then(|position| row_indices.get(position))
        .and_then(|index| state.logs.records.get(*index))
        .map(|record| record.sequence)
}

fn restore_log_selection(state: &mut AppState, sequence: u64) {
    let row_indices = filtered_log_indices(&state.logs);
    if let Some(position) = row_indices.iter().position(|index| {
        state
            .logs
            .records
            .get(*index)
            .is_some_and(|record| record.sequence == sequence)
    }) {
        state.logs.scroll = row_indices.len().saturating_sub(position.saturating_add(1));
    } else {
        state.logs.scroll = clamp_index(state.logs.scroll, row_indices.len());
    }
}

fn activate_selected(state: &mut AppState) -> Vec<Command> {
    if state.modal == Some(Modal::CommandPalette) {
        return filtered_palette_actions(&state.command_palette)
            .get(state.command_palette.selected)
            .copied()
            .map_or_else(Vec::new, |action| run_palette_action(state, action));
    }
    if state.modal == Some(Modal::Profiles) {
        return filtered_profiles(&state.profiles)
            .get(state.profiles.selected)
            .map(|row| row.id)
            .map_or_else(Vec::new, |profile_id| {
                issue_profile_activation(state, profile_id)
            });
    }
    match state.page {
        Page::Proxies if state.proxies.group_load_pending.is_some() => Vec::new(),
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
        Page::Rules if state.rules_projection_ready() => {
            open_selected_rule_editor(state);
            Vec::new()
        }
        Page::Rules => Vec::new(),
        Page::Connections => Vec::new(),
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
    commands.extend(cancel_rule_load(state));
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

fn cancel_rule_load(state: &mut AppState) -> Vec<Command> {
    state
        .rules
        .load_pending
        .take()
        .map_or_else(Vec::new, |pending| {
            vec![Command::Cancel {
                request_id: pending.request_id,
            }]
        })
}

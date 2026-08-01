use std::collections::VecDeque;
use std::io;

use crossterm::event::{
    Event as CrosstermEvent, KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent,
    MouseEventKind,
};
use ratatui::Terminal;
use ratatui::backend::TestBackend;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;

use hopash::application::{LatencyFreshness, LatencyProbeStatus};
use hopash::constants::{
    LOG_CAPACITY, MAX_ACTIVE_NODES, MINIMUM_TERMINAL_HEIGHT, MINIMUM_TERMINAL_WIDTH,
    TRAFFIC_SERIES_CAPACITY,
};
use hopash::domain::{
    ActiveProfileSummary, ApplyState, CoreDiagnosticCategory, CoreLifecycle, CoreRestartStatus,
    CoreStatus, NodeRecordId, ProbeQueueStatus, ProfileId, ProxyGroupId, RuntimeApplyPhase,
    RuntimeApplySnapshot, RuntimeGeneration, RuntimeRecoverySnapshot, RuntimeRecoveryStatus,
    SampleState, SelectedNodeSummary, StatusSnapshot, StreamHealthSet, StreamState,
    SupervisorHealthReason, SupervisorLifecycle, SupervisorStatus, TrafficSample, TunReason,
    TunStatus,
};
use hopash::ipc::RequestId;
use hopash::telemetry::{LogLevel, LogSource};
use hopash::tui::{
    AppState, Command, ConnectionStatus, EventBudgets, EventSource, FairEventInbox, Focus,
    FullViewSnapshot, InteractionMap, KeyInput, LogLevelFilter, Modal, MouseInput, MouseInputKind,
    MutationSuccess, Page, ProfileRow, ProxyGroupRow, ProxyGroupSnapshot, ProxyRow, TerminalAction,
    TerminalControl, TerminalInput, TerminalSession, UiEvent, UiIntent, ViewLogRecord,
    from_crossterm_event, input_to_intent, render, render_buffer, update,
};

#[test]
fn all_four_pages_render_their_primary_content() {
    let mut state = connected_state();
    for (page, expected) in [
        (Page::Overview, "Supervisor"),
        (Page::Proxies, "Tokyo"),
        (Page::Profiles, "Work"),
        (Page::Logs, "connected to Core"),
    ] {
        state.page = page;
        let (text, _) = render_with_backend(&state, 100, 30);
        assert!(text.contains(expected), "{page:?} should render {expected}");
        for title in ["Overview", "Proxies", "Profiles", "Logs"] {
            assert!(text.contains(title));
        }
    }
}

#[test]
fn overview_renders_probe_queue_overload_metrics() {
    let mut state = connected_state();
    let status = state
        .status
        .as_mut()
        .expect("the connected fixture should have status");
    status.probe_queue = ProbeQueueStatus {
        active_node_count: 10,
        queue_depth: 7,
        in_flight_count: 2,
        overloaded: true,
        oldest_due_age_ms: Some(12_345),
        estimated_full_pass_duration_ms: 30_000,
        stale_node_count: 4,
    };
    status.selection_restore_pending = true;

    let (text, _) = render_with_backend(&state, 120, 32);

    assert!(text.contains("Probe Queue: overloaded"));
    assert!(text.contains("Selection Restore: pending"));
    assert!(text.contains("queued 7, in-flight 2, stale 40.0%"));
    assert!(text.contains("oldest 12345 ms, full pass 30000 ms"));
}

#[test]
fn overview_renders_core_restart_and_tun_diagnostics() {
    let mut state = connected_state();
    let status = state
        .status
        .as_mut()
        .expect("the connected fixture should have status");
    status.core.lifecycle = CoreLifecycle::Starting;
    status.core.restart = CoreRestartStatus {
        pending: true,
        attempts: 2,
        backoff_ms: Some(4_000),
        diagnostic: Some(CoreDiagnosticCategory::RestartLimitReached),
    };
    status.tun.capable = false;
    status.tun.effective = false;
    status.tun.reason = Some(TunReason::PermissionDenied);

    let (text, _) = render_with_backend(&state, 80, 24);

    assert!(text.contains("Restart: on, tries=2, wait=4000 ms"));
    assert!(text.contains("Diagnostic: core_restart_limit_reached"));
    assert!(text.contains("TUN: off, cap=no, permission_denied"));
    assert!(text.contains("Connections: 3 | Uptime: 60s"));
}

#[test]
fn overview_renders_the_final_stopped_supervisor_lifecycle() {
    let mut state = connected_state();
    let status = state
        .status
        .as_mut()
        .expect("the connected fixture should have status");
    status.supervisor.lifecycle = SupervisorLifecycle::Stopped;
    status.core.lifecycle = CoreLifecycle::Stopped;

    let (text, _) = render_with_backend(&state, 80, 24);

    assert!(text.contains("Supervisor: Stopped | Core: Stopped"));
}

#[test]
fn overview_renders_supervisor_health_reasons() {
    let mut state = connected_state();
    let status = state
        .status
        .as_mut()
        .expect("the connected fixture should have status");
    status.supervisor.lifecycle = SupervisorLifecycle::Degraded;
    status.supervisor.health_reasons = vec![
        SupervisorHealthReason::RuntimeRecovery,
        SupervisorHealthReason::SelectionRestoration,
    ];

    let (text, _) = render_with_backend(&state, 100, 28);

    assert!(text.contains("Health: runtime_recovery, selection_restoration"));
}

#[test]
fn compact_overview_renders_runtime_apply_recovery() {
    let mut state = connected_state();
    let status = state
        .status
        .as_mut()
        .expect("the connected fixture should have status");
    status.apply_state = ApplyState::Recovering;
    status.runtime_apply = RuntimeApplySnapshot {
        candidate_generation: Some(RuntimeGeneration(2)),
        committed_generation: Some(RuntimeGeneration(1)),
        phase: RuntimeApplyPhase::Recovering,
        recovery: RuntimeRecoverySnapshot {
            status: RuntimeRecoveryStatus::Pending,
            restored_generation: Some(RuntimeGeneration(1)),
            message: Some("Committed Runtime Generation cleanup is pending".to_owned()),
        },
    };

    let (text, _) = render_with_backend(&state, 80, 24);

    assert!(text.contains("Apply: recovering, candidate=2, committed=1"));
    assert!(text.contains("Recovery: pending, restored=1"));
    assert!(text.contains("Why: Committed Runtime Generation cleanup is pending"));
    assert!(text.contains("Connections: 3 | Uptime: 60s"));
}

#[test]
fn proxy_rows_without_stable_node_ids_are_visible_and_read_only() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.proxies.rows = vec![ProxyRow {
        group_id: ProxyGroupId::for_name("Automatic"),
        group: "Automatic".to_owned(),
        node_id: None,
        name: "Missing member".to_owned(),
        node_type: "missing".to_owned(),
        available: false,
        selected: false,
        delay_ms: None,
        sampled_at_unix_ms: None,
        freshness: LatencyFreshness::NotSampled,
        probe_status: LatencyProbeStatus::NotSampled,
    }];
    state.proxies.selected = 0;

    let (text, map) = render_with_backend(&state, 100, 30);
    state.publish_interaction_map(map);

    assert!(text.contains("Missing member"));
    assert!(text.contains("unavailable"));
    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Enter)),
        None
    );
    assert!(
        state
            .interaction_map()
            .expect("rendered state should publish interactions")
            .interactions
            .iter()
            .all(|interaction| !matches!(interaction.intent, UiIntent::SelectNode { .. }))
    );
}

#[test]
fn rendering_replaces_terminal_control_characters_in_external_text() {
    let mut state = connected_state();
    state
        .status
        .as_mut()
        .expect("connected fixture should have status")
        .active_profile
        .as_mut()
        .expect("connected fixture should have an active Profile")
        .name = "Profile\u{1b}X".to_owned();
    state.proxies.rows[0].name = "Node\u{1b}X".to_owned();
    state.profiles.rows[0].name = "Stored\u{1b}X".to_owned();
    state.logs.records[0].message = "Log\u{1b}X".to_owned();
    state.toast = Some("Toast\u{1b}X".to_owned());

    for (page, expected) in [
        (Page::Overview, "Profile?X"),
        (Page::Proxies, "Node?X"),
        (Page::Profiles, "Stored?X"),
        (Page::Logs, "Log?X"),
    ] {
        state.page = page;
        let (text, _) = render_with_backend(&state, 100, 30);
        assert!(text.contains(expected), "{page:?} should sanitize its text");
        assert!(text.contains("Toast?X"));
        assert!(!text.contains('\u{1b}'));
    }
}

#[test]
fn minimum_size_view_reports_required_and_current_dimensions() {
    let state = AppState::new();
    let area = Rect::new(0, 0, 70, 20);
    let mut buffer = Buffer::empty(area);

    let map = render_buffer(&state, area, &mut buffer);
    let text = buffer_text(&buffer);

    assert!(text.contains("Terminal too small"));
    assert!(text.contains(&format!(
        "Required: {}x{}",
        MINIMUM_TERMINAL_WIDTH, MINIMUM_TERMINAL_HEIGHT
    )));
    assert!(text.contains("Current: 70x20"));
    assert!(map.interactions.is_empty());
}

#[test]
fn resize_invalidates_the_presented_interaction_map_and_recomputes_layout() {
    let mut state = connected_state();
    let (_, map) = render_with_backend(&state, 100, 30);
    state.publish_interaction_map(map);
    assert!(state.interaction_map().is_some());

    update(
        &mut state,
        UiEvent::Resize {
            width: 70,
            height: 20,
        },
    );

    assert!(state.interaction_map().is_none());
    assert!(state.render_dirty);
    let area = Rect::new(0, 0, 70, 20);
    let mut buffer = Buffer::empty(area);
    let replacement = render_buffer(&state, area, &mut buffer);
    assert!(replacement.interactions.is_empty());
}

#[test]
fn keyboard_and_mouse_profile_activation_produce_the_same_intent_and_command() {
    let mut keyboard_state = connected_state();
    keyboard_state.page = Page::Profiles;
    let (_, map) = render_with_backend(&keyboard_state, 100, 30);
    let profile_hit = hit_for(&map, |intent| {
        matches!(intent, UiIntent::ActivateProfile(_))
    });
    keyboard_state.publish_interaction_map(map.clone());
    let mut mouse_state = keyboard_state.clone();

    let keyboard = input_to_intent(&keyboard_state, TerminalInput::Key(KeyInput::Enter))
        .expect("selected Profile should be actionable");
    let mouse = input_to_intent(
        &mouse_state,
        TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: profile_hit.0,
            row: profile_hit.1,
        }),
    )
    .expect("Profile row should be clickable");

    assert_eq!(keyboard, mouse);
    assert_eq!(
        update(&mut keyboard_state, UiEvent::Intent(keyboard)),
        update(&mut mouse_state, UiEvent::Intent(mouse))
    );
}

#[test]
fn keyboard_and_mouse_node_selection_produce_the_same_intent() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    let (_, map) = render_with_backend(&state, 100, 30);
    let node_hit = hit_for(&map, |intent| matches!(intent, UiIntent::SelectNode { .. }));
    state.publish_interaction_map(map);

    let keyboard = input_to_intent(&state, TerminalInput::Key(KeyInput::Enter));
    let mouse = input_to_intent(
        &state,
        TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: node_hit.0,
            row: node_hit.1,
        }),
    );

    assert_eq!(keyboard, mouse);
}

#[test]
fn keyboard_and_mouse_proxy_group_switches_share_the_same_intent() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.focus = Focus::ProxyGroups;
    state.proxies.groups.push(manual_proxy_group());
    state.proxies.group_cursor = 1;
    let (_, map) = render_with_backend(&state, 140, 30);
    let manual_group_id = ProxyGroupId::for_name("Manual");
    let manual_hit = hit_for(&map, |intent| {
        *intent == UiIntent::ShowProxyGroup(manual_group_id.clone())
    });
    let next_hit = hit_for(&map, |intent| *intent == UiIntent::NextProxyGroup);
    state.publish_interaction_map(map);

    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Right)),
        input_to_intent(
            &state,
            TerminalInput::Mouse(MouseInput {
                kind: MouseInputKind::LeftClick,
                column: next_hit.0,
                row: next_hit.1,
            })
        )
    );

    let keyboard = input_to_intent(&state, TerminalInput::Key(KeyInput::Enter));
    let mouse = input_to_intent(
        &state,
        TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: manual_hit.0,
            row: manual_hit.1,
        }),
    );

    assert_eq!(keyboard, mouse);
    assert!(matches!(
        update(
            &mut state,
            UiEvent::Intent(keyboard.expect("focused Proxy Group should switch"))
        )
        .as_slice(),
        [Command::FetchProxyGroup { group_id, .. }] if group_id == &manual_group_id
    ));
}

#[test]
fn keyboard_and_mouse_sort_controls_share_the_same_intent() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    let (_, map) = render_with_backend(&state, 100, 30);
    let name_sort = hit_for(&map, |intent| {
        *intent == UiIntent::SetProxySort(hopash::tui::ProxySort::Name)
    });
    state.publish_interaction_map(map);

    let keyboard = input_to_intent(&state, TerminalInput::Key(KeyInput::Character('s')));
    let mouse = input_to_intent(
        &state,
        TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: name_sort.0,
            row: name_sort.1,
        }),
    );

    assert_eq!(keyboard, mouse);
    update(
        &mut state,
        UiEvent::Intent(keyboard.expect("sort intent should exist")),
    );
    let (text, _) = render_with_backend(&state, 100, 30);
    let nodes = text
        .split_once("Nodes (")
        .map(|(_, nodes)| nodes)
        .expect("Node list should render");
    assert!(
        nodes.find("Berlin").expect("Berlin should render")
            < nodes.find("Tokyo").expect("Tokyo should render")
    );
}

#[test]
fn mouse_input_before_the_first_successful_frame_is_ignored() {
    let state = connected_state();

    assert_eq!(
        input_to_intent(
            &state,
            TerminalInput::Mouse(MouseInput {
                kind: MouseInputKind::LeftClick,
                column: 1,
                row: 1,
            })
        ),
        None
    );
}

#[test]
fn interaction_map_covers_tabs_search_footer_log_controls_and_scroll() {
    let mut state = connected_state();
    state.page = Page::Logs;
    let (_, map) = render_with_backend(&state, 100, 30);
    for expected in [
        UiIntent::SwitchPage(Page::Overview),
        UiIntent::SwitchPage(Page::Proxies),
        UiIntent::SwitchPage(Page::Profiles),
        UiIntent::SwitchPage(Page::Logs),
        UiIntent::FocusSearch,
        UiIntent::ToggleHelp,
        UiIntent::Quit,
        UiIntent::SetLogLevel(LogLevelFilter::All),
        UiIntent::SetLogLevel(LogLevelFilter::Error),
        UiIntent::ToggleLogPause,
        UiIntent::FollowLogs,
    ] {
        assert!(
            map.interactions
                .iter()
                .any(|interaction| interaction.intent == expected),
            "missing interaction for {expected:?}"
        );
    }
    let scroll = map
        .scroll_regions
        .first()
        .expect("Logs list should be scrollable");
    assert_eq!(
        map.intent_for(MouseInput {
            kind: MouseInputKind::ScrollUp,
            column: scroll.area.x,
            row: scroll.area.y,
        }),
        Some(UiIntent::ScrollUp)
    );
    let error_filter = hit_for(&map, |intent| {
        *intent == UiIntent::SetLogLevel(LogLevelFilter::Error)
    });
    state.publish_interaction_map(map);
    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Character('e'))),
        input_to_intent(
            &state,
            TerminalInput::Mouse(MouseInput {
                kind: MouseInputKind::LeftClick,
                column: error_filter.0,
                row: error_filter.1,
            })
        )
    );
}

#[test]
fn focused_search_receives_q_and_slash_as_text() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.focus = Focus::Search;

    for character in ['q', '/'] {
        let intent = input_to_intent(&state, TerminalInput::Key(KeyInput::Character(character)))
            .expect("focused search should receive text");
        update(&mut state, UiEvent::Intent(intent));
    }

    assert_eq!(state.proxies.filter, "q/");
    assert!(!state.should_quit);
}

#[test]
fn global_shortcuts_and_modal_routing_follow_input_priority() {
    let mut state = connected_state();

    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('?'))),
    );
    assert_eq!(state.modal, Some(Modal::Help));
    assert_eq!(state.focus, Focus::Modal);
    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('q'))),
    );
    assert!(state.modal.is_none());
    assert!(!state.should_quit);

    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('q'))),
    );
    assert!(state.should_quit);
}

#[test]
fn tab_shift_tab_and_page_shortcuts_update_navigation_state() {
    let mut state = connected_state();
    state.focus = Focus::Tabs;

    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Tab)),
    );
    assert_eq!(state.focus, Focus::Content);
    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::BackTab)),
    );
    assert_eq!(state.focus, Focus::Tabs);
    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('4'))),
    );
    assert_eq!(state.page, Page::Logs);
}

#[test]
fn tab_and_shift_tab_visit_only_controls_present_on_each_page() {
    for (page, first_focus) in [
        (Page::Overview, Focus::Content),
        (Page::Proxies, Focus::ProxyGroups),
        (Page::Profiles, Focus::Content),
        (Page::Logs, Focus::Content),
    ] {
        let mut state = connected_state();
        state.page = page;
        state.focus = Focus::Tabs;

        update(
            &mut state,
            UiEvent::Terminal(TerminalInput::Key(KeyInput::Tab)),
        );
        assert_eq!(state.focus, first_focus, "unexpected focus on {page:?}");
        update(
            &mut state,
            UiEvent::Terminal(TerminalInput::Key(KeyInput::BackTab)),
        );
        assert_eq!(
            state.focus,
            Focus::Tabs,
            "unexpected reverse focus on {page:?}"
        );
    }
}

#[test]
fn proxy_group_browsing_keeps_mutations_and_discards_late_group_results() {
    let mut state = connected_state();
    state.proxies.groups.push(manual_proxy_group());
    let profile_id = state.profiles.rows[1].id;
    let mutation_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::ActivateProfile(profile_id)),
    );
    let (mutation_request_id, generation) = activation_identity(&mutation_commands);
    let mutation_snapshot = snapshot_from_state(&state);

    let group_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::ShowProxyGroup(ProxyGroupId::for_name("Manual"))),
    );
    let (group_request_id, group_generation) = proxy_group_identity(&group_commands);
    assert_eq!(group_generation, generation);
    assert!(
        group_commands
            .iter()
            .all(|command| !matches!(command, Command::Cancel { request_id } if *request_id == mutation_request_id))
    );
    assert_eq!(
        state.pending.as_ref().map(|pending| pending.request_id),
        Some(mutation_request_id)
    );

    update(
        &mut state,
        UiEvent::CommandResult {
            request_id: mutation_request_id,
            connection_generation: generation,
            result: Ok(MutationSuccess {
                message: "Profile activated".to_owned(),
                snapshot: mutation_snapshot,
            }),
        },
    );
    let toast = state.toast.clone();
    update(
        &mut state,
        UiEvent::ProxyGroupLoaded {
            request_id: group_request_id,
            connection_generation: group_generation,
            result: Ok(manual_proxy_snapshot()),
        },
    );

    assert_eq!(state.proxies.selected_group.as_deref(), Some("Automatic"));
    assert_eq!(state.toast, toast);
}

#[test]
fn proxy_group_load_and_mutation_states_are_visually_distinct() {
    let mut profile_state = connected_state();
    profile_state.page = Page::Profiles;
    profile_state.profiles.selected = 1;
    update(
        &mut profile_state,
        UiEvent::Intent(UiIntent::ActivateSelected),
    );
    let (profile_text, _) = render_with_backend(&profile_state, 180, 30);
    assert!(profile_text.contains("[active]"));
    assert!(profile_text.contains("[pending]"));

    let mut state = connected_state();
    state.page = Page::Proxies;
    state.proxies.groups.push(manual_proxy_group());
    state.proxies.selected = 1;
    update(&mut state, UiEvent::Intent(UiIntent::ActivateSelected));
    let (pending_text, _) = render_with_backend(&state, 180, 30);
    assert!(pending_text.contains("[current]"));
    assert!(pending_text.contains("[pending]"));

    let group_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::ShowProxyGroup(ProxyGroupId::for_name("Manual"))),
    );
    let (request_id, generation) = proxy_group_identity(&group_commands);
    update(
        &mut state,
        UiEvent::ProxyGroupLoaded {
            request_id,
            connection_generation: generation,
            result: Ok(manual_proxy_snapshot()),
        },
    );
    let (success_text, _) = render_with_backend(&state, 180, 30);
    assert!(success_text.contains("[current] Manual"));
    assert!(success_text.contains("Success: Loaded Proxy Group Manual"));

    let failed_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::ShowProxyGroup(ProxyGroupId::for_name(
            "Automatic",
        ))),
    );
    let (failed_request_id, failed_generation) = proxy_group_identity(&failed_commands);
    update(
        &mut state,
        UiEvent::ProxyGroupLoaded {
            request_id: failed_request_id,
            connection_generation: failed_generation,
            result: Err("injected group failure".to_owned()),
        },
    );
    let (failure_text, _) = render_with_backend(&state, 180, 30);
    assert!(failure_text.contains("Error: injected group failure"));
    assert_eq!(state.proxies.selected_group.as_deref(), Some("Manual"));
}

#[test]
fn stale_command_results_are_discarded_by_request_and_connection_generation() {
    let mut state = connected_state();
    state.page = Page::Profiles;
    let profile_id = state.profiles.rows[1].id;
    let commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::ActivateProfile(profile_id)),
    );
    let (request_id, generation) = activation_identity(&commands);
    let mut refreshed_status = status_snapshot();
    refreshed_status.active_profile = Some(ActiveProfileSummary {
        id: profile_id,
        name: "Backup".to_owned(),
    });
    refreshed_status.selected_node = Some(SelectedNodeSummary {
        id: NodeRecordId::for_core("Berlin"),
        name: "Berlin".to_owned(),
    });
    refreshed_status.traffic.upload_bytes_per_second = 900;
    let refreshed_snapshot = FullViewSnapshot {
        status: refreshed_status,
        proxy_groups: vec![proxy_group()],
        proxies: vec![proxy("Tokyo", false), proxy("Berlin", true)],
        profiles: vec![
            ProfileRow {
                id: state.profiles.rows[0].id,
                name: "Work".to_owned(),
                active: false,
                ..state.profiles.rows[0].clone()
            },
            ProfileRow {
                id: profile_id,
                name: "Backup".to_owned(),
                active: true,
                ..state.profiles.rows[1].clone()
            },
        ],
        logs: vec![log(2, LogLevel::Info, "Profile activated")],
        dropped_logs: 3,
    };

    update(
        &mut state,
        UiEvent::CommandResult {
            request_id: RequestId(request_id.0 + 1),
            connection_generation: generation,
            result: Ok(MutationSuccess {
                message: "stale".to_owned(),
                snapshot: refreshed_snapshot.clone(),
            }),
        },
    );
    assert!(state.pending.is_some());
    assert_ne!(state.toast.as_deref(), Some("stale"));
    assert_eq!(
        state
            .status
            .as_ref()
            .expect("connected state should retain its snapshot")
            .traffic
            .upload_bytes_per_second,
        100
    );

    update(
        &mut state,
        UiEvent::CommandResult {
            request_id,
            connection_generation: generation + 1,
            result: Ok(MutationSuccess {
                message: "wrong generation".to_owned(),
                snapshot: refreshed_snapshot.clone(),
            }),
        },
    );
    assert!(state.pending.is_some());
    assert_ne!(state.toast.as_deref(), Some("wrong generation"));

    update(
        &mut state,
        UiEvent::CommandResult {
            request_id,
            connection_generation: generation,
            result: Ok(MutationSuccess {
                message: "Profile activated".to_owned(),
                snapshot: refreshed_snapshot,
            }),
        },
    );
    assert!(state.pending.is_none());
    assert_eq!(state.toast.as_deref(), Some("Success: Profile activated"));
    assert_eq!(
        state
            .status
            .as_ref()
            .and_then(|status| status.active_profile.as_ref())
            .map(|profile| profile.name.as_str()),
        Some("Backup")
    );
    assert_eq!(
        state
            .status
            .as_ref()
            .expect("refreshed status should be present")
            .traffic
            .upload_bytes_per_second,
        900
    );
    assert_eq!(
        state
            .profiles
            .rows
            .iter()
            .find(|profile| profile.id == profile_id)
            .map(|profile| profile.active),
        Some(true)
    );
    assert_eq!(
        state
            .proxies
            .rows
            .iter()
            .find(|proxy| proxy.name == "Berlin")
            .map(|proxy| proxy.selected),
        Some(true)
    );
    assert_eq!(state.logs.dropped_total, 3);
}

#[test]
fn disconnect_retains_a_stale_snapshot_and_reconnect_replaces_all_view_data() {
    let mut state = connected_state();
    let old_status_started_at = state
        .status
        .as_ref()
        .expect("fixture has status")
        .supervisor
        .started_at_unix_ms;

    let commands = update(
        &mut state,
        UiEvent::Disconnected {
            connection_generation: 1,
        },
    );

    assert_eq!(state.connection.status, ConnectionStatus::Disconnected);
    assert!(state.connection.snapshot_stale);
    assert_eq!(
        state
            .status
            .as_ref()
            .expect("stale snapshot is retained")
            .supervisor
            .started_at_unix_ms,
        old_status_started_at
    );
    assert_eq!(
        commands,
        [Command::ScheduleReconnect {
            connection_generation: 1,
        }]
    );

    let reconnect = update(
        &mut state,
        UiEvent::ReconnectDeadline {
            connection_generation: 1,
        },
    );
    assert_eq!(
        reconnect,
        [Command::Connect {
            connection_generation: 2,
        }]
    );
    let replacement_profile = profile("Replacement", true);
    let replacement_node = proxy("Replacement Node", true);
    let mut replacement_status = status_snapshot();
    replacement_status.supervisor.started_at_unix_ms = 9_999;
    update(
        &mut state,
        UiEvent::Connected {
            connection_generation: 2,
            snapshot: FullViewSnapshot {
                status: replacement_status,
                proxy_groups: vec![proxy_group()],
                proxies: vec![replacement_node],
                profiles: vec![replacement_profile],
                logs: vec![log(99, LogLevel::Warn, "replacement")],
                dropped_logs: 4,
            },
        },
    );

    assert_eq!(state.connection.status, ConnectionStatus::Connected);
    assert!(!state.connection.snapshot_stale);
    assert_eq!(state.proxies.rows[0].name, "Replacement Node");
    assert_eq!(state.profiles.rows[0].name, "Replacement");
    assert_eq!(state.logs.records[0].sequence, 99);
    assert_eq!(state.logs.dropped_total, 4);
}

#[test]
fn view_caches_remain_bounded_at_release_scale() {
    let proxies = (0..=MAX_ACTIVE_NODES)
        .map(|index| proxy(&format!("node-{index}"), false))
        .collect::<Vec<_>>();
    let profiles = (0..=hopash::tui::PROFILE_VIEW_CAPACITY)
        .map(|index| profile(&format!("profile-{index}"), index == 0))
        .collect::<Vec<_>>();
    let logs = (0..=LOG_CAPACITY)
        .map(|index| log(index as u64, LogLevel::Info, "bounded"))
        .collect::<Vec<_>>();
    let mut state = AppState::new();

    update(
        &mut state,
        UiEvent::Connected {
            connection_generation: 1,
            snapshot: FullViewSnapshot {
                status: status_snapshot(),
                proxy_groups: vec![proxy_group()],
                proxies,
                profiles,
                logs,
                dropped_logs: 0,
            },
        },
    );

    assert_eq!(state.proxies.rows.len(), MAX_ACTIVE_NODES);
    assert_eq!(
        state.profiles.rows.len(),
        hopash::tui::PROFILE_VIEW_CAPACITY
    );
    assert_eq!(state.logs.records.len(), LOG_CAPACITY);
}

#[test]
fn traffic_series_use_latest_samples_with_fixed_capacity() {
    let mut state = connected_state();

    for index in 0..TRAFFIC_SERIES_CAPACITY + 10 {
        let mut status = status_snapshot();
        status.traffic.upload_bytes_per_second = index as u64;
        status.traffic.download_bytes_per_second = index as u64 * 2;
        update(
            &mut state,
            UiEvent::StatusSnapshot {
                connection_generation: 1,
                status,
            },
        );
    }

    assert_eq!(state.upload_series.len(), TRAFFIC_SERIES_CAPACITY);
    assert_eq!(state.download_series.len(), TRAFFIC_SERIES_CAPACITY);
    assert_eq!(
        state.upload_series.back(),
        Some(&((TRAFFIC_SERIES_CAPACITY + 9) as u64))
    );
}

#[test]
fn paused_logs_freeze_the_anchor_and_resume_requests_a_bounded_tail() {
    let mut state = connected_state();
    state.page = Page::Logs;
    let anchor = state.logs.records.back().map(|record| record.sequence);

    let pause_commands = update(&mut state, UiEvent::Intent(UiIntent::ToggleLogPause));
    assert!(pause_commands.is_empty());
    assert!(state.logs.paused);
    assert_eq!(state.logs.paused_anchor, anchor);
    let length = state.logs.records.len();
    update(
        &mut state,
        UiEvent::LogBatch {
            connection_generation: 1,
            records: vec![log(50, LogLevel::Error, "ignored while paused")],
            gap: false,
            dropped_total: 0,
        },
    );
    assert_eq!(state.logs.records.len(), length);

    let resume = update(&mut state, UiEvent::Intent(UiIntent::ToggleLogPause));
    assert_eq!(
        resume,
        [Command::FetchLogTail {
            connection_generation: 1,
            after_sequence: anchor,
        }]
    );
    assert!(!state.logs.paused);
}

#[test]
fn user_log_query_filters_time_level_and_content_and_shows_stream_state() {
    let mut state = connected_state();
    state.page = Page::Logs;
    state.logs.records = VecDeque::from([
        log_at(10, 10, LogLevel::Warn, "retry too early"),
        log_at(11, 20, LogLevel::Info, "retry wrong level"),
        log_at(12, 30, LogLevel::Warn, "retry accepted"),
        log_at(13, 50, LogLevel::Warn, "retry too late"),
    ]);
    state.logs.paused = true;
    state.logs.follow = false;
    state.logs.dropped_total = 7;
    state.logs.gap = true;
    state.focus = Focus::Search;
    for character in "since:20 until:40 level:warn content:retry".chars() {
        update(
            &mut state,
            UiEvent::Intent(UiIntent::InputCharacter(character)),
        );
    }

    let (text, _) = render_with_backend(&state, 180, 30);

    assert!(text.contains("retry accepted"));
    assert!(!text.contains("retry too early"));
    assert!(!text.contains("retry wrong level"));
    assert!(!text.contains("retry too late"));
    assert!(text.contains("paused"));
    assert!(text.contains("manual"));
    assert!(text.contains("dropped=7"));
    assert!(text.contains("gap"));
}

#[test]
fn overview_and_proxy_rows_show_latency_time_freshness_and_probe_state() {
    let mut state = connected_state();
    let (overview, _) = render_with_backend(&state, 180, 30);
    assert!(overview.contains("Sampled At: 1000"));
    assert!(overview.contains("Freshness: fresh"));
    assert!(overview.contains("Probe: succeeded (generation 1)"));

    state.page = Page::Proxies;
    let (proxies, _) = render_with_backend(&state, 180, 30);
    assert!(proxies.contains("sampled=1000"));
    assert!(proxies.contains("freshness=fresh"));
    assert!(proxies.contains("probe=succeeded"));
}

#[test]
fn modal_rendering_stacks_over_the_page_and_exposes_only_close_interaction() {
    let mut state = connected_state();
    state.modal = Some(Modal::Help);
    state.focus = Focus::Modal;

    let (text, map) = render_with_backend(&state, 100, 30);

    assert!(text.contains("Keyboard and mouse help"));
    assert!(text.contains("[Close]"));
    assert_eq!(map.interactions.len(), 1);
    assert_eq!(map.interactions[0].intent, UiIntent::CloseModal);
}

#[test]
fn fair_event_inbox_gives_each_ready_source_its_budget_every_round() {
    let budgets = EventBudgets {
        terminal: 2,
        command_result: 1,
        deadline: 1,
        telemetry: 1,
    };
    let mut inbox = FairEventInbox::new(16, budgets).expect("fixture limits should be valid");
    for _ in 0..10 {
        inbox.push(
            EventSource::Terminal,
            UiEvent::Terminal(TerminalInput::Key(KeyInput::Down)),
        );
    }
    inbox.push(
        EventSource::CommandResult,
        UiEvent::CommandResult {
            request_id: RequestId(1),
            connection_generation: 1,
            result: Err("failed".to_owned()),
        },
    );
    inbox.push(
        EventSource::Deadline,
        UiEvent::ReconnectDeadline {
            connection_generation: 1,
        },
    );
    inbox.push(
        EventSource::Telemetry,
        UiEvent::LogBatch {
            connection_generation: 1,
            records: vec![],
            gap: false,
            dropped_total: 0,
        },
    );

    let round = inbox.drain_round();

    assert_eq!(round.len(), 5);
    assert!(matches!(round[0], UiEvent::Terminal(_)));
    assert!(matches!(round[1], UiEvent::Terminal(_)));
    assert!(matches!(round[2], UiEvent::CommandResult { .. }));
    assert!(matches!(round[3], UiEvent::ReconnectDeadline { .. }));
    assert!(matches!(round[4], UiEvent::LogBatch { .. }));
    assert_eq!(inbox.len(EventSource::Terminal), 8);
}

#[test]
fn shutdown_has_priority_and_source_queues_are_bounded() {
    let mut inbox =
        FairEventInbox::new(2, EventBudgets::default()).expect("fixture limits should be valid");
    for _ in 0..3 {
        inbox.push(
            EventSource::Terminal,
            UiEvent::Terminal(TerminalInput::Key(KeyInput::Down)),
        );
    }
    inbox.push(EventSource::Telemetry, UiEvent::Shutdown);

    assert_eq!(inbox.len(EventSource::Terminal), 2);
    assert_eq!(inbox.dropped(EventSource::Terminal), 1);
    assert!(matches!(
        inbox.drain_round().as_slice(),
        [UiEvent::Shutdown]
    ));
    assert_eq!(inbox.len(EventSource::Terminal), 2);
}

#[test]
fn telemetry_inbox_coalesces_status_to_the_latest_value() {
    let mut inbox =
        FairEventInbox::new(4, EventBudgets::default()).expect("fixture limits should be valid");
    for value in [10, 20, 30] {
        let mut status = status_snapshot();
        status.traffic.upload_bytes_per_second = value;
        inbox.push(
            EventSource::Telemetry,
            UiEvent::StatusSnapshot {
                connection_generation: 1,
                status,
            },
        );
    }

    assert_eq!(inbox.len(EventSource::Telemetry), 1);
    let round = inbox.drain_round();
    assert!(matches!(
        round.as_slice(),
        [UiEvent::StatusSnapshot { status, .. }]
            if status.traffic.upload_bytes_per_second == 30
    ));
}

#[test]
fn terminal_cleanup_is_reverse_ordered_and_idempotent() {
    let mut recorder = RecordingTerminal::default();
    {
        let mut session = TerminalSession::enter(&mut recorder)
            .expect("fixture terminal initialization should succeed");
        assert!(!session.is_cleaned());
        session.cleanup().expect("cleanup should succeed");
        assert!(session.is_cleaned());
        session
            .cleanup()
            .expect("repeated cleanup should be a no-op");
    }

    assert_eq!(
        recorder.actions,
        [
            TerminalAction::EnableRawMode,
            TerminalAction::EnterAlternateScreen,
            TerminalAction::EnableMouseCapture,
            TerminalAction::EnableFocusReporting,
            TerminalAction::EnableBracketedPaste,
            TerminalAction::HideCursor,
            TerminalAction::ShowCursor,
            TerminalAction::DisableBracketedPaste,
            TerminalAction::DisableFocusReporting,
            TerminalAction::DisableMouseCapture,
            TerminalAction::LeaveAlternateScreen,
            TerminalAction::DisableRawMode,
        ]
    );
}

#[test]
fn terminal_guard_drop_restores_modes_during_panic_unwind() {
    let mut recorder = RecordingTerminal::default();

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _session = TerminalSession::enter(&mut recorder)
            .expect("fixture terminal initialization should succeed");
        panic!("injected panic");
    }));

    assert!(result.is_err());
    assert_eq!(
        recorder.actions.last(),
        Some(&TerminalAction::DisableRawMode)
    );
    assert!(recorder.actions.contains(&TerminalAction::ShowCursor));
    assert!(
        recorder
            .actions
            .contains(&TerminalAction::LeaveAlternateScreen)
    );
}

#[test]
fn partial_terminal_initialization_cleans_every_attempted_mode() {
    let mut recorder = RecordingTerminal {
        fail_once: Some(TerminalAction::EnableFocusReporting),
        ..RecordingTerminal::default()
    };

    let error = TerminalSession::enter(&mut recorder)
        .err()
        .expect("configured initialization step should fail");

    assert_eq!(error.failed_action, TerminalAction::EnableFocusReporting);
    assert_eq!(
        recorder.actions,
        [
            TerminalAction::EnableRawMode,
            TerminalAction::EnterAlternateScreen,
            TerminalAction::EnableMouseCapture,
            TerminalAction::EnableFocusReporting,
            TerminalAction::DisableFocusReporting,
            TerminalAction::DisableMouseCapture,
            TerminalAction::LeaveAlternateScreen,
            TerminalAction::DisableRawMode,
        ]
    );
}

#[test]
fn terminal_cleanup_continues_after_a_restore_error() {
    let mut recorder = RecordingTerminal {
        fail_once: Some(TerminalAction::DisableBracketedPaste),
        ..RecordingTerminal::default()
    };
    let mut session = TerminalSession::enter(&mut recorder)
        .expect("fixture terminal initialization should succeed");
    assert!(session.cleanup().is_err());
    drop(session);
    assert!(recorder.actions.contains(&TerminalAction::DisableRawMode));
    assert!(recorder.actions.contains(&TerminalAction::ShowCursor));
}

#[test]
fn crossterm_events_map_to_typed_terminal_events() {
    assert!(matches!(
        from_crossterm_event(CrosstermEvent::Key(KeyEvent::new(
            KeyCode::Char('q'),
            KeyModifiers::NONE,
        ))),
        Some(UiEvent::Terminal(TerminalInput::Key(KeyInput::Character(
            'q'
        ))))
    ));
    assert!(matches!(
        from_crossterm_event(CrosstermEvent::Mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column: 4,
            row: 5,
            modifiers: KeyModifiers::NONE,
        })),
        Some(UiEvent::Terminal(TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: 4,
            row: 5,
        })))
    ));
    assert!(matches!(
        from_crossterm_event(CrosstermEvent::Resize(90, 40)),
        Some(UiEvent::Resize {
            width: 90,
            height: 40,
        })
    ));
}

#[derive(Default)]
struct RecordingTerminal {
    actions: Vec<TerminalAction>,
    fail_once: Option<TerminalAction>,
}

impl TerminalControl for RecordingTerminal {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()> {
        self.actions.push(action);
        if self.fail_once == Some(action) {
            self.fail_once = None;
            return Err(io::Error::other("injected terminal failure"));
        }
        Ok(())
    }
}

fn connected_state() -> AppState {
    let mut state = AppState::new();
    update(
        &mut state,
        UiEvent::Connected {
            connection_generation: 1,
            snapshot: FullViewSnapshot {
                status: status_snapshot(),
                proxy_groups: vec![proxy_group()],
                proxies: vec![proxy("Tokyo", true), proxy("Berlin", false)],
                profiles: vec![profile("Work", true), profile("Backup", false)],
                logs: vec![log(1, LogLevel::Info, "connected to Core")],
                dropped_logs: 0,
            },
        },
    );
    state.toast = None;
    state
}

fn status_snapshot() -> StatusSnapshot {
    let profile_id = ProfileId::new();
    let node_id = NodeRecordId::for_core("Tokyo");
    StatusSnapshot {
        supervisor: SupervisorStatus {
            lifecycle: SupervisorLifecycle::Ready,
            started_at_unix_ms: 1_000,
            uptime_seconds: 60,
            health_reasons: Vec::new(),
        },
        core: CoreStatus {
            lifecycle: CoreLifecycle::Ready,
            pid: Some(42),
            instance_generation: Some(hopash::domain::CoreInstanceGeneration(1)),
            restart: hopash::domain::CoreRestartStatus::default(),
        },
        tun: TunStatus {
            requested: true,
            capable: true,
            effective: true,
            reason: None,
        },
        active_profile: Some(ActiveProfileSummary {
            id: profile_id,
            name: "Work".to_owned(),
        }),
        primary_proxy_group: Some("Automatic".to_owned()),
        selected_node: Some(SelectedNodeSummary {
            id: node_id.clone(),
            name: "Tokyo".to_owned(),
        }),
        latency: Some(hopash::domain::LatencySample {
            node_id,
            delay_ms: Some(42),
            sampled_at_unix_ms: Some(1_000),
            state: SampleState::Fresh,
            probe_generation: hopash::domain::ProbeGeneration(1),
        }),
        traffic: TrafficSample {
            upload_bytes_per_second: 100,
            download_bytes_per_second: 200,
            sampled_at_unix_ms: Some(1_000),
            state: SampleState::Fresh,
        },
        connection_count: 3,
        runtime_generation: Some(RuntimeGeneration(1)),
        apply_state: ApplyState::Idle,
        runtime_apply: RuntimeApplySnapshot {
            candidate_generation: Some(RuntimeGeneration(1)),
            committed_generation: Some(RuntimeGeneration(1)),
            phase: RuntimeApplyPhase::Succeeded,
            ..RuntimeApplySnapshot::default()
        },
        selection_restore_pending: false,
        probe_queue: ProbeQueueStatus::default(),
        stream_health: StreamHealthSet {
            traffic: StreamState::Healthy,
            connections: StreamState::Healthy,
            logs: StreamState::Healthy,
        },
    }
}

fn proxy(name: &str, selected: bool) -> ProxyRow {
    ProxyRow {
        group_id: ProxyGroupId::for_name("Automatic"),
        group: "Automatic".to_owned(),
        node_id: Some(NodeRecordId::for_core(name)),
        name: name.to_owned(),
        node_type: "Shadowsocks".to_owned(),
        available: true,
        selected,
        delay_ms: selected.then_some(42),
        sampled_at_unix_ms: selected.then_some(1_000),
        freshness: if selected {
            LatencyFreshness::Fresh
        } else {
            LatencyFreshness::NotSampled
        },
        probe_status: if selected {
            LatencyProbeStatus::Succeeded
        } else {
            LatencyProbeStatus::NotSampled
        },
    }
}

fn proxy_group() -> ProxyGroupRow {
    ProxyGroupRow {
        id: ProxyGroupId::for_name("Automatic"),
        name: "Automatic".to_owned(),
        proxy_type: "Selector".to_owned(),
        selected_node: Some("Tokyo".to_owned()),
    }
}

fn manual_proxy_group() -> ProxyGroupRow {
    ProxyGroupRow {
        id: ProxyGroupId::for_name("Manual"),
        name: "Manual".to_owned(),
        proxy_type: "Selector".to_owned(),
        selected_node: Some("Paris".to_owned()),
    }
}

fn manual_proxy_snapshot() -> ProxyGroupSnapshot {
    let mut paris = proxy("Paris", true);
    paris.group_id = ProxyGroupId::for_name("Manual");
    paris.group = "Manual".to_owned();
    ProxyGroupSnapshot {
        group: manual_proxy_group(),
        groups: vec![proxy_group(), manual_proxy_group()],
        proxies: vec![paris],
    }
}

fn snapshot_from_state(state: &AppState) -> FullViewSnapshot {
    FullViewSnapshot {
        status: state
            .status
            .clone()
            .expect("connected state should have status"),
        proxy_groups: state.proxies.groups.clone(),
        proxies: state.proxies.rows.clone(),
        profiles: state.profiles.rows.clone(),
        logs: state.logs.records.iter().cloned().collect(),
        dropped_logs: state.logs.dropped_total,
    }
}

fn profile(name: &str, active: bool) -> ProfileRow {
    ProfileRow {
        id: ProfileId::new(),
        name: name.to_owned(),
        active,
        fresh: true,
        last_success_at_unix_ms: 1_000,
        next_refresh_at_unix_ms: 2_000,
        error: None,
    }
}

fn log(sequence: u64, level: LogLevel, message: &str) -> ViewLogRecord {
    log_at(sequence, sequence.saturating_mul(10), level, message)
}

fn log_at(sequence: u64, timestamp_unix_ms: u64, level: LogLevel, message: &str) -> ViewLogRecord {
    ViewLogRecord {
        sequence,
        timestamp_unix_ms,
        level,
        source: LogSource::CoreApi,
        message: message.to_owned(),
    }
}

fn render_with_backend(state: &AppState, width: u16, height: u16) -> (String, InteractionMap) {
    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("test terminal should initialize");
    let mut interaction_map = None;
    terminal
        .draw(|frame| interaction_map = Some(render(frame, state)))
        .expect("test frame should render");
    (
        buffer_text(terminal.backend().buffer()),
        interaction_map.expect("render should produce an interaction map"),
    )
}

fn buffer_text(buffer: &Buffer) -> String {
    let area = buffer.area;
    (area.y..area.y + area.height)
        .map(|row| {
            (area.x..area.x + area.width)
                .map(|column| buffer[(column, row)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn hit_for(map: &InteractionMap, predicate: impl Fn(&UiIntent) -> bool) -> (u16, u16) {
    let interaction = map
        .interactions
        .iter()
        .find(|interaction| predicate(&interaction.intent))
        .expect("expected interaction should exist");
    (interaction.area.x, interaction.area.y)
}

fn activation_identity(commands: &[Command]) -> (RequestId, u64) {
    commands
        .iter()
        .find_map(|command| match command {
            Command::ActivateProfile {
                request_id,
                connection_generation,
                ..
            } => Some((*request_id, *connection_generation)),
            _ => None,
        })
        .expect("activation command should exist")
}

fn proxy_group_identity(commands: &[Command]) -> (RequestId, u64) {
    commands
        .iter()
        .find_map(|command| match command {
            Command::FetchProxyGroup {
                request_id,
                connection_generation,
                ..
            } => Some((*request_id, *connection_generation)),
            _ => None,
        })
        .expect("Proxy Group command should exist")
}

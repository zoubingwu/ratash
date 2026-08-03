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
use ratatui::style::Color;

use ratash::application::PolicyTargetValidation;
use ratash::constants::{
    CORE_LOG_LINE_MAX_BYTES, LOCAL_RULE_COUNT_MAX, LOG_CAPACITY, LOG_RETENTION_MAX_BYTES,
    MAX_ACTIVE_NODES, MINIMUM_TERMINAL_HEIGHT, MINIMUM_TERMINAL_WIDTH, TUI_SEARCH_MAX_BYTES,
    TUI_SEARCH_MAX_CHARACTERS,
};
use ratash::domain::{
    ActiveProfileSummary, ApplyState, ConnectionRecord, CoreLifecycle, CoreStatus,
    LocalRuleSetRevision, NodeRecordId, ProbeQueueStatus, ProfileId, ProxyGroupId,
    RuntimeApplyPhase, RuntimeApplySnapshot, RuntimeGeneration, SampleState, SelectedNodeSummary,
    StatusSnapshot, StreamHealthSet, StreamState, SupervisorHealthReason, SupervisorLifecycle,
    SupervisorStatus, TrafficSample, TunStatus,
};
use ratash::ipc::RequestId;
use ratash::telemetry::{LogLevel, LogSource};
use ratash::tui::{
    AppState, Command, CommandPaletteAction, ConnectionStatus, EventBudgets, EventSource,
    FairEventInbox, Focus, FullViewSnapshot, InteractionMap, KeyInput, LogLevelFilter, Modal,
    MouseInput, MouseInputKind, MutationSuccess, Page, ProfileRow, ProxyGroupRow,
    ProxyGroupSnapshot, ProxyRow, RuleListSnapshot, RuleRow, RulesState, TerminalAction,
    TerminalControl, TerminalInput, TerminalSession, UiEvent, UiIntent, ViewLogRecord,
    from_crossterm_event, input_to_intent, render, render_buffer, update,
};

#[test]
fn all_four_pages_render_their_primary_content() {
    let mut state = connected_state();
    for (page, expected) in [
        (Page::Proxies, "Tokyo"),
        (Page::Connections, "example.com:443"),
        (Page::Rules, "DOMAIN-SUFFIX"),
        (Page::Logs, "connected to Core"),
    ] {
        state.page = page;
        let (text, _) = render_with_backend(&state, 100, 30);
        assert!(text.contains(expected), "{page:?} should render {expected}");
        for title in ["Proxies", "Connections", "Rules", "Logs"] {
            assert!(text.contains(title));
        }
    }
}

#[test]
fn global_header_uses_two_status_rows_above_navigation() {
    let state = connected_state();
    let area = Rect::new(0, 0, 160, 30);
    let (regions, _) = ratash::tui::compute_layout(&state, area, 1);
    let (text, _) = render_with_backend(&state, 160, 30);
    let lines = text.lines().collect::<Vec<_>>();

    assert_eq!(regions.status.height, 2);
    assert_eq!(regions.navigation.y, area.y + 2);
    assert!(lines[0].contains("Profile:"));
    assert!(lines[0].contains("Work"));
    assert!(lines[0].contains("Node:"));
    assert!(lines[0].contains("Tokyo"));
    assert!(lines[0].contains("Traffic:"));
    assert!(lines[0].find("Traffic:").is_some_and(|column| column > 100));
    assert!(lines[1].contains("Connections: 3"));
    assert!(lines[1].contains("Total:"));
    assert!(lines[1].contains("Memory:"));
    assert!(lines[1].contains("Mihomo: v1.19.28"));
    assert!(lines[1].contains("Mode: RULE"));
    assert!(lines[1].contains("Mixed: OFF"));
    assert!(lines[2].contains("Proxies"));
    assert!(lines[2].contains("Connections"));
}

#[test]
fn logs_default_to_info_from_the_mihomo_api() {
    let mut state = connected_state();
    state.page = Page::Logs;
    let mut process_log = log(2, LogLevel::Info, "Managed Core startup output");
    process_log.source = LogSource::Stdout;
    state.logs.records = VecDeque::from([
        log(
            1,
            LogLevel::Info,
            "[TCP] api.github.com:443 match DomainKeyword(github) using Manual",
        ),
        process_log,
    ]);

    let (text, _) = render_with_backend(&state, 160, 30);

    assert_eq!(state.logs.level_filter, LogLevelFilter::Info);
    assert!(text.contains("api.github.com:443"));
    assert!(!text.contains("Managed Core startup output"));
}

#[test]
fn connections_page_renders_target_rule_chain_and_traffic() {
    let mut state = connected_state();
    state.page = Page::Connections;

    let (text, _) = render_with_backend(&state, 140, 30);

    for expected in [
        "example.com:443",
        "DOMAIN-SUFFIX · example.com",
        "provider-only",
        "Manual",
        "2.0 KiB",
        "1.0 KiB",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    assert!(!text.contains("bounded Core projection"));
    assert!(!text.contains("Traffic sample"));
}

#[test]
fn automatic_proxy_groups_are_browsable_and_read_only() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.proxies.groups[0].proxy_type = "URLTest".to_owned();
    state.proxies.groups[0].selectable = false;

    let (text, map) = render_with_backend(&state, 140, 30);
    let commands = update(&mut state, UiEvent::Intent(UiIntent::ActivateSelected));

    assert!(text.contains("Automatic · URLTest"));
    assert!(
        map.interactions
            .iter()
            .all(|interaction| !matches!(interaction.intent, UiIntent::SelectNode { .. }))
    );
    assert!(commands.is_empty());
    assert!(state.proxies.selection_pending.is_none());
}

#[test]
fn keyboard_and_mouse_connection_selection_target_the_same_row() {
    let mut base = connected_state();
    base.page = Page::Connections;
    let mut second = base
        .status
        .as_ref()
        .expect("fixture status should exist")
        .connections[0]
        .clone();
    second.id = "second-connection".to_owned();
    second.host = Some("second.example.com".to_owned());
    base.status
        .as_mut()
        .expect("fixture status should exist")
        .connections
        .push(second);

    let mut keyboard = base.clone();
    let (_, keyboard_map) = render_with_backend(&keyboard, 100, 30);
    keyboard.publish_interaction_map(keyboard_map);
    update(
        &mut keyboard,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Down)),
    );

    let mut mouse = base;
    let (_, mouse_map) = render_with_backend(&mouse, 100, 30);
    let hit = hit_for(&mouse_map, |intent| {
        *intent == UiIntent::SelectConnection(1)
    });
    mouse.publish_interaction_map(mouse_map);
    update(
        &mut mouse,
        UiEvent::Terminal(TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: hit.0,
            row: hit.1,
        })),
    );

    assert_eq!(keyboard.connections.selected, 1);
    assert_eq!(mouse.connections.selected, keyboard.connections.selected);
}

#[test]
fn proxy_selection_moves_inside_the_viewport_before_the_list_scrolls() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.proxies.rows = (0..40)
        .map(|index| proxy(&format!("Node {index:02}"), index == 0))
        .collect();
    let (_, map) = render_with_backend(&state, 100, 24);
    let visible = map
        .scroll_regions
        .first()
        .expect("Proxy list should be scrollable")
        .area
        .height
        .saturating_sub(1) as usize;
    state.publish_interaction_map(map);

    for _ in 1..visible {
        update(&mut state, UiEvent::Intent(UiIntent::MoveDown));
    }
    assert_eq!(state.proxies.selected, visible.saturating_sub(1));
    assert_eq!(state.proxies.scroll, 0);

    update(&mut state, UiEvent::Intent(UiIntent::MoveDown));
    assert_eq!(state.proxies.selected, visible);
    assert_eq!(state.proxies.scroll, 1);

    update(&mut state, UiEvent::Intent(UiIntent::MoveUp));
    assert_eq!(state.proxies.scroll, 1);
}

#[test]
fn rule_selection_moves_inside_the_viewport_before_the_list_scrolls() {
    let mut state = connected_state();
    state.page = Page::Rules;
    let template = state.rules.rows[0].clone();
    state.rules.rows = (0..40)
        .map(|index| RuleRow {
            index,
            rule_string: format!("DOMAIN,example-{index}.com,Automatic"),
            payload: Some(format!("example-{index}.com")),
            ..template.clone()
        })
        .collect();
    let (_, map) = render_with_backend(&state, 100, 24);
    let visible = map
        .scroll_regions
        .first()
        .expect("Rule list should be scrollable")
        .area
        .height
        .saturating_sub(1) as usize;
    state.publish_interaction_map(map);

    for _ in 1..visible {
        update(&mut state, UiEvent::Intent(UiIntent::MoveDown));
    }
    assert_eq!(state.rules.scroll, 0);
    update(&mut state, UiEvent::Intent(UiIntent::MoveDown));
    assert_eq!(state.rules.scroll, 1);
    update(&mut state, UiEvent::Intent(UiIntent::MoveUp));
    assert_eq!(state.rules.scroll, 1);
}

#[test]
fn log_selection_moves_inside_the_viewport_before_the_list_scrolls() {
    let mut state = connected_state();
    state.page = Page::Logs;
    state.logs.records = (0..40)
        .map(|index| log(index, LogLevel::Info, &format!("message {index}")))
        .collect();
    state.logs.follow = false;
    state.logs.scroll = state.logs.records.len().saturating_sub(1);
    state.logs.viewport_start = 0;
    let (_, map) = render_with_backend(&state, 100, 24);
    let visible = map
        .scroll_regions
        .first()
        .expect("Log list should be scrollable")
        .area
        .height as usize;
    state.publish_interaction_map(map);

    for _ in 1..visible {
        update(&mut state, UiEvent::Intent(UiIntent::MoveDown));
    }
    assert_eq!(state.logs.viewport_start, 0);
    update(&mut state, UiEvent::Intent(UiIntent::MoveDown));
    assert_eq!(state.logs.viewport_start, 1);
    update(&mut state, UiEvent::Intent(UiIntent::MoveUp));
    assert_eq!(state.logs.viewport_start, 1);
}

#[test]
fn switching_to_rules_fetches_once_for_each_runtime_generation() {
    let mut state = connected_state();
    state.rules = RulesState::default();

    let first_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::SwitchPage(Page::Rules)),
    );
    let (first_request, connection_generation) = rule_request_identity(&first_commands);
    assert!(
        update(
            &mut state,
            UiEvent::Intent(UiIntent::SwitchPage(Page::Rules))
        )
        .is_empty()
    );
    update(
        &mut state,
        UiEvent::RulesLoaded {
            request_id: first_request,
            connection_generation,
            result: Ok(rule_list_snapshot("DOMAIN-SUFFIX,example.com,PROXY", 7)),
        },
    );
    assert_eq!(
        state.rules.loaded_connection_generation,
        Some(connection_generation)
    );
    assert_eq!(
        state.rules.loaded_runtime_generation,
        Some(RuntimeGeneration(1))
    );
    assert!(
        update(
            &mut state,
            UiEvent::Intent(UiIntent::SwitchPage(Page::Rules))
        )
        .is_empty()
    );

    let mut status = state.status.clone().expect("fixture status should exist");
    status.runtime_generation = Some(RuntimeGeneration(2));
    let reload = update(
        &mut state,
        UiEvent::StatusSnapshot {
            connection_generation,
            status,
        },
    );
    assert!(matches!(reload.as_slice(), [Command::FetchRules { .. }]));
}

#[test]
fn unconfigured_runtime_accepts_an_uninitialized_rule_projection() {
    let mut state = connected_state();
    state
        .status
        .as_mut()
        .expect("fixture status should exist")
        .runtime_generation = None;
    state.rules = RulesState::default();
    let commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::SwitchPage(Page::Rules)),
    );
    let (request_id, connection_generation) = rule_request_identity(&commands);

    update(
        &mut state,
        UiEvent::RulesLoaded {
            request_id,
            connection_generation,
            result: Ok(RuleListSnapshot {
                initialized: false,
                revision: None,
                rows: Vec::new(),
            }),
        },
    );

    assert_eq!(
        state.rules.loaded_connection_generation,
        Some(connection_generation)
    );
    assert_eq!(state.rules.loaded_runtime_generation, None);
    assert!(state.rules.load_pending.is_none());
}

#[test]
fn failed_rule_load_is_distinct_from_uninitialized_or_prior_rule_state() {
    let mut state = connected_state();
    state.rules.loaded_connection_generation = None;
    state.rules.loaded_runtime_generation = None;
    let commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::SwitchPage(Page::Rules)),
    );
    let (request_id, connection_generation) = rule_request_identity(&commands);

    update(
        &mut state,
        UiEvent::RulesLoaded {
            request_id,
            connection_generation,
            result: Err("Rule projection unavailable".to_owned()),
        },
    );
    let (text, _) = render_with_backend(&state, 160, 30);

    assert!(text.contains("Rule load failed: Rule projection unavailable"));
    assert!(!text.contains("DOMAIN-SUFFIX"));
    assert!(!text.contains("Local Rule Set is uninitialized"));
}

#[test]
fn rule_editor_maps_add_replace_and_confirmed_remove_to_typed_commands() {
    let mut add = connected_state();
    add.page = Page::Rules;
    update(&mut add, UiEvent::Intent(UiIntent::OpenRuleAdd));
    for character in "DOMAIN-SUFFIX,openai.com,PROXY".chars() {
        update(
            &mut add,
            UiEvent::Intent(UiIntent::InputCharacter(character)),
        );
    }
    let add_commands = update(&mut add, UiEvent::Intent(UiIntent::SubmitRuleEditor));
    let (add_request, connection_generation) = add_commands
        .iter()
        .find_map(|command| match command {
            Command::AddRule {
                request_id,
                connection_generation,
                rule,
            } if rule == "DOMAIN-SUFFIX,openai.com,PROXY" => {
                Some((*request_id, *connection_generation))
            }
            _ => None,
        })
        .expect("Rule add should produce a typed command");
    let reload = update(
        &mut add,
        UiEvent::CommandResult {
            request_id: add_request,
            connection_generation,
            result: Ok(MutationSuccess {
                message: "Rule added".to_owned(),
            }),
        },
    );
    assert!(add.modal.is_none());
    assert!(matches!(reload.as_slice(), [Command::FetchRules { .. }]));

    let mut replace = connected_state();
    replace.page = Page::Rules;
    update(
        &mut replace,
        UiEvent::Intent(UiIntent::OpenSelectedRuleEditor),
    );
    let Modal::RuleEditor { value, .. } = replace
        .modal
        .as_mut()
        .expect("selected Rule should open the editor")
    else {
        panic!("selected Rule should use the Rule editor modal");
    };
    *value = "DOMAIN-SUFFIX,example.org,DIRECT".to_owned();
    let replace_commands = update(&mut replace, UiEvent::Intent(UiIntent::SubmitRuleEditor));
    assert!(matches!(
        replace_commands.as_slice(),
        [Command::ReplaceRule {
            old_rule,
            new_rule,
            ..
        }] if old_rule == "DOMAIN-SUFFIX,example.com,Automatic"
            && new_rule == "DOMAIN-SUFFIX,example.org,DIRECT"
    ));

    let mut remove = connected_state();
    remove.page = Page::Rules;
    update(
        &mut remove,
        UiEvent::Intent(UiIntent::RequestSelectedRuleRemoval),
    );
    assert!(matches!(
        remove.modal,
        Some(Modal::RuleRemovalConfirmation { .. })
    ));
    let remove_commands = update(&mut remove, UiEvent::Intent(UiIntent::ConfirmRuleRemoval));
    assert!(matches!(
        remove_commands.as_slice(),
        [Command::RemoveRule { rule, .. }]
            if rule == "DOMAIN-SUFFIX,example.com,Automatic"
    ));
}

#[test]
fn rule_editor_submission_is_inert_while_apply_is_pending() {
    let mut state = connected_state();
    state.page = Page::Rules;
    update(&mut state, UiEvent::Intent(UiIntent::OpenRuleAdd));
    for character in "DOMAIN,openai.com,DIRECT".chars() {
        update(
            &mut state,
            UiEvent::Intent(UiIntent::InputCharacter(character)),
        );
    }
    let first = update(&mut state, UiEvent::Intent(UiIntent::SubmitRuleEditor));
    assert!(matches!(first.as_slice(), [Command::AddRule { .. }]));

    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Enter)),
        None
    );
    assert!(
        update(&mut state, UiEvent::Intent(UiIntent::SubmitRuleEditor)).is_empty(),
        "a repeated direct intent must preserve the in-flight mutation"
    );
    let (text, map) = render_with_backend(&state, 100, 30);
    assert!(text.contains("Applying…"));
    assert!(text.contains("[Esc] Close"));
    assert!(!text.contains("[Esc] Cancel"));
    assert!(
        map.interactions
            .iter()
            .all(|interaction| interaction.intent != UiIntent::SubmitRuleEditor)
    );
}

#[test]
fn rule_removal_confirmation_is_inert_while_remove_is_pending() {
    let mut state = connected_state();
    state.page = Page::Rules;
    update(
        &mut state,
        UiEvent::Intent(UiIntent::RequestSelectedRuleRemoval),
    );
    let first = update(&mut state, UiEvent::Intent(UiIntent::ConfirmRuleRemoval));
    assert!(matches!(first.as_slice(), [Command::RemoveRule { .. }]));

    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Character('y'))),
        None
    );
    assert!(
        update(&mut state, UiEvent::Intent(UiIntent::ConfirmRuleRemoval)).is_empty(),
        "a repeated direct intent must preserve the in-flight mutation"
    );
    let (text, map) = render_with_backend(&state, 100, 30);
    assert!(text.contains("Removing…"));
    assert!(text.contains("[Esc] Close"));
    assert!(!text.contains("[Esc] Cancel"));
    assert!(
        map.interactions
            .iter()
            .all(|interaction| interaction.intent != UiIntent::ConfirmRuleRemoval)
    );
}

#[test]
fn rules_render_selected_details_and_mouse_row_selection() {
    let mut state = connected_state();
    state.page = Page::Rules;

    let (text, map) = render_with_backend(&state, 120, 30);

    assert!(text.contains("SELECTED · RULE 1"));
    assert!(text.contains("0001"));
    assert!(text.contains("DOMAIN-SUFFIX,example.com,Automatic"));
    assert!(
        map.interactions
            .iter()
            .any(|interaction| interaction.intent == UiIntent::SelectRule(0))
    );
}

#[test]
fn stale_rule_projection_keeps_search_without_row_actions() {
    let mut state = connected_state();
    state.page = Page::Rules;
    let mut status = state.status.clone().expect("fixture status should exist");
    status.runtime_generation = Some(RuntimeGeneration(2));
    update(
        &mut state,
        UiEvent::StatusSnapshot {
            connection_generation: 1,
            status,
        },
    );

    for key in [
        KeyInput::Enter,
        KeyInput::Character('a'),
        KeyInput::Character('x'),
    ] {
        assert_eq!(input_to_intent(&state, TerminalInput::Key(key)), None);
    }
    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Character('/'))),
        Some(UiIntent::FocusSearch)
    );
    let (_, map) = render_with_backend(&state, 100, 30);
    assert!(
        map.interactions
            .iter()
            .all(|interaction| { !matches!(interaction.intent, UiIntent::SelectRule(_)) })
    );
    assert!(
        map.interactions
            .iter()
            .any(|interaction| interaction.intent == UiIntent::FocusSearch)
    );
    update(&mut state, UiEvent::Intent(UiIntent::FocusSearch));
    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Character('q'))),
        Some(UiIntent::InputCharacter('q'))
    );
    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('q'))),
    );
    assert_eq!(state.rules.filter, "q");
    assert!(!state.should_quit);
    update(
        &mut state,
        UiEvent::Intent(UiIntent::OpenSelectedRuleEditor),
    );
    assert!(state.modal.is_none());
}

#[test]
fn command_palette_filters_and_routes_profiles_restart_and_confirmed_stop() {
    let mut state = connected_state();
    update(&mut state, UiEvent::Intent(UiIntent::OpenCommandPalette));
    let (text, map) = render_with_backend(&state, 100, 30);
    assert!(text.contains("profile switch"));
    assert!(text.contains("restart"));
    assert!(text.contains("stop"));
    assert!(map.interactions.iter().any(|interaction| {
        interaction.intent == UiIntent::RunPaletteAction(CommandPaletteAction::Profiles)
    }));

    for character in "restart".chars() {
        update(
            &mut state,
            UiEvent::Intent(UiIntent::InputCharacter(character)),
        );
    }
    let restart = update(&mut state, UiEvent::Intent(UiIntent::ActivateSelected));
    assert!(matches!(
        restart.as_slice(),
        [Command::RestartSupervisor { .. }]
    ));

    let mut profiles = connected_state();
    update(
        &mut profiles,
        UiEvent::Intent(UiIntent::RunPaletteAction(CommandPaletteAction::Profiles)),
    );
    assert_eq!(profiles.modal, Some(Modal::Profiles));

    let mut stop = connected_state();
    update(
        &mut stop,
        UiEvent::Intent(UiIntent::RunPaletteAction(
            CommandPaletteAction::StopSupervisor,
        )),
    );
    assert!(matches!(
        stop.modal,
        Some(Modal::LifecycleConfirmation {
            action: CommandPaletteAction::StopSupervisor
        })
    ));
    let stop_commands = update(&mut stop, UiEvent::Intent(UiIntent::ConfirmLifecycleAction));
    let (request_id, connection_generation) = stop_commands
        .iter()
        .find_map(|command| match command {
            Command::StopSupervisor {
                request_id,
                connection_generation,
            } => Some((*request_id, *connection_generation)),
            _ => None,
        })
        .expect("confirmed stop should produce a typed command");
    update(
        &mut stop,
        UiEvent::CommandResult {
            request_id,
            connection_generation,
            result: Ok(MutationSuccess {
                message: "Supervisor stopped".to_owned(),
            }),
        },
    );
    assert!(stop.should_quit);
}

#[test]
fn lifecycle_confirmation_is_inert_while_stop_is_pending() {
    let mut state = connected_state();
    update(
        &mut state,
        UiEvent::Intent(UiIntent::RunPaletteAction(
            CommandPaletteAction::StopSupervisor,
        )),
    );
    let first = update(
        &mut state,
        UiEvent::Intent(UiIntent::ConfirmLifecycleAction),
    );
    assert!(matches!(first.as_slice(), [Command::StopSupervisor { .. }]));

    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Enter)),
        None
    );
    assert!(
        update(
            &mut state,
            UiEvent::Intent(UiIntent::ConfirmLifecycleAction)
        )
        .is_empty(),
        "a repeated direct intent must preserve the in-flight stop"
    );
    let (text, map) = render_with_backend(&state, 100, 30);
    assert!(text.contains("Stopping…"));
    assert!(text.contains("[Esc] Close"));
    assert!(!text.contains("[Esc] Cancel"));
    assert!(
        map.interactions
            .iter()
            .all(|interaction| interaction.intent != UiIntent::ConfirmLifecycleAction)
    );
}

#[test]
fn stop_disconnect_exits_without_scheduling_a_reconnect() {
    let mut state = connected_state();
    update(
        &mut state,
        UiEvent::Intent(UiIntent::RunPaletteAction(
            CommandPaletteAction::StopSupervisor,
        )),
    );
    let stop = update(
        &mut state,
        UiEvent::Intent(UiIntent::ConfirmLifecycleAction),
    );
    let (request_id, connection_generation) = stop
        .iter()
        .find_map(|command| match command {
            Command::StopSupervisor {
                request_id,
                connection_generation,
            } => Some((*request_id, *connection_generation)),
            _ => None,
        })
        .expect("confirmed stop should produce a typed command");

    let disconnect = update(
        &mut state,
        UiEvent::Disconnected {
            connection_generation,
        },
    );
    assert!(state.should_quit);
    assert!(
        disconnect
            .iter()
            .all(|command| !matches!(command, Command::ScheduleReconnect { .. }))
    );

    update(
        &mut state,
        UiEvent::CommandResult {
            request_id,
            connection_generation,
            result: Ok(MutationSuccess {
                message: "Supervisor stopped".to_owned(),
            }),
        },
    );
    assert!(state.should_quit);
}

#[test]
fn runtime_generation_change_supersedes_an_in_flight_rule_load() {
    let mut state = connected_state();
    state.rules = RulesState::default();
    let first_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::SwitchPage(Page::Rules)),
    );
    let (first_request, connection_generation) = rule_request_identity(&first_commands);

    let mut status = state.status.clone().expect("fixture status should exist");
    status.runtime_generation = Some(RuntimeGeneration(2));
    let replacement_commands = update(
        &mut state,
        UiEvent::StatusSnapshot {
            connection_generation,
            status,
        },
    );
    assert!(matches!(
        replacement_commands.as_slice(),
        [Command::Cancel { request_id }, Command::FetchRules { .. }]
            if *request_id == first_request
    ));
    let replacement_request = rule_request_identity(&replacement_commands[1..]).0;

    update(
        &mut state,
        UiEvent::RulesLoaded {
            request_id: first_request,
            connection_generation,
            result: Ok(rule_list_snapshot("DOMAIN,stale.example,DIRECT", 2)),
        },
    );

    assert!(state.rules.rows.is_empty());
    assert_eq!(
        state
            .rules
            .load_pending
            .as_ref()
            .map(|pending| pending.request_id),
        Some(replacement_request)
    );
}

#[test]
fn profile_activation_requeues_a_cancelled_rule_load() {
    let mut state = connected_state();
    state.rules = RulesState::default();
    let initial_rule_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::SwitchPage(Page::Rules)),
    );
    let (initial_rule_request, connection_generation) =
        rule_request_identity(&initial_rule_commands);
    update(&mut state, UiEvent::Intent(UiIntent::OpenProfiles));
    let mutation_commands = update(&mut state, UiEvent::Intent(UiIntent::ActivateSelected));
    assert!(matches!(
        mutation_commands.first(),
        Some(Command::Cancel { request_id }) if *request_id == initial_rule_request
    ));
    let mutation_request = mutation_commands
        .iter()
        .find_map(|command| match command {
            Command::ActivateProfile { request_id, .. } => Some(*request_id),
            _ => None,
        })
        .expect("Profile activation command should exist");

    let reload = update(
        &mut state,
        UiEvent::CommandResult {
            request_id: mutation_request,
            connection_generation,
            result: Ok(MutationSuccess {
                message: "Profile activated".to_owned(),
            }),
        },
    );

    assert!(matches!(reload.as_slice(), [Command::FetchRules { .. }]));
    assert!(state.modal.is_none());
}

#[test]
fn header_health_reflects_a_stopped_managed_core() {
    let mut state = connected_state();
    state
        .status
        .as_mut()
        .expect("fixture status should exist")
        .core
        .lifecycle = CoreLifecycle::Stopped;

    let (text, _) = render_with_backend(&state, 160, 30);
    let header = text.lines().next().expect("header row should exist");

    assert!(header.contains("STOPPED"));
}

#[test]
fn shared_header_uses_one_healthy_dot_and_an_active_navigation_segment() {
    let area = Rect::new(0, 0, 100, 30);
    let state = connected_state();
    let mut buffer = Buffer::empty(area);

    render_buffer(&state, area, &mut buffer);

    let healthy_header_cells = (area.x..area.right())
        .filter(|column| buffer[(*column, area.y)].fg == Color::Green)
        .count();
    assert_eq!(healthy_header_cells, 1);
    let separator = (area.x..area.right())
        .map(|column| buffer[(column, area.y + 3)].symbol())
        .collect::<String>();
    assert!(separator.contains('━'));
}

#[test]
fn minimum_width_header_keeps_status_profile_mode_tun_and_live_metrics() {
    let mut state = connected_state();
    let status = state.status.as_mut().expect("fixture status should exist");
    status
        .active_profile
        .as_mut()
        .expect("fixture Profile should exist")
        .name = "A very long production Profile name".to_owned();
    status.traffic.download_bytes_per_second = 18 * 1024 * 1024;
    status.traffic.upload_bytes_per_second = 2 * 1024 * 1024;
    status.connection_count = 428;

    let (text, _) = render_with_backend(&state, 80, 24);
    let header = text.lines().take(3).collect::<Vec<_>>().join("\n");

    for expected in [
        "● UP",
        "Profile:",
        "Node:",
        "Traffic:",
        "↓18.0M",
        "↑2.0M",
        "Connections: 428",
    ] {
        assert!(
            header.contains(expected),
            "compact header should retain {expected}: {header}"
        );
    }
}

#[test]
fn standard_header_truncates_long_profile_before_live_metrics() {
    let mut state = connected_state();
    let status = state.status.as_mut().expect("fixture status should exist");
    status
        .active_profile
        .as_mut()
        .expect("fixture Profile should exist")
        .name = "P".repeat(80);
    status.traffic.download_bytes_per_second = 18 * 1024 * 1024;
    status.traffic.upload_bytes_per_second = 2 * 1024 * 1024;
    status.connection_count = 428;

    let (text, _) = render_with_backend(&state, 160, 30);
    let header = text.lines().take(3).collect::<Vec<_>>().join("\n");

    for expected in [
        "CONNECTED",
        "Mode: RULE",
        "TUN: ON",
        "↓ 18.0 MiB/s",
        "↑ 2.0 MiB/s",
        "Connections: 428",
    ] {
        assert!(
            header.contains(expected),
            "standard header should retain {expected}: {header}"
        );
    }
    assert!(!header.contains(&"P".repeat(80)));
}

#[test]
fn footer_hints_follow_the_active_focus_and_hide_inactive_globals() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.focus = Focus::ProxyGroups;
    let (groups, _) = render_with_backend(&state, 80, 24);
    let groups_footer = groups.lines().last().expect("footer row should exist");
    for expected in ["Enter Open", "l Nodes", "z Zoom", "[?] Help", "[q] Quit"] {
        assert!(
            groups_footer.contains(expected),
            "missing {expected}: {groups_footer}"
        );
    }
    assert!(!groups_footer.contains("d Details"));

    state.focus = Focus::Content;
    let (nodes, _) = render_with_backend(&state, 80, 24);
    let nodes_footer = nodes.lines().last().expect("footer row should exist");
    for expected in ["Enter Select", "p Profiles", "[?] Help", "[q] Quit"] {
        assert!(
            nodes_footer.contains(expected),
            "missing {expected}: {nodes_footer}"
        );
    }
    assert!(!nodes_footer.contains("z Zoom"));

    state.focus = Focus::Search;
    let (search, search_map) = render_with_backend(&state, 80, 24);
    let search_footer = search.lines().last().expect("footer row should exist");
    assert!(search_footer.contains("Type Filter"));
    assert!(!search_footer.contains("[?] Help"));
    assert!(!search_footer.contains("[q] Quit"));
    assert!(search_map.interactions.iter().all(|interaction| {
        !matches!(interaction.intent, UiIntent::ToggleHelp | UiIntent::Quit)
    }));
}

#[test]
fn footer_hints_reflect_rule_availability_and_log_pause_state() {
    let mut state = connected_state();
    state.page = Page::Rules;
    state.rules.loaded_runtime_generation = None;
    state.rules.load_pending = Some(ratash::tui::PendingRuleLoad {
        request_id: RequestId(99),
        connection_generation: 1,
        runtime_generation: Some(RuntimeGeneration(1)),
    });
    let (loading, _) = render_with_backend(&state, 100, 30);
    let loading_footer = loading.lines().last().expect("footer row should exist");
    assert!(loading_footer.contains("Loading Rules…"));
    assert!(!loading_footer.contains("Enter Edit"));

    state.rules.load_pending = None;
    let (unavailable, _) = render_with_backend(&state, 100, 30);
    let unavailable_footer = unavailable.lines().last().expect("footer row should exist");
    assert!(unavailable_footer.contains("Rules unavailable"));
    assert!(!unavailable_footer.contains("a Add"));

    state.page = Page::Logs;
    state.logs.paused = true;
    let (paused, _) = render_with_backend(&state, 100, 30);
    let paused_footer = paused.lines().last().expect("footer row should exist");
    assert!(paused_footer.contains("p Resume"));
    assert!(!paused_footer.contains("p Pause"));
}

#[test]
fn healthy_header_dot_is_the_only_green_cell_on_standard_pages() {
    let area = Rect::new(0, 0, 180, 30);
    let mut state = connected_state();

    for page in [Page::Proxies, Page::Connections, Page::Rules, Page::Logs] {
        state.page = page;
        let mut buffer = Buffer::empty(area);
        render_buffer(&state, area, &mut buffer);

        let green_cells = (area.y..area.bottom())
            .flat_map(|row| (area.x..area.right()).map(move |column| (column, row)))
            .filter(|position| buffer[*position].fg == Color::Green)
            .count();
        assert_eq!(green_cells, 1, "unexpected green styling on {page:?}");
    }
}

#[test]
fn proxy_search_and_current_node_use_the_cyan_gray_selection_palette() {
    let area = Rect::new(0, 0, 130, 30);
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.focus = Focus::Search;
    state.proxies.filter = "Tokyo".to_owned();
    let (regions, _) = ratash::tui::compute_layout(&state, area, 1);
    let mut search_buffer = Buffer::empty(area);
    render_buffer(&state, area, &mut search_buffer);
    let search = regions.search.expect("Proxy search row should render");
    let slash = (search.x..search.right())
        .find(|column| search_buffer[(*column, search.y)].symbol() == "/")
        .expect("focused Proxy query should render a slash");
    assert_eq!(search_buffer[(slash, search.y)].fg, Color::Cyan);

    state.focus = Focus::Content;
    state.proxies.filter.clear();
    state.proxies.selected = 1;
    let (regions, _) = ratash::tui::compute_layout(&state, area, 2);
    let mut node_buffer = Buffer::empty(area);
    render_buffer(&state, area, &mut node_buffer);
    let list = regions.list.expect("Proxy node list should render");
    let current_row = (list.y..list.bottom())
        .find(|row| {
            (list.x..list.right())
                .map(|column| node_buffer[(column, *row)].symbol())
                .collect::<String>()
                .contains("Tokyo")
        })
        .expect("current Node should render");
    assert!((list.x..list.right()).all(|column| {
        !matches!(
            node_buffer[(column, current_row)].fg,
            Color::Green | Color::Yellow
        )
    }));
    assert!((list.x..list.right()).any(|column| {
        node_buffer[(column, current_row)].symbol() == "T"
            && node_buffer[(column, current_row)].fg == Color::Cyan
    }));
}

#[test]
fn selected_log_renders_a_sanitized_detail_and_full_row_muted_cyan_highlight() {
    let area = Rect::new(0, 0, 100, 30);
    let mut state = connected_state();
    state.page = Page::Logs;
    state.logs.level_filter = LogLevelFilter::All;
    state.logs.records = VecDeque::from([
        log(1, LogLevel::Info, "first"),
        log(2, LogLevel::Warn, "selected\u{1b}message"),
        log(3, LogLevel::Error, "latest"),
    ]);
    update(
        &mut state,
        UiEvent::Intent(UiIntent::SelectLog { tail_offset: 1 }),
    );
    let (regions, _) = ratash::tui::compute_layout(&state, area, 1);
    let list = regions.list.expect("selected Logs should keep a list");
    let detail = regions
        .detail
        .expect("selected Logs should expose compact detail when height allows");
    let mut buffer = Buffer::empty(area);
    render_buffer(&state, area, &mut buffer);
    let text = buffer_text(&buffer);

    assert!(text.contains("SELECTED LOG · SOURCE core · SEQUENCE 2"));
    assert!(text.contains("MESSAGE  selected?message"));
    assert!(!text.contains('\u{1b}'));
    let selected_row = (list.y..list.bottom())
        .find(|row| {
            (list.x..list.right())
                .map(|column| buffer[(column, *row)].symbol())
                .collect::<String>()
                .contains("selected?message")
        })
        .expect("selected Log row should render");
    assert!(selected_row < detail.y);
    assert_eq!(buffer[(list.x, selected_row)].symbol(), "▌");
    assert!(
        (list.x..list.right())
            .all(|column| buffer[(column, selected_row)].bg == Color::Rgb(0, 72, 78))
    );
}

#[test]
fn degraded_header_keeps_primary_metrics_and_shows_the_recovery_reason() {
    let mut state = connected_state();
    let status = state.status.as_mut().expect("fixture status should exist");
    status.supervisor.lifecycle = SupervisorLifecycle::Degraded;
    status.supervisor.health_reasons = vec![SupervisorHealthReason::RuntimeRecovery];

    let (text, _) = render_with_backend(&state, 120, 30);
    let header = text.lines().take(3).collect::<Vec<_>>().join("\n");

    assert!(header.contains("Recovery: runtime_recovery"));
    assert!(header.contains('↓'));
    assert!(header.contains("Connections: 3"));
}

#[test]
fn compact_pages_keep_controls_and_visualizations_within_their_contract() {
    let mut state = connected_state();
    let (page, _) = render_with_backend(&state, 100, 30);
    assert!(!page.contains("Apply:"));
    assert!(!page.contains("Recovery:"));
    assert!(!page.contains("Diagnostic:"));

    state.page = Page::Logs;
    state.logs.search = "openai".to_owned();
    let (logs, _) = render_with_backend(&state, 100, 30);
    let controls = logs
        .lines()
        .find(|line| line.contains("LOGS"))
        .expect("Logs controls should render");
    assert!(controls.contains("/openai"));

    state.page = Page::Connections;
    let (connections, _) = render_with_backend(&state, 120, 30);
    assert!(!connections.contains("THROUGHPUT"));
    assert!(!connections.contains("LATEST 60 SAMPLES"));
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
fn proxy_latency_omits_probe_state_when_no_sample_exists() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.proxies.rows = ["Node A", "Node B", "Node C", "Node D"]
        .into_iter()
        .map(|name| ProxyRow {
            group_id: ProxyGroupId::for_name("Automatic"),
            group: "Automatic".to_owned(),
            node_id: Some(NodeRecordId::for_core(name)),
            name: name.to_owned(),
            node_type: "Shadowsocks".to_owned(),
            available: name != "Node D",
            selected: false,
            delay_ms: None,
        })
        .collect();

    let (text, _) = render_with_backend(&state, 100, 30);

    assert!(
        ["Node A", "Node B", "Node C", "Node D"]
            .iter()
            .all(|name| text.contains(name))
    );
    assert!(!text.contains("queued"));
    assert!(!text.contains("probing"));
    assert!(!text.contains("failed"));
    assert!(!text.contains("timeout"));
}

#[test]
fn profiles_sheet_omits_refresh_timestamps_and_freshness() {
    let mut state = connected_state();
    state.profiles.rows[0].fresh = false;
    state.profiles.rows[0].next_refresh_at_unix_ms = 9_876_543_210;
    state.profiles.rows[0].error = Some("refresh unavailable".to_owned());
    update(&mut state, UiEvent::Intent(UiIntent::OpenProfiles));

    let (text, _) = render_with_backend(&state, 100, 30);

    assert!(text.contains("STATE"));
    assert!(text.contains("PROFILE"));
    assert!(text.contains("ERROR"));
    assert!(text.contains("refresh unavailable"));
    assert!(!text.contains("FRESH"));
    assert!(!text.contains("NEXT REFRESH"));
    assert!(!text.contains("9876543210"));
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
    state.rules.rows[0].rule_string = "Rule\u{1b}X".to_owned();
    state.rules.rows[0].payload = Some("Rule\u{1b}X".to_owned());
    state.logs.records[0].message = "Log\u{1b}X".to_owned();
    state.toast = Some("Toast\u{1b}X".to_owned());

    for (page, expected) in [
        (Page::Proxies, "Node?X"),
        (Page::Connections, "Profile?X"),
        (Page::Rules, "Rule?X"),
        (Page::Logs, "Log?X"),
    ] {
        state.page = page;
        let (text, _) = render_with_backend(&state, 100, 30);
        assert!(text.contains(expected), "{page:?} should sanitize its text");
        assert!(text.contains("Toast?X"));
        assert!(!text.contains('\u{1b}'));
    }

    update(&mut state, UiEvent::Intent(UiIntent::OpenProfiles));
    let (text, _) = render_with_backend(&state, 100, 30);
    assert!(text.contains("Stored?X"));
    assert!(!text.contains('\u{1b}'));
}

#[test]
fn compact_rules_keep_type_value_and_target_visible_with_wide_text() {
    let mut state = connected_state();
    state.page = Page::Rules;
    state.rules.rows[0].rule_type = "DOMAIN-SUFFIX".to_owned();
    state.rules.rows[0].payload = Some("服务节点服务节点服务节点服务节点服务节点".to_owned());
    state.rules.rows[0].policy_target = "DIRECT".to_owned();

    let (text, _) = render_with_backend(&state, 80, 24);

    assert!(text.contains("DOMAIN-SUFFIX"));
    assert!(text.contains('服'));
    assert!(text.contains('务'));
    assert!(text.contains("DIRECT"));
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
    update(&mut keyboard_state, UiEvent::Intent(UiIntent::OpenProfiles));
    let (_, map) = render_with_backend(&keyboard_state, 100, 30);
    let profile_hit = hit_for(&map, |intent| {
        matches!(intent, UiIntent::ActivateProfile(_))
    });
    keyboard_state.publish_interaction_map(map);
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
    state.publish_interaction_map(map);

    assert_eq!(
        input_to_intent(&state, TerminalInput::Key(KeyInput::Right)),
        Some(UiIntent::FocusRight)
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
fn proxy_group_last_page_mouse_hits_match_the_rendered_rows() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.focus = Focus::ProxyGroups;
    state.proxies.groups = (0..20)
        .map(|index| ProxyGroupRow {
            id: ProxyGroupId::for_name(&format!("Group {index:02}")),
            name: format!("Group {index:02}"),
            proxy_type: "Selector".to_owned(),
            selectable: true,
            selected_node: None,
        })
        .collect();
    state.proxies.group_cursor = 19;

    let (text, map) = render_with_backend(&state, 140, 24);
    let first_visible = ProxyGroupId::for_name("Group 05");
    let hit = hit_for(&map, |intent| {
        *intent == UiIntent::ShowProxyGroup(first_visible.clone())
    });
    state.publish_interaction_map(map);

    assert!(text.contains("Group 05"));
    assert_eq!(
        input_to_intent(
            &state,
            TerminalInput::Mouse(MouseInput {
                kind: MouseInputKind::LeftClick,
                column: hit.0,
                row: hit.1,
            })
        ),
        Some(UiIntent::ShowProxyGroup(first_visible))
    );
}

#[test]
fn keyboard_and_mouse_sort_controls_share_the_same_intent() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    let (_, map) = render_with_backend(&state, 100, 30);
    let name_sort = hit_for(&map, |intent| {
        *intent == UiIntent::SetProxySort(ratash::tui::ProxySort::Name)
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
        UiIntent::SwitchPage(Page::Proxies),
        UiIntent::SwitchPage(Page::Connections),
        UiIntent::SwitchPage(Page::Rules),
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
fn every_search_field_enforces_character_and_utf8_byte_limits() {
    for page in [Page::Proxies, Page::Rules, Page::Logs] {
        let mut ascii = connected_state();
        ascii.page = page;
        ascii.focus = Focus::Search;
        for _ in 0..TUI_SEARCH_MAX_CHARACTERS + 1 {
            update(&mut ascii, UiEvent::Intent(UiIntent::InputCharacter('a')));
        }
        let ascii_search = search_for_page(&ascii, page);
        assert_eq!(ascii_search.chars().count(), TUI_SEARCH_MAX_CHARACTERS);
        assert!(ascii_search.len() <= TUI_SEARCH_MAX_BYTES);

        let mut utf8 = connected_state();
        utf8.page = page;
        utf8.focus = Focus::Search;
        for _ in 0..TUI_SEARCH_MAX_CHARACTERS + 1 {
            update(
                &mut utf8,
                UiEvent::Intent(UiIntent::InputCharacter('\u{754c}')),
            );
        }
        let utf8_search = search_for_page(&utf8, page);
        assert!(utf8_search.chars().count() <= TUI_SEARCH_MAX_CHARACTERS);
        assert!(utf8_search.len() <= TUI_SEARCH_MAX_BYTES);
        assert!(utf8_search.len().saturating_add('\u{754c}'.len_utf8()) > TUI_SEARCH_MAX_BYTES);
    }

    let mut profiles = connected_state();
    update(&mut profiles, UiEvent::Intent(UiIntent::OpenProfiles));
    profiles.focus = Focus::Search;
    for _ in 0..TUI_SEARCH_MAX_CHARACTERS + 1 {
        update(
            &mut profiles,
            UiEvent::Intent(UiIntent::InputCharacter('a')),
        );
    }
    assert_eq!(
        profiles.profiles.filter.chars().count(),
        TUI_SEARCH_MAX_CHARACTERS
    );
    assert!(profiles.profiles.filter.len() <= TUI_SEARCH_MAX_BYTES);
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
    assert_eq!(state.focus, Focus::ProxyGroups);
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
        (Page::Proxies, Focus::ProxyGroups),
        (Page::Connections, Focus::Content),
        (Page::Rules, Focus::Content),
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
fn compact_current_proxy_group_activation_focuses_the_node_list_without_reload() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.focus = Focus::ProxyGroups;
    state.terminal_width = 89;

    let commands = update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Enter)),
    );

    assert!(commands.is_empty());
    assert_eq!(state.focus, Focus::Content);
}

#[test]
fn proxy_group_browsing_remains_independent_from_mutations() {
    let mut state = connected_state();
    state.proxies.groups.push(manual_proxy_group());
    let profile_id = state.profiles.rows[1].id;
    let mutation_commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::ActivateProfile(profile_id)),
    );
    let (mutation_request_id, generation) = activation_identity(&mutation_commands);

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
            }),
        },
    );
    update(
        &mut state,
        UiEvent::ProxyGroupLoaded {
            request_id: group_request_id,
            connection_generation: group_generation,
            result: Ok(manual_proxy_snapshot()),
        },
    );

    assert_eq!(state.proxies.selected_group.as_deref(), Some("Manual"));
    assert_eq!(
        state.toast.as_deref(),
        Some("Success: Loaded Proxy Group Manual")
    );
}

#[test]
fn proxy_group_load_and_mutation_states_are_visually_distinct() {
    let mut profile_state = connected_state();
    update(&mut profile_state, UiEvent::Intent(UiIntent::OpenProfiles));
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
    assert!(pending_text.contains("● Automatic"));
    assert!(pending_text.contains('◌'));

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
    assert!(success_text.contains("● Manual"));
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
            }),
        },
    );
    assert!(state.pending.is_none());
    assert_eq!(state.toast.as_deref(), Some("Success: Profile activated"));
    assert!(state.connection.snapshot_stale);
    assert_eq!(
        state
            .status
            .as_ref()
            .and_then(|status| status.active_profile.as_ref())
            .map(|profile| profile.name.as_str()),
        Some("Work")
    );
    assert_eq!(
        state
            .status
            .as_ref()
            .expect("committed mutation should retain the current status")
            .traffic
            .upload_bytes_per_second,
        100
    );
    let base_view_revision = state.view_revision();
    let base_status_revision = state.status_revision();
    update(
        &mut state,
        UiEvent::SnapshotRefreshFailed {
            connection_generation: generation,
            base_view_revision,
        },
    );
    assert_eq!(state.toast.as_deref(), Some("Success: Profile activated"));
    assert!(state.connection.snapshot_stale);

    update(
        &mut state,
        UiEvent::SnapshotRefreshed {
            connection_generation: generation,
            base_view_revision,
            base_status_revision,
            snapshot: refreshed_snapshot,
        },
    );
    assert!(!state.connection.snapshot_stale);
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
    assert_eq!(state.logs.dropped_total, 0);
}

#[test]
fn snapshot_refresh_applies_collections_without_rolling_back_newer_live_status() {
    let mut state = connected_state();
    let base_view_revision = state.view_revision();
    let base_status_revision = state.status_revision();
    let mut refreshed = snapshot_from_state(&state);
    refreshed
        .profiles
        .push(profile("Added during refresh", false));
    refreshed.status.traffic.upload_bytes_per_second = 50;

    let mut live = state.status.clone().expect("connected fixture has status");
    live.traffic.upload_bytes_per_second = 900;
    update(
        &mut state,
        UiEvent::StatusSnapshot {
            connection_generation: 1,
            status: live,
        },
    );
    assert_eq!(state.view_revision(), base_view_revision);

    update(
        &mut state,
        UiEvent::SnapshotRefreshed {
            connection_generation: 1,
            base_view_revision,
            base_status_revision,
            snapshot: refreshed,
        },
    );

    assert_eq!(state.profiles.rows.len(), 3);
    assert_eq!(
        state
            .status
            .as_ref()
            .expect("live status should remain present")
            .traffic
            .upload_bytes_per_second,
        900
    );
}

#[test]
fn collection_revision_discards_a_refresh_started_before_a_relevant_status_change() {
    let mut state = connected_state();
    let base_view_revision = state.view_revision();
    let base_status_revision = state.status_revision();
    let mut stale = snapshot_from_state(&state);
    stale.profiles.push(profile("Stale", false));

    let mut live = state.status.clone().expect("connected fixture has status");
    live.runtime_generation = Some(RuntimeGeneration(2));
    update(
        &mut state,
        UiEvent::StatusSnapshot {
            connection_generation: 1,
            status: live,
        },
    );
    update(
        &mut state,
        UiEvent::SnapshotRefreshed {
            connection_generation: 1,
            base_view_revision,
            base_status_revision,
            snapshot: stale,
        },
    );

    assert_eq!(state.profiles.rows.len(), 2);
    assert_eq!(
        state
            .status
            .as_ref()
            .and_then(|status| status.runtime_generation),
        Some(RuntimeGeneration(2))
    );
}

#[test]
fn disconnect_retains_a_stale_snapshot_and_reconnect_replaces_all_view_data() {
    let mut state = connected_state();
    state.logs.dropped_total = 9;
    state.logs.evicted_total = 2;
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
    assert_eq!(state.logs.dropped_total, 9);
    assert_eq!(state.logs.evicted_total, 2);
}

#[test]
fn view_caches_remain_bounded_at_release_scale() {
    let proxies = (0..=MAX_ACTIVE_NODES)
        .map(|index| proxy(&format!("node-{index}"), false))
        .collect::<Vec<_>>();
    let profiles = (0..=ratash::tui::PROFILE_VIEW_CAPACITY)
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
        ratash::tui::PROFILE_VIEW_CAPACITY
    );
    assert_eq!(state.logs.records.len(), LOG_CAPACITY);

    let commands = update(
        &mut state,
        UiEvent::Intent(UiIntent::SwitchPage(Page::Rules)),
    );
    let (request_id, connection_generation) = rule_request_identity(&commands);
    update(
        &mut state,
        UiEvent::RulesLoaded {
            request_id,
            connection_generation,
            result: Ok(RuleListSnapshot {
                initialized: true,
                revision: Some(LocalRuleSetRevision(1)),
                rows: (0..=LOCAL_RULE_COUNT_MAX)
                    .map(|index| RuleRow {
                        index,
                        rule_string: format!("DOMAIN,node-{index}.example,DIRECT"),
                        rule_type: "DOMAIN".to_owned(),
                        payload: Some(format!("node-{index}.example")),
                        policy_target: "DIRECT".to_owned(),
                        policy_target_validation: PolicyTargetValidation::Valid,
                    })
                    .collect(),
            }),
        },
    );
    assert_eq!(state.rules.rows.len(), LOCAL_RULE_COUNT_MAX);
}

#[test]
fn log_view_evicts_by_aggregate_message_bytes() {
    let retained_records = LOG_RETENTION_MAX_BYTES / CORE_LOG_LINE_MAX_BYTES;
    let message = "x".repeat(CORE_LOG_LINE_MAX_BYTES);
    let mut state = connected_state();

    update(
        &mut state,
        UiEvent::LogBatch {
            connection_generation: 1,
            records: (0..=retained_records)
                .map(|index| log(index as u64, LogLevel::Info, &message))
                .collect(),
            gap: false,
            dropped_total: 0,
        },
    );

    assert_eq!(state.logs.records.len(), retained_records);
    assert_eq!(state.logs.retained_bytes, LOG_RETENTION_MAX_BYTES);
    assert_eq!(state.logs.evicted_total, 2);
}

#[test]
fn log_drop_counter_remains_monotonic_across_recovery_batches() {
    let mut state = connected_state();

    update(
        &mut state,
        UiEvent::LogBatch {
            connection_generation: 1,
            records: Vec::new(),
            gap: true,
            dropped_total: 9,
        },
    );
    update(
        &mut state,
        UiEvent::LogBatch {
            connection_generation: 1,
            records: Vec::new(),
            gap: false,
            dropped_total: 3,
        },
    );

    assert_eq!(state.logs.dropped_total, 9);
}

#[test]
fn keyboard_log_selection_pins_a_record_until_escape_resumes_follow() {
    let mut state = connected_state();
    state.page = Page::Logs;
    state.focus = Focus::Content;
    state.logs.records = (1..=5)
        .map(|sequence| log(sequence, LogLevel::Info, &format!("message {sequence}")))
        .collect();

    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('k'))),
    );
    assert!(!state.logs.follow);
    assert_eq!(state.logs.scroll, 1);
    let (selected, _) = render_with_backend(&state, 100, 30);
    assert!(selected.contains("SEQUENCE 4"));

    update(
        &mut state,
        UiEvent::LogBatch {
            connection_generation: 1,
            records: vec![log(6, LogLevel::Info, "message 6")],
            gap: false,
            dropped_total: 0,
        },
    );
    assert_eq!(state.logs.scroll, 2);
    let (pinned, _) = render_with_backend(&state, 100, 30);
    assert!(pinned.contains("SEQUENCE 4"));

    update(
        &mut state,
        UiEvent::Terminal(TerminalInput::Key(KeyInput::Escape)),
    );
    assert!(state.logs.follow);
    assert_eq!(state.logs.scroll, 0);
    assert!(
        ratash::tui::compute_layout(&state, Rect::new(0, 0, 100, 30), 1)
            .0
            .detail
            .is_none()
    );
}

#[test]
fn keyboard_and_mouse_log_selection_pin_the_same_tail_offset() {
    let mut base = connected_state();
    base.page = Page::Logs;
    base.focus = Focus::Content;
    base.logs.records = (1..=8)
        .map(|sequence| log(sequence, LogLevel::Info, &format!("message {sequence}")))
        .collect();

    let mut keyboard = base.clone();
    for _ in 0..2 {
        update(
            &mut keyboard,
            UiEvent::Terminal(TerminalInput::Key(KeyInput::Character('k'))),
        );
    }

    let mut mouse = base;
    let (_, map) = render_with_backend(&mouse, 100, 30);
    let hit = hit_for(&map, |intent| {
        *intent == UiIntent::SelectLog { tail_offset: 2 }
    });
    mouse.publish_interaction_map(map);
    update(
        &mut mouse,
        UiEvent::Terminal(TerminalInput::Mouse(MouseInput {
            kind: MouseInputKind::LeftClick,
            column: hit.0,
            row: hit.1,
        })),
    );

    assert_eq!(mouse.logs.scroll, keyboard.logs.scroll);
    assert_eq!(mouse.logs.follow, keyboard.logs.follow);
}

#[test]
fn direct_log_selection_requires_the_unobscured_logs_page_and_a_valid_row() {
    let mut wrong_page = connected_state();
    update(
        &mut wrong_page,
        UiEvent::Intent(UiIntent::SelectLog { tail_offset: 0 }),
    );
    assert!(wrong_page.logs.follow);

    let mut obscured = connected_state();
    obscured.page = Page::Logs;
    obscured.modal = Some(Modal::Help);
    update(
        &mut obscured,
        UiEvent::Intent(UiIntent::SelectLog { tail_offset: 0 }),
    );
    assert!(obscured.logs.follow);

    let mut invalid = connected_state();
    invalid.page = Page::Logs;
    update(
        &mut invalid,
        UiEvent::Intent(UiIntent::SelectLog { tail_offset: 1 }),
    );
    assert!(invalid.logs.follow);
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
fn proxy_rows_keep_operational_fields_and_remove_sampling_metadata() {
    let mut state = connected_state();
    state.page = Page::Proxies;
    let (proxies, _) = render_with_backend(&state, 180, 30);
    for expected in ["NODE", "TYPE", "STATUS", "LATENCY", "Tokyo", "42 ms"] {
        assert!(proxies.contains(expected), "missing {expected}: {proxies}");
    }
    for removed in ["SAMPLED", "FRESHNESS", "PROBE", "1000", "succeeded"] {
        assert!(
            !proxies.contains(removed),
            "unexpected {removed}: {proxies}"
        );
    }
}

#[test]
fn proxy_node_names_reserve_a_guard_cell_after_leading_emoji() {
    let area = Rect::new(0, 0, 180, 30);
    let mut state = connected_state();
    state.page = Page::Proxies;
    state.proxies.rows = vec![proxy("🇯🇵日本高速01|BGP|CUCM", true)];
    state
        .status
        .as_mut()
        .and_then(|status| status.selected_node.as_mut())
        .expect("selected Node should exist")
        .name = "🇯🇵日本高速01|BGP|CUCM".to_owned();
    let (regions, _) = ratash::tui::compute_layout(&state, area, 1);
    let list = regions.list.expect("Proxy node list should render");
    let mut buffer = Buffer::empty(area);

    render_buffer(&state, area, &mut buffer);

    let flags = (area.y..area.bottom())
        .flat_map(|row| (area.x..area.right()).map(move |column| (column, row)))
        .filter(|position| buffer[*position].symbol() == "🇯🇵")
        .collect::<Vec<_>>();
    assert_eq!(
        flags.len(),
        2,
        "header and Proxy row should render the flag"
    );
    assert!(
        flags
            .iter()
            .any(|(_, row)| *row >= list.y && *row < list.bottom())
    );
    for (flag_column, row) in flags {
        assert_eq!(buffer[(flag_column + 1, row)].symbol(), " ");
        assert_eq!(buffer[(flag_column + 2, row)].symbol(), " ");
        assert_eq!(buffer[(flag_column + 3, row)].symbol(), "日");
    }
}

#[test]
fn modal_rendering_stacks_over_the_page_and_exposes_only_close_interaction() {
    let mut state = connected_state();
    state.modal = Some(Modal::Help);
    state.focus = Focus::Modal;

    let (text, map) = render_with_backend(&state, 100, 30);

    assert!(text.contains("Keyboard and mouse help"));
    assert!(text.contains("[Esc] Close"));
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
    assert!(matches!(round[2], UiEvent::LogBatch { .. }));
    assert!(matches!(round[3], UiEvent::CommandResult { .. }));
    assert!(matches!(round[4], UiEvent::ReconnectDeadline { .. }));
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
    state.rules.initialized = true;
    state.rules.rows = vec![RuleRow {
        index: 0,
        rule_string: "DOMAIN-SUFFIX,example.com,Automatic".to_owned(),
        rule_type: "DOMAIN-SUFFIX".to_owned(),
        payload: Some("example.com".to_owned()),
        policy_target: "Automatic".to_owned(),
        policy_target_validation: PolicyTargetValidation::Valid,
    }];
    state.rules.loaded_connection_generation = Some(1);
    state.rules.loaded_runtime_generation = Some(RuntimeGeneration(1));
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
            instance_generation: Some(ratash::domain::CoreInstanceGeneration(1)),
            restart: ratash::domain::CoreRestartStatus::default(),
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
        latency: Some(ratash::domain::LatencySample {
            node_id,
            delay_ms: Some(42),
            sampled_at_unix_ms: Some(1_000),
            state: SampleState::Fresh,
            probe_generation: ratash::domain::ProbeGeneration(1),
        }),
        traffic: TrafficSample {
            upload_bytes_per_second: 100,
            download_bytes_per_second: 200,
            sampled_at_unix_ms: Some(1_000),
            state: SampleState::Fresh,
        },
        connection_count: 3,
        upload_total_bytes: 4 * 1024,
        download_total_bytes: 8 * 1024,
        memory_bytes: Some(1024 * 1024),
        connections: vec![ConnectionRecord {
            id: "fixture-connection".to_owned(),
            network: "tcp".to_owned(),
            host: Some("example.com".to_owned()),
            destination_ip: Some("93.184.216.34".to_owned()),
            destination_port: Some("443".to_owned()),
            chains: vec!["provider-only".to_owned(), "Manual".to_owned()],
            rule: "DOMAIN-SUFFIX".to_owned(),
            rule_payload: Some("example.com".to_owned()),
            upload_bytes: 1024,
            download_bytes: 2048,
        }],
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
    }
}

fn proxy_group() -> ProxyGroupRow {
    ProxyGroupRow {
        id: ProxyGroupId::for_name("Automatic"),
        name: "Automatic".to_owned(),
        proxy_type: "Selector".to_owned(),
        selectable: true,
        selected_node: Some("Tokyo".to_owned()),
    }
}

fn manual_proxy_group() -> ProxyGroupRow {
    ProxyGroupRow {
        id: ProxyGroupId::for_name("Manual"),
        name: "Manual".to_owned(),
        proxy_type: "Selector".to_owned(),
        selectable: true,
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

fn rule_request_identity(commands: &[Command]) -> (RequestId, u64) {
    commands
        .iter()
        .find_map(|command| match command {
            Command::FetchRules {
                request_id,
                connection_generation,
            } => Some((*request_id, *connection_generation)),
            _ => None,
        })
        .expect("Rule load command should exist")
}

fn rule_list_snapshot(rule_string: &str, revision: u64) -> RuleListSnapshot {
    let fields = rule_string.split(',').collect::<Vec<_>>();
    RuleListSnapshot {
        initialized: true,
        revision: Some(LocalRuleSetRevision(revision)),
        rows: vec![RuleRow {
            index: 0,
            rule_string: rule_string.to_owned(),
            rule_type: fields.first().copied().unwrap_or("MATCH").to_owned(),
            payload: fields.get(1).copied().map(str::to_owned),
            policy_target: fields.last().copied().unwrap_or("DIRECT").to_owned(),
            policy_target_validation: PolicyTargetValidation::Valid,
        }],
    }
}

fn search_for_page(state: &AppState, page: Page) -> &str {
    match page {
        Page::Proxies => &state.proxies.filter,
        Page::Rules => &state.rules.filter,
        Page::Logs => &state.logs.search,
        Page::Connections => "",
    }
}

fn snapshot_from_state(state: &AppState) -> FullViewSnapshot {
    FullViewSnapshot {
        status: state.status.clone().expect("connected fixture has status"),
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

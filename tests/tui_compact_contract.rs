use ratash::tui::{
    AppState, Focus, KeyInput, Modal, Page, TerminalInput, UiEvent, UiIntent, compute_layout,
    input_to_intent, render_buffer, update,
};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;

#[test]
fn shared_frame_renders_the_four_page_information_architecture_at_minimum_size() {
    let area = Rect::new(0, 0, 80, 24);
    let mut state = AppState::new();

    for page in [Page::Proxies, Page::Connections, Page::Rules, Page::Logs] {
        state.page = page;
        let mut buffer = Buffer::empty(area);

        render_buffer(&state, area, &mut buffer);

        let text = buffer_text(&buffer);
        assert!(
            ["Proxies", "Connections", "Rules", "Logs"]
                .iter()
                .all(|title| text.lines().take(3).any(|line| line.contains(title))),
            "missing compact navigation at {page:?}:\n{text}"
        );
        assert!(
            ['┌', '┐', '└', '┘']
                .iter()
                .all(|corner| !text.contains(*corner)),
            "decorative card border rendered at {page:?}:\n{text}"
        );
    }
}

#[test]
fn proxy_layout_switches_at_the_compact_breakpoints() {
    let mut state = AppState::new();
    state.page = Page::Proxies;

    let (wide, _) = compute_layout(&state, Rect::new(0, 0, 130, 30), 1);
    assert!(wide.proxy_groups.is_some());
    assert!(wide.list.is_some());
    assert!(wide.detail.is_none());

    let (medium, _) = compute_layout(&state, Rect::new(0, 0, 129, 30), 1);
    assert!(medium.proxy_groups.is_some());
    assert!(medium.list.is_some());
    assert!(medium.detail.is_none());

    let (compact_nodes, _) = compute_layout(&state, Rect::new(0, 0, 89, 30), 1);
    assert!(compact_nodes.proxy_groups.is_none());
    assert!(compact_nodes.list.is_some());

    state.focus = Focus::ProxyGroups;
    let (compact_groups, _) = compute_layout(&state, Rect::new(0, 0, 89, 30), 1);
    assert!(compact_groups.proxy_groups.is_some());
    assert!(compact_groups.list.is_none());
}

#[test]
fn narrow_proxy_groups_use_one_title_row() {
    let area = Rect::new(0, 0, 80, 24);
    let mut state = AppState::new();
    state.page = Page::Proxies;
    state.focus = Focus::ProxyGroups;
    let mut buffer = Buffer::empty(area);

    render_buffer(&state, area, &mut buffer);
    let text = buffer_text(&buffer);

    assert_eq!(text.matches("PROXY GROUPS").count(), 1, "{text}");
}

#[test]
fn proxy_node_fields_follow_the_actual_pane_width() {
    let mut state = AppState::new();
    state.page = Page::Proxies;
    state.focus = Focus::Content;

    let medium_area = Rect::new(0, 0, 160, 30);
    let (medium_regions, _) = compute_layout(&state, medium_area, 1);
    let medium_list = medium_regions.list.expect("Node pane should render");
    let mut medium_buffer = Buffer::empty(medium_area);
    render_buffer(&state, medium_area, &mut medium_buffer);
    let medium_header = (medium_list.x..medium_list.right())
        .map(|column| medium_buffer[(column, medium_list.y)].symbol())
        .collect::<String>();
    assert!(medium_header.contains("STATUS"));
    assert!(medium_header.contains("LATENCY"));
    assert!(!medium_header.contains("FRESHNESS"));
    assert!(!medium_header.contains("PROBE"));

    let extended_area = Rect::new(0, 0, 180, 30);
    let (extended_regions, _) = compute_layout(&state, extended_area, 2);
    let extended_list = extended_regions.list.expect("wide Node pane should render");
    let mut extended_buffer = Buffer::empty(extended_area);
    render_buffer(&state, extended_area, &mut extended_buffer);
    let extended_header = (extended_list.x..extended_list.right())
        .map(|column| extended_buffer[(column, extended_list.y)].symbol())
        .collect::<String>();
    assert!(extended_header.contains("TYPE"));
    assert!(extended_header.contains("STATUS"));
    assert!(extended_header.contains("LATENCY"));
    assert!(!extended_header.contains("SAMPLED"));
    assert!(!extended_header.contains("FRESHNESS"));
    assert!(!extended_header.contains("PROBE"));
}

#[test]
fn proxy_focus_zoom_is_reversible() {
    let area = Rect::new(0, 0, 100, 30);
    let mut state = AppState::new();
    state.page = Page::Proxies;
    state.focus = Focus::Content;

    state.focus = Focus::ProxyGroups;
    update(&mut state, UiEvent::Intent(UiIntent::ToggleZoom));
    let (zoomed, _) = compute_layout(&state, area, 1);
    assert!(zoomed.proxy_groups.is_some());
    assert!(zoomed.list.is_none());
    assert!(zoomed.detail.is_none());

    update(&mut state, UiEvent::Intent(UiIntent::Escape));
    let (restored, _) = compute_layout(&state, area, 2);
    assert!(restored.proxy_groups.is_some());
    assert!(restored.list.is_some());
}

#[test]
fn zoom_shortcut_and_state_are_scoped_to_proxies() {
    for page in [Page::Rules, Page::Logs] {
        let mut state = AppState::new();
        state.page = page;

        assert_eq!(
            input_to_intent(&state, TerminalInput::Key(KeyInput::Character('z'))),
            None
        );
        update(&mut state, UiEvent::Intent(UiIntent::ToggleZoom));
        assert!(!state.zoomed_focus);
    }
}

#[test]
fn compact_logs_prioritize_gap_and_drop_signals() {
    let area = Rect::new(0, 0, 80, 24);
    let mut state = AppState::new();
    state.page = Page::Logs;
    state.logs.paused = true;
    state.logs.follow = false;
    state.logs.gap = true;
    state.logs.dropped_total = 7;
    state.logs.evicted_total = 2;
    let mut buffer = Buffer::empty(area);

    render_buffer(&state, area, &mut buffer);
    let text = buffer_text(&buffer);

    assert!(text.contains("gap d=7 e=2 paused manual"));
}

#[test]
fn medium_logs_use_compact_controls_and_prioritize_loss_status() {
    for width in [100, 149] {
        let area = Rect::new(0, 0, width, 24);
        let mut state = AppState::new();
        state.page = Page::Logs;
        state.logs.paused = true;
        state.logs.follow = false;
        state.logs.gap = true;
        state.logs.dropped_total = 7;
        state.logs.evicted_total = 2;
        let mut buffer = Buffer::empty(area);

        let map = render_buffer(&state, area, &mut buffer);
        let text = buffer_text(&buffer);
        let controls = text
            .lines()
            .find(|line| line.contains("LOGS"))
            .expect("Logs controls should render");
        assert!(controls.contains("gap · dropped=7 · evicted=2"));
        assert!(
            controls.find("gap").expect("gap should render")
                < controls.find("paused").expect("pause state should render")
        );
        for intent in [
            UiIntent::SetLogLevel(ratash::tui::LogLevelFilter::All),
            UiIntent::SetLogLevel(ratash::tui::LogLevelFilter::Error),
            UiIntent::ToggleLogPause,
            UiIntent::FollowLogs,
        ] {
            let area = map
                .interactions
                .iter()
                .find(|interaction| interaction.intent == intent)
                .map(|interaction| interaction.area)
                .expect("compact Log control should remain interactive");
            assert!(area.width <= 3, "wide hit area for {intent:?}: {area:?}");
        }
        let search = map
            .interactions
            .iter()
            .find(|interaction| interaction.intent == UiIntent::FocusSearch)
            .map(|interaction| interaction.area)
            .expect("compact Log search should remain interactive");
        assert_eq!(search.width, 18);
    }
}

#[test]
fn profile_sheet_preserves_the_borderless_frame() {
    let area = Rect::new(0, 0, 80, 24);
    let mut state = AppState::new();
    state.modal = Some(Modal::Profiles);
    let mut buffer = Buffer::empty(area);

    render_buffer(&state, area, &mut buffer);
    let text = buffer_text(&buffer);

    assert!(text.contains("COMMANDS · PROFILES"));
    assert!(
        ['┌', '┐', '└', '┘']
            .iter()
            .all(|corner| !text.contains(*corner))
    );
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

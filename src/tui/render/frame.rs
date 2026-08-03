use std::borrow::Cow;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

use crate::constants::{MINIMUM_TERMINAL_HEIGHT, MINIMUM_TERMINAL_WIDTH};
use crate::domain::{CoreLifecycle, RuntimeRecoveryStatus, SupervisorLifecycle};

use super::super::{
    AppState, CommandPaletteAction, ConnectionStatus, Focus, Modal, Page, filtered_palette_actions,
    filtered_profiles,
};
use super::layout::{
    LayoutRegions, command_palette_area, footer_controls_visible, footer_help_area,
    footer_quit_area, navigation_items, profile_sheet_regions, sheet_action_area, sheet_close_area,
};
use super::{
    ACCENT, GOOD, MUTED, WARN, diagnostic_title, fit_column, format_rate, health_reason_title,
    render_separator, selected_style, terminal_safe, terminal_safe_multiline, title_line,
    tun_reason_title,
};

pub(super) fn render_header(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    let (profile, tun, download, upload, connections) = state.status.as_ref().map_or_else(
        || {
            (
                Cow::Borrowed("-"),
                "--",
                "-".to_owned(),
                "-".to_owned(),
                "-".to_owned(),
            )
        },
        |status| {
            (
                status.active_profile.as_ref().map_or_else(
                    || Cow::Borrowed("-"),
                    |profile| terminal_safe(&profile.name),
                ),
                if status.tun.effective { "ON" } else { "OFF" },
                format_rate(status.traffic.download_bytes_per_second),
                format_rate(status.traffic.upload_bytes_per_second),
                status.connection_count.to_string(),
            )
        },
    );
    let (health, dot_color) = header_health(state);
    if regions.status.width < 90 {
        render_compact_status(
            state,
            regions.status,
            health,
            dot_color,
            profile.as_ref(),
            tun,
            buffer,
        );
    } else {
        render_full_status(
            state,
            regions.status,
            health,
            dot_color,
            profile,
            tun,
            download,
            upload,
            connections,
            buffer,
        );
    }

    render_navigation(state, regions, buffer);
}

#[allow(clippy::too_many_arguments)]
fn render_full_status(
    state: &AppState,
    area: Rect,
    health: &'static str,
    dot_color: Color,
    profile: Cow<'_, str>,
    tun: &'static str,
    download: String,
    upload: String,
    connections: String,
    buffer: &mut Buffer,
) {
    let prefix_width =
        Span::raw("● ").width() + Span::raw(health).width() + Span::raw("  Profile: ").width();
    let mode_and_tun_width = Span::raw("  Mode: RULE  TUN: ").width() + Span::raw(tun).width();
    let profile = terminal_safe(profile.as_ref());
    let mut line = vec![
        Span::styled("●", Style::default().fg(dot_color)),
        Span::raw(" "),
        Span::styled(
            health,
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  Profile: "),
    ];
    if health == "CONNECTED" {
        let metrics = format!("  ↓ {download}  ↑ {upload}  {connections} conn");
        let profile_width = (area.width as usize)
            .saturating_sub(prefix_width)
            .saturating_sub(mode_and_tun_width)
            .saturating_sub(Span::raw(&metrics).width());
        line.push(Span::styled(
            fit_column(profile.as_ref(), profile_width),
            Style::default().fg(ACCENT),
        ));
        push_mode_and_tun(&mut line, tun);
        line.push(Span::raw(metrics));
    } else {
        let recovery_label = "  Recovery: ";
        let reason = header_recovery_reason(state);
        let available = (area.width as usize)
            .saturating_sub(prefix_width)
            .saturating_sub(mode_and_tun_width)
            .saturating_sub(Span::raw(recovery_label).width());
        let reason_width = Span::raw(reason.as_ref()).width().min(available);
        let profile_width = Span::raw(profile.as_ref())
            .width()
            .min(available.saturating_sub(reason_width));
        let reason_width = available.saturating_sub(profile_width);
        line.push(Span::styled(
            fit_column(profile.as_ref(), profile_width),
            Style::default().fg(ACCENT),
        ));
        push_mode_and_tun(&mut line, tun);
        line.push(Span::raw("  Recovery: "));
        line.push(Span::styled(
            fit_column(reason.as_ref(), reason_width),
            Style::default().fg(WARN),
        ));
    }
    Paragraph::new(Line::from(line)).render(area, buffer);
}

fn push_mode_and_tun<'a>(line: &mut Vec<Span<'a>>, tun: &'static str) {
    line.push(Span::raw("  Mode: "));
    line.push(Span::styled("RULE", Style::default().fg(ACCENT)));
    line.push(Span::raw("  TUN: "));
    line.push(Span::styled(
        tun,
        Style::default().fg(if tun == "ON" { ACCENT } else { WARN }),
    ));
}

fn render_compact_status(
    state: &AppState,
    area: Rect,
    health: &'static str,
    dot_color: Color,
    profile: &str,
    tun: &'static str,
    buffer: &mut Buffer,
) {
    let short_health = compact_health_title(health);
    let prefix = format!("● {short_health}  ");
    if health == "CONNECTED" {
        let status = state
            .status
            .as_ref()
            .expect("connected header health requires a status snapshot");
        let tun_badge = if tun == "ON" { "[TUN]" } else { "[TUN OFF]" };
        let suffix = format!(
            "  [RULE] {tun_badge}  ↓{} ↑{} {}",
            compact_rate(status.traffic.download_bytes_per_second),
            compact_rate(status.traffic.upload_bytes_per_second),
            status.connection_count,
        );
        let profile_width = (area.width as usize)
            .saturating_sub(Span::raw(&prefix).width())
            .saturating_sub(Span::raw(&suffix).width());
        Paragraph::new(Line::from(vec![
            Span::styled("●", Style::default().fg(dot_color)),
            Span::raw(format!(" {short_health}  ")),
            Span::styled(
                fit_column(profile, profile_width),
                Style::default().fg(ACCENT),
            ),
            Span::raw("  "),
            Span::styled("[RULE]", Style::default().fg(ACCENT)),
            Span::raw(" "),
            Span::styled(tun_badge, Style::default().fg(ACCENT)),
            Span::raw(format!(
                "  ↓{} ↑{} {}",
                compact_rate(status.traffic.download_bytes_per_second),
                compact_rate(status.traffic.upload_bytes_per_second),
                status.connection_count,
            )),
        ]))
        .render(area, buffer);
        return;
    }

    let recovery_label = "  Recovery: ";
    let available = (area.width as usize)
        .saturating_sub(Span::raw(&prefix).width())
        .saturating_sub(Span::raw(recovery_label).width());
    let natural_profile_width = Span::raw(profile).width().max(1);
    let profile_width = natural_profile_width.min(available.saturating_sub(12).min(18));
    let reason_width = available.saturating_sub(profile_width);
    Paragraph::new(Line::from(vec![
        Span::styled("●", Style::default().fg(dot_color)),
        Span::raw(format!(" {short_health}  ")),
        Span::styled(
            fit_column(profile, profile_width),
            Style::default().fg(ACCENT),
        ),
        Span::raw(recovery_label),
        Span::styled(
            fit_column(header_recovery_reason(state).as_ref(), reason_width),
            Style::default().fg(WARN),
        ),
    ]))
    .render(area, buffer);
}

fn compact_health_title(health: &str) -> &str {
    match health {
        "CONNECTED" => "UP",
        "CONNECTING" => "WAIT",
        "DISCONNECTED" => "DOWN",
        "UNCONFIGURED" => "SETUP",
        "DEGRADED" => "DEG",
        "RELOADING" => "RELOAD",
        "STARTING" => "START",
        "STOPPED" => "STOP",
        "SYNCING" => "SYNC",
        "TUN OFF" => "TUN OFF",
        "STALE" => "STALE",
        value => value,
    }
}

fn compact_rate(bytes_per_second: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;
    let rate = bytes_per_second as f64;
    if rate >= TIB {
        format!("{:.1}T", rate / TIB)
    } else if rate >= GIB {
        format!("{:.1}G", rate / GIB)
    } else if rate >= MIB {
        format!("{:.1}M", rate / MIB)
    } else if rate >= KIB {
        format!("{:.1}K", rate / KIB)
    } else {
        format!("{bytes_per_second}B")
    }
}

fn render_navigation(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    for (page, area) in navigation_items(regions.navigation) {
        let selected = page == state.page;
        let focused = selected && state.focus == Focus::Tabs;
        let style = if focused {
            Style::default()
                .fg(Color::Black)
                .bg(ACCENT)
                .add_modifier(Modifier::BOLD)
        } else if selected {
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        };
        Paragraph::new(format!("{} {}", page.index() + 1, page.title()))
            .style(style)
            .render(area, buffer);
    }
    Paragraph::new(": commands")
        .style(Style::default().fg(MUTED))
        .render(command_palette_area(regions.navigation), buffer);
    render_separator(regions.header_separator, buffer);
    if let Some((_, area)) = navigation_items(regions.navigation)
        .into_iter()
        .find(|(page, _)| *page == state.page)
    {
        Paragraph::new("━".repeat(area.width as usize))
            .style(Style::default().fg(ACCENT))
            .render(
                Rect::new(
                    area.x,
                    regions.header_separator.y,
                    area.width
                        .min(regions.header_separator.right().saturating_sub(area.x)),
                    1,
                ),
                buffer,
            );
    }
}

fn header_health(state: &AppState) -> (&'static str, Color) {
    if state.connection.snapshot_stale {
        return ("STALE", WARN);
    }
    match state.connection.status {
        ConnectionStatus::Connecting => ("CONNECTING", WARN),
        ConnectionStatus::Disconnected => ("DISCONNECTED", Color::Red),
        ConnectionStatus::Connected => {
            let Some(status) = &state.status else {
                return ("SYNCING", WARN);
            };
            match (status.supervisor.lifecycle, status.core.lifecycle) {
                (SupervisorLifecycle::Degraded, _) | (_, CoreLifecycle::Degraded) => {
                    ("DEGRADED", WARN)
                }
                (SupervisorLifecycle::Stopping | SupervisorLifecycle::Stopped, _)
                | (_, CoreLifecycle::Stopping | CoreLifecycle::Stopped) => ("STOPPED", Color::Red),
                (SupervisorLifecycle::Starting, _) => ("STARTING", WARN),
                (_, CoreLifecycle::Unconfigured) => ("UNCONFIGURED", WARN),
                (_, CoreLifecycle::Starting) => ("STARTING", WARN),
                (_, CoreLifecycle::Reloading) => ("RELOADING", WARN),
                (_, CoreLifecycle::Ready) if status.tun.effective => ("CONNECTED", GOOD),
                (_, CoreLifecycle::Ready) => ("TUN OFF", WARN),
            }
        }
    }
}

fn header_recovery_reason(state: &AppState) -> Cow<'_, str> {
    match state.connection.status {
        ConnectionStatus::Connecting => return Cow::Borrowed("Connecting to Supervisor"),
        ConnectionStatus::Disconnected => return Cow::Borrowed("Supervisor IPC is unavailable"),
        ConnectionStatus::Connected => {}
    }
    if state.connection.snapshot_stale {
        return Cow::Borrowed("Waiting for a fresh status snapshot");
    }
    let Some(status) = &state.status else {
        return Cow::Borrowed("Synchronizing status");
    };
    if let Some(reason) = status.supervisor.health_reasons.first().copied() {
        return Cow::Borrowed(health_reason_title(reason));
    }
    if matches!(
        status.runtime_apply.recovery.status,
        RuntimeRecoveryStatus::Pending | RuntimeRecoveryStatus::Failed
    ) && let Some(message) = status.runtime_apply.recovery.message.as_deref()
    {
        return terminal_safe(message);
    }
    if let Some(diagnostic) = status.core.restart.diagnostic {
        return Cow::Borrowed(diagnostic_title(diagnostic));
    }
    if let Some(reason) = status.tun.reason {
        return Cow::Borrowed(tun_reason_title(reason));
    }
    match (status.supervisor.lifecycle, status.core.lifecycle) {
        (SupervisorLifecycle::Starting, _) | (_, CoreLifecycle::Starting) => {
            Cow::Borrowed("Startup is in progress")
        }
        (SupervisorLifecycle::Stopping, _) | (_, CoreLifecycle::Stopping) => {
            Cow::Borrowed("Shutdown is in progress")
        }
        (SupervisorLifecycle::Stopped, _) | (_, CoreLifecycle::Stopped) => {
            Cow::Borrowed("Supervisor is stopped")
        }
        (_, CoreLifecycle::Reloading) => Cow::Borrowed("Managed Core is reloading"),
        (_, CoreLifecycle::Unconfigured) => Cow::Borrowed("Add a Profile to configure the Core"),
        _ => Cow::Borrowed("Recovery is in progress"),
    }
}

pub(super) fn render_footer(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    render_separator(regions.footer_separator, buffer);
    let context = footer_context(state);
    let message = state
        .toast
        .as_ref()
        .map_or_else(|| Cow::Borrowed(context), |toast| terminal_safe(toast));
    Paragraph::new(message)
        .style(Style::default().fg(Color::Gray))
        .centered()
        .render(regions.footer, buffer);

    if !footer_controls_visible(state) {
        return;
    }

    Paragraph::new("[?] Help")
        .style(if state.focus == Focus::FooterHelp {
            selected_style(true)
        } else {
            Style::default().fg(ACCENT)
        })
        .render(footer_help_area(regions.footer), buffer);
    Paragraph::new("[q] Quit")
        .style(if state.focus == Focus::FooterQuit {
            selected_style(true)
        } else {
            Style::default().fg(MUTED)
        })
        .render(footer_quit_area(regions.footer), buffer);
}

fn footer_context(state: &AppState) -> &'static str {
    match state.modal.as_ref() {
        Some(Modal::CommandPalette) => "Type Filter  j/k Move  Enter Run  Esc Close",
        Some(Modal::Profiles) if state.profiles.activation_pending.is_some() => {
            "Activating…  Esc Close"
        }
        Some(Modal::Profiles) if state.focus == Focus::Search => {
            "Type Filter  Backspace Delete  Enter Done  Esc List"
        }
        Some(Modal::Profiles) => "j/k Move  Enter Activate  / Search  Esc Close",
        Some(Modal::RuleEditor { .. }) if state.modal_action_pending() => "Applying…  Esc Close",
        Some(Modal::RuleEditor { .. }) if !state.rules_projection_ready() => {
            "Rules unavailable  Esc Cancel"
        }
        Some(Modal::RuleEditor { .. }) => "Type Rule  Backspace Delete  Enter Apply  Esc Cancel",
        Some(Modal::RuleRemovalConfirmation { .. }) if state.modal_action_pending() => {
            "Removing…  Esc Close"
        }
        Some(Modal::RuleRemovalConfirmation { .. }) if !state.rules_projection_ready() => {
            "Rules unavailable  Esc Cancel"
        }
        Some(Modal::RuleRemovalConfirmation { .. }) => "Enter Confirm  Esc Cancel",
        Some(Modal::LifecycleConfirmation { .. }) if state.modal_action_pending() => {
            "Stopping…  Esc Close"
        }
        Some(Modal::LifecycleConfirmation { .. }) => "Enter Confirm  Esc Cancel",
        Some(Modal::Help | Modal::Message { .. }) => "Esc Close",
        None => match state.focus {
            Focus::Tabs => "1–5 Pages  Tab Next Focus",
            Focus::ProxyGroups => "j/k Move  Enter Open  l Nodes  z Zoom",
            Focus::Search => "Type Filter  Backspace Delete  Enter Done  Esc Close",
            Focus::FooterHelp => "Enter Help  Tab Next Focus",
            Focus::FooterQuit => "Enter Quit  Tab Next Focus",
            Focus::Modal => "Esc Close",
            Focus::Content => match state.page {
                Page::Overview => "p Profiles  1–5 Pages",
                Page::Proxies => "j/k Move  Enter Select  / Search  d Details",
                Page::Connections => "1–5 Pages  : Commands",
                Page::Rules if state.rules_projection_ready() => {
                    "j/k Move  Enter Edit  a Add  x Remove"
                }
                Page::Rules if state.rules.load_pending.is_some() => {
                    "Loading Rules…  / Search  r Reload"
                }
                Page::Rules => "Rules unavailable  / Search  r Reload",
                Page::Logs if state.logs.paused => "j/k Scroll  / Search  p Resume  f Follow",
                Page::Logs => "j/k Scroll  / Search  p Pause  f Follow",
            },
        },
    }
}

pub(super) fn render_modal(
    state: &AppState,
    modal: &Modal,
    regions: &LayoutRegions,
    buffer: &mut Buffer,
) {
    let Some(area) = regions.modal else {
        return;
    };
    Clear.render(area, buffer);
    match modal {
        Modal::CommandPalette => render_command_palette(state, area, buffer),
        Modal::Profiles => render_profiles(state, area, buffer),
        Modal::RuleEditor { original, value } => {
            render_rule_editor(state, original.as_deref(), value, area, buffer);
        }
        Modal::RuleRemovalConfirmation { rule } => {
            render_rule_removal_confirmation(state, rule, area, buffer);
        }
        Modal::LifecycleConfirmation { action } => {
            render_lifecycle_confirmation(state, *action, area, buffer);
        }
        Modal::Help => render_message_sheet(
            "Keyboard and mouse help",
            "1–5 pages · Tab/Shift+Tab focus · arrows or j/k move\n\
             Enter activates · / searches · d opens Node detail · z zooms Proxy focus\n\
             s sorts Nodes · a adds, Enter edits, x removes Rules\n\
             p pauses Logs · f follows Logs\n\
             Logs: a/d/i/w/e selects All/Debug/Info/Warn/Error\n\
             : opens commands · Esc closes · q quits from the main interface\n\n\
             Mouse: click tabs, commands, controls, search, Profile, Rule, or Node; wheel scrolls.",
            area,
            buffer,
        ),
        Modal::Message { title, body } => {
            render_message_sheet(title, body, area, buffer);
        }
    }
}

fn render_command_palette(state: &AppState, area: Rect, buffer: &mut Buffer) {
    render_separator(Rect::new(area.x, area.y, area.width, 1), buffer);
    Paragraph::new(Line::from(vec![
        Span::styled(
            "COMMANDS  ",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(":{}", terminal_safe(&state.command_palette.query)),
            Style::default().fg(ACCENT),
        ),
    ]))
    .render(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    let actions = filtered_palette_actions(&state.command_palette);
    for (row, action) in actions
        .iter()
        .copied()
        .take(area.height.saturating_sub(3) as usize)
        .enumerate()
    {
        let selected = row == state.command_palette.selected;
        Paragraph::new(format!(
            "{} {} {}",
            if selected { "▌" } else { " " },
            fit_column(action.label(), 22),
            action.description()
        ))
        .style(if selected {
            selected_style(true)
        } else {
            Style::default()
        })
        .render(
            Rect::new(area.x, area.y.saturating_add(2 + row as u16), area.width, 1),
            buffer,
        );
    }
    if actions.is_empty() {
        Paragraph::new("No matching commands")
            .style(Style::default().fg(MUTED))
            .render(
                Rect::new(area.x, area.y.saturating_add(2), area.width, 1),
                buffer,
            );
    }
    Paragraph::new("[Esc] Close  [↑/↓] Move  [Enter] Run")
        .style(Style::default().fg(MUTED))
        .render(
            Rect::new(area.x, area.bottom().saturating_sub(1), area.width, 1),
            buffer,
        );
}

fn render_rule_editor(
    state: &AppState,
    original: Option<&str>,
    value: &str,
    area: Rect,
    buffer: &mut Buffer,
) {
    render_separator(Rect::new(area.x, area.y, area.width, 1), buffer);
    Paragraph::new(title_line(if original.is_some() {
        "EDIT RULE STRING"
    } else {
        "ADD RULE STRING · APPEND"
    }))
    .render(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    Paragraph::new(Line::from(vec![
        Span::styled(
            "> ",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::raw(fit_column(value, area.width.saturating_sub(2) as usize)),
    ]))
    .render(
        Rect::new(area.x, area.y.saturating_add(2), area.width, 1),
        buffer,
    );
    Paragraph::new(format!(
        "{} / {} bytes",
        value.len(),
        crate::constants::RULE_STRING_MAX_BYTES
    ))
    .style(Style::default().fg(MUTED))
    .render(
        Rect::new(area.x, area.y.saturating_add(3), area.width, 1),
        buffer,
    );
    Paragraph::new(if state.modal_action_pending() {
        "[Esc] Close"
    } else {
        "[Esc] Cancel"
    })
    .style(Style::default().fg(MUTED))
    .render(sheet_close_area(area), buffer);
    Paragraph::new(if state.modal_action_pending() {
        " Applying…"
    } else if !state.rules_projection_ready() {
        " Rules unavailable"
    } else {
        "[Enter] Apply"
    })
    .style(Style::default().fg(ACCENT).add_modifier(Modifier::BOLD))
    .render(sheet_action_area(area), buffer);
}

fn render_rule_removal_confirmation(state: &AppState, rule: &str, area: Rect, buffer: &mut Buffer) {
    render_separator(Rect::new(area.x, area.y, area.width, 1), buffer);
    Paragraph::new(title_line("REMOVE RULE?")).render(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    Paragraph::new(fit_column(rule, area.width as usize)).render(
        Rect::new(area.x, area.y.saturating_add(2), area.width, 1),
        buffer,
    );
    Paragraph::new("This commits a new Runtime Generation.")
        .style(Style::default().fg(MUTED))
        .render(
            Rect::new(area.x, area.y.saturating_add(3), area.width, 1),
            buffer,
        );
    Paragraph::new(if state.modal_action_pending() {
        "[Esc] Close"
    } else {
        "[Esc] Cancel"
    })
    .style(Style::default().fg(MUTED))
    .render(sheet_close_area(area), buffer);
    Paragraph::new(if state.modal_action_pending() {
        " Removing…"
    } else if !state.rules_projection_ready() {
        " Rules unavailable"
    } else {
        "[Enter] Remove"
    })
    .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    .render(sheet_action_area(area), buffer);
}

fn render_lifecycle_confirmation(
    state: &AppState,
    action: CommandPaletteAction,
    area: Rect,
    buffer: &mut Buffer,
) {
    render_separator(Rect::new(area.x, area.y, area.width, 1), buffer);
    Paragraph::new(title_line("STOP SUPERVISOR?")).render(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    Paragraph::new(action.description()).render(
        Rect::new(area.x, area.y.saturating_add(2), area.width, 1),
        buffer,
    );
    Paragraph::new("The Status Interface exits after the Supervisor stops.")
        .style(Style::default().fg(MUTED))
        .render(
            Rect::new(area.x, area.y.saturating_add(3), area.width, 1),
            buffer,
        );
    Paragraph::new(if state.modal_action_pending() {
        "[Esc] Close"
    } else {
        "[Esc] Cancel"
    })
    .style(Style::default().fg(MUTED))
    .render(sheet_close_area(area), buffer);
    Paragraph::new(if state.modal_action_pending() {
        " Stopping…"
    } else {
        "[Enter] Stop"
    })
    .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    .render(sheet_action_area(area), buffer);
}

fn render_profiles(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let regions = profile_sheet_regions(area);
    render_separator(regions.separator, buffer);
    Paragraph::new(title_line(format!(
        "COMMANDS · PROFILES ({})",
        state.profiles.rows.len()
    )))
    .render(regions.title, buffer);
    let search_style = if state.focus == Focus::Search {
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Gray)
    };
    Paragraph::new(format!("/{}", terminal_safe(&state.profiles.filter)))
        .style(search_style)
        .render(regions.search, buffer);
    Paragraph::new("  STATE               PROFILE                      FRESH  NEXT REFRESH")
        .style(Style::default().fg(MUTED))
        .render(regions.header, buffer);

    let rows = filtered_profiles(&state.profiles);
    let offset = state.profiles.scroll.min(state.profiles.selected);
    for (visible_index, row) in rows
        .into_iter()
        .skip(offset)
        .take(regions.list.height as usize)
        .enumerate()
    {
        let selected = offset + visible_index == state.profiles.selected;
        let pending = state.profiles.activation_pending == Some(row.id);
        let state_label = match (row.active, pending) {
            (true, true) => "[active] [pending]",
            (true, false) => "[active]          ",
            (false, true) => "         [pending]",
            (false, false) => "                  ",
        };
        let freshness = if row.fresh { "fresh" } else { "stale" };
        let error = row
            .error
            .as_deref()
            .map_or_else(|| Cow::Borrowed(""), terminal_safe);
        let line = format!(
            "{} {state_label:<18} {} {freshness:<6} {} {error}",
            if selected { "▌" } else { " " },
            fit_column(&row.name, 28),
            row.next_refresh_at_unix_ms,
        );
        Paragraph::new(line)
            .style(if selected {
                selected_style(state.focus == Focus::Content)
            } else if row.active {
                Style::default().fg(ACCENT)
            } else {
                Style::default()
            })
            .render(
                Rect::new(
                    regions.list.x,
                    regions.list.y.saturating_add(visible_index as u16),
                    regions.list.width,
                    1,
                ),
                buffer,
            );
    }
    Paragraph::new("[Esc] Close")
        .style(Style::default().fg(MUTED))
        .render(regions.close, buffer);
}

fn render_message_sheet(title: &str, body: &str, area: Rect, buffer: &mut Buffer) {
    render_separator(Rect::new(area.x, area.y, area.width, 1), buffer);
    Paragraph::new(title_line(terminal_safe(title))).render(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    Paragraph::new(terminal_safe_multiline(body).as_ref()).render(
        Rect::new(
            area.x,
            area.y.saturating_add(3),
            area.width,
            area.height.saturating_sub(4),
        ),
        buffer,
    );
    Paragraph::new("[Esc] Close")
        .style(Style::default().fg(MUTED))
        .render(
            Rect::new(
                area.right().saturating_sub(11),
                area.bottom().saturating_sub(1),
                11_u16.min(area.width),
                1,
            ),
            buffer,
        );
}

pub(super) fn render_minimum_size(area: Rect, buffer: &mut Buffer) {
    Paragraph::new(Line::from(vec![
        Span::styled(
            "RATASH",
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  Terminal too small"),
    ]))
    .render(Rect::new(area.x, area.y, area.width, 1), buffer);
    render_separator(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    Paragraph::new(format!(
        "Required: {}x{}\nCurrent: {}x{}",
        MINIMUM_TERMINAL_WIDTH, MINIMUM_TERMINAL_HEIGHT, area.width, area.height
    ))
    .render(
        Rect::new(
            area.x,
            area.y.saturating_add(3),
            area.width,
            area.height.saturating_sub(3),
        ),
        buffer,
    );
}

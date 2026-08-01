//! Pure layout, rendering, and interaction projection for the Status Interface.

use std::borrow::Cow;

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Sparkline, Tabs, Widget};

use crate::application::{LatencyFreshness, LatencyProbeStatus};
use crate::constants::{MINIMUM_TERMINAL_HEIGHT, MINIMUM_TERMINAL_WIDTH};
use crate::domain::{
    CoreDiagnosticCategory, RuntimeApplyPhase, RuntimeRecoveryStatus, SampleState,
    SupervisorHealthReason, TunReason,
};
use crate::telemetry::{LogLevel, LogSource};

use super::{
    AppState, ConnectionState, ConnectionStatus, Focus, Interaction, InteractionMap,
    LogLevelFilter, Modal, Page, ProxiesState, ProxyGroupRow, ProxySort, ScrollInteraction,
    UiIntent, filtered_logs, filtered_profiles, filtered_proxies,
};

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
                intent: UiIntent::ShowProxyGroup(group.id.clone()),
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
                            group_id: row.group_id.clone(),
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
        .constraints([Constraint::Percentage(70), Constraint::Percentage(30)])
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
        let probe_queue_state = if status.probe_queue.overloaded {
            "overloaded"
        } else {
            "healthy"
        };
        let oldest_due = status
            .probe_queue
            .oldest_due_age_ms
            .map_or_else(|| "-".to_owned(), |age| format!("{age} ms"));
        let selection_restore = if status.selection_restore_pending {
            "pending"
        } else {
            "settled"
        };
        let restart_backoff = status
            .core
            .restart
            .backoff_ms
            .map_or_else(|| "-".to_owned(), |backoff| format!("{backoff} ms"));
        let restart_diagnostic =
            status
                .core
                .restart
                .diagnostic
                .map_or("-", |diagnostic| match diagnostic {
                    CoreDiagnosticCategory::RestartLimitReached => "core_restart_limit_reached",
                });
        let restart_pending = if status.core.restart.pending {
            "on"
        } else {
            "off"
        };
        let tun_reason = status.tun.reason.map_or("-", |reason| match reason {
            TunReason::NoActiveProfile => "no_active_profile",
            TunReason::PermissionDenied => "permission_denied",
            TunReason::Unsupported => "unsupported",
            TunReason::CoreUnavailable => "core_unavailable",
        });
        let tun_effective = if status.tun.effective { "on" } else { "off" };
        let tun_capable = if status.tun.capable { "yes" } else { "no" };
        let candidate_generation = status
            .runtime_apply
            .candidate_generation
            .map_or_else(|| "-".to_owned(), |generation| generation.0.to_string());
        let committed_generation = status
            .runtime_apply
            .committed_generation
            .map_or_else(|| "-".to_owned(), |generation| generation.0.to_string());
        let restored_generation = status
            .runtime_apply
            .recovery
            .restored_generation
            .map_or_else(|| "-".to_owned(), |generation| generation.0.to_string());
        let recovery_detail = status
            .runtime_apply
            .recovery
            .message
            .as_deref()
            .map_or_else(String::new, |message| {
                format!("\nWhy: {}", terminal_safe(message))
            });
        let health_reasons = if status.supervisor.health_reasons.is_empty() {
            "none".to_owned()
        } else {
            status
                .supervisor
                .health_reasons
                .iter()
                .map(|reason| match reason {
                    SupervisorHealthReason::RuntimeRecovery => "runtime_recovery",
                    SupervisorHealthReason::SelectionCompensation => "selection_compensation",
                    SupervisorHealthReason::ConfigurationProjection => "configuration_projection",
                    SupervisorHealthReason::ProbeScheduler => "probe_scheduler",
                    SupervisorHealthReason::SelectionRestoration => "selection_restoration",
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        format!(
            "Connection: {connection}\nSupervisor: {:?} | Core: {:?}\nHealth: {health_reasons}\nRestart: {restart_pending}, tries={}, wait={restart_backoff}\nDiagnostic: {restart_diagnostic}\nTUN: {tun_effective}, cap={tun_capable}, {tun_reason}\nApply: {}, candidate={candidate_generation}, committed={committed_generation}\nRecovery: {}, restored={restored_generation}{recovery_detail}\nProfile: {} | Group: {}\nNode: {} | Latency: {delay}\nSelection Restore: {selection_restore}\nSampled At: {sampled_at}\nFreshness: {freshness}\nProbe: {probe_status} (generation {probe_generation})\nProbe Queue: {probe_queue_state}, queued {}, in-flight {}, stale {:.1}%\nProbe Window: oldest {oldest_due}, full pass {} ms\nConnections: {} | Uptime: {}s",
            status.supervisor.lifecycle,
            status.core.lifecycle,
            status.core.restart.attempts,
            runtime_apply_phase_title(status.runtime_apply.phase),
            runtime_recovery_status_title(status.runtime_apply.recovery.status),
            active_profile,
            primary_group,
            current_node,
            status.probe_queue.queue_depth,
            status.probe_queue.in_flight_count,
            status.probe_queue.stale_ratio() * 100.0,
            status.probe_queue.estimated_full_pass_duration_ms,
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

fn runtime_apply_phase_title(phase: RuntimeApplyPhase) -> &'static str {
    match phase {
        RuntimeApplyPhase::Idle => "idle",
        RuntimeApplyPhase::Applying => "applying",
        RuntimeApplyPhase::Succeeded => "succeeded",
        RuntimeApplyPhase::Recovering => "recovering",
        RuntimeApplyPhase::Failed => "failed",
    }
}

fn runtime_recovery_status_title(status: RuntimeRecoveryStatus) -> &'static str {
    match status {
        RuntimeRecoveryStatus::NotRequired => "not_required",
        RuntimeRecoveryStatus::Succeeded => "succeeded",
        RuntimeRecoveryStatus::Pending => "pending",
        RuntimeRecoveryStatus::Failed => "failed",
    }
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
        .is_some_and(|pending| pending.group_id == group.id)
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
            .is_some_and(|load| load.group_id == group.id);
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
    let (Some(proxy_groups_area), Some(search_area), Some(list_area)) =
        (regions.proxy_groups, regions.search, regions.list)
    else {
        return;
    };
    render_proxy_groups(state, proxy_groups_area, buffer);
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
                    .is_some_and(|(group_id, node_id)| {
                        group_id == &row.group_id && row.node_id.as_ref() == Some(node_id)
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
        .render(list_area, buffer);
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
    let (Some(search_area), Some(list_area)) = (regions.search, regions.list) else {
        return;
    };
    render_search(
        "Profile search",
        &state.profiles.filter,
        state.focus == Focus::Search,
        search_area,
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
        .render(list_area, buffer);
}

fn render_logs(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    let (Some(search_area), Some(list_area)) = (regions.search, regions.list) else {
        return;
    };
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
        search_area,
        buffer,
    );

    let rows = filtered_logs(&state.logs);
    let visible = list_area.height.saturating_sub(2) as usize;
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
        .render(list_area, buffer);
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

use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Sparkline, Widget};

use crate::domain::{RuntimeApplyPhase, RuntimeRecoveryStatus};

use super::super::AppState;
use super::{
    ACCENT, MUTED, core_lifecycle_title, diagnostic_title, format_rate, health_reason_title,
    latency_probe_status_title, log_level_title, render_separator, render_vertical_separator,
    runtime_apply_phase_title, runtime_recovery_status_title, sample_state_title,
    supervisor_lifecycle_title, terminal_safe, title_line, tun_reason_title,
};

pub(super) fn render(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let (summary, separator, traffic) = if area.width >= 110 {
        let columns = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Min(70),
                Constraint::Length(1),
                Constraint::Length(32),
            ])
            .split(area);
        (columns[0], Some(columns[1]), Some(columns[2]))
    } else {
        (area, None, None)
    };
    render_summary(state, summary, buffer);
    if let (Some(separator), Some(traffic)) = (separator, traffic) {
        render_vertical_separator(separator, buffer);
        render_traffic(state, traffic, buffer);
    }
}

fn render_summary(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let Some(status) = &state.status else {
        Paragraph::new(vec![
            title_line("OVERVIEW"),
            Line::from("Waiting for the first status snapshot"),
        ])
        .render(area, buffer);
        return;
    };

    let active_profile = status
        .active_profile
        .as_ref()
        .map(|profile| terminal_safe(&profile.name));
    let primary_group = status.primary_proxy_group.as_deref().map(terminal_safe);
    let current_node = status
        .selected_node
        .as_ref()
        .map(|node| terminal_safe(&node.name));
    let tun_reason = status.tun.reason.map_or("ready", tun_reason_title);
    let mut lines = vec![
        title_line("OVERVIEW"),
        Line::from(format!(
            "Supervisor: {} | Core: {}",
            supervisor_lifecycle_title(status.supervisor.lifecycle),
            core_lifecycle_title(status.core.lifecycle)
        )),
        Line::from(format!(
            "Profile: {} | Group: {}",
            active_profile.as_deref().unwrap_or("unconfigured"),
            primary_group.as_deref().unwrap_or("unconfigured")
        )),
    ];
    if let Some(sample) = &status.latency {
        let delay = sample
            .delay_ms
            .map_or_else(|| "not_sampled".to_owned(), |delay| format!("{delay} ms"));
        let selected_probe = status.selected_node.as_ref().and_then(|selected| {
            state
                .proxies
                .rows
                .iter()
                .find(|row| row.node_id.as_ref() == Some(&selected.id))
                .map(|row| latency_probe_status_title(row.probe_status))
        });
        let probe_status = selected_probe.unwrap_or_else(|| {
            if sample.delay_ms.is_some() {
                "succeeded"
            } else {
                "failed"
            }
        });
        lines.push(Line::from(format!(
            "Node: {} | Latency: {delay}",
            current_node.as_deref().unwrap_or("unselected")
        )));
        lines.push(Line::from(format!(
            "Sampled At: {} · Freshness: {} · Probe: {probe_status} (generation {})",
            sample
                .sampled_at_unix_ms
                .map_or_else(|| "pending".to_owned(), |time| time.to_string()),
            sample_state_title(sample.state),
            sample.probe_generation.0
        )));
    } else if current_node.is_some() {
        lines.push(Line::from(format!(
            "Node: {} | Latency: not_sampled",
            current_node.as_deref().unwrap_or("unselected")
        )));
    }
    lines.extend([
        Line::from(format!(
            "Traffic: ↓ {} | ↑ {}",
            format_rate(status.traffic.download_bytes_per_second),
            format_rate(status.traffic.upload_bytes_per_second)
        )),
        Line::from(format!(
            "Connections: {} | Uptime: {}s",
            status.connection_count, status.supervisor.uptime_seconds
        )),
        Line::from(format!(
            "TUN: {}, cap={}, {tun_reason}",
            if status.tun.effective { "on" } else { "off" },
            if status.tun.capable { "yes" } else { "no" }
        )),
    ]);

    if !status.supervisor.health_reasons.is_empty() {
        lines.push(Line::from(format!(
            "Health: {}",
            status
                .supervisor
                .health_reasons
                .iter()
                .copied()
                .map(health_reason_title)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    if !matches!(
        status.runtime_apply.phase,
        RuntimeApplyPhase::Idle | RuntimeApplyPhase::Succeeded
    ) || status.runtime_apply.recovery.status != RuntimeRecoveryStatus::NotRequired
    {
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
        lines.push(Line::from(format!(
            "Apply: {}, candidate={candidate_generation}, committed={committed_generation}",
            runtime_apply_phase_title(status.runtime_apply.phase)
        )));
        lines.push(Line::from(format!(
            "Recovery: {}, restored={restored_generation}",
            runtime_recovery_status_title(status.runtime_apply.recovery.status)
        )));
        if let Some(message) = status.runtime_apply.recovery.message.as_deref() {
            lines.push(Line::from(format!("Why: {}", terminal_safe(message))));
        }
    }
    if status.core.restart.pending
        || status.core.restart.attempts > 0
        || status.core.restart.diagnostic.is_some()
    {
        let restart_backoff = status
            .core
            .restart
            .backoff_ms
            .map_or_else(|| "-".to_owned(), |backoff| format!("{backoff} ms"));
        lines.push(Line::from(format!(
            "Restart: {}, tries={}, wait={restart_backoff}",
            if status.core.restart.pending {
                "on"
            } else {
                "off"
            },
            status.core.restart.attempts
        )));
        if let Some(diagnostic) = status.core.restart.diagnostic {
            lines.push(Line::from(format!(
                "Diagnostic: {}",
                diagnostic_title(diagnostic)
            )));
        }
    }
    if status.selection_restore_pending {
        lines.push(Line::from("Selection Restore: pending"));
    }
    if status.probe_queue.overloaded
        || status.probe_queue.queue_depth > 0
        || status.probe_queue.in_flight_count > 0
        || status.probe_queue.stale_node_count > 0
    {
        let oldest_due = status
            .probe_queue
            .oldest_due_age_ms
            .map_or_else(|| "-".to_owned(), |age| format!("{age} ms"));
        lines.extend([
            Line::from(format!(
                "Probe Queue: {}",
                if status.probe_queue.overloaded {
                    "overloaded"
                } else {
                    "active"
                }
            )),
            Line::from(format!(
                "queued {}, in-flight {}, stale {:.1}%",
                status.probe_queue.queue_depth,
                status.probe_queue.in_flight_count,
                status.probe_queue.stale_ratio() * 100.0
            )),
            Line::from(format!(
                "oldest {oldest_due}, full pass {} ms",
                status.probe_queue.estimated_full_pass_duration_ms
            )),
        ]);
    }

    let available_recent = (area.height as usize).saturating_sub(lines.len());
    if available_recent >= 3 && !state.logs.records.is_empty() {
        lines.push(Line::from(""));
        lines.push(title_line("RECENT"));
        lines.extend(
            state
                .logs
                .records
                .iter()
                .rev()
                .take(available_recent.saturating_sub(2).min(3))
                .map(|record| {
                    Line::from(format!(
                        "{}  {:<5}  {}",
                        record.timestamp_unix_ms,
                        log_level_title(record.level),
                        terminal_safe(&record.message)
                    ))
                }),
        );
    }
    Paragraph::new(lines).render(area, buffer);
}

fn render_traffic(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let Some(status) = &state.status else {
        return;
    };
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(2),
            Constraint::Length(1),
            Constraint::Length(4),
            Constraint::Length(1),
            Constraint::Length(4),
            Constraint::Min(1),
        ])
        .split(area);
    Paragraph::new(title_line("TRAFFIC")).render(rows[0], buffer);
    Paragraph::new(Line::from(vec![
        Span::styled("↓ ", Style::default().fg(ACCENT)),
        Span::styled(
            format_rate(status.traffic.download_bytes_per_second),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw("   "),
        Span::styled("↑ ", Style::default().fg(Color::Gray)),
        Span::styled(
            format_rate(status.traffic.upload_bytes_per_second),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ]))
    .render(rows[1], buffer);
    render_separator(rows[2], buffer);
    let download = state.download_series.iter().copied().collect::<Vec<_>>();
    Sparkline::default()
        .data(&download)
        .style(Style::default().fg(ACCENT))
        .render(rows[3], buffer);
    Paragraph::new("DOWNLOAD HISTORY")
        .style(Style::default().fg(MUTED))
        .render(rows[4], buffer);
    let upload = state.upload_series.iter().copied().collect::<Vec<_>>();
    Sparkline::default()
        .data(&upload)
        .style(Style::default().fg(Color::Gray))
        .render(rows[5], buffer);
    Paragraph::new("UPLOAD HISTORY")
        .style(Style::default().fg(MUTED))
        .render(rows[6], buffer);
}

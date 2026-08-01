//! Pure layout, rendering, and interaction projection for the Status Interface.

use std::borrow::Cow;

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use crate::application::{LatencyFreshness, LatencyProbeStatus, PolicyTargetValidation};
use crate::domain::{
    CoreDiagnosticCategory, CoreLifecycle, RuntimeApplyPhase, RuntimeRecoveryStatus, SampleState,
    StreamState, SupervisorHealthReason, SupervisorLifecycle, TunReason,
};
use crate::telemetry::{LogLevel, LogSource};

use super::{AppState, InteractionMap, Page};

mod connections;
mod frame;
mod layout;
mod logs;
mod overview;
mod proxies;
mod rules;

pub use layout::{LayoutRegions, compute_layout};

pub(super) const ACCENT: Color = Color::Cyan;
pub(super) const GOOD: Color = Color::Green;
pub(super) const MUTED: Color = Color::DarkGray;
pub(super) const WARN: Color = Color::Yellow;

pub fn render(frame: &mut Frame<'_>, state: &AppState) -> InteractionMap {
    render_buffer(state, frame.area(), frame.buffer_mut())
}

pub fn render_buffer(state: &AppState, area: Rect, buffer: &mut Buffer) -> InteractionMap {
    let (regions, map) = compute_layout(state, area, state.next_frame_revision);
    if regions.minimum_size {
        frame::render_minimum_size(area, buffer);
        return map;
    }

    frame::render_header(state, &regions, buffer);
    match state.page {
        Page::Overview => overview::render(state, regions.content, buffer),
        Page::Proxies => proxies::render(state, &regions, buffer),
        Page::Connections => connections::render(state, regions.content, buffer),
        Page::Rules => rules::render(state, &regions, buffer),
        Page::Logs => logs::render(state, &regions, buffer),
    }
    frame::render_footer(state, &regions, buffer);
    if let Some(modal) = &state.modal {
        frame::render_modal(state, modal, &regions, buffer);
    }
    map
}

pub(super) fn render_separator(area: Rect, buffer: &mut Buffer) {
    if area.is_empty() {
        return;
    }
    Paragraph::new("─".repeat(area.width as usize))
        .style(Style::default().fg(MUTED))
        .render(area, buffer);
}

pub(super) fn render_vertical_separator(area: Rect, buffer: &mut Buffer) {
    for y in area.y..area.bottom() {
        buffer
            .cell_mut(Position::new(area.x, y))
            .map(|cell| cell.set_symbol("│").set_fg(MUTED));
    }
}

pub(super) fn title_line<'a>(title: impl Into<Cow<'a, str>>) -> Line<'a> {
    Line::from(Span::styled(
        title,
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
    ))
}

pub(super) fn selected_style(focused: bool) -> Style {
    if focused {
        Style::default()
            .fg(Color::Black)
            .bg(ACCENT)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
    }
}

pub(super) fn terminal_safe(value: &str) -> Cow<'_, str> {
    terminal_safe_with_newlines(value, false)
}

pub(super) fn terminal_safe_multiline(value: &str) -> Cow<'_, str> {
    terminal_safe_with_newlines(value, true)
}

pub(super) fn fit_column(value: &str, width: usize) -> String {
    if width == 0 {
        return String::new();
    }
    let safe = terminal_safe(value);
    let safe_width = Span::raw(safe.as_ref()).width();
    if safe_width <= width {
        return format!("{}{}", safe, " ".repeat(width - safe_width));
    }

    let content_width = width.saturating_sub(1);
    let mut fitted = String::new();
    let mut fitted_width = 0_usize;
    for character in safe.chars() {
        let character_width = Span::raw(character.to_string()).width();
        if fitted_width.saturating_add(character_width) > content_width {
            break;
        }
        fitted.push(character);
        fitted_width = fitted_width.saturating_add(character_width);
    }
    fitted.push('…');
    fitted.push_str(&" ".repeat(width.saturating_sub(fitted_width + 1)));
    fitted
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

pub(super) fn format_rate(bytes_per_second: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let rate = bytes_per_second as f64;
    if rate >= GIB {
        format!("{:.1} GiB/s", rate / GIB)
    } else if rate >= MIB {
        format!("{:.1} MiB/s", rate / MIB)
    } else if rate >= KIB {
        format!("{:.1} KiB/s", rate / KIB)
    } else {
        format!("{bytes_per_second} B/s")
    }
}

pub(super) fn supervisor_lifecycle_title(value: SupervisorLifecycle) -> &'static str {
    match value {
        SupervisorLifecycle::Starting => "Starting",
        SupervisorLifecycle::Ready => "Ready",
        SupervisorLifecycle::Stopping => "Stopping",
        SupervisorLifecycle::Stopped => "Stopped",
        SupervisorLifecycle::Degraded => "Degraded",
    }
}

pub(super) fn core_lifecycle_title(value: CoreLifecycle) -> &'static str {
    match value {
        CoreLifecycle::Unconfigured => "Unconfigured",
        CoreLifecycle::Stopped => "Stopped",
        CoreLifecycle::Starting => "Starting",
        CoreLifecycle::Ready => "Ready",
        CoreLifecycle::Reloading => "Reloading",
        CoreLifecycle::Stopping => "Stopping",
        CoreLifecycle::Degraded => "Degraded",
    }
}

pub(super) fn health_reason_title(value: SupervisorHealthReason) -> &'static str {
    match value {
        SupervisorHealthReason::RuntimeRecovery => "runtime_recovery",
        SupervisorHealthReason::SelectionCompensation => "selection_compensation",
        SupervisorHealthReason::ConfigurationProjection => "configuration_projection",
        SupervisorHealthReason::ProbeScheduler => "probe_scheduler",
        SupervisorHealthReason::SelectionRestoration => "selection_restoration",
    }
}

pub(super) fn diagnostic_title(value: CoreDiagnosticCategory) -> &'static str {
    match value {
        CoreDiagnosticCategory::RestartLimitReached => "core_restart_limit_reached",
    }
}

pub(super) fn tun_reason_title(value: TunReason) -> &'static str {
    match value {
        TunReason::NoActiveProfile => "no_active_profile",
        TunReason::PermissionDenied => "permission_denied",
        TunReason::Unsupported => "unsupported",
        TunReason::CoreUnavailable => "core_unavailable",
    }
}

pub(super) fn runtime_apply_phase_title(value: RuntimeApplyPhase) -> &'static str {
    match value {
        RuntimeApplyPhase::Idle => "idle",
        RuntimeApplyPhase::Applying => "applying",
        RuntimeApplyPhase::Succeeded => "succeeded",
        RuntimeApplyPhase::Recovering => "recovering",
        RuntimeApplyPhase::Failed => "failed",
    }
}

pub(super) fn runtime_recovery_status_title(value: RuntimeRecoveryStatus) -> &'static str {
    match value {
        RuntimeRecoveryStatus::NotRequired => "not_required",
        RuntimeRecoveryStatus::Succeeded => "succeeded",
        RuntimeRecoveryStatus::Pending => "pending",
        RuntimeRecoveryStatus::Failed => "failed",
    }
}

pub(super) fn sample_state_title(value: SampleState) -> &'static str {
    match value {
        SampleState::Fresh => "fresh",
        SampleState::Stale => "stale",
        SampleState::Unavailable => "unavailable",
    }
}

pub(super) fn stream_state_title(value: StreamState) -> &'static str {
    match value {
        StreamState::Disconnected => "disconnected",
        StreamState::Connecting => "connecting",
        StreamState::Healthy => "healthy",
        StreamState::Stale => "stale",
        StreamState::Degraded => "degraded",
    }
}

pub(super) fn latency_freshness_title(value: LatencyFreshness) -> &'static str {
    match value {
        LatencyFreshness::NotSampled => "not_sampled",
        LatencyFreshness::Fresh => "fresh",
        LatencyFreshness::Stale => "stale",
        LatencyFreshness::Unavailable => "unavailable",
    }
}

pub(super) fn latency_probe_status_title(value: LatencyProbeStatus) -> &'static str {
    match value {
        LatencyProbeStatus::NotSampled => "not_sampled",
        LatencyProbeStatus::Queued => "queued",
        LatencyProbeStatus::InFlight => "in_flight",
        LatencyProbeStatus::Succeeded => "succeeded",
        LatencyProbeStatus::Failed => "failed",
    }
}

pub(super) fn policy_validation_title(value: PolicyTargetValidation) -> &'static str {
    match value {
        PolicyTargetValidation::Valid => "valid",
        PolicyTargetValidation::Missing => "missing",
        PolicyTargetValidation::Unavailable => "unavailable",
    }
}

pub(super) fn log_level_title(value: LogLevel) -> &'static str {
    match value {
        LogLevel::Debug => "DEBUG",
        LogLevel::Info => "INFO",
        LogLevel::Warn => "WARN",
        LogLevel::Error => "ERROR",
    }
}

pub(super) fn log_source_title(value: LogSource) -> &'static str {
    match value {
        LogSource::CoreApi => "core",
        LogSource::Stdout => "stdout",
        LogSource::Stderr => "stderr",
    }
}

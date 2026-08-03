//! Pure layout, rendering, and interaction projection for the Status Interface.

use std::borrow::Cow;

use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::{Position, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use crate::application::PolicyTargetValidation;
use crate::domain::{CoreDiagnosticCategory, SupervisorHealthReason, TunReason};
use crate::telemetry::{LogLevel, LogSource};

use super::{AppState, InteractionMap, Page};

mod connections;
mod frame;
mod layout;
mod logs;
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
        Page::Proxies => proxies::render(state, &regions, buffer),
        Page::Connections => connections::render(state, &regions, buffer),
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

pub(super) fn fit_node_name(value: &str, width: usize) -> String {
    let safe = terminal_safe(value);
    let span = Span::raw(safe.as_ref());
    let Some(first) = span.styled_graphemes(Style::default()).next() else {
        return fit_column(safe.as_ref(), width);
    };
    if !first.symbol.chars().any(is_emoji_scalar) {
        return fit_column(safe.as_ref(), width);
    }

    // Reserve one visual guard cell for color emoji in macOS Terminal.
    let mut guarded = String::with_capacity(safe.len().saturating_add(1));
    guarded.push_str(first.symbol);
    guarded.push(' ');
    guarded.push_str(&safe[first.symbol.len()..]);
    fit_column(&guarded, width)
}

fn is_emoji_scalar(character: char) -> bool {
    matches!(
        character as u32,
        0x2300..=0x23ff | 0x2600..=0x27bf | 0x1f000..=0x1faff | 0xfe0f
    )
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

pub(super) fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{} B", bytes as u64)
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

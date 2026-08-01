use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use crate::telemetry::LogLevel;

use super::super::{AppState, Focus, selected_log_position, visible_log_start};
use super::layout::{LayoutRegions, log_follow_area, log_level_areas, log_pause_area};
use super::{
    ACCENT, MUTED, WARN, fit_column, log_level_title, log_source_title, render_separator,
    terminal_safe, title_line,
};

const SELECTED_LOG_BACKGROUND: Color = Color::Rgb(0, 72, 78);

pub(super) fn render(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    let controls = Rect::new(
        regions.content.x,
        regions.content.y,
        regions.content.width,
        1,
    );
    Paragraph::new(title_line("LOGS")).render(
        Rect::new(controls.x, controls.y, 6_u16.min(controls.width), 1),
        buffer,
    );
    let compact_controls = controls.width < 150;
    for (level, area) in log_level_areas(controls) {
        let label = if compact_controls {
            level.title().chars().next().unwrap_or('-').to_string()
        } else {
            level.title().to_owned()
        };
        Paragraph::new(format!(" {label} "))
            .style(if level == state.logs.level_filter {
                Style::default().fg(Color::Black).bg(ACCENT)
            } else {
                Style::default().fg(Color::Gray)
            })
            .render(area, buffer);
    }
    Paragraph::new(if compact_controls && state.logs.paused {
        " R "
    } else if compact_controls {
        " P "
    } else if state.logs.paused {
        " Resume "
    } else {
        " Pause  "
    })
    .style(if state.logs.paused {
        Style::default().fg(Color::Black).bg(WARN)
    } else {
        Style::default().fg(ACCENT)
    })
    .render(log_pause_area(controls), buffer);
    Paragraph::new(if compact_controls { " F " } else { " Follow " })
        .style(if state.logs.follow {
            Style::default().fg(Color::Black).bg(ACCENT)
        } else {
            Style::default().fg(ACCENT)
        })
        .render(log_follow_area(controls), buffer);
    let follow_area = log_follow_area(controls);
    let status_x = follow_area.right().saturating_add(1).min(controls.right());
    let status_area = Rect::new(
        status_x,
        controls.y,
        controls.right().saturating_sub(status_x),
        1,
    );
    let status = if status_area.width < 16 {
        format!(
            "{}d{} e{}",
            if state.logs.gap { "g " } else { "" },
            state.logs.dropped_total,
            state.logs.evicted_total,
        )
    } else if status_area.width < 40 {
        format!(
            "{}d={} e={} {} {}",
            if state.logs.gap { "gap " } else { "" },
            state.logs.dropped_total,
            state.logs.evicted_total,
            if state.logs.paused { "paused" } else { "live" },
            if state.logs.follow {
                "follow"
            } else {
                "manual"
            },
        )
    } else {
        format!(
            "{}dropped={} · evicted={} · {} · {}",
            if state.logs.gap { "gap · " } else { "" },
            state.logs.dropped_total,
            state.logs.evicted_total,
            if state.logs.paused { "paused" } else { "live" },
            if state.logs.follow {
                "following"
            } else {
                "manual"
            },
        )
    };
    Paragraph::new(status)
        .style(Style::default().fg(MUTED))
        .render(status_area, buffer);

    if let Some(search) = regions.search {
        Paragraph::new(format!("/{}", terminal_safe(&state.logs.search)))
            .style(if state.focus == Focus::Search {
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            })
            .render(search, buffer);
    }
    let Some(list) = regions.list else {
        return;
    };
    let row_indices = regions.log_row_indices();
    let selected = selected_log_position(&state.logs, row_indices.len());
    let start = visible_log_start(&state.logs, row_indices.len(), list.height as usize);
    for (visible_index, position) in (start..row_indices.len())
        .take(list.height as usize)
        .enumerate()
    {
        let Some(record) = row_indices
            .get(position)
            .and_then(|index| state.logs.records.get(*index))
        else {
            continue;
        };
        let level_style = match record.level {
            LogLevel::Debug => Style::default().fg(MUTED),
            LogLevel::Info => Style::default().fg(Color::Gray),
            LogLevel::Warn => Style::default().fg(WARN),
            LogLevel::Error => Style::default().fg(Color::Red),
        };
        let row = Rect::new(
            list.x,
            list.y.saturating_add(visible_index as u16),
            list.width,
            1,
        );
        Paragraph::new(Line::from(vec![
            Span::styled(
                if selected == Some(position) {
                    "▌ "
                } else {
                    "  "
                },
                Style::default().fg(ACCENT),
            ),
            Span::styled(
                format!("{:<13} ", record.timestamp_unix_ms),
                Style::default().fg(MUTED),
            ),
            Span::styled(
                format!("{:<5} ", log_level_title(record.level)),
                level_style,
            ),
            Span::styled(
                format!("{:<7} ", log_source_title(record.source)),
                Style::default().fg(ACCENT),
            ),
            Span::raw(terminal_safe(&record.message)),
        ]))
        .style(if selected == Some(position) {
            Style::default().bg(SELECTED_LOG_BACKGROUND)
        } else {
            Style::default()
        })
        .render(row, buffer);
    }

    if let (Some(detail), Some(selected)) = (regions.detail, selected)
        && let Some(record) = row_indices
            .get(selected)
            .and_then(|index| state.logs.records.get(*index))
    {
        render_selected_detail(record, detail, buffer);
    }
}

fn render_selected_detail(record: &super::super::ViewLogRecord, area: Rect, buffer: &mut Buffer) {
    let safe_message = terminal_safe(&record.message);
    render_separator(Rect::new(area.x, area.y, area.width, 1), buffer);
    Paragraph::new(title_line(format!(
        "SELECTED LOG · SOURCE {} · SEQUENCE {}",
        log_source_title(record.source),
        record.sequence
    )))
    .render(
        Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        buffer,
    );
    Paragraph::new(format!(
        "MESSAGE  {}",
        fit_column(safe_message.as_ref(), area.width.saturating_sub(9) as usize)
    ))
    .render(
        Rect::new(area.x, area.y.saturating_add(2), area.width, 1),
        buffer,
    );
}

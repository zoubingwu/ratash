use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use super::super::AppState;
use super::{format_rate, sample_state_title, stream_state_title, title_line};

pub(super) fn render(state: &AppState, area: Rect, buffer: &mut Buffer) {
    render_summary(state, area, buffer);
}

fn render_summary(state: &AppState, area: Rect, buffer: &mut Buffer) {
    let Some(status) = &state.status else {
        Paragraph::new(vec![
            title_line("CONNECTIONS"),
            Line::from("Waiting for aggregate connection telemetry"),
        ])
        .render(area, buffer);
        return;
    };
    let sampled_at = status
        .traffic
        .sampled_at_unix_ms
        .map_or_else(|| "-".to_owned(), |value| value.to_string());
    Paragraph::new(vec![
        title_line(format!("CONNECTIONS · {} ACTIVE", status.connection_count)),
        Line::from(""),
        Line::from(vec![
            Span::raw("Active connections  "),
            Span::styled(
                status.connection_count.to_string(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(format!(
            "Connection stream   {}",
            stream_state_title(status.stream_health.connections)
        )),
        Line::from(format!(
            "Traffic stream      {}",
            stream_state_title(status.stream_health.traffic)
        )),
        Line::from(format!(
            "Traffic sample      {} at {sampled_at}",
            sample_state_title(status.traffic.state)
        )),
        Line::from(""),
        Line::from(format!(
            "Download            {}",
            format_rate(status.traffic.download_bytes_per_second)
        )),
        Line::from(format!(
            "Upload              {}",
            format_rate(status.traffic.upload_bytes_per_second)
        )),
        Line::from(""),
        Line::from("Aggregate connection telemetry from the Managed Core."),
        Line::from("Per-connection records require a bounded Core projection."),
    ])
    .render(area, buffer);
}

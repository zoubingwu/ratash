use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Paragraph, Widget};

use crate::domain::ConnectionRecord;

use super::super::{AppState, Focus, visible_window_start};
use super::layout::LayoutRegions;
use super::{MUTED, fit_column, format_bytes, selected_style, terminal_safe, title_line};

pub(super) fn render(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    let Some(status) = &state.status else {
        Paragraph::new(vec![
            title_line("CONNECTIONS"),
            Line::from("Waiting for active connection telemetry"),
        ])
        .render(regions.content, buffer);
        return;
    };
    let shown = status.connections.len();
    Paragraph::new(title_line(format!(
        "CONNECTIONS · {} ACTIVE · {shown} SHOWN",
        status.connection_count
    )))
    .render(
        Rect::new(
            regions.content.x,
            regions.content.y,
            regions.content.width,
            1,
        ),
        buffer,
    );

    let Some(list) = regions.list else {
        return;
    };
    let density = ConnectionRowDensity::for_width(list.width);
    Paragraph::new(connection_header(density))
        .style(Style::default().fg(MUTED))
        .render(Rect::new(list.x, list.y, list.width, 1), buffer);

    if status.connections.is_empty() {
        Paragraph::new("No active connections.").render(
            Rect::new(list.x, list.y.saturating_add(1), list.width, 1),
            buffer,
        );
        return;
    }

    let visible = list.height.saturating_sub(1) as usize;
    let offset = visible_window_start(
        state.connections.scroll,
        state.connections.selected,
        visible,
        status.connections.len(),
    );
    for (visible_index, connection) in status
        .connections
        .iter()
        .skip(offset)
        .take(visible)
        .enumerate()
    {
        let index = offset + visible_index;
        let selected = index == state.connections.selected;
        let cursor = if selected { "▌" } else { " " };
        let target = connection_target(connection);
        let rule = connection_rule(connection);
        let chain = if connection.chains.is_empty() {
            "-".to_owned()
        } else {
            connection.chains.join(" → ")
        };
        let line = match density {
            ConnectionRowDensity::Compact => format!(
                "{cursor} {} {} {}",
                fit_column(&target, 31),
                fit_column(&rule, 22),
                fit_column(&chain, 20),
            ),
            ConnectionRowDensity::Standard => format!(
                "{cursor} {} {} {} {}",
                fit_column(&target, 30),
                fit_column(&rule, 24),
                fit_column(&chain, 22),
                fit_column(&connection_traffic(connection), 17),
            ),
            ConnectionRowDensity::Extended => format!(
                "{cursor} {} {} {} {} {} {}",
                fit_column(&target, 34),
                fit_column(&connection.network, 8),
                fit_column(&rule, 30),
                fit_column(&chain, 28),
                fit_column(&format_bytes(connection.download_bytes), 10),
                fit_column(&format_bytes(connection.upload_bytes), 10),
            ),
        };
        Paragraph::new(terminal_safe(&line))
            .style(if selected {
                selected_style(state.focus == Focus::Content)
            } else {
                Style::default()
            })
            .render(
                Rect::new(
                    list.x,
                    list.y.saturating_add(1 + visible_index as u16),
                    list.width,
                    1,
                ),
                buffer,
            );
    }
}

#[derive(Clone, Copy)]
enum ConnectionRowDensity {
    Compact,
    Standard,
    Extended,
}

impl ConnectionRowDensity {
    fn for_width(width: u16) -> Self {
        if width >= 125 {
            Self::Extended
        } else if width >= 96 {
            Self::Standard
        } else {
            Self::Compact
        }
    }
}

fn connection_header(density: ConnectionRowDensity) -> &'static str {
    match density {
        ConnectionRowDensity::Compact => {
            "  TARGET                          RULE                   CHAIN"
        }
        ConnectionRowDensity::Standard => {
            "  TARGET                         RULE                     CHAIN                  TRAFFIC"
        }
        ConnectionRowDensity::Extended => {
            "  TARGET                              NETWORK  RULE                           CHAIN                        DOWNLOAD   UPLOAD"
        }
    }
}

fn connection_target(connection: &ConnectionRecord) -> String {
    let target = connection
        .host
        .as_deref()
        .or(connection.destination_ip.as_deref())
        .unwrap_or("-");
    connection
        .destination_port
        .as_ref()
        .map_or_else(|| target.to_owned(), |port| format!("{target}:{port}"))
}

fn connection_rule(connection: &ConnectionRecord) -> String {
    connection.rule_payload.as_ref().map_or_else(
        || connection.rule.clone(),
        |payload| format!("{} · {payload}", connection.rule),
    )
}

fn connection_traffic(connection: &ConnectionRecord) -> String {
    format!(
        "↓{} ↑{}",
        format_bytes(connection.download_bytes),
        format_bytes(connection.upload_bytes)
    )
}

use std::borrow::Cow;

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use super::super::{AppState, Focus, ProxyRow};
use super::layout::{LayoutRegions, proxy_group_offset, proxy_sort_areas};
use super::{
    ACCENT, MUTED, WARN, fit_column, fit_node_name, render_vertical_separator, selected_style,
    terminal_safe, title_line,
};
use crate::tui::visible_window_start;

pub(super) fn render(state: &AppState, regions: &LayoutRegions, buffer: &mut Buffer) {
    let row_indices = regions.proxy_row_indices();
    render_title(state, regions, row_indices.len(), buffer);
    if let Some(groups) = regions.proxy_groups {
        render_groups(state, groups, buffer);
        if let Some(list) = regions.list {
            render_vertical_separator(
                Rect::new(groups.right(), groups.y, 1, groups.height),
                buffer,
            );
            render_nodes(state, row_indices, list, buffer);
        }
    } else if let Some(list) = regions.list {
        render_nodes(state, row_indices, list, buffer);
    }
}

fn render_title(
    state: &AppState,
    regions: &LayoutRegions,
    visible_count: usize,
    buffer: &mut Buffer,
) {
    let Some(area) = regions.search else {
        return;
    };
    let title = if regions.list.is_some() {
        format!(
            "Nodes ({}) · {}",
            visible_count,
            state
                .proxies
                .selected_group
                .as_deref()
                .map_or_else(|| Cow::Borrowed("unconfigured"), terminal_safe)
        )
    } else {
        format!("PROXY GROUPS ({})", state.proxies.groups.len())
    };
    Paragraph::new(title_line(title)).render(area, buffer);

    if regions.list.is_some() {
        let sort_areas = proxy_sort_areas(area);
        let sort_start = sort_areas.first().map_or(area.right(), |(_, rect)| rect.x);
        let query_x = area.x.saturating_add(30).min(sort_start);
        let query_area = Rect::new(query_x, area.y, sort_start.saturating_sub(query_x), 1);
        Paragraph::new(format!("/{}", terminal_safe(&state.proxies.filter)))
            .style(if state.focus == Focus::Search {
                Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            })
            .render(query_area, buffer);
        for (sort, sort_area) in sort_areas {
            let style = if sort == state.proxies.sort {
                Style::default().fg(Color::Black).bg(ACCENT)
            } else {
                Style::default().fg(Color::Gray)
            };
            Paragraph::new(format!(" {} ", sort.title()))
                .style(style)
                .render(sort_area, buffer);
        }
    }
}

fn render_groups(state: &AppState, area: Rect, buffer: &mut Buffer) {
    Paragraph::new(Line::from(vec![Span::styled(
        "PROXY GROUPS",
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD),
    )]))
    .render(Rect::new(area.x, area.y, area.width, 1), buffer);
    let offset = proxy_group_offset(&state.proxies, area.height.saturating_sub(1) as usize);
    for (visible_index, group) in state
        .proxies
        .groups
        .iter()
        .skip(offset)
        .take(area.height.saturating_sub(1) as usize)
        .enumerate()
    {
        let index = offset + visible_index;
        let focused = index == state.proxies.group_cursor;
        let current = state.proxies.selected_group.as_deref() == Some(group.name.as_str());
        let pending = state
            .proxies
            .group_load_pending
            .as_ref()
            .is_some_and(|load| load.group_id == group.id);
        let marker = match (current, pending) {
            (true, true) => "◉ ",
            (true, false) => "● ",
            (false, true) => "◌ ",
            (false, false) => "",
        };
        let line = format!(
            "{}{}{} · {}",
            if focused { "▌" } else { " " },
            marker,
            terminal_safe(&group.name),
            terminal_safe(&group.proxy_type),
        );
        Paragraph::new(line)
            .style(if focused {
                selected_style(state.focus == Focus::ProxyGroups)
            } else if current {
                Style::default().fg(ACCENT)
            } else {
                Style::default()
            })
            .render(
                Rect::new(
                    area.x,
                    area.y.saturating_add(1 + visible_index as u16),
                    area.width,
                    1,
                ),
                buffer,
            );
    }
}

fn render_nodes(state: &AppState, row_indices: &[usize], area: Rect, buffer: &mut Buffer) {
    let density = NodeRowDensity::for_width(area.width);
    let header = node_header(density);
    Paragraph::new(header)
        .style(Style::default().fg(MUTED))
        .render(Rect::new(area.x, area.y, area.width, 1), buffer);
    let visible = area.height.saturating_sub(1) as usize;
    let offset = visible_window_start(
        state.proxies.scroll,
        state.proxies.selected,
        visible,
        row_indices.len(),
    );
    for (visible_index, row_index_in_state) in row_indices
        .iter()
        .copied()
        .skip(offset)
        .take(area.height.saturating_sub(1) as usize)
        .enumerate()
    {
        let row = &state.proxies.rows[row_index_in_state];
        let index = offset + visible_index;
        let pending =
            state
                .proxies
                .selection_pending
                .as_ref()
                .is_some_and(|(group_id, node_id)| {
                    group_id == &row.group_id && row.node_id.as_ref() == Some(node_id)
                });
        let marker = match (row.selected, pending) {
            (true, true) => "◉",
            (true, false) => "●",
            (false, true) => "◌",
            (false, false) => " ",
        };
        let availability = if row.available {
            "ready"
        } else {
            "unavailable"
        };
        let delay = delay_label(row);
        let cursor = if index == state.proxies.selected {
            "▌"
        } else {
            " "
        };
        let line = match density {
            NodeRowDensity::Compact => format!(
                "{cursor}{marker} {} {} {}",
                fit_node_name(&row.name, 28),
                fit_column(availability, 11),
                fit_column(&delay, 10),
            ),
            NodeRowDensity::Standard => format!(
                "{cursor}{marker} {} {} {} {}",
                fit_node_name(&row.name, 28),
                fit_column(&row.node_type, 12),
                fit_column(availability, 11),
                fit_column(&delay, 10),
            ),
        };
        Paragraph::new(line)
            .style(proxy_row_style(
                row,
                index == state.proxies.selected,
                state.focus == Focus::Content,
            ))
            .render(
                Rect::new(
                    area.x,
                    area.y.saturating_add(1 + visible_index as u16),
                    area.width,
                    1,
                ),
                buffer,
            );
    }
    if state.proxies.group_load_pending.is_some() && state.proxies.rows.is_empty() {
        Paragraph::new("Loading Proxy Group…")
            .style(Style::default().fg(WARN))
            .render(
                Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
                buffer,
            );
    }
}

#[derive(Clone, Copy)]
enum NodeRowDensity {
    Compact,
    Standard,
}

impl NodeRowDensity {
    fn for_width(width: u16) -> Self {
        if width >= 65 {
            Self::Standard
        } else {
            Self::Compact
        }
    }
}

fn node_header(density: NodeRowDensity) -> &'static str {
    match density {
        NodeRowDensity::Compact => "  NODE                         STATUS      LATENCY   ",
        NodeRowDensity::Standard => {
            "  NODE                         TYPE         STATUS      LATENCY   "
        }
    }
}

fn proxy_row_style(row: &ProxyRow, selected: bool, focused: bool) -> Style {
    if selected {
        selected_style(focused)
    } else if !row.available {
        Style::default().fg(Color::Red)
    } else if row.selected {
        Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    }
}

fn delay_label(row: &ProxyRow) -> String {
    row.delay_ms
        .map_or_else(|| "-".to_owned(), |delay| format!("{delay} ms"))
}

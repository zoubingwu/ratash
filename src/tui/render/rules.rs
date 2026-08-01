use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use crate::application::PolicyTargetValidation;

use super::super::{AppState, Focus};
use super::{
    ACCENT, MUTED, WARN, fit_column, policy_validation_title, selected_style, terminal_safe,
    title_line,
};

pub(super) fn render(
    state: &AppState,
    regions: &super::layout::LayoutRegions,
    buffer: &mut Buffer,
) {
    let (Some(title), Some(list)) = (regions.search, regions.list) else {
        return;
    };
    let row_indices = regions.rule_row_indices();
    let count = row_indices.len();
    let revision = state
        .rules
        .revision
        .map_or_else(|| "-".to_owned(), |revision| revision.0.to_string());
    Paragraph::new(title_line(format!("RULES ({count}) · REVISION {revision}")))
        .render(title, buffer);
    let query_x = title.x.saturating_add(34).min(title.right());
    Paragraph::new(format!("/{}", terminal_safe(&state.rules.filter)))
        .style(if state.focus == Focus::Search {
            Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Gray)
        })
        .render(
            Rect::new(query_x, title.y, title.right().saturating_sub(query_x), 1),
            buffer,
        );

    let detail_height = if !row_indices.is_empty() && list.height >= 6 {
        3
    } else {
        0
    };
    let table = Rect::new(
        list.x,
        list.y,
        list.width,
        list.height.saturating_sub(detail_height),
    );
    let compact = table.width < 100;
    Paragraph::new(if compact {
        "  #     TYPE             VALUE                              TARGET"
    } else {
        "  #     TYPE             VALUE                                      TARGET          STATUS"
    })
    .style(Style::default().fg(MUTED))
    .render(Rect::new(table.x, table.y, table.width, 1), buffer);
    if state.rules.load_pending.is_some() {
        Paragraph::new("Loading Local Rule Set…")
            .style(Style::default().fg(WARN))
            .render(
                Rect::new(table.x, table.y.saturating_add(1), table.width, 1),
                buffer,
            );
        return;
    }
    if let Some(error) = &state.rules.load_error {
        Paragraph::new(format!("Rule load failed: {}", terminal_safe(error)))
            .style(Style::default().fg(Color::Red))
            .render(
                Rect::new(table.x, table.y.saturating_add(1), table.width, 1),
                buffer,
            );
        return;
    }
    if !state.rules.initialized {
        Paragraph::new("Local Rule Set is uninitialized.")
            .style(Style::default().fg(WARN))
            .render(
                Rect::new(table.x, table.y.saturating_add(1), table.width, 1),
                buffer,
            );
        return;
    }

    let offset = state.rules.scroll.min(state.rules.selected);
    for (visible_index, row_index_in_state) in row_indices
        .iter()
        .copied()
        .skip(offset)
        .take(table.height.saturating_sub(1) as usize)
        .enumerate()
    {
        let row = &state.rules.rows[row_index_in_state];
        let selected = offset + visible_index == state.rules.selected;
        let ordinal = row.index.saturating_add(1);
        let value = row.payload.as_deref().unwrap_or("-");
        let mut spans = vec![
            Span::raw(if selected { "▌ " } else { "  " }),
            Span::raw(format!("{} ", fit_column(&format!("{ordinal:04}"), 5))),
            Span::raw(format!("{} ", fit_column(&row.rule_type, 16))),
            Span::raw(format!(
                "{} ",
                fit_column(value, if compact { 34 } else { 42 })
            )),
            Span::styled(
                fit_column(&row.policy_target, 15),
                validation_style(row.policy_target_validation),
            ),
        ];
        if !compact {
            spans.push(Span::styled(
                policy_validation_title(row.policy_target_validation),
                validation_style(row.policy_target_validation),
            ));
        }
        let line = Line::from(spans);
        Paragraph::new(line)
            .style(if selected {
                selected_style(state.focus == Focus::Content)
            } else {
                Style::default()
            })
            .render(
                Rect::new(
                    table.x,
                    table.y.saturating_add(1 + visible_index as u16),
                    table.width,
                    1,
                ),
                buffer,
            );
    }

    if state.rules.initialized && state.rules.rows.is_empty() {
        Paragraph::new(Line::from(vec![
            Span::styled("✓ ", Style::default().fg(ACCENT)),
            Span::raw("Local Rule Set contains zero rules."),
        ]))
        .render(
            Rect::new(table.x, table.y.saturating_add(1), table.width, 1),
            buffer,
        );
    }

    if detail_height > 0
        && let Some(row_index) = row_indices.get(state.rules.selected)
    {
        let row = &state.rules.rows[*row_index];
        let ordinal = row.index.saturating_add(1);
        let detail = Rect::new(table.x, table.bottom(), table.width, detail_height);
        Paragraph::new(title_line(format!("SELECTED · RULE {ordinal}")))
            .render(Rect::new(detail.x, detail.y, detail.width, 1), buffer);
        Paragraph::new(terminal_safe(&row.rule_string).as_ref()).render(
            Rect::new(detail.x, detail.y.saturating_add(1), detail.width, 1),
            buffer,
        );
        Paragraph::new(format!(
            "Effective position {} · local revision {} · target {}",
            ordinal,
            revision,
            policy_validation_title(row.policy_target_validation)
        ))
        .style(Style::default().fg(MUTED))
        .render(
            Rect::new(detail.x, detail.y.saturating_add(2), detail.width, 1),
            buffer,
        );
    }
}

fn validation_style(validation: PolicyTargetValidation) -> Style {
    match validation {
        PolicyTargetValidation::Valid => Style::default().fg(ACCENT),
        PolicyTargetValidation::Missing => Style::default().fg(Color::Red),
        PolicyTargetValidation::Unavailable => Style::default().fg(WARN),
    }
}

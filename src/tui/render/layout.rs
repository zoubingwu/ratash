use ratatui::layout::{Constraint, Direction, Layout, Rect};

use crate::constants::{MINIMUM_TERMINAL_HEIGHT, MINIMUM_TERMINAL_WIDTH};

use super::super::{
    AppState, Focus, Interaction, InteractionMap, LogLevelFilter, Modal, Page, ProxiesState,
    ProxySort, ScrollInteraction, UiIntent, filtered_log_indices, filtered_palette_actions,
    filtered_profiles, filtered_proxy_indices, filtered_rule_indices, selected_log_position,
    visible_log_start, visible_window_start,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LayoutRegions {
    pub area: Rect,
    pub status: Rect,
    pub navigation: Rect,
    pub header_separator: Rect,
    pub content: Rect,
    pub footer_separator: Rect,
    pub footer: Rect,
    pub proxy_groups: Option<Rect>,
    pub search: Option<Rect>,
    pub list: Option<Rect>,
    pub detail: Option<Rect>,
    pub modal: Option<Rect>,
    pub minimum_size: bool,
    proxy_row_indices: Vec<usize>,
    rule_row_indices: Vec<usize>,
    log_row_indices: Vec<usize>,
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
                status: Rect::default(),
                navigation: Rect::default(),
                header_separator: Rect::default(),
                content: area,
                footer_separator: Rect::default(),
                footer: Rect::default(),
                proxy_groups: None,
                search: None,
                list: None,
                detail: None,
                modal: None,
                minimum_size: true,
                proxy_row_indices: Vec::new(),
                rule_row_indices: Vec::new(),
                log_row_indices: Vec::new(),
            },
            InteractionMap {
                frame_revision,
                interactions: Vec::new(),
                scroll_regions: Vec::new(),
            },
        );
    }

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Min(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);
    let content = rows[3];
    let proxy_row_indices = if state.page == Page::Proxies {
        filtered_proxy_indices(&state.proxies)
    } else {
        Vec::new()
    };
    let log_row_indices = if state.page == Page::Logs {
        filtered_log_indices(&state.logs)
    } else {
        Vec::new()
    };
    let (proxy_groups, search, list, detail) = page_regions(state, content, log_row_indices.len());
    let rule_row_indices = if state.page == Page::Rules && state.rules_projection_ready() {
        filtered_rule_indices(&state.rules)
    } else {
        Vec::new()
    };
    let modal = state
        .modal
        .as_ref()
        .map(|modal| bottom_sheet(rows[4].y, area, modal));
    let regions = LayoutRegions {
        area,
        status: rows[0],
        navigation: rows[1],
        header_separator: rows[2],
        content,
        footer_separator: rows[4],
        footer: rows[5],
        proxy_groups,
        search,
        list,
        detail,
        modal,
        minimum_size: false,
        proxy_row_indices,
        rule_row_indices,
        log_row_indices,
    };
    let map = interaction_map(state, &regions, frame_revision);
    (regions, map)
}

impl LayoutRegions {
    pub(super) fn proxy_row_indices(&self) -> &[usize] {
        &self.proxy_row_indices
    }

    pub(super) fn rule_row_indices(&self) -> &[usize] {
        &self.rule_row_indices
    }

    pub(super) fn log_row_indices(&self) -> &[usize] {
        &self.log_row_indices
    }
}

fn page_regions(
    state: &AppState,
    content: Rect,
    filtered_log_count: usize,
) -> (Option<Rect>, Option<Rect>, Option<Rect>, Option<Rect>) {
    match state.page {
        Page::Connections => {
            let (_, list) = title_and_list(content, 1);
            (None, None, Some(list), None)
        }
        Page::Proxies => proxy_regions(state, content),
        Page::Rules => {
            let (search, list) = title_and_list(content, 1);
            (None, Some(search), Some(list), None)
        }
        Page::Logs => {
            let (controls, body) = title_and_list(content, 1);
            let (list, detail) = log_regions(state, body, filtered_log_count);
            (None, Some(log_query_area(controls)), Some(list), detail)
        }
    }
}

fn log_regions(state: &AppState, body: Rect, filtered_log_count: usize) -> (Rect, Option<Rect>) {
    if body.height < 6 || selected_log_position(&state.logs, filtered_log_count).is_none() {
        return (body, None);
    }
    let detail_height = 3;
    let list = Rect::new(
        body.x,
        body.y,
        body.width,
        body.height.saturating_sub(detail_height),
    );
    let detail = Rect::new(
        body.x,
        list.bottom(),
        body.width,
        detail_height.min(body.height),
    );
    (list, Some(detail))
}

fn proxy_regions(
    state: &AppState,
    content: Rect,
) -> (Option<Rect>, Option<Rect>, Option<Rect>, Option<Rect>) {
    let (title, body) = title_and_list(content, 1);
    if state.zoomed_focus {
        return if state.focus == Focus::ProxyGroups {
            (Some(content), None, None, None)
        } else {
            (None, Some(title), Some(body), None)
        };
    }
    if content.width < 90 {
        return if state.focus == Focus::ProxyGroups {
            (Some(content), None, None, None)
        } else {
            (None, Some(title), Some(body), None)
        };
    }

    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(22),
            Constraint::Length(1),
            Constraint::Min(30),
        ])
        .split(body);
    (Some(columns[0]), Some(title), Some(columns[2]), None)
}

fn title_and_list(content: Rect, title_height: u16) -> (Rect, Rect) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(title_height), Constraint::Min(1)])
        .split(content);
    (rows[0], rows[1])
}

fn bottom_sheet(bottom: u16, area: Rect, modal: &Modal) -> Rect {
    let requested_height = match modal {
        Modal::CommandPalette => 8,
        Modal::Profiles => 14,
        Modal::RuleEditor { .. }
        | Modal::RuleRemovalConfirmation { .. }
        | Modal::LifecycleConfirmation { .. } => 6,
        Modal::Help | Modal::Message { .. } => 13,
    };
    let available = bottom.saturating_sub(area.y).saturating_sub(2);
    let height = requested_height.min(available.max(6));
    Rect::new(area.x, bottom.saturating_sub(height), area.width, height)
}

pub(super) fn navigation_items(area: Rect) -> Vec<(Page, Rect)> {
    let mut x = area.x.saturating_add(1);
    Page::ALL
        .iter()
        .copied()
        .filter_map(|page| {
            let width = (page.title().len() as u16).saturating_add(4);
            let remaining = area.right().saturating_sub(x);
            if remaining == 0 {
                return None;
            }
            let item = Rect::new(x, area.y, width.min(remaining), 1);
            x = x.saturating_add(width).saturating_add(1);
            Some((page, item))
        })
        .collect()
}

pub(super) fn command_palette_area(area: Rect) -> Rect {
    let width = 12_u16.min(area.width);
    Rect::new(area.right().saturating_sub(width), area.y, width, 1)
}

pub(super) fn footer_help_area(area: Rect) -> Rect {
    Rect::new(area.x, area.y, 8_u16.min(area.width), 1)
}

pub(super) fn footer_quit_area(area: Rect) -> Rect {
    let width = 8_u16.min(area.width);
    Rect::new(area.right().saturating_sub(width), area.y, width, 1)
}

pub(super) fn footer_controls_visible(state: &AppState) -> bool {
    state.modal.is_none() && state.focus != Focus::Search
}

pub(super) fn sheet_close_area(area: Rect) -> Rect {
    Rect::new(
        area.x,
        area.bottom().saturating_sub(1),
        12_u16.min(area.width),
        1,
    )
}

pub(super) fn sheet_action_area(area: Rect) -> Rect {
    let width = 16_u16.min(area.width);
    Rect::new(
        area.right().saturating_sub(width),
        area.bottom().saturating_sub(1),
        width,
        1,
    )
}

pub(super) fn proxy_sort_areas(area: Rect) -> Vec<(ProxySort, Rect)> {
    let widths = [10_u16, 6, 7];
    let total = widths.iter().sum::<u16>();
    let mut x = area.right().saturating_sub(total);
    ProxySort::ALL
        .iter()
        .copied()
        .zip(widths)
        .map(|(sort, width)| {
            let rect = Rect::new(x, area.y, width.min(area.right().saturating_sub(x)), 1);
            x = x.saturating_add(width);
            (sort, rect)
        })
        .collect()
}

pub(super) fn proxy_group_offset(state: &ProxiesState, visible: usize) -> usize {
    state
        .group_cursor
        .saturating_sub(visible.saturating_sub(1) / 2)
        .min(state.groups.len().saturating_sub(visible))
}

pub(super) fn log_level_areas(area: Rect) -> Vec<(LogLevelFilter, Rect)> {
    let mut x = log_query_area(area).right();
    let width = if area.width < 150 { 3 } else { 7 };
    LogLevelFilter::ALL
        .iter()
        .copied()
        .map(|level| {
            let rect = Rect::new(x, area.y, width.min(area.right().saturating_sub(x)), 1);
            x = x.saturating_add(width);
            (level, rect)
        })
        .collect()
}

pub(super) fn log_query_area(area: Rect) -> Rect {
    let x = area.x.saturating_add(6).min(area.right());
    let requested_width = if area.width < 100 {
        12
    } else if area.width < 150 {
        18
    } else {
        30
    };
    Rect::new(
        x,
        area.y,
        requested_width.min(area.right().saturating_sub(x)),
        1,
    )
}

pub(super) fn log_pause_area(area: Rect) -> Rect {
    let x = log_level_areas(area)
        .last()
        .map_or_else(|| log_query_area(area).right(), |(_, level)| level.right())
        .saturating_add(1)
        .min(area.right());
    let width = if area.width < 150 { 3_u16 } else { 8_u16 };
    Rect::new(x, area.y, width.min(area.right().saturating_sub(x)), 1)
}

pub(super) fn log_follow_area(area: Rect) -> Rect {
    let pause = log_pause_area(area);
    Rect::new(
        pause.right(),
        area.y,
        (if area.width < 150 { 3_u16 } else { 10_u16 })
            .min(area.right().saturating_sub(pause.right())),
        1,
    )
}

pub(super) fn profile_sheet_regions(area: Rect) -> ProfileSheetRegions {
    ProfileSheetRegions {
        separator: Rect::new(area.x, area.y, area.width, 1),
        title: Rect::new(area.x, area.y.saturating_add(1), area.width, 1),
        search: Rect::new(area.x, area.y.saturating_add(2), area.width, 1),
        header: Rect::new(area.x, area.y.saturating_add(3), area.width, 1),
        list: Rect::new(
            area.x,
            area.y.saturating_add(4),
            area.width,
            area.height.saturating_sub(5),
        ),
        close: Rect::new(
            area.right().saturating_sub(11),
            area.bottom().saturating_sub(1),
            11_u16.min(area.width),
            1,
        ),
    }
}

pub(super) struct ProfileSheetRegions {
    pub separator: Rect,
    pub title: Rect,
    pub search: Rect,
    pub header: Rect,
    pub list: Rect,
    pub close: Rect,
}

fn interaction_map(
    state: &AppState,
    regions: &LayoutRegions,
    frame_revision: u64,
) -> InteractionMap {
    let mut interactions = navigation_items(regions.navigation)
        .into_iter()
        .map(|(page, area)| Interaction {
            area,
            intent: UiIntent::SwitchPage(page),
        })
        .collect::<Vec<_>>();
    interactions.push(Interaction {
        area: command_palette_area(regions.navigation),
        intent: UiIntent::OpenCommandPalette,
    });
    let mut scroll_regions = Vec::new();
    if footer_controls_visible(state) {
        interactions.push(Interaction {
            area: footer_help_area(regions.footer),
            intent: UiIntent::ToggleHelp,
        });
        interactions.push(Interaction {
            area: footer_quit_area(regions.footer),
            intent: UiIntent::Quit,
        });
    }

    match state.page {
        Page::Connections => {
            connections_interactions(state, regions, &mut interactions, &mut scroll_regions)
        }
        Page::Proxies => proxy_interactions(state, regions, &mut interactions, &mut scroll_regions),
        Page::Rules => rules_interactions(state, regions, &mut interactions, &mut scroll_regions),
        Page::Logs => logs_interactions(state, regions, &mut interactions, &mut scroll_regions),
    }

    if let (Some(modal), Some(area)) = (&state.modal, regions.modal) {
        interactions.clear();
        scroll_regions.clear();
        match modal {
            Modal::CommandPalette => {
                interactions.push(Interaction {
                    area: sheet_close_area(area),
                    intent: UiIntent::CloseModal,
                });
                for (row, action) in filtered_palette_actions(&state.command_palette)
                    .into_iter()
                    .take(area.height.saturating_sub(3) as usize)
                    .enumerate()
                {
                    interactions.push(Interaction {
                        area: Rect::new(
                            area.x,
                            area.y.saturating_add(2 + row as u16),
                            area.width,
                            1,
                        ),
                        intent: UiIntent::RunPaletteAction(action),
                    });
                }
            }
            Modal::Profiles => {
                let sheet = profile_sheet_regions(area);
                interactions.push(Interaction {
                    area: sheet.search,
                    intent: UiIntent::FocusSearch,
                });
                interactions.push(Interaction {
                    area: sheet.close,
                    intent: UiIntent::CloseModal,
                });
                scroll_regions.push(ScrollInteraction { area: sheet.list });
                let visible = sheet.list.height as usize;
                let offset = visible_window_start(
                    state.profiles.scroll,
                    state.profiles.selected,
                    visible,
                    state.filtered_profile_count(),
                );
                for (row_index, row) in filtered_profiles(&state.profiles)
                    .into_iter()
                    .skip(offset)
                    .take(visible)
                    .enumerate()
                {
                    interactions.push(Interaction {
                        area: Rect::new(
                            sheet.list.x,
                            sheet.list.y.saturating_add(row_index as u16),
                            sheet.list.width,
                            1,
                        ),
                        intent: UiIntent::ActivateProfile(row.id),
                    });
                }
            }
            Modal::RuleEditor { .. } => {
                interactions.push(Interaction {
                    area: sheet_close_area(area),
                    intent: UiIntent::CloseModal,
                });
                if state.rules_projection_ready() && !state.modal_action_pending() {
                    interactions.push(Interaction {
                        area: sheet_action_area(area),
                        intent: UiIntent::SubmitRuleEditor,
                    });
                }
            }
            Modal::RuleRemovalConfirmation { .. } => {
                interactions.push(Interaction {
                    area: sheet_close_area(area),
                    intent: UiIntent::CloseModal,
                });
                if state.rules_projection_ready() && !state.modal_action_pending() {
                    interactions.push(Interaction {
                        area: sheet_action_area(area),
                        intent: UiIntent::ConfirmRuleRemoval,
                    });
                }
            }
            Modal::LifecycleConfirmation { .. } => {
                interactions.push(Interaction {
                    area: sheet_close_area(area),
                    intent: UiIntent::CloseModal,
                });
                if !state.modal_action_pending() {
                    interactions.push(Interaction {
                        area: sheet_action_area(area),
                        intent: UiIntent::ConfirmLifecycleAction,
                    });
                }
            }
            Modal::Help | Modal::Message { .. } => interactions.push(Interaction {
                area: Rect::new(
                    area.right().saturating_sub(11),
                    area.bottom().saturating_sub(1),
                    11_u16.min(area.width),
                    1,
                ),
                intent: UiIntent::CloseModal,
            }),
        }
    }

    InteractionMap {
        frame_revision,
        interactions,
        scroll_regions,
    }
}

fn proxy_interactions(
    state: &AppState,
    regions: &LayoutRegions,
    interactions: &mut Vec<Interaction>,
    scroll_regions: &mut Vec<ScrollInteraction>,
) {
    if regions.list.is_some()
        && let Some(search) = regions.search
    {
        interactions.push(Interaction {
            area: search,
            intent: UiIntent::FocusSearch,
        });
        interactions.extend(
            proxy_sort_areas(search)
                .into_iter()
                .map(|(sort, area)| Interaction {
                    area,
                    intent: UiIntent::SetProxySort(sort),
                }),
        );
    }
    if let Some(groups) = regions.proxy_groups {
        let visible = groups.height.saturating_sub(1) as usize;
        let offset = proxy_group_offset(&state.proxies, visible);
        for (visible_index, group) in state
            .proxies
            .groups
            .iter()
            .skip(offset)
            .take(visible)
            .enumerate()
        {
            interactions.push(Interaction {
                area: Rect::new(
                    groups.x,
                    groups.y.saturating_add(1 + visible_index as u16),
                    groups.width,
                    1,
                ),
                intent: UiIntent::ShowProxyGroup(group.id.clone()),
            });
        }
    }
    if let Some(list) = regions.list {
        scroll_regions.push(ScrollInteraction { area: list });
        if state.proxies.group_load_pending.is_some() {
            return;
        }
        let visible = list.height.saturating_sub(1) as usize;
        let offset = visible_window_start(
            state.proxies.scroll,
            state.proxies.selected,
            visible,
            regions.proxy_row_indices().len(),
        );
        for (row_index, row_index_in_state) in regions
            .proxy_row_indices()
            .iter()
            .copied()
            .skip(offset)
            .take(visible)
            .enumerate()
        {
            let row = &state.proxies.rows[row_index_in_state];
            if !state
                .proxies
                .groups
                .iter()
                .find(|group| group.id == row.group_id)
                .is_some_and(|group| group.selectable)
            {
                continue;
            }
            let Some(node_id) = row.node_id.clone() else {
                continue;
            };
            interactions.push(Interaction {
                area: Rect::new(
                    list.x,
                    list.y.saturating_add(1 + row_index as u16),
                    list.width,
                    1,
                ),
                intent: UiIntent::SelectNode {
                    group_id: row.group_id.clone(),
                    node_id,
                },
            });
        }
    }
}

fn rules_interactions(
    state: &AppState,
    regions: &LayoutRegions,
    interactions: &mut Vec<Interaction>,
    scroll_regions: &mut Vec<ScrollInteraction>,
) {
    if let Some(search) = regions.search {
        interactions.push(Interaction {
            area: search,
            intent: UiIntent::FocusSearch,
        });
    }
    if !state.rules_projection_ready() {
        return;
    }
    if let Some(list) = regions.list {
        let detail_height = if !regions.rule_row_indices().is_empty() && list.height >= 6 {
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
        scroll_regions.push(ScrollInteraction { area: table });
        let visible = table.height.saturating_sub(1) as usize;
        let offset = visible_window_start(
            state.rules.scroll,
            state.rules.selected,
            visible,
            regions.rule_row_indices().len(),
        );
        for visible_index in 0..regions
            .rule_row_indices()
            .len()
            .saturating_sub(offset)
            .min(table.height.saturating_sub(1) as usize)
        {
            interactions.push(Interaction {
                area: Rect::new(
                    table.x,
                    table.y.saturating_add(1 + visible_index as u16),
                    table.width,
                    1,
                ),
                intent: UiIntent::SelectRule(offset + visible_index),
            });
        }
    }
}

fn connections_interactions(
    state: &AppState,
    regions: &LayoutRegions,
    interactions: &mut Vec<Interaction>,
    scroll_regions: &mut Vec<ScrollInteraction>,
) {
    let Some(list) = regions.list else {
        return;
    };
    scroll_regions.push(ScrollInteraction { area: list });
    let total = state.connection_record_count();
    let visible = list.height.saturating_sub(1) as usize;
    let offset = visible_window_start(
        state.connections.scroll,
        state.connections.selected,
        visible,
        total,
    );
    for visible_index in 0..total.saturating_sub(offset).min(visible) {
        interactions.push(Interaction {
            area: Rect::new(
                list.x,
                list.y.saturating_add(1 + visible_index as u16),
                list.width,
                1,
            ),
            intent: UiIntent::SelectConnection(offset + visible_index),
        });
    }
}

fn logs_interactions(
    state: &AppState,
    regions: &LayoutRegions,
    interactions: &mut Vec<Interaction>,
    scroll_regions: &mut Vec<ScrollInteraction>,
) {
    let controls = Rect::new(
        regions.content.x,
        regions.content.y,
        regions.content.width,
        1,
    );
    interactions.extend(
        log_level_areas(controls)
            .into_iter()
            .map(|(level, area)| Interaction {
                area,
                intent: UiIntent::SetLogLevel(level),
            }),
    );
    interactions.push(Interaction {
        area: log_pause_area(controls),
        intent: UiIntent::ToggleLogPause,
    });
    interactions.push(Interaction {
        area: log_follow_area(controls),
        intent: UiIntent::FollowLogs,
    });
    if let Some(search) = regions.search {
        interactions.push(Interaction {
            area: search,
            intent: UiIntent::FocusSearch,
        });
    }
    if let Some(list) = regions.list {
        scroll_regions.push(ScrollInteraction { area: list });
        let filtered_count = regions.log_row_indices().len();
        let start = visible_log_start(&state.logs, filtered_count, list.height as usize);
        for (visible_index, position) in (start..filtered_count)
            .take(list.height as usize)
            .enumerate()
        {
            interactions.push(Interaction {
                area: Rect::new(
                    list.x,
                    list.y.saturating_add(visible_index as u16),
                    list.width,
                    1,
                ),
                intent: UiIntent::SelectLog {
                    tail_offset: filtered_count.saturating_sub(position.saturating_add(1)),
                },
            });
        }
    }
}

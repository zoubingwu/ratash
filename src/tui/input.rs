//! Terminal input translation and rendered interaction contracts.

use crossterm::event::{
    Event as CrosstermEvent, KeyCode, KeyEventKind, KeyModifiers,
    MouseButton as CrosstermMouseButton, MouseEventKind,
};
use ratatui::layout::Rect;

use super::reducer::{UiEvent, UiIntent, filtered_palette_actions};
use super::state::{
    AppState, Focus, LogLevelFilter, Modal, Page, filtered_profiles, filtered_proxies,
    filtered_rules,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KeyInput {
    Character(char),
    Enter,
    Escape,
    Tab,
    BackTab,
    Up,
    Down,
    Left,
    Right,
    Backspace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MouseInputKind {
    LeftClick,
    ScrollUp,
    ScrollDown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MouseInput {
    pub kind: MouseInputKind,
    pub column: u16,
    pub row: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalInput {
    Key(KeyInput),
    Mouse(MouseInput),
}

pub fn from_crossterm_event(event: CrosstermEvent) -> Option<UiEvent> {
    match event {
        CrosstermEvent::Key(event) if event.kind == KeyEventKind::Press => {
            let key = match event.code {
                KeyCode::Char(character) => KeyInput::Character(character),
                KeyCode::Enter => KeyInput::Enter,
                KeyCode::Esc => KeyInput::Escape,
                KeyCode::Tab => KeyInput::Tab,
                KeyCode::BackTab => KeyInput::BackTab,
                KeyCode::Up => KeyInput::Up,
                KeyCode::Down => KeyInput::Down,
                KeyCode::Left => KeyInput::Left,
                KeyCode::Right => KeyInput::Right,
                KeyCode::Backspace => KeyInput::Backspace,
                _ => return None,
            };
            let key = if event.modifiers.contains(KeyModifiers::SHIFT) && key == KeyInput::Tab {
                KeyInput::BackTab
            } else {
                key
            };
            Some(UiEvent::Terminal(TerminalInput::Key(key)))
        }
        CrosstermEvent::Mouse(event) => {
            let kind = match event.kind {
                MouseEventKind::Down(CrosstermMouseButton::Left) => MouseInputKind::LeftClick,
                MouseEventKind::ScrollUp => MouseInputKind::ScrollUp,
                MouseEventKind::ScrollDown => MouseInputKind::ScrollDown,
                _ => return None,
            };
            Some(UiEvent::Terminal(TerminalInput::Mouse(MouseInput {
                kind,
                column: event.column,
                row: event.row,
            })))
        }
        CrosstermEvent::Resize(width, height) => Some(UiEvent::Resize { width, height }),
        _ => None,
    }
}

pub fn input_to_intent(state: &AppState, input: TerminalInput) -> Option<UiIntent> {
    if let TerminalInput::Mouse(mouse) = input {
        return state
            .interaction_map
            .as_ref()
            .and_then(|map| map.intent_for(mouse));
    }
    let TerminalInput::Key(key) = input else {
        return None;
    };

    match state.modal.as_ref() {
        Some(Modal::CommandPalette) => {
            return match key {
                KeyInput::Escape => Some(UiIntent::CloseModal),
                KeyInput::Enter => selected_intent(state),
                KeyInput::Backspace => Some(UiIntent::Backspace),
                KeyInput::Character('j') | KeyInput::Down => Some(UiIntent::MoveDown),
                KeyInput::Character('k') | KeyInput::Up => Some(UiIntent::MoveUp),
                KeyInput::Character(character) => Some(UiIntent::InputCharacter(character)),
                _ => None,
            };
        }
        Some(Modal::Profiles) => {
            if state.focus == Focus::Search {
                return search_key_intent(key);
            }
            return match key {
                KeyInput::Escape | KeyInput::Character('q') => Some(UiIntent::CloseModal),
                KeyInput::Enter => selected_intent(state),
                KeyInput::Tab => Some(UiIntent::FocusNext),
                KeyInput::BackTab => Some(UiIntent::FocusPrevious),
                KeyInput::Character('j') | KeyInput::Down => Some(UiIntent::MoveDown),
                KeyInput::Character('k') | KeyInput::Up => Some(UiIntent::MoveUp),
                KeyInput::Character('/') => Some(UiIntent::FocusSearch),
                _ => None,
            };
        }
        Some(Modal::RuleEditor { .. }) => {
            if !state.rules_projection_ready() {
                return (key == KeyInput::Escape).then_some(UiIntent::CloseModal);
            }
            return match key {
                KeyInput::Escape => Some(UiIntent::CloseModal),
                KeyInput::Enter if !state.modal_action_pending() => {
                    Some(UiIntent::SubmitRuleEditor)
                }
                KeyInput::Backspace => Some(UiIntent::Backspace),
                KeyInput::Character(character) => Some(UiIntent::InputCharacter(character)),
                _ => None,
            };
        }
        Some(Modal::RuleRemovalConfirmation { .. }) => {
            if !state.rules_projection_ready() {
                return (key == KeyInput::Escape).then_some(UiIntent::CloseModal);
            }
            return match key {
                KeyInput::Enter | KeyInput::Character('y') if !state.modal_action_pending() => {
                    Some(UiIntent::ConfirmRuleRemoval)
                }
                KeyInput::Escape | KeyInput::Character('n' | 'q') => Some(UiIntent::CloseModal),
                _ => None,
            };
        }
        Some(Modal::LifecycleConfirmation { .. }) => {
            return match key {
                KeyInput::Enter | KeyInput::Character('y') if !state.modal_action_pending() => {
                    Some(UiIntent::ConfirmLifecycleAction)
                }
                KeyInput::Escape | KeyInput::Character('n' | 'q') => Some(UiIntent::CloseModal),
                _ => None,
            };
        }
        Some(Modal::Help | Modal::Message { .. }) => {
            return match key {
                KeyInput::Escape | KeyInput::Enter | KeyInput::Character('q') => {
                    Some(UiIntent::CloseModal)
                }
                _ => None,
            };
        }
        None => {}
    }
    if state.focus == Focus::Search {
        return search_key_intent(key);
    }

    match key {
        KeyInput::Character('1') => Some(UiIntent::SwitchPage(Page::Proxies)),
        KeyInput::Character('2') => Some(UiIntent::SwitchPage(Page::Connections)),
        KeyInput::Character('3') => Some(UiIntent::SwitchPage(Page::Rules)),
        KeyInput::Character('4') => Some(UiIntent::SwitchPage(Page::Logs)),
        KeyInput::Character('j') | KeyInput::Down if state.focus == Focus::ProxyGroups => {
            Some(UiIntent::NextProxyGroup)
        }
        KeyInput::Character('k') | KeyInput::Up if state.focus == Focus::ProxyGroups => {
            Some(UiIntent::PreviousProxyGroup)
        }
        KeyInput::Character('j') | KeyInput::Down
            if state.page == Page::Logs && state.focus == Focus::Content =>
        {
            Some(UiIntent::MoveDown)
        }
        KeyInput::Character('k') | KeyInput::Up
            if state.page == Page::Logs && state.focus == Focus::Content =>
        {
            Some(UiIntent::MoveUp)
        }
        KeyInput::Character('j') | KeyInput::Down
            if state.page != Page::Logs
                && (state.page != Page::Rules || state.rules_projection_ready()) =>
        {
            Some(UiIntent::MoveDown)
        }
        KeyInput::Character('k') | KeyInput::Up
            if state.page != Page::Logs
                && (state.page != Page::Rules || state.rules_projection_ready()) =>
        {
            Some(UiIntent::MoveUp)
        }
        KeyInput::Enter => selected_intent(state),
        KeyInput::Character('h') | KeyInput::Left => Some(UiIntent::FocusLeft),
        KeyInput::Character('l') | KeyInput::Right => Some(UiIntent::FocusRight),
        KeyInput::Tab => Some(UiIntent::FocusNext),
        KeyInput::BackTab => Some(UiIntent::FocusPrevious),
        KeyInput::Character('/') => Some(UiIntent::FocusSearch),
        KeyInput::Character(':') => Some(UiIntent::OpenCommandPalette),
        KeyInput::Character('p') if state.page != Page::Logs => Some(UiIntent::OpenProfiles),
        KeyInput::Character('?') => Some(UiIntent::ToggleHelp),
        KeyInput::Character('q') => Some(UiIntent::Quit),
        KeyInput::Escape => Some(UiIntent::Escape),
        KeyInput::Character('s') if state.page == Page::Proxies => {
            Some(UiIntent::SetProxySort(state.proxies.sort.next()))
        }
        KeyInput::Character('z') if state.page == Page::Proxies => Some(UiIntent::ToggleZoom),
        KeyInput::Character('r') if state.page == Page::Rules => Some(UiIntent::LoadRules),
        KeyInput::Character('a') if state.page == Page::Rules && state.rules_projection_ready() => {
            Some(UiIntent::OpenRuleAdd)
        }
        KeyInput::Character('x') if state.page == Page::Rules && state.rules_projection_ready() => {
            Some(UiIntent::RequestSelectedRuleRemoval)
        }
        KeyInput::Character('a') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::All))
        }
        KeyInput::Character('d') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Debug))
        }
        KeyInput::Character('i') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Info))
        }
        KeyInput::Character('w') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Warn))
        }
        KeyInput::Character('e') if state.page == Page::Logs => {
            Some(UiIntent::SetLogLevel(LogLevelFilter::Error))
        }
        KeyInput::Character('p') if state.page == Page::Logs => Some(UiIntent::ToggleLogPause),
        KeyInput::Character('f') if state.page == Page::Logs => Some(UiIntent::FollowLogs),
        _ => None,
    }
}

fn search_key_intent(key: KeyInput) -> Option<UiIntent> {
    match key {
        KeyInput::Escape | KeyInput::Enter => Some(UiIntent::Escape),
        KeyInput::Backspace => Some(UiIntent::Backspace),
        KeyInput::Tab => Some(UiIntent::FocusNext),
        KeyInput::BackTab => Some(UiIntent::FocusPrevious),
        KeyInput::Character(character) => Some(UiIntent::InputCharacter(character)),
        _ => None,
    }
}

fn selected_intent(state: &AppState) -> Option<UiIntent> {
    if state.modal == Some(Modal::CommandPalette) {
        return filtered_palette_actions(&state.command_palette)
            .get(state.command_palette.selected)
            .copied()
            .map(UiIntent::RunPaletteAction);
    }
    if state.modal == Some(Modal::Profiles) {
        if state.focus != Focus::Content {
            return None;
        }
        return filtered_profiles(&state.profiles)
            .get(state.profiles.selected)
            .copied()
            .map(|row| UiIntent::ActivateProfile(row.id));
    }
    match state.focus {
        Focus::Tabs => return Some(UiIntent::SwitchPage(state.page)),
        Focus::ProxyGroups => {
            return state
                .proxies
                .groups
                .get(state.proxies.group_cursor)
                .map(|group| UiIntent::ShowProxyGroup(group.id.clone()));
        }
        Focus::FooterHelp => return Some(UiIntent::ToggleHelp),
        Focus::FooterQuit => return Some(UiIntent::Quit),
        Focus::Search | Focus::Modal => return None,
        Focus::Content => {}
    }
    match state.page {
        Page::Proxies if state.proxies.group_load_pending.is_some() => None,
        Page::Proxies => filtered_proxies(&state.proxies)
            .get(state.proxies.selected)
            .filter(|row| {
                state
                    .proxies
                    .groups
                    .iter()
                    .find(|group| group.id == row.group_id)
                    .is_some_and(|group| group.selectable)
            })
            .and_then(|row| {
                row.node_id.clone().map(|node_id| UiIntent::SelectNode {
                    group_id: row.group_id.clone(),
                    node_id,
                })
            }),
        Page::Rules if state.rules_projection_ready() => filtered_rules(&state.rules)
            .get(state.rules.selected)
            .map(|_| UiIntent::OpenSelectedRuleEditor),
        Page::Rules => None,
        Page::Connections => None,
        Page::Logs => Some(UiIntent::ToggleLogPause),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Interaction {
    pub area: Rect,
    pub intent: UiIntent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScrollInteraction {
    pub area: Rect,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InteractionMap {
    pub frame_revision: u64,
    pub interactions: Vec<Interaction>,
    pub scroll_regions: Vec<ScrollInteraction>,
}

impl InteractionMap {
    #[must_use]
    pub fn intent_for(&self, mouse: MouseInput) -> Option<UiIntent> {
        match mouse.kind {
            MouseInputKind::LeftClick => self
                .interactions
                .iter()
                .rev()
                .find(|interaction| contains(interaction.area, mouse.column, mouse.row))
                .map(|interaction| interaction.intent.clone()),
            MouseInputKind::ScrollUp | MouseInputKind::ScrollDown => self
                .scroll_regions
                .iter()
                .any(|interaction| contains(interaction.area, mouse.column, mouse.row))
                .then_some(if mouse.kind == MouseInputKind::ScrollUp {
                    UiIntent::ScrollUp
                } else {
                    UiIntent::ScrollDown
                }),
        }
    }
}

fn contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && column < area.x.saturating_add(area.width)
        && row >= area.y
        && row < area.y.saturating_add(area.height)
}

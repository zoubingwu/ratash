//! State, reducer, and input contracts for the Ratatui Status Interface.

mod event_inbox;
mod input;
mod reducer;
mod render;
mod state;
mod terminal;

pub use event_inbox::{EventBudgets, EventInboxError, EventSource, FairEventInbox};
pub use input::{
    Interaction, InteractionMap, KeyInput, MouseInput, MouseInputKind, ScrollInteraction,
    TerminalInput, from_crossterm_event, input_to_intent,
};
pub(crate) use reducer::status_requires_snapshot_refresh;
pub use reducer::{Command, UiEvent, UiIntent, update};
pub use render::{LayoutRegions, compute_layout, render, render_buffer};
pub use state::{
    AppState, CommandPaletteAction, CommandPaletteState, ConnectionState, ConnectionStatus, Focus,
    FullViewSnapshot, LogLevelFilter, LogsState, Modal, MutationSuccess, Page, PendingOperation,
    PendingOperationKind, PendingProxyGroupLoad, PendingRuleLoad, ProfileRow, ProfilesState,
    ProxiesState, ProxyGroupRow, ProxyGroupSnapshot, ProxyRow, ProxySort, RuleListSnapshot,
    RuleRow, RulesState, ViewLogRecord,
};
pub use terminal::{
    CrosstermControl, TerminalAction, TerminalControl, TerminalSession, TerminalSessionError,
};

use reducer::filtered_palette_actions;
use state::{
    filtered_log_indices, filtered_profiles, filtered_proxy_indices, filtered_rule_indices,
    selected_log_position, visible_log_start,
};

pub const PROFILE_VIEW_CAPACITY: usize = 100;
pub const EVENT_SOURCE_CAPACITY: usize = 256;

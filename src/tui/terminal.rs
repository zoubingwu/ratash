//! Crossterm setup and idempotent terminal restoration.

use std::fmt;
use std::io::{self, Write};

use crossterm::cursor::{Hide, Show};
use crossterm::event::{
    DisableBracketedPaste, DisableFocusChange, DisableMouseCapture, EnableBracketedPaste,
    EnableFocusChange, EnableMouseCapture,
};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};

// -----------------------------------------------------------------------------
// Idempotent terminal cleanup seam and Crossterm adapter
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalAction {
    EnableRawMode,
    DisableRawMode,
    EnterAlternateScreen,
    LeaveAlternateScreen,
    EnableMouseCapture,
    DisableMouseCapture,
    EnableFocusReporting,
    DisableFocusReporting,
    EnableBracketedPaste,
    DisableBracketedPaste,
    HideCursor,
    ShowCursor,
}

pub trait TerminalControl {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()>;
}

pub struct TerminalSession<'a> {
    control: &'a mut dyn TerminalControl,
    cleanup_actions: Vec<TerminalAction>,
    cleaned: bool,
}

impl<'a> TerminalSession<'a> {
    pub fn enter(control: &'a mut dyn TerminalControl) -> Result<Self, TerminalSessionError> {
        let mut session = Self {
            control,
            cleanup_actions: Vec::with_capacity(6),
            cleaned: false,
        };
        for (enable, cleanup) in [
            (
                TerminalAction::EnableRawMode,
                TerminalAction::DisableRawMode,
            ),
            (
                TerminalAction::EnterAlternateScreen,
                TerminalAction::LeaveAlternateScreen,
            ),
            (
                TerminalAction::EnableMouseCapture,
                TerminalAction::DisableMouseCapture,
            ),
            (
                TerminalAction::EnableFocusReporting,
                TerminalAction::DisableFocusReporting,
            ),
            (
                TerminalAction::EnableBracketedPaste,
                TerminalAction::DisableBracketedPaste,
            ),
            (TerminalAction::HideCursor, TerminalAction::ShowCursor),
        ] {
            session.cleanup_actions.push(cleanup);
            if let Err(source) = session.control.apply(enable) {
                let cleanup_error = session.cleanup().err();
                return Err(TerminalSessionError {
                    failed_action: enable,
                    source,
                    cleanup_error,
                });
            }
        }
        Ok(session)
    }

    pub fn cleanup(&mut self) -> io::Result<()> {
        if self.cleaned {
            return Ok(());
        }
        self.cleaned = true;
        let mut first_error = None;
        while let Some(action) = self.cleanup_actions.pop() {
            if let Err(error) = self.control.apply(action) {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[must_use]
    pub fn is_cleaned(&self) -> bool {
        self.cleaned
    }
}

impl Drop for TerminalSession<'_> {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

#[derive(Debug)]
pub struct TerminalSessionError {
    pub failed_action: TerminalAction,
    pub source: io::Error,
    pub cleanup_error: Option<io::Error>,
}

impl fmt::Display for TerminalSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "terminal initialization failed during {:?}: {}",
            self.failed_action, self.source
        )
    }
}

impl std::error::Error for TerminalSessionError {}

pub struct CrosstermControl<W> {
    writer: W,
}

impl<W> CrosstermControl<W> {
    #[must_use]
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> TerminalControl for CrosstermControl<W> {
    fn apply(&mut self, action: TerminalAction) -> io::Result<()> {
        match action {
            TerminalAction::EnableRawMode => enable_raw_mode(),
            TerminalAction::DisableRawMode => disable_raw_mode(),
            TerminalAction::EnterAlternateScreen => execute!(self.writer, EnterAlternateScreen),
            TerminalAction::LeaveAlternateScreen => execute!(self.writer, LeaveAlternateScreen),
            TerminalAction::EnableMouseCapture => execute!(self.writer, EnableMouseCapture),
            TerminalAction::DisableMouseCapture => execute!(self.writer, DisableMouseCapture),
            TerminalAction::EnableFocusReporting => execute!(self.writer, EnableFocusChange),
            TerminalAction::DisableFocusReporting => execute!(self.writer, DisableFocusChange),
            TerminalAction::EnableBracketedPaste => execute!(self.writer, EnableBracketedPaste),
            TerminalAction::DisableBracketedPaste => execute!(self.writer, DisableBracketedPaste),
            TerminalAction::HideCursor => execute!(self.writer, Hide),
            TerminalAction::ShowCursor => execute!(self.writer, Show),
        }
    }
}

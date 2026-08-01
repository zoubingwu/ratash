//! Shared transport, authorization, and protocol error translation.

use std::io;

use crate::core::{CoreRuntimeError, CoreRuntimeErrorKind};
use crate::ipc::FrameError;

pub(super) fn transport_unavailable(_error: io::Error) -> CoreRuntimeError {
    unavailable_error("Core service IPC endpoint is unavailable")
}

pub(super) fn cancelled_apply_error() -> CoreRuntimeError {
    CoreRuntimeError::new(
        CoreRuntimeErrorKind::ReloadTimeout,
        "Core service Runtime Apply wait was cancelled during Supervisor shutdown",
    )
}

pub(super) fn map_write_error(error: FrameError) -> CoreRuntimeError {
    match error {
        FrameError::Io(error) if is_timeout(&error) => {
            unavailable_error("Core service IPC request timed out")
        }
        FrameError::Io(_) => unavailable_error("Core service IPC request failed"),
        FrameError::Json(_) | FrameError::FrameTooLarge { .. } => {
            protocol_error("Core service IPC request encoding failed")
        }
    }
}

pub(super) fn map_read_error(error: FrameError) -> CoreRuntimeError {
    match error {
        FrameError::Io(error) if is_timeout(&error) => {
            unavailable_error("Core service IPC response timed out")
        }
        FrameError::Io(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::BrokenPipe
            ) =>
        {
            unavailable_error("Core service IPC connection closed")
        }
        FrameError::Io(_) => unavailable_error("Core service IPC response failed"),
        FrameError::Json(_) | FrameError::FrameTooLarge { .. } => {
            protocol_error("Core service IPC response is invalid")
        }
    }
}

fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
    )
}

pub(super) fn authentication_error(diagnostic: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(CoreRuntimeErrorKind::Authentication, diagnostic)
}

pub(super) fn protocol_error(diagnostic: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(CoreRuntimeErrorKind::ProtocolMismatch, diagnostic)
}

pub(super) fn unavailable_error(diagnostic: &'static str) -> CoreRuntimeError {
    CoreRuntimeError::new(CoreRuntimeErrorKind::Unavailable, diagnostic)
}

pub(super) fn unexpected_response() -> CoreRuntimeError {
    protocol_error("Core service IPC response operation mismatch")
}

pub(super) fn safe_io_error(error: io::Error, message: &'static str) -> io::Error {
    io::Error::new(error.kind(), message)
}

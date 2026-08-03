//! Shared benchmark helpers with no sibling-module dependencies.

use std::error::Error;
use std::io;
use std::time::Instant;

use ratash::tui_runtime::ShutdownSignal;

pub(super) fn elapsed_ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1_000.0
}

pub(super) fn ensure_collection_running(signal: &dyn ShutdownSignal) -> Result<(), Box<dyn Error>> {
    if signal.shutdown_requested() {
        Err(invalid("release benchmark collection was interrupted"))
    } else {
        Ok(())
    }
}

pub(super) fn invalid(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

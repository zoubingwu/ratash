//! Foreground IPC adapters for the Status Interface and streaming Core Logs.

use std::collections::VecDeque;
use std::fmt;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use serde::Serialize;

use crate::application::ApplicationError;
use crate::constants::{
    CORE_LOG_LINE_MAX_BYTES, JSON_OUTPUT_MAX_BYTES, LOG_CAPACITY, LOG_SUBSCRIBER_CAPACITY,
};
use crate::contract::{ApiError, JsonEnvelope, SCHEMA_VERSION};
use crate::domain::StatusSnapshot;
use crate::error::{ErrorCode, ProcessExitCode};
use crate::ipc::{LogRecordV1, LogStreamItem, LogTailV1};
use crate::ipc_runtime::{
    IpcClient, IpcStreamCancellation, LogStream, StatusStream, StatusStreamUpdate,
};
use crate::telemetry::{LogLevel, LogSource};
use crate::tui::ViewLogRecord;
use crate::tui_runtime::{
    CancellationToken, LogTail, StatusInterfaceError, StatusInterfaceErrorKind, StatusLogEvent,
    StatusLogEventSource,
};

const SAFE_ERROR_MAX_CHARACTERS: usize = 512;

// -----------------------------------------------------------------------------
// Ratatui status and log event source
// -----------------------------------------------------------------------------

pub struct IpcStatusLogEventSource {
    client: Arc<IpcClient>,
    state: Mutex<EventSourceState>,
    resume: Arc<Mutex<ResumeState>>,
}

#[derive(Default)]
struct EventSourceState {
    active: Option<ActiveConnection>,
    retired: Vec<JoinHandle<()>>,
}

#[derive(Default)]
struct ResumeState {
    status_sequence: Option<u64>,
    log_sequence: Option<u64>,
}

struct ActiveConnection {
    generation: u64,
    control: Arc<ConnectionControl>,
    buffer: Arc<ConnectionBuffer>,
    readers: Vec<JoinHandle<()>>,
}

struct ConnectionControl {
    active: AtomicBool,
    status: IpcStreamCancellation,
    logs: Mutex<Option<IpcStreamCancellation>>,
}

#[derive(Default)]
struct ConnectionBuffer {
    state: Mutex<ConnectionBufferState>,
}

#[derive(Default)]
struct ConnectionBufferState {
    status: Option<StatusSnapshot>,
    logs: VecDeque<ViewLogRecord>,
    covered_through: Option<u64>,
    remote_dropped_total: u64,
    local_dropped_total: u64,
    gap: bool,
    disconnected: bool,
}

impl IpcStatusLogEventSource {
    #[must_use]
    pub fn new(client: Arc<IpcClient>) -> Self {
        Self {
            client,
            state: Mutex::new(EventSourceState::default()),
            resume: Arc::new(Mutex::new(ResumeState::default())),
        }
    }

    fn install_connection(
        &self,
        generation: u64,
        status: StatusStream,
        logs: LogStream,
        cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError> {
        let status_cancellation = status.cancellation();
        let log_cancellation = logs.cancellation();
        let control = Arc::new(ConnectionControl {
            active: AtomicBool::new(true),
            status: status_cancellation,
            logs: Mutex::new(Some(log_cancellation)),
        });
        let buffer = Arc::new(ConnectionBuffer::default());

        let status_reader = spawn_status_reader(
            generation,
            status,
            Arc::clone(&control),
            Arc::clone(&buffer),
            Arc::clone(&self.resume),
        )?;
        let log_reader = match spawn_log_reader(
            generation,
            logs,
            Arc::clone(&self.client),
            Arc::clone(&control),
            Arc::clone(&buffer),
        ) {
            Ok(reader) => reader,
            Err(error) => {
                control.stop();
                let _ = status_reader.join();
                return Err(error);
            }
        };

        if cancellation.is_cancelled() {
            control.stop();
            let _ = status_reader.join();
            let _ = log_reader.join();
            return Err(cancelled_stream_error());
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retire_active(&mut state);
        reap_finished(&mut state.retired);
        state.active = Some(ActiveConnection {
            generation,
            control,
            buffer,
            readers: vec![status_reader, log_reader],
        });
        Ok(())
    }

    fn active_buffer(
        &self,
        generation: u64,
    ) -> Result<Arc<ConnectionBuffer>, StatusInterfaceError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .active
            .as_ref()
            .filter(|active| {
                active.generation == generation && active.control.active.load(Ordering::Acquire)
            })
            .map(|active| Arc::clone(&active.buffer))
            .ok_or_else(disconnected_stream_error)
    }
}

impl StatusLogEventSource for IpcStatusLogEventSource {
    fn connect(
        &self,
        connection_generation: u64,
        cancellation: &CancellationToken,
    ) -> Result<(), StatusInterfaceError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_stream_error());
        }
        self.disconnect_current();
        let (status_sequence, log_sequence) = {
            let resume = self
                .resume
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (resume.status_sequence, resume.log_sequence)
        };
        let status = self
            .client
            .subscribe_status(status_sequence, connection_generation)
            .map_err(status_stream_error)?;
        if cancellation.is_cancelled() {
            status.cancellation().cancel();
            return Err(cancelled_stream_error());
        }
        let logs = match self.client.follow_logs(log_sequence, connection_generation) {
            Ok(logs) => logs,
            Err(error) => {
                status.cancellation().cancel();
                return Err(status_stream_error(error));
            }
        };
        self.install_connection(connection_generation, status, logs, cancellation)
    }

    fn try_next(&self) -> Result<Option<StatusLogEvent>, StatusInterfaceError> {
        let (generation, buffer) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reap_finished(&mut state.retired);
            let Some(active) = state.active.as_ref() else {
                return Ok(None);
            };
            (active.generation, Arc::clone(&active.buffer))
        };
        let event = buffer.take_event(generation);
        if let Some(StatusLogEvent::Logs { records, .. }) = &event
            && let Some(sequence) = records.last().map(|record| record.sequence)
        {
            let mut resume = self
                .resume
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            resume.log_sequence = max_sequence(resume.log_sequence, Some(sequence));
        }
        Ok(event)
    }

    fn fetch_log_tail(
        &self,
        connection_generation: u64,
        after_sequence: Option<u64>,
        cancellation: &CancellationToken,
    ) -> Result<LogTail, StatusInterfaceError> {
        if cancellation.is_cancelled() {
            return Err(cancelled_stream_error());
        }
        let buffer = self.active_buffer(connection_generation)?;
        let tail = self
            .client
            .log_tail(after_sequence)
            .map_err(status_stream_error)?;
        if cancellation.is_cancelled() {
            return Err(cancelled_stream_error());
        }
        let records = convert_log_records(&tail.records)?;
        buffer.apply_tail_coverage(&tail);
        let mut resume = self
            .resume
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        resume.log_sequence = if after_sequence.is_none() {
            tail.latest_sequence
        } else {
            max_sequence(resume.log_sequence, tail.latest_sequence)
        };
        Ok(LogTail {
            records,
            gap: tail.gap,
            dropped_total: tail.dropped_total,
        })
    }

    fn disconnect(&self, connection_generation: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state
            .active
            .as_ref()
            .is_some_and(|active| active.generation == connection_generation)
        {
            retire_active(&mut state);
        }
        reap_finished(&mut state.retired);
    }
}

impl IpcStatusLogEventSource {
    fn disconnect_current(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retire_active(&mut state);
        reap_finished(&mut state.retired);
    }
}

impl Drop for IpcStatusLogEventSource {
    fn drop(&mut self) {
        let (active, retired) = {
            let state = self
                .state
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (state.active.take(), std::mem::take(&mut state.retired))
        };
        let mut readers = retired;
        if let Some(active) = active {
            active.control.stop();
            readers.extend(active.readers);
        }
        for reader in readers {
            let _ = reader.join();
        }
    }
}

impl fmt::Debug for IpcStatusLogEventSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let generation = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .as_ref()
            .map(|active| active.generation);
        formatter
            .debug_struct("IpcStatusLogEventSource")
            .field("connection_generation", &generation)
            .finish_non_exhaustive()
    }
}

impl ConnectionControl {
    fn install_log_stream(&self, cancellation: IpcStreamCancellation) -> bool {
        let mut current = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.active.load(Ordering::Acquire) {
            cancellation.cancel();
            return false;
        }
        *current = Some(cancellation);
        true
    }

    fn fail(&self, buffer: &ConnectionBuffer) {
        if self.active.swap(false, Ordering::AcqRel) {
            self.cancel_streams();
            buffer.mark_disconnected();
        }
    }

    fn stop(&self) {
        self.active.store(false, Ordering::Release);
        self.cancel_streams();
    }

    fn cancel_streams(&self) {
        self.status.cancel();
        if let Some(logs) = self
            .logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            logs.cancel();
        }
    }
}

impl ConnectionBuffer {
    fn publish_status(&self, status: StatusSnapshot) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.disconnected {
            state.status = Some(status);
        }
    }

    fn publish_log(&self, record: ViewLogRecord) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.disconnected
            || state
                .covered_through
                .is_some_and(|covered| record.sequence <= covered)
        {
            return;
        }
        if state.logs.len() == LOG_CAPACITY {
            state.logs.pop_front();
            state.local_dropped_total = state.local_dropped_total.saturating_add(1);
            state.gap = true;
        }
        state.logs.push_back(record);
    }

    fn publish_tail(&self, tail: &LogTailV1) -> Result<(), StatusInterfaceError> {
        let records = convert_log_records(&tail.records)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.disconnected {
            return Ok(());
        }
        state.remote_dropped_total = state.remote_dropped_total.max(tail.dropped_total);
        state.gap |= tail.gap;
        for record in records {
            if state
                .covered_through
                .is_some_and(|covered| record.sequence <= covered)
            {
                continue;
            }
            if state.logs.len() == LOG_CAPACITY {
                state.logs.pop_front();
                state.local_dropped_total = state.local_dropped_total.saturating_add(1);
                state.gap = true;
            }
            state.logs.push_back(record);
        }
        Ok(())
    }

    fn apply_tail_coverage(&self, tail: &LogTailV1) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.covered_through = max_sequence(state.covered_through, tail.latest_sequence);
        if let Some(covered) = state.covered_through {
            state.logs.retain(|record| record.sequence > covered);
        }
        state.remote_dropped_total = tail.dropped_total;
        state.local_dropped_total = 0;
        state.gap = false;
    }

    fn mark_disconnected(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .disconnected = true;
    }

    fn take_event(&self, generation: u64) -> Option<StatusLogEvent> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.disconnected {
            state.disconnected = false;
            return Some(StatusLogEvent::Disconnected {
                connection_generation: generation,
            });
        }
        if let Some(status) = state.status.take() {
            return Some(StatusLogEvent::Status {
                connection_generation: generation,
                status: Box::new(status),
            });
        }
        if state.logs.is_empty() && !state.gap {
            return None;
        }
        let count = state.logs.len().min(LOG_SUBSCRIBER_CAPACITY);
        let records = state.logs.drain(..count).collect();
        let gap = std::mem::take(&mut state.gap);
        Some(StatusLogEvent::Logs {
            connection_generation: generation,
            records,
            gap,
            dropped_total: state
                .remote_dropped_total
                .saturating_add(state.local_dropped_total),
        })
    }
}

fn spawn_status_reader(
    generation: u64,
    mut stream: StatusStream,
    control: Arc<ConnectionControl>,
    buffer: Arc<ConnectionBuffer>,
    resume: Arc<Mutex<ResumeState>>,
) -> Result<JoinHandle<()>, StatusInterfaceError> {
    thread::Builder::new()
        .name(format!("hopash-status-ipc-{generation}"))
        .spawn(move || {
            while control.active.load(Ordering::Acquire) {
                let item = match stream.next_item() {
                    Ok(Some(item)) => item,
                    Ok(None) => {
                        if control.active.load(Ordering::Acquire) {
                            control.fail(&buffer);
                        }
                        return;
                    }
                    Err(_) => {
                        control.fail(&buffer);
                        return;
                    }
                };
                match item.item {
                    StatusStreamUpdate::Snapshot {
                        sequence, snapshot, ..
                    }
                    | StatusStreamUpdate::Delta {
                        sequence, snapshot, ..
                    } => {
                        resume
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .status_sequence = Some(sequence);
                        buffer.publish_status(snapshot);
                    }
                    StatusStreamUpdate::ResyncRequired { .. } => {
                        control.fail(&buffer);
                        return;
                    }
                }
            }
        })
        .map_err(|_| thread_spawn_error())
}

fn spawn_log_reader(
    generation: u64,
    stream: LogStream,
    client: Arc<IpcClient>,
    control: Arc<ConnectionControl>,
    buffer: Arc<ConnectionBuffer>,
) -> Result<JoinHandle<()>, StatusInterfaceError> {
    thread::Builder::new()
        .name(format!("hopash-log-ipc-{generation}"))
        .spawn(move || run_log_reader(generation, stream, &client, &control, &buffer))
        .map_err(|_| thread_spawn_error())
}

fn run_log_reader(
    generation: u64,
    mut stream: LogStream,
    client: &IpcClient,
    control: &ConnectionControl,
    buffer: &ConnectionBuffer,
) {
    let mut after_sequence = stream.resume_after_sequence();
    while control.active.load(Ordering::Acquire) {
        match stream.next_item() {
            Ok(Some(item)) => match item.item {
                LogStreamItem::Record { record } => match convert_log_record(&record) {
                    Ok(record) => {
                        after_sequence = Some(record.sequence);
                        buffer.publish_log(record);
                    }
                    Err(_) => {
                        control.fail(buffer);
                        return;
                    }
                },
                LogStreamItem::Gap { .. } => {
                    let tail = match client.log_tail(after_sequence) {
                        Ok(tail) => tail,
                        Err(_) => {
                            control.fail(buffer);
                            return;
                        }
                    };
                    if buffer.publish_tail(&tail).is_err() {
                        control.fail(buffer);
                        return;
                    }
                    after_sequence = tail.latest_sequence;
                    let next = match client.follow_logs(after_sequence, generation) {
                        Ok(next) => next,
                        Err(_) => {
                            control.fail(buffer);
                            return;
                        }
                    };
                    if !control.install_log_stream(next.cancellation()) {
                        return;
                    }
                    stream = next;
                }
            },
            Ok(None) => {
                if control.active.load(Ordering::Acquire) {
                    control.fail(buffer);
                }
                return;
            }
            Err(_) => {
                match recover_log_stream(generation, after_sequence, client, control, buffer) {
                    Some((next, recovered_after)) => {
                        stream = next;
                        after_sequence = recovered_after;
                    }
                    None => {
                        control.fail(buffer);
                        return;
                    }
                }
            }
        }
    }
}

fn recover_log_stream(
    generation: u64,
    after_sequence: Option<u64>,
    client: &IpcClient,
    control: &ConnectionControl,
    buffer: &ConnectionBuffer,
) -> Option<(LogStream, Option<u64>)> {
    if !control.active.load(Ordering::Acquire) {
        return None;
    }
    let mut tail = client.log_tail(after_sequence).ok()?;
    let sequence_reset = after_sequence
        .zip(tail.latest_sequence)
        .is_some_and(|(after, latest)| latest < after)
        || (after_sequence.is_some() && tail.latest_sequence.is_none());
    if sequence_reset {
        tail = client.log_tail(None).ok()?;
        tail.gap = true;
    }
    buffer.publish_tail(&tail).ok()?;
    let recovered_after = tail.latest_sequence;
    let next = client.follow_logs(recovered_after, generation).ok()?;
    if !control.install_log_stream(next.cancellation()) {
        return None;
    }
    Some((next, recovered_after))
}

fn retire_active(state: &mut EventSourceState) {
    if let Some(active) = state.active.take() {
        active.control.stop();
        state.retired.extend(active.readers);
    }
}

fn reap_finished(readers: &mut Vec<JoinHandle<()>>) {
    let mut index = 0;
    while index < readers.len() {
        if readers[index].is_finished() {
            let reader = readers.swap_remove(index);
            let _ = reader.join();
        } else {
            index += 1;
        }
    }
}

// -----------------------------------------------------------------------------
// Foreground log following
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogFollowFormat {
    Human,
    Ndjson,
}

#[derive(Clone, Default)]
pub struct LogFollowCancellation {
    inner: Arc<LogFollowCancellationInner>,
}

#[derive(Default)]
struct LogFollowCancellationInner {
    cancelled: AtomicBool,
    stream: Mutex<Option<IpcStreamCancellation>>,
}

impl LogFollowCancellation {
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        if let Some(stream) = self
            .inner
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            stream.cancel();
        }
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    fn install(&self, stream: IpcStreamCancellation) -> bool {
        let mut current = self
            .inner
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_cancelled() {
            stream.cancel();
            return false;
        }
        *current = Some(stream);
        true
    }

    fn clear(&self) {
        *self
            .inner
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
}

impl fmt::Debug for LogFollowCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogFollowCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

pub struct ForegroundLogFollower {
    client: Arc<IpcClient>,
}

impl ForegroundLogFollower {
    #[must_use]
    pub fn new(client: Arc<IpcClient>) -> Self {
        Self { client }
    }

    pub fn run(
        &self,
        format: LogFollowFormat,
        stdout: &mut dyn Write,
        stderr: &mut dyn Write,
        cancellation: &LogFollowCancellation,
    ) -> ProcessExitCode {
        match self.follow(format, stdout, cancellation) {
            Ok(LogFollowCompletion::Interrupted) => ProcessExitCode::Interrupted,
            Err(LogFollowError::Application(error)) => write_follow_error(error, format, stderr),
            Err(LogFollowError::Output) => ProcessExitCode::InternalFailure,
        }
    }

    fn follow(
        &self,
        format: LogFollowFormat,
        stdout: &mut dyn Write,
        cancellation: &LogFollowCancellation,
    ) -> Result<LogFollowCompletion, LogFollowError> {
        let mut after_sequence = None;
        let mut generation = 1_u64;
        loop {
            if cancellation.is_cancelled() {
                return Ok(LogFollowCompletion::Interrupted);
            }
            let mut stream = self
                .client
                .follow_logs(after_sequence, generation)
                .map_err(LogFollowError::Application)?;
            if !cancellation.install(stream.cancellation()) {
                return Ok(LogFollowCompletion::Interrupted);
            }
            loop {
                let next = stream.next_item().map_err(LogFollowError::Application)?;
                let Some(next) = next else {
                    cancellation.clear();
                    if cancellation.is_cancelled() {
                        return Ok(LogFollowCompletion::Interrupted);
                    }
                    return Err(LogFollowError::Application(disconnected_application_error()));
                };
                match next.item {
                    LogStreamItem::Record { record } => {
                        write_log_record(&record, format, stdout)?;
                        after_sequence = Some(record.sequence);
                    }
                    LogStreamItem::Gap { .. } => {
                        cancellation.clear();
                        if cancellation.is_cancelled() {
                            return Ok(LogFollowCompletion::Interrupted);
                        }
                        let tail = self
                            .client
                            .log_tail(after_sequence)
                            .map_err(LogFollowError::Application)?;
                        for record in &tail.records {
                            write_log_record(record, format, stdout)?;
                        }
                        after_sequence = tail.latest_sequence;
                        generation = generation.wrapping_add(1).max(1);
                        break;
                    }
                }
                if cancellation.is_cancelled() {
                    return Ok(LogFollowCompletion::Interrupted);
                }
            }
        }
    }
}

impl fmt::Debug for ForegroundLogFollower {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForegroundLogFollower")
            .finish_non_exhaustive()
    }
}

enum LogFollowCompletion {
    Interrupted,
}

enum LogFollowError {
    Application(ApplicationError),
    Output,
}

#[derive(Serialize)]
struct VersionedLogEvent<'a> {
    schema_version: u16,
    event: &'a LogRecordV1,
}

fn write_log_record(
    record: &LogRecordV1,
    format: LogFollowFormat,
    output: &mut dyn Write,
) -> Result<(), LogFollowError> {
    validate_log_record(record).map_err(|()| {
        LogFollowError::Application(ApplicationError::new(
            ErrorCode::ProtocolMismatch,
            "The Supervisor IPC log record is invalid",
            false,
        ))
    })?;
    match format {
        LogFollowFormat::Human => {
            let message = terminal_safe(&record.message, CORE_LOG_LINE_MAX_BYTES);
            writeln!(
                output,
                "{} {:<5} {:<8} {}",
                record.timestamp_unix_ms,
                record.level.to_ascii_uppercase(),
                record.source,
                message
            )
            .map_err(|_| LogFollowError::Output)
        }
        LogFollowFormat::Ndjson => {
            let event = VersionedLogEvent {
                schema_version: SCHEMA_VERSION,
                event: record,
            };
            let encoded = serde_json::to_vec(&event).map_err(|_| LogFollowError::Output)?;
            if encoded.len().saturating_add(1) > JSON_OUTPUT_MAX_BYTES {
                return Err(LogFollowError::Output);
            }
            output
                .write_all(&encoded)
                .and_then(|()| output.write_all(b"\n"))
                .map_err(|_| LogFollowError::Output)
        }
    }
}

fn write_follow_error(
    error: ApplicationError,
    format: LogFollowFormat,
    stderr: &mut dyn Write,
) -> ProcessExitCode {
    let exit = error.code.process_exit_code();
    let written = match format {
        LogFollowFormat::Human => writeln!(
            stderr,
            "Error: {}",
            terminal_safe(&error.message, SAFE_ERROR_MAX_CHARACTERS)
        ),
        LogFollowFormat::Ndjson => {
            let envelope = JsonEnvelope::<serde_json::Value>::failure(ApiError::from(error));
            serde_json::to_vec(&envelope)
                .map_err(std::io::Error::other)
                .and_then(|encoded| stderr.write_all(&encoded))
                .and_then(|()| stderr.write_all(b"\n"))
        }
    };
    if written.is_ok() {
        exit
    } else {
        ProcessExitCode::InternalFailure
    }
}

// -----------------------------------------------------------------------------
// Projection and errors
// -----------------------------------------------------------------------------

fn convert_log_records(
    records: &[LogRecordV1],
) -> Result<Vec<ViewLogRecord>, StatusInterfaceError> {
    records.iter().map(convert_log_record).collect()
}

fn convert_log_record(record: &LogRecordV1) -> Result<ViewLogRecord, StatusInterfaceError> {
    validate_log_record(record).map_err(|()| invalid_log_record_error())?;
    let level = match record.level.as_str() {
        "debug" => LogLevel::Debug,
        "info" => LogLevel::Info,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => unreachable!("validated log level"),
    };
    let source = match record.source.as_str() {
        "core_api" => LogSource::CoreApi,
        "stdout" => LogSource::Stdout,
        "stderr" => LogSource::Stderr,
        _ => unreachable!("validated log source"),
    };
    Ok(ViewLogRecord {
        sequence: record.sequence,
        timestamp_unix_ms: record.timestamp_unix_ms,
        level,
        source,
        message: record.message.clone(),
    })
}

fn validate_log_record(record: &LogRecordV1) -> Result<(), ()> {
    if !matches!(record.level.as_str(), "debug" | "info" | "warn" | "error")
        || !matches!(record.source.as_str(), "core_api" | "stdout" | "stderr")
        || record.message.len() > CORE_LOG_LINE_MAX_BYTES
    {
        return Err(());
    }
    Ok(())
}

fn max_sequence(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (left, right) => left.or(right),
    }
}

fn terminal_safe(value: &str, max_characters: usize) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(max_characters)
        .collect()
}

fn status_stream_error(error: ApplicationError) -> StatusInterfaceError {
    let message = terminal_safe(&error.message, SAFE_ERROR_MAX_CHARACTERS);
    StatusInterfaceError::new(StatusInterfaceErrorKind::Stream, message)
}

fn cancelled_stream_error() -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Stream,
        "The foreground IPC operation was cancelled",
    )
}

fn disconnected_stream_error() -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Stream,
        "The Supervisor IPC stream is disconnected",
    )
}

fn invalid_log_record_error() -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Stream,
        "The Supervisor IPC log record is invalid",
    )
}

fn thread_spawn_error() -> StatusInterfaceError {
    StatusInterfaceError::new(
        StatusInterfaceErrorKind::Stream,
        "The foreground IPC reader could not start",
    )
}

fn disconnected_application_error() -> ApplicationError {
    ApplicationError::new(
        ErrorCode::SupervisorUnavailable,
        "The Supervisor IPC log stream disconnected",
        true,
    )
}

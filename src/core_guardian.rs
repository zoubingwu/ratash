//! Same-binary Core guardian that contains and reaps one verified Mihomo child.

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};
use std::os::fd::AsFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use nix::errno::Errno;
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};

use crate::constants::MIHOMO_BINARY_MAX_BYTES;
use crate::lifecycle::{ProcessInspector, PsProcessInspector};

pub const INTERNAL_CORE_GUARDIAN_MODE: &str = "__core-guardian";
pub const CORE_GUARDIAN_PROTOCOL_VERSION: u16 = 1;

const HANDSHAKE_MAX_BYTES: usize = 1_024;
const START_IDENTITY_MAX_BYTES: usize = 512;
const TERMINATION_GRACE: Duration = Duration::from_millis(250);
const CHILD_EXIT_POLL_INTERVAL: Duration = Duration::from_millis(100);

struct ArmedChild {
    child: Child,
    armed: bool,
    exit_status: Option<ExitStatus>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ControlEvent {
    ParentEof,
    Canceled,
}

struct ControlMonitor {
    cancel: UnixStream,
    events: Receiver<io::Result<ControlEvent>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ControlMonitor {
    fn start(cancel: UnixStream, wake: UnixStream) -> io::Result<Self> {
        let (event_sender, events) = mpsc::sync_channel(1);
        let handle = thread::Builder::new()
            .name("hopash-core-guardian-control".to_owned())
            .spawn(move || {
                let _ = event_sender.send(monitor_control_pipe(cancel));
            })
            .map_err(|_| io::Error::other("the Core guardian monitor could not start"))?;
        Ok(Self {
            cancel: wake,
            events,
            handle: Some(handle),
        })
    }

    fn receive(&self, timeout: Duration) -> io::Result<Option<ControlEvent>> {
        match self.events.recv_timeout(timeout) {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(error)) => Err(error),
            Err(RecvTimeoutError::Timeout) => Ok(None),
            Err(RecvTimeoutError::Disconnected) => {
                Err(io::Error::other("the Core guardian monitor disconnected"))
            }
        }
    }

    fn finish(mut self) -> io::Result<()> {
        self.cancel_and_join()
    }

    fn cancel_and_join(&mut self) -> io::Result<()> {
        let _ = self.cancel.write_all(&[1]);
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle
            .join()
            .map_err(|_| io::Error::other("the Core guardian monitor failed"))
    }
}

impl Drop for ControlMonitor {
    fn drop(&mut self) {
        let _ = self.cancel_and_join();
    }
}

impl ArmedChild {
    fn new(child: Child) -> Self {
        Self {
            child,
            armed: true,
            exit_status: None,
        }
    }

    fn wait(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.exit_status {
            return Ok(status);
        }
        let status = self.child.wait()?;
        self.armed = false;
        self.exit_status = Some(status);
        Ok(status)
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.exit_status {
            return Ok(Some(status));
        }
        let status = self.child.try_wait()?;
        if let Some(status) = status {
            self.armed = false;
            self.exit_status = Some(status);
        }
        Ok(status)
    }

    fn terminate_and_reap(&mut self) -> io::Result<()> {
        if !self.armed {
            return Ok(());
        }
        if let Ok(pid) = i32::try_from(self.child.id()) {
            let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
        }
        let deadline = Instant::now() + TERMINATION_GRACE;
        loop {
            match self.try_wait() {
                Ok(Some(_)) => {
                    return Ok(());
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) | Err(_) => break,
            }
        }
        let _ = self.child.kill();
        self.wait().map(|_| ())
    }
}

impl Deref for ArmedChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl DerefMut for ArmedChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

impl Drop for ArmedChild {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap();
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct CoreGuardianInvocation {
    mihomo: PathBuf,
    mihomo_sha256: String,
    working_directory: PathBuf,
    configuration: PathBuf,
    control_socket: PathBuf,
}

impl CoreGuardianInvocation {
    pub fn new(
        mihomo: PathBuf,
        mihomo_sha256: String,
        working_directory: PathBuf,
        configuration: PathBuf,
        control_socket: PathBuf,
    ) -> io::Result<Self> {
        let invocation = Self {
            mihomo,
            mihomo_sha256,
            working_directory,
            configuration,
            control_socket,
        };
        invocation.validate_arguments()?;
        Ok(invocation)
    }

    pub fn parse_process_arguments(args: &[OsString]) -> io::Result<Option<Self>> {
        if args.get(1).and_then(|value| value.to_str()) != Some(INTERNAL_CORE_GUARDIAN_MODE) {
            return Ok(None);
        }
        if args.len() != 12
            || args.get(2).and_then(|value| value.to_str()) != Some("--mihomo")
            || args.get(4).and_then(|value| value.to_str()) != Some("--mihomo-sha256")
            || args.get(6).and_then(|value| value.to_str()) != Some("--working-directory")
            || args.get(8).and_then(|value| value.to_str()) != Some("--configuration")
            || args.get(10).and_then(|value| value.to_str()) != Some("--control-socket")
        {
            return Err(invalid_invocation());
        }
        let invocation = Self::new(
            PathBuf::from(&args[3]),
            args[5].to_string_lossy().into_owned(),
            PathBuf::from(&args[7]),
            PathBuf::from(&args[9]),
            PathBuf::from(&args[11]),
        )?;
        Ok(Some(invocation))
    }

    pub(crate) fn configure_command(&self, command: &mut Command) {
        command
            .arg(INTERNAL_CORE_GUARDIAN_MODE)
            .arg("--mihomo")
            .arg(&self.mihomo)
            .arg("--mihomo-sha256")
            .arg(&self.mihomo_sha256)
            .arg("--working-directory")
            .arg(&self.working_directory)
            .arg("--configuration")
            .arg(&self.configuration)
            .arg("--control-socket")
            .arg(&self.control_socket);
    }

    fn validate_arguments(&self) -> io::Result<()> {
        if !self.mihomo.is_absolute()
            || !self.working_directory.is_absolute()
            || !self.configuration.is_absolute()
            || !self.control_socket.is_absolute()
            || self.mihomo != self.working_directory.join("mihomo")
            || self.configuration != self.working_directory.join("config.yaml")
            || !valid_sha256(&self.mihomo_sha256)
        {
            return Err(invalid_invocation());
        }
        Ok(())
    }
}

impl fmt::Debug for CoreGuardianInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreGuardianInvocation")
            .field("mihomo", &"[REDACTED]")
            .field("mihomo_sha256", &"[REDACTED]")
            .field("working_directory", &"[REDACTED]")
            .field("configuration", &"[REDACTED]")
            .field("control_socket", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CoreGuardianHandshake {
    protocol_version: u16,
    pid: u32,
    process_start_identity: String,
}

impl CoreGuardianHandshake {
    #[must_use]
    pub const fn pid(&self) -> u32 {
        self.pid
    }

    #[must_use]
    pub fn process_start_identity(&self) -> &str {
        &self.process_start_identity
    }

    fn validate(&self) -> io::Result<()> {
        if self.protocol_version != CORE_GUARDIAN_PROTOCOL_VERSION
            || self.pid == 0
            || self.process_start_identity.is_empty()
            || self.process_start_identity.len() > START_IDENTITY_MAX_BYTES
            || self.process_start_identity.contains(['\r', '\n', '\0'])
        {
            return Err(invalid_handshake());
        }
        Ok(())
    }
}

impl fmt::Debug for CoreGuardianHandshake {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoreGuardianHandshake")
            .field("protocol_version", &self.protocol_version)
            .field("pid", &self.pid)
            .field("process_start_identity", &"[REDACTED]")
            .finish()
    }
}

pub fn read_handshake(reader: &mut impl Read) -> io::Result<CoreGuardianHandshake> {
    let mut length = [0_u8; 4];
    reader.read_exact(&mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| invalid_handshake())?;
    if length == 0 || length > HANDSHAKE_MAX_BYTES {
        return Err(invalid_handshake());
    }
    let mut body = vec![0_u8; length];
    reader.read_exact(&mut body)?;
    let handshake: CoreGuardianHandshake =
        serde_json::from_slice(&body).map_err(|_| invalid_handshake())?;
    handshake.validate()?;
    Ok(handshake)
}

pub fn run_core_guardian(invocation: CoreGuardianInvocation) -> io::Result<()> {
    run_core_guardian_with_handshake_writer(invocation, |handshake| {
        write_handshake(&mut io::stdout().lock(), handshake)
    })
}

fn run_core_guardian_with_handshake_writer(
    invocation: CoreGuardianInvocation,
    handshake_writer: impl FnOnce(&CoreGuardianHandshake) -> io::Result<()>,
) -> io::Result<()> {
    verify_runtime_inputs(&invocation)?;
    let (cancel_reader, cancel_writer) = UnixStream::pair()
        .map_err(|_| io::Error::other("the Core guardian monitor could not start"))?;
    let mut child = ArmedChild::new(
        Command::new(&invocation.mihomo)
            .arg("-d")
            .arg(&invocation.working_directory)
            .arg("-f")
            .arg(&invocation.configuration)
            .arg("-ext-ctl-unix")
            .arg(&invocation.control_socket)
            .current_dir(&invocation.working_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| io::Error::other("the guarded Core could not start"))?,
    );
    let pid = child.id();
    let control_monitor = ControlMonitor::start(cancel_reader, cancel_writer)?;
    let process_start_identity = match discover_identity(pid) {
        Ok(identity) => identity,
        Err(error) => {
            let _ = child.terminate_and_reap();
            return Err(error);
        }
    };
    let child_stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = child.terminate_and_reap();
            return Err(io::Error::other("the guarded Core output is unavailable"));
        }
    };
    let child_stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            let _ = child.terminate_and_reap();
            return Err(io::Error::other("the guarded Core output is unavailable"));
        }
    };
    let handshake = CoreGuardianHandshake {
        protocol_version: CORE_GUARDIAN_PROTOCOL_VERSION,
        pid,
        process_start_identity,
    };
    if let Err(error) = handshake_writer(&handshake) {
        let _ = child.terminate_and_reap();
        return Err(error);
    }

    let stdout_forwarder = thread::spawn(move || forward(child_stdout, io::stdout()));
    let stderr_forwarder = thread::spawn(move || forward(child_stderr, io::stderr()));

    let wait_result = supervise_child(
        &mut child,
        |timeout| control_monitor.receive(timeout),
        ArmedChild::terminate_and_reap,
    );
    let control_result = control_monitor.finish();
    let stdout_result = stdout_forwarder
        .join()
        .map_err(|_| io::Error::other("the guarded Core output forwarder failed"))?;
    let stderr_result = stderr_forwarder
        .join()
        .map_err(|_| io::Error::other("the guarded Core output forwarder failed"))?;
    wait_result
        .and(control_result)
        .and(stdout_result)
        .and(stderr_result)
        .map(|_| ())
}

fn supervise_child(
    child: &mut ArmedChild,
    mut receive_control: impl FnMut(Duration) -> io::Result<Option<ControlEvent>>,
    mut terminate: impl FnMut(&mut ArmedChild) -> io::Result<()>,
) -> io::Result<()> {
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(_) => {
                let _ = terminate(child);
                return Err(io::Error::other("the guarded Core could not be reaped"));
            }
        }
        match receive_control(CHILD_EXIT_POLL_INTERVAL) {
            Ok(Some(ControlEvent::ParentEof)) => return terminate(child),
            Ok(Some(ControlEvent::Canceled)) => {
                let _ = terminate(child);
                return Err(io::Error::other(
                    "the Core guardian monitor stopped unexpectedly",
                ));
            }
            Ok(None) => {}
            Err(_) => {
                let _ = terminate(child);
                return Err(io::Error::other("the Core guardian monitor failed"));
            }
        }
    }
}

fn verify_runtime_inputs(invocation: &CoreGuardianInvocation) -> io::Result<()> {
    invocation.validate_arguments()?;
    let root = fs::symlink_metadata(&invocation.working_directory)?;
    let binary = fs::symlink_metadata(&invocation.mihomo)?;
    let configuration = fs::symlink_metadata(&invocation.configuration)?;
    if root.file_type().is_symlink()
        || !root.is_dir()
        || binary.file_type().is_symlink()
        || !binary.is_file()
        || binary.permissions().mode() & 0o111 == 0
        || binary.len() > MIHOMO_BINARY_MAX_BYTES as u64
        || configuration.file_type().is_symlink()
        || !configuration.is_file()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the guarded Core runtime is invalid",
        ));
    }
    let file = File::open(&invocation.mihomo)?;
    let opened = file.metadata()?;
    if opened.dev() != binary.dev() || opened.ino() != binary.ino() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the guarded Core runtime changed while opening",
        ));
    }
    let mut content = Vec::with_capacity(binary.len() as usize);
    file.take((MIHOMO_BINARY_MAX_BYTES as u64).saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() > MIHOMO_BINARY_MAX_BYTES
        || crate::digest::sha256_hex(&content) != invocation.mihomo_sha256
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "the guarded Core executable identity is invalid",
        ));
    }
    Ok(())
}

fn discover_identity(pid: u32) -> io::Result<String> {
    for attempt in 0..20 {
        match PsProcessInspector.identity(pid) {
            Ok(Some(identity)) if !identity.is_empty() => return Ok(identity),
            Ok(_) if attempt < 19 => thread::sleep(Duration::from_millis(10)),
            Ok(_) => break,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::other(
        "the guarded Core process identity is unavailable",
    ))
}

fn write_handshake(writer: &mut impl Write, handshake: &CoreGuardianHandshake) -> io::Result<()> {
    handshake.validate()?;
    let body = serde_json::to_vec(handshake).map_err(|_| invalid_handshake())?;
    if body.is_empty() || body.len() > HANDSHAKE_MAX_BYTES {
        return Err(invalid_handshake());
    }
    let length = u32::try_from(body.len()).map_err(|_| invalid_handshake())?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&body)?;
    writer.flush()
}

fn forward(mut reader: impl Read, mut writer: impl Write) -> io::Result<()> {
    io::copy(&mut reader, &mut writer)?;
    writer.flush()
}

fn monitor_control_pipe(cancel: UnixStream) -> io::Result<ControlEvent> {
    let mut control = io::stdin().lock();
    let mut buffer = [0_u8; 64];
    loop {
        let (cancel_ready, control_ready) = {
            let mut descriptors = [
                PollFd::new(cancel.as_fd(), PollFlags::POLLIN),
                PollFd::new(control.as_fd(), PollFlags::POLLIN),
            ];
            match poll(&mut descriptors, PollTimeout::NONE) {
                Ok(_) => {}
                Err(Errno::EINTR) => continue,
                Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
            }
            (
                descriptor_ready(&descriptors[0]),
                descriptor_ready(&descriptors[1]),
            )
        };
        if cancel_ready {
            return Ok(ControlEvent::Canceled);
        }
        if !control_ready {
            continue;
        }
        match control.read(&mut buffer) {
            Ok(0) => return Ok(ControlEvent::ParentEof),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

fn descriptor_ready(descriptor: &PollFd<'_>) -> bool {
    descriptor.revents().is_some_and(|events| {
        events.intersects(
            PollFlags::POLLIN | PollFlags::POLLHUP | PollFlags::POLLERR | PollFlags::POLLNVAL,
        )
    })
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn invalid_invocation() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "the internal Core guardian invocation is invalid",
    )
}

fn invalid_handshake() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "the Core guardian handshake is invalid",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureChild(Child);

    impl Drop for FixtureChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn arguments(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn parser_requires_the_exact_absolute_hidden_invocation() {
        let invocation = CoreGuardianInvocation::parse_process_arguments(&arguments(&[
            "hopash",
            INTERNAL_CORE_GUARDIAN_MODE,
            "--mihomo",
            "/private/tmp/g1/mihomo",
            "--mihomo-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--working-directory",
            "/private/tmp/g1",
            "--configuration",
            "/private/tmp/g1/config.yaml",
            "--control-socket",
            "/private/tmp/control.sock",
        ]))
        .expect("the fixture invocation should parse")
        .expect("the guardian mode should be detected");

        let diagnostics = format!("{invocation:?}");
        assert!(!diagnostics.contains("/private/tmp"));
        assert!(!diagnostics.contains("aaaaaaaa"));
    }

    #[test]
    fn parser_ignores_public_arguments_and_rejects_escaped_runtime_paths() {
        assert_eq!(
            CoreGuardianInvocation::parse_process_arguments(&arguments(&["hopash", "status"]))
                .expect("public arguments should be valid"),
            None
        );
        let error = CoreGuardianInvocation::parse_process_arguments(&arguments(&[
            "hopash",
            INTERNAL_CORE_GUARDIAN_MODE,
            "--mihomo",
            "/private/tmp/other/mihomo",
            "--mihomo-sha256",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--working-directory",
            "/private/tmp/g1",
            "--configuration",
            "/private/tmp/g1/config.yaml",
            "--control-socket",
            "/private/tmp/control.sock",
        ]))
        .expect_err("an escaped executable should be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn handshake_is_versioned_bounded_and_redacted() {
        let handshake = CoreGuardianHandshake {
            protocol_version: CORE_GUARDIAN_PROTOCOL_VERSION,
            pid: 42,
            process_start_identity: "fixture-start-identity".to_owned(),
        };
        let mut wire = Vec::new();
        write_handshake(&mut wire, &handshake).expect("the fixture handshake should encode");

        assert_eq!(read_handshake(&mut wire.as_slice()).unwrap(), handshake);
        assert!(!format!("{handshake:?}").contains("fixture-start-identity"));

        let oversized =
            u32::try_from(HANDSHAKE_MAX_BYTES + 1).expect("the fixture handshake limit should fit");
        let error = read_handshake(&mut oversized.to_be_bytes().as_slice())
            .expect_err("an oversized handshake should fail before allocation");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn handshake_write_failure_reaps_the_exact_core_and_preserves_unrelated_processes() {
        let root = PathBuf::from("/private/tmp").join(format!(
            "hopash-guardian-write-failure-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).expect("the fixture root should be created");
        let executable = root.join("mihomo");
        let script = b"#!/bin/sh\nexec /bin/sleep 30\n";
        fs::write(&executable, script).expect("the fixture Core should be written");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("the fixture Core should be executable");
        let configuration = root.join("config.yaml");
        fs::write(&configuration, b"mode: rule\n")
            .expect("the fixture configuration should be written");
        let invocation = CoreGuardianInvocation::new(
            executable,
            crate::digest::sha256_hex(script),
            root.clone(),
            configuration,
            root.join("control.sock"),
        )
        .expect("the fixture invocation should be valid");
        let mut unrelated = FixtureChild(
            Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .expect("the unrelated fixture should start"),
        );
        let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
        let captured = std::sync::Arc::clone(&observed);

        let error = run_core_guardian_with_handshake_writer(invocation, move |handshake| {
            *captured
                .lock()
                .expect("the handshake capture should remain available") = Some((
                handshake.pid(),
                handshake.process_start_identity().to_owned(),
            ));
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "fixture handshake failure",
            ))
        })
        .expect_err("the injected handshake write should fail");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        let (core_pid, core_identity) = observed
            .lock()
            .expect("the handshake capture should remain available")
            .clone()
            .expect("the handshake should identify the guarded Core");
        assert_ne!(
            PsProcessInspector
                .identity(core_pid)
                .expect("the guarded Core should be inspectable")
                .as_deref(),
            Some(core_identity.as_str())
        );
        assert!(
            unrelated
                .0
                .try_wait()
                .expect("the unrelated fixture should be inspectable")
                .is_none()
        );
        fs::remove_dir_all(root).expect("the fixture root should be removed");
    }

    #[test]
    fn ordinary_exit_wins_a_queued_parent_eof_without_post_reap_termination() {
        let mut child = ArmedChild::new(
            Command::new("/bin/sh")
                .arg("-c")
                .arg("exit 0")
                .spawn()
                .expect("the ordinary-exit fixture should start"),
        );
        child
            .wait()
            .expect("the ordinary-exit fixture should be reaped");
        let mut unrelated = FixtureChild(
            Command::new("/bin/sleep")
                .arg("30")
                .spawn()
                .expect("the unrelated fixture should start"),
        );
        let mut control_was_read = false;
        let mut termination_was_called = false;

        supervise_child(
            &mut child,
            |_| {
                control_was_read = true;
                Ok(Some(ControlEvent::ParentEof))
            },
            |_| {
                termination_was_called = true;
                Ok(())
            },
        )
        .expect("the already-reaped Core should complete cleanly");

        assert!(!control_was_read);
        assert!(!termination_was_called);
        assert!(
            unrelated
                .0
                .try_wait()
                .expect("the unrelated fixture should be inspectable")
                .is_none()
        );
    }
}

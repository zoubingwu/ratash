use std::fmt::Write as _;
use std::fs;
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use ratash::core_guardian::{INTERNAL_CORE_GUARDIAN_MODE, read_handshake};
use ratash::lifecycle::{ProcessInspector, PsProcessInspector};
use sha2::{Digest, Sha256};

const TEST_TMP: &str = if cfg!(target_os = "macos") {
    "/private/tmp"
} else {
    "/tmp"
};

struct TestRuntime {
    root: PathBuf,
    executable: PathBuf,
    configuration: PathBuf,
    control_socket: PathBuf,
    executable_sha256: String,
}

impl TestRuntime {
    fn new(label: &str, script: &[u8]) -> Self {
        let root = Path::new(TEST_TMP).join(format!(
            "ratash-guardian-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        fs::create_dir_all(&root).expect("the fixture runtime should be created");
        let executable = root.join("mihomo");
        fs::write(&executable, script).expect("the fixture Core should be written");
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))
            .expect("the fixture Core should be executable");
        let configuration = root.join("config.yaml");
        fs::write(&configuration, b"mode: rule\n")
            .expect("the fixture configuration should be written");
        let executable_sha256 = hex_sha256(script);
        let control_socket = root.join("control.sock");
        Self {
            root,
            executable,
            configuration,
            control_socket,
            executable_sha256,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ratash"));
        command
            .arg(INTERNAL_CORE_GUARDIAN_MODE)
            .arg("--mihomo")
            .arg(&self.executable)
            .arg("--mihomo-sha256")
            .arg(&self.executable_sha256)
            .arg("--working-directory")
            .arg(&self.root)
            .arg("--configuration")
            .arg(&self.configuration)
            .arg("--control-socket")
            .arg(&self.control_socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }
}

impl Drop for TestRuntime {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn parent_control_eof_terminates_the_exact_core_and_reaps_it() {
    let runtime = TestRuntime::new(
        "eof",
        b"#!/bin/sh\nprintf 'guarded stdout\\n'\nprintf 'guarded stderr\\n' >&2\n/usr/bin/touch started\nexec /bin/sleep 30\n",
    );
    let mut guardian = runtime
        .command()
        .spawn()
        .expect("the fixture guardian should start");
    let mut stdout = guardian
        .stdout
        .take()
        .expect("the guardian stdout should be piped");
    let mut stderr = guardian
        .stderr
        .take()
        .expect("the guardian stderr should be piped");
    let handshake = read_fixture_handshake(&mut guardian, &mut stdout, &mut stderr);
    let identity = handshake.process_start_identity().to_owned();
    assert_eq!(
        PsProcessInspector
            .identity(handshake.pid())
            .expect("the fixture Core identity should be readable")
            .as_deref(),
        Some(identity.as_str())
    );
    wait_for_path(&runtime.root.join("started"), Duration::from_secs(1));

    drop(guardian.stdin.take());
    let status = wait_for_exit(&mut guardian, Duration::from_secs(2));
    assert!(status.success());
    assert_process_identity_gone(handshake.pid(), &identity);

    let mut forwarded_stdout = String::new();
    stdout
        .read_to_string(&mut forwarded_stdout)
        .expect("forwarded stdout should be readable");
    let mut forwarded_stderr = String::new();
    stderr
        .read_to_string(&mut forwarded_stderr)
        .expect("forwarded stderr should be readable");
    assert!(
        forwarded_stdout.contains("guarded stdout"),
        "unexpected forwarded stdout: {forwarded_stdout:?}"
    );
    assert!(
        forwarded_stderr.contains("guarded stderr"),
        "unexpected forwarded stderr: {forwarded_stderr:?}"
    );
}

#[test]
fn ordinary_core_exit_is_reaped_while_the_parent_control_pipe_remains_open() {
    let runtime = TestRuntime::new(
        "ordinary-exit",
        b"#!/bin/sh\nprintf 'ordinary stdout\\n'\nprintf 'ordinary stderr\\n' >&2\nexit 0\n",
    );
    let mut guardian = runtime
        .command()
        .spawn()
        .expect("the fixture guardian should start");
    let control = guardian
        .stdin
        .take()
        .expect("the guardian control pipe should be open");
    let mut stdout = guardian
        .stdout
        .take()
        .expect("the guardian stdout should be piped");
    let mut stderr = guardian
        .stderr
        .take()
        .expect("the guardian stderr should be piped");
    let handshake = read_fixture_handshake(&mut guardian, &mut stdout, &mut stderr);
    let identity = handshake.process_start_identity().to_owned();

    let status = wait_for_exit(&mut guardian, Duration::from_secs(2));
    drop(control);
    assert!(status.success());
    assert_process_identity_gone(handshake.pid(), &identity);

    let mut forwarded_stdout = String::new();
    stdout
        .read_to_string(&mut forwarded_stdout)
        .expect("forwarded stdout should be readable");
    let mut forwarded_stderr = String::new();
    stderr
        .read_to_string(&mut forwarded_stderr)
        .expect("forwarded stderr should be readable");
    assert!(forwarded_stdout.contains("ordinary stdout"));
    assert!(forwarded_stderr.contains("ordinary stderr"));
}

#[test]
fn guardian_failures_redact_runtime_paths_and_executable_identity() {
    let runtime = TestRuntime::new("secret-path", b"#!/bin/sh\nexit 0\n");
    let secret_digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let output = Command::new(env!("CARGO_BIN_EXE_ratash"))
        .arg(INTERNAL_CORE_GUARDIAN_MODE)
        .arg("--mihomo")
        .arg(&runtime.executable)
        .arg("--mihomo-sha256")
        .arg(secret_digest)
        .arg("--working-directory")
        .arg(&runtime.root)
        .arg("--configuration")
        .arg(&runtime.configuration)
        .arg("--control-socket")
        .arg(&runtime.control_socket)
        .output()
        .expect("the invalid guardian invocation should finish");

    assert_eq!(output.status.code(), Some(70));
    assert!(output.stdout.is_empty());
    let diagnostics = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostics.contains("The Core guardian stopped with an error"));
    assert!(!diagnostics.contains("secret-path"));
    assert!(!diagnostics.contains(secret_digest));
}

fn wait_for_exit(child: &mut Child, timeout: Duration) -> ExitStatus {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child
            .try_wait()
            .expect("the fixture process should be waitable")
        {
            return status;
        }
        assert!(Instant::now() < deadline, "the fixture process should exit");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_for_path(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(Instant::now() < deadline, "the fixture path should appear");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn read_fixture_handshake(
    guardian: &mut Child,
    stdout: &mut impl Read,
    stderr: &mut impl Read,
) -> ratash::core_guardian::CoreGuardianHandshake {
    match read_handshake(stdout) {
        Ok(handshake) => handshake,
        Err(error) => {
            let status = guardian
                .wait()
                .expect("the failed fixture guardian should be waitable");
            let mut diagnostics = String::new();
            stderr
                .read_to_string(&mut diagnostics)
                .expect("the fixture diagnostics should be readable");
            panic!("guardian handshake failed: {error}; status={status}; stderr={diagnostics:?}");
        }
    }
}

fn assert_process_identity_gone(pid: u32, identity: &str) {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let current = PsProcessInspector
            .identity(pid)
            .expect("the fixture process identity should be inspectable");
        if current.as_deref() != Some(identity) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the guarded Core should be absent"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn hex_sha256(content: &[u8]) -> String {
    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(content) {
        write!(&mut encoded, "{byte:02x}").expect("digest formatting should succeed");
    }
    encoded
}

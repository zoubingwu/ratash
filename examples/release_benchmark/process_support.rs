//! Owns bounded child-process and PTY lifecycles.

use std::error::Error;
use std::io::Read;
use std::process::{Child, ExitStatus, Output};
use std::thread;
use std::time::{Duration, Instant};

use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use ratash::tui_runtime::ShutdownSignal;

use super::CHILD_CLEANUP_TIMEOUT;
use super::support::{ensure_collection_running, invalid};

pub(super) struct ProcessChildGuard {
    child: Option<Child>,
}

impl ProcessChildGuard {
    pub(super) fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    pub(super) fn id(&self) -> Result<u32, Box<dyn Error>> {
        self.child
            .as_ref()
            .map(Child::id)
            .ok_or_else(|| invalid("benchmark child is unavailable"))
    }

    #[cfg(test)]
    pub(super) fn into_child(mut self) -> Child {
        self.child
            .take()
            .expect("benchmark fixture child should be available")
    }

    pub(super) fn wait_with_output(
        mut self,
        timeout: Duration,
        signal: &dyn ShutdownSignal,
    ) -> Result<Output, Box<dyn Error>> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| invalid("benchmark child is unavailable"))?;
        let deadline = Instant::now() + timeout;
        let status = loop {
            ensure_collection_running(signal)?;
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                return Err(invalid("benchmark child exceeded its completion deadline"));
            }
            thread::sleep(Duration::from_millis(25));
        };
        let mut stdout = Vec::new();
        if let Some(mut pipe) = child.stdout.take() {
            pipe.read_to_end(&mut stdout)?;
        }
        let mut stderr = Vec::new();
        if let Some(mut pipe) = child.stderr.take() {
            pipe.read_to_end(&mut stderr)?;
        }
        self.child.take();
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }

    fn terminate_and_reap(&mut self) -> Result<Option<ExitStatus>, Box<dyn Error>> {
        self.terminate_and_reap_with_timeout(CHILD_CLEANUP_TIMEOUT)
    }

    pub(super) fn terminate_and_reap_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<ExitStatus>, Box<dyn Error>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        let status = terminate_process_child(child, timeout)?;
        self.child.take();
        Ok(Some(status))
    }

    pub(super) fn terminate_and_collect_stderr(
        mut self,
    ) -> Result<(ExitStatus, String), Box<dyn Error>> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| invalid("benchmark child is unavailable"))?;
        let status = terminate_process_child(child, CHILD_CLEANUP_TIMEOUT)?;
        let mut diagnostic = String::new();
        if let Some(mut stderr) = child.stderr.take() {
            stderr.read_to_string(&mut diagnostic)?;
        }
        self.child.take();
        Ok((status, diagnostic.trim().to_owned()))
    }
}

impl Drop for ProcessChildGuard {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap();
    }
}

fn terminate_process_child(
    child: &mut Child,
    timeout: Duration,
) -> Result<ExitStatus, Box<dyn Error>> {
    if let Some(status) = child.try_wait()? {
        return Ok(status);
    }
    let deadline = Instant::now() + timeout;
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    let graceful_deadline = Instant::now() + timeout / 2;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= graceful_deadline {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    let _ = child.kill();
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            return Err(invalid("benchmark child did not exit after termination"));
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub(super) struct PtyChildGuard {
    child: Option<Box<dyn portable_pty::Child + Send + Sync>>,
}

impl PtyChildGuard {
    pub(super) fn new(child: Box<dyn portable_pty::Child + Send + Sync>) -> Self {
        Self { child: Some(child) }
    }

    pub(super) fn process_id(&self) -> Option<u32> {
        self.child.as_ref().and_then(|child| child.process_id())
    }

    pub(super) fn terminate_and_reap(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<portable_pty::ExitStatus>, Box<dyn Error>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        if let Some(status) = child.try_wait()? {
            self.child.take();
            return Ok(Some(status));
        }
        let _ = child.kill();
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                self.child.take();
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Err(invalid("Status Interface did not exit after termination"));
            }
            thread::sleep(Duration::from_millis(25));
        }
    }

    pub(super) fn wait_bounded(
        &mut self,
        graceful_timeout: Duration,
    ) -> Result<portable_pty::ExitStatus, Box<dyn Error>> {
        let deadline = Instant::now() + graceful_timeout;
        loop {
            let child = self
                .child
                .as_mut()
                .ok_or_else(|| invalid("Status Interface child is unavailable"))?;
            if let Some(status) = child.try_wait()? {
                self.child.take();
                return Ok(status);
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }

        self.terminate_and_reap(CHILD_CLEANUP_TIMEOUT)?
            .ok_or_else(|| invalid("Status Interface child is unavailable"))
    }
}

impl Drop for PtyChildGuard {
    fn drop(&mut self) {
        let _ = self.terminate_and_reap(CHILD_CLEANUP_TIMEOUT);
    }
}

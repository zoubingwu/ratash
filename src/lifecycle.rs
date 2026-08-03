use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::ipc::IPC_PROTOCOL_VERSION;

const LEASE_SCHEMA_VERSION: u16 = 1;
const INSTANCE_SCHEMA_VERSION: u16 = 1;
const RECORD_MAX_BYTES: usize = 64 * 1_024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatePaths {
    pub root: PathBuf,
    pub ipc_socket: PathBuf,
    pub shutdown_socket: PathBuf,
    pub persistence: PathBuf,
    pub runtime: PathBuf,
    pub instance_record: PathBuf,
}

impl StatePaths {
    #[must_use]
    pub fn for_root(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            ipc_socket: root.join("supervisor.sock"),
            shutdown_socket: root.join("supervisor-control.sock"),
            persistence: root.join("state"),
            runtime: root.join("runtime"),
            instance_record: root.join("instance.json"),
            root,
        }
    }

    pub fn discover() -> Result<Self, LifecycleError> {
        let override_root = std::env::var_os("RATASH_STATE_DIR");
        let home = std::env::var_os("HOME");
        Self::from_environment(override_root.as_deref(), home.as_deref())
    }

    pub fn from_environment(
        override_root: Option<&OsStr>,
        home: Option<&OsStr>,
    ) -> Result<Self, LifecycleError> {
        let root = match override_root {
            Some(value) => {
                let path = PathBuf::from(value);
                if !path.is_absolute() {
                    return Err(LifecycleError::new(LifecycleErrorKind::InvalidStateRoot));
                }
                path
            }
            None => PathBuf::from(
                home.ok_or_else(|| LifecycleError::new(LifecycleErrorKind::HomeUnavailable))?,
            )
            .join("Library")
            .join("Application Support")
            .join("ratash"),
        };
        Ok(Self::for_root(root))
    }

    pub fn prepare(&self) -> Result<(), LifecycleError> {
        create_private_directory(&self.root)?;
        create_private_directory(&self.persistence)?;
        create_private_directory(&self.runtime)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessIdentity {
    pub pid: u32,
    pub start_identity: String,
}

pub trait ProcessInspector: Send + Sync {
    fn identity(&self, pid: u32) -> io::Result<Option<String>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PsProcessInspector;

impl ProcessInspector for PsProcessInspector {
    fn identity(&self, pid: u32) -> io::Result<Option<String>> {
        let output = Command::new("/bin/ps")
            .args(["-o", "lstart=", "-p"])
            .arg(pid.to_string())
            .output()?;
        if !output.status.success() {
            return Ok(None);
        }
        let identity = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if identity.is_empty() {
            Ok(None)
        } else {
            Ok(Some(identity))
        }
    }
}

pub fn current_process_identity(
    inspector: &dyn ProcessInspector,
) -> Result<ProcessIdentity, LifecycleError> {
    let pid = std::process::id();
    let start_identity = inspector
        .identity(pid)
        .map_err(LifecycleError::io)?
        .ok_or_else(|| LifecycleError::new(LifecycleErrorKind::ProcessIdentityUnavailable))?;
    Ok(ProcessIdentity {
        pid,
        start_identity,
    })
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LeaseOwner {
    schema_version: u16,
    pub process: ProcessIdentity,
    instance_token: String,
}

impl LeaseOwner {
    #[must_use]
    pub fn new(process: ProcessIdentity) -> Self {
        Self {
            schema_version: LEASE_SCHEMA_VERSION,
            process,
            instance_token: uuid::Uuid::new_v4().to_string(),
        }
    }

    #[must_use]
    pub fn instance_token(&self) -> &str {
        &self.instance_token
    }
}

impl fmt::Debug for LeaseOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaseOwner")
            .field("schema_version", &self.schema_version)
            .field("process", &self.process)
            .field("instance_token", &"[REDACTED]")
            .finish()
    }
}

pub enum LeaseAcquisition {
    Acquired(DirectoryLease),
    HeldByLiveProcess(LeaseOwner),
}

pub struct DirectoryLease {
    lock_path: PathBuf,
    owner: LeaseOwner,
    released: bool,
}

impl DirectoryLease {
    pub fn acquire(
        root: &Path,
        name: &str,
        process: ProcessIdentity,
        inspector: &dyn ProcessInspector,
    ) -> Result<LeaseAcquisition, LifecycleError> {
        validate_lock_name(name)?;
        create_private_directory(root)?;
        let lock_path = root.join(format!("{name}.lock"));
        let owner = LeaseOwner::new(process);

        for _ in 0..4 {
            let pending = root.join(format!(".{name}.{}.pending", owner.instance_token));
            match create_pending_lease(&pending, &owner) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    remove_lease_directory(&pending).map_err(LifecycleError::io)?;
                    continue;
                }
                Err(error) => return Err(LifecycleError::io(error)),
            }
            match fs::rename(&pending, &lock_path) {
                Ok(()) => {
                    sync_directory(root)?;
                    return Ok(LeaseAcquisition::Acquired(Self {
                        lock_path,
                        owner,
                        released: false,
                    }));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::AlreadyExists
                            | io::ErrorKind::DirectoryNotEmpty
                            | io::ErrorKind::PermissionDenied
                    ) =>
                {
                    remove_lease_directory(&pending).map_err(LifecycleError::io)?;
                }
                Err(error) => {
                    let _ = remove_lease_directory(&pending);
                    return Err(LifecycleError::io(error));
                }
            }

            let existing = read_lease_owner(&lock_path)?;
            let live_identity = inspector
                .identity(existing.process.pid)
                .map_err(LifecycleError::io)?;
            if live_identity.as_deref() == Some(existing.process.start_identity.as_str()) {
                return Ok(LeaseAcquisition::HeldByLiveProcess(existing));
            }
            quarantine_stale_lease(root, name, &lock_path, &owner.instance_token)?;
        }

        Err(LifecycleError::new(LifecycleErrorKind::LeaseContention))
    }

    #[must_use]
    pub fn owner(&self) -> &LeaseOwner {
        &self.owner
    }

    pub fn release(mut self) -> Result<(), LifecycleError> {
        self.release_inner()?;
        self.released = true;
        Ok(())
    }

    fn release_inner(&self) -> Result<(), LifecycleError> {
        let current = match read_lease_owner(&self.lock_path) {
            Ok(owner) => owner,
            Err(error) if error.kind() == LifecycleErrorKind::LeaseMissing => return Ok(()),
            Err(error) => return Err(error),
        };
        if current.instance_token != self.owner.instance_token {
            return Err(LifecycleError::new(LifecycleErrorKind::LeaseOwnershipLost));
        }
        let parent = self
            .lock_path
            .parent()
            .ok_or_else(|| LifecycleError::new(LifecycleErrorKind::InvalidStateRoot))?;
        let released = parent.join(format!(".released.{}", self.owner.instance_token.as_str()));
        fs::rename(&self.lock_path, &released).map_err(LifecycleError::io)?;
        remove_lease_directory(&released).map_err(LifecycleError::io)?;
        sync_directory(parent)
    }
}

impl fmt::Debug for DirectoryLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectoryLease")
            .field("lock_path", &self.lock_path)
            .field("owner", &self.owner)
            .field("released", &self.released)
            .finish()
    }
}

impl Drop for DirectoryLease {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.release_inner();
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedCoreIdentityRecord {
    pub pid: u32,
    pub process_start_identity: String,
    pub control_endpoint: PathBuf,
    pub runtime_generation: u64,
    pub core_instance_generation: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceRecord {
    schema_version: u16,
    pub supervisor: ProcessIdentity,
    instance_token: String,
    pub started_at_unix_ms: u64,
    pub socket_path: PathBuf,
    pub protocol_version: u16,
    pub active_profile_id: Option<String>,
    pub managed_core: Option<ManagedCoreIdentityRecord>,
}

impl InstanceRecord {
    #[must_use]
    pub fn new(owner: &LeaseOwner, started_at_unix_ms: u64, socket_path: PathBuf) -> Self {
        Self {
            schema_version: INSTANCE_SCHEMA_VERSION,
            supervisor: owner.process.clone(),
            instance_token: owner.instance_token.clone(),
            started_at_unix_ms,
            socket_path,
            protocol_version: IPC_PROTOCOL_VERSION,
            active_profile_id: None,
            managed_core: None,
        }
    }

    #[must_use]
    pub fn instance_token(&self) -> &str {
        &self.instance_token
    }

    pub fn write_private(&self, path: &Path) -> Result<(), LifecycleError> {
        let content = serde_json::to_vec(self)
            .map_err(|_| LifecycleError::new(LifecycleErrorKind::InvalidInstanceRecord))?;
        write_private_atomic(path, &content)
    }

    pub fn read_private(path: &Path) -> Result<Option<Self>, LifecycleError> {
        let content = match read_limited(path, RECORD_MAX_BYTES) {
            Ok(content) => content,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(LifecycleError::io(error)),
        };
        let record: Self = serde_json::from_slice(&content)
            .map_err(|_| LifecycleError::new(LifecycleErrorKind::InvalidInstanceRecord))?;
        if record.schema_version != INSTANCE_SCHEMA_VERSION
            || record.protocol_version != IPC_PROTOCOL_VERSION
            || record.instance_token.is_empty()
            || record.supervisor.start_identity.is_empty()
        {
            return Err(LifecycleError::new(
                LifecycleErrorKind::InvalidInstanceRecord,
            ));
        }
        Ok(Some(record))
    }
}

impl fmt::Debug for InstanceRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstanceRecord")
            .field("schema_version", &self.schema_version)
            .field("supervisor", &self.supervisor)
            .field("instance_token", &"[REDACTED]")
            .field("started_at_unix_ms", &self.started_at_unix_ms)
            .field("protocol_version", &self.protocol_version)
            .field("has_active_profile", &self.active_profile_id.is_some())
            .field("has_managed_core", &self.managed_core.is_some())
            .finish()
    }
}

pub fn remove_verified_stale_socket(path: &Path) -> Result<bool, LifecycleError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(LifecycleError::io(error)),
    };
    if !metadata.file_type().is_socket() {
        return Err(LifecycleError::new(
            LifecycleErrorKind::UnsafeSocketCleanupTarget,
        ));
    }
    fs::remove_file(path).map_err(LifecycleError::io)?;
    Ok(true)
}

fn create_pending_lease(path: &Path, owner: &LeaseOwner) -> io::Result<()> {
    fs::create_dir(path)?;
    let result = (|| {
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        let content = serde_json::to_vec(owner)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let owner_path = path.join("owner.json");
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        let mut file = options.open(owner_path)?;
        file.write_all(&content)?;
        file.sync_all()?;
        sync_directory_io(path)
    })();
    if result.is_err() {
        let _ = remove_lease_directory(path);
    }
    result
}

fn read_lease_owner(path: &Path) -> Result<LeaseOwner, LifecycleError> {
    let content = read_limited(&path.join("owner.json"), RECORD_MAX_BYTES).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            LifecycleError::new(LifecycleErrorKind::LeaseMissing)
        } else {
            LifecycleError::io(error)
        }
    })?;
    let owner: LeaseOwner = serde_json::from_slice(&content)
        .map_err(|_| LifecycleError::new(LifecycleErrorKind::InvalidLeaseRecord))?;
    if owner.schema_version != LEASE_SCHEMA_VERSION
        || owner.instance_token.is_empty()
        || owner.process.start_identity.is_empty()
    {
        return Err(LifecycleError::new(LifecycleErrorKind::InvalidLeaseRecord));
    }
    Ok(owner)
}

fn quarantine_stale_lease(
    root: &Path,
    name: &str,
    lock_path: &Path,
    replacement_token: &str,
) -> Result<(), LifecycleError> {
    let quarantine = root.join(format!(".{name}.{replacement_token}.stale"));
    match fs::rename(lock_path, &quarantine) {
        Ok(()) => {
            remove_lease_directory(&quarantine).map_err(LifecycleError::io)?;
            sync_directory(root)
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound
                    | io::ErrorKind::AlreadyExists
                    | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(LifecycleError::io(error)),
    }
}

fn remove_lease_directory(path: &Path) -> io::Result<()> {
    match fs::remove_file(path.join("owner.json")) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    fs::remove_dir(path)
}

fn validate_lock_name(name: &str) -> Result<(), LifecycleError> {
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        Err(LifecycleError::new(LifecycleErrorKind::InvalidLeaseName))
    } else {
        Ok(())
    }
}

fn create_private_directory(path: &Path) -> Result<(), LifecycleError> {
    fs::create_dir_all(path).map_err(LifecycleError::io)?;
    let metadata = fs::symlink_metadata(path).map_err(LifecycleError::io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(LifecycleError::new(LifecycleErrorKind::InvalidStateRoot));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(LifecycleError::io)
}

fn write_private_atomic(path: &Path, content: &[u8]) -> Result<(), LifecycleError> {
    if content.len() > RECORD_MAX_BYTES {
        return Err(LifecycleError::new(LifecycleErrorKind::RecordTooLarge));
    }
    let parent = path
        .parent()
        .ok_or_else(|| LifecycleError::new(LifecycleErrorKind::InvalidStateRoot))?;
    create_private_directory(parent)?;
    let temporary = parent.join(format!(".instance.{}.tmp", uuid::Uuid::new_v4()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options.open(&temporary).map_err(LifecycleError::io)?;
    if let Err(error) = file.write_all(content).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(LifecycleError::io(error));
    }
    drop(file);
    fs::rename(&temporary, path).map_err(LifecycleError::io)?;
    sync_directory(parent)
}

fn read_limited(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record exceeds its size limit",
        ));
    }
    let mut content = Vec::new();
    file.take((limit as u64).saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record exceeds its size limit",
        ));
    }
    Ok(content)
}

fn sync_directory(path: &Path) -> Result<(), LifecycleError> {
    sync_directory_io(path).map_err(LifecycleError::io)
}

fn sync_directory_io(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LifecycleErrorKind {
    HomeUnavailable,
    InvalidStateRoot,
    InvalidLeaseName,
    InvalidLeaseRecord,
    InvalidInstanceRecord,
    RecordTooLarge,
    ProcessIdentityUnavailable,
    LeaseContention,
    LeaseMissing,
    LeaseOwnershipLost,
    UnsafeSocketCleanupTarget,
    Io,
}

pub struct LifecycleError {
    kind: LifecycleErrorKind,
    source: Option<io::Error>,
}

impl LifecycleError {
    #[must_use]
    pub const fn kind(&self) -> LifecycleErrorKind {
        self.kind
    }

    const fn new(kind: LifecycleErrorKind) -> Self {
        Self { kind, source: None }
    }

    fn io(source: io::Error) -> Self {
        Self {
            kind: LifecycleErrorKind::Io,
            source: Some(source),
        }
    }
}

impl fmt::Debug for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LifecycleError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            LifecycleErrorKind::HomeUnavailable => "the user home directory is unavailable",
            LifecycleErrorKind::InvalidStateRoot => "the Ratash state root is invalid",
            LifecycleErrorKind::InvalidLeaseName => "the lifecycle lease name is invalid",
            LifecycleErrorKind::InvalidLeaseRecord => "the lifecycle lease record is invalid",
            LifecycleErrorKind::InvalidInstanceRecord => "the instance record is invalid",
            LifecycleErrorKind::RecordTooLarge => "the lifecycle record exceeds its size limit",
            LifecycleErrorKind::ProcessIdentityUnavailable => {
                "the process start identity is unavailable"
            }
            LifecycleErrorKind::LeaseContention => "the lifecycle lease remains contended",
            LifecycleErrorKind::LeaseMissing => "the lifecycle lease is missing",
            LifecycleErrorKind::LeaseOwnershipLost => "the lifecycle lease ownership changed",
            LifecycleErrorKind::UnsafeSocketCleanupTarget => {
                "the stale IPC cleanup target is not a Unix socket"
            }
            LifecycleErrorKind::Io => "the lifecycle state operation failed",
        })
    }
}

impl std::error::Error for LifecycleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

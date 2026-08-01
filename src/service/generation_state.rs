//! Durable owner and Core Instance Generation state storage.

use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::{CoreRuntimeError, CoreRuntimeErrorKind};

use super::error::service_error;

const SERVICE_GENERATION_STATE_SCHEMA_VERSION: u16 = 1;
const SERVICE_GENERATION_STATE_FILE: &str = "generation-state-v1.json";
#[cfg(unix)]
const SERVICE_GENERATION_LOCK_FILE: &str = "generation-state-v1.lock";
const SERVICE_GENERATION_STATE_MAX_BYTES: usize = 1_024;
#[cfg(unix)]
const SERVICE_DIRECTORY_MODE: u32 = 0o711;
#[cfg(unix)]
const SERVICE_GENERATION_STATE_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ServiceGenerationStateV1 {
    schema_version: u16,
    pub(super) owner_generation: u64,
    pub(super) core_instance_generation: u64,
}

impl ServiceGenerationStateV1 {
    const fn initial() -> Self {
        Self {
            schema_version: SERVICE_GENERATION_STATE_SCHEMA_VERSION,
            owner_generation: 0,
            core_instance_generation: 0,
        }
    }

    pub(super) const fn new(owner_generation: u64, core_instance_generation: u64) -> Self {
        Self {
            schema_version: SERVICE_GENERATION_STATE_SCHEMA_VERSION,
            owner_generation,
            core_instance_generation,
        }
    }
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceGenerationStateCommitFault {
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

pub(super) fn prepare_control_root(service_owned_root: &Path) -> Result<PathBuf, CoreRuntimeError> {
    let control_root = service_owned_root.join("control");
    match fs::symlink_metadata(&control_root) {
        Ok(metadata) => validate_control_root_metadata(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(&control_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "control root creation failed",
                )
            })?;
            #[cfg(unix)]
            fs::set_permissions(
                &control_root,
                fs::Permissions::from_mode(SERVICE_DIRECTORY_MODE),
            )
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "control root permission update failed",
                )
            })?;
            let metadata = fs::symlink_metadata(&control_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "control root validation failed",
                )
            })?;
            validate_control_root_metadata(&metadata)?;
            sync_private_service_directory(service_owned_root)?;
        }
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "control root validation failed",
            ));
        }
    }
    Ok(control_root)
}

pub(super) fn prepare_service_owned_root(
    configured_root: &Path,
) -> Result<PathBuf, CoreRuntimeError> {
    validate_service_root_parent(configured_root)?;
    match fs::symlink_metadata(configured_root) {
        Ok(metadata) => validate_service_directory_metadata(
            &metadata,
            "service root ownership, permissions, or type are invalid",
        )?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir(configured_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service root creation failed",
                )
            })?;
            #[cfg(unix)]
            fs::set_permissions(
                configured_root,
                fs::Permissions::from_mode(SERVICE_DIRECTORY_MODE),
            )
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service root permission update failed",
                )
            })?;
            let metadata = fs::symlink_metadata(configured_root).map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service root validation failed",
                )
            })?;
            validate_service_directory_metadata(
                &metadata,
                "service root ownership, permissions, or type are invalid",
            )?;
            sync_service_root_parent(configured_root)?;
        }
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service root validation failed",
            ));
        }
    }
    let canonical = fs::canonicalize(configured_root).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root canonicalization failed",
        )
    })?;
    let metadata = fs::symlink_metadata(&canonical).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root validation failed",
        )
    })?;
    validate_service_directory_metadata(
        &metadata,
        "service root ownership, permissions, or type are invalid",
    )?;
    Ok(canonical)
}

fn validate_service_root_parent(service_owned_root: &Path) -> Result<(), CoreRuntimeError> {
    let parent = service_owned_root.parent().ok_or_else(|| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent is unavailable",
        )
    })?;
    let metadata = fs::symlink_metadata(parent).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent validation failed",
        )
    })?;
    if !metadata.file_type().is_dir() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent type is invalid",
        ));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o022 != 0
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent ownership or permissions are invalid",
        ));
    }
    Ok(())
}

fn sync_service_root_parent(service_owned_root: &Path) -> Result<(), CoreRuntimeError> {
    let parent = service_owned_root.parent().ok_or_else(|| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent is unavailable",
        )
    })?;
    let path_metadata = fs::symlink_metadata(parent).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent validation failed",
        )
    })?;
    if !path_metadata.file_type().is_dir() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent type is invalid",
        ));
    }
    #[cfg(unix)]
    if path_metadata.uid() != nix::unistd::geteuid().as_raw() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent ownership is invalid",
        ));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW);
    let directory = options.open(parent).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent sync open failed",
        )
    })?;
    let opened_metadata = directory.metadata().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent validation failed",
        )
    })?;
    if !opened_metadata.file_type().is_dir() {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent type is invalid",
        ));
    }
    #[cfg(unix)]
    if path_metadata.dev() != opened_metadata.dev()
        || path_metadata.ino() != opened_metadata.ino()
        || opened_metadata.uid() != nix::unistd::geteuid().as_raw()
        || path_metadata.permissions().mode() & 0o022 != 0
        || opened_metadata.permissions().mode() & 0o022 != 0
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent identity changed",
        ));
    }
    directory.sync_all().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service root parent sync failed",
        )
    })
}

fn validate_control_root(control_root: &Path) -> Result<(), CoreRuntimeError> {
    let metadata = fs::symlink_metadata(control_root).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "control root validation failed",
        )
    })?;
    validate_control_root_metadata(&metadata)
}

fn validate_control_root_metadata(metadata: &fs::Metadata) -> Result<(), CoreRuntimeError> {
    validate_service_directory_metadata(
        metadata,
        "control root ownership, permissions, or type are invalid",
    )
}

fn validate_service_directory_metadata(
    metadata: &fs::Metadata,
    diagnostic: &'static str,
) -> Result<(), CoreRuntimeError> {
    if !metadata.file_type().is_dir() {
        return Err(service_error(CoreRuntimeErrorKind::Unavailable, diagnostic));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != SERVICE_DIRECTORY_MODE
    {
        return Err(service_error(CoreRuntimeErrorKind::Unavailable, diagnostic));
    }
    Ok(())
}

pub(super) fn load_or_initialize_generation_state(
    control_root: &Path,
) -> Result<ServiceGenerationStateV1, CoreRuntimeError> {
    validate_control_root(control_root)?;
    let state_path = control_root.join(SERVICE_GENERATION_STATE_FILE);
    match fs::symlink_metadata(&state_path) {
        Ok(metadata) => read_generation_state(&state_path, &metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let state = ServiceGenerationStateV1::initial();
            persist_generation_state(control_root, state)?;
            Ok(state)
        }
        Err(_) => Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state validation failed",
        )),
    }
}

pub(super) fn load_generation_state(
    control_root: &Path,
) -> Result<ServiceGenerationStateV1, CoreRuntimeError> {
    validate_control_root(control_root)?;
    let state_path = control_root.join(SERVICE_GENERATION_STATE_FILE);
    let metadata = fs::symlink_metadata(&state_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state validation failed",
        )
    })?;
    read_generation_state(&state_path, &metadata)
}

#[cfg(unix)]
pub(super) fn with_generation_state_lock<T>(
    control_root: &Path,
    operation: impl FnOnce() -> Result<T, CoreRuntimeError>,
) -> Result<T, CoreRuntimeError> {
    let file = open_generation_state_lock(control_root)?;
    let _lock = nix::fcntl::Flock::lock(file, nix::fcntl::FlockArg::LockExclusiveNonblock)
        .map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state lock is unavailable",
            )
        })?;
    operation()
}

#[cfg(not(unix))]
pub(super) fn with_generation_state_lock<T>(
    _control_root: &Path,
    operation: impl FnOnce() -> Result<T, CoreRuntimeError>,
) -> Result<T, CoreRuntimeError> {
    operation()
}

#[cfg(unix)]
fn open_generation_state_lock(control_root: &Path) -> Result<fs::File, CoreRuntimeError> {
    validate_control_root(control_root)?;
    let lock_path = control_root.join(SERVICE_GENERATION_LOCK_FILE);
    for _ in 0..2 {
        match fs::symlink_metadata(&lock_path) {
            Ok(path_metadata) => {
                validate_generation_lock_metadata(&path_metadata)?;
                let mut options = OpenOptions::new();
                options
                    .read(true)
                    .write(true)
                    .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
                let file = options.open(&lock_path).map_err(|_| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "service generation state lock open failed",
                    )
                })?;
                let opened_metadata = file.metadata().map_err(|_| {
                    service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "service generation state lock validation failed",
                    )
                })?;
                validate_generation_lock_metadata(&opened_metadata)?;
                if path_metadata.dev() != opened_metadata.dev()
                    || path_metadata.ino() != opened_metadata.ino()
                {
                    return Err(service_error(
                        CoreRuntimeErrorKind::Unavailable,
                        "service generation state lock identity changed",
                    ));
                }
                return Ok(file);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut options = OpenOptions::new();
                options
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .mode(SERVICE_GENERATION_STATE_MODE)
                    .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
                match options.open(&lock_path) {
                    Ok(file) => {
                        file.set_permissions(fs::Permissions::from_mode(
                            SERVICE_GENERATION_STATE_MODE,
                        ))
                        .map_err(|_| {
                            service_error(
                                CoreRuntimeErrorKind::Unavailable,
                                "service generation state lock permission update failed",
                            )
                        })?;
                        validate_generation_lock_metadata(&file.metadata().map_err(|_| {
                            service_error(
                                CoreRuntimeErrorKind::Unavailable,
                                "service generation state lock validation failed",
                            )
                        })?)?;
                        file.sync_all().map_err(|_| {
                            service_error(
                                CoreRuntimeErrorKind::Unavailable,
                                "service generation state lock sync failed",
                            )
                        })?;
                        sync_private_service_directory(control_root)?;
                        return Ok(file);
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(_) => {
                        return Err(service_error(
                            CoreRuntimeErrorKind::Unavailable,
                            "service generation state lock creation failed",
                        ));
                    }
                }
            }
            Err(_) => {
                return Err(service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service generation state lock validation failed",
                ));
            }
        }
    }
    Err(service_error(
        CoreRuntimeErrorKind::Unavailable,
        "service generation state lock creation raced",
    ))
}

#[cfg(unix)]
fn validate_generation_lock_metadata(metadata: &fs::Metadata) -> Result<(), CoreRuntimeError> {
    if !metadata.file_type().is_file()
        || metadata.len() != 0
        || metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != SERVICE_GENERATION_STATE_MODE
        || metadata.nlink() != 1
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state lock ownership, permissions, or type are invalid",
        ));
    }
    Ok(())
}

pub(super) fn cleanup_pending_generation_states(
    control_root: &Path,
) -> Result<(), CoreRuntimeError> {
    validate_control_root(control_root)?;
    let entries = fs::read_dir(control_root).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "control root cleanup scan failed",
        )
    })?;
    let mut removed = false;
    for entry in entries {
        let entry = entry.map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "control root cleanup scan failed",
            )
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if !is_pending_generation_state_name(&name) {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "pending generation state validation failed",
            )
        })?;
        if !is_private_pending_generation_state(&metadata) {
            continue;
        }
        fs::remove_file(path).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "pending generation state cleanup failed",
            )
        })?;
        removed = true;
    }
    if removed {
        sync_private_service_directory(control_root)?;
    }
    Ok(())
}

fn is_pending_generation_state_name(name: &str) -> bool {
    let prefix = format!(".{SERVICE_GENERATION_STATE_FILE}.");
    let Some(identifier) = name
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix(".pending"))
    else {
        return false;
    };
    uuid::Uuid::parse_str(identifier)
        .is_ok_and(|parsed| parsed.hyphenated().to_string() == identifier)
}

fn is_private_pending_generation_state(metadata: &fs::Metadata) -> bool {
    if !metadata.file_type().is_file() || metadata.len() > SERVICE_GENERATION_STATE_MAX_BYTES as u64
    {
        return false;
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 & !SERVICE_GENERATION_STATE_MODE != 0
        || metadata.nlink() != 1
    {
        return false;
    }
    true
}

fn read_generation_state(
    state_path: &Path,
    path_metadata: &fs::Metadata,
) -> Result<ServiceGenerationStateV1, CoreRuntimeError> {
    validate_generation_state_metadata(path_metadata)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let mut file = options.open(state_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state open failed",
        )
    })?;
    let opened_metadata = file.metadata().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state validation failed",
        )
    })?;
    validate_generation_state_metadata(&opened_metadata)?;
    #[cfg(unix)]
    if path_metadata.dev() != opened_metadata.dev() || path_metadata.ino() != opened_metadata.ino()
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state identity changed",
        ));
    }

    let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
    (&mut file)
        .take((SERVICE_GENERATION_STATE_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state read failed",
            )
        })?;
    if bytes.is_empty() || bytes.len() > SERVICE_GENERATION_STATE_MAX_BYTES {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state size is invalid",
        ));
    }
    let state: ServiceGenerationStateV1 = serde_json::from_slice(&bytes).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state is invalid",
        )
    })?;
    if state.schema_version != SERVICE_GENERATION_STATE_SCHEMA_VERSION {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state version is unsupported",
        ));
    }
    Ok(state)
}

fn validate_generation_state_metadata(metadata: &fs::Metadata) -> Result<(), CoreRuntimeError> {
    if !metadata.file_type().is_file()
        || metadata.len() == 0
        || metadata.len() > SERVICE_GENERATION_STATE_MAX_BYTES as u64
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state shape is invalid",
        ));
    }
    #[cfg(unix)]
    if metadata.uid() != nix::unistd::geteuid().as_raw()
        || metadata.permissions().mode() & 0o7777 != SERVICE_GENERATION_STATE_MODE
        || metadata.nlink() != 1
    {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state ownership or permissions are invalid",
        ));
    }
    Ok(())
}

fn persist_generation_state(
    control_root: &Path,
    state: ServiceGenerationStateV1,
) -> Result<(), CoreRuntimeError> {
    persist_generation_state_with_fault(control_root, state, None)
}

pub(super) fn persist_generation_state_with_fault(
    control_root: &Path,
    state: ServiceGenerationStateV1,
    fault: Option<ServiceGenerationStateCommitFault>,
) -> Result<(), CoreRuntimeError> {
    validate_control_root(control_root)?;
    let bytes = serde_json::to_vec(&state).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state serialization failed",
        )
    })?;
    if bytes.is_empty() || bytes.len() > SERVICE_GENERATION_STATE_MAX_BYTES {
        return Err(service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state serialization is invalid",
        ));
    }

    let state_path = control_root.join(SERVICE_GENERATION_STATE_FILE);
    match fs::symlink_metadata(&state_path) {
        Ok(metadata) => validate_generation_state_metadata(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(_) => {
            return Err(service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state validation failed",
            ));
        }
    }

    let temporary_path = control_root.join(format!(
        ".{SERVICE_GENERATION_STATE_FILE}.{}.pending",
        uuid::Uuid::new_v4()
    ));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options
        .mode(SERVICE_GENERATION_STATE_MODE)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW);
    let mut file = options.open(&temporary_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state staging failed",
        )
    })?;
    let staged_identity = pending_file_identity(&file.metadata().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service generation state staging validation failed",
        )
    })?);
    let result = (|| {
        #[cfg(unix)]
        file.set_permissions(fs::Permissions::from_mode(SERVICE_GENERATION_STATE_MODE))
            .map_err(|_| {
                service_error(
                    CoreRuntimeErrorKind::Unavailable,
                    "service generation state staging permission update failed",
                )
            })?;
        inject_generation_state_commit_fault(fault, ServiceGenerationStateCommitFault::Write)?;
        file.write_all(&bytes).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state write failed",
            )
        })?;
        inject_generation_state_commit_fault(fault, ServiceGenerationStateCommitFault::FileSync)?;
        file.sync_all().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state sync failed",
            )
        })?;
        inject_generation_state_commit_fault(fault, ServiceGenerationStateCommitFault::Rename)?;
        fs::rename(&temporary_path, &state_path).map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service generation state commit failed",
            )
        })?;
        inject_generation_state_commit_fault(
            fault,
            ServiceGenerationStateCommitFault::DirectorySync,
        )?;
        sync_private_service_directory(control_root)
    })();
    if result.is_err() {
        remove_created_pending_generation_state(&temporary_path, staged_identity);
    }
    result
}

fn inject_generation_state_commit_fault(
    armed: Option<ServiceGenerationStateCommitFault>,
    stage: ServiceGenerationStateCommitFault,
) -> Result<(), CoreRuntimeError> {
    if armed != Some(stage) {
        return Ok(());
    }
    let diagnostic = match stage {
        ServiceGenerationStateCommitFault::Write => {
            "injected service generation state write failure"
        }
        ServiceGenerationStateCommitFault::FileSync => {
            "injected service generation state file sync failure"
        }
        ServiceGenerationStateCommitFault::Rename => {
            "injected service generation state rename failure"
        }
        ServiceGenerationStateCommitFault::DirectorySync => {
            "injected service generation state directory sync failure"
        }
    };
    Err(service_error(CoreRuntimeErrorKind::Unavailable, diagnostic))
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct PendingFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Clone, Copy)]
struct PendingFileIdentity;

fn pending_file_identity(metadata: &fs::Metadata) -> PendingFileIdentity {
    #[cfg(unix)]
    {
        PendingFileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        PendingFileIdentity
    }
}

fn remove_created_pending_generation_state(path: &Path, identity: PendingFileIdentity) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    #[cfg(unix)]
    let matches = metadata.file_type().is_file()
        && metadata.dev() == identity.device
        && metadata.ino() == identity.inode;
    #[cfg(not(unix))]
    let matches = false;
    if matches {
        let _ = fs::remove_file(path);
    }
}

fn sync_private_service_directory(directory_path: &Path) -> Result<(), CoreRuntimeError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW);
    let directory = options.open(directory_path).map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service directory sync open failed",
        )
    })?;
    validate_service_directory_metadata(
        &directory.metadata().map_err(|_| {
            service_error(
                CoreRuntimeErrorKind::Unavailable,
                "service directory validation failed",
            )
        })?,
        "service directory ownership, permissions, or type are invalid",
    )?;
    directory.sync_all().map_err(|_| {
        service_error(
            CoreRuntimeErrorKind::Unavailable,
            "service directory sync failed",
        )
    })
}

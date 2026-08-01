//! Privileged runtime root and Unix socket filesystem policy.

use std::fs::{self, File};
use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt, chown};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};

use super::error::safe_io_error;

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct SocketIdentity {
    pub(super) device: u64,
    pub(super) inode: u64,
}

pub(super) fn prepare_runtime_root(path: &Path) -> io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service runtime root requires a parent directory",
            )
        })?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    if parent_metadata.file_type().is_symlink()
        || !parent_metadata.is_dir()
        || parent_metadata.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service runtime parent must be a real directory",
        ));
    }
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error),
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service runtime root must be a real directory",
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o711))?;
    fs::canonicalize(path)
}

pub(super) fn bind_service_listener(
    socket_path: &Path,
    allowed_owner_uid: u32,
) -> io::Result<UnixListener> {
    let parent = socket_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Core service IPC socket requires a parent directory",
            )
        })?;
    prepare_service_socket_parent(parent)
        .map_err(|error| safe_io_error(error, "Core service IPC parent setup failed"))?;
    recover_stale_service_socket(socket_path, parent, allowed_owner_uid)
        .map_err(|error| safe_io_error(error, "Core service IPC stale socket check failed"))?;
    let listener = UnixListener::bind(socket_path)
        .map_err(|error| safe_io_error(error, "Core service IPC bind failed"))?;
    let configure_result = configure_service_socket(socket_path, parent, allowed_owner_uid)
        .map_err(|error| safe_io_error(error, "Core service IPC access setup failed"));
    if let Err(error) = configure_result {
        drop(listener);
        let _ = fs::remove_file(socket_path);
        return Err(error);
    }
    Ok(listener)
}

fn recover_stale_service_socket(
    socket_path: &Path,
    parent: &Path,
    allowed_owner_uid: u32,
) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket() {
        return Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            "Core service IPC path is occupied",
        ));
    }
    if metadata.uid() != allowed_owner_uid || metadata.mode() & 0o777 != 0o600 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC stale socket identity is invalid",
        ));
    }
    let identity = SocketIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    match UnixStream::connect(socket_path) {
        Ok(stream) => {
            drop(stream);
            Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                "Core service IPC socket is active",
            ))
        }
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {
            cleanup_socket(socket_path, identity)?;
            sync_directory(parent)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn prepare_service_socket_parent(parent: &Path) -> io::Result<()> {
    match fs::symlink_metadata(parent) {
        Ok(metadata) => validate_service_socket_parent(&metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let ancestor = parent
                .parent()
                .filter(|ancestor| !ancestor.as_os_str().is_empty())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Core service IPC socket parent is invalid",
                    )
                })?;
            let ancestor_metadata = fs::symlink_metadata(ancestor)?;
            validate_service_socket_parent(&ancestor_metadata)?;
            let mut builder = fs::DirBuilder::new();
            builder.mode(0o700);
            builder.create(parent)?;
            let metadata = fs::symlink_metadata(parent)?;
            validate_service_socket_parent(&metadata)
        }
        Err(error) => Err(error),
    }?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o711))?;
    let metadata = fs::symlink_metadata(parent)?;
    validate_service_socket_parent(&metadata)?;
    if metadata.mode() & 0o777 != 0o711 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC parent access policy failed",
        ));
    }
    Ok(())
}

fn validate_service_socket_parent(metadata: &fs::Metadata) -> io::Result<()> {
    if metadata.file_type().is_symlink()
        || !metadata.is_dir()
        || metadata.uid() != nix::unistd::geteuid().as_raw()
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC parent ownership is invalid",
        ));
    }
    Ok(())
}

fn configure_service_socket(
    socket_path: &Path,
    parent: &Path,
    allowed_owner_uid: u32,
) -> io::Result<()> {
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    let metadata = fs::symlink_metadata(socket_path)?;
    if metadata.uid() != allowed_owner_uid {
        chown(socket_path, Some(allowed_owner_uid), None)?;
    }
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    fs::set_permissions(parent, fs::Permissions::from_mode(0o711))?;
    let metadata = fs::symlink_metadata(socket_path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != allowed_owner_uid
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC socket access policy failed",
        ));
    }
    Ok(())
}

pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

pub(super) fn cleanup_socket(path: &Path, identity: SocketIdentity) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.file_type().is_socket()
        || metadata.dev() != identity.device
        || metadata.ino() != identity.inode
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "Core service IPC socket identity changed",
        ));
    }
    fs::remove_file(path)
}

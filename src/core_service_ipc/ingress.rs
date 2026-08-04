//! Verified Runtime Bundle ingress and generation staging.

use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::fd::OwnedFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use nix::errno::Errno;
use nix::fcntl::{AtFlags, OFlag, open, openat, renameat};
use nix::sys::stat::{Mode, SFlag, fstat, fstatat};
use nix::unistd::{UnlinkatFlags, unlinkat};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::constants::{
    EFFECTIVE_CONFIGURATION_MAX_BYTES, MIHOMO_BINARY_MAX_BYTES, PROFILE_RESPONSE_MAX_BYTES,
    RUNTIME_CORE_EXECUTABLE_NAME,
};
use crate::core::{CoreRuntimeError, CoreRuntimeErrorKind, RuntimeBundle};
use crate::digest::is_lower_sha256_hex;
use crate::domain::RuntimeGeneration;
use crate::geodata::{GeoDataAsset, GeoDataCatalog};

use super::socket::sync_directory;

const RUNTIME_MANIFEST_SCHEMA_VERSION: u16 = 1;
const RUNTIME_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_PROVIDER_FILE_MAX: usize = 1_024;

#[derive(Clone)]
pub(super) struct GeoDataIngress {
    root: PathBuf,
    expected_owner_uid: u32,
    catalog: GeoDataCatalog,
}

impl GeoDataIngress {
    pub(super) fn new(root: PathBuf, expected_owner_uid: u32, catalog: GeoDataCatalog) -> Self {
        Self {
            root,
            expected_owner_uid,
            catalog,
        }
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IngressRuntimeManifest {
    schema_version: u16,
    runtime_generation: u64,
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
    configuration_sha256: String,
    executable: String,
    configuration: String,
    provider_files: Vec<IngressManifestFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IngressManifestFile {
    path: String,
    sha256: String,
    size: u64,
}

#[derive(Clone, Copy)]
pub(super) enum BundleIngressError {
    Invalid,
    Unavailable,
}

impl BundleIngressError {
    pub(super) fn into_core(self) -> CoreRuntimeError {
        match self {
            Self::Invalid => {
                CoreRuntimeError::new(CoreRuntimeErrorKind::InvalidBundle, "bundle ingress failed")
            }
            Self::Unavailable => CoreRuntimeError::new(
                CoreRuntimeErrorKind::Unavailable,
                "bundle staging is unavailable",
            ),
        }
    }
}

pub(super) fn stage_runtime_bundle(
    runtime_root: &Path,
    owner_uid: u32,
    bundle: &RuntimeBundle,
    geo_data: Option<&GeoDataIngress>,
) -> Result<RuntimeBundle, CoreRuntimeError> {
    stage_runtime_bundle_inner(runtime_root, owner_uid, bundle, geo_data)
        .map_err(BundleIngressError::into_core)
}

fn stage_runtime_bundle_inner(
    runtime_root: &Path,
    owner_uid: u32,
    bundle: &RuntimeBundle,
    geo_data: Option<&GeoDataIngress>,
) -> Result<RuntimeBundle, BundleIngressError> {
    if !bundle.generation_root.is_absolute()
        || !is_lower_sha256_hex(&bundle.manifest_sha256)
        || !is_lower_sha256_hex(&bundle.compiler_policy_sha256)
        || !is_lower_sha256_hex(&bundle.mihomo_binary_sha256)
    {
        return Err(BundleIngressError::Invalid);
    }
    let final_root = runtime_root.join(format!("generation-{:020}", bundle.generation.0));
    if generation_directory_exists(&final_root)? {
        if let Some(geo_data) = geo_data {
            ensure_staged_geo_data(&final_root, nix::unistd::geteuid().as_raw(), geo_data)?;
        }
        return Ok(staged_bundle(bundle, final_root));
    }

    let source_root = open_source_root(&bundle.generation_root, owner_uid)?;
    let manifest_bytes = read_source_bytes(
        &source_root,
        owner_uid,
        Path::new("manifest.json"),
        RUNTIME_MANIFEST_MAX_BYTES,
    )?;
    if sha256_hex(&manifest_bytes) != bundle.manifest_sha256 {
        return Err(BundleIngressError::Invalid);
    }
    let manifest: IngressRuntimeManifest =
        serde_json::from_slice(&manifest_bytes).map_err(|_| BundleIngressError::Invalid)?;
    validate_ingress_manifest(&manifest, bundle)?;

    let pending_root = create_pending_root(runtime_root, bundle.generation)?;
    let stage_result = stage_pending_bundle(
        &source_root,
        owner_uid,
        &pending_root,
        &manifest_bytes,
        &manifest,
        geo_data,
    );
    if let Err(error) = stage_result {
        let _ = remove_pending_root(runtime_root, &pending_root);
        return Err(error);
    }

    match fs::rename(&pending_root, &final_root) {
        Ok(()) => sync_directory(runtime_root).map_err(|_| BundleIngressError::Unavailable)?,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            remove_pending_root(runtime_root, &pending_root)?;
            if !generation_directory_exists(&final_root)? {
                return Err(BundleIngressError::Unavailable);
            }
        }
        Err(_) => {
            let _ = remove_pending_root(runtime_root, &pending_root);
            return Err(BundleIngressError::Unavailable);
        }
    }
    Ok(staged_bundle(bundle, final_root))
}

fn validate_ingress_manifest(
    manifest: &IngressRuntimeManifest,
    bundle: &RuntimeBundle,
) -> Result<(), BundleIngressError> {
    if manifest.schema_version != RUNTIME_MANIFEST_SCHEMA_VERSION
        || manifest.runtime_generation != bundle.generation.0
        || manifest.compiler_policy_sha256 != bundle.compiler_policy_sha256
        || manifest.mihomo_binary_sha256 != bundle.mihomo_binary_sha256
        || !is_lower_sha256_hex(&manifest.configuration_sha256)
        || manifest.executable != RUNTIME_CORE_EXECUTABLE_NAME
        || manifest.configuration != "config.yaml"
        || manifest.provider_files.len() > RUNTIME_PROVIDER_FILE_MAX
    {
        return Err(BundleIngressError::Invalid);
    }
    let mut previous: Option<&str> = None;
    for file in &manifest.provider_files {
        if previous.is_some_and(|previous| previous >= file.path.as_str())
            || !valid_provider_path(&file.path)
            || !is_lower_sha256_hex(&file.sha256)
            || file.size > PROFILE_RESPONSE_MAX_BYTES as u64
        {
            return Err(BundleIngressError::Invalid);
        }
        previous = Some(&file.path);
    }
    Ok(())
}

fn stage_pending_bundle(
    source_root: &OwnedFd,
    owner_uid: u32,
    pending_root: &Path,
    manifest_bytes: &[u8],
    manifest: &IngressRuntimeManifest,
    geo_data: Option<&GeoDataIngress>,
) -> Result<(), BundleIngressError> {
    copy_verified_file(
        source_root,
        owner_uid,
        BundleFileCopy {
            relative_path: Path::new(RUNTIME_CORE_EXECUTABLE_NAME),
            destination: &pending_root.join(RUNTIME_CORE_EXECUTABLE_NAME),
            limit: MIHOMO_BINARY_MAX_BYTES,
            expected_size: None,
            expected_sha256: &manifest.mihomo_binary_sha256,
            mode: 0o500,
        },
    )?;
    copy_verified_file(
        source_root,
        owner_uid,
        BundleFileCopy {
            relative_path: Path::new("config.yaml"),
            destination: &pending_root.join("config.yaml"),
            limit: EFFECTIVE_CONFIGURATION_MAX_BYTES,
            expected_size: None,
            expected_sha256: &manifest.configuration_sha256,
            mode: 0o400,
        },
    )?;
    for provider in &manifest.provider_files {
        let relative = Path::new(&provider.path);
        let destination = pending_root.join(relative);
        create_destination_parents(pending_root, &destination)?;
        copy_verified_file(
            source_root,
            owner_uid,
            BundleFileCopy {
                relative_path: relative,
                destination: &destination,
                limit: PROFILE_RESPONSE_MAX_BYTES,
                expected_size: Some(provider.size),
                expected_sha256: &provider.sha256,
                mode: 0o400,
            },
        )?;
    }
    if let Some(geo_data) = geo_data {
        stage_geo_data(pending_root, geo_data)?;
    }
    write_new_file(&pending_root.join("manifest.json"), manifest_bytes, 0o400)?;
    sync_tree_directories(pending_root)?;
    Ok(())
}

fn stage_geo_data(
    pending_root: &Path,
    geo_data: &GeoDataIngress,
) -> Result<(), BundleIngressError> {
    let source_root = open_source_root(&geo_data.root, geo_data.expected_owner_uid)?;
    for asset in geo_data.catalog.assets() {
        let relative_path = Path::new(asset.file_name());
        if !is_single_file_name(relative_path) {
            return Err(BundleIngressError::Invalid);
        }
        let limit = usize::try_from(asset.size()).map_err(|_| BundleIngressError::Invalid)?;
        let destination = pending_root.join(relative_path);
        match fs::symlink_metadata(&destination) {
            Ok(_) => return Err(BundleIngressError::Invalid),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(BundleIngressError::Unavailable),
        }
        copy_verified_file(
            &source_root,
            geo_data.expected_owner_uid,
            BundleFileCopy {
                relative_path,
                destination: &destination,
                limit,
                expected_size: Some(asset.size()),
                expected_sha256: asset.sha256(),
                mode: 0o400,
            },
        )?;
    }
    Ok(())
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum StagedGeoDataState {
    Valid,
    Repairable,
}

fn ensure_staged_geo_data(
    generation_path: &Path,
    generation_owner_uid: u32,
    geo_data: &GeoDataIngress,
) -> Result<(), BundleIngressError> {
    let generation_root = open_generation_root(generation_path, generation_owner_uid)?;
    let mut removed_pending = false;
    let mut needs_repair = false;
    for asset in geo_data.catalog.assets() {
        removed_pending |=
            remove_interrupted_geo_data(&generation_root, generation_owner_uid, asset)?;
        needs_repair |= inspect_staged_geo_data(&generation_root, generation_owner_uid, asset)?
            == StagedGeoDataState::Repairable;
    }
    if removed_pending {
        sync_directory_descriptor(&generation_root)?;
    }
    if !needs_repair {
        return Ok(());
    }

    let source_root = open_source_root(&geo_data.root, geo_data.expected_owner_uid)?;
    for asset in geo_data.catalog.assets() {
        if let Err(error) = stage_geo_data_repair(
            &source_root,
            geo_data.expected_owner_uid,
            generation_path,
            &generation_root,
            generation_owner_uid,
            asset,
        ) {
            let _ =
                cleanup_geo_data_repair(&generation_root, generation_owner_uid, &geo_data.catalog);
            return Err(error);
        }
    }
    sync_directory_descriptor(&generation_root)?;

    for asset in geo_data.catalog.assets() {
        if let Err(error) =
            validate_geo_data_replacement_target(&generation_root, generation_owner_uid, asset)
                .and_then(|()| {
                    renameat(
                        &generation_root,
                        &pending_geo_data_name(asset),
                        &generation_root,
                        Path::new(asset.file_name()),
                    )
                    .map_err(|_| BundleIngressError::Unavailable)
                })
        {
            let _ =
                cleanup_geo_data_repair(&generation_root, generation_owner_uid, &geo_data.catalog);
            return Err(error);
        }
    }
    sync_directory_descriptor(&generation_root)?;

    for asset in geo_data.catalog.assets() {
        if inspect_staged_geo_data(&generation_root, generation_owner_uid, asset)?
            != StagedGeoDataState::Valid
        {
            return Err(BundleIngressError::Invalid);
        }
    }
    Ok(())
}

fn open_generation_root(path: &Path, owner_uid: u32) -> Result<OwnedFd, BundleIngressError> {
    let descriptor = open(
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| BundleIngressError::Invalid)?;
    let metadata = fstat(&descriptor).map_err(|_| BundleIngressError::Invalid)?;
    if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFDIR
        || metadata.st_uid != owner_uid
        || metadata.st_mode & 0o777 != 0o700
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok(descriptor)
}

fn inspect_staged_geo_data(
    generation_root: &OwnedFd,
    owner_uid: u32,
    asset: &GeoDataAsset,
) -> Result<StagedGeoDataState, BundleIngressError> {
    inspect_geo_data_file(
        generation_root,
        owner_uid,
        Path::new(asset.file_name()),
        asset,
    )
}

fn inspect_geo_data_file(
    generation_root: &OwnedFd,
    owner_uid: u32,
    path: &Path,
    asset: &GeoDataAsset,
) -> Result<StagedGeoDataState, BundleIngressError> {
    if !is_single_file_name(path) {
        return Err(BundleIngressError::Invalid);
    }
    let metadata = match fstatat(generation_root, path, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(Errno::ENOENT) => return Ok(StagedGeoDataState::Repairable),
        Err(_) => return Err(BundleIngressError::Invalid),
    };
    if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFREG
        || metadata.st_uid != owner_uid
        || metadata.st_nlink != 1
        || metadata.st_size < 0
    {
        return Err(BundleIngressError::Invalid);
    }
    if metadata.st_size as u64 != asset.size() || metadata.st_mode & 0o777 != 0o400 {
        return Ok(StagedGeoDataState::Repairable);
    }
    let descriptor = openat(
        generation_root,
        path,
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| BundleIngressError::Invalid)?;
    let opened = fstat(&descriptor).map_err(|_| BundleIngressError::Invalid)?;
    if opened.st_dev != metadata.st_dev || opened.st_ino != metadata.st_ino {
        return Err(BundleIngressError::Invalid);
    }
    if file_digest_matches(descriptor, asset.size(), asset.sha256())? {
        Ok(StagedGeoDataState::Valid)
    } else {
        Ok(StagedGeoDataState::Repairable)
    }
}

fn remove_interrupted_geo_data(
    generation_root: &OwnedFd,
    owner_uid: u32,
    asset: &GeoDataAsset,
) -> Result<bool, BundleIngressError> {
    let pending = pending_geo_data_name(asset);
    let metadata = match fstatat(generation_root, &pending, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(Errno::ENOENT) => return Ok(false),
        Err(_) => return Err(BundleIngressError::Invalid),
    };
    if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFREG
        || metadata.st_uid != owner_uid
        || metadata.st_nlink != 1
        || metadata.st_mode & 0o377 != 0
    {
        return Err(BundleIngressError::Invalid);
    }
    unlinkat(generation_root, &pending, UnlinkatFlags::NoRemoveDir)
        .map_err(|_| BundleIngressError::Unavailable)?;
    Ok(true)
}

fn stage_geo_data_repair(
    source_root: &OwnedFd,
    source_owner_uid: u32,
    generation_path: &Path,
    generation_root: &OwnedFd,
    generation_owner_uid: u32,
    asset: &GeoDataAsset,
) -> Result<(), BundleIngressError> {
    let relative_path = Path::new(asset.file_name());
    if !is_single_file_name(relative_path) {
        return Err(BundleIngressError::Invalid);
    }
    let pending = pending_geo_data_name(asset);
    copy_verified_file(
        source_root,
        source_owner_uid,
        BundleFileCopy {
            relative_path,
            destination: &generation_path.join(&pending),
            limit: usize::try_from(asset.size()).map_err(|_| BundleIngressError::Invalid)?,
            expected_size: Some(asset.size()),
            expected_sha256: asset.sha256(),
            mode: 0o400,
        },
    )?;
    if inspect_geo_data_file(generation_root, generation_owner_uid, &pending, asset)?
        != StagedGeoDataState::Valid
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok(())
}

fn validate_geo_data_replacement_target(
    generation_root: &OwnedFd,
    owner_uid: u32,
    asset: &GeoDataAsset,
) -> Result<(), BundleIngressError> {
    let path = Path::new(asset.file_name());
    let metadata = match fstatat(generation_root, path, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(metadata) => metadata,
        Err(Errno::ENOENT) => return Ok(()),
        Err(_) => return Err(BundleIngressError::Invalid),
    };
    if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFREG
        || metadata.st_uid != owner_uid
        || metadata.st_nlink != 1
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok(())
}

fn cleanup_geo_data_repair(
    generation_root: &OwnedFd,
    owner_uid: u32,
    catalog: &GeoDataCatalog,
) -> Result<(), BundleIngressError> {
    for asset in catalog.assets() {
        remove_interrupted_geo_data(generation_root, owner_uid, asset)?;
    }
    sync_directory_descriptor(generation_root)
}

fn pending_geo_data_name(asset: &GeoDataAsset) -> PathBuf {
    PathBuf::from(format!(".ratash-geodata-{}.pending", asset.file_name()))
}

fn sync_directory_descriptor(directory: &OwnedFd) -> Result<(), BundleIngressError> {
    File::from(
        directory
            .try_clone()
            .map_err(|_| BundleIngressError::Unavailable)?,
    )
    .sync_all()
    .map_err(|_| BundleIngressError::Unavailable)
}

fn file_digest_matches(
    descriptor: OwnedFd,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<bool, BundleIngressError> {
    let mut source = File::from(descriptor);
    let mut hasher = Sha256::new();
    let mut read_bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|_| BundleIngressError::Invalid)?;
        if read == 0 {
            break;
        }
        read_bytes = read_bytes
            .checked_add(read as u64)
            .ok_or(BundleIngressError::Invalid)?;
        if read_bytes > expected_size {
            return Ok(false);
        }
        hasher.update(&buffer[..read]);
    }
    Ok(read_bytes == expected_size && encode_digest(hasher.finalize().as_ref()) == expected_sha256)
}

fn is_single_file_name(path: &Path) -> bool {
    let mut components = path.components();
    matches!(components.next(), Some(Component::Normal(_))) && components.next().is_none()
}

fn open_source_root(path: &Path, owner_uid: u32) -> Result<OwnedFd, BundleIngressError> {
    let descriptor = open(
        path,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| BundleIngressError::Invalid)?;
    let metadata = fstat(&descriptor).map_err(|_| BundleIngressError::Invalid)?;
    if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFDIR
        || metadata.st_uid != owner_uid
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok(descriptor)
}

fn open_source_file(
    root: &OwnedFd,
    owner_uid: u32,
    path: &Path,
) -> Result<(OwnedFd, u64), BundleIngressError> {
    let components = path
        .components()
        .map(|component| match component {
            Component::Normal(component) => Ok(component),
            _ => Err(BundleIngressError::Invalid),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (file_name, directories) = components.split_last().ok_or(BundleIngressError::Invalid)?;
    let mut directory = root.try_clone().map_err(|_| BundleIngressError::Invalid)?;
    for component in directories {
        directory = openat(
            &directory,
            Path::new(component),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| BundleIngressError::Invalid)?;
        let metadata = fstat(&directory).map_err(|_| BundleIngressError::Invalid)?;
        if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFDIR
            || metadata.st_uid != owner_uid
        {
            return Err(BundleIngressError::Invalid);
        }
    }
    let descriptor = openat(
        &directory,
        Path::new(file_name),
        OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| BundleIngressError::Invalid)?;
    let metadata = fstat(&descriptor).map_err(|_| BundleIngressError::Invalid)?;
    if SFlag::from_bits_truncate(metadata.st_mode) & SFlag::S_IFMT != SFlag::S_IFREG
        || metadata.st_size < 0
        || metadata.st_uid != owner_uid
    {
        return Err(BundleIngressError::Invalid);
    }
    Ok((descriptor, metadata.st_size as u64))
}

fn read_source_bytes(
    root: &OwnedFd,
    owner_uid: u32,
    path: &Path,
    limit: usize,
) -> Result<Vec<u8>, BundleIngressError> {
    let (descriptor, size) = open_source_file(root, owner_uid, path)?;
    if size > limit as u64 {
        return Err(BundleIngressError::Invalid);
    }
    let mut content = Vec::with_capacity(size as usize);
    File::from(descriptor)
        .take((limit as u64).saturating_add(1))
        .read_to_end(&mut content)
        .map_err(|_| BundleIngressError::Invalid)?;
    if content.len() > limit || content.len() as u64 != size {
        return Err(BundleIngressError::Invalid);
    }
    Ok(content)
}

struct BundleFileCopy<'a> {
    relative_path: &'a Path,
    destination: &'a Path,
    limit: usize,
    expected_size: Option<u64>,
    expected_sha256: &'a str,
    mode: u32,
}

fn copy_verified_file(
    source_root: &OwnedFd,
    owner_uid: u32,
    copy: BundleFileCopy<'_>,
) -> Result<(), BundleIngressError> {
    let (descriptor, initial_size) = open_source_file(source_root, owner_uid, copy.relative_path)?;
    if initial_size > copy.limit as u64
        || copy.expected_size.is_some_and(|size| size != initial_size)
    {
        return Err(BundleIngressError::Invalid);
    }
    let mut source = File::from(descriptor);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(copy.mode);
    let mut target = options
        .open(copy.destination)
        .map_err(|_| BundleIngressError::Unavailable)?;
    let mut hasher = Sha256::new();
    let mut copied = 0_u64;
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = source
            .read(&mut buffer)
            .map_err(|_| BundleIngressError::Invalid)?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(read as u64)
            .ok_or(BundleIngressError::Invalid)?;
        if copied > copy.limit as u64 {
            return Err(BundleIngressError::Invalid);
        }
        hasher.update(&buffer[..read]);
        target
            .write_all(&buffer[..read])
            .map_err(|_| BundleIngressError::Unavailable)?;
    }
    if copied != initial_size
        || copy.expected_size.is_some_and(|size| size != copied)
        || encode_digest(hasher.finalize().as_ref()) != copy.expected_sha256
    {
        return Err(BundleIngressError::Invalid);
    }
    target
        .set_permissions(fs::Permissions::from_mode(copy.mode))
        .and_then(|()| target.sync_all())
        .map_err(|_| BundleIngressError::Unavailable)
}

fn create_destination_parents(
    pending_root: &Path,
    destination: &Path,
) -> Result<(), BundleIngressError> {
    let parent = destination.parent().ok_or(BundleIngressError::Invalid)?;
    if parent == pending_root {
        return Ok(());
    }
    let relative = parent
        .strip_prefix(pending_root)
        .map_err(|_| BundleIngressError::Invalid)?;
    let mut current = pending_root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(BundleIngressError::Invalid);
        };
        current.push(component);
        match fs::create_dir(&current) {
            Ok(()) => fs::set_permissions(&current, fs::Permissions::from_mode(0o700))
                .map_err(|_| BundleIngressError::Unavailable)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let metadata =
                    fs::symlink_metadata(&current).map_err(|_| BundleIngressError::Unavailable)?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(BundleIngressError::Unavailable);
                }
            }
            Err(_) => return Err(BundleIngressError::Unavailable),
        }
    }
    Ok(())
}

fn write_new_file(path: &Path, content: &[u8], mode: u32) -> Result<(), BundleIngressError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(mode);
    let mut file = options
        .open(path)
        .map_err(|_| BundleIngressError::Unavailable)?;
    file.write_all(content)
        .and_then(|()| file.set_permissions(fs::Permissions::from_mode(mode)))
        .and_then(|()| file.sync_all())
        .map_err(|_| BundleIngressError::Unavailable)
}

fn create_pending_root(
    runtime_root: &Path,
    generation: RuntimeGeneration,
) -> Result<PathBuf, BundleIngressError> {
    for _ in 0..4 {
        let pending = runtime_root.join(format!(
            ".generation-{:020}-{}.pending",
            generation.0,
            uuid::Uuid::new_v4()
        ));
        match fs::create_dir(&pending) {
            Ok(()) => {
                if fs::set_permissions(&pending, fs::Permissions::from_mode(0o700)).is_err() {
                    let _ = fs::remove_dir(&pending);
                    return Err(BundleIngressError::Unavailable);
                }
                return Ok(pending);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(BundleIngressError::Unavailable),
        }
    }
    Err(BundleIngressError::Unavailable)
}

fn remove_pending_root(runtime_root: &Path, pending: &Path) -> Result<(), BundleIngressError> {
    if pending.parent() != Some(runtime_root)
        || !pending
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".generation-") && name.ends_with(".pending"))
    {
        return Err(BundleIngressError::Unavailable);
    }
    fs::remove_dir_all(pending).map_err(|_| BundleIngressError::Unavailable)
}

fn generation_directory_exists(path: &Path) -> Result<bool, BundleIngressError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_symlink() && metadata.is_dir() => Ok(true),
        Ok(_) => Err(BundleIngressError::Unavailable),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(BundleIngressError::Unavailable),
    }
}

fn staged_bundle(bundle: &RuntimeBundle, generation_root: PathBuf) -> RuntimeBundle {
    RuntimeBundle {
        generation: bundle.generation,
        generation_root,
        manifest_sha256: bundle.manifest_sha256.clone(),
        compiler_policy_sha256: bundle.compiler_policy_sha256.clone(),
        mihomo_binary_sha256: bundle.mihomo_binary_sha256.clone(),
    }
}

fn valid_provider_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && !matches!(value, "manifest.json" | "config.yaml" | "mihomo")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn sha256_hex(content: &[u8]) -> String {
    encode_digest(Sha256::digest(content).as_ref())
}

fn encode_digest(digest: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn sync_tree_directories(root: &Path) -> Result<(), BundleIngressError> {
    let mut directories = vec![root.to_path_buf()];
    let mut pending = VecDeque::from([root.to_path_buf()]);
    while let Some(directory) = pending.pop_front() {
        for entry in fs::read_dir(&directory).map_err(|_| BundleIngressError::Unavailable)? {
            let entry = entry.map_err(|_| BundleIngressError::Unavailable)?;
            let file_type = entry
                .file_type()
                .map_err(|_| BundleIngressError::Unavailable)?;
            if file_type.is_symlink() {
                return Err(BundleIngressError::Unavailable);
            }
            if file_type.is_dir() {
                let path = entry.path();
                directories.push(path.clone());
                pending.push_back(path);
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory).map_err(|_| BundleIngressError::Unavailable)?;
    }
    Ok(())
}

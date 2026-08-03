use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use crate::config::{EffectiveConfiguration, ProviderKind};
use crate::constants::{
    EFFECTIVE_CONFIGURATION_MAX_BYTES, MIHOMO_BINARY_MAX_BYTES, PROFILE_RESPONSE_MAX_BYTES,
};
use crate::core::RuntimeBundle;
use crate::digest::is_lower_sha256_hex;
use crate::domain::RuntimeGeneration;
use crate::service::{RuntimeManifestFileV1, RuntimeManifestV1};

const MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_GENERATION_SCAN_LIMIT: usize = 4_096;

struct ProviderStagingPlan {
    destinations: Vec<PathBuf>,
    local_files: Vec<StagedProviderFile>,
}

struct StagedProviderFile {
    path: PathBuf,
    content: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeBundleStageErrorKind {
    InvalidPolicy,
    RootUnavailable,
    InvalidBinary,
    BinaryIdentityMismatch,
    CompilerPolicyMismatch,
    ConfigurationTooLarge,
    InvalidProviderPath,
    ProviderUnavailable,
    ExistingGenerationMismatch,
    Io,
}

pub struct RuntimeBundleStageError {
    kind: RuntimeBundleStageErrorKind,
    source: Option<io::Error>,
}

impl RuntimeBundleStageError {
    #[must_use]
    pub const fn kind(&self) -> RuntimeBundleStageErrorKind {
        self.kind
    }

    const fn new(kind: RuntimeBundleStageErrorKind) -> Self {
        Self { kind, source: None }
    }

    fn io(source: io::Error) -> Self {
        Self {
            kind: RuntimeBundleStageErrorKind::Io,
            source: Some(source),
        }
    }
}

impl fmt::Debug for RuntimeBundleStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeBundleStageError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for RuntimeBundleStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            RuntimeBundleStageErrorKind::InvalidPolicy => {
                "the Runtime Bundle staging policy is invalid"
            }
            RuntimeBundleStageErrorKind::RootUnavailable => {
                "the Runtime Bundle staging root is unavailable"
            }
            RuntimeBundleStageErrorKind::InvalidBinary => {
                "the bundled Mihomo executable is invalid"
            }
            RuntimeBundleStageErrorKind::BinaryIdentityMismatch => {
                "the bundled Mihomo executable identity changed"
            }
            RuntimeBundleStageErrorKind::CompilerPolicyMismatch => {
                "the configuration compiler policy is incompatible"
            }
            RuntimeBundleStageErrorKind::ConfigurationTooLarge => {
                "the Effective Configuration exceeds its size limit"
            }
            RuntimeBundleStageErrorKind::InvalidProviderPath => {
                "a provider staging path is invalid"
            }
            RuntimeBundleStageErrorKind::ProviderUnavailable => {
                "a local provider source is unavailable"
            }
            RuntimeBundleStageErrorKind::ExistingGenerationMismatch => {
                "the existing Runtime Generation has different content"
            }
            RuntimeBundleStageErrorKind::Io => "Runtime Bundle staging failed",
        })
    }
}

impl std::error::Error for RuntimeBundleStageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeGenerationRetention {
    pub current: Option<RuntimeGeneration>,
    pub previous: Option<RuntimeGeneration>,
    pub prepared: Option<RuntimeGeneration>,
}

impl RuntimeGenerationRetention {
    #[must_use]
    pub const fn new(
        current: Option<RuntimeGeneration>,
        previous: Option<RuntimeGeneration>,
        prepared: Option<RuntimeGeneration>,
    ) -> Self {
        Self {
            current,
            previous,
            prepared,
        }
    }

    fn retained(self) -> BTreeSet<RuntimeGeneration> {
        [self.current, self.previous, self.prepared]
            .into_iter()
            .flatten()
            .collect()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RuntimeGenerationPruneResult {
    pub scanned_entries: usize,
    pub removed_generations: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeGenerationPruneErrorKind {
    RootUnavailable,
    UnsafeEntry,
    Io,
}

pub struct RuntimeGenerationPruneError {
    kind: RuntimeGenerationPruneErrorKind,
}

impl RuntimeGenerationPruneError {
    #[must_use]
    pub const fn kind(&self) -> RuntimeGenerationPruneErrorKind {
        self.kind
    }

    const fn new(kind: RuntimeGenerationPruneErrorKind) -> Self {
        Self { kind }
    }
}

impl fmt::Debug for RuntimeGenerationPruneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeGenerationPruneError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for RuntimeGenerationPruneError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            RuntimeGenerationPruneErrorKind::RootUnavailable => {
                "the Runtime Generation root is unavailable"
            }
            RuntimeGenerationPruneErrorKind::UnsafeEntry => {
                "the Runtime Generation root contains an unsafe entry"
            }
            RuntimeGenerationPruneErrorKind::Io => "Runtime Generation cleanup failed",
        })
    }
}

impl std::error::Error for RuntimeGenerationPruneError {}

#[derive(Clone)]
struct RuntimeGenerationEntry {
    generation: RuntimeGeneration,
    path: PathBuf,
    device: u64,
    inode: u64,
    owner_uid: u32,
}

pub fn prune_runtime_generations(
    root: &Path,
    retention: RuntimeGenerationRetention,
) -> Result<RuntimeGenerationPruneResult, RuntimeGenerationPruneError> {
    prune_runtime_generations_with_reserved(root, retention, &[])
}

pub(crate) fn inspect_runtime_generations_with_reserved(
    root: &Path,
    reserved_directories: &[&str],
) -> Result<Vec<RuntimeGeneration>, RuntimeGenerationPruneError> {
    let (entries, _, _) = scan_runtime_generations(root, reserved_directories)?;
    Ok(entries.into_iter().map(|entry| entry.generation).collect())
}

pub(crate) fn prune_runtime_generations_with_reserved(
    root: &Path,
    retention: RuntimeGenerationRetention,
    reserved_directories: &[&str],
) -> Result<RuntimeGenerationPruneResult, RuntimeGenerationPruneError> {
    prune_runtime_generations_with_reserved_and_hook(root, retention, reserved_directories, |_| {})
}

fn prune_runtime_generations_with_reserved_and_hook(
    root: &Path,
    retention: RuntimeGenerationRetention,
    reserved_directories: &[&str],
    mut before_quarantine: impl FnMut(&Path),
) -> Result<RuntimeGenerationPruneResult, RuntimeGenerationPruneError> {
    let (entries, quarantines, scanned_entries) =
        scan_runtime_generations(root, reserved_directories)?;
    let retained = retention.retained();
    let stale = entries
        .into_iter()
        .filter(|entry| !retained.contains(&entry.generation))
        .collect::<Vec<_>>();

    for quarantine in &quarantines {
        verify_generation_identity(quarantine)?;
    }
    for entry in &stale {
        verify_generation_identity(entry)?;
    }
    for quarantine in &quarantines {
        fs::remove_dir_all(&quarantine.path)
            .map_err(|_| RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::Io))?;
    }

    let mut removed_generations = quarantines.len();
    for entry in &stale {
        before_quarantine(&entry.path);
        let quarantine = root.join(format!(
            ".generation-{:020}-{}.pruning",
            entry.generation.0,
            uuid::Uuid::new_v4()
        ));
        match fs::symlink_metadata(&quarantine) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Ok(_) | Err(_) => {
                return Err(RuntimeGenerationPruneError::new(
                    RuntimeGenerationPruneErrorKind::UnsafeEntry,
                ));
            }
        }
        fs::rename(&entry.path, &quarantine)
            .map_err(|_| RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::Io))?;
        let quarantined = RuntimeGenerationEntry {
            path: quarantine,
            ..entry.clone()
        };
        verify_generation_identity(&quarantined)?;
        fs::remove_dir_all(&quarantined.path)
            .map_err(|_| RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::Io))?;
        removed_generations += 1;
    }
    if removed_generations > 0 {
        File::open(root)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::Io))?;
    }

    Ok(RuntimeGenerationPruneResult {
        scanned_entries,
        removed_generations,
    })
}

fn scan_runtime_generations(
    root: &Path,
    reserved_directories: &[&str],
) -> Result<
    (
        Vec<RuntimeGenerationEntry>,
        Vec<RuntimeGenerationEntry>,
        usize,
    ),
    RuntimeGenerationPruneError,
> {
    let root_metadata = fs::symlink_metadata(root).map_err(|_| {
        RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::RootUnavailable)
    })?;
    if root_metadata.file_type().is_symlink()
        || !root_metadata.is_dir()
        || root_metadata.mode() & 0o022 != 0
    {
        return Err(RuntimeGenerationPruneError::new(
            RuntimeGenerationPruneErrorKind::RootUnavailable,
        ));
    }
    let owner_uid = root_metadata.uid();
    let mut entries = Vec::new();
    let mut quarantines = Vec::new();
    let mut scanned_entries = 0_usize;
    let directory = fs::read_dir(root).map_err(|_| {
        RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::RootUnavailable)
    })?;
    for entry in directory {
        let entry = entry
            .map_err(|_| RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::Io))?;
        scanned_entries = scanned_entries.checked_add(1).ok_or_else(|| {
            RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::UnsafeEntry)
        })?;
        if scanned_entries > RUNTIME_GENERATION_SCAN_LIMIT {
            return Err(RuntimeGenerationPruneError::new(
                RuntimeGenerationPruneErrorKind::UnsafeEntry,
            ));
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(RuntimeGenerationPruneError::new(
                RuntimeGenerationPruneErrorKind::UnsafeEntry,
            ));
        };
        let metadata = fs::symlink_metadata(entry.path()).map_err(|_| {
            RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::UnsafeEntry)
        })?;
        if reserved_directories.contains(&name) {
            if metadata.file_type().is_symlink()
                || !metadata.is_dir()
                || metadata.uid() != owner_uid
                || metadata.mode() & 0o022 != 0
            {
                return Err(RuntimeGenerationPruneError::new(
                    RuntimeGenerationPruneErrorKind::UnsafeEntry,
                ));
            }
            continue;
        }
        let (generation, quarantine) = if let Some(generation) = parse_runtime_generation_name(name)
        {
            (generation, false)
        } else if let Some(generation) = parse_runtime_quarantine_name(name) {
            (generation, true)
        } else {
            return Err(RuntimeGenerationPruneError::new(
                RuntimeGenerationPruneErrorKind::UnsafeEntry,
            ));
        };
        if !strict_generation_metadata(&metadata, owner_uid) {
            return Err(RuntimeGenerationPruneError::new(
                RuntimeGenerationPruneErrorKind::UnsafeEntry,
            ));
        }
        let entry = RuntimeGenerationEntry {
            generation,
            path: entry.path(),
            device: metadata.dev(),
            inode: metadata.ino(),
            owner_uid,
        };
        if quarantine {
            quarantines.push(entry);
        } else {
            entries.push(entry);
        }
    }
    entries.sort_by_key(|entry| entry.generation);
    quarantines.sort_by_key(|entry| entry.generation);
    Ok((entries, quarantines, scanned_entries))
}

fn parse_runtime_generation_name(name: &str) -> Option<RuntimeGeneration> {
    let digits = name.strip_prefix("generation-")?;
    if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let generation = digits.parse::<u64>().ok()?;
    (generation > 0 && name == format!("generation-{generation:020}"))
        .then_some(RuntimeGeneration(generation))
}

fn parse_runtime_quarantine_name(name: &str) -> Option<RuntimeGeneration> {
    let value = name.strip_prefix(".generation-")?;
    let (generation, suffix) = value.split_once('-')?;
    let uuid = suffix.strip_suffix(".pruning")?;
    if generation.len() != 20
        || !generation.bytes().all(|byte| byte.is_ascii_digit())
        || uuid.len() != 36
    {
        return None;
    }
    let generation = generation.parse::<u64>().ok()?;
    let parsed_uuid = uuid::Uuid::parse_str(uuid).ok()?;
    (generation > 0 && name == format!(".generation-{generation:020}-{parsed_uuid}.pruning"))
        .then_some(RuntimeGeneration(generation))
}

fn verify_generation_identity(
    entry: &RuntimeGenerationEntry,
) -> Result<(), RuntimeGenerationPruneError> {
    let metadata = fs::symlink_metadata(&entry.path).map_err(|_| {
        RuntimeGenerationPruneError::new(RuntimeGenerationPruneErrorKind::UnsafeEntry)
    })?;
    if !strict_generation_metadata(&metadata, entry.owner_uid)
        || metadata.dev() != entry.device
        || metadata.ino() != entry.inode
    {
        return Err(RuntimeGenerationPruneError::new(
            RuntimeGenerationPruneErrorKind::UnsafeEntry,
        ));
    }
    Ok(())
}

fn strict_generation_metadata(metadata: &fs::Metadata, owner_uid: u32) -> bool {
    !metadata.file_type().is_symlink()
        && metadata.is_dir()
        && metadata.uid() == owner_uid
        && metadata.mode() & 0o777 == 0o700
}

pub struct RuntimeBundleStager {
    root: PathBuf,
    binary: PathBuf,
    binary_sha256: String,
    compiler_policy_sha256: String,
}

impl RuntimeBundleStager {
    pub fn new(
        root: impl Into<PathBuf>,
        binary: impl Into<PathBuf>,
        binary_sha256: impl Into<String>,
        compiler_policy_sha256: impl Into<String>,
    ) -> Result<Self, RuntimeBundleStageError> {
        let root = root.into();
        let binary = binary.into();
        let binary_sha256 = binary_sha256.into();
        let compiler_policy_sha256 = compiler_policy_sha256.into();
        if !root.is_absolute()
            || !binary.is_absolute()
            || !is_lower_sha256_hex(&binary_sha256)
            || !is_lower_sha256_hex(&compiler_policy_sha256)
        {
            return Err(RuntimeBundleStageError::new(
                RuntimeBundleStageErrorKind::InvalidPolicy,
            ));
        }
        prepare_private_root(&root)?;
        let root = fs::canonicalize(root).map_err(|_| {
            RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::RootUnavailable)
        })?;
        Ok(Self {
            root,
            binary,
            binary_sha256,
            compiler_policy_sha256,
        })
    }

    pub fn stage(
        &self,
        generation: RuntimeGeneration,
        configuration: &EffectiveConfiguration,
    ) -> Result<RuntimeBundle, RuntimeBundleStageError> {
        if configuration.compiler_policy_sha256() != self.compiler_policy_sha256 {
            return Err(RuntimeBundleStageError::new(
                RuntimeBundleStageErrorKind::CompilerPolicyMismatch,
            ));
        }
        let configuration_bytes = configuration.yaml().as_bytes();
        if configuration_bytes.len() > EFFECTIVE_CONFIGURATION_MAX_BYTES {
            return Err(RuntimeBundleStageError::new(
                RuntimeBundleStageErrorKind::ConfigurationTooLarge,
            ));
        }
        let binary = read_verified_binary(&self.binary, &self.binary_sha256)?;
        let providers = provider_staging_plan(configuration)?;
        let configuration_sha256 = crate::digest::sha256_hex(configuration_bytes);
        let manifest = RuntimeManifestV1::new(
            generation,
            self.compiler_policy_sha256.clone(),
            self.binary_sha256.clone(),
            configuration_sha256,
        )
        .with_provider_files(provider_manifest_files(&providers.local_files)?);
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(|_| {
            RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::InvalidPolicy)
        })?;
        if manifest_bytes.len() > MANIFEST_MAX_BYTES {
            return Err(RuntimeBundleStageError::new(
                RuntimeBundleStageErrorKind::InvalidPolicy,
            ));
        }
        let manifest_sha256 = crate::digest::sha256_hex(&manifest_bytes);
        let final_root = self.root.join(format!("generation-{:020}", generation.0));

        if final_root.exists() {
            verify_existing(
                &final_root,
                &manifest_bytes,
                configuration_bytes,
                &binary,
                &providers,
            )?;
            return Ok(self.bundle(generation, final_root, manifest_sha256));
        }

        let pending_root = self.root.join(format!(
            ".generation-{:020}-{}.pending",
            generation.0,
            uuid::Uuid::new_v4()
        ));
        create_private_directory(&pending_root)?;
        let stage_result = self.stage_pending(
            &pending_root,
            configuration_bytes,
            &binary,
            &manifest_bytes,
            &providers,
        );
        if let Err(error) = stage_result {
            let _ = remove_pending(&self.root, &pending_root);
            return Err(error);
        }

        match fs::rename(&pending_root, &final_root) {
            Ok(()) => sync_directory(&self.root)?,
            Err(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::AlreadyExists | io::ErrorKind::DirectoryNotEmpty
                ) =>
            {
                remove_pending(&self.root, &pending_root)?;
                verify_existing(
                    &final_root,
                    &manifest_bytes,
                    configuration_bytes,
                    &binary,
                    &providers,
                )?;
            }
            Err(error) => {
                let _ = remove_pending(&self.root, &pending_root);
                return Err(RuntimeBundleStageError::io(error));
            }
        }

        Ok(self.bundle(generation, final_root, manifest_sha256))
    }

    fn stage_pending(
        &self,
        pending_root: &Path,
        configuration_bytes: &[u8],
        binary: &[u8],
        manifest: &[u8],
        providers: &ProviderStagingPlan,
    ) -> Result<(), RuntimeBundleStageError> {
        write_new(&pending_root.join("mihomo"), binary, 0o500)?;
        write_new(
            &pending_root.join("config.yaml"),
            configuration_bytes,
            0o400,
        )?;
        for relative_path in &providers.destinations {
            let destination = pending_root.join(relative_path);
            let parent = destination.parent().ok_or_else(|| {
                RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::InvalidProviderPath)
            })?;
            create_private_directory(parent)?;
        }
        for provider in &providers.local_files {
            write_new(&pending_root.join(&provider.path), &provider.content, 0o400)?;
        }
        write_new(&pending_root.join("manifest.json"), manifest, 0o400)?;
        sync_tree_directories(pending_root)?;
        Ok(())
    }

    fn bundle(
        &self,
        generation: RuntimeGeneration,
        generation_root: PathBuf,
        manifest_sha256: String,
    ) -> RuntimeBundle {
        RuntimeBundle {
            generation,
            generation_root,
            manifest_sha256,
            compiler_policy_sha256: self.compiler_policy_sha256.clone(),
            mihomo_binary_sha256: self.binary_sha256.clone(),
        }
    }
}

impl fmt::Debug for RuntimeBundleStager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeBundleStager")
            .field("root", &"[REDACTED]")
            .field("binary", &"[REDACTED]")
            .field("binary_sha256", &"[REDACTED]")
            .field("compiler_policy_sha256", &"[REDACTED]")
            .finish()
    }
}

fn provider_staging_plan(
    configuration: &EffectiveConfiguration,
) -> Result<ProviderStagingPlan, RuntimeBundleStageError> {
    let mut destinations = BTreeMap::<PathBuf, Option<Vec<u8>>>::new();
    for provider in configuration.providers() {
        let path = validate_relative_path(&provider.relative_path)?.to_owned();
        let content = match &provider.kind {
            ProviderKind::Remote { .. } => None,
            ProviderKind::Local { source } => Some(read_local_provider(source)?),
        };
        match destinations.get(&path) {
            None => {
                destinations.insert(path, content);
            }
            Some(existing) if existing == &content => {}
            Some(_) => {
                return Err(RuntimeBundleStageError::new(
                    RuntimeBundleStageErrorKind::InvalidProviderPath,
                ));
            }
        }
    }
    let local_files = destinations
        .iter()
        .filter_map(|(path, content)| {
            content.as_ref().map(|content| StagedProviderFile {
                path: path.clone(),
                content: content.clone(),
            })
        })
        .collect();
    Ok(ProviderStagingPlan {
        destinations: destinations.into_keys().collect(),
        local_files,
    })
}

fn provider_manifest_files(
    providers: &[StagedProviderFile],
) -> Result<Vec<RuntimeManifestFileV1>, RuntimeBundleStageError> {
    providers
        .iter()
        .map(|provider| {
            let path = provider.path.to_str().ok_or_else(|| {
                RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::InvalidProviderPath)
            })?;
            Ok(RuntimeManifestFileV1 {
                path: path.to_owned(),
                sha256: crate::digest::sha256_hex(&provider.content),
                size: provider.content.len() as u64,
            })
        })
        .collect()
}

fn prepare_private_root(root: &Path) -> Result<(), RuntimeBundleStageError> {
    if let Ok(metadata) = fs::symlink_metadata(root)
        && metadata.file_type().is_symlink()
    {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::RootUnavailable,
        ));
    }
    fs::create_dir_all(root).map_err(RuntimeBundleStageError::io)?;
    let metadata = fs::symlink_metadata(root).map_err(RuntimeBundleStageError::io)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::RootUnavailable,
        ));
    }
    fs::set_permissions(root, fs::Permissions::from_mode(0o700))
        .map_err(RuntimeBundleStageError::io)
}

fn read_verified_binary(
    path: &Path,
    expected_sha256: &str,
) -> Result<Vec<u8>, RuntimeBundleStageError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::InvalidBinary))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.permissions().mode() & 0o111 == 0
        || metadata.len() > MIHOMO_BINARY_MAX_BYTES as u64
    {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::InvalidBinary,
        ));
    }
    let content = read_limited(path, MIHOMO_BINARY_MAX_BYTES)
        .map_err(|_| RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::InvalidBinary))?;
    if crate::digest::sha256_hex(&content) != expected_sha256 {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::BinaryIdentityMismatch,
        ));
    }
    Ok(content)
}

fn read_local_provider(path: &Path) -> Result<Vec<u8>, RuntimeBundleStageError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::ProviderUnavailable)
    })?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > PROFILE_RESPONSE_MAX_BYTES as u64
    {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::ProviderUnavailable,
        ));
    }
    read_limited(path, PROFILE_RESPONSE_MAX_BYTES)
        .map_err(|_| RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::ProviderUnavailable))
}

fn validate_relative_path(path: &Path) -> Result<&Path, RuntimeBundleStageError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || matches!(
            path.to_str(),
            Some("manifest.json" | "config.yaml" | "mihomo")
        )
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::InvalidProviderPath,
        ));
    }
    Ok(path)
}

fn verify_existing(
    root: &Path,
    manifest: &[u8],
    configuration: &[u8],
    binary: &[u8],
    providers: &ProviderStagingPlan,
) -> Result<(), RuntimeBundleStageError> {
    let metadata = fs::symlink_metadata(root).map_err(RuntimeBundleStageError::io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::ExistingGenerationMismatch,
        ));
    }
    for (path, expected, limit, executable) in [
        (
            root.join("manifest.json"),
            manifest,
            MANIFEST_MAX_BYTES,
            false,
        ),
        (
            root.join("config.yaml"),
            configuration,
            EFFECTIVE_CONFIGURATION_MAX_BYTES,
            false,
        ),
        (root.join("mihomo"), binary, MIHOMO_BINARY_MAX_BYTES, true),
    ] {
        let actual = read_existing_file(&path, limit, executable).map_err(|_| {
            RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::ExistingGenerationMismatch)
        })?;
        if actual != expected {
            return Err(RuntimeBundleStageError::new(
                RuntimeBundleStageErrorKind::ExistingGenerationMismatch,
            ));
        }
    }
    for provider in &providers.local_files {
        let actual = read_existing_file(
            &root.join(&provider.path),
            PROFILE_RESPONSE_MAX_BYTES,
            false,
        )
        .map_err(|_| {
            RuntimeBundleStageError::new(RuntimeBundleStageErrorKind::ExistingGenerationMismatch)
        })?;
        if actual != provider.content {
            return Err(RuntimeBundleStageError::new(
                RuntimeBundleStageErrorKind::ExistingGenerationMismatch,
            ));
        }
    }
    Ok(())
}

fn read_existing_file(path: &Path, limit: usize, executable: bool) -> io::Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || executable && metadata.permissions().mode() & 0o111 == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "staged file shape is invalid",
        ));
    }
    read_limited(path, limit)
}

fn write_new(path: &Path, content: &[u8], mode: u32) -> Result<(), RuntimeBundleStageError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(mode);
    let mut file = options.open(path).map_err(RuntimeBundleStageError::io)?;
    file.write_all(content)
        .and_then(|()| file.sync_all())
        .map_err(RuntimeBundleStageError::io)
}

fn create_private_directory(path: &Path) -> Result<(), RuntimeBundleStageError> {
    fs::create_dir_all(path).map_err(RuntimeBundleStageError::io)?;
    let metadata = fs::symlink_metadata(path).map_err(RuntimeBundleStageError::io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::InvalidProviderPath,
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(RuntimeBundleStageError::io)
}

fn sync_tree_directories(root: &Path) -> Result<(), RuntimeBundleStageError> {
    let mut directories = vec![root.to_path_buf()];
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory).map_err(RuntimeBundleStageError::io)? {
            let entry = entry.map_err(RuntimeBundleStageError::io)?;
            let file_type = entry.file_type().map_err(RuntimeBundleStageError::io)?;
            if file_type.is_symlink() {
                return Err(RuntimeBundleStageError::new(
                    RuntimeBundleStageErrorKind::InvalidProviderPath,
                ));
            }
            if file_type.is_dir() {
                let path = entry.path();
                directories.push(path.clone());
                pending.push(path);
            }
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        sync_directory(&directory)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), RuntimeBundleStageError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(RuntimeBundleStageError::io)
}

fn remove_pending(root: &Path, pending: &Path) -> Result<(), RuntimeBundleStageError> {
    if pending.parent() != Some(root)
        || !pending
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(".generation-") && name.ends_with(".pending"))
    {
        return Err(RuntimeBundleStageError::new(
            RuntimeBundleStageErrorKind::InvalidPolicy,
        ));
    }
    fs::remove_dir_all(pending).map_err(RuntimeBundleStageError::io)
}

fn read_limited(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let file = File::open(path)?;
    if file.metadata()?.len() > limit as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file exceeds its size limit",
        ));
    }
    let mut content = Vec::new();
    file.take((limit as u64).saturating_add(1))
        .read_to_end(&mut content)?;
    if content.len() > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "file exceeds its size limit",
        ));
    }
    Ok(content)
}

#[cfg(test)]
mod generation_prune_tests {
    use super::*;

    #[test]
    fn a_generation_replaced_after_preflight_is_quarantined_without_deletion() {
        let root = std::env::temp_dir().join(format!(
            "ratash-runtime-prune-race-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        fs::create_dir(&root).expect("the fixture root should be created");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("the fixture root should be private");
        let generation = root.join("generation-00000000000000000001");
        fs::create_dir(&generation).expect("the generation should be created");
        fs::set_permissions(&generation, fs::Permissions::from_mode(0o700))
            .expect("the generation should be private");
        let original = root.join("original-generation");

        let error = prune_runtime_generations_with_reserved_and_hook(
            &root,
            RuntimeGenerationRetention::default(),
            &[],
            |source| {
                fs::rename(source, &original).expect("the original should be moved");
                fs::create_dir(source).expect("the replacement should be created");
                fs::set_permissions(source, fs::Permissions::from_mode(0o700))
                    .expect("the replacement should be private");
            },
        )
        .expect_err("the identity replacement should stop deletion");

        assert_eq!(error.kind(), RuntimeGenerationPruneErrorKind::UnsafeEntry);
        assert!(original.exists());
        let quarantines = fs::read_dir(&root)
            .expect("the fixture root should be readable")
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.ends_with(".pruning"))
            })
            .count();
        assert_eq!(quarantines, 1);
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains(&root.display().to_string()));

        fs::remove_dir_all(root).expect("the fixture root should be removed");
    }
}

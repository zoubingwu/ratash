//! Runtime Bundle manifest types and service-side verification.

use std::fmt;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::constants::{
    EFFECTIVE_CONFIGURATION_MAX_BYTES, MIHOMO_BINARY_MAX_BYTES, PROFILE_RESPONSE_MAX_BYTES,
    RUNTIME_CORE_EXECUTABLE_NAME,
};
use crate::core::{CoreControlEndpoint, CoreRuntimeError, RuntimeBundle};
use crate::digest::is_lower_sha256_hex;
use crate::domain::RuntimeGeneration;

use super::error::{ServicePlatformError, invalid_bundle};

const RUNTIME_MANIFEST_SCHEMA_VERSION: u16 = 1;
const RUNTIME_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_PROVIDER_FILE_MAX: usize = 1_024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeManifestFileV1 {
    pub path: String,
    pub sha256: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeManifestV1 {
    schema_version: u16,
    pub runtime_generation: u64,
    pub compiler_policy_sha256: String,
    pub mihomo_binary_sha256: String,
    pub configuration_sha256: String,
    pub executable: String,
    pub configuration: String,
    pub provider_files: Vec<RuntimeManifestFileV1>,
}

impl RuntimeManifestV1 {
    #[must_use]
    pub fn new(
        runtime_generation: RuntimeGeneration,
        compiler_policy_sha256: impl Into<String>,
        mihomo_binary_sha256: impl Into<String>,
        configuration_sha256: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: RUNTIME_MANIFEST_SCHEMA_VERSION,
            runtime_generation: runtime_generation.0,
            compiler_policy_sha256: compiler_policy_sha256.into(),
            mihomo_binary_sha256: mihomo_binary_sha256.into(),
            configuration_sha256: configuration_sha256.into(),
            executable: RUNTIME_CORE_EXECUTABLE_NAME.to_owned(),
            configuration: "config.yaml".to_owned(),
            provider_files: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_provider_files(mut self, mut provider_files: Vec<RuntimeManifestFileV1>) -> Self {
        provider_files.sort_by(|left, right| left.path.cmp(&right.path));
        self.provider_files = provider_files;
        self
    }
}

pub trait RuntimeConfigurationPolicy: Send + Sync {
    fn validate(
        &self,
        configuration: &[u8],
        endpoint: &CoreControlEndpoint,
        provider_files: &[RuntimeManifestFileV1],
    ) -> Result<(), ServicePlatformError>;
}

#[derive(Clone, Eq, PartialEq)]
pub struct VerifiedRuntimeBundle {
    bundle: RuntimeBundle,
    manifest_path: PathBuf,
    executable_path: PathBuf,
    configuration_path: PathBuf,
}

impl VerifiedRuntimeBundle {
    #[must_use]
    pub fn bundle(&self) -> &RuntimeBundle {
        &self.bundle
    }

    #[must_use]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    #[must_use]
    pub fn executable_path(&self) -> &Path {
        &self.executable_path
    }

    #[must_use]
    pub fn configuration_path(&self) -> &Path {
        &self.configuration_path
    }
}

impl fmt::Debug for VerifiedRuntimeBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("VerifiedRuntimeBundle")
            .field("generation", &self.bundle.generation)
            .field("manifest_sha256", &self.bundle.manifest_sha256)
            .field(
                "compiler_policy_sha256",
                &self.bundle.compiler_policy_sha256,
            )
            .field("mihomo_binary_sha256", &self.bundle.mihomo_binary_sha256)
            .finish()
    }
}

pub(super) fn verify_runtime_bundle(
    service_owned_root: &Path,
    compiler_policy_sha256: &str,
    mihomo_binary_sha256: &str,
    configuration_policy: &dyn RuntimeConfigurationPolicy,
    bundle: &RuntimeBundle,
    endpoint: &CoreControlEndpoint,
) -> Result<VerifiedRuntimeBundle, CoreRuntimeError> {
    if !is_lower_sha256_hex(&bundle.manifest_sha256)
        || bundle.compiler_policy_sha256 != compiler_policy_sha256
        || bundle.mihomo_binary_sha256 != mihomo_binary_sha256
    {
        return Err(invalid_bundle("runtime identity mismatch"));
    }
    let generation_root = fs::canonicalize(&bundle.generation_root)
        .map_err(|_| invalid_bundle("runtime root unavailable"))?;
    if generation_root == service_owned_root || !generation_root.starts_with(service_owned_root) {
        return Err(invalid_bundle("runtime root escaped service root"));
    }
    let manifest_path = generation_root.join("manifest.json");
    let manifest_bytes =
        read_bounded_regular(&generation_root, &manifest_path, RUNTIME_MANIFEST_MAX_BYTES)?;
    if crate::digest::sha256_hex(&manifest_bytes) != bundle.manifest_sha256 {
        return Err(invalid_bundle("runtime manifest digest mismatch"));
    }
    let manifest: RuntimeManifestV1 = serde_json::from_slice(&manifest_bytes)
        .map_err(|_| invalid_bundle("runtime manifest is invalid"))?;
    if manifest.schema_version != RUNTIME_MANIFEST_SCHEMA_VERSION
        || manifest.runtime_generation != bundle.generation.0
        || manifest.compiler_policy_sha256 != bundle.compiler_policy_sha256
        || manifest.mihomo_binary_sha256 != bundle.mihomo_binary_sha256
        || !is_lower_sha256_hex(&manifest.configuration_sha256)
        || manifest.executable != RUNTIME_CORE_EXECUTABLE_NAME
        || manifest.configuration != "config.yaml"
    {
        return Err(invalid_bundle("runtime manifest fields mismatch"));
    }

    let executable_path = generation_root.join(RUNTIME_CORE_EXECUTABLE_NAME);
    let binary =
        read_bounded_executable(&generation_root, &executable_path, MIHOMO_BINARY_MAX_BYTES)?;
    if crate::digest::sha256_hex(&binary) != bundle.mihomo_binary_sha256 {
        return Err(invalid_bundle("Mihomo binary identity mismatch"));
    }
    let configuration_path = generation_root.join("config.yaml");
    let configuration = read_bounded_regular(
        &generation_root,
        &configuration_path,
        EFFECTIVE_CONFIGURATION_MAX_BYTES,
    )?;
    if crate::digest::sha256_hex(&configuration) != manifest.configuration_sha256 {
        return Err(invalid_bundle("runtime configuration identity mismatch"));
    }
    verify_provider_files(&generation_root, &manifest.provider_files)?;
    configuration_policy
        .validate(&configuration, endpoint, &manifest.provider_files)
        .map_err(|_| invalid_bundle("runtime configuration policy mismatch"))?;

    Ok(VerifiedRuntimeBundle {
        bundle: RuntimeBundle {
            generation: bundle.generation,
            generation_root,
            manifest_sha256: bundle.manifest_sha256.clone(),
            compiler_policy_sha256: bundle.compiler_policy_sha256.clone(),
            mihomo_binary_sha256: bundle.mihomo_binary_sha256.clone(),
        },
        manifest_path,
        executable_path,
        configuration_path,
    })
}

fn verify_provider_files(
    generation_root: &Path,
    files: &[RuntimeManifestFileV1],
) -> Result<(), CoreRuntimeError> {
    if files.len() > RUNTIME_PROVIDER_FILE_MAX {
        return Err(invalid_bundle("runtime provider file count exceeded"));
    }
    let mut previous_path: Option<&str> = None;
    for file in files {
        if previous_path.is_some_and(|previous| previous >= file.path.as_str())
            || !valid_manifest_relative_path(&file.path)
            || !is_lower_sha256_hex(&file.sha256)
            || file.size > PROFILE_RESPONSE_MAX_BYTES as u64
        {
            return Err(invalid_bundle("runtime provider manifest entry is invalid"));
        }
        let bytes = read_bounded_regular(
            generation_root,
            &generation_root.join(&file.path),
            PROFILE_RESPONSE_MAX_BYTES,
        )?;
        if bytes.len() as u64 != file.size || crate::digest::sha256_hex(&bytes) != file.sha256 {
            return Err(invalid_bundle("runtime provider identity mismatch"));
        }
        previous_path = Some(&file.path);
    }
    Ok(())
}

fn valid_manifest_relative_path(value: &str) -> bool {
    let path = Path::new(value);
    !value.is_empty()
        && !path.is_absolute()
        && !matches!(
            value,
            "manifest.json" | "config.yaml" | RUNTIME_CORE_EXECUTABLE_NAME
        )
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn read_bounded_regular(
    root: &Path,
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, CoreRuntimeError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid_bundle("runtime file is unavailable"))?;
    if !metadata.file_type().is_file() || metadata.len() > max_bytes as u64 {
        return Err(invalid_bundle("runtime file shape or size is invalid"));
    }
    let canonical = fs::canonicalize(path)
        .map_err(|_| invalid_bundle("runtime file canonicalization failed"))?;
    if !canonical.starts_with(root) {
        return Err(invalid_bundle("runtime file escaped generation root"));
    }
    let bytes = fs::read(canonical).map_err(|_| invalid_bundle("runtime file read failed"))?;
    if bytes.len() > max_bytes {
        return Err(invalid_bundle("runtime file exceeded its size limit"));
    }
    Ok(bytes)
}

fn read_bounded_executable(
    root: &Path,
    path: &Path,
    max_bytes: usize,
) -> Result<Vec<u8>, CoreRuntimeError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| invalid_bundle("runtime file is unavailable"))?;
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o111 == 0 {
        return Err(invalid_bundle("Mihomo binary is not executable"));
    }
    read_bounded_regular(root, path, max_bytes)
}

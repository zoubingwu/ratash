//! Production adapters used by the configuration transaction coordinator.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::config::BUNDLED_CORE_VERSION;
use crate::core::{
    CoreRuntimeError, CoreRuntimeErrorKind, ManagedCoreHandle, MihomoAdapter, MihomoReadiness,
    RuntimeBundle,
};
use crate::service::RuntimeManifestV1;
use crate::transaction::{
    RuntimeApplyFailure, RuntimeBundleResolveError, RuntimeBundleResolver, RuntimeHealthError,
    RuntimeHealthProbe,
};

const RUNTIME_MANIFEST_MAX_BYTES: usize = 64 * 1_024;
const RUNTIME_PROVIDER_FILE_MAX: usize = 1_024;

pub struct MihomoRuntimeHealthProbe {
    mihomo: Arc<dyn MihomoAdapter>,
    expected_version: String,
}

impl MihomoRuntimeHealthProbe {
    #[must_use]
    pub fn bundled(mihomo: Arc<dyn MihomoAdapter>) -> Self {
        Self {
            mihomo,
            expected_version: BUNDLED_CORE_VERSION.to_owned(),
        }
    }

    #[must_use]
    pub fn new(mihomo: Arc<dyn MihomoAdapter>, expected_version: impl Into<String>) -> Self {
        Self {
            mihomo,
            expected_version: expected_version.into(),
        }
    }
}

impl RuntimeHealthProbe for MihomoRuntimeHealthProbe {
    fn confirm_ready(&self, managed_core: &ManagedCoreHandle) -> Result<(), RuntimeHealthError> {
        if self
            .mihomo
            .readiness(&managed_core.endpoint)
            .map_err(|_| RuntimeHealthError)?
            != MihomoReadiness::Ready
        {
            return Err(RuntimeHealthError);
        }
        let version = self
            .mihomo
            .version(&managed_core.endpoint)
            .map_err(|_| RuntimeHealthError)?;
        if version.version != self.expected_version || !version.meta {
            return Err(RuntimeHealthError);
        }
        Ok(())
    }
}

pub struct StagedRuntimeBundleResolver {
    root: PathBuf,
    compiler_policy_sha256: String,
    mihomo_binary_sha256: String,
}

impl StagedRuntimeBundleResolver {
    pub fn new(
        root: impl Into<PathBuf>,
        compiler_policy_sha256: impl Into<String>,
        mihomo_binary_sha256: impl Into<String>,
    ) -> Result<Self, RuntimeBundleResolveError> {
        let root = root.into();
        let compiler_policy_sha256 = compiler_policy_sha256.into();
        let mihomo_binary_sha256 = mihomo_binary_sha256.into();
        if !root.is_absolute()
            || !valid_digest(&compiler_policy_sha256)
            || !valid_digest(&mihomo_binary_sha256)
        {
            return Err(RuntimeBundleResolveError);
        }
        let metadata = fs::symlink_metadata(&root).map_err(|_| RuntimeBundleResolveError)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(RuntimeBundleResolveError);
        }
        let root = fs::canonicalize(root).map_err(|_| RuntimeBundleResolveError)?;
        Ok(Self {
            root,
            compiler_policy_sha256,
            mihomo_binary_sha256,
        })
    }

    fn resolve_generation(
        &self,
        generation: crate::domain::RuntimeGeneration,
    ) -> Result<RuntimeBundle, RuntimeBundleResolveError> {
        let generation_root = self.root.join(format!("generation-{:020}", generation.0));
        let root_metadata =
            fs::symlink_metadata(&generation_root).map_err(|_| RuntimeBundleResolveError)?;
        if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
            return Err(RuntimeBundleResolveError);
        }
        let manifest_path = generation_root.join("manifest.json");
        let manifest = read_bounded_regular(&manifest_path, RUNTIME_MANIFEST_MAX_BYTES)?;
        let decoded: RuntimeManifestV1 =
            serde_json::from_slice(&manifest).map_err(|_| RuntimeBundleResolveError)?;
        if decoded.runtime_generation != generation.0
            || decoded.compiler_policy_sha256 != self.compiler_policy_sha256
            || decoded.mihomo_binary_sha256 != self.mihomo_binary_sha256
            || decoded.configuration != "config.yaml"
            || decoded.executable != "mihomo"
            || decoded.provider_files.len() > RUNTIME_PROVIDER_FILE_MAX
        {
            return Err(RuntimeBundleResolveError);
        }
        Ok(RuntimeBundle {
            generation,
            generation_root,
            manifest_sha256: crate::digest::sha256_hex(&manifest),
            compiler_policy_sha256: self.compiler_policy_sha256.clone(),
            mihomo_binary_sha256: self.mihomo_binary_sha256.clone(),
        })
    }
}

impl RuntimeBundleResolver for StagedRuntimeBundleResolver {
    fn resolve(
        &self,
        transaction: &crate::persistence::TransactionBundle,
    ) -> Result<RuntimeBundle, RuntimeBundleResolveError> {
        self.resolve_generation(transaction.runtime_generation)
    }
}

#[must_use]
pub const fn classify_runtime_apply_error(error: &CoreRuntimeError) -> RuntimeApplyFailure {
    match error.kind {
        CoreRuntimeErrorKind::ReloadTimeout | CoreRuntimeErrorKind::Unavailable => {
            RuntimeApplyFailure::Indeterminate
        }
        CoreRuntimeErrorKind::Authentication
        | CoreRuntimeErrorKind::ProtocolMismatch
        | CoreRuntimeErrorKind::TunPermissionDenied
        | CoreRuntimeErrorKind::InvalidBundle
        | CoreRuntimeErrorKind::ProcessIdentityMismatch
        | CoreRuntimeErrorKind::Apply
        | CoreRuntimeErrorKind::Readiness => RuntimeApplyFailure::Definite,
    }
}

fn read_bounded_regular(path: &Path, limit: usize) -> Result<Vec<u8>, RuntimeBundleResolveError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| RuntimeBundleResolveError)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(RuntimeBundleResolveError);
    }
    let content = fs::read(path).map_err(|_| RuntimeBundleResolveError)?;
    if content.len() > limit {
        return Err(RuntimeBundleResolveError);
    }
    Ok(content)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

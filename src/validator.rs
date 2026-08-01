use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::config::{CoreConfigValidator, CoreValidationError, EffectiveConfiguration};
use crate::constants::{
    EFFECTIVE_CONFIGURATION_MAX_BYTES, MIHOMO_BINARY_MAX_BYTES, MIHOMO_VALIDATION_OUTPUT_MAX_BYTES,
    MIHOMO_VALIDATION_TIMEOUT,
};
use crate::digest::is_lower_sha256_hex;
use crate::geodata::{GeoDataCatalog, GeoDataError};
use crate::mihomo_command::enforce_managed_runtime;

#[derive(Clone)]
pub struct MihomoCommandValidator {
    binary: PathBuf,
    expected_sha256: String,
    timeout: Duration,
    cancellation: Arc<AtomicBool>,
    geodata: Option<ValidationGeoData>,
}

#[derive(Clone)]
struct ValidationGeoData {
    root: PathBuf,
    catalog: GeoDataCatalog,
}

impl MihomoCommandValidator {
    pub fn new(
        binary: impl Into<PathBuf>,
        expected_sha256: impl Into<String>,
        timeout: Duration,
    ) -> Result<Self, MihomoValidationError> {
        let binary = binary.into();
        let expected_sha256 = expected_sha256.into();
        if !binary.is_absolute() || !is_lower_sha256_hex(&expected_sha256) || timeout.is_zero() {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::InvalidPolicy,
            ));
        }
        Ok(Self {
            binary,
            expected_sha256,
            timeout,
            cancellation: Arc::new(AtomicBool::new(false)),
            geodata: None,
        })
    }

    pub fn bundled(
        binary: impl Into<PathBuf>,
        expected_sha256: impl Into<String>,
        geodata_root: impl Into<PathBuf>,
    ) -> Result<Self, MihomoValidationError> {
        Self::new(binary, expected_sha256, MIHOMO_VALIDATION_TIMEOUT)?.with_geodata(
            geodata_root,
            GeoDataCatalog::bundled().map_err(map_geodata_error)?,
        )
    }

    pub fn with_geodata(
        mut self,
        root: impl Into<PathBuf>,
        catalog: GeoDataCatalog,
    ) -> Result<Self, MihomoValidationError> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::InvalidPolicy,
            ));
        }
        self.geodata = Some(ValidationGeoData { root, catalog });
        Ok(self)
    }

    pub fn validate_detailed(
        &self,
        configuration: &EffectiveConfiguration,
        staging_root: &Path,
    ) -> Result<(), MihomoValidationError> {
        if self.cancellation.load(Ordering::Acquire) {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::Cancelled,
            ));
        }
        if configuration.yaml().len() > EFFECTIVE_CONFIGURATION_MAX_BYTES {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::ConfigurationTooLarge,
            ));
        }
        let binary = self.verify_binary()?;
        let staging_root = verify_staging_root(staging_root)?;
        if let Some(geodata) = &self.geodata {
            geodata
                .catalog
                .stage_from(&geodata.root, &staging_root)
                .map_err(map_geodata_error)?;
        }
        let mut files = ValidationFiles::create(&staging_root, configuration.yaml().as_bytes())?;
        let stdout = files.stdout_file()?;
        let stderr = files.stderr_file()?;

        let mut command = Command::new(binary);
        command
            .args(["-t", "-d"])
            .arg(&staging_root)
            .arg("-f")
            .arg(&files.configuration)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        enforce_managed_runtime(&mut command);
        let mut child = command
            .spawn()
            .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::SpawnFailed))?;

        let deadline = Instant::now() + self.timeout;
        let status = loop {
            if self.cancellation.load(Ordering::Acquire) {
                let _ = child.kill();
                let _ = child.wait();
                return Err(MihomoValidationError::new(
                    MihomoValidationErrorKind::Cancelled,
                ));
            }
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(MihomoValidationError::new(
                        MihomoValidationErrorKind::TimedOut,
                    ));
                }
                Err(_) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(MihomoValidationError::new(
                        MihomoValidationErrorKind::WaitFailed,
                    ));
                }
            }
        };

        let stdout = read_limited(&files.stdout, MIHOMO_VALIDATION_OUTPUT_MAX_BYTES)
            .map_err(map_output_error)?;
        let stderr = read_limited(&files.stderr, MIHOMO_VALIDATION_OUTPUT_MAX_BYTES)
            .map_err(map_output_error)?;
        let fatal_output = contains_fatal_marker(&stdout) || contains_fatal_marker(&stderr);
        if !status.success() || fatal_output {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::ConfigurationRejected,
            ));
        }

        files.cleanup()?;
        Ok(())
    }

    fn verify_binary(&self) -> Result<PathBuf, MihomoValidationError> {
        let metadata = fs::symlink_metadata(&self.binary).map_err(|_| {
            MihomoValidationError::new(MihomoValidationErrorKind::BinaryUnavailable)
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.permissions().mode() & 0o111 == 0
            || metadata.len() > MIHOMO_BINARY_MAX_BYTES as u64
        {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::InvalidBinary,
            ));
        }
        let content = read_limited(&self.binary, MIHOMO_BINARY_MAX_BYTES)
            .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::InvalidBinary))?;
        if crate::digest::sha256_hex(&content) != self.expected_sha256 {
            return Err(MihomoValidationError::new(
                MihomoValidationErrorKind::BinaryIdentityMismatch,
            ));
        }
        self.binary
            .canonicalize()
            .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::InvalidBinary))
    }
}

impl fmt::Debug for MihomoCommandValidator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MihomoCommandValidator")
            .field("binary", &"[REDACTED]")
            .field("expected_sha256", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .field("geodata", &self.geodata.as_ref().map(|_| "configured"))
            .finish()
    }
}

impl CoreConfigValidator for MihomoCommandValidator {
    fn validate(
        &self,
        configuration: &EffectiveConfiguration,
        staging_root: &Path,
    ) -> Result<(), CoreValidationError> {
        self.validate_detailed(configuration, staging_root)
            .map_err(|error| CoreValidationError::new(error.to_string()))
    }

    fn cancel_pending(&self) {
        self.cancellation.store(true, Ordering::Release);
    }
}

struct ValidationFiles {
    configuration: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
    cleaned: bool,
}

impl ValidationFiles {
    fn create(root: &Path, configuration: &[u8]) -> Result<Self, MihomoValidationError> {
        let token = uuid::Uuid::new_v4();
        let files = Self {
            configuration: root.join(format!(".hopash-validation-{token}.yaml")),
            stdout: root.join(format!(".hopash-validation-{token}.stdout")),
            stderr: root.join(format!(".hopash-validation-{token}.stderr")),
            cleaned: false,
        };
        write_private_new(&files.configuration, configuration)?;
        if let Err(error) = write_private_new(&files.stdout, b"") {
            let _ = fs::remove_file(&files.configuration);
            return Err(error);
        }
        if let Err(error) = write_private_new(&files.stderr, b"") {
            let _ = fs::remove_file(&files.configuration);
            let _ = fs::remove_file(&files.stdout);
            return Err(error);
        }
        sync_directory(root)?;
        Ok(files)
    }

    fn stdout_file(&self) -> Result<File, MihomoValidationError> {
        OpenOptions::new()
            .append(true)
            .open(&self.stdout)
            .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::StagingIo))
    }

    fn stderr_file(&self) -> Result<File, MihomoValidationError> {
        OpenOptions::new()
            .append(true)
            .open(&self.stderr)
            .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::StagingIo))
    }

    fn cleanup(&mut self) -> Result<(), MihomoValidationError> {
        for path in [&self.configuration, &self.stdout, &self.stderr] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => {
                    return Err(MihomoValidationError::new(
                        MihomoValidationErrorKind::CleanupFailed,
                    ));
                }
            }
        }
        self.cleaned = true;
        sync_directory(
            self.configuration
                .parent()
                .expect("validation file always has a parent"),
        )
    }
}

impl Drop for ValidationFiles {
    fn drop(&mut self) {
        if !self.cleaned {
            let _ = fs::remove_file(&self.configuration);
            let _ = fs::remove_file(&self.stdout);
            let _ = fs::remove_file(&self.stderr);
        }
    }
}

fn verify_staging_root(path: &Path) -> Result<PathBuf, MihomoValidationError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::InvalidStagingRoot))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(MihomoValidationError::new(
            MihomoValidationErrorKind::InvalidStagingRoot,
        ));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::InvalidStagingRoot))?;
    fs::set_permissions(&canonical, fs::Permissions::from_mode(0o700))
        .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::StagingIo))?;
    Ok(canonical)
}

fn write_private_new(path: &Path, content: &[u8]) -> Result<(), MihomoValidationError> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true).mode(0o600);
    let mut file = options
        .open(path)
        .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::StagingIo))?;
    if let Err(_error) = file.write_all(content).and_then(|()| file.sync_all()) {
        drop(file);
        let _ = fs::remove_file(path);
        return Err(MihomoValidationError::new(
            MihomoValidationErrorKind::StagingIo,
        ));
    }
    Ok(())
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

fn map_output_error(error: io::Error) -> MihomoValidationError {
    let kind = if error.kind() == io::ErrorKind::InvalidData {
        MihomoValidationErrorKind::OutputTooLarge
    } else {
        MihomoValidationErrorKind::StagingIo
    };
    MihomoValidationError::new(kind)
}

fn map_geodata_error(error: GeoDataError) -> MihomoValidationError {
    let kind = match error {
        GeoDataError::InvalidManifest => MihomoValidationErrorKind::InvalidPolicy,
        GeoDataError::InvalidRoot | GeoDataError::InvalidAsset | GeoDataError::Io => {
            MihomoValidationErrorKind::GeoDataUnavailable
        }
    };
    MihomoValidationError::new(kind)
}

fn contains_fatal_marker(bytes: &[u8]) -> bool {
    let value = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    value.contains("fatal") || value.contains("parse config error")
}

fn sync_directory(path: &Path) -> Result<(), MihomoValidationError> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| MihomoValidationError::new(MihomoValidationErrorKind::StagingIo))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MihomoValidationErrorKind {
    InvalidPolicy,
    BinaryUnavailable,
    InvalidBinary,
    BinaryIdentityMismatch,
    InvalidStagingRoot,
    ConfigurationTooLarge,
    GeoDataUnavailable,
    StagingIo,
    SpawnFailed,
    WaitFailed,
    TimedOut,
    Cancelled,
    OutputTooLarge,
    ConfigurationRejected,
    CleanupFailed,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct MihomoValidationError {
    kind: MihomoValidationErrorKind,
}

impl MihomoValidationError {
    const fn new(kind: MihomoValidationErrorKind) -> Self {
        Self { kind }
    }

    #[must_use]
    pub const fn kind(&self) -> MihomoValidationErrorKind {
        self.kind
    }
}

impl fmt::Debug for MihomoValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MihomoValidationError")
            .field("kind", &self.kind)
            .finish()
    }
}

impl fmt::Display for MihomoValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.kind {
            MihomoValidationErrorKind::InvalidPolicy => "Mihomo validator policy is invalid",
            MihomoValidationErrorKind::BinaryUnavailable => "the Mihomo binary is unavailable",
            MihomoValidationErrorKind::InvalidBinary => "the Mihomo binary is invalid",
            MihomoValidationErrorKind::BinaryIdentityMismatch => {
                "the Mihomo binary identity does not match the release contract"
            }
            MihomoValidationErrorKind::InvalidStagingRoot => {
                "the Mihomo validation staging root is invalid"
            }
            MihomoValidationErrorKind::ConfigurationTooLarge => {
                "the Effective Configuration exceeds its size limit"
            }
            MihomoValidationErrorKind::GeoDataUnavailable => {
                "the bundled Mihomo Geo data is unavailable or invalid"
            }
            MihomoValidationErrorKind::StagingIo => "Mihomo validation staging failed",
            MihomoValidationErrorKind::SpawnFailed => {
                "the Mihomo validation process failed to start"
            }
            MihomoValidationErrorKind::WaitFailed => {
                "the Mihomo validation process could not be observed"
            }
            MihomoValidationErrorKind::TimedOut => "Mihomo configuration validation timed out",
            MihomoValidationErrorKind::Cancelled => {
                "Mihomo validation was cancelled during Supervisor shutdown"
            }
            MihomoValidationErrorKind::OutputTooLarge => {
                "Mihomo validation output exceeded its size limit"
            }
            MihomoValidationErrorKind::ConfigurationRejected => {
                "Mihomo rejected the Effective Configuration"
            }
            MihomoValidationErrorKind::CleanupFailed => "Mihomo validation cleanup failed",
        })
    }
}

impl std::error::Error for MihomoValidationError {}

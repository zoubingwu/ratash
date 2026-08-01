//! Injected privileged-service platform ports and process value types.

use crate::core::{CoreControlEndpoint, OwnerSessionRequest, ProcessOutputSource};
use crate::domain::CoreInstanceGeneration;

use super::bundle::{RuntimeConfigurationPolicy, VerifiedRuntimeBundle};
use super::error::ServicePlatformError;

pub trait CallerCredentialValidator: Send + Sync {
    fn validate(&self, request: &OwnerSessionRequest) -> Result<(), ServicePlatformError>;
}

pub trait ProcessIdentityProbe: Send + Sync {
    fn start_identity(&self, pid: u32) -> Result<Option<String>, ServicePlatformError>;
}

pub trait TunCapabilityPreflight: Send + Sync {
    fn check(&self, owner_uid: u32) -> Result<(), ServicePlatformError>;
}

pub trait SecretGenerator: Send + Sync {
    fn generate(&self) -> Result<String, ServicePlatformError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UuidSecretGenerator;

impl SecretGenerator for UuidSecretGenerator {
    fn generate(&self) -> Result<String, ServicePlatformError> {
        Ok(uuid::Uuid::new_v4().to_string())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedProcessIdentity {
    pub pid: u32,
    pub process_start_identity: String,
    pub instance_generation: CoreInstanceGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnedCoreProcess {
    pub pid: u32,
    pub process_start_identity: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreProcessLog {
    pub timestamp_unix_ms: u64,
    pub source: ProcessOutputSource,
    pub message: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreProcessLogBatch {
    pub records: Vec<CoreProcessLog>,
    pub dropped: u64,
}

pub trait CoreProcessController: Send + Sync {
    fn spawn(
        &self,
        bundle: &VerifiedRuntimeBundle,
        endpoint: &CoreControlEndpoint,
        instance_generation: CoreInstanceGeneration,
    ) -> Result<SpawnedCoreProcess, ServicePlatformError>;

    fn reload(
        &self,
        process: &OwnedProcessIdentity,
        bundle: &VerifiedRuntimeBundle,
    ) -> Result<(), ServicePlatformError>;

    fn stop(&self, process: &OwnedProcessIdentity) -> Result<(), ServicePlatformError>;

    fn readiness(
        &self,
        process: &OwnedProcessIdentity,
        endpoint: &CoreControlEndpoint,
    ) -> Result<(), ServicePlatformError>;

    fn grant_endpoint_access(
        &self,
        endpoint: &CoreControlEndpoint,
        owner_uid: u32,
    ) -> Result<(), ServicePlatformError>;

    fn reap_if_exited(&self, process: &OwnedProcessIdentity) -> Result<bool, ServicePlatformError>;

    fn take_logs(
        &self,
        process: &OwnedProcessIdentity,
        limit: usize,
    ) -> Result<CoreProcessLogBatch, ServicePlatformError>;

    /// Replaces the in-memory cancellation epoch without waiting for process I/O.
    fn reset_apply_cancellation(&self, _owner_generation: u64) {}

    /// Cancels matching in-memory work without joining or waiting for process I/O.
    fn cancel_pending_apply(&self, _owner_generation: u64) {}
}

pub struct PrivilegedServiceDependencies {
    pub credentials: Box<dyn CallerCredentialValidator>,
    pub identities: Box<dyn ProcessIdentityProbe>,
    pub tun: Box<dyn TunCapabilityPreflight>,
    pub configuration_policy: Box<dyn RuntimeConfigurationPolicy>,
    pub secrets: Box<dyn SecretGenerator>,
    pub processes: Box<dyn CoreProcessController>,
}

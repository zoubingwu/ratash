//! Defines external Supervisor adapters and application-facing ports.

use super::{
    Arc, CoreRuntime, CoreRuntimeStatus, ManagedCoreHandle, MihomoAdapter, MihomoError,
    MihomoErrorKind, NodeSelection, ProfileSource, ProxyView, SubscriptionUrl, bounded_message, io,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FetchedProfile {
    pub body: Vec<u8>,
    pub metadata_name: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileFetchError {
    pub message: String,
    pub retryable: bool,
}

impl ProfileFetchError {
    #[must_use]
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: bounded_message(message.into()),
            retryable,
        }
    }
}

pub trait ProfileFetchPort: Send + Sync {
    fn fetch(&self, url: &SubscriptionUrl) -> Result<FetchedProfile, ProfileFetchError>;

    fn cancel_pending(&self) {}
}

pub struct BlockingProfileFetchPort {
    runtime: tokio::runtime::Runtime,
    source: Arc<dyn ProfileSource>,
}

impl BlockingProfileFetchPort {
    pub fn new(source: Arc<dyn ProfileSource>) -> io::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(crate::constants::PROFILE_REFRESH_CONCURRENCY)
            .thread_name("hopash-profile-download")
            .enable_all()
            .build()?;
        Ok(Self { runtime, source })
    }
}

impl ProfileFetchPort for BlockingProfileFetchPort {
    fn fetch(&self, url: &SubscriptionUrl) -> Result<FetchedProfile, ProfileFetchError> {
        let download = self
            .runtime
            .block_on(self.source.download(url))
            .map_err(|error| ProfileFetchError::new(error.to_string(), error.retryable()))?;
        Ok(FetchedProfile {
            body: download.body().to_vec(),
            metadata_name: download.metadata_name().map(str::to_owned),
        })
    }

    fn cancel_pending(&self) {
        self.source.cancel_pending();
    }
}

pub trait SupervisorCorePort: Send + Sync {
    fn runtime_status(&self) -> Result<CoreRuntimeStatus, MihomoError>;

    fn proxy_view(
        &self,
        core: &ManagedCoreHandle,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError>;

    fn select_node(
        &self,
        core: &ManagedCoreHandle,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError>;

    fn cancel_pending(&self) {}
}

pub struct DirectSupervisorCorePort {
    runtime: Arc<dyn CoreRuntime>,
    mihomo: Arc<dyn MihomoAdapter>,
    owner: crate::core::OwnerSessionProof,
}

impl DirectSupervisorCorePort {
    #[must_use]
    pub fn new(
        runtime: Arc<dyn CoreRuntime>,
        mihomo: Arc<dyn MihomoAdapter>,
        owner: crate::core::OwnerSessionProof,
    ) -> Self {
        Self {
            runtime,
            mihomo,
            owner,
        }
    }
}

impl SupervisorCorePort for DirectSupervisorCorePort {
    fn runtime_status(&self) -> Result<CoreRuntimeStatus, MihomoError> {
        self.runtime
            .status(&self.owner)
            .map_err(|error| MihomoError::new(MihomoErrorKind::Unavailable, error.to_string()))
    }

    fn proxy_view(
        &self,
        core: &ManagedCoreHandle,
        effective_group_order: &[String],
    ) -> Result<ProxyView, MihomoError> {
        self.mihomo
            .proxy_view(&core.endpoint, effective_group_order)
    }

    fn select_node(
        &self,
        core: &ManagedCoreHandle,
        selection: &NodeSelection,
    ) -> Result<(), MihomoError> {
        self.mihomo.select_node(&core.endpoint, selection)
    }

    fn cancel_pending(&self) {
        self.mihomo.cancel_pending();
    }
}

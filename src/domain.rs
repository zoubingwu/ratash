#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupervisorLifecycle {
    Starting,
    Ready,
    Stopping,
    Degraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CoreLifecycle {
    Unconfigured,
    Stopped,
    Starting,
    Ready,
    Reloading,
    Stopping,
    Degraded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TunReason {
    NoActiveProfile,
    PermissionDenied,
    Unsupported,
    CoreUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyState {
    Idle,
    Applying,
    Recovering,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SampleState {
    Fresh,
    Stale,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamState {
    Disconnected,
    Connecting,
    Healthy,
    Stale,
    Degraded,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeGeneration(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CoreInstanceGeneration(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProbeGeneration(pub u64);

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProfileId(pub String);

#[derive(Clone, Eq, PartialEq)]
pub struct SubscriptionUrl(url::Url);

impl SubscriptionUrl {
    pub fn parse(value: &str) -> Result<Self, InvalidSubscriptionUrl> {
        let parsed = url::Url::parse(value).map_err(|_| InvalidSubscriptionUrl)?;
        if matches!(parsed.scheme(), "http" | "https") && parsed.host().is_some() {
            Ok(Self(parsed))
        } else {
            Err(InvalidSubscriptionUrl)
        }
    }

    #[must_use]
    pub fn expose(&self) -> &url::Url {
        &self.0
    }
}

impl fmt::Debug for SubscriptionUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SubscriptionUrl([REDACTED])")
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidSubscriptionUrl;

impl fmt::Display for InvalidSubscriptionUrl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("subscription URL must use HTTP or HTTPS and include a host")
    }
}

impl std::error::Error for InvalidSubscriptionUrl {}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NodeRecordId(String);

impl NodeRecordId {
    #[must_use]
    pub fn for_core(proxy_name: &str) -> Self {
        Self(source_aware_node_id("core", &[proxy_name]))
    }

    #[must_use]
    pub fn for_provider(provider_name: &str, proxy_name: &str) -> Self {
        Self(source_aware_node_id(
            "provider",
            &[provider_name, proxy_name],
        ))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn source_aware_node_id(source: &str, components: &[&str]) -> String {
    components
        .iter()
        .fold(source.to_owned(), |mut id, component| {
            id.push(':');
            id.push_str(&component.len().to_string());
            id.push(':');
            id.push_str(component);
            id
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SupervisorStatus {
    pub lifecycle: SupervisorLifecycle,
    pub started_at_unix_ms: u64,
    pub uptime_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreStatus {
    pub lifecycle: CoreLifecycle,
    pub pid: Option<u32>,
    pub instance_generation: Option<CoreInstanceGeneration>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TunStatus {
    pub requested: bool,
    pub capable: bool,
    pub effective: bool,
    pub reason: Option<TunReason>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActiveProfileSummary {
    pub id: ProfileId,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectedNodeSummary {
    pub id: NodeRecordId,
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LatencySample {
    pub node_id: NodeRecordId,
    pub delay_ms: Option<u64>,
    pub sampled_at_unix_ms: Option<u64>,
    pub state: SampleState,
    pub probe_generation: ProbeGeneration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrafficSample {
    pub upload_bytes_per_second: u64,
    pub download_bytes_per_second: u64,
    pub sampled_at_unix_ms: Option<u64>,
    pub state: SampleState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamHealthSet {
    pub traffic: StreamState,
    pub connections: StreamState,
    pub logs: StreamState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusSnapshot {
    pub supervisor: SupervisorStatus,
    pub core: CoreStatus,
    pub tun: TunStatus,
    pub active_profile: Option<ActiveProfileSummary>,
    pub primary_proxy_group: Option<String>,
    pub selected_node: Option<SelectedNodeSummary>,
    pub latency: Option<LatencySample>,
    pub traffic: TrafficSample,
    pub connection_count: u64,
    pub runtime_generation: Option<RuntimeGeneration>,
    pub apply_state: ApplyState,
    pub stream_health: StreamHealthSet,
}
use std::fmt;

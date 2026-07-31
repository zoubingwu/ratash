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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalRuleSetRevision(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProfileId(uuid::Uuid);

impl ProfileId {
    #[must_use]
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn parse(value: &str) -> Result<Self, uuid::Error> {
        uuid::Uuid::parse_str(value).map(Self)
    }
}

impl Default for ProfileId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ProfileId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct SubscriptionUrl(url::Url);

impl SubscriptionUrl {
    pub fn parse(value: &str) -> Result<Self, InvalidSubscriptionUrl> {
        if value.len() > crate::constants::SUBSCRIPTION_URL_MAX_BYTES {
            return Err(InvalidSubscriptionUrl);
        }
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

    #[must_use]
    pub fn redacted(&self) -> String {
        let mut output = self.0.origin().ascii_serialization();
        if let Some(segments) = self.0.path_segments() {
            for segment in segments {
                output.push('/');
                if is_token_like_path_segment(segment) {
                    output.push_str("[redacted]");
                } else {
                    output.push_str(segment);
                }
            }
        }
        if let Some(query) = self.0.query() {
            output.push('?');
            for (index, pair) in query.split('&').enumerate() {
                if index > 0 {
                    output.push('&');
                }
                let (key, has_value) = pair
                    .split_once('=')
                    .map_or((pair, false), |(key, _)| (key, true));
                if has_value && !is_token_like_path_segment(key) {
                    output.push_str(key);
                } else {
                    output.push_str("[redacted]");
                }
                output.push_str("=[redacted]");
            }
        }
        output
    }
}

pub(crate) fn is_token_like_path_segment(segment: &str) -> bool {
    let decoded = percent_encoding::percent_decode_str(segment).decode_utf8_lossy();
    let normalized = decoded.to_ascii_lowercase();
    ["token", "secret", "credential", "auth", "api-key", "apikey"]
        .iter()
        .any(|marker| normalized.contains(marker))
        || looks_like_encoded_secret(&decoded)
}

fn looks_like_encoded_secret(value: &str) -> bool {
    let value = [".yaml", ".yml", ".txt"]
        .iter()
        .find_map(|suffix| value.strip_suffix(suffix))
        .unwrap_or(value);
    let jwt_parts = value.split('.').collect::<Vec<_>>();
    if jwt_parts.len() == 3
        && jwt_parts.iter().all(|part| {
            part.len() >= 8
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        })
    {
        return true;
    }

    if value.len() < 24
        || !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'+' | b'=' | b'.')
        })
    {
        return false;
    }

    let has_alpha = value.bytes().any(|byte| byte.is_ascii_alphabetic());
    let has_digit = value.bytes().any(|byte| byte.is_ascii_digit());
    let has_upper = value.bytes().any(|byte| byte.is_ascii_uppercase());
    let has_lower = value.bytes().any(|byte| byte.is_ascii_lowercase());
    (has_alpha && has_digit) || (has_upper && has_lower) || value.contains('=')
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
    pub fn parse(value: &str) -> Result<Self, InvalidNodeRecordId> {
        let digest = value.strip_prefix("node_v1_").ok_or(InvalidNodeRecordId)?;
        if digest.len() == 64
            && digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            Ok(Self(value.to_owned()))
        } else {
            Err(InvalidNodeRecordId)
        }
    }

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidNodeRecordId;

impl fmt::Display for InvalidNodeRecordId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Node record ID is invalid")
    }
}

impl std::error::Error for InvalidNodeRecordId {}

fn source_aware_node_id(source: &str, components: &[&str]) -> String {
    let mut canonical = b"hopash-node-v1\0".to_vec();
    canonical.extend_from_slice(source.as_bytes());
    canonical.push(0);
    for component in components {
        canonical.extend_from_slice(&(component.len() as u64).to_be_bytes());
        canonical.extend_from_slice(component.as_bytes());
    }
    format!("node_v1_{}", crate::digest::sha256_hex(&canonical))
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProbeQueueStatus {
    pub active_node_count: u64,
    pub queue_depth: u64,
    pub in_flight_count: u64,
    pub overloaded: bool,
    pub oldest_due_age_ms: Option<u64>,
    pub estimated_full_pass_duration_ms: u64,
    pub stale_node_count: u64,
}

impl ProbeQueueStatus {
    #[must_use]
    pub fn stale_ratio(self) -> f64 {
        if self.active_node_count == 0 {
            0.0
        } else {
            self.stale_node_count as f64 / self.active_node_count as f64
        }
    }
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
    pub selection_restore_pending: bool,
    pub probe_queue: ProbeQueueStatus,
    pub stream_health: StreamHealthSet,
}
use std::fmt;

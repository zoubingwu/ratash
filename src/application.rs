use crate::domain::{
    ApplyState, CoreLifecycle, CoreStatus, SampleState, StatusSnapshot, StreamHealthSet,
    StreamState, SubscriptionUrl, SupervisorLifecycle, SupervisorStatus, TrafficSample, TunReason,
    TunStatus,
};
use crate::error::ErrorCode;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync {
    fn now_unix_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

pub struct ApplicationService {
    clock: Arc<dyn Clock>,
    started_at_unix_ms: u64,
}

pub trait ApplicationClient {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError>;
}

impl Default for ApplicationService {
    fn default() -> Self {
        Self::new()
    }
}

impl ApplicationService {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    #[must_use]
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let started_at_unix_ms = clock.now_unix_ms();
        Self {
            clock,
            started_at_unix_ms,
        }
    }

    #[must_use]
    pub fn status(&self) -> StatusSnapshot {
        let uptime_seconds = self
            .clock
            .now_unix_ms()
            .saturating_sub(self.started_at_unix_ms)
            / 1_000;
        StatusSnapshot {
            supervisor: SupervisorStatus {
                lifecycle: SupervisorLifecycle::Ready,
                started_at_unix_ms: self.started_at_unix_ms,
                uptime_seconds,
            },
            core: CoreStatus {
                lifecycle: CoreLifecycle::Unconfigured,
                pid: None,
                instance_generation: None,
            },
            tun: TunStatus {
                requested: true,
                capable: false,
                effective: false,
                reason: Some(TunReason::NoActiveProfile),
            },
            active_profile: None,
            primary_proxy_group: None,
            selected_node: None,
            latency: None,
            traffic: TrafficSample {
                upload_bytes_per_second: 0,
                download_bytes_per_second: 0,
                sampled_at_unix_ms: None,
                state: SampleState::Unavailable,
            },
            connection_count: 0,
            runtime_generation: None,
            apply_state: ApplyState::Idle,
            stream_health: StreamHealthSet {
                traffic: StreamState::Disconnected,
                connections: StreamState::Disconnected,
                logs: StreamState::Disconnected,
            },
        }
    }

    pub fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        match operation {
            ApplicationOperation::GetStatus => Ok(ApplicationOutput::Status(self.status())),
            _ => Err(ApplicationError::new(
                ErrorCode::OperationUnavailable,
                "The lifecycle service is not connected",
                true,
            )),
        }
    }
}

impl ApplicationClient for ApplicationService {
    fn execute(
        &self,
        operation: ApplicationOperation,
    ) -> Result<ApplicationOutput, ApplicationError> {
        ApplicationService::execute(self, operation)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationOperation {
    Start,
    Stop,
    Restart,
    GetStatus,
    ProfileAdd {
        subscription_url: SubscriptionUrl,
    },
    ProfileList,
    ProfileUse {
        profile: String,
    },
    ProfileRemove {
        profile: String,
    },
    ProxyList {
        group: String,
    },
    ProxySelect {
        group: String,
        node: String,
    },
    LatencyList,
    LatencyShow {
        node: String,
    },
    RuleList,
    RuleAdd {
        rule: String,
        placement: RulePlacement,
    },
    RuleReplace {
        old_rule: String,
        new_rule: String,
    },
    RuleRemove {
        rule: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RulePlacement {
    Prepend,
    Append,
    Before(String),
    After(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationOutput {
    Status(StatusSnapshot),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationError {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub details: Option<ApplicationErrorDetails>,
}

impl ApplicationError {
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            code,
            message: message.into(),
            retryable,
            details: None,
        }
    }

    #[must_use]
    pub fn with_details(mut self, details: ApplicationErrorDetails) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplicationErrorDetails {
    CandidateIds { candidate_ids: Vec<String> },
}

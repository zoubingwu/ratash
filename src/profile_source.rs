use crate::domain::SubscriptionUrl;
use crate::profile::RefreshStage;
use async_trait::async_trait;
use futures_util::StreamExt;
use percent_encoding::percent_decode_str;
use reqwest::header::{CONTENT_DISPOSITION, HeaderMap};
use reqwest::redirect::{Attempt, Policy};
use std::fmt;
use std::time::Duration;
use tokio::time::timeout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProfileSourcePolicy {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub total_timeout: Duration,
    pub max_redirects: usize,
    pub max_body_bytes: usize,
    pub max_metadata_name_bytes: usize,
}

impl ProfileSourcePolicy {
    #[must_use]
    pub const fn product() -> Self {
        Self {
            connect_timeout: crate::constants::PROFILE_CONNECT_TIMEOUT,
            request_timeout: crate::constants::PROFILE_REQUEST_TIMEOUT,
            total_timeout: crate::constants::PROFILE_TOTAL_TIMEOUT,
            max_redirects: crate::constants::PROFILE_REDIRECT_LIMIT,
            max_body_bytes: crate::constants::PROFILE_RESPONSE_MAX_BYTES,
            max_metadata_name_bytes: crate::constants::PROFILE_METADATA_NAME_MAX_BYTES,
        }
    }
}

impl Default for ProfileSourcePolicy {
    fn default() -> Self {
        Self::product()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProfileDownload {
    body: Vec<u8>,
    metadata_name: Option<String>,
    safe_final_url: String,
}

impl ProfileDownload {
    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    #[must_use]
    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    #[must_use]
    pub fn metadata_name(&self) -> Option<&str> {
        self.metadata_name.as_deref()
    }

    #[must_use]
    pub fn safe_final_url(&self) -> &str {
        &self.safe_final_url
    }
}

impl fmt::Debug for ProfileDownload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileDownload")
            .field("body_bytes", &self.body.len())
            .field("has_metadata_name", &self.metadata_name.is_some())
            .field("safe_final_url", &self.safe_final_url)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DownloadErrorKind {
    InvalidPolicy,
    ClientInitialization,
    Connect,
    ConnectTimeout,
    Request,
    RequestTimeout,
    TotalTimeout,
    RedirectRejected,
    HttpStatus { status: u16 },
    BodyRead,
    BodyTooLarge { limit: usize },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProfileSourceError {
    kind: DownloadErrorKind,
}

impl ProfileSourceError {
    #[must_use]
    pub const fn stage(&self) -> RefreshStage {
        RefreshStage::Download
    }

    #[must_use]
    pub const fn kind(&self) -> DownloadErrorKind {
        self.kind
    }

    #[must_use]
    pub const fn retryable(&self) -> bool {
        match self.kind {
            DownloadErrorKind::InvalidPolicy
            | DownloadErrorKind::ClientInitialization
            | DownloadErrorKind::RedirectRejected
            | DownloadErrorKind::HttpStatus { status: 400..=499 }
            | DownloadErrorKind::BodyTooLarge { .. } => false,
            DownloadErrorKind::Connect
            | DownloadErrorKind::ConnectTimeout
            | DownloadErrorKind::Request
            | DownloadErrorKind::RequestTimeout
            | DownloadErrorKind::TotalTimeout
            | DownloadErrorKind::HttpStatus { .. }
            | DownloadErrorKind::BodyRead => true,
        }
    }

    const fn new(kind: DownloadErrorKind) -> Self {
        Self { kind }
    }
}

impl fmt::Display for ProfileSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            DownloadErrorKind::InvalidPolicy => {
                formatter.write_str("profile download policy is invalid")
            }
            DownloadErrorKind::ClientInitialization => {
                formatter.write_str("profile download client initialization failed")
            }
            DownloadErrorKind::Connect => formatter.write_str("profile download connection failed"),
            DownloadErrorKind::ConnectTimeout => {
                formatter.write_str("profile download connection timed out")
            }
            DownloadErrorKind::Request => formatter.write_str("profile download request failed"),
            DownloadErrorKind::RequestTimeout => {
                formatter.write_str("profile download request timed out")
            }
            DownloadErrorKind::TotalTimeout => {
                formatter.write_str("profile download exceeded its total time limit")
            }
            DownloadErrorKind::RedirectRejected => {
                formatter.write_str("profile download redirect was rejected")
            }
            DownloadErrorKind::HttpStatus { status } => {
                write!(formatter, "profile download returned HTTP status {status}")
            }
            DownloadErrorKind::BodyRead => {
                formatter.write_str("profile download response body failed")
            }
            DownloadErrorKind::BodyTooLarge { limit } => {
                write!(
                    formatter,
                    "profile download exceeded the {limit}-byte limit"
                )
            }
        }
    }
}

impl std::error::Error for ProfileSourceError {}

#[async_trait]
pub trait ProfileSource: Send + Sync {
    async fn download(
        &self,
        subscription_url: &SubscriptionUrl,
    ) -> Result<ProfileDownload, ProfileSourceError>;
}

#[derive(Clone)]
pub struct ReqwestProfileSource {
    client: reqwest::Client,
    policy: ProfileSourcePolicy,
}

impl ReqwestProfileSource {
    pub fn new(policy: ProfileSourcePolicy) -> Result<Self, ProfileSourceError> {
        if policy.connect_timeout.is_zero()
            || policy.request_timeout.is_zero()
            || policy.total_timeout.is_zero()
            || policy.max_body_bytes == 0
            || policy.max_metadata_name_bytes == 0
        {
            return Err(ProfileSourceError::new(DownloadErrorKind::InvalidPolicy));
        }
        let max_redirects = policy.max_redirects;
        let redirect_policy = Policy::custom(move |attempt| redirect(attempt, max_redirects));
        let client = reqwest::Client::builder()
            .connect_timeout(policy.connect_timeout)
            .redirect(redirect_policy)
            .no_proxy()
            .build()
            .map_err(|_| ProfileSourceError::new(DownloadErrorKind::ClientInitialization))?;
        Ok(Self { client, policy })
    }

    async fn download_with_deadlines(
        &self,
        subscription_url: &SubscriptionUrl,
    ) -> Result<ProfileDownload, ProfileSourceError> {
        let request = self.client.get(subscription_url.expose().clone());
        let response = timeout(self.policy.request_timeout, request.send())
            .await
            .map_err(|_| ProfileSourceError::new(DownloadErrorKind::RequestTimeout))?
            .map_err(map_request_error)?;

        if response.status().is_redirection() {
            return Err(ProfileSourceError::new(DownloadErrorKind::RedirectRejected));
        }
        if !response.status().is_success() {
            return Err(ProfileSourceError::new(DownloadErrorKind::HttpStatus {
                status: response.status().as_u16(),
            }));
        }

        if response
            .content_length()
            .is_some_and(|length| length > self.policy.max_body_bytes as u64)
        {
            return Err(ProfileSourceError::new(DownloadErrorKind::BodyTooLarge {
                limit: self.policy.max_body_bytes,
            }));
        }

        let metadata_name = metadata_name(response.headers(), self.policy.max_metadata_name_bytes);
        let final_url = SubscriptionUrl::parse(response.url().as_str())
            .map_err(|_| ProfileSourceError::new(DownloadErrorKind::RedirectRejected))?;
        let safe_final_url = final_url.redacted();
        let initial_capacity = response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or_default()
            .min(self.policy.max_body_bytes);
        let mut body = Vec::with_capacity(initial_capacity);
        let mut stream = response.bytes_stream();

        loop {
            let next = timeout(self.policy.request_timeout, stream.next())
                .await
                .map_err(|_| ProfileSourceError::new(DownloadErrorKind::RequestTimeout))?;
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(|_| ProfileSourceError::new(DownloadErrorKind::BodyRead))?;
            if body.len().saturating_add(chunk.len()) > self.policy.max_body_bytes {
                return Err(ProfileSourceError::new(DownloadErrorKind::BodyTooLarge {
                    limit: self.policy.max_body_bytes,
                }));
            }
            body.extend_from_slice(&chunk);
        }

        Ok(ProfileDownload {
            body,
            metadata_name,
            safe_final_url,
        })
    }
}

#[async_trait]
impl ProfileSource for ReqwestProfileSource {
    async fn download(
        &self,
        subscription_url: &SubscriptionUrl,
    ) -> Result<ProfileDownload, ProfileSourceError> {
        timeout(
            self.policy.total_timeout,
            self.download_with_deadlines(subscription_url),
        )
        .await
        .map_err(|_| ProfileSourceError::new(DownloadErrorKind::TotalTimeout))?
    }
}

fn redirect(attempt: Attempt<'_>, max_redirects: usize) -> reqwest::redirect::Action {
    if !matches!(attempt.url().scheme(), "http" | "https")
        || attempt.previous().len() > max_redirects
    {
        attempt.error(RedirectPolicyError)
    } else {
        attempt.follow()
    }
}

fn map_request_error(error: reqwest::Error) -> ProfileSourceError {
    let kind = if error.is_redirect() {
        DownloadErrorKind::RedirectRejected
    } else if error.is_connect() && error.is_timeout() {
        DownloadErrorKind::ConnectTimeout
    } else if error.is_connect() {
        DownloadErrorKind::Connect
    } else if error.is_timeout() {
        DownloadErrorKind::RequestTimeout
    } else {
        DownloadErrorKind::Request
    };
    ProfileSourceError::new(kind)
}

fn metadata_name(headers: &HeaderMap, max_bytes: usize) -> Option<String> {
    headers
        .get("profile-title")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| validate_metadata_name(value, max_bytes, false))
        .or_else(|| {
            headers
                .get(CONTENT_DISPOSITION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| content_disposition_filename(value, max_bytes))
                .and_then(|value| validate_metadata_name(&value, max_bytes, true))
        })
}

fn validate_metadata_name(
    value: &str,
    max_bytes: usize,
    strip_yaml_suffix: bool,
) -> Option<String> {
    let value = value.trim();
    let value = if strip_yaml_suffix {
        value
            .strip_suffix(".yaml")
            .or_else(|| value.strip_suffix(".yml"))
            .unwrap_or(value)
    } else {
        value
    };
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.to_owned())
}

fn content_disposition_filename(value: &str, max_bytes: usize) -> Option<String> {
    let mut plain = None;
    let mut encoded = None;
    let max_parameter_bytes = max_bytes.saturating_mul(3).saturating_add(16);
    for parameter in value.split(';').skip(1) {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            continue;
        };
        let value = value.trim();
        if value.len() > max_parameter_bytes {
            continue;
        }
        let Some(value) = unquote(value) else {
            continue;
        };
        if name.eq_ignore_ascii_case("filename*") {
            encoded = decode_extended_filename(&value);
        } else if name.eq_ignore_ascii_case("filename") {
            plain = Some(value);
        }
    }
    encoded.or(plain)
}

fn unquote(value: &str) -> Option<String> {
    if let Some(value) = value.strip_prefix('"') {
        let value = value.strip_suffix('"')?;
        let mut output = String::with_capacity(value.len());
        let mut chars = value.chars();
        while let Some(character) = chars.next() {
            if character == '\\' {
                output.push(chars.next()?);
            } else {
                output.push(character);
            }
        }
        Some(output)
    } else {
        Some(value.to_owned())
    }
}

fn decode_extended_filename(value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '\'');
    let charset = parts.next()?;
    let _language = parts.next()?;
    let encoded = parts.next()?;
    if !charset.eq_ignore_ascii_case("utf-8") {
        return None;
    }
    percent_decode_str(encoded)
        .decode_utf8()
        .ok()
        .map(|value| value.into_owned())
}

#[derive(Debug)]
struct RedirectPolicyError;

impl fmt::Display for RedirectPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("redirect rejected by profile source policy")
    }
}

impl std::error::Error for RedirectPolicyError {}

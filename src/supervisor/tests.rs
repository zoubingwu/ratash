//! Verifies focused Supervisor concurrency and activation queue behavior.

use super::*;
use crate::profile_source::{ProfileDownload, ProfileSource, ProfileSourceError};
use async_trait::async_trait;
use std::sync::Barrier;
use std::sync::atomic::{AtomicUsize, Ordering};

struct ConcurrentSource {
    active: AtomicUsize,
    maximum: AtomicUsize,
    entered: Barrier,
}

#[async_trait]
impl ProfileSource for ConcurrentSource {
    async fn download(
        &self,
        subscription_url: &SubscriptionUrl,
    ) -> Result<ProfileDownload, ProfileSourceError> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.maximum.fetch_max(active, Ordering::AcqRel);
        self.entered.wait();
        self.active.fetch_sub(1, Ordering::AcqRel);
        Ok(ProfileDownload::from_parts(
            b"rules: [MATCH,DIRECT]".to_vec(),
            None,
            subscription_url.redacted(),
        ))
    }
}

#[test]
fn blocking_adapter_runs_two_profile_downloads_concurrently() {
    let source = Arc::new(ConcurrentSource {
        active: AtomicUsize::new(0),
        maximum: AtomicUsize::new(0),
        entered: Barrier::new(crate::constants::PROFILE_REFRESH_CONCURRENCY),
    });
    let adapter = Arc::new(
        BlockingProfileFetchPort::new(source.clone())
            .expect("the Profile download runtime should start"),
    );
    let url = SubscriptionUrl::parse("https://example.test/profile.yaml")
        .expect("the fixture URL should be valid");
    let workers = (0..crate::constants::PROFILE_REFRESH_CONCURRENCY)
        .map(|_| {
            let adapter = Arc::clone(&adapter);
            let url = url.clone();
            std::thread::spawn(move || adapter.fetch(&url))
        })
        .collect::<Vec<_>>();

    for worker in workers {
        worker
            .join()
            .expect("the download worker should finish")
            .expect("the fixture download should succeed");
    }
    assert_eq!(
        source.maximum.load(Ordering::Acquire),
        crate::constants::PROFILE_REFRESH_CONCURRENCY
    );
}

#[test]
fn activation_queue_keeps_only_the_latest_pending_target() {
    let mut queue = ActivationQueue {
        running: true,
        pending: None,
    };
    let superseded = queue.enqueue("secondary");
    let latest = queue.enqueue("tertiary");

    let error = superseded
        .wait()
        .expect_err("the older pending activation should be superseded");
    assert_eq!(error.code, ErrorCode::OperationUnavailable);
    assert!(error.retryable);
    assert_eq!(
        queue
            .pending
            .as_ref()
            .map(|pending| pending.selector.as_str()),
        Some("tertiary")
    );
    assert!(Arc::ptr_eq(
        &queue
            .pending
            .as_ref()
            .expect("the latest activation should remain pending")
            .completion,
        &latest
    ));
}

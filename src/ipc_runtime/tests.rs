use super::*;

use std::fs;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use mio::{Events, Poll, Waker};

use crate::application::{
    ApplicationClient, ApplicationError, ApplicationOperation, ApplicationOutput,
    RulePlacement as ApplicationRulePlacement,
};
use crate::cancellation::CancellationToken;
use crate::constants::IPC_REQUEST_TIMEOUT;
use crate::domain::SubscriptionUrl;
use crate::ipc::{PeerAuthorizationError, PeerAuthorizer};

use super::client::CLIENT_CANCEL_TOKEN;
use super::client_error::poll_for_ipc_connect;

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use crate::constants::{
        CORE_HEALTH_TIMEOUT, CORE_READINESS_TIMEOUT, MIHOMO_VALIDATION_TIMEOUT,
        PROFILE_TOTAL_TIMEOUT,
    };

    struct IdleAuthorizer {
        calls: AtomicUsize,
    }

    impl PeerAuthorizer for IdleAuthorizer {
        fn authorize(&self, _peer: &UnixStream) -> Result<(), PeerAuthorizationError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    struct IdleApplication {
        calls: AtomicUsize,
    }

    impl ApplicationClient for IdleApplication {
        fn execute(
            &self,
            _operation: ApplicationOperation,
        ) -> Result<ApplicationOutput, ApplicationError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(ApplicationOutput::Status(
                crate::application::ApplicationService::new().status(),
            ))
        }
    }

    #[test]
    fn product_client_covers_the_complete_bounded_mutation_path() {
        let client = IpcClient::new("/tmp/hopash-timeout-contract.sock");
        let profile_add = ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/profile.yaml")
                .expect("the fixture URL should be valid"),
        };
        let minimum_profile_add = PROFILE_TOTAL_TIMEOUT
            .saturating_add(MIHOMO_VALIDATION_TIMEOUT)
            .saturating_add(CORE_READINESS_TIMEOUT)
            .saturating_add(CORE_HEALTH_TIMEOUT);
        assert!(client.response_timeout(&profile_add) > minimum_profile_add);

        let rule_add = ApplicationOperation::RuleAdd {
            rule: "MATCH,DIRECT".to_owned(),
            placement: ApplicationRulePlacement::Append,
        };
        let minimum_runtime_mutation = MIHOMO_VALIDATION_TIMEOUT
            .saturating_add(CORE_READINESS_TIMEOUT)
            .saturating_add(CORE_HEALTH_TIMEOUT);
        assert!(client.response_timeout(&rule_add) > minimum_runtime_mutation);
        assert_eq!(
            client.response_timeout(&ApplicationOperation::GetStatus),
            IPC_REQUEST_TIMEOUT
        );
    }

    #[test]
    fn explicit_test_timeouts_remain_exact_for_every_operation() {
        let timeout = Duration::from_millis(7);
        let client = IpcClient::with_timeouts(
            "/tmp/hopash-fixed-timeout.sock",
            Duration::from_millis(5),
            timeout,
        );
        let operation = ApplicationOperation::ProfileAdd {
            subscription_url: SubscriptionUrl::parse("https://example.test/profile.yaml")
                .expect("the fixture URL should be valid"),
        };
        assert_eq!(client.response_timeout(&operation), timeout);
        assert_eq!(client.stream_timeout(), timeout);
    }

    #[test]
    fn cancellation_wakes_a_stalled_connect_poll() {
        let mut poll = Poll::new().expect("fixture poll should initialize");
        let waker = Arc::new(
            Waker::new(poll.registry(), CLIENT_CANCEL_TOKEN)
                .expect("fixture cancellation waker should initialize"),
        );
        let cancellation = CancellationToken::default();
        let interrupt_waker = Arc::clone(&waker);
        let _registration = cancellation.register_interrupt(move || {
            let _ = interrupt_waker.wake();
        });
        let worker_cancellation = cancellation.clone();
        let worker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            worker_cancellation.cancel();
        });
        let mut events = Events::with_capacity(2);
        let started = Instant::now();

        let error = poll_for_ipc_connect(
            &mut poll,
            &mut events,
            Duration::from_secs(5),
            &cancellation,
        )
        .expect_err("cancellation should wake the stalled connect poll");

        worker.join().expect("cancellation worker should stop");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(started.elapsed() < Duration::from_millis(250));
    }

    #[test]
    fn idle_server_blocks_without_periodic_wakes_and_shutdown_bypasses_handlers() {
        let root = PathBuf::from("/tmp").join(format!(
            "hopash-idle-ipc-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let socket = root.join("supervisor.sock");
        let application = Arc::new(IdleApplication {
            calls: AtomicUsize::new(0),
        });
        let authorizer = Arc::new(IdleAuthorizer {
            calls: AtomicUsize::new(0),
        });
        let mut server = IpcServer::start(
            &socket,
            Arc::clone(&application),
            Arc::clone(&authorizer),
            IpcServerConfig {
                io_timeout: Duration::from_millis(100),
                worker_count: 1,
                pending_connection_capacity: 1,
            },
        )
        .expect("the idle fixture server should start");

        thread::sleep(Duration::from_millis(75));
        assert_eq!(server.accept_metrics.poll_returns(), 0);
        assert_eq!(authorizer.calls.load(Ordering::Relaxed), 0);
        assert_eq!(application.calls.load(Ordering::Relaxed), 0);
        let started = std::time::Instant::now();
        server
            .shutdown()
            .expect("the idle fixture server should stop");

        assert!(started.elapsed() < Duration::from_millis(250));
        assert_eq!(server.accept_metrics.poll_returns(), 1);
        assert_eq!(authorizer.calls.load(Ordering::Relaxed), 0);
        assert_eq!(application.calls.load(Ordering::Relaxed), 0);
        assert!(!socket.exists());
        let _ = fs::remove_dir(&root);
    }
}

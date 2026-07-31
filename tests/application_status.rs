use hopash::application::{ApplicationOperation, ApplicationOutput, ApplicationService};
use hopash::domain::{CoreLifecycle, SupervisorLifecycle, TunReason};

#[test]
fn new_application_is_ready_with_an_unconfigured_core() {
    let application = ApplicationService::new();

    let status = application.status();

    assert_eq!(status.supervisor.lifecycle, SupervisorLifecycle::Ready);
    assert_eq!(status.core.lifecycle, CoreLifecycle::Unconfigured);
    assert!(status.tun.requested);
    assert!(!status.tun.capable);
    assert!(!status.tun.effective);
    assert_eq!(status.tun.reason, Some(TunReason::NoActiveProfile));
}

#[test]
fn get_status_operation_returns_the_same_application_snapshot() {
    let application = ApplicationService::new();

    let output = application
        .execute(ApplicationOperation::GetStatus)
        .expect("status should be available from a new application");

    let ApplicationOutput::Status(status) = output else {
        panic!("status operation should return a status output");
    };
    assert_eq!(status.supervisor.lifecycle, SupervisorLifecycle::Ready);
    assert_eq!(status.core.lifecycle, CoreLifecycle::Unconfigured);
}

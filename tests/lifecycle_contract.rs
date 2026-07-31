use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use hopash::lifecycle::{
    DirectoryLease, InstanceRecord, LeaseAcquisition, LifecycleErrorKind, ProcessIdentity,
    ProcessInspector, StatePaths, remove_verified_stale_socket,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: std::path::PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hopash-lifecycle-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory should be created");
        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.path).expect("test directory should be removed");
    }
}

#[derive(Default)]
struct FakeInspector {
    identities: Mutex<BTreeMap<u32, String>>,
}

impl FakeInspector {
    fn set(&self, pid: u32, identity: Option<&str>) {
        let mut identities = self.identities.lock().expect("fixture lock should work");
        match identity {
            Some(identity) => {
                identities.insert(pid, identity.to_owned());
            }
            None => {
                identities.remove(&pid);
            }
        }
    }
}

impl ProcessInspector for FakeInspector {
    fn identity(&self, pid: u32) -> io::Result<Option<String>> {
        Ok(self
            .identities
            .lock()
            .expect("fixture lock should work")
            .get(&pid)
            .cloned())
    }
}

#[test]
fn state_paths_use_an_absolute_override_or_the_macos_user_location() {
    let override_paths = StatePaths::from_environment(
        Some(OsStr::new("/tmp/hopash-state-fixture")),
        Some(OsStr::new("/Users/example")),
    )
    .expect("absolute override should resolve");
    let default_paths = StatePaths::from_environment(None, Some(OsStr::new("/Users/example")))
        .expect("home path should resolve");

    assert_eq!(
        override_paths.root,
        std::path::Path::new("/tmp/hopash-state-fixture")
    );
    assert_eq!(
        default_paths.root,
        std::path::Path::new("/Users/example/Library/Application Support/Hopash RS")
    );
    assert_eq!(
        default_paths.ipc_socket,
        default_paths.root.join("supervisor.sock")
    );
    assert_eq!(
        default_paths.shutdown_socket,
        default_paths.root.join("supervisor-control.sock")
    );
    assert_eq!(
        StatePaths::from_environment(Some(OsStr::new("relative")), None)
            .expect_err("relative override should fail")
            .kind(),
        LifecycleErrorKind::InvalidStateRoot
    );
}

#[test]
fn live_process_ownership_makes_singleton_acquisition_idempotent() {
    let directory = TestDirectory::new();
    let inspector = FakeInspector::default();
    inspector.set(41, Some("process-start-a"));
    let process = ProcessIdentity {
        pid: 41,
        start_identity: "process-start-a".to_owned(),
    };
    let first =
        match DirectoryLease::acquire(&directory.path, "supervisor", process.clone(), &inspector)
            .expect("first acquisition should succeed")
        {
            LeaseAcquisition::Acquired(lease) => lease,
            LeaseAcquisition::HeldByLiveProcess(_) => {
                panic!("first acquisition should own the lease")
            }
        };

    let second = DirectoryLease::acquire(&directory.path, "supervisor", process, &inspector)
        .expect("second acquisition should inspect the owner");

    match second {
        LeaseAcquisition::HeldByLiveProcess(owner) => {
            assert_eq!(owner.process.pid, 41);
            assert_eq!(owner.instance_token(), first.owner().instance_token());
        }
        LeaseAcquisition::Acquired(_) => {
            panic!("live ownership should remain with the first lease")
        }
    }
}

#[test]
fn stale_process_identity_is_quarantined_before_reacquisition() {
    let directory = TestDirectory::new();
    let inspector = FakeInspector::default();
    inspector.set(42, Some("old-start"));
    let old = match DirectoryLease::acquire(
        &directory.path,
        "supervisor",
        ProcessIdentity {
            pid: 42,
            start_identity: "old-start".to_owned(),
        },
        &inspector,
    )
    .expect("old lease should acquire")
    {
        LeaseAcquisition::Acquired(lease) => lease,
        LeaseAcquisition::HeldByLiveProcess(_) => panic!("fixture lease should be new"),
    };
    let old_token = old.owner().instance_token().to_owned();
    std::mem::forget(old);
    inspector.set(42, None);
    inspector.set(43, Some("new-start"));

    let replacement = match DirectoryLease::acquire(
        &directory.path,
        "supervisor",
        ProcessIdentity {
            pid: 43,
            start_identity: "new-start".to_owned(),
        },
        &inspector,
    )
    .expect("stale lease should be replaced")
    {
        LeaseAcquisition::Acquired(lease) => lease,
        LeaseAcquisition::HeldByLiveProcess(_) => panic!("stale lease should not remain live"),
    };

    assert_ne!(replacement.owner().instance_token(), old_token);
    assert_eq!(replacement.owner().process.pid, 43);
}

#[test]
fn releasing_a_lease_allows_a_clean_reacquisition() {
    let directory = TestDirectory::new();
    let inspector = FakeInspector::default();
    inspector.set(44, Some("same-process"));
    let process = ProcessIdentity {
        pid: 44,
        start_identity: "same-process".to_owned(),
    };
    let first =
        match DirectoryLease::acquire(&directory.path, "lifecycle", process.clone(), &inspector)
            .expect("first lease should acquire")
        {
            LeaseAcquisition::Acquired(lease) => lease,
            LeaseAcquisition::HeldByLiveProcess(_) => panic!("fixture lease should be new"),
        };
    first.release().expect("first lease should release");

    assert!(matches!(
        DirectoryLease::acquire(&directory.path, "lifecycle", process, &inspector)
            .expect("released lease should reacquire"),
        LeaseAcquisition::Acquired(_)
    ));
}

#[test]
fn instance_records_round_trip_with_private_permissions_and_redacted_debug() {
    let directory = TestDirectory::new();
    let paths = StatePaths::for_root(directory.path.join("state-root"));
    paths.prepare().expect("state paths should prepare");
    let inspector = FakeInspector::default();
    inspector.set(45, Some("record-start"));
    let lease = match DirectoryLease::acquire(
        &paths.root,
        "supervisor",
        ProcessIdentity {
            pid: 45,
            start_identity: "record-start".to_owned(),
        },
        &inspector,
    )
    .expect("lease should acquire")
    {
        LeaseAcquisition::Acquired(lease) => lease,
        LeaseAcquisition::HeldByLiveProcess(_) => panic!("fixture lease should be new"),
    };
    let record = InstanceRecord::new(lease.owner(), 1_234, paths.ipc_socket.clone());

    record
        .write_private(&paths.instance_record)
        .expect("instance record should write");
    let loaded = InstanceRecord::read_private(&paths.instance_record)
        .expect("instance record should read")
        .expect("instance record should exist");

    assert_eq!(loaded, record);
    assert_eq!(
        fs::metadata(&paths.instance_record)
            .expect("instance metadata should exist")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let debug = format!("{record:?}");
    assert!(!debug.contains(record.instance_token()));
    assert!(!debug.contains(paths.ipc_socket.to_string_lossy().as_ref()));
}

#[test]
fn stale_socket_cleanup_accepts_only_a_real_unix_socket() {
    let directory = TestDirectory::new();
    let socket_path = directory.path.join("stale.sock");
    let listener = UnixListener::bind(&socket_path).expect("fixture socket should bind");
    drop(listener);

    assert!(remove_verified_stale_socket(&socket_path).expect("real socket should be removed"));
    assert!(!socket_path.exists());
    fs::write(&socket_path, b"valuable file").expect("fixture file should write");

    let error =
        remove_verified_stale_socket(&socket_path).expect_err("regular file should be preserved");

    assert_eq!(error.kind(), LifecycleErrorKind::UnsafeSocketCleanupTarget);
    assert_eq!(
        fs::read(&socket_path).expect("regular file should remain"),
        b"valuable file"
    );
}

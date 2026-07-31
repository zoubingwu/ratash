use std::fmt::Write as _;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};

use hopash::domain::{LocalRuleSetRevision, ProfileId, RuntimeGeneration};
use hopash::persistence::{ObjectId, PersistenceStore, TransactionBundle, TransactionId};
use hopash::profile::ProfileRevision;
use sha2::{Digest, Sha256};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: std::path::PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hopash-persistence-{}-{sequence}",
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

#[test]
fn immutable_objects_are_content_addressed_and_readable() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");

    let id = store.put_object(b"hello").expect("object should be stored");

    assert_eq!(
        id,
        ObjectId::parse("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
            .expect("known SHA-256 ID should parse")
    );
    assert_eq!(
        store.read_object(&id).expect("object should be readable"),
        b"hello"
    );
    assert_eq!(
        store
            .put_object(b"hello")
            .expect("duplicate content should be accepted"),
        id
    );
}

#[test]
fn interrupted_object_install_is_repaired_by_the_next_identical_write() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let content = b"complete immutable content";
    let id = store.put_object(content).expect("object should be stored");
    fs::write(directory.path.join("objects").join(id.as_str()), b"partial")
        .expect("fixture should simulate a partial final object");

    assert_eq!(
        store
            .put_object(content)
            .expect("object should be repaired"),
        id
    );
    assert_eq!(
        store.read_object(&id).expect("repaired object should read"),
        content
    );
}

#[cfg(unix)]
#[test]
fn state_directories_and_objects_are_user_private() {
    let directory = TestDirectory::new();
    fs::set_permissions(&directory.path, fs::Permissions::from_mode(0o755))
        .expect("test permissions should be widened");
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let id = store
        .put_object(b"private")
        .expect("object should be stored");

    let root_mode = fs::metadata(&directory.path)
        .expect("root metadata should be available")
        .permissions()
        .mode()
        & 0o777;
    let object_mode = fs::metadata(directory.path.join("objects").join(id.as_str()))
        .expect("object metadata should be available")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(root_mode, 0o700);
    assert_eq!(object_mode, 0o600);
}

#[test]
fn prepared_transaction_survives_restart_for_recovery() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let bundle = transaction_bundle(&store, "profile-a", 7);

    let prepared = store
        .prepare(&bundle)
        .expect("transaction should be prepared");
    drop(store);

    let reopened = PersistenceStore::open(&directory.path).expect("store should reopen");
    let recovery = reopened.recover().expect("state should be recovered");

    assert_eq!(recovery.committed, None);
    assert_eq!(recovery.prepared, Some(prepared.clone()));
    assert_eq!(prepared.previous, None);
    assert_eq!(
        reopened
            .load_transaction(&prepared.candidate)
            .expect("candidate transaction should load"),
        bundle
    );
}

#[test]
fn commit_switches_the_manifest_and_journal_cleanup_is_explicit() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let prepared = store
        .prepare(&transaction_bundle(&store, "profile-a", 1))
        .expect("transaction should be prepared");

    store
        .commit_prepared(&prepared)
        .expect("prepared transaction should commit");

    let recovery = store.recover().expect("state should recover");
    let committed = recovery.committed.expect("manifest should be committed");
    assert_eq!(committed.current, prepared.candidate);
    assert_eq!(committed.previous, None);
    assert_eq!(recovery.prepared, Some(prepared.clone()));

    store
        .clear_prepared(&prepared)
        .expect("prepared journal should clear explicitly");
    assert_eq!(
        store.recover().expect("state should recover").prepared,
        None
    );
}

#[test]
fn commit_revalidates_every_bundle_object_before_switching_the_manifest() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let bundle = transaction_bundle(&store, "profile-a", 1);
    let prepared = store.prepare(&bundle).expect("transaction should prepare");
    fs::write(
        directory
            .path
            .join("objects")
            .join(bundle.profile_snapshot.as_str()),
        b"corrupted",
    )
    .expect("fixture should corrupt a referenced object");

    let error = store
        .commit_prepared(&prepared)
        .expect_err("corrupt object should block commit");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    let recovery = store.recover().expect("state should recover");
    assert_eq!(recovery.committed, None);
    assert_eq!(recovery.prepared, Some(prepared));
}

#[test]
fn committed_manifest_retains_current_and_previous_generations() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let first_bundle = transaction_bundle(&store, "profile-a", 1);
    let first = store
        .prepare(&first_bundle)
        .expect("first transaction should be prepared");
    store
        .commit_prepared(&first)
        .expect("first transaction should commit");
    store
        .clear_prepared(&first)
        .expect("first journal should clear");

    let second_bundle = transaction_bundle(&store, "profile-b", 2);
    let second = store
        .prepare(&second_bundle)
        .expect("second transaction should be prepared");
    assert_eq!(second.previous, Some(first.candidate.clone()));
    store
        .commit_prepared(&second)
        .expect("second transaction should commit");
    store
        .clear_prepared(&second)
        .expect("second journal should clear");

    let committed = store
        .recover()
        .expect("state should recover")
        .committed
        .expect("manifest should exist");
    assert_eq!(committed.current, second.candidate);
    assert_eq!(committed.previous, Some(first.candidate.clone()));
    assert_eq!(
        store
            .load_transaction(&committed.current)
            .expect("current bundle should load"),
        second_bundle
    );
    assert_eq!(
        store
            .load_transaction(committed.previous.as_ref().expect("previous should exist"))
            .expect("previous bundle should load"),
        first_bundle
    );
}

#[test]
fn new_prepare_preserves_an_interrupted_journal() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let interrupted = store
        .prepare(&transaction_bundle(&store, "profile-a", 1))
        .expect("first transaction should be prepared");

    let error = store
        .prepare(&transaction_bundle(&store, "profile-b", 2))
        .expect_err("second prepare should wait for recovery");

    assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    assert_eq!(
        store.recover().expect("state should recover").prepared,
        Some(interrupted)
    );
}

#[test]
fn prepare_requires_every_referenced_immutable_object() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let mut bundle = transaction_bundle(&store, "profile-a", 1);
    bundle.supervisor_state =
        ObjectId::parse("0000000000000000000000000000000000000000000000000000000000000000")
            .expect("fixture object ID should parse");

    let error = store
        .prepare(&bundle)
        .expect_err("missing state object should reject the transaction");

    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    let recovery = store.recover().expect("state should recover");
    assert_eq!(recovery.committed, None);
    assert_eq!(recovery.prepared, None);
}

#[test]
fn interrupted_apply_keeps_the_committed_generation_authoritative() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let first = store
        .prepare(&transaction_bundle(&store, "profile-a", 1))
        .expect("first transaction should be prepared");
    store
        .commit_prepared(&first)
        .expect("first transaction should commit");
    store
        .clear_prepared(&first)
        .expect("first journal should clear");
    let interrupted = store
        .prepare(&transaction_bundle(&store, "profile-b", 2))
        .expect("second transaction should be prepared");

    let recovery = store.recover().expect("state should recover");

    assert_eq!(
        recovery.committed.expect("manifest should exist").current,
        first.candidate
    );
    assert_eq!(recovery.prepared, Some(interrupted));
}

#[cfg(unix)]
#[test]
fn transaction_state_files_are_user_private() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let prepared = store
        .prepare(&transaction_bundle(&store, "profile-a", 1))
        .expect("transaction should be prepared");
    store
        .commit_prepared(&prepared)
        .expect("transaction should commit");

    for path in [
        directory.path.join("prepared.json"),
        directory.path.join("manifest.json"),
        directory
            .path
            .join("transactions")
            .join(prepared.candidate.as_str()),
    ] {
        let mode = fs::metadata(path)
            .expect("state metadata should be available")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}

#[test]
fn transaction_loading_rejects_invalid_typed_identifiers() {
    let directory = TestDirectory::new();
    let store = PersistenceStore::open(&directory.path).expect("store should open");
    let object = store
        .put_object(b"referenced")
        .expect("fixture object should be stored");
    let content = format!(
        concat!(
            "{{\"schema_version\":1,",
            "\"supervisor_state\":\"{object}\",",
            "\"profile_snapshot\":\"{object}\",",
            "\"local_rule_set\":\"{object}\",",
            "\"effective_configuration\":\"{object}\",",
            "\"profile_revision\":1,",
            "\"local_rule_set_revision\":1,",
            "\"active_profile_id\":\"profile-a\",",
            "\"runtime_generation\":1}}"
        ),
        object = object.as_str()
    );
    let digest = Sha256::digest(content.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut output, byte| {
            write!(output, "{byte:02x}").expect("writing to a String should succeed");
            output
        },
    );
    fs::write(directory.path.join("transactions").join(&digest), content)
        .expect("fixture transaction should be written");
    let id = TransactionId::parse(&digest).expect("fixture transaction ID should parse");

    let error = store
        .load_transaction(&id)
        .expect_err("invalid Profile ID should reject the transaction");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

fn transaction_bundle(
    store: &PersistenceStore,
    active_profile_id: &str,
    runtime_generation: u64,
) -> TransactionBundle {
    let active_profile_id = match active_profile_id {
        "profile-a" => ProfileId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8"),
        "profile-b" => ProfileId::parse("7c9e6679-7425-40de-944b-e07fc1f90ae7"),
        other => panic!("unexpected fixture profile label: {other}"),
    }
    .expect("fixture Profile ID should parse");
    TransactionBundle {
        supervisor_state: store
            .put_object(format!("state-{runtime_generation}").as_bytes())
            .expect("supervisor state should be stored"),
        profile_snapshot: store
            .put_object(format!("snapshot-{runtime_generation}").as_bytes())
            .expect("profile snapshot should be stored"),
        local_rule_set: store
            .put_object(format!("rules-{runtime_generation}").as_bytes())
            .expect("local rule set should be stored"),
        effective_configuration: store
            .put_object(format!("config-{runtime_generation}").as_bytes())
            .expect("effective configuration should be stored"),
        profile_revision: ProfileRevision(runtime_generation * 10),
        local_rule_set_revision: LocalRuleSetRevision(runtime_generation * 100),
        active_profile_id,
        runtime_generation: RuntimeGeneration(runtime_generation),
    }
}

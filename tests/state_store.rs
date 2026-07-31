use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use hopash::domain::{NodeRecordId, ProfileId, RuntimeGeneration, SubscriptionUrl};
use hopash::profile::{
    Profile, ProfileCatalog, ProfileRevision, ProfileSnapshot, RefreshFailure, RefreshStage,
    SnapshotLimits,
};
use hopash::rule::{LocalRuleSet, RuleSetLimits, RuleString};
use hopash::state::{AuthoritativeState, AuthoritativeStateStore, StateStoreErrorKind};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: std::path::PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let sequence = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "hopash-state-store-{}-{sequence}",
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
fn committed_state_round_trips_multiple_profiles_and_private_metadata() {
    let directory = TestDirectory::new();
    let store = AuthoritativeStateStore::open(&directory.path).expect("state store should open");
    let expected = fixture_state();
    let bundle = store
        .stage_candidate(AuthoritativeState {
            profiles: &expected.profiles,
            local_rules: &expected.rules,
            effective_configuration: &expected.effective_configuration,
            runtime_generation: RuntimeGeneration(8),
        })
        .expect("candidate should stage");
    let prepared = store
        .persistence()
        .prepare(&bundle)
        .expect("candidate should prepare");
    store
        .persistence()
        .commit_prepared(&prepared)
        .expect("candidate should commit");
    store
        .persistence()
        .clear_prepared(&prepared)
        .expect("journal should clear");

    let hydrated = store
        .load_committed(snapshot_limits(), RuleSetLimits::product())
        .expect("committed state should load")
        .expect("committed state should exist");

    assert_eq!(hydrated.runtime_generation, RuntimeGeneration(8));
    assert_eq!(
        hydrated.effective_configuration,
        expected.effective_configuration
    );
    assert_eq!(hydrated.local_rules, expected.rules);
    assert_eq!(hydrated.profiles, expected.profiles);
}

#[test]
fn removed_inactive_profile_snapshots_are_collected_after_the_previous_generation_expires() {
    let directory = TestDirectory::new();
    let store = AuthoritativeStateStore::open(&directory.path).expect("state store should open");
    let mut expected = fixture_state();
    let inactive_snapshot = expected
        .profiles
        .profiles()
        .find(|profile| profile.name == "Backup")
        .expect("inactive Profile should exist")
        .snapshot
        .raw()
        .to_vec();
    let inactive_snapshot = store
        .persistence()
        .put_object(&inactive_snapshot)
        .expect("inactive snapshot should be stored");

    commit_state(&store, &expected, 1);
    store
        .persistence()
        .prune_unreachable()
        .expect("first pruning should succeed");
    expected
        .profiles
        .remove("Backup")
        .expect("inactive Profile should be removed");
    commit_state(&store, &expected, 2);
    store
        .persistence()
        .prune_unreachable()
        .expect("second pruning should succeed");
    assert!(
        directory
            .path
            .join("objects")
            .join(inactive_snapshot.as_str())
            .exists()
    );

    commit_state(&store, &expected, 3);
    let result = store
        .persistence()
        .prune_unreachable()
        .expect("third pruning should succeed");

    assert!(result.removed_objects > 0);
    assert!(
        !directory
            .path
            .join("objects")
            .join(inactive_snapshot.as_str())
            .exists()
    );
    let hydrated = store
        .load_committed(snapshot_limits(), RuleSetLimits::product())
        .expect("pruned committed state should load")
        .expect("committed state should exist");
    assert_eq!(hydrated.profiles, expected.profiles);
}

#[test]
fn absent_manifest_hydrates_as_a_zero_profile_state() {
    let directory = TestDirectory::new();
    let store = AuthoritativeStateStore::open(&directory.path).expect("state store should open");

    assert_eq!(
        store
            .load_committed(snapshot_limits(), RuleSetLimits::product())
            .expect("empty state should load"),
        None
    );
}

#[test]
fn candidate_requires_one_active_profile_initialized_rules_and_a_generation() {
    let directory = TestDirectory::new();
    let store = AuthoritativeStateStore::open(&directory.path).expect("state store should open");
    let empty_profiles = ProfileCatalog::new();
    let uninitialized_rules = LocalRuleSet::uninitialized();

    let error = store
        .stage_candidate(AuthoritativeState {
            profiles: &empty_profiles,
            local_rules: &uninitialized_rules,
            effective_configuration: b"mode: rule\n",
            runtime_generation: RuntimeGeneration(0),
        })
        .expect_err("incomplete candidate should fail");

    assert_eq!(error.kind(), StateStoreErrorKind::InvalidState);
}

#[test]
fn hydration_rejects_a_corrupted_referenced_snapshot() {
    let directory = TestDirectory::new();
    let store = AuthoritativeStateStore::open(&directory.path).expect("state store should open");
    let expected = fixture_state();
    let bundle = store
        .stage_candidate(AuthoritativeState {
            profiles: &expected.profiles,
            local_rules: &expected.rules,
            effective_configuration: &expected.effective_configuration,
            runtime_generation: RuntimeGeneration(2),
        })
        .expect("candidate should stage");
    let prepared = store
        .persistence()
        .prepare(&bundle)
        .expect("candidate should prepare");
    store
        .persistence()
        .commit_prepared(&prepared)
        .expect("candidate should commit");
    store
        .persistence()
        .clear_prepared(&prepared)
        .expect("journal should clear");
    fs::write(
        directory
            .path
            .join("objects")
            .join(bundle.profile_snapshot.as_str()),
        b"corrupted",
    )
    .expect("fixture should corrupt the snapshot");

    let error = store
        .load_committed(snapshot_limits(), RuleSetLimits::product())
        .expect_err("corrupt snapshot should fail hydration");

    assert_eq!(error.kind(), StateStoreErrorKind::Io);
    assert!(!format!("{error:?} {error}").contains("corrupted"));
}

#[test]
fn hydration_rejects_metadata_that_disagrees_with_the_transaction_bundle() {
    let directory = TestDirectory::new();
    let store = AuthoritativeStateStore::open(&directory.path).expect("state store should open");
    let expected = fixture_state();
    let mut bundle = store
        .stage_candidate(AuthoritativeState {
            profiles: &expected.profiles,
            local_rules: &expected.rules,
            effective_configuration: &expected.effective_configuration,
            runtime_generation: RuntimeGeneration(4),
        })
        .expect("candidate should stage");
    bundle.runtime_generation = RuntimeGeneration(5);
    let prepared = store
        .persistence()
        .prepare(&bundle)
        .expect("fixture bundle should prepare");
    store
        .persistence()
        .commit_prepared(&prepared)
        .expect("fixture bundle should commit");
    store
        .persistence()
        .clear_prepared(&prepared)
        .expect("journal should clear");

    let error = store
        .load_committed(snapshot_limits(), RuleSetLimits::product())
        .expect_err("mismatched generation should fail hydration");

    assert_eq!(error.kind(), StateStoreErrorKind::InvalidState);
}

struct FixtureState {
    profiles: ProfileCatalog,
    rules: LocalRuleSet,
    effective_configuration: Vec<u8>,
}

fn fixture_state() -> FixtureState {
    let active_id =
        ProfileId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8").expect("active ID should parse");
    let inactive_id =
        ProfileId::parse("7c9e6679-7425-40de-944b-e07fc1f90ae7").expect("inactive ID should parse");
    let mut active = profile(active_id, "Primary", "primary", 1);
    active.selections.insert(
        "Proxy".to_owned(),
        NodeRecordId::for_provider("provider-a", "HK 01"),
    );
    let mut inactive = profile(inactive_id, "Backup", "backup", 3);
    inactive.last_error = Some(RefreshFailure {
        stage: RefreshStage::Download,
        safe_message: "Profile download timed out".to_owned(),
    });
    let mut profiles = ProfileCatalog::new();
    profiles
        .insert(active)
        .expect("active Profile should insert");
    profiles
        .insert(inactive)
        .expect("inactive Profile should insert");
    profiles
        .activate(&active_id.to_string())
        .expect("Profile should activate");
    let rules = LocalRuleSet::initialized(vec![
        RuleString::new("DOMAIN-SUFFIX,example.com,DIRECT", 1_024).expect("rule should validate"),
        RuleString::new("MATCH,Proxy", 1_024).expect("rule should validate"),
    ]);
    FixtureState {
        profiles,
        rules,
        effective_configuration: b"mode: rule\ntun:\n  enable: true\n".to_vec(),
    }
}

fn commit_state(store: &AuthoritativeStateStore, state: &FixtureState, generation: u64) {
    let bundle = store
        .stage_candidate(AuthoritativeState {
            profiles: &state.profiles,
            local_rules: &state.rules,
            effective_configuration: &state.effective_configuration,
            runtime_generation: RuntimeGeneration(generation),
        })
        .expect("candidate should stage");
    let prepared = store
        .persistence()
        .prepare(&bundle)
        .expect("candidate should prepare");
    store
        .persistence()
        .commit_prepared(&prepared)
        .expect("candidate should commit");
    store
        .persistence()
        .clear_prepared(&prepared)
        .expect("journal should clear");
}

fn profile(id: ProfileId, name: &str, marker: &str, revision: u64) -> Profile {
    let subscription_url = SubscriptionUrl::parse(&format!(
        "https://user:password@profiles.example.test/{marker}.yaml?token=secret"
    ))
    .expect("fixture URL should parse");
    let snapshot = ProfileSnapshot::parse(
        format!("proxies: []\nrules:\n  - MATCH,{marker}\n").as_bytes(),
        snapshot_limits(),
    )
    .expect("fixture snapshot should parse");
    let mut profile = Profile::new(
        id,
        name.to_owned(),
        subscription_url,
        snapshot,
        1_000,
        2_000,
    );
    profile.revision = ProfileRevision(revision);
    profile
}

fn snapshot_limits() -> SnapshotLimits {
    SnapshotLimits::new(4_096, 16)
}

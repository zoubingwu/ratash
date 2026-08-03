use ratash::domain::{ProfileId, SubscriptionUrl};
use ratash::profile::{
    ActiveProfileRevision, Profile, ProfileCatalog, ProfileInsertError, ProfileRevision,
    ProfileSelectorError, ProfileSnapshot, RefreshCommitError, SnapshotLimits, derive_profile_name,
};

fn profile(id: ProfileId, name: &str, marker: &str) -> Profile {
    let url = SubscriptionUrl::parse(&format!("https://profiles.example.test/{marker}.yaml"))
        .expect("fixture URL should be valid");
    let snapshot = ProfileSnapshot::parse(
        format!("proxies: []\nrules:\n  - MATCH,{marker}\n").as_bytes(),
        SnapshotLimits::new(4_096, 16),
    )
    .expect("fixture snapshot should be valid");
    Profile::new(id, name.to_owned(), url, snapshot, 1_000, 2_000)
}

#[test]
fn selectors_prefer_ids_and_require_unique_case_sensitive_names() {
    let first_id = ProfileId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8")
        .expect("fixture ID should be valid");
    let second_id = ProfileId::parse("7c9e6679-7425-40de-944b-e07fc1f90ae7")
        .expect("fixture ID should be valid");
    let mut catalog = ProfileCatalog::new();
    catalog
        .insert(profile(first_id, "Shared", "first"))
        .expect("first profile should insert");
    catalog
        .insert(profile(second_id, "Shared", "second"))
        .expect("second profile should insert");

    assert_eq!(catalog.resolve(&first_id.to_string()), Ok(first_id));
    assert_eq!(
        catalog.resolve("Shared"),
        Err(ProfileSelectorError::Ambiguous {
            candidate_ids: vec![first_id, second_id]
        })
    );
    assert_eq!(
        catalog.resolve("shared"),
        Err(ProfileSelectorError::NotFound)
    );
}

#[test]
fn active_profile_removal_is_rejected_and_inactive_removal_succeeds() {
    let active_id = ProfileId::new();
    let inactive_id = ProfileId::new();
    let mut catalog = ProfileCatalog::new();
    catalog
        .insert(profile(active_id, "Active", "active"))
        .expect("active profile should insert");
    catalog
        .insert(profile(inactive_id, "Inactive", "inactive"))
        .expect("inactive profile should insert");
    catalog
        .activate(&active_id.to_string())
        .expect("activation should succeed");

    assert_eq!(
        catalog.remove(&active_id.to_string()),
        Err(ProfileSelectorError::Active)
    );
    assert!(catalog.remove(&inactive_id.to_string()).is_ok());
    assert_eq!(catalog.len(), 1);
}

#[test]
fn refresh_commit_rechecks_revision_and_preserves_the_latest_valid_snapshot() {
    let id = ProfileId::new();
    let mut catalog = ProfileCatalog::new();
    catalog
        .insert(profile(id, "Profile", "initial"))
        .expect("profile should insert");
    let context = catalog
        .refresh_context(id)
        .expect("refresh context should be captured");
    let expected = context.profile_revision;
    let refreshed = ProfileSnapshot::parse(
        b"proxies: []\nrules:\n  - MATCH,REFRESHED\n",
        SnapshotLimits::new(4_096, 16),
    )
    .expect("fixture snapshot should be valid");

    let revision = catalog
        .commit_refresh(id, context, refreshed, 3_000, 4_000)
        .expect("matching revision should commit");
    assert_eq!(revision, ProfileRevision(2));
    assert_eq!(
        catalog
            .get(id)
            .expect("profile should exist")
            .snapshot
            .rule_strings(),
        ["MATCH,REFRESHED"]
    );

    let stale = ProfileSnapshot::parse(
        b"proxies: []\nrules:\n  - MATCH,STALE\n",
        SnapshotLimits::new(4_096, 16),
    )
    .expect("fixture snapshot should be valid");
    assert_eq!(
        catalog.commit_refresh(
            id,
            ratash::profile::RefreshContext {
                profile_revision: expected,
                active_revision: None,
            },
            stale,
            5_000,
            6_000
        ),
        Err(RefreshCommitError::StaleRevision {
            expected,
            actual: ProfileRevision(2)
        })
    );
    assert_eq!(
        catalog
            .get(id)
            .expect("profile should exist")
            .snapshot
            .rule_strings(),
        ["MATCH,REFRESHED"]
    );
}

#[test]
fn active_revision_rejects_an_a_to_b_to_a_refresh_race() {
    let a = ProfileId::new();
    let b = ProfileId::new();
    let mut catalog = ProfileCatalog::new();
    catalog
        .insert(profile(a, "A", "a"))
        .expect("profile A should insert");
    catalog
        .insert(profile(b, "B", "b"))
        .expect("profile B should insert");
    catalog.activate(&a.to_string()).expect("A should activate");
    let context = catalog
        .refresh_context(a)
        .expect("active refresh context should be captured");
    assert_eq!(context.active_revision, Some(ActiveProfileRevision(1)));
    catalog.activate(&b.to_string()).expect("B should activate");
    catalog
        .activate(&a.to_string())
        .expect("A should reactivate");
    let candidate = ProfileSnapshot::parse(
        b"proxies: []\nrules:\n  - MATCH,CANDIDATE\n",
        SnapshotLimits::new(4_096, 16),
    )
    .expect("candidate should be valid");

    assert_eq!(
        catalog.commit_refresh(a, context, candidate, 3_000, 4_000),
        Err(RefreshCommitError::ActiveRevisionChanged {
            expected: Some(ActiveProfileRevision(1)),
            actual: ActiveProfileRevision(3),
        })
    );
    assert_eq!(
        catalog
            .get(a)
            .expect("profile A should remain")
            .snapshot
            .rule_strings(),
        ["MATCH,a"]
    );
}

#[test]
fn derived_name_can_be_used_when_creating_a_profile() {
    let id = ProfileId::new();
    let url = SubscriptionUrl::parse("https://profiles.example.test/team.yaml")
        .expect("fixture URL should be valid");
    let name = derive_profile_name(None, &url, id);

    assert_eq!(name.value, "team");
}

#[test]
fn duplicate_profile_id_preserves_the_original_record() {
    let id = ProfileId::new();
    let mut catalog = ProfileCatalog::new();
    catalog
        .insert(profile(id, "Original", "original"))
        .expect("first profile should insert");

    assert_eq!(
        catalog.insert(profile(id, "Replacement", "replacement")),
        Err(ProfileInsertError::DuplicateId(id))
    );
    assert_eq!(
        catalog.get(id).expect("profile should remain").name,
        "Original"
    );
}

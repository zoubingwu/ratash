use hopash::domain::{ProfileId, SubscriptionUrl};
use hopash::profile::{
    Profile, ProfileNameSource, ProfileSnapshot, SnapshotError, SnapshotLimits, derive_profile_name,
};

#[test]
fn snapshot_preserves_validated_raw_yaml_and_complete_rules() {
    let raw = b"proxies: []\nrules:\n  - DOMAIN,example.com,DIRECT\n  - AND,((DOMAIN,a.example),(NETWORK,TCP)),REJECT\n";

    let snapshot = ProfileSnapshot::parse(raw, SnapshotLimits::new(4_096, 16))
        .expect("fixture should be valid");

    assert_eq!(snapshot.raw(), raw);
    assert_eq!(
        snapshot.rule_strings(),
        [
            "DOMAIN,example.com,DIRECT",
            "AND,((DOMAIN,a.example),(NETWORK,TCP)),REJECT"
        ]
    );
    assert_eq!(snapshot.content_sha256().len(), 64);
}

#[test]
fn snapshot_enforces_body_depth_top_level_and_rule_shape_limits() {
    assert_eq!(
        ProfileSnapshot::parse(b"proxies: []\n", SnapshotLimits::new(4, 16)),
        Err(SnapshotError::BodyTooLarge { limit: 4 })
    );
    assert_eq!(
        ProfileSnapshot::parse(b"- item\n", SnapshotLimits::new(4_096, 16)),
        Err(SnapshotError::TopLevelMappingRequired)
    );
    assert_eq!(
        ProfileSnapshot::parse(b"rules:\n  key: value\n", SnapshotLimits::new(4_096, 16)),
        Err(SnapshotError::RulesSequenceRequired)
    );
    assert_eq!(
        ProfileSnapshot::parse(
            b"rules:\n  - DOMAIN,example.com,DIRECT\n  - 42\n",
            SnapshotLimits::new(4_096, 16)
        ),
        Err(SnapshotError::RuleStringRequired { index: 1 })
    );
    assert_eq!(
        ProfileSnapshot::parse(
            b"outer:\n  inner:\n    value: true\n",
            SnapshotLimits::new(4_096, 2)
        ),
        Err(SnapshotError::DepthExceeded { limit: 2 })
    );
}

#[test]
fn profile_name_uses_metadata_then_filename_then_host_and_short_id() {
    let id = ProfileId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8")
        .expect("fixture ID should be valid");
    let url = SubscriptionUrl::parse("https://profiles.example.test/team.yaml?token=secret")
        .expect("fixture URL should be valid");

    let metadata = derive_profile_name(Some("  Primary Team  "), &url, id);
    assert_eq!(metadata.value, "Primary Team");
    assert_eq!(metadata.source, ProfileNameSource::Metadata);

    let filename = derive_profile_name(None, &url, id);
    assert_eq!(filename.value, "team");
    assert_eq!(filename.source, ProfileNameSource::Filename);

    let host_url = SubscriptionUrl::parse("https://profiles.example.test/")
        .expect("fixture URL should be valid");
    let fallback = derive_profile_name(None, &host_url, id);
    assert_eq!(fallback.value, "profiles.example.test-67e55044");
    assert_eq!(fallback.source, ProfileNameSource::HostAndShortId);

    let sensitive_filename = SubscriptionUrl::parse(
        "https://profiles.example.test/0123456789abcdef0123456789abcdef.yaml",
    )
    .expect("fixture URL should be valid");
    let sensitive_fallback = derive_profile_name(None, &sensitive_filename, id);
    assert_eq!(sensitive_fallback.value, "profiles.example.test-67e55044");
    assert_eq!(sensitive_fallback.source, ProfileNameSource::HostAndShortId);
}

#[test]
fn snapshot_and_profile_debug_output_exposes_only_safe_metadata() {
    let raw = b"proxy-providers:\n  private:\n    type: http\n    url: https://alice:password@example.test/token-value\nrules: []\nsecret: core-secret\n";
    let snapshot = ProfileSnapshot::parse(raw, SnapshotLimits::new(4_096, 16))
        .expect("fixture should be valid");

    let profile = Profile::new(
        ProfileId::parse("67e55044-10b1-426f-9247-bb680e5fe0c8")
            .expect("fixture ID should be valid"),
        "Private Profile".to_owned(),
        SubscriptionUrl::parse(
            "https://alice:password@example.test/token-value.yaml?credential=core-secret",
        )
        .expect("fixture URL should be valid"),
        snapshot.clone(),
        1,
        2,
    );
    let debug = format!("{snapshot:?} {profile:?}");

    assert!(debug.contains(snapshot.content_sha256()));
    assert!(debug.contains("raw_bytes"));
    for secret in ["alice", "password", "token-value", "core-secret"] {
        assert!(!debug.contains(secret), "{secret} leaked in {debug}");
    }
}

use hopash::domain::{NodeRecordId, ProfileId, ProxyGroupId, SubscriptionUrl};

#[test]
fn subscription_url_accepts_http_and_https_at_the_domain_boundary() {
    for value in [
        "http://example.test/profile.yaml",
        "https://user:secret@example.test/access-token?token=query-secret",
    ] {
        let url = SubscriptionUrl::parse(value).expect("HTTP(S) URL should be accepted");

        assert_eq!(url.expose().as_str(), value);
        assert_eq!(format!("{url:?}"), "SubscriptionUrl([REDACTED])");
    }

    assert!(SubscriptionUrl::parse("file:///tmp/profile.yaml").is_err());
    assert!(SubscriptionUrl::parse("https://?token=secret").is_err());
    let oversized = format!(
        "https://example.test/{}",
        "a".repeat(hopash::constants::SUBSCRIPTION_URL_MAX_BYTES)
    );
    assert!(SubscriptionUrl::parse(&oversized).is_err());
}

#[test]
fn subscription_url_redaction_removes_credentials_and_sensitive_values() {
    let url = SubscriptionUrl::parse(
        "https://alice:password@profiles.example.test/api/access-token-value/profile.yaml?token=query-secret&client=private-value#fragment",
    )
    .expect("fixture URL should be valid");

    let redacted = url.redacted();

    assert!(redacted.starts_with("https://profiles.example.test/"));
    assert!(redacted.contains("[redacted]"));
    for secret in [
        "alice",
        "password",
        "access-token-value",
        "query-secret",
        "private-value",
        "fragment",
    ] {
        assert!(!redacted.contains(secret), "{secret} leaked in {redacted}");
    }
}

#[test]
fn subscription_url_redaction_covers_encoded_jwt_base64_and_token_filenames() {
    for value in [
        "https://profiles.example.test/eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signaturevalue/profile.yaml",
        "https://profiles.example.test/QWxhZGRpbjpvcGVuIHNlc2FtZQ==/profile.yaml",
        "https://profiles.example.test/%61ccess-%74oken-value/profile.yaml",
        "https://profiles.example.test/0123456789abcdef0123456789abcdef.yaml",
    ] {
        let url = SubscriptionUrl::parse(value).expect("fixture URL should be valid");
        let redacted = url.redacted();

        assert!(redacted.contains("[redacted]"), "{redacted}");
        assert!(!redacted.contains("eyJhbGci"), "{redacted}");
        assert!(!redacted.contains("QWxhZGRp"), "{redacted}");
        assert!(!redacted.contains("%61ccess"), "{redacted}");
        assert!(!redacted.contains("0123456789abcdef"), "{redacted}");
    }
}

#[test]
fn subscription_url_redaction_hides_token_shaped_query_keys() {
    for value in [
        "https://profiles.example.test/profile.yaml?0123456789abcdef0123456789abcdef",
        "https://profiles.example.test/profile.yaml?access-token-value=unused",
    ] {
        let url = SubscriptionUrl::parse(value).expect("fixture URL should be valid");
        let redacted = url.redacted();

        assert!(redacted.contains("[redacted]=[redacted]"), "{redacted}");
        assert!(!redacted.contains("0123456789abcdef"), "{redacted}");
        assert!(!redacted.contains("access-token-value"), "{redacted}");
    }
}

#[test]
fn node_record_ids_are_deterministic_and_source_aware() {
    let core = NodeRecordId::for_core("Shared Node");
    let provider_a = NodeRecordId::for_provider("Provider A", "Shared Node");
    let provider_b = NodeRecordId::for_provider("Provider B", "Shared Node");

    assert_eq!(core, NodeRecordId::for_core("Shared Node"));
    assert_eq!(
        provider_a,
        NodeRecordId::for_provider("Provider A", "Shared Node")
    );
    assert_ne!(core, provider_a);
    assert_ne!(provider_a, provider_b);
    assert!(core.as_str().starts_with("node_v1_"));
    assert_eq!(core.as_str().len(), "node_v1_".len() + 64);
    assert!(!core.as_str().contains("Shared Node"));
    assert!(!provider_a.as_str().contains("Provider A"));
    assert_eq!(
        NodeRecordId::parse(core.as_str()).expect("generated ID should parse"),
        core
    );
    assert!(NodeRecordId::parse("node_v1_invalid").is_err());
}

#[test]
fn proxy_group_ids_are_stable_opaque_and_round_trip_through_text() {
    let automatic = ProxyGroupId::for_name("Automatic");

    assert_eq!(
        automatic.as_str(),
        "group_v1_d06398df81b2e85f5b7b89ec63befdda5d65ad8ebbd9a9da46eda1d0d61b0b6e"
    );
    assert_eq!(automatic, ProxyGroupId::for_name("Automatic"));
    assert_ne!(automatic, ProxyGroupId::for_name("automatic"));
    assert!(!automatic.as_str().contains("Automatic"));
    assert_eq!(
        ProxyGroupId::parse(automatic.as_str()).expect("generated ID should parse"),
        automatic
    );
    assert!(ProxyGroupId::parse("group_v1_invalid").is_err());
}

#[test]
fn profile_ids_are_opaque_unique_and_round_trip_through_text() {
    let first = ProfileId::new();
    let second = ProfileId::new();

    assert_ne!(first, second);
    assert_eq!(
        ProfileId::parse(&first.to_string()).expect("generated ID should parse"),
        first
    );
    assert!(ProfileId::parse("profile-a").is_err());
}

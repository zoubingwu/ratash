use hopash::domain::{NodeRecordId, SubscriptionUrl};

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
}

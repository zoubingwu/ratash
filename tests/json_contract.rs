use hopash::contract::{ApiError, ErrorCode, JsonEnvelope};

#[test]
fn success_envelope_has_a_versioned_machine_contract() {
    let envelope = JsonEnvelope::success(serde_json::json!({
        "supervisor": "ready",
        "core": "unconfigured"
    }));

    let json = serde_json::to_value(envelope).expect("success envelope should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "schema_version": 1,
            "data": {
                "supervisor": "ready",
                "core": "unconfigured"
            }
        })
    );
}

#[test]
fn error_envelope_has_a_stable_code_and_retry_contract() {
    let envelope = JsonEnvelope::<serde_json::Value>::failure(ApiError::new(
        ErrorCode::SupervisorUnavailable,
        "The Supervisor is unavailable",
        true,
    ));

    let json = serde_json::to_value(envelope).expect("error envelope should serialize");

    assert_eq!(
        json,
        serde_json::json!({
            "schema_version": 1,
            "error": {
                "code": "supervisor_unavailable",
                "message": "The Supervisor is unavailable",
                "retryable": true
            }
        })
    );
}

#[test]
fn error_details_are_present_only_when_the_operation_provides_them() {
    let envelope = JsonEnvelope::<serde_json::Value>::failure(
        ApiError::new(
            ErrorCode::ProfileAmbiguous,
            "Profile name is ambiguous",
            false,
        )
        .with_details(serde_json::json!({
            "candidate_ids": ["profile-a", "profile-b"]
        })),
    );

    let json = serde_json::to_value(envelope).expect("error envelope should serialize");
    assert_eq!(
        json["error"]["details"],
        serde_json::json!({"candidate_ids": ["profile-a", "profile-b"]})
    );
}

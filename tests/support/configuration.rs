use serde_yaml_ng::{Mapping, Value};

pub fn canonicalize_configuration(value: Value) -> Value {
    match value {
        Value::Sequence(values) => {
            Value::Sequence(values.into_iter().map(canonicalize_configuration).collect())
        }
        Value::Mapping(mapping) => {
            let mut entries = mapping.into_iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| key.as_str().unwrap_or_default().to_owned());
            Value::Mapping(Mapping::from_iter(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonicalize_configuration(value))),
            ))
        }
        other => other,
    }
}

pub fn remove_v5_domain_recovery(configuration: &mut Value) {
    configuration
        .as_mapping_mut()
        .expect("the configuration should be a mapping")
        .remove("ipv6");
    let dns = configuration["dns"]
        .as_mapping_mut()
        .expect("DNS should be a mapping");
    for field in [
        "ipv6",
        "enhanced-mode",
        "fake-ip-range",
        "fake-ip-range6",
        "fake-ip-filter-mode",
        "fake-ip-filter",
    ] {
        dns.remove(field);
    }
    configuration
        .as_mapping_mut()
        .expect("the configuration should be a mapping")
        .remove("sniffer");
}

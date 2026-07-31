use hopash::config::{
    AuthoritativeConfig, ConfigCompiler, ConfigError, CoreConfigValidator, CoreValidationError,
    EffectiveConfiguration, ProviderKind, ProviderSection,
};
use hopash::profile::{ProfileSnapshot, SnapshotLimits};
use serde_yaml_ng::Value;
use std::cell::{Cell, RefCell};
use std::path::PathBuf;

fn snapshot(yaml: &str) -> ProfileSnapshot {
    ProfileSnapshot::parse(yaml.as_bytes(), SnapshotLimits::new(128 * 1024, 32))
        .expect("the fixture should be a valid profile snapshot")
}

fn temporary_root(test_name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "hopash-{test_name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("the test clock should be after the Unix epoch")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("the staging root should be created");
    root
}

#[test]
fn representative_profile_compiles_with_authoritative_values_and_provider_paths() {
    let staging_root = temporary_root("representative-config");
    std::fs::create_dir_all(staging_root.join("providers"))
        .expect("the provider directory should be created");
    std::fs::write(staging_root.join("providers/local.yaml"), "payload: []\n")
        .expect("the local provider fixture should be written");
    let snapshot = snapshot(
        r#"
mode: global
allow-lan: true
external-controller-unix: /tmp/untrusted.sock
secret: untrusted
dns:
  enable: true
  listen: 0.0.0.0:53
  nameserver: [1.1.1.1]
tun:
  enable: false
  stack: system
sniffer:
  enable: true
  force-dns-mapping: true
proxies:
  - name: node-a
    type: ss
    server: 127.0.0.1
    port: 443
    cipher: aes-128-gcm
    password: password
proxy-groups:
  - name: Main
    type: select
    proxies: [node-a, DIRECT]
proxy-providers:
  remote:
    type: http
    url: https://example.com/proxies.yaml
    path: ../../untrusted.yaml
    interval: 3600
rule-providers:
  local:
    type: file
    behavior: domain
    format: yaml
    path: providers/local.yaml
rules:
  - MATCH,REJECT
"#,
    );
    let rules = vec![
        "DOMAIN,example.com,DIRECT".to_owned(),
        "MATCH,Main".to_owned(),
    ];
    let authoritative = AuthoritativeConfig::new("/private/tmp/hopash-core.sock", "runtime-secret");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");

    let effective = compiler
        .compile(&snapshot, &rules, &authoritative, &staging_root)
        .expect("the representative profile should compile");
    let document: Value =
        serde_yaml_ng::from_str(effective.yaml()).expect("the result should be valid YAML");

    assert_eq!(effective.core_version(), "v1.19.28");
    assert_eq!(document["mode"].as_str(), Some("rule"));
    assert_eq!(document["allow-lan"].as_bool(), Some(false));
    assert_eq!(document["tun"]["enable"].as_bool(), Some(true));
    assert_eq!(document["dns"].get("listen"), None);
    assert_eq!(
        document["external-controller-unix"].as_str(),
        Some("/private/tmp/hopash-core.sock")
    );
    assert_eq!(document["secret"].as_str(), Some("runtime-secret"));
    assert_eq!(
        document["rules"]
            .as_sequence()
            .expect("rules should be a sequence")
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>(),
        ["DOMAIN,example.com,DIRECT", "MATCH,Main"]
    );

    let providers = effective.providers();
    assert_eq!(providers.len(), 2);
    let remote = providers
        .iter()
        .find(|provider| provider.name == "remote")
        .expect("the remote provider should be classified");
    assert_eq!(remote.section, ProviderSection::Proxy);
    assert!(matches!(remote.kind, ProviderKind::Remote { .. }));
    assert!(remote.relative_path.starts_with("providers/proxy"));
    assert_eq!(
        document["proxy-providers"]["remote"]["path"].as_str(),
        remote.relative_path.to_str()
    );

    let local = providers
        .iter()
        .find(|provider| provider.name == "local")
        .expect("the local provider should be classified");
    assert_eq!(local.section, ProviderSection::Rule);
    assert!(matches!(local.kind, ProviderKind::Local { .. }));
    assert_eq!(local.relative_path, PathBuf::from("providers/local.yaml"));

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn closed_catalog_rejects_unknown_fields_at_supported_boundaries() {
    let staging_root = temporary_root("closed-catalog");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");
    let cases = [
        ("unknown-root: true\n", "unknown-root"),
        ("dns:\n  unknown-dns: true\n", "dns.unknown-dns"),
        ("tun:\n  unknown-tun: true\n", "tun.unknown-tun"),
        (
            "sniffer:\n  unknown-sniffer: true\n",
            "sniffer.unknown-sniffer",
        ),
        (
            "proxy-providers:\n  remote:\n    type: http\n    url: https://example.com/p.yaml\n    unknown-provider: true\n",
            "proxy-providers.remote.unknown-provider",
        ),
        (
            "rule-providers:\n  local:\n    type: file\n    path: local.yaml\n    unknown-provider: true\n",
            "rule-providers.local.unknown-provider",
        ),
        (
            "geox-url:\n  unknown-database: https://example.com/data\n",
            "geox-url.unknown-database",
        ),
        (
            "sniffer:\n  sniff:\n    TLS:\n      ports: ['443']\n      unknown-option: true\n",
            "sniffer.sniff.TLS.unknown-option",
        ),
        (
            "proxies:\n  - {name: node, type: vmess, server: 127.0.0.1, port: 443, uuid: id, ws-opts: {unknown-option: true}}\n",
            "proxies[0].ws-opts.unknown-option",
        ),
    ];

    for (yaml, expected_path) in cases {
        let error = compiler
            .compile(&snapshot(yaml), &[], &authoritative, &staging_root)
            .expect_err("the catalog should reject unknown fields");
        assert_eq!(
            error,
            ConfigError::UnsupportedField {
                path: expected_path.to_owned()
            }
        );
    }

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn catalog_enforces_required_fields_and_concrete_scalar_shapes() {
    let staging_root = temporary_root("catalog-shapes");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");
    let cases = [
        (
            "proxies:\n  - {type: ss, server: 127.0.0.1, port: 443, cipher: aes-128-gcm, password: secret}\n",
            ConfigError::MissingField {
                path: "proxies[0].name".to_owned(),
            },
        ),
        (
            "proxies:\n  - {name: node, type: ss, server: 127.0.0.1, port: 443, password: secret}\n",
            ConfigError::MissingField {
                path: "proxies[0].cipher".to_owned(),
            },
        ),
        (
            "proxy-groups:\n  - {name: Main, proxies: [DIRECT]}\n",
            ConfigError::MissingDiscriminator {
                path: "proxy-groups[0]".to_owned(),
                field: "type".to_owned(),
            },
        ),
        (
            "dns:\n  enable: 'true'\n",
            ConfigError::InvalidShape {
                path: "dns.enable".to_owned(),
                expected: "a boolean",
            },
        ),
        (
            "hosts:\n  example.com: {address: 127.0.0.1}\n",
            ConfigError::InvalidShape {
                path: "hosts.example.com".to_owned(),
                expected: "one of the catalog shapes",
            },
        ),
    ];

    for (yaml, expected) in cases {
        let error = match compiler.compile(&snapshot(yaml), &[], &authoritative, &staging_root) {
            Ok(_) => panic!("the invalid catalog shape should be rejected: {yaml}"),
            Err(error) => error,
        };
        assert_eq!(error, expected, "{yaml}");
    }

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn compiler_validates_rule_shape_policy_targets_and_rule_provider_references() {
    let staging_root = temporary_root("rule-references");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");
    let snapshot = snapshot(
        r#"
proxies:
  - {name: node-a, type: ss, server: 127.0.0.1, port: 443, cipher: aes-128-gcm, password: secret}
proxy-groups:
  - {name: Main, type: select, proxies: [node-a, DIRECT]}
rule-providers:
  domains: {type: inline, behavior: domain, payload: [example.com]}
sub-rules:
  nested: ['DOMAIN,internal.example,DIRECT']
"#,
    );

    compiler
        .compile(
            &snapshot,
            &[
                "DOMAIN,example.com,Main".to_owned(),
                "RULE-SET,domains,node-a".to_owned(),
                "SUB-RULE,((NETWORK,TCP)),nested".to_owned(),
            ],
            &authoritative,
            &staging_root,
        )
        .expect("available references should compile");

    let cases = [
        (
            "DOMAIN,example.com,Missing",
            ConfigError::UnavailableReference {
                path: "rules[0]".to_owned(),
                reference_kind: "Policy Target",
                name: "Missing".to_owned(),
            },
        ),
        (
            "RULE-SET,missing,DIRECT",
            ConfigError::UnavailableReference {
                path: "rules[0]".to_owned(),
                reference_kind: "rule provider",
                name: "missing".to_owned(),
            },
        ),
        (
            "DOMAIN,example.com",
            ConfigError::InvalidRoutingRule {
                path: "rules[0]".to_owned(),
                reason: "DOMAIN rule policy target is required".to_owned(),
            },
        ),
    ];
    for (rule, expected) in cases {
        let error = compiler
            .compile(&snapshot, &[rule.to_owned()], &authoritative, &staging_root)
            .expect_err("the invalid routing rule should be rejected");
        assert_eq!(error, expected, "{rule}");
    }

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn compiler_rejects_duplicate_targets_and_missing_group_members() {
    let staging_root = temporary_root("target-collisions");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");

    let duplicate = compiler
        .compile(
            &snapshot(
                "proxies:\n  - {name: Main, type: ss, server: 127.0.0.1, port: 443, cipher: aes-128-gcm, password: secret}\nproxy-groups:\n  - {name: Main, type: select}\n",
            ),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("duplicate target names should be rejected");
    assert_eq!(
        duplicate,
        ConfigError::DuplicateName {
            path: "proxy-groups[0].name".to_owned(),
            name: "Main".to_owned(),
        }
    );

    let missing_member = compiler
        .compile(
            &snapshot("proxy-groups:\n  - {name: Main, type: select, proxies: [Missing]}\n"),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("missing group members should be rejected");
    assert_eq!(
        missing_member,
        ConfigError::UnavailableReference {
            path: "proxy-groups[0].proxies[0]".to_owned(),
            reference_kind: "proxy or Proxy Group",
            name: "Missing".to_owned(),
        }
    );

    compiler
        .compile(
            &snapshot("proxy-groups:\n  - {name: GLOBAL, type: select, proxies: [DIRECT]}\n"),
            &["MATCH,GLOBAL".to_owned()],
            &authoritative,
            &staging_root,
        )
        .expect("an explicit GLOBAL group should match Mihomo semantics");

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn provider_catalog_applies_fields_by_provider_type() {
    let staging_root = temporary_root("provider-types");
    std::fs::write(staging_root.join("local.yaml"), "payload: []\n")
        .expect("the local provider fixture should be written");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let error = compiler
        .compile(
            &snapshot(
                "proxy-providers:\n  local:\n    type: file\n    path: local.yaml\n    url: https://example.com/unexpected.yaml\n",
            ),
            &[],
            &AuthoritativeConfig::new("/tmp/core.sock", "secret"),
            &staging_root,
        )
        .expect_err("a file provider should reject HTTP-only fields");

    assert_eq!(
        error,
        ConfigError::UnsupportedField {
            path: "proxy-providers.local.url".to_owned()
        }
    );

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn remote_providers_require_http_urls_and_local_providers_require_regular_files() {
    let staging_root = temporary_root("provider-sources");
    std::fs::create_dir(staging_root.join("directory.yaml"))
        .expect("the directory fixture should be created");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");

    for url in ["file:///tmp/provider.yaml", "relative/provider.yaml"] {
        let error = compiler
            .compile(
                &snapshot(&format!(
                    "proxy-providers:\n  remote:\n    type: http\n    url: {url}\n"
                )),
                &[],
                &authoritative,
                &staging_root,
            )
            .expect_err("unsupported provider URLs should be rejected");
        assert_eq!(
            error,
            ConfigError::ProviderUrlInvalid {
                path: "proxy-providers.remote".to_owned(),
            }
        );
    }

    let directory = compiler
        .compile(
            &snapshot("rule-providers:\n  local:\n    type: file\n    path: directory.yaml\n"),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("provider directories should be rejected");
    assert!(matches!(
        directory,
        ConfigError::ProviderFileUnavailable { .. }
    ));

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn compiler_removes_every_profile_owned_inbound_and_control_field() {
    let staging_root = temporary_root("managed-fields");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/owned.sock", "owned-secret");
    let snapshot = snapshot(
        r#"
listeners: [{name: unsafe-listener}]
external-ui: /tmp/ui
external-ui-name: unsafe-ui
external-ui-url: https://example.com/ui.zip
external-controller: 0.0.0.0:9090
external-controller-routing-mark: 123
external-controller-tls: 0.0.0.0:9443
external-controller-unix: /tmp/untrusted.sock
external-controller-pipe: untrusted-pipe
external-controller-cors: {allow-origins: ['*']}
external-doh-server: 0.0.0.0:8853
secret: untrusted-secret
authentication: [user:password]
skip-auth-prefixes: [/unsafe]
bind-address: '*'
inbound-tfo: true
inbound-mptcp: true
ss-config: unsafe
vmess-config: unsafe
tuic-server: {enable: true}
iptables: {enable: true}
lan-allowed-ips: [0.0.0.0/0]
lan-disallowed-ips: [127.0.0.1/32]
port: 8080
socks-port: 1080
redir-port: 7892
tproxy-port: 7893
mixed-port: 7890
tunnels: [unsafe-tunnel]
dns:
  enable: true
  listen: 0.0.0.0:53
"#,
    );

    let effective = compiler
        .compile(&snapshot, &[], &authoritative, &staging_root)
        .expect("managed fields should be replaced or removed");
    let document: Value =
        serde_yaml_ng::from_str(effective.yaml()).expect("the result should be valid YAML");

    for field in [
        "listeners",
        "external-ui",
        "external-ui-name",
        "external-ui-url",
        "external-controller",
        "external-controller-routing-mark",
        "external-controller-tls",
        "external-controller-pipe",
        "external-controller-cors",
        "external-doh-server",
        "authentication",
        "skip-auth-prefixes",
        "bind-address",
        "inbound-tfo",
        "inbound-mptcp",
        "ss-config",
        "vmess-config",
        "tuic-server",
        "iptables",
        "lan-allowed-ips",
        "lan-disallowed-ips",
        "port",
        "socks-port",
        "redir-port",
        "tproxy-port",
        "mixed-port",
        "tunnels",
    ] {
        assert_eq!(document.get(field), None, "{field} should be removed");
    }
    assert_eq!(document["dns"].get("listen"), None);
    assert_eq!(document["external-controller-unix"], "/tmp/owned.sock");
    assert_eq!(document["secret"], "owned-secret");

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn compiler_requires_private_endpoint_inputs_and_a_directory_staging_root() {
    let staging_root = temporary_root("authoritative-inputs");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let snapshot = snapshot("{}\n");

    for (authoritative, field) in [
        (
            AuthoritativeConfig::new("relative.sock", "secret"),
            "external-controller-unix",
        ),
        (AuthoritativeConfig::new("/tmp/core.sock", ""), "secret"),
    ] {
        assert_eq!(
            compiler
                .compile(&snapshot, &[], &authoritative, &staging_root)
                .expect_err("invalid authoritative input should be rejected"),
            ConfigError::InvalidAuthoritativeValue { field }
        );
    }

    let file_root = staging_root.join("file-root");
    std::fs::write(&file_root, "fixture").expect("the file root should be written");
    assert!(matches!(
        compiler.compile(
            &snapshot,
            &[],
            &AuthoritativeConfig::new("/tmp/core.sock", "secret"),
            &file_root,
        ),
        Err(ConfigError::StagingRootUnavailable { .. })
    ));

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn local_provider_paths_reject_traversal_external_absolute_paths_and_symlink_escape() {
    let staging_root = temporary_root("provider-paths");
    let outside_root = temporary_root("provider-paths-outside");
    let outside_file = outside_root.join("outside.yaml");
    std::fs::write(&outside_file, "payload: []\n").expect("the outside fixture should be written");
    std::fs::create_dir_all(staging_root.join("providers"))
        .expect("the provider directory should be created");
    std::os::unix::fs::symlink(&outside_file, staging_root.join("providers/escape.yaml"))
        .expect("the escaping symlink should be created");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");

    let traversal = compiler
        .compile(
            &snapshot("rule-providers:\n  local:\n    type: file\n    path: ../outside.yaml\n"),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("parent traversal should be rejected");
    assert!(matches!(
        traversal,
        ConfigError::ProviderPathTraversal { .. }
    ));

    let external_absolute = compiler
        .compile(
            &snapshot(&format!(
                "rule-providers:\n  local:\n    type: file\n    path: {:?}\n",
                outside_file.to_string_lossy()
            )),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("an external absolute path should be rejected");
    assert!(matches!(
        external_absolute,
        ConfigError::ProviderPathOutsideStagingRoot { .. }
    ));

    let symlink_escape = compiler
        .compile(
            &snapshot(
                "proxy-providers:\n  local:\n    type: file\n    path: providers/escape.yaml\n",
            ),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("a symlink escape should be rejected");
    assert!(matches!(
        symlink_escape,
        ConfigError::ProviderPathOutsideStagingRoot { .. }
    ));

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
    std::fs::remove_dir_all(outside_root).expect("the outside fixture should be removed");
}

#[test]
fn equivalent_documents_produce_identical_yaml_and_policy_sha256() {
    let staging_root = temporary_root("deterministic-config");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "secret");
    let first =
        snapshot("mode: global\ndns:\n  nameserver: [1.1.1.1]\n  enable: true\nallow-lan: true\n");
    let second =
        snapshot("allow-lan: true\ndns:\n  enable: true\n  nameserver: [1.1.1.1]\nmode: global\n");

    let first = compiler
        .compile(&first, &[], &authoritative, &staging_root)
        .expect("the first ordering should compile");
    let second = compiler
        .compile(&second, &[], &authoritative, &staging_root)
        .expect("the second ordering should compile");

    assert_eq!(first.yaml(), second.yaml());
    assert_eq!(
        first.compiler_policy_sha256(),
        second.compiler_policy_sha256()
    );
    assert_eq!(first.compiler_policy_sha256().len(), 64);
    assert!(
        first
            .compiler_policy_sha256()
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    );

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

struct RecordingValidator {
    calls: Cell<usize>,
    yaml: RefCell<Option<String>>,
    root: RefCell<Option<PathBuf>>,
}

impl CoreConfigValidator for RecordingValidator {
    fn validate(
        &self,
        configuration: &EffectiveConfiguration,
        staging_root: &std::path::Path,
    ) -> Result<(), CoreValidationError> {
        self.calls.set(self.calls.get() + 1);
        self.yaml.replace(Some(configuration.yaml().to_owned()));
        self.root.replace(Some(staging_root.to_path_buf()));
        Ok(())
    }
}

#[test]
fn fake_core_validator_receives_compiled_candidate_and_staging_root() {
    let staging_root = temporary_root("core-validator");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let validator = RecordingValidator {
        calls: Cell::new(0),
        yaml: RefCell::new(None),
        root: RefCell::new(None),
    };
    let effective = compiler
        .compile_validated(
            &snapshot("rules: ['MATCH,DIRECT']\n"),
            &["MATCH,DIRECT".to_owned()],
            &AuthoritativeConfig::new("/tmp/core.sock", "secret"),
            &staging_root,
            &validator,
        )
        .expect("the fake validator should accept the configuration");

    assert_eq!(validator.calls.get(), 1);
    assert_eq!(validator.yaml.borrow().as_deref(), Some(effective.yaml()));
    assert_eq!(
        validator.root.borrow().as_deref(),
        Some(
            std::fs::canonicalize(&staging_root)
                .expect("the staging root should canonicalize")
                .as_path()
        )
    );

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

struct RejectingValidator;

impl CoreConfigValidator for RejectingValidator {
    fn validate(
        &self,
        _configuration: &EffectiveConfiguration,
        _staging_root: &std::path::Path,
    ) -> Result<(), CoreValidationError> {
        Err(CoreValidationError::new(
            "candidate contains runtime-secret and proxy-password",
        ))
    }
}

#[test]
fn compiler_propagates_core_validation_failure_with_safe_debug_output() {
    let staging_root = temporary_root("core-validator-error");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let error = compiler
        .compile_validated(
            &snapshot("rules: ['MATCH,DIRECT']\n"),
            &["MATCH,DIRECT".to_owned()],
            &AuthoritativeConfig::new("/tmp/core.sock", "runtime-secret"),
            &staging_root,
            &RejectingValidator,
        )
        .expect_err("the Core validation failure should be propagated");

    assert!(matches!(error, ConfigError::CoreValidationFailed(_)));
    let debug = format!("{error:?} {error}");
    assert!(!debug.contains("runtime-secret"));
    assert!(!debug.contains("proxy-password"));

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

#[test]
fn compiler_types_keep_secrets_and_profile_yaml_out_of_debug_output() {
    let staging_root = temporary_root("config-debug");
    let compiler = ConfigCompiler::bundled().expect("the bundled catalog should load");
    let authoritative = AuthoritativeConfig::new("/tmp/core.sock", "runtime-secret");
    let effective = compiler
        .compile(
            &snapshot(
                "proxies:\n  - name: node\n    type: ss\n    server: 127.0.0.1\n    port: 443\n    cipher: aes-128-gcm\n    password: proxy-password\nproxy-providers:\n  remote:\n    type: http\n    url: https://user:token@example.com/provider.yaml\n",
            ),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect("the sensitive fixture should compile");

    let providers_debug = format!("{:?}", effective.providers());
    let unsupported = compiler
        .compile(
            &snapshot(
                "proxies:\n  - name: node\n    type: profile-token\n    server: 127.0.0.1\n    port: 443\n",
            ),
            &[],
            &authoritative,
            &staging_root,
        )
        .expect_err("the unsupported proxy type should be rejected");
    let debug =
        format!("{authoritative:?} {effective:?} {providers_debug} {unsupported:?} {unsupported}");
    for sensitive in [
        "runtime-secret",
        "proxy-password",
        "user:token",
        "provider.yaml",
        "profile-token",
    ] {
        assert!(
            !debug.contains(sensitive),
            "debug output exposed {sensitive}"
        );
    }

    std::fs::remove_dir_all(staging_root).expect("the staging fixture should be removed");
}

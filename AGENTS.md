# Repository Instructions

## Working Style

- State assumptions and unresolved tradeoffs before implementation.
- Prefer the smallest change that satisfies the request.
- Keep every changed line traceable to the requested behavior.
- Preserve unrelated work and follow the established style.
- Update this file when stable architecture or directory boundaries change.

## Documents and Language

- Use English for committed documentation, source code, code comments, identifiers, UI text, CLI help, errors, tests, and commit messages.
- Use English comments only. When a section comment helps readability, use an ASCII separator such as `// -----------------------------------------------------------------------------`.
- Keep `README.md` user-facing. It contains only the product introduction, current status, installation, and usage.
- Keep stable contributor rules, architecture boundaries, and domain vocabulary in `AGENTS.md`.
- Keep `SPEC.md` and `USER_STORIES.md` as local planning artifacts. They may remain in Chinese and are excluded from Git.
- Never stage, commit, or link local planning artifacts from committed files.
- Keep committed code and documentation self-contained while consulting local planning artifacts when they are present.

## Product and Platform

- The product name is Ratash, the repository is `ratash`, and the executable is `ratash`.
- Ratash is a Rust control-plane wrapper around Mihomo. Mihomo remains the data plane for proxy traffic, rule matching, connections, logs, traffic telemetry, and network delay measurement.
- The MVP targets macOS and uses TUN Capture with Rule Routing Mode.
- Start with one Cargo package and one `ratash` executable. Internal modes may run the Supervisor and the macOS privileged service.
- Use stable Rust, Tokio, Clap, Ratatui, and Crossterm. Keep the dependency set small.
- Keep the first package organized around `cli`, `ipc`, `application`, `domain`, `persistence`, `profile`, `config`, `core`, `scheduler`, `telemetry`, and `tui` modules. Split crates only after a concrete boundary appears.
- Keep Tauri, WebView, and browser frontend dependencies outside the CLI and TUI implementation.

## Architecture Boundaries

- The One-shot CLI and Ratatui Status Interface are foreground clients of the same application use cases over user-local IPC.
- The Supervisor is the sole authoritative owner and writer of Profiles, the Local Rule Set, Effective Configuration, Runtime Apply state, Probe Queue state, and telemetry buffers.
- A zero-Profile Supervisor is ready while the Core state is `unconfigured`. The first valid Profile creates the initial Local Rule Set, Active Profile, Runtime Generation, and Managed Core through one recoverable transaction.
- The macOS privileged service implements the narrow `CoreRuntime` boundary. It authenticates owner sessions, stages verified runtime bundles, performs Core process operations, and forwards process logs. It owns no product or domain state.
- Production Core-service connections bind the owner session to the kernel-reported UID, PID, and audit token. Signed releases validate the root-owned canonical `/usr/local/bin/ratash` dynamic guest against the packaged Developer ID requirement without network access. Explicit `local-unsigned` builds validate an ad-hoc code identity plus the same canonical path and kernel identity boundary.
- The privileged service independently revalidates application-authoritative controller, secret, TUN, DNS, listener, and provider-path policy before any Core spawn or recovery. Each Runtime Generation transition uses a controlled Core stop and spawn so the generation root remains the Core home. Its macOS TUN capability preflight only opens and closes a `PF_SYSTEM` control socket.
- Route every Core configuration change through `CoreRuntime.apply_candidate`. Keep readiness, proxy queries, node selection, delay checks, bounded active-connection projections, traffic, and logs behind the Mihomo adapter.
- Use separate operating-system locks for Supervisor singleton ownership and cross-process lifecycle operations. Verify the PID, process-start identity, instance token, and Core Instance Generation before controlling a process.
- Serialize every Runtime Generation producer through one Config Transaction Coordinator. Use the lock order `Config Transaction Coordinator -> lifecycle lock` and revalidate captured revisions before apply.
- Persist immutable objects and transaction bundles. Commit through one manifest pointer and journal `Prepared(candidate, previous)` before changing the Core. Recovery converges the Core to the committed pointer.
- Keep Runtime Generation, Core Instance Generation, and Probe Generation as distinct concepts. Carry the appropriate generation or revision through asynchronous work and discard stale results.
- Keep CLI, TUI, persistence, scheduling, HTTP, IPC transport, and Mihomo integration behind explicit application boundaries.

## Current Source Layout

- `src/application.rs` defines transport-independent operations, outputs, errors, and the application service seam.
- `src/background.rs` owns bounded Profile refresh, delay-probe, and generation-scoped Mihomo telemetry workers with wakeable shutdown and reconnect backoff.
- `src/supervisor.rs` coordinates copy-on-write Profile, proxy, latency, rule, refresh, probe, telemetry, cause-scoped Wrapper health reasons and diagnostic transitions, public Core health projection, and bounded list-page projection through injected persistence, Runtime Apply, Profile source, and Core ports.
- `src/supervisor/ports.rs` defines injected Supervisor boundaries; `transactions.rs` owns candidate assembly and transaction execution; `projections.rs` owns transport-independent status and list projection; `errors.rs` owns safe error translation; `outcomes.rs` owns operation result types; `tests.rs` owns focused unit fixtures.
- `src/domain.rs` defines shared lifecycle, identity, status, and validated value types.
- `src/diagnostics.rs` owns safe typed Wrapper diagnostic categories, structured transition records, and bounded tail and gap semantics.
- `src/contract.rs` owns the versioned JSON V1 envelope and explicit status DTO projection.
- `src/ipc.rs` owns the versioned local wire protocol, bounded JSON framing, private Unix socket boundary, and per-subscriber status and log backpressure state.
- `src/ipc_runtime.rs` is the public facade for the live user-local IPC runtime.
- `src/ipc_runtime/client.rs` owns cancellable deadline-bounded request and streaming clients; `client_error.rs` owns safe client error translation; `server.rs` owns same-user authorization and the stoppable bounded Unix socket server; `stream.rs` owns bounded status and Core Log fan-out; `wire.rs` and `wire/status.rs` own private application DTO projections.
- `src/frontend_ipc.rs` adapts status and Core Log streams for foreground clients with a count-and-byte-bounded delivery queue that preserves dropped and gap signals.
- `src/cancellation.rs` owns the shared operation cancellation token and interrupt-registration boundary used to wake blocking foreground work.
- `src/lifecycle.rs` owns state-root discovery, process identities, recoverable directory leases, instance records, and verified stale-socket cleanup.
- `src/profile.rs` owns sanitized Subscription URLs, validated Profile Snapshots, Profile naming, catalog selection, and refresh revision checks.
- `src/profile_source.rs` owns bounded HTTP(S) Profile downloads, redirect policy, metadata extraction, and safe download errors.
- `src/persistence.rs` owns private content-addressed objects, recoverable transaction journals, the committed manifest pointer, and bounded reachability pruning after durable journal cleanup.
- `src/state.rs` stages and hydrates the complete authoritative Supervisor state through immutable objects and the committed transaction pointer.
- `src/transaction.rs` serializes every Runtime Generation producer, revalidates revisions, confirms Core identity and health, and converges failures to the committed pointer.
- `src/config.rs` compiles Profile Snapshots through the bundled configuration policy, owns security-sensitive and authoritative fields including disabled Geo-data auto-update, validates structural fields consumed by Ratash, and exposes both user-side Core validation and privileged authoritative-policy validation seams.
- `src/validator.rs` verifies the pinned Mihomo binary, stages verified bundled Geo data, and runs bounded `-t` validation inside the private staging root without starting the Core.
- `src/geodata.rs` validates the versioned Geo-data manifest and stages verified links to its pinned `ASN.mmdb`, `Country.mmdb`, `GeoIP.dat`, and `GeoSite.dat` assets for Core parsing. Privileged Runtime Generations receive verified regular-file copies.
- `src/mihomo_command.rs` removes Mihomo configuration, lifecycle-hook, controller, secret, and path-safety override environment variables from validation and runtime processes.
- `fixtures/mihomo/v1.19.28/config-policy.yaml` is the compact configuration policy bound to the bundled Core version. It records security-sensitive and authoritative field ownership plus the structural fields Ratash consumes; native Mihomo fields pass through to the pinned `mihomo -t` parser and semantic validator.
- `src/core.rs` defines the authenticated CoreRuntime boundary, Mihomo adapter contract, versioned Proxy View, selection resolution, and fixed API codecs.
- `src/core_service_ipc.rs` implements the versioned privileged CoreRuntime Unix socket protocol, kernel-identity-bound owner sessions, injectable dynamic-peer authorization, and secure Runtime Bundle ingress.
- `src/core_service_ipc/authorization.rs` owns peer identity and authorization; `client.rs` owns the CoreRuntime client; `error.rs` owns shared transport, authorization, and protocol error translation; `ingress.rs` owns secure Runtime Bundle and pinned Geo-data staging; `server.rs` owns dispatch and bounded lifecycle; `socket.rs` owns private socket setup and cleanup; `wire.rs` owns private protocol DTOs.
- `src/daemon.rs` owns lifecycle-operation serialization, Supervisor singleton ownership, detached internal launch, one-time readiness, identity-bound shutdown, and validated stale-state cleanup.
- `src/mihomo.rs` implements bounded authenticated Mihomo REST and WebSocket access over the private Core Unix socket.
- `src/service.rs` owns the injected privileged CoreRuntime state machine, authenticated owner sessions, verified runtime bundles, process identity enforcement, retained cleanup authority for uncommitted Core candidates, count-and-byte-bounded log forwarding, and bounded restart policy.
- `src/service/bundle.rs` owns Runtime Bundle manifests and verification; `error.rs` owns safe platform-to-runtime error mapping; `generation_state.rs` owns durable generation high-water marks and private service directories; `platform.rs` defines injected process, credential, TUN preflight, and secret-generation boundaries.
- `src/process_controller.rs` launches the same-binary Core guardian, uses EOF-first containment until the bounded PID handshake transfers cleanup authority, performs the open-and-close-only macOS TUN capability probe, and implements bounded readiness, controlled stop, and count-and-byte-bounded stdout/stderr capture for the privileged runtime service; its injected constructor retains direct fixture process control.
- `src/core_guardian.rs` owns and reaps one verified Mihomo child, forwards its output after a versioned handshake, and terminates that exact child when the privileged service control pipe closes.
- `src/runtime_bundle.rs` atomically stages private Runtime Generations and binds the Effective Configuration, bundled Mihomo executable, and local provider files to one verified manifest.
- `src/runtime_adapters.rs` confirms pinned Mihomo readiness, classifies uncertain CoreRuntime outcomes, and resolves previously staged Runtime Generations for recovery.
- `fixtures/mihomo/v1.19.28/*.json` are the pinned Core API contract fixtures for projection, readiness, probes, and telemetry.
- `tests/support/configuration.rs` owns shared Effective Configuration canonicalization and legacy-policy fixture helpers used across integration-test crates.
- `fixtures/release/product-contract-v1.json` freezes protocol versions, user-visible timing, capacities, size limits, and process exit codes for the first release contract; `benchmark-metadata-v1.json` freezes the release workload and measurement schema.
- `src/rule.rs` owns Rule String parsing, ordered Local Rule Set mutations, revisions, and deterministic `rules.yaml` serialization.
- `src/scheduler.rs` owns deterministic bounded Profile Refresh and Active Profile Delay Probe scheduling state.
- `src/telemetry.rs` owns generation-scoped latest values, fixed traffic history, bounded active-connection snapshots, the count-and-byte-bounded authoritative Core Log ring, and bounded latest-tail projection.
- `src/tui.rs` is the concise Ratatui facade. `src/tui/state.rs` owns revision-aware view state, bounded projections, command palette state, list viewport and zoom state, count-and-byte-bounded Core Log state, and bounded text input; `src/tui/reducer.rs` owns state transitions, Runtime-Generation-bound Rule loading and mutation, and cancellable command production; `src/tui/input.rs` maps Crossterm keyboard and mouse input through the shared interaction model.
- `src/tui/render.rs` is the pure rendering facade. `src/tui/render/layout.rs` owns compact responsive geometry and interaction projection; `frame.rs` owns the shared three-line status header, navigation, footer, command, Profile, editor, and confirmation sheets; `proxies.rs`, `connections.rs`, `rules.rs`, and `logs.rs` own page rendering. `src/tui/event_inbox.rs` owns fair bounded event scheduling; `src/tui/terminal.rs` owns reversible Crossterm terminal sessions.
- `src/tui_runtime.rs` owns pre-terminal bootstrap, latest-intent mutation dispatch, cancellable foreground waits, bounded snapshot resynchronization, reconnect timing, live status and log intake, the coalesced event loop, signal handling, and the Ratatui/Crossterm runner.
- `src/tui_runtime/command.rs` owns cancellable application command execution and the bounded latest-intent dispatcher; `src/tui_runtime/snapshot.rs` owns full snapshot loading and transport-independent TUI view projection.
- `src/constants.rs` centralizes versioned product intervals, capacities, terminal limits, and input-size boundaries.
- `src/digest.rs` is the internal SHA-256 helper shared by stable identities, immutable storage, and compiler policies.
- `src/cli/command.rs` defines the public Clap command tree and maps parsed commands to typed invocations.
- `src/cli/process.rs` owns process argument errors, JSON usage envelopes, and sensitive argument redaction.
- `src/cli/help.rs` generates Agent Help from the Clap command tree plus the fixed recovery workflow.
- `src/cli/runner.rs` executes typed invocations against an injected application client and owns stdout/stderr formatting.
- `skills/ratash/` is the packaged AI Skill and treats `ratash help agent` as the live command authority.
- `examples/generate-release-assets.rs` derives Bash, Zsh, Fish, and `ratash(1)` assets from the public Clap command tree and verifies committed copies.
- `examples/release-benchmark.rs` is the command facade for the deterministic fixture-backed release workload; `examples/release_benchmark/` separates fixture generation, workload execution, collection orchestration, process sampling, profile serving, reporting, runtime support, and tests. `support.rs` owns leaf helpers, `process_support.rs` owns child-process and PTY guards, and sibling dependencies remain acyclic. The collector digest covers every collector source file.
- `scripts/capture-release-benchmarks-macos.sh`, `scripts/macos-release-resource-probe.sh`, and `packaging/release/benchmark-capture.md` define fixed-runner capture, resource sampling, provenance approval, and the release gate without exercising live network capture.
- `scripts/test-profile-connectivity-docker.sh` runs an explicit local Profile and Mihomo data-plane acceptance test through digest-pinned Alpine containers and disposable Docker networks.
- `packaging/macos/` and `scripts/package-macos.sh` define the signed per-architecture installer payload, pinned Mihomo and Geo-data artifacts, LaunchDaemon, and uninstaller without performing installation during development. `scripts/package-local-macos.sh` builds a personal `local-unsigned` package with the same payload and pinned resources using credential-free ad-hoc code identities. `scripts/validate-pinned-mihomo-geodata.sh` runs parse-only MMDB and DAT acceptance checks through pinned Mihomo without starting the Core data plane.
- `.github/workflows/ci.yml` validates formatting, linting, tests, generated assets, and release-scale bounds; `.github/workflows/release.yml` builds, signs, notarizes, checksums, and publishes both macOS installer targets.
- `src/production.rs` composes privileged-service startup, Supervisor ownership, shutdown coordination, and production adapters; `src/production/foreground.rs` owns the public application client, CLI and TUI runners, lifecycle error projection, shutdown control, and foreground log signal bridge; `src/production/tests.rs` owns focused unit fixtures and lifecycle tests.
- `src/main.rs` remains the thin executable composition root.

## Product Constraints

- Profiles originate from remote HTTP(S) Subscription URLs. A Profile Snapshot is read-only and retains the latest validated content.
- Exactly one Active Profile participates in Effective Configuration composition, Runtime Apply, proxy selection, and Delay Probes.
- An Inactive Profile refresh updates stored state only. An Active Profile refresh commits after a successful Runtime Apply.
- Treat every Profile Snapshot as untrusted input. Apply the bundled configuration policy to security-sensitive, authoritative, and Ratash-consumed structural fields; preserve Core-owned fields and require the pinned `mihomo -t` validator to accept the Effective Configuration.
- Ship immutable, digest-pinned Mihomo Geo data with the installer, link it into Profile validation workspaces, copy it into privileged Runtime Generation roots, and keep Core Geo-data auto-update disabled.
- The Local Rule Set fully replaces the Profile Snapshot's top-level `rules` field.
- Effective Configuration always uses Rule mode, TUN DNS hijacking, dual-stack Fake-IP ranges, and the fixed HTTP/TLS/QUIC domain sniffer. These domain-recovery settings are Wrapper-owned and have no user-facing configuration surface.
- Rule mutations use complete, case-sensitive Rule Strings and the shared configuration transaction path.
- Delay Probes cover the deduplicated Node set of the Active Profile only.
- Expose Core proxies through a versioned projection with source-aware Node identities and explicit missing, ambiguous, and provider-unavailable states.
- Keep queues, channels, buffers, histories, stream subscribers, task concurrency, and retry policies bounded.

## Development Host Safety

- Treat the development host's network state as read-only.
- Keep TUN devices, system proxy settings, DNS settings, routes, firewall rules, privileged-service installation, and live Mihomo traffic capture unchanged during development and verification.
- Exercise CoreRuntime, privileged-service, and Mihomo lifecycle behavior through fakes, fixture subprocesses, temporary directories, and contract tests.
- Use loopback-only HTTP servers and Unix sockets for integration fixtures.
- Keep the Docker Profile connectivity acceptance unprivileged and ephemeral. Place its probe client on an internal-only network, connect Mihomo to a separate disposable egress network, mount the Profile read-only, and keep host networking, published ports, Docker socket mounts, TUN devices, and added capabilities outside the test.
- Reserve real TUN and privileged-service end-to-end validation for a disposable isolated environment outside this development host.

## Domain Vocabulary

Use these terms with their exact meanings and capitalization:

- **Core**: the Mihomo process that handles proxy traffic and runtime telemetry.
- **Wrapper**: all Ratash functionality around the Core.
- **Supervisor**: the background Wrapper process that manages Profiles, the Managed Core, and background work.
- **Managed Core**: the Core instance logically owned by the Supervisor.
- **Profile**: one remote subscription and its latest validated Profile Snapshot.
- **Profile Snapshot**: validated, read-only YAML downloaded from a Subscription URL.
- **Profile Refresh**: the operation that downloads and validates a new Profile Snapshot.
- **Active Profile**: the single Profile used for runtime composition and probing.
- **Effective Configuration**: the validated configuration ready for the Managed Core.
- **Runtime Apply**: the transition that makes an Effective Configuration current.
- **Proxy Group**: a group exposed by the Core. Selector groups allow manual Node changes; automatic groups expose Core-managed selection.
- **Node**: one proxy target selectable through a Proxy Group.
- **Delay Probe**: a Core-executed latency measurement through one Node.
- **Probe Generation**: the probe scheduling scope created by an Active Profile activation.
- **Latency Sample**: one Delay Probe result with its time and Probe Generation.
- **Routing Rule**: one ordered Mihomo matching rule.
- **Rule String**: the complete, case-sensitive Mihomo YAML string for a Routing Rule.
- **Local Rule Set**: the authoritative ordered rules stored locally.
- **Rule Mutation**: one atomic add, replace, or remove operation.
- **Policy Target**: the Proxy Group, Node, or built-in action selected by a rule.
- **One-shot CLI**: one `ratash` command that performs an operation and exits.
- **Status Interface**: the foreground Ratatui interface launched by `ratash status`.
- **Core Log** and **Traffic Sample**: runtime data emitted by the Core.

Keep Core, Wrapper, Supervisor, Profile selection, Node selection, Profile Refresh, Runtime Apply, and Delay Probe separate in code and documentation.

## Rust and TUI Constraints

- Keep domain and application logic independent from Clap, Ratatui, Crossterm, IPC transport, persistence, HTTP, and Mihomo adapters.
- Keep blocking I/O and long-running work outside the TUI event loop.
- Model TUI behavior with typed events, intents, state transitions, and cancellable commands.
- Map keyboard and mouse input to the same intent and application operation.
- Keep rendering free of domain side effects.
- Restore raw mode, alternate screen, mouse capture, focus reporting, bracketed paste, and cursor state after normal exit, errors, signals, and panic.
- Keep unsafe code outside the project unless a documented platform boundary requires it.

## Testing

- Test user-observable behavior at the highest practical seam.
- Use the application service as the primary behavior seam with real domain logic and persistence plus a fake clock, subscription source, Mihomo adapter, and CoreRuntime adapter.
- Keep focused contract suites for IPC framing, CLI JSON and exit codes, Mihomo APIs, and privileged process boundaries.
- Use Ratatui `TestBackend` or `Buffer` for layout and rendering behavior.
- Use a small PTY suite for terminal lifecycle, input, resize, signals, panic cleanup, and shell restoration.
- Test keyboard and mouse parity through shared intents.
- Inject failures around validation, atomic commit, Runtime Apply, and rollback.
- Test scheduler generations, cancellation, bounded concurrency, stale results, and deterministic deadlines with a fake clock.
- Include regression coverage for 100 Profiles, 10,000 Active Nodes, 20,000 Local Rules, sustained Core Log and Traffic Sample input, and long-running TUI resource bounds.
- Run `scripts/test-profile-connectivity-docker.sh <profile.yaml>` explicitly when validating a real Profile against the pinned Mihomo data plane. Keep this credential-bearing network acceptance outside deterministic CI and treat it as separate from Ratash control-plane, privileged-service, and TUN end-to-end coverage.
- Run formatting, Clippy, unit tests, integration tests, and relevant benchmarks before publication.

## Git and GitHub

- Commit completed tracked changes after verification unless the user explicitly requests a working-tree-only change.
- Never stage or commit `SPEC.md` or `USER_STORIES.md`.
- Preserve unrelated dirty work and stage only the requested changes.
- Use Conventional Commit subjects.
- Every commit body must describe the user's original intent and the rationale for the change.
- Pass the subject and body as separate `git commit -m` arguments. Never embed literal `\n` sequences.
- Update branches with fetch and rebase.
- Use neutral repository or business wording for branches and pull-request titles. Exclude `codex`, `[codex]`, and agent-branding labels.
- When using `gh`, rely on the keyring-authenticated account and ignore `GITHUB_TOKEN`.
- When addressing a review comment, reply to the exact thread and resolve it after the fix or explanation is complete.

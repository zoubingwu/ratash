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

- The product name is Hopash RS, the repository is `hopash-rs`, and the executable is `hopash`.
- Hopash RS is a Rust control-plane wrapper around Mihomo. Mihomo remains the data plane for proxy traffic, rule matching, connections, logs, traffic telemetry, and network delay measurement.
- The MVP targets macOS and uses TUN Capture with Rule Routing Mode.
- Start with one Cargo package and one `hopash` executable. Internal modes may run the Supervisor and the macOS privileged service.
- Use stable Rust, Tokio, Clap, Ratatui, and Crossterm. Keep the dependency set small.
- Keep the first package organized around `cli`, `ipc`, `application`, `domain`, `persistence`, `profile`, `config`, `core`, `scheduler`, `telemetry`, and `tui` modules. Split crates only after a concrete boundary appears.
- Keep Tauri, WebView, and browser frontend dependencies outside the CLI and TUI implementation.

## Architecture Boundaries

- The One-shot CLI and Ratatui Status Interface are foreground clients of the same application use cases over user-local IPC.
- The Supervisor is the sole authoritative owner and writer of Profiles, the Local Rule Set, Effective Configuration, Runtime Apply state, Probe Queue state, and telemetry buffers.
- A zero-Profile Supervisor is ready while the Core state is `unconfigured`. The first valid Profile creates the initial Local Rule Set, Active Profile, Runtime Generation, and Managed Core through one recoverable transaction.
- The macOS privileged service implements the narrow `CoreRuntime` boundary. It authenticates owner sessions, stages verified runtime bundles, performs Core process operations, and forwards process logs. It owns no product or domain state.
- Route every Core configuration change through `CoreRuntime.apply_candidate`. Keep readiness, proxy queries, node selection, delay checks, connection summaries, traffic, and logs behind the Mihomo adapter.
- Use separate operating-system locks for Supervisor singleton ownership and cross-process lifecycle operations. Verify the PID, process-start identity, instance token, and Core Instance Generation before controlling a process.
- Serialize every Runtime Generation producer through one Config Transaction Coordinator. Use the lock order `Config Transaction Coordinator -> lifecycle lock` and revalidate captured revisions before apply.
- Persist immutable objects and transaction bundles. Commit through one manifest pointer and journal `Prepared(candidate, previous)` before changing the Core. Recovery converges the Core to the committed pointer.
- Keep Runtime Generation, Core Instance Generation, and Probe Generation as distinct concepts. Carry the appropriate generation or revision through asynchronous work and discard stale results.
- Keep CLI, TUI, persistence, scheduling, HTTP, IPC transport, and Mihomo integration behind explicit application boundaries.

## Current Source Layout

- `src/application.rs` defines transport-independent operations, outputs, errors, and the application service seam.
- `src/background.rs` owns bounded Profile refresh, delay-probe, and generation-scoped Mihomo telemetry workers with wakeable shutdown and reconnect backoff.
- `src/supervisor.rs` coordinates copy-on-write Profile, proxy, latency, rule, refresh, probe, and telemetry use cases through injected persistence, Runtime Apply, Profile source, and Core ports.
- `src/domain.rs` defines shared lifecycle, identity, status, and validated value types.
- `src/contract.rs` owns the versioned JSON V1 envelope and explicit status DTO projection.
- `src/ipc.rs` owns the versioned local wire protocol, bounded JSON framing, private Unix socket boundary, and per-subscriber status and log backpressure state.
- `src/ipc_runtime.rs` implements the deadline-bounded one-shot IPC client, same-user peer authorization, and the stoppable bounded Unix socket server.
- `src/lifecycle.rs` owns state-root discovery, process identities, recoverable directory leases, instance records, and verified stale-socket cleanup.
- `src/profile.rs` owns sanitized Subscription URLs, validated Profile Snapshots, Profile naming, catalog selection, and refresh revision checks.
- `src/profile_source.rs` owns bounded HTTP(S) Profile downloads, redirect policy, metadata extraction, and safe download errors.
- `src/persistence.rs` owns private content-addressed objects, recoverable transaction journals, and the committed manifest pointer.
- `src/state.rs` stages and hydrates the complete authoritative Supervisor state through immutable objects and the committed transaction pointer.
- `src/transaction.rs` serializes every Runtime Generation producer, revalidates revisions, confirms Core identity and health, and converges failures to the committed pointer.
- `src/config.rs` compiles Profile Snapshots through the bundled Mihomo field catalog, applies authoritative fields, and exposes the final Core validation seam.
- `src/validator.rs` verifies the pinned Mihomo binary and runs bounded `-t` validation inside the private staging root without starting the Core.
- `fixtures/mihomo/v1.19.28/config-schema.yaml` is the closed field catalog bound to the bundled Core version.
- `src/core.rs` defines the authenticated CoreRuntime boundary, Mihomo adapter contract, versioned Proxy View, selection resolution, and fixed API codecs.
- `src/mihomo.rs` implements bounded authenticated Mihomo REST and WebSocket access over the private Core Unix socket.
- `src/service.rs` owns the injected privileged CoreRuntime state machine, authenticated owner sessions, verified runtime bundles, process identity enforcement, bounded log forwarding, and bounded restart policy.
- `src/process_controller.rs` implements verified Mihomo process spawn, bounded readiness and reload control, identity-matched stop, and bounded stdout/stderr capture for the privileged runtime service.
- `src/runtime_bundle.rs` atomically stages private Runtime Generations and binds the Effective Configuration, bundled Mihomo executable, and local provider files to one verified manifest.
- `fixtures/mihomo/v1.19.28/*.json` are the pinned Core API contract fixtures for projection, readiness, probes, and telemetry.
- `fixtures/release/product-contract-v1.json` freezes protocol versions, user-visible timing, capacities, size limits, and process exit codes for the first release contract.
- `src/rule.rs` owns Rule String parsing, ordered Local Rule Set mutations, revisions, and deterministic `rules.yaml` serialization.
- `src/scheduler.rs` owns deterministic bounded Profile Refresh and Active Profile Delay Probe scheduling state.
- `src/telemetry.rs` owns generation-scoped latest values, fixed traffic history, and the bounded Core Log ring.
- `src/tui.rs` owns the Ratatui view model, reducer, input mapping, pure rendering, fair event inbox, and reversible Crossterm terminal session.
- `src/tui_runtime.rs` owns pre-terminal bootstrap, bounded background command dispatch, reconnect timing, live status and log intake, the coalesced event loop, signal handling, and the Ratatui/Crossterm runner.
- `src/constants.rs` centralizes versioned product intervals, capacities, terminal limits, and input-size boundaries.
- `src/digest.rs` is the internal SHA-256 helper shared by stable identities, immutable storage, and compiler policies.
- `src/cli/command.rs` defines the public Clap command tree and maps parsed commands to typed invocations.
- `src/cli/process.rs` owns process argument errors, JSON usage envelopes, and sensitive argument redaction.
- `src/cli/help.rs` generates Agent Help from the Clap command tree plus the fixed recovery workflow.
- `src/cli/runner.rs` executes typed invocations against an injected application client and owns stdout/stderr formatting.
- `skills/hopash/` is the packaged AI Skill and treats `hopash help agent` as the live command authority.
- `src/main.rs` remains the thin executable composition root.

## Product Constraints

- Profiles originate from remote HTTP(S) Subscription URLs. A Profile Snapshot is read-only and retains the latest validated content.
- Exactly one Active Profile participates in Effective Configuration composition, Runtime Apply, proxy selection, and Delay Probes.
- An Inactive Profile refresh updates stored state only. An Active Profile refresh commits after a successful Runtime Apply.
- Treat every Profile Snapshot as untrusted input. Compile it against the field catalog for the bundled Mihomo version, reject unknown fields, remove inbound and external-control fields, and set application-owned values explicitly.
- The Local Rule Set fully replaces the Profile Snapshot's top-level `rules` field.
- Rule mutations use complete, case-sensitive Rule Strings and the shared configuration transaction path.
- Delay Probes cover the deduplicated Node set of the Active Profile only.
- Expose Core proxies through a versioned projection with source-aware Node identities and explicit missing, ambiguous, and provider-unavailable states.
- Keep queues, channels, buffers, histories, stream subscribers, task concurrency, and retry policies bounded.

## Development Host Safety

- Treat the development host's network state as read-only.
- Keep TUN devices, system proxy settings, DNS settings, routes, firewall rules, privileged-service installation, and live Mihomo traffic capture unchanged during development and verification.
- Exercise CoreRuntime, privileged-service, and Mihomo lifecycle behavior through fakes, fixture subprocesses, temporary directories, and contract tests.
- Use loopback-only HTTP servers and Unix sockets for integration fixtures.
- Reserve real TUN and privileged-service end-to-end validation for a disposable isolated environment outside this development host.

## Domain Vocabulary

Use these terms with their exact meanings and capitalization:

- **Core**: the Mihomo process that handles proxy traffic and runtime telemetry.
- **Wrapper**: all Hopash RS functionality around the Core.
- **Supervisor**: the background Wrapper process that manages Profiles, the Managed Core, and background work.
- **Managed Core**: the Core instance logically owned by the Supervisor.
- **Profile**: one remote subscription and its latest validated Profile Snapshot.
- **Profile Snapshot**: validated, read-only YAML downloaded from a Subscription URL.
- **Profile Refresh**: the operation that downloads and validates a new Profile Snapshot.
- **Active Profile**: the single Profile used for runtime composition and probing.
- **Effective Configuration**: the validated configuration ready for the Managed Core.
- **Runtime Apply**: the transition that makes an Effective Configuration current.
- **Proxy Group**: a selectable group exposed by the Core.
- **Node**: one proxy target selectable through a Proxy Group.
- **Delay Probe**: a Core-executed latency measurement through one Node.
- **Probe Generation**: the probe scheduling scope created by an Active Profile activation.
- **Latency Sample**: one Delay Probe result with its time and Probe Generation.
- **Routing Rule**: one ordered Mihomo matching rule.
- **Rule String**: the complete, case-sensitive Mihomo YAML string for a Routing Rule.
- **Local Rule Set**: the authoritative ordered rules stored locally.
- **Rule Mutation**: one atomic add, replace, or remove operation.
- **Policy Target**: the Proxy Group, Node, or built-in action selected by a rule.
- **One-shot CLI**: one `hopash` command that performs an operation and exits.
- **Status Interface**: the foreground Ratatui interface launched by `hopash status`.
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

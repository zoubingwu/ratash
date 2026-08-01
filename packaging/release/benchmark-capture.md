# Release Benchmark Capture

Hopash RS uses one deterministic workload generator and one versioned metadata validator for resource release approval. Collection runs on the dedicated macOS 15 arm64 release runner named in `fixtures/release/benchmark-metadata-v1.json`. Approval freezes its hardware model, CPU model, logical CPU count, memory size, and operating-system version. The development host remains network-read-only throughout this workflow.

## Workload

Generate the complete release workload in a new directory:

```sh
cargo run --locked --release --example release-benchmark -- \
  generate "$RUNNER_TEMP/hopash-release-workload"
```

The generator writes 100 Profile records, 10,000 Active Node records, 20,000 Local Rules, 30 minutes of deterministic Core Log and Traffic Sample input, and 30 minutes of deterministic Status Interface render ticks. `workload-manifest-v1.json` records the generator version, seed, record counts, byte sizes, and SHA-256 digest of every artifact.

## Capture

Run the versioned fixed-runner collector in a new output directory:

```sh
scripts/capture-release-benchmarks-macos.sh \
  "$RUNNER_TEMP/hopash-release-benchmark"
```

The script fails before creating output unless the Git index and working tree are clean, including untracked files. It then runs two full warm-ups and ten full samples. Every sample contains the complete 21-key measurement set, workload-manifest digest, release executable digest, safe fixture executable digest, resource-probe digest, architecture-neutral Rust and Cargo version identities, runner profile, source-tree identity, and five duration-checked RSS curves. Aggregation rejects smoke samples and any sample whose collector, environment, executable inputs, workload, duration, or measurement schema differs from the other samples. The shell forwards `SIGHUP`, `SIGINT`, and `SIGTERM` to the active collector. The collector converts those signals into bounded cancellation and synchronously reaps its resource probes, Status Interface PTY, fixture Supervisor, privileged service, guardian, and Core fixture before returning.

The Rust collector drives these product seams:

- The normal optimized `hopash` binary supplies executable size and One-shot CLI cold start.
- The benchmark executable starts `PrivilegedCoreRuntimeService` behind the real `CoreServiceServer` protocol with injected TUN capability and process dependencies. A loopback-only subscription feeds the generated 10,000-Node and 20,000-Rule Profile into the production configuration compiler, persistence transaction, Runtime Apply, `NativeCoreProcessController`, real guardian, harmless Unix-socket Core fixture, readiness adapter, and background Probe Queue. The Core fixture uses a fixed worker pool and deterministic finite Delay Probe latency so health requests exercise bounded concurrency at realistic request rates.
- The normal release executable drives all foreground Profile, Proxy, Rule, status, and TUI operations through the production IPC and application boundaries. The collector creates 100 Profiles, verifies the complete paged Profile, Proxy, and Rule projections, waits for all Active Nodes to enter the background Probe Queue, and times a persisted 20,000-Rule Runtime Apply mutation.
- The collector retries only public errors that carry both the expected transient code and `retryable: true`, using a fixed deadline. It waits for the post-apply Proxy View before launching the Status Interface and fails immediately on every other application error.
- An optimized fixture Supervisor build with debug assertions supplies only the temporary Core-service socket override. This preserves the production privileged endpoint boundary in the shipped executable. The safe service and Core fixture remain the declared residual for process-level Supervisor and privileged-service measurements.
- The normal release `hopash status` process runs inside a PTY for cold-start, idle RSS, and 30-minute peak-memory capture. A bounded child guard terminates and reaps failed or stalled PTY runs.
- Supplemental component measurements use `ProbeScheduler`, `LocalRuleSet`, `TelemetryStore`, and Ratatui `render_buffer` for isolated queue, parse, filter, sustained telemetry, and render costs.

`scripts/macos-release-resource-probe.sh` is the system measurement seam. It reads RSS and CPU through `ps` and reads per-task interrupt wakeups through Apple `powermetrics`. The dedicated runner grants passwordless access only to the exact `powermetrics` invocation used by this script. Collection uses harmless fixture executables, temporary directories, private Unix sockets, and PTYs. It leaves TUN devices, system proxy settings, DNS, routes, firewall rules, privileged-service installation, and live traffic capture unchanged.

`benchmark-report-v1.json` records the runner, frozen hardware profile, operating system, architecture-neutral Rust and Cargo version identities, Git revision, source-tree digest, `Cargo.lock` digest, collector digest, executable input digests, capture time, generator version, workload scale, workload-manifest digest, sample count, all 21 medians, and each raw sample's digest plus measurement projection. Each raw sample file retains Supervisor, privileged-service, combined background, telemetry, and TUI RSS curves. The report has `review_required` status.

Release approval is a deliberate maintainer action: review every raw sample and curve, copy the medians into `measurements`, preserve the accepted reference values in `baseline_measurements`, set a maximum `thresholds` and `regression_budgets_percent` value for every key, embed the exact report as `approved_capture.reviewed_report`, record the SHA-256 digest of its pretty JSON file as `approved_capture.reviewed_report_sha256`, copy the report environment and workload-manifest digest into `approved_capture`, and then set the committed metadata status to `approved`.

The approval validator recomputes the embedded reviewed-report digest, raw-sample digest set, medians from the embedded raw measurement projections, and exact approved median projection. It also recomputes the source-tree identity while excluding only the approval metadata file, verifies `Cargo.lock`, collector source, architecture-neutral compiler and Cargo versions, and a freshly regenerated deterministic workload manifest. The release workflow validates the committed median projection on both architectures, then validates each derived target record after replacing only `wrapper_binary_bytes` with that target's shipped executable size.

## Validation

CI runs a bounded generator smoke scenario and validates the current metadata shape:

```sh
cargo build --locked --release --bin hopash --example release-benchmark
CARGO_TARGET_DIR="$RUNNER_TEMP/hopash-benchmark-fixture-target" \
RUSTFLAGS='-C debug-assertions=yes' \
  cargo build --locked --release --bin hopash
target/release/examples/release-benchmark smoke \
  fixtures/release/benchmark-metadata-v1.json \
  "$PWD/target/release/hopash" \
  "$RUNNER_TEMP/hopash-benchmark-fixture-target/release/hopash"
```

The release gate requires explicit approval and enforces every threshold and baseline-relative regression budget:

```sh
cargo run --locked --release --example release-benchmark -- \
  validate fixtures/release/benchmark-metadata-v1.json --require-approved
```

`capture_required` metadata keeps measurements, baselines, thresholds, regression budgets, and approval provenance as null values. This state records the open fixed-runner acceptance gate without inventing measurements.

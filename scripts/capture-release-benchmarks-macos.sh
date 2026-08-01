#!/bin/sh

set -eu

fail() {
  echo "$1" >&2
  exit 1
}

test "$(uname -s)" = "Darwin" || fail "Release benchmark capture requires macOS."
test "$#" -eq 1 || fail "Usage: capture-release-benchmarks-macos.sh <new-output-directory>"

invocation_directory=$(pwd)
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_root"
test -z "$(git status --porcelain --untracked-files=all)" || \
  fail "Release benchmark capture requires a clean Git working tree and index."

output=$1
case "$output" in
  /*) ;;
  *) output=$invocation_directory/$output ;;
esac
test ! -e "$output" || fail "The benchmark output directory already exists."
/bin/mkdir -p "$output/samples"

temporary=$(mktemp -d "${TMPDIR:-/tmp}/hopash-release-capture.XXXXXX")
active_collector=

stop_active_collector() {
  if test -n "$active_collector"; then
    kill -TERM "$active_collector" 2>/dev/null || true
    wait "$active_collector" 2>/dev/null || true
    active_collector=
  fi
}

cleanup_and_exit() {
  status=$?
  trap - 0 HUP INT TERM
  stop_active_collector
  rm -rf "$temporary"
  exit "$status"
}

interrupt_and_exit() {
  status=$1
  trap - 0 HUP INT TERM
  stop_active_collector
  rm -rf "$temporary"
  exit "$status"
}

run_collector() {
  "$collector" "$@" &
  active_collector=$!
  if wait "$active_collector"; then
    status=0
  else
    status=$?
  fi
  active_collector=
  return "$status"
}

trap cleanup_and_exit 0
trap 'interrupt_and_exit 129' HUP
trap 'interrupt_and_exit 130' INT
trap 'interrupt_and_exit 143' TERM

cargo build --locked --release --bin hopash --example release-benchmark
release_binary="$project_root/target/release/hopash"
collector="$project_root/target/release/examples/release-benchmark"
resource_probe="$project_root/scripts/macos-release-resource-probe.sh"

CARGO_TARGET_DIR="$temporary/fixture-target" \
RUSTFLAGS='-C debug-assertions=yes' \
  cargo build --locked --release --bin hopash
fixture_binary="$temporary/fixture-target/release/hopash"

run_collector generate "$output/workload"

warmup=1
while test "$warmup" -le 2; do
  run_collector collect \
    "$project_root/fixtures/release/benchmark-metadata-v1.json" \
    "$output/workload/workload-manifest-v1.json" \
    "$release_binary" \
    "$fixture_binary" \
    "$resource_probe" \
    "$temporary/warmup-$(printf '%02d' "$warmup").json"
  warmup=$((warmup + 1))
done

sample=1
while test "$sample" -le 10; do
  run_collector collect \
    "$project_root/fixtures/release/benchmark-metadata-v1.json" \
    "$output/workload/workload-manifest-v1.json" \
    "$release_binary" \
    "$fixture_binary" \
    "$resource_probe" \
    "$output/samples/sample-$(printf '%02d' "$sample").json"
  sample=$((sample + 1))
done

run_collector capture \
  "$project_root/fixtures/release/benchmark-metadata-v1.json" \
  "$output/workload/workload-manifest-v1.json" \
  "$output/samples" \
  "$output/benchmark-report-v1.json"

echo "$output/benchmark-report-v1.json"

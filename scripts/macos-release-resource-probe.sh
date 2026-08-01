#!/bin/sh

set -eu

export LC_ALL=C

fail() {
  echo "$1" >&2
  exit 1
}

test "$(uname -s)" = "Darwin" || fail "The release resource probe requires macOS."
test "$#" -ge 2 || fail "Usage: macos-release-resource-probe.sh <rss|cpu|wakeups> <arguments...>"

mode=$1
shift

case "$mode" in
  rss)
    test "$#" -eq 1 || fail "RSS mode requires one PID."
    rss_kib=$(/bin/ps -o rss= -p "$1" | /usr/bin/awk '{ print $1 }')
    test -n "$rss_kib" || fail "RSS mode could not observe the process."
    /usr/bin/awk -v kib="$rss_kib" 'BEGIN { printf "%.0f\n", kib * 1024 }'
    ;;
  cpu)
    test "$#" -eq 2 || fail "CPU mode requires a PID and duration in seconds."
    pid=$1
    seconds=$2
    test "$seconds" -gt 0 || fail "CPU duration must be positive."
    sample=0
    total=0
    while test "$sample" -lt "$seconds"; do
      value=$(/bin/ps -o %cpu= -p "$pid" | /usr/bin/awk '{ print $1 }')
      test -n "$value" || fail "CPU mode could not observe the process."
      total=$(/usr/bin/awk -v total="$total" -v value="$value" 'BEGIN { print total + value }')
      sample=$((sample + 1))
      /bin/sleep 1
    done
    /usr/bin/awk -v total="$total" -v samples="$seconds" 'BEGIN { printf "%.6f\n", total / samples }'
    ;;
  wakeups)
    test "$#" -ge 2 || fail "Wakeup mode requires a duration and at least one PID."
    seconds=$1
    shift
    test "$seconds" -gt 0 || fail "Wakeup duration must be positive."
    pid_list=
    for pid in "$@"; do
      case "$pid" in
        *[!0-9]*|'') fail "Wakeup PIDs must be positive integers." ;;
      esac
      if test -z "$pid_list"; then
        pid_list=$pid
      else
        pid_list="$pid_list,$pid"
      fi
    done
    output=$(mktemp "${TMPDIR:-/tmp}/hopash-powermetrics.XXXXXX")
    trap 'rm -f "$output"' EXIT HUP INT TERM
    /usr/bin/sudo -n /usr/bin/powermetrics \
      --samplers tasks \
      --show-process-qos \
      --order pid \
      --sample-rate 1000 \
      --sample-count "$seconds" \
      --output-file "$output"
    /usr/bin/grep -q 'Wakeups (Intr, Pkg idle)' "$output" || fail "powermetrics task schema changed."
    /usr/bin/awk -v pids="$pid_list" -v samples="$seconds" '
      BEGIN {
        count = split(pids, values, ",")
        for (index = 1; index <= count; index++) wanted[values[index]] = 1
      }
      $2 in wanted {
        if ($7 !~ /^[0-9]+([.][0-9]+)?$/) invalid = 1
        seen[$2] = 1
        interrupt_wakeups += $7
      }
      END {
        for (pid in wanted) if (!(pid in seen)) missing = 1
        if (invalid || missing) exit 2
        printf "%.6f\n", interrupt_wakeups / samples
      }
    ' "$output" || fail "powermetrics did not report every requested process with a valid wakeup value."
    ;;
  *)
    fail "Unknown resource probe mode."
    ;;
esac

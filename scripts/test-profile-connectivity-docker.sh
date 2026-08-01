#!/bin/sh

set -eu

MIHOMO_IMAGE='docker.io/metacubex/mihomo@sha256:e6acd921addecfd59a8e2d38203f88356d635b54de6c0673db0e015139989312'
CURL_IMAGE='docker.io/curlimages/curl@sha256:7c12af72ceb38b7432ab85e1a265cff6ae58e06f95539d539b654f2cfa64bb13'
GOOGLE_PROBE_URL='https://www.google.com/generate_204'
CONTROLLER_PORT=19090

fail() {
    echo "$1" >&2
    exit 1
}

test "$#" -eq 1 || fail 'Usage: test-profile-connectivity-docker.sh <profile.yaml>'
command -v docker >/dev/null 2>&1 || fail 'Docker is unavailable.'
docker info >/dev/null 2>&1 || fail 'The Docker daemon is unavailable.'

profile_input=$1
test -f "$profile_input" || fail 'The Profile is unavailable.'
test -r "$profile_input" || fail 'The Profile is unreadable.'
profile_directory=$(CDPATH='' cd -- "$(dirname -- "$profile_input")" && pwd)
profile="$profile_directory/$(basename -- "$profile_input")"

mixed_port=$(
    LC_ALL=C awk '
        /^mixed-port[[:space:]]*:/ {
            value = $0
            sub(/^mixed-port[[:space:]]*:[[:space:]]*/, "", value)
            sub(/[[:space:]]+#.*$/, "", value)
            gsub(/[[:space:]]/, "", value)
            print value
            found = 1
            exit
        }
        END { if (!found) exit 1 }
    ' "$profile"
) || fail 'The Profile must define a top-level mixed-port.'
case "$mixed_port" in
    ''|*[!0-9]*) fail 'The Profile mixed-port must be an integer.' ;;
esac
test "${#mixed_port}" -le 5 || fail 'The Profile mixed-port is outside the valid range.'
test "$mixed_port" -ge 1 && test "$mixed_port" -le 65535 || \
    fail 'The Profile mixed-port is outside the valid range.'
test "$mixed_port" -ne "$CONTROLLER_PORT" || \
    fail 'The Profile mixed-port conflicts with the test controller port.'

ensure_image() {
    image=$1
    if ! docker image inspect "$image" >/dev/null 2>&1; then
        docker pull "$image" >/dev/null || fail "Unable to pull $image."
    fi
}

ensure_image "$MIHOMO_IMAGE"
ensure_image "$CURL_IMAGE"

run_id="$(date +%s)-$$"
core_name="hopash-profile-core-$run_id"
client_network="hopash-profile-client-$run_id"
egress_network="hopash-profile-egress-$run_id"
controller_secret="hopash-profile-controller-$run_id"
test_label='profile-connectivity-test'

remove_test_container() {
    container_label=$(docker inspect \
        --format '{{ index .Config.Labels "io.hopash.purpose" }}' \
        "$core_name" 2>/dev/null || true)
    if test "$container_label" = "$test_label"; then
        docker rm --force --volumes "$core_name" >/dev/null 2>&1 || true
    fi
}

remove_test_network() {
    network_name=$1
    network_label=$(docker network inspect \
        --format '{{ index .Labels "io.hopash.purpose" }}' \
        "$network_name" 2>/dev/null || true)
    if test "$network_label" = "$test_label"; then
        docker network rm "$network_name" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    status=$?
    trap - 0 HUP INT TERM
    remove_test_container
    remove_test_network "$client_network"
    remove_test_network "$egress_network"
    exit "$status"
}

trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

docker network create \
    --internal \
    --label "io.hopash.purpose=$test_label" \
    "$client_network" >/dev/null
docker network create \
    --label "io.hopash.purpose=$test_label" \
    "$egress_network" >/dev/null

docker create \
    --rm \
    --name "$core_name" \
    --label "io.hopash.purpose=$test_label" \
    --log-driver=none \
    --network "$egress_network" \
    --read-only \
    --cap-drop=ALL \
    --security-opt=no-new-privileges:true \
    --pids-limit=128 \
    --memory=256m \
    --cpus=1 \
    --tmpfs /tmp:rw,nosuid,nodev,noexec,size=16m \
    --mount "type=bind,src=$profile,dst=/run/hopash-profile.yaml,readonly" \
    "$MIHOMO_IMAGE" \
    -f /run/hopash-profile.yaml \
    -ext-ctl "127.0.0.1:$CONTROLLER_PORT" \
    -secret "$controller_secret" >/dev/null
docker start "$core_name" >/dev/null

docker network connect \
    --alias hopash-core \
    "$client_network" \
    "$core_name"

run_curl() {
    network=$1
    shift
    docker run \
        --rm \
        --pull=never \
        --network "$network" \
        --read-only \
        --cap-drop=ALL \
        --security-opt=no-new-privileges:true \
        --pids-limit=32 \
        --memory=32m \
        --cpus=0.25 \
        --env http_proxy= \
        --env https_proxy= \
        --env all_proxy= \
        --env HTTP_PROXY= \
        --env HTTPS_PROXY= \
        --env ALL_PROXY= \
        --env NO_PROXY= \
        --env no_proxy= \
        "$CURL_IMAGE" \
        "$@"
}

controller_ready=0
attempt=1
while test "$attempt" -le 15; do
    if run_curl "container:$core_name" \
        --fail \
        --silent \
        --connect-timeout 1 \
        --max-time 2 \
        --request PATCH \
        --header "Authorization: Bearer $controller_secret" \
        --header 'Content-Type: application/json' \
        --data '{"allow-lan":true,"bind-address":"*"}' \
        --output /dev/null \
        "http://127.0.0.1:$CONTROLLER_PORT/configs" >/dev/null 2>&1
    then
        controller_ready=1
        break
    fi
    attempt=$((attempt + 1))
    sleep 1
done
test "$controller_ready" -eq 1 || fail 'The Mihomo controller did not become ready.'

probe_direct() {
    run_curl "$1" \
        --ipv4 \
        --noproxy '*' \
        --silent \
        --connect-timeout 3 \
        --max-time 5 \
        --output /dev/null \
        --write-out '%{remote_ip}|%{http_code}' \
        "$GOOGLE_PROBE_URL"
}

if egress_result=$(probe_direct "$egress_network" 2>/dev/null)
then
    echo "INFO egress baseline: Google returned HTTP ${egress_result#*|} without a proxy."
else
    echo 'INFO egress baseline: direct Google access is unavailable.'
fi

if isolated_result=$(probe_direct "$client_network" 2>/dev/null)
then
    fail "The isolation control unexpectedly reached Google: $isolated_result."
fi
case "$isolated_result" in
    '|000') ;;
    *) fail "The isolation control established an unexpected external connection: $isolated_result." ;;
esac
echo 'PASS isolation control: the internal-only client cannot reach Google.'

proxy_code=''
attempt=1
while test "$attempt" -le 3; do
    if proxy_code=$(run_curl "$client_network" \
        --ipv4 \
        --proxy "http://hopash-core:$mixed_port" \
        --fail \
        --silent \
        --connect-timeout 8 \
        --max-time 20 \
        --output /dev/null \
        --write-out '%{http_code}' \
        "$GOOGLE_PROBE_URL" 2>/dev/null)
    then
        break
    fi
    attempt=$((attempt + 1))
done
test "$proxy_code" = '204' || fail "The proxied Google probe returned HTTP ${proxy_code:-000}."
echo 'PASS proxy path: Mihomo reached Google and returned HTTP 204.'

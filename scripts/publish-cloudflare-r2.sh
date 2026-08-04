#!/bin/sh
set -eu

bucket='ratash-releases'
wrangler_bin=${WRANGLER_BIN:-wrangler}

usage() {
    echo 'Usage: publish-cloudflare-r2.sh VERSION DISTRIBUTION_DIRECTORY' >&2
    exit 2
}

[ "$#" -eq 2 ] || usage
version=$1
distribution_directory=$2

case "$version" in
    '' | .* | *. | *..* | *[!0-9.]*) usage ;;
esac

previous_ifs=$IFS
IFS=.
set -- $version
IFS=$previous_ifs
[ "$#" -eq 3 ] || usage

: "${CLOUDFLARE_ACCOUNT_ID:?CLOUDFLARE_ACCOUNT_ID is required}"
: "${CLOUDFLARE_API_TOKEN:?CLOUDFLARE_API_TOKEN is required}"
[ -d "$distribution_directory" ] || usage

temporary_directory=$(/usr/bin/mktemp -d '/tmp/ratash-r2-publish.XXXXXX')
cleanup() {
    /bin/rm -rf -- "$temporary_directory"
}
trap cleanup 0
trap 'exit 1' 1 2 15

put_object() {
    source=$1
    key=$2
    content_type=$3
    cache_control=$4
    "$wrangler_bin" r2 object put "$bucket/$key" \
        --remote \
        --file "$source" \
        --content-type "$content_type" \
        --cache-control "$cache_control" \
        --force
}

upload_release_asset() {
    source=$1
    content_type=$2
    key="releases/v$version/${source##*/}"
    existing="$temporary_directory/existing"
    get_error="$temporary_directory/get-error"

    if "$wrangler_bin" r2 object get "$bucket/$key" \
        --remote \
        --file "$existing" 2>"$get_error"; then
        if /usr/bin/cmp -s "$source" "$existing"; then
            return
        fi
        echo "Refusing to replace immutable object $bucket/$key" >&2
        exit 1
    fi
    if ! /usr/bin/grep -Fq 'The specified key does not exist.' "$get_error"; then
        /bin/cat "$get_error" >&2
        exit 1
    fi

    put_object \
        "$source" \
        "$key" \
        "$content_type" \
        'public, max-age=31536000, immutable'
}

upload_public_file() {
    source=$1
    key=$2
    content_type=$3
    cache_control=$4
    put_object "$source" "$key" "$content_type" "$cache_control"
}

for target in aarch64-apple-darwin x86_64-apple-darwin; do
    package="$distribution_directory/ratash-$version-$target.pkg"
    checksum="$package.sha256"
    [ -f "$package" ] || { echo "Missing $package" >&2; exit 1; }
    [ -f "$checksum" ] || { echo "Missing $checksum" >&2; exit 1; }
    upload_release_asset "$package" 'application/vnd.apple.installer+xml'
    upload_release_asset "$checksum" 'text/plain; charset=utf-8'
done

[ -f "$distribution_directory/install.sh" ] \
    || { echo "Missing $distribution_directory/install.sh" >&2; exit 1; }
upload_public_file "$distribution_directory/install.sh" \
    'install.sh' \
    'text/x-shellscript; charset=utf-8' \
    'public, max-age=300'

latest_manifest="$temporary_directory/latest.json"
/usr/bin/printf '{"version":"%s"}\n' "$version" >"$latest_manifest"
upload_public_file "$latest_manifest" \
    'releases/latest.json' \
    'application/json' \
    'public, max-age=60'

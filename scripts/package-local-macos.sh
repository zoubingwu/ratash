#!/bin/sh
set -eu

usage() {
    echo 'usage: package-local-macos.sh [--output DIRECTORY]' >&2
    exit 2
}

output='dist'
while [ "$#" -gt 0 ]; do
    case "$1" in
        --output)
            [ "$#" -ge 2 ] || usage
            output=$2
            shift 2
            ;;
        -h|--help)
            echo 'usage: package-local-macos.sh [--output DIRECTORY]'
            exit 0
            ;;
        *) usage ;;
    esac
done

[ "$(/usr/bin/uname -s)" = 'Darwin' ] || {
    echo 'The personal macOS package must be built on macOS.' >&2
    exit 1
}
case $(/usr/bin/uname -m) in
    arm64) target='aarch64-apple-darwin' ;;
    x86_64) target='x86_64-apple-darwin' ;;
    *)
        echo 'The current Mac architecture is unsupported.' >&2
        exit 1
        ;;
esac

for command in cargo curl jq; do
    command -v "$command" >/dev/null 2>&1 || {
        echo "$command is required to build the personal package." >&2
        exit 1
    }
done

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
version=$(/usr/bin/sed -n 's/^version = "\([^"]*\)"/\1/p' "$project_root/Cargo.toml" | /usr/bin/head -n 1)
[ -n "$version" ] || {
    echo 'The Hopash version is unavailable.' >&2
    exit 1
}

/bin/mkdir -p "$output"
output=$(CDPATH= cd -- "$output" && pwd)
package_name="hopash-$version-$target-local-unsigned.pkg"
package="$output/$package_name"
checksum="$package.sha256"
[ ! -e "$package" ] || {
    echo "The personal package already exists: $package" >&2
    exit 1
}
[ ! -e "$checksum" ] || {
    echo "The personal package checksum already exists: $checksum" >&2
    exit 1
}

work_dir=$(/usr/bin/mktemp -d "${TMPDIR:-/tmp}/hopash-local-package.XXXXXX")
cleanup() {
    if [ -n "$work_dir" ] && [ -d "$work_dir" ]; then
        /bin/rm -rf -- "$work_dir"
    fi
}
trap cleanup EXIT HUP INT TERM

contract="$project_root/packaging/macos/package-contract-v1.json"
url=$(jq -r ".targets[\"$target\"].url" "$contract")
expected=$(jq -r ".targets[\"$target\"].sha256" "$contract")
curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
    "$url" --output "$work_dir/mihomo.gz"
actual=$(/usr/bin/shasum -a 256 "$work_dir/mihomo.gz" | /usr/bin/awk '{print $1}')
[ "$actual" = "$expected" ] || {
    echo 'The downloaded Mihomo archive failed SHA-256 verification.' >&2
    exit 1
}
/usr/bin/gzip -dc "$work_dir/mihomo.gz" >"$work_dir/mihomo"
/bin/chmod 0755 "$work_dir/mihomo"

curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
    'https://raw.githubusercontent.com/MetaCubeX/mihomo/v1.19.28/LICENSE' \
    --output "$work_dir/Mihomo-GPL-3.0.txt"

(
    CDPATH= cd -- "$project_root"
    cargo build --locked --release --target "$target" --no-default-features \
        --features local-unsigned --bin hopash
)
/usr/bin/install -m 0755 \
    "$project_root/target/$target/release/hopash" \
    "$work_dir/hopash"

/usr/bin/codesign --force --options runtime --identifier 'hopash' --sign - "$work_dir/hopash"
/usr/bin/codesign --force --options runtime --identifier 'mihomo' --sign - "$work_dir/mihomo"
/usr/bin/codesign --verify --strict --verbose=2 "$work_dir/hopash"
/usr/bin/codesign --verify --strict --verbose=2 "$work_dir/mihomo"

mihomo_sha256=$(/usr/bin/shasum -a 256 "$work_dir/mihomo" | /usr/bin/awk '{print $1}')
package_output="$work_dir/package"
"$project_root/scripts/package-macos.sh" \
    --version "$version" \
    --target "$target" \
    --hopash "$work_dir/hopash" \
    --mihomo "$work_dir/mihomo" \
    --mihomo-sha256 "$mihomo_sha256" \
    --mihomo-license "$work_dir/Mihomo-GPL-3.0.txt" \
    --output "$package_output" >/dev/null

/bin/mv "$package_output/hopash-$version-$target.pkg" "$package"
(
    CDPATH= cd -- "$output"
    /usr/bin/shasum -a 256 "$package_name" >"$package_name.sha256"
)
printf '%s\n' "$package"

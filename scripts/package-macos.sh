#!/bin/sh
set -eu

usage() {
    echo 'usage: package-macos.sh --version VERSION --target TARGET --hopash PATH --mihomo PATH --mihomo-sha256 SHA256 --mihomo-license PATH --geodata-directory DIRECTORY --geodata-manifest PATH --geodata-license PATH (--output DIRECTORY | --stage-only DIRECTORY) [--application-identity IDENTITY] [--installer-identity IDENTITY]' >&2
    exit 2
}

version=''
target=''
hopash=''
mihomo=''
mihomo_sha256=''
mihomo_license=''
geodata_directory=''
geodata_manifest=''
geodata_license=''
output=''
stage_only=''
application_identity=''
installer_identity=''

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version|--target|--hopash|--mihomo|--mihomo-sha256|--mihomo-license|--geodata-directory|--geodata-manifest|--geodata-license|--output|--stage-only|--application-identity|--installer-identity)
            [ "$#" -ge 2 ] || usage
            option=$1
            value=$2
            shift 2
            case "$option" in
                --version) version=$value ;;
                --target) target=$value ;;
                --hopash) hopash=$value ;;
                --mihomo) mihomo=$value ;;
                --mihomo-sha256) mihomo_sha256=$value ;;
                --mihomo-license) mihomo_license=$value ;;
                --geodata-directory) geodata_directory=$value ;;
                --geodata-manifest) geodata_manifest=$value ;;
                --geodata-license) geodata_license=$value ;;
                --output) output=$value ;;
                --stage-only) stage_only=$value ;;
                --application-identity) application_identity=$value ;;
                --installer-identity) installer_identity=$value ;;
            esac
            ;;
        *) usage ;;
    esac
done

[ -n "$version" ] || usage
[ -n "$hopash" ] || usage
[ -n "$mihomo" ] || usage
[ -n "$mihomo_sha256" ] || usage
[ -n "$mihomo_license" ] || usage
[ -n "$geodata_directory" ] || usage
[ -n "$geodata_manifest" ] || usage
[ -n "$geodata_license" ] || usage
case "$target" in
    aarch64-apple-darwin|x86_64-apple-darwin) ;;
    *) usage ;;
esac
case "$version" in
    *[!0-9.]*|'') usage ;;
esac
printf '%s\n' "$version" | /usr/bin/grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' || usage
case "$mihomo_sha256" in
    *[!0-9A-Fa-f]*|'') usage ;;
esac
[ "${#mihomo_sha256}" -eq 64 ] || usage
[ -f "$hopash" ] || { echo 'Hopash executable is unavailable.' >&2; exit 1; }
[ -f "$mihomo" ] || { echo 'Mihomo executable is unavailable.' >&2; exit 1; }
[ -f "$mihomo_license" ] || { echo 'Mihomo license is unavailable.' >&2; exit 1; }
if [ ! -d "$geodata_directory" ] || [ -L "$geodata_directory" ]; then
    echo 'Geo data directory is unavailable.' >&2
    exit 1
fi
if [ ! -f "$geodata_manifest" ] || [ -L "$geodata_manifest" ]; then
    echo 'Geo data manifest is unavailable.' >&2
    exit 1
fi
if [ ! -f "$geodata_license" ] || [ -L "$geodata_license" ]; then
    echo 'Geo data license is unavailable.' >&2
    exit 1
fi
if [ -n "$output" ] && [ -n "$stage_only" ]; then
    usage
fi
if [ -z "$output" ] && [ -z "$stage_only" ]; then
    usage
fi
if [ -n "$application_identity" ] && [ -z "$installer_identity" ]; then
    echo 'Application and installer signing identities must be provided together.' >&2
    exit 1
fi
if [ -z "$application_identity" ] && [ -n "$installer_identity" ]; then
    echo 'Application and installer signing identities must be provided together.' >&2
    exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
command -v jq >/dev/null 2>&1 || {
    echo 'jq is required to verify the Geo data manifest.' >&2
    exit 1
}
if [ "${HOPASH_TEST_ALLOW_CUSTOM_GEODATA_MANIFEST:-0}" = '1' ]; then
    if [ -z "$stage_only" ] || [ -n "$output" ] || [ -n "$application_identity" ] || [ -n "$installer_identity" ]; then
        echo 'Custom Geo data manifests are restricted to unsigned test staging.' >&2
        exit 1
    fi
else
    bundled_geodata_manifest="$project_root/fixtures/mihomo/v1.19.28/geodata-manifest.json"
    bundled_manifest_sha256=$(/usr/bin/shasum -a 256 "$bundled_geodata_manifest" | /usr/bin/awk '{print $1}')
    supplied_manifest_sha256=$(/usr/bin/shasum -a 256 "$geodata_manifest" | /usr/bin/awk '{print $1}')
    [ "$supplied_manifest_sha256" = "$bundled_manifest_sha256" ] || {
        echo 'Geo data manifest identity does not match the bundled catalog.' >&2
        exit 1
    }
fi
actual_mihomo_sha256=$(/usr/bin/shasum -a 256 "$mihomo" | /usr/bin/awk '{print $1}')
if [ "$actual_mihomo_sha256" != "$mihomo_sha256" ]; then
    echo 'Mihomo SHA-256 verification failed.' >&2
    exit 1
fi

manifest_value() {
    jq -er "$1" "$geodata_manifest"
}

[ "$(manifest_value '.schema_version')" = '1' ] || {
    echo 'Geo data manifest schema is unsupported.' >&2
    exit 1
}
[ "$(manifest_value '.core_version')" = 'v1.19.28' ] || {
    echo 'Geo data manifest Core version does not match the bundled Mihomo version.' >&2
    exit 1
}
[ "$(manifest_value '.repository_license')" = 'GPL-3.0-only' ] || {
    echo 'Geo data manifest license is unsupported.' >&2
    exit 1
}
[ "$(manifest_value '.assets | length')" = '4' ] || {
    echo 'Geo data manifest must contain exactly four assets.' >&2
    exit 1
}

verify_geodata_asset() {
    index=$1
    expected_name=$2
    path="$geodata_directory/$expected_name"
    [ "$(manifest_value ".assets[$index].file_name")" = "$expected_name" ] || {
        echo "Geo data manifest entry $index has an unexpected file name." >&2
        exit 1
    }
    if [ ! -f "$path" ] || [ -L "$path" ]; then
        echo "Geo data asset $expected_name is unavailable." >&2
        exit 1
    fi
    expected_size=$(manifest_value ".assets[$index].size")
    actual_size=$(/usr/bin/wc -c <"$path" | /usr/bin/awk '{print $1}')
    [ "$actual_size" = "$expected_size" ] || {
        echo "Geo data asset $expected_name failed size verification." >&2
        exit 1
    }
    expected_sha256=$(manifest_value ".assets[$index].sha256")
    actual_sha256=$(/usr/bin/shasum -a 256 "$path" | /usr/bin/awk '{print $1}')
    [ "$actual_sha256" = "$expected_sha256" ] || {
        echo "Geo data asset $expected_name failed SHA-256 verification." >&2
        exit 1
    }
}

verify_geodata_asset 0 'ASN.mmdb'
verify_geodata_asset 1 'Country.mmdb'
verify_geodata_asset 2 'GeoIP.dat'
verify_geodata_asset 3 'GeoSite.dat'

work_dir=$(/usr/bin/mktemp -d "${TMPDIR:-/tmp}/hopash-package.XXXXXX")
cleanup() {
    if [ -n "$work_dir" ] && [ -d "$work_dir" ]; then
        /bin/rm -rf -- "$work_dir"
    fi
}
trap cleanup EXIT HUP INT TERM

stage="$work_dir/stage"
payload="$stage/payload"
package_scripts="$stage/scripts"
/bin/mkdir -p \
    "$payload/usr/local/bin" \
    "$payload/usr/local/share/man/man1" \
    "$payload/usr/local/share/bash-completion/completions" \
    "$payload/usr/local/share/zsh/site-functions" \
    "$payload/usr/local/share/fish/vendor_completions.d" \
    "$payload/usr/local/share/hopash/skills/hopash/agents" \
    "$payload/usr/local/share/hopash/licenses" \
    "$payload/usr/local/share/hopash/release" \
    "$payload/Library/PrivilegedHelperTools" \
    "$payload/Library/LaunchDaemons" \
    "$payload/Library/Application Support/Hopash RS/bin" \
    "$payload/Library/Application Support/Hopash RS/share/geodata" \
    "$package_scripts"

/usr/bin/install -m 0755 "$hopash" "$payload/usr/local/bin/hopash"
/usr/bin/install -m 0755 "$hopash" "$payload/Library/PrivilegedHelperTools/io.hopash.core-runtime"
/usr/bin/install -m 0755 "$mihomo" "$payload/Library/Application Support/Hopash RS/bin/mihomo"
/usr/bin/install -m 0644 "$geodata_directory/ASN.mmdb" "$payload/Library/Application Support/Hopash RS/share/geodata/ASN.mmdb"
/usr/bin/install -m 0644 "$geodata_directory/Country.mmdb" "$payload/Library/Application Support/Hopash RS/share/geodata/Country.mmdb"
/usr/bin/install -m 0644 "$geodata_directory/GeoIP.dat" "$payload/Library/Application Support/Hopash RS/share/geodata/GeoIP.dat"
/usr/bin/install -m 0644 "$geodata_directory/GeoSite.dat" "$payload/Library/Application Support/Hopash RS/share/geodata/GeoSite.dat"
/usr/bin/install -m 0755 "$project_root/packaging/macos/uninstall.sh" "$payload/usr/local/share/hopash/uninstall.sh"
/usr/bin/install -m 0644 "$project_root/packaging/macos/io.hopash.core-runtime.plist" "$payload/Library/LaunchDaemons/io.hopash.core-runtime.plist"
for man_page in "$project_root"/packaging/generated/man/man1/*.1; do
    /usr/bin/install -m 0644 "$man_page" "$payload/usr/local/share/man/man1/$(/usr/bin/basename "$man_page")"
done
/usr/bin/install -m 0644 "$project_root/packaging/generated/completions/hopash.bash" "$payload/usr/local/share/bash-completion/completions/hopash"
/usr/bin/install -m 0644 "$project_root/packaging/generated/completions/_hopash" "$payload/usr/local/share/zsh/site-functions/_hopash"
/usr/bin/install -m 0644 "$project_root/packaging/generated/completions/hopash.fish" "$payload/usr/local/share/fish/vendor_completions.d/hopash.fish"
/usr/bin/install -m 0644 "$project_root/skills/hopash/SKILL.md" "$payload/usr/local/share/hopash/skills/hopash/SKILL.md"
/usr/bin/install -m 0644 "$project_root/skills/hopash/agents/openai.yaml" "$payload/usr/local/share/hopash/skills/hopash/agents/openai.yaml"
/usr/bin/install -m 0644 "$mihomo_license" "$payload/usr/local/share/hopash/licenses/Mihomo-GPL-3.0.txt"
/usr/bin/install -m 0644 "$project_root/packaging/macos/Mihomo-NOTICE.txt" "$payload/usr/local/share/hopash/licenses/Mihomo-NOTICE.txt"
/usr/bin/install -m 0644 "$geodata_license" "$payload/usr/local/share/hopash/licenses/MetaCubeX-meta-rules-dat-GPL-3.0.txt"
/usr/bin/install -m 0644 "$project_root/packaging/macos/GeoData-NOTICE.txt" "$payload/usr/local/share/hopash/licenses/GeoData-NOTICE.txt"
/usr/bin/install -m 0644 "$project_root/fixtures/release/product-contract-v1.json" "$payload/usr/local/share/hopash/release/product-contract-v1.json"
/usr/bin/install -m 0644 "$project_root/fixtures/release/benchmark-metadata-v1.json" "$payload/usr/local/share/hopash/release/benchmark-metadata-v1.json"
/usr/bin/install -m 0644 "$project_root/packaging/macos/package-contract-v1.json" "$payload/usr/local/share/hopash/release/package-contract-v1.json"
/usr/bin/install -m 0644 "$geodata_manifest" "$payload/usr/local/share/hopash/release/geodata-manifest.json"
/usr/bin/install -m 0755 "$project_root/packaging/macos/scripts/postinstall" "$package_scripts/postinstall"

if [ -n "$application_identity" ]; then
    /usr/bin/codesign --force --options runtime --timestamp --identifier 'hopash' --sign "$application_identity" "$payload/usr/local/bin/hopash"
    /usr/bin/codesign --force --options runtime --timestamp --sign "$application_identity" "$payload/Library/PrivilegedHelperTools/io.hopash.core-runtime"
    /usr/bin/codesign --force --options runtime --timestamp --sign "$application_identity" "$payload/Library/Application Support/Hopash RS/bin/mihomo"
    /usr/bin/codesign --verify --strict --verbose=2 "$payload/usr/local/bin/hopash"
    /usr/bin/codesign --verify --strict --verbose=2 "$payload/Library/PrivilegedHelperTools/io.hopash.core-runtime"
    /usr/bin/codesign --verify --strict --verbose=2 "$payload/Library/Application Support/Hopash RS/bin/mihomo"
fi

if [ -n "$stage_only" ]; then
    [ ! -e "$stage_only" ] || { echo 'Stage destination already exists.' >&2; exit 1; }
    /bin/mv "$stage" "$stage_only"
    exit 0
fi

/bin/mkdir -p "$output"
unsigned_package="$work_dir/hopash-$version-$target-unsigned.pkg"
final_package="$output/hopash-$version-$target.pkg"
[ ! -e "$final_package" ] || { echo 'Output package already exists.' >&2; exit 1; }
[ ! -e "$final_package.sha256" ] || { echo 'Output checksum already exists.' >&2; exit 1; }
/usr/bin/pkgbuild \
    --root "$payload" \
    --scripts "$package_scripts" \
    --identifier 'io.hopash.rs' \
    --version "$version" \
    --ownership recommended \
    --install-location / \
    "$unsigned_package"
if [ -n "$installer_identity" ]; then
    /usr/bin/productsign --sign "$installer_identity" "$unsigned_package" "$final_package"
else
    /bin/mv "$unsigned_package" "$final_package"
fi
package_name=$(basename -- "$final_package")
(
    CDPATH= cd -- "$output"
    /usr/bin/shasum -a 256 "$package_name" >"$package_name.sha256"
)
echo "$final_package"

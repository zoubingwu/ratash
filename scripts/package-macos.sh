#!/bin/sh
set -eu

usage() {
    echo 'usage: package-macos.sh --version VERSION --target TARGET --hopash PATH --mihomo PATH --mihomo-sha256 SHA256 --mihomo-license PATH (--output DIRECTORY | --stage-only DIRECTORY) [--application-identity IDENTITY] [--installer-identity IDENTITY]' >&2
    exit 2
}

version=''
target=''
hopash=''
mihomo=''
mihomo_sha256=''
mihomo_license=''
output=''
stage_only=''
application_identity=''
installer_identity=''

while [ "$#" -gt 0 ]; do
    case "$1" in
        --version|--target|--hopash|--mihomo|--mihomo-sha256|--mihomo-license|--output|--stage-only|--application-identity|--installer-identity)
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
actual_mihomo_sha256=$(/usr/bin/shasum -a 256 "$mihomo" | /usr/bin/awk '{print $1}')
if [ "$actual_mihomo_sha256" != "$mihomo_sha256" ]; then
    echo 'Mihomo SHA-256 verification failed.' >&2
    exit 1
fi

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
    "$package_scripts"

/usr/bin/install -m 0755 "$hopash" "$payload/usr/local/bin/hopash"
/usr/bin/install -m 0755 "$hopash" "$payload/Library/PrivilegedHelperTools/io.hopash.core-runtime"
/usr/bin/install -m 0755 "$mihomo" "$payload/Library/Application Support/Hopash RS/bin/mihomo"
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
/usr/bin/install -m 0644 "$project_root/fixtures/release/product-contract-v1.json" "$payload/usr/local/share/hopash/release/product-contract-v1.json"
/usr/bin/install -m 0644 "$project_root/fixtures/release/benchmark-metadata-v1.json" "$payload/usr/local/share/hopash/release/benchmark-metadata-v1.json"
/usr/bin/install -m 0644 "$project_root/packaging/macos/package-contract-v1.json" "$payload/usr/local/share/hopash/release/package-contract-v1.json"
/usr/bin/install -m 0755 "$project_root/packaging/macos/scripts/postinstall" "$package_scripts/postinstall"

if [ -n "$application_identity" ]; then
    /usr/bin/codesign --force --options runtime --timestamp --sign "$application_identity" "$payload/usr/local/bin/hopash"
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

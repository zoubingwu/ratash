#!/bin/sh
set -eu

download_url='https://ratash.zoubingwu.com'
package_identifier='io.ratash'
ratash_binary='/usr/local/bin/ratash'
installed_uninstaller='/usr/local/share/ratash/uninstall.sh'
temporary_directory=''

usage() {
    echo 'Usage: install.sh [install|update|uninstall]'
    echo
    echo '  install     Install the latest release or update an existing installation.'
    echo '  update      Update an existing installation to the latest release.'
    echo '  uninstall   Remove Ratash while preserving user Profiles and rules.'
}

fail() {
    echo "Ratash: $*" >&2
    exit 1
}

progress() {
    echo "Ratash: $*"
}

cleanup() {
    if [ -n "$temporary_directory" ] && [ -d "$temporary_directory" ]; then
        /bin/rm -rf -- "$temporary_directory"
    fi
}

download() {
    source_url=$1
    destination=$2
    /usr/bin/curl \
        --fail \
        --silent \
        --show-error \
        --location \
        --proto '=https' \
        --proto-redir '=https' \
        --tlsv1.2 \
        --retry 3 \
        --output "$destination" \
        "$source_url"
}

is_installed() {
    /usr/sbin/pkgutil --pkg-info "$package_identifier" >/dev/null 2>&1
}

installed_version() {
    /usr/sbin/pkgutil --pkg-info "$package_identifier" 2>/dev/null \
        | /usr/bin/sed -n 's/^version: //p'
}

is_release_version() {
    candidate=$1
    case "$candidate" in
        '' | .* | *. | *..* | *[!0-9.]*) return 1 ;;
    esac

    previous_ifs=$IFS
    IFS=.
    set -- $candidate
    IFS=$previous_ifs
    [ "$#" -eq 3 ] || return 1
    for component in "$@"; do
        case "$component" in
            '' | *[!0-9]*) return 1 ;;
        esac
    done
}

latest_release() {
    manifest=$1
    download "$download_url/releases/latest.json" "$manifest"
    version=$(
        /usr/bin/sed -n 's/^{"version":"\([0-9][0-9.]*\)"}$/\1/p' "$manifest"
    )
    if [ "$(/usr/bin/wc -l <"$manifest")" -ne 1 ] \
        || ! is_release_version "$version"; then
        fail 'The latest Ratash release metadata is invalid.'
    fi
    echo "v$version"
}

require_macos_user() {
    [ "$(/usr/bin/uname -s)" = 'Darwin' ] || fail 'The installer supports macOS.'
    [ "$(/usr/bin/id -u)" -ne 0 ] \
        || fail 'Run this script as your normal macOS user. It will request sudo itself.'
}

release_target() {
    case "$(/usr/bin/uname -m)" in
        arm64) echo 'aarch64-apple-darwin' ;;
        x86_64) echo 'x86_64-apple-darwin' ;;
        *) fail 'This Mac architecture is unsupported.' ;;
    esac
}

make_temporary_directory() {
    /usr/bin/mktemp -d '/tmp/ratash-install.XXXXXX'
}

verify_package() {
    package_name=$1
    package_directory=$2
    (
        CDPATH= cd -- "$package_directory"
        /usr/bin/shasum -a 256 -c "$package_name.sha256"
    )
}

acquire_privileges() {
    /usr/bin/sudo -v
}

stop_ratash() {
    if [ -x "$ratash_binary" ]; then
        /usr/local/bin/ratash stop --json
    fi
}

install_package() {
    package=$1
    owner_uid=$(/usr/bin/id -u)
    /usr/bin/sudo /usr/bin/env RATASH_OWNER_UID="$owner_uid" \
        /usr/sbin/installer \
        -allowUntrusted \
        -pkg "$package" \
        -target /
}

verify_installation() {
    expected_version=$1
    installed=$(installed_version)
    [ "$installed" = "$expected_version" ] \
        || fail "The installed package reports version ${installed:-unknown}; expected $expected_version."
    /usr/local/bin/ratash --version
    /usr/bin/sudo /bin/launchctl print 'system/io.ratash.core-runtime' >/dev/null
}

start_ratash() {
    /usr/local/bin/ratash start --json
}

has_installed_uninstaller() {
    [ -x "$installed_uninstaller" ]
}

ratash_artifact_exists() {
    [ -e "$ratash_binary" ]
}

run_uninstaller() {
    /usr/bin/sudo "$installed_uninstaller"
}

install_release() {
    requested_action=$1
    current_version=''
    if is_installed; then
        current_version=$(installed_version)
    elif [ "$requested_action" = 'update' ]; then
        fail 'Ratash is not installed. Run the install command first.'
    fi

    target=$(release_target)
    temporary_directory=$(make_temporary_directory)
    progress 'Checking for the latest release...'
    tag=$(latest_release "$temporary_directory/latest.json")
    version=${tag#v}
    if [ "$current_version" = "$version" ]; then
        echo "Ratash $version is already up to date."
        return
    fi

    package_name="ratash-$version-$target.pkg"
    asset_url="$download_url/releases/$tag/$package_name"
    package="$temporary_directory/$package_name"
    checksum="$package.sha256"

    progress "Downloading version $version for this Mac..."
    download "$asset_url" "$package"
    download "$asset_url.sha256" "$checksum"
    progress 'Verifying the downloaded package...'
    verify_package "$package_name" "$temporary_directory"
    progress 'Requesting administrator access...'
    acquire_privileges
    progress 'Preparing the current service...'
    stop_ratash >/dev/null
    progress "Installing version $version..."
    install_package "$package"
    progress 'Verifying the installation...'
    verify_installation "$version"
    progress 'Starting the service...'
    start_ratash >/dev/null
    echo "Ratash $version is installed and running."
}

uninstall_ratash() {
    if ! has_installed_uninstaller; then
        if ! is_installed && ! ratash_artifact_exists; then
            echo 'Ratash is already removed.'
            return
        fi
        fail 'The installed Ratash uninstaller is unavailable. Reinstall Ratash, then uninstall it.'
    fi

    progress 'Requesting administrator access...'
    acquire_privileges
    progress 'Stopping the service...'
    stop_ratash >/dev/null
    progress 'Removing the application...'
    run_uninstaller >/dev/null
    echo 'Ratash was removed. Your Profiles and rules were preserved.'
}

main() {
    if [ "$#" -eq 0 ]; then
        action='install'
    else
        action=$1
        shift
    fi
    [ "$#" -eq 0 ] || fail 'Only one lifecycle action is accepted.'

    case "$action" in
        -h | --help)
            usage
            exit 0
            ;;
        install | update | uninstall) ;;
        *)
            usage >&2
            exit 2
            ;;
    esac

    require_macos_user
    trap cleanup 0
    trap 'exit 1' 1 2 15

    case "$action" in
        install|update) install_release "$action" ;;
        uninstall) uninstall_ratash ;;
    esac
}

main "$@"

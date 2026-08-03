#!/bin/sh
set -eu

package_name='@PACKAGE_NAME@'
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
package="$script_dir/$package_name"
checksum="$package.sha256"

case "$package_name" in
    *-aarch64-apple-darwin-*) expected_machine='arm64' ;;
    *-x86_64-apple-darwin-*) expected_machine='x86_64' ;;
    *)
        echo 'The Ratash package architecture is unavailable.' >&2
        exit 1
        ;;
esac
if [ "$(/usr/bin/uname -m)" != "$expected_machine" ]; then
    echo "This Ratash package requires $expected_machine." >&2
    exit 1
fi
if [ "$(/usr/bin/id -u)" -eq 0 ]; then
    echo 'Run this script as your normal macOS user. It will request sudo itself.' >&2
    exit 1
fi
if [ ! -f "$package" ] || [ ! -f "$checksum" ]; then
    echo "Place $package_name and its .sha256 file next to this script." >&2
    exit 1
fi

(
    CDPATH= cd -- "$script_dir"
    /usr/bin/shasum -a 256 -c "$package_name.sha256"
)

/usr/bin/sudo -v
if [ -x /usr/local/bin/ratash ]; then
    /usr/local/bin/ratash stop --json
fi

owner_uid=$(/usr/bin/id -u)
/usr/bin/sudo /usr/bin/env RATASH_OWNER_UID="$owner_uid" \
    /usr/sbin/installer \
    -allowUntrusted \
    -pkg "$package" \
    -target /

/usr/local/bin/ratash --version
/usr/bin/sudo /bin/launchctl print 'system/io.ratash.core-runtime' >/dev/null
/usr/local/bin/ratash start --json

echo 'Ratash was installed and started.'

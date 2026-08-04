#!/bin/sh
set -eu

if [ "$(/usr/bin/id -u)" -ne 0 ]; then
    echo 'Run this uninstaller with sudo.' >&2
    exit 1
fi

SERVICE_LABEL='io.ratash.core-runtime'
/bin/launchctl bootout "system/$SERVICE_LABEL" >/dev/null 2>&1 || true

/bin/rm -f -- \
    '/usr/local/bin/ratash' \
    '/Library/PrivilegedHelperTools/ratash-core-runtime' \
    '/Library/PrivilegedHelperTools/io.ratash.core-runtime' \
    '/Library/LaunchDaemons/io.ratash.core-runtime.plist' \
    '/usr/local/share/bash-completion/completions/ratash' \
    '/usr/local/share/zsh/site-functions/_ratash' \
    '/usr/local/share/fish/vendor_completions.d/ratash.fish' \
    /usr/local/share/man/man1/ratash*.1
/bin/rm -rf -- \
    '/Library/Application Support/ratash' \
    '/usr/local/share/ratash' \
    '/var/run/ratash'
/usr/sbin/pkgutil --forget 'io.ratash' >/dev/null 2>&1 || true

echo 'Ratash was removed. User Profile and rule data remains in each user account.'

#!/bin/sh
set -eu

if [ "$(/usr/bin/id -u)" -ne 0 ]; then
    echo 'Run this uninstaller with sudo.' >&2
    exit 1
fi

SERVICE_LABEL='io.hopash.core-runtime'
/bin/launchctl bootout "system/$SERVICE_LABEL" >/dev/null 2>&1 || true

/bin/rm -f -- \
    '/usr/local/bin/hopash' \
    '/Library/PrivilegedHelperTools/io.hopash.core-runtime' \
    '/Library/LaunchDaemons/io.hopash.core-runtime.plist' \
    '/usr/local/share/bash-completion/completions/hopash' \
    '/usr/local/share/zsh/site-functions/_hopash' \
    '/usr/local/share/fish/vendor_completions.d/hopash.fish' \
    /usr/local/share/man/man1/hopash*.1
/bin/rm -rf -- \
    '/Library/Application Support/Hopash RS' \
    '/usr/local/share/hopash' \
    '/var/run/hopash-rs'
/usr/sbin/pkgutil --forget 'io.hopash.rs' >/dev/null 2>&1 || true

echo 'Hopash RS was removed. User Profile and rule data remains in each user account.'

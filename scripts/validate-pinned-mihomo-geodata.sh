#!/bin/sh
set -eu

usage() {
    echo 'usage: validate-pinned-mihomo-geodata.sh MIHOMO GEODATA_DIRECTORY' >&2
    exit 2
}

[ "$#" -eq 2 ] || usage
mihomo=$1
geodata_directory=$2

[ -x "$mihomo" ] || {
    echo 'The Mihomo executable is unavailable.' >&2
    exit 1
}
[ -d "$geodata_directory" ] || {
    echo 'The Geo data directory is unavailable.' >&2
    exit 1
}

mihomo_directory=$(CDPATH= cd -- "$(dirname -- "$mihomo")" && pwd)
mihomo="$mihomo_directory/$(basename -- "$mihomo")"
geodata_directory=$(CDPATH= cd -- "$geodata_directory" && pwd)

umask 077
validation_home=$(/usr/bin/mktemp -d "${TMPDIR:-/tmp}/ratash-geodata-validation.XXXXXX")
cleanup() {
    /bin/rm -rf -- "$validation_home"
}
trap cleanup EXIT HUP INT TERM

for asset in ASN.mmdb Country.mmdb GeoIP.dat GeoSite.dat; do
    [ -f "$geodata_directory/$asset" ] || {
        echo "The Geo data asset $asset is unavailable." >&2
        exit 1
    }
    /bin/ln -s "$geodata_directory/$asset" "$validation_home/$asset"
done

unset CLASH_AGE_SECRET_KEY \
    CLASH_CONFIG_FILE \
    CLASH_CONFIG_STRING \
    CLASH_HOME_DIR \
    CLASH_OVERRIDE_EXTERNAL_CONTROLLER \
    CLASH_OVERRIDE_EXTERNAL_CONTROLLER_PIPE \
    CLASH_OVERRIDE_EXTERNAL_CONTROLLER_ROUTING_MARK \
    CLASH_OVERRIDE_EXTERNAL_CONTROLLER_TLS \
    CLASH_OVERRIDE_EXTERNAL_CONTROLLER_UNIX \
    CLASH_OVERRIDE_EXTERNAL_UI_DIR \
    CLASH_OVERRIDE_SECRET \
    CLASH_POST_DOWN \
    CLASH_POST_UP \
    SAFE_PATHS \
    SKIP_SAFE_PATH_CHECK

mmdb_config="$validation_home/mmdb.yaml"
/usr/bin/printf '%s\n' \
    'mode: rule' \
    'log-level: silent' \
    'geo-auto-update: false' \
    'geodata-mode: false' \
    'tun:' \
    '  enable: false' \
    'dns:' \
    '  enable: false' \
    'rules:' \
    '  - GEOIP,CN,DIRECT' \
    '  - GEOSITE,CN,DIRECT' \
    '  - IP-ASN,13335,DIRECT' \
    '  - MATCH,DIRECT' >"$mmdb_config"
"$mihomo" -t -d "$validation_home" -f "$mmdb_config"

dat_config="$validation_home/dat.yaml"
/usr/bin/printf '%s\n' \
    'mode: rule' \
    'log-level: silent' \
    'geo-auto-update: false' \
    'geodata-mode: true' \
    'tun:' \
    '  enable: false' \
    'dns:' \
    '  enable: false' \
    'rules:' \
    '  - GEOIP,CN,DIRECT' \
    '  - GEOSITE,CN,DIRECT' \
    '  - IP-ASN,13335,DIRECT' \
    '  - MATCH,DIRECT' >"$dat_config"
"$mihomo" -t -d "$validation_home" -f "$dat_config"

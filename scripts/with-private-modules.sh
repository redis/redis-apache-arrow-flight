#!/bin/sh
# Scope private fetching to the pinned Redis ADBC fork. Keep checksums enabled
# for public dependencies, and keep Git credentials out of URLs and config files.
set -eu
private_module=github.com/alberttwong/redis-adbc-driver/go
export GONOPROXY="${GONOPROXY:+$GONOPROXY,}$private_module"
export GONOSUMDB="${GONOSUMDB:+$GONOSUMDB,}$private_module"
if [ -n "${REDIS_ADBC_TOKEN:-}" ] || { [ -n "${REDIS_ADBC_TOKEN_FILE:-}" ] && [ -r "$REDIS_ADBC_TOKEN_FILE" ]; }; then
    script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
    count=${GIT_CONFIG_COUNT:-0}
    case "$count" in *[!0-9]*|'') echo 'invalid GIT_CONFIG_COUNT' >&2; exit 1 ;; esac
    export "GIT_CONFIG_KEY_${count}=credential.https://github.com.helper"
    export "GIT_CONFIG_VALUE_${count}="
    count=$((count + 1))
    export "GIT_CONFIG_KEY_${count}=credential.https://github.com.helper"
    export "GIT_CONFIG_VALUE_${count}=!\"$script_dir/github-credential.sh\""
    export GIT_CONFIG_COUNT=$((count + 1))
fi
exec "$@"

#!/bin/sh
# Git invokes this helper through its credential protocol. Never log the token.
set -eu
[ "${1:-}" = get ] || exit 0
protocol= host=
while IFS='=' read -r name value; do
    case "$name" in
        protocol) protocol=$value ;;
        host) host=$value ;;
    esac
done
[ "$protocol" = https ] && [ "$host" = github.com ] || exit 0
if [ -n "${REDIS_ADBC_TOKEN_FILE:-}" ] && [ -r "$REDIS_ADBC_TOKEN_FILE" ]; then
    token=$(cat "$REDIS_ADBC_TOKEN_FILE")
else
    token=${REDIS_ADBC_TOKEN:-}
fi
[ -n "$token" ] || exit 0
printf 'username=x-access-token\npassword=%s\n' "$token"

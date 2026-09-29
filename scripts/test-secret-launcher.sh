#!/usr/bin/env bash
set -Eeuo pipefail

test_dir="$(mktemp -d)"
cleanup() {
  rm -f "$test_dir/launcher" "$test_dir/database-url" "$test_dir/api-key"
  rmdir "$test_dir"
}
trap cleanup EXIT

cc -O2 -Wall -Wextra -Werror -DFLASH_SECRET_DIR="\"$test_dir\"" \
  -o "$test_dir/launcher" src/flash-secret-env-launcher.c
printf 'synthetic-database-value' > "$test_dir/database-url"
printf 'synthetic-api-value\nsecond-line' > "$test_dir/api-key"

"$test_dir/launcher" \
  --secret-env DATABASE_URL=database-url \
  --secret-env API_KEY=api-key \
  -- /bin/sh -c 'test "$DATABASE_URL" = synthetic-database-value && test "$API_KEY" = "$(printf "synthetic-api-value\nsecond-line")"'

if "$test_dir/launcher" --secret-env DATABASE_URL=missing -- /bin/true 2>/dev/null; then
  echo "missing secret unexpectedly started the workload" >&2
  exit 1
fi
if "$test_dir/launcher" --secret-env BAD-NAME=database-url -- /bin/true 2>/dev/null; then
  echo "invalid environment name unexpectedly started the workload" >&2
  exit 1
fi

echo "secret environment launcher passed"

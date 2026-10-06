#!/usr/bin/env bash
# Resolve this checkout as a dependency, without its root-project lockfile or
# installed package cache. Run through `make check-consumer-deps`.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
nimble=${1:?Usage: check_consumer_deps.sh <nimble> [nim]}
compiler=${2:-}

scratch=$(mktemp -d "${TMPDIR:-/tmp}/logos-delivery-consumer.XXXXXX")
trap 'rm -rf "${scratch}"' EXIT
mkdir -p "${scratch}/consumer" "${scratch}/config/nimble"

# Copy the current manifest into a temporary git commit.
# This tests local edits through the same git dependency path as consumers.
git clone --quiet --no-hardlinks "${root}" "${scratch}/package"
git -C "${scratch}/package" remote remove origin
cp "${root}/logos_delivery.nimble" "${scratch}/package/logos_delivery.nimble"
git -C "${scratch}/package" add logos_delivery.nimble
git -C "${scratch}/package" -c core.hooksPath=/dev/null \
  -c user.name='Consumer check' -c user.email='consumer-check@example.invalid' \
  commit --quiet --no-gpg-sign --allow-empty -m 'Snapshot consumer requirements'
revision=$(git -C "${scratch}/package" rev-parse HEAD)

# Redirect package downloads to the local commit.
# This also works for PR commits and uncommitted manifest edits.
git_config_count=${GIT_CONFIG_COUNT:-0}
export "GIT_CONFIG_KEY_${git_config_count}=url.file://${scratch}/package.insteadOf"
export "GIT_CONFIG_VALUE_${git_config_count}=https://github.com/logos-messaging/logos-delivery"
export GIT_CONFIG_COUNT=$((git_config_count + 1))

# --nimbleDir isolates packages and discovery metadata.
# The Nimble configuration also isolates the buildtemp directory.
export XDG_CONFIG_HOME="${scratch}/config"
printf 'nimbleDir = "%s"\n' "${scratch}/nimble" > "${XDG_CONFIG_HOME}/nimble/nimble.ini"

cat > "${scratch}/consumer/consumer.nimble" <<'EOF'
version = "0.1.0"
author = "Logos"
description = "Consumer dependency resolution check"
license = "MIT or Apache License 2.0"
EOF
printf 'requires "https://github.com/logos-messaging/logos-delivery#%s"\n' \
  "${revision}" >> "${scratch}/consumer/consumer.nimble"

cd "${scratch}/consumer"
echo "Resolving logos-delivery as a consumer dependency with an empty Nimble cache"
set -- "${nimble}" setup -y --noLockfile "--nimbleDir:${scratch}/nimble"
if [ -n "${compiler}" ]; then
  set -- "$@" "--nim:${compiler}"
fi
"$@"
test -s nimble.paths
echo "Consumer dependency setup passed"

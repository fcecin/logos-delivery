#!/usr/bin/env bash
# Prints the directory holding the Nimble this project pins, for the Windows
# job's PATH. scripts/install_nimble.sh writes it under the Git Bash home and
# the MSYS2 HOME differs, so it is not on PATH there without this.
#
# <id> is RequiredNimbleRevision when logos_delivery.nimble defines it, else
# RequiredNimbleVersion, matching scripts/check_environment.sh.
set -eu

cd "$(dirname "${BASH_SOURCE[0]}")/.."

required_const() {
  grep -E "^const $1\s*=" logos_delivery.nimble | grep -oE '"[^"]+"' | tr -d '"'
}

nimble_id="$(required_const RequiredNimbleRevision)"
nimble_id="${nimble_id:-$(required_const RequiredNimbleVersion)}"
printf '%s/.local/nimble-%s/bin\n' "$(cygpath -u "${USERPROFILE}")" "${nimble_id}"

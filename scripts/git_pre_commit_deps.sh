#!/bin/sh
# Pre-commit hook. When the commit touches a dependency file, run the same
# preflight CI runs before `nimble setup`: URL requirements and nix/deps.nix
# against nimble.lock, then `nimble check`. Offline; about a second once the
# pinned Nimble is built.

files=$(git diff --cached --name-only --diff-filter=ACMR \
  | grep -E '^(logos_delivery\.nimble|nimble\.lock|nix/deps\.nix|scripts/audit_deps\.nims)$')
[ -z "$files" ] && exit 0

echo "Dependency files staged; running make preflight-deps"
make -s preflight-deps || {
  1>&2 echo "preflight failed; commit refused. Regenerate nimble.lock with 'nimble lock' and nix/deps.nix with tools/gen-nix-deps.sh, or fix the requirement."
  exit 1
}

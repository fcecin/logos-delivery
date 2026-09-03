#!/bin/sh
# Pre-commit hook. When the commit touches a dependency file, run the same
# preflight CI runs before `nimble setup` (`make preflight-deps`) on a
# snapshot of the index, so the check sees exactly what is being committed.
# Offline; about a second once the pinned Nimble is built.

# --no-renames: a monitored file renamed away must show as its own deletion.
files=$(git diff --cached --name-only --no-renames --diff-filter=ACMD \
  | grep -E '^(logos_delivery\.nimble|nimble\.lock|nix/deps\.nix|scripts/audit_deps\.nims)$')
[ -z "$files" ] && exit 0

echo "Dependency files staged; running make preflight-deps on the index"
snap=$(mktemp -d) || exit 1
trap 'rm -rf "$snap"' EXIT
git checkout-index --prefix="$snap/" -a || exit 1
if ! (cd "$snap" && make -s preflight-deps); then
  1>&2 echo "preflight failed; commit refused. Regenerate nimble.lock with 'nimble lock' and nix/deps.nix with tools/gen-nix-deps.sh, or fix the requirement."
  exit 1
fi

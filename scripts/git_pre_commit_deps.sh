#!/bin/sh
# Pre-commit hook, a convenience only; CI is the enforcement. When the commit
# touches a dependency file and the dependencies are installed, run
# `make audit-deps` on the working tree: Nimble's own offline verdict on the
# lock, nix/deps.nix against the lock, installed packages against the lock.
# Install by copying this file to "$(git rev-parse --git-path hooks/pre-commit)".

files=$(git diff --cached --name-only --no-renames --diff-filter=ACMDT \
  | grep -E '^(logos_delivery\.nimble|nimble\.lock|nix/deps\.nix|scripts/audit_deps\.nims)$')
[ -z "$files" ] && exit 0
if [ ! -d nimbledeps/pkgs2 ]; then
  echo "dependency files staged, but nimbledeps is not built; skipping the audit (run make build-deps)"
  exit 0
fi
echo "Dependency files staged; running make audit-deps"
make -s audit-deps || {
  1>&2 echo "audit failed; commit refused. Regenerate nimble.lock with 'nimble lock' and nix/deps.nix with tools/gen-nix-deps.sh, or fix the requirement."
  exit 1
}

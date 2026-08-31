#!/usr/bin/env bash
# Makes nimbledeps/ current for the declared inputs, then audits it.
#
# Usage (from make): scripts/ensure_nimbledeps.sh
#
# Copies of the inputs that produced the current nimbledeps/ are kept in
# nimbledeps/.inputs. When every copy matches byte for byte, only the audit
# runs. Otherwise nimbledeps/ and nimble.paths are removed, Nimble installs
# from empty, the audit runs, and the copies are written last. This is the
# same rule as the CI cache key in .github/actions/nimble-deps.

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${ROOT}"

MAKE_CMD="${MAKE:-make}"
INPUTS="requires.generated nimble.lock logos_delivery.nimble BearSSL.mk Nat.mk"
RECEIPTS="nimbledeps/.inputs"

# NIMBLE_DIR would redirect Nimble away from nimbledeps/.
unset NIMBLE_DIR

# Both write fixed paths in this worktree.
"${MAKE_CMD}" --no-print-directory logos_delivery.nims requires.generated

current=1
for f in ${INPUTS}; do
  if ! cmp -s "${f}" "${RECEIPTS}/${f}"; then
    current=0
    break
  fi
done

if [ "${current}" -eq 1 ]; then
  "${MAKE_CMD}" --no-print-directory audit-deps
  exit 0
fi

echo "nimbledeps: inputs changed, discarding nimbledeps/"
rm -rf nimbledeps nimble.paths
mkdir -p nimbledeps

# --useSystemNim uses the Nim compiler on PATH; --disableNimBinaries stops
# Nimble from installing a Nim of its own when the compiler differs from
# the requirement.
# Nimble accepts the generated value only in attached --requires:<value> form;
# a separate argument leaves the constraints unapplied.
if ! nimble setup --localdeps -y --useSystemNim --disableNimBinaries --requires:"$(cat requires.generated)"; then
  echo "nimbledeps: dependency setup failed" >&2
  exit 1
fi

"${MAKE_CMD}" --no-print-directory audit-deps

mkdir -p "${RECEIPTS}"
cp ${INPUTS} "${RECEIPTS}/"

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
#
# The lock, kept outside nimbledeps/, serializes generation replacement:
# without it two Make processes with changed inputs would both remove the
# tree, one of them while the other installs into it. It is released before
# builds run, so two independent Make processes in one worktree remain
# unsupported.

set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${ROOT}"

MAKE_CMD="${MAKE:-make}"
INPUTS="requires.generated nimble.lock logos_delivery.nimble BearSSL.mk Nat.mk"
RECEIPTS="nimbledeps/.inputs"
STAMP="nimbledeps/.nimble-setup"
LOCK_DIR="${ROOT}/.nimbledeps-setup.lock"
OWNER_FILE="${LOCK_DIR}/owner"
WAIT_SECONDS=600

release_lock() {
  if [ -f "${OWNER_FILE}" ] && [ "$(sed -n '1p' "${OWNER_FILE}")" = "$$" ]; then
    rm -f "${OWNER_FILE}"
    rmdir "${LOCK_DIR}" 2>/dev/null || true
  fi
}

waited=0
while ! mkdir "${LOCK_DIR}" 2>/dev/null; do
  owner=""
  if [ -f "${OWNER_FILE}" ]; then
    owner=$(sed -n '1p' "${OWNER_FILE}")
  fi
  # A killed run can leave its lock directory behind. Recover it only when
  # the recorded process is gone.
  if [[ "${owner}" =~ ^[0-9]+$ ]] && ! kill -0 "${owner}" 2>/dev/null &&
      [ "$(sed -n '1p' "${OWNER_FILE}" 2>/dev/null || true)" = "${owner}" ]; then
    rm -f "${OWNER_FILE}"
    rmdir "${LOCK_DIR}" 2>/dev/null || true
    continue
  fi
  if [ "${waited}" -eq 0 ]; then
    echo "nimbledeps: another dependency setup holds ${LOCK_DIR}; waiting"
  elif [ "${waited}" -ge "${WAIT_SECONDS}" ]; then
    echo "nimbledeps: timed out waiting for ${LOCK_DIR}" >&2
    exit 1
  fi
  sleep 1
  waited=$((waited + 1))
done
printf '%s\n' "$$" > "${OWNER_FILE}"
trap release_lock EXIT
trap 'exit 1' HUP INT TERM

# NIMBLE_DIR would redirect Nimble away from nimbledeps/.
unset NIMBLE_DIR

# Generated under the lock: both write fixed paths in this worktree.
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
if ! nimble setup --localdeps -y --useSystemNim --disableNimBinaries --requires "$(cat requires.generated)"; then
  echo "nimbledeps: dependency setup failed. If this began after a Nimble upgrade, move ~/.nimble/pkgcache aside and retry." >&2
  exit 1
fi

"${MAKE_CMD}" --no-print-directory audit-deps

mkdir -p "${RECEIPTS}"
cp ${INPUTS} "${RECEIPTS}/"
touch "${STAMP}"

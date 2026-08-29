#!/usr/bin/env bash
# Makes nimbledeps/ current for the declared inputs, then audits it.
#
# Usage (from make): scripts/ensure_nimbledeps.sh
#
# Copies of the inputs that produced the current nimbledeps/ are kept in
# nimbledeps/.inputs, the same rule as the CI cache key in
# .github/actions/nimble-deps.
#
# A stamped generation whose copies match is audited and reused. Otherwise an
# audited generation moves to .nimbledeps-prev/, uncommitted output is
# discarded, and Nimble installs from empty. The audit, the copies and then
# the stamp commit the replacement; only then is the previous generation
# removed. A normal failure restores it. After an untrappable kill the next
# run uses the stamp to keep the committed replacement or restore the
# previous one.
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
PREV=".nimbledeps-prev"
QUARANTINED=0
LOCK_DIR="${ROOT}/.nimbledeps-setup.lock"
OWNER_FILE="${LOCK_DIR}/owner"
WAIT_SECONDS=600

release_lock() {
  if [ -f "${OWNER_FILE}" ] && [ "$(sed -n '1p' "${OWNER_FILE}")" = "$$" ]; then
    rm -f "${OWNER_FILE}"
    rmdir "${LOCK_DIR}" 2>/dev/null || true
  fi
}

# A generation is proven only when the audit passed and the copies committed.
has_proven_generation() {
  [ -f nimble.paths ] && [ -f "${STAMP}" ] || return 1
  for f in ${INPUTS}; do
    [ -f "${RECEIPTS}/${f}" ] || return 1
  done
}

receipts_match_current() {
  for f in ${INPUTS}; do
    cmp -s "${f}" "${RECEIPTS}/${f}" || return 1
  done
}

# Each half is restored only if it was moved: a run killed between the two
# moves left the other half in place, and moving it back would lose it.
restore_prev() {
  if [ -d "${PREV}/nimbledeps" ]; then
    rm -rf nimbledeps
    mv "${PREV}/nimbledeps" nimbledeps
  fi
  if [ -f "${PREV}/nimble.paths" ]; then
    rm -f nimble.paths
    mv "${PREV}/nimble.paths" nimble.paths
  fi
  rmdir "${PREV}" 2>/dev/null || rm -rf "${PREV}"
}

cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ "${QUARANTINED}" -eq 1 ] && [ -d "${PREV}" ]; then
    restore_prev
    echo "nimbledeps: replacement failed, kept the previous generation" >&2
  fi
  release_lock
  exit "${status}"
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
trap cleanup EXIT
trap 'exit 1' HUP INT TERM

# NIMBLE_DIR would redirect Nimble away from nimbledeps/.
unset NIMBLE_DIR

# Recovered before the generation step, which needs the network: a failure
# there must not leave the previous generation in .nimbledeps-prev/.
if [ -d "${PREV}" ]; then
  if has_proven_generation && receipts_match_current; then
    rm -rf "${PREV}"
  else
    restore_prev
    echo "nimbledeps: restored the previous generation" >&2
  fi
fi

# Generated under the lock: both write fixed paths in this worktree.
"${MAKE_CMD}" --no-print-directory logos_delivery.nims requires.generated

if has_proven_generation && receipts_match_current; then
  "${MAKE_CMD}" --no-print-directory audit-deps
  exit 0
fi

if has_proven_generation; then
  echo "nimbledeps: inputs changed, quarantining nimbledeps/"
  # Armed before the first move: a signal between the two must still roll back.
  QUARANTINED=1
  mkdir "${PREV}"
  mv nimbledeps "${PREV}/nimbledeps"
  mv nimble.paths "${PREV}/nimble.paths"
else
  echo "nimbledeps: inputs changed, discarding nimbledeps/ (nothing audited to keep)"
  rm -rf nimbledeps nimble.paths
fi
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

QUARANTINED=0
rm -rf "${PREV}"

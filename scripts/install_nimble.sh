#!/usr/bin/env bash
# Build the pinned Nimble from source into ~/.local/nimble-<pin>/bin/nimble.
# <pin> is a git revision (40 hex digits) or a release version such as
# 0.24.1, built from tag v<version>. The directory is outside ~/.nimble/bin,
# which `nimble setup` can rewrite. An executable that reports the pin is
# reused.

set -e

PIN="${1:-}"
if [ -z "${PIN}" ]; then
  echo "Usage: $0 <nimble-revision-or-version>" >&2
  exit 1
fi

NIMBLE_DIR="${HOME}/.local/nimble-${PIN}/bin"
NIMBLE_BIN="${NIMBLE_DIR}/nimble"

if [[ "${PIN}" =~ ^[0-9a-f]{40}$ ]]; then
  REF="${PIN}"
  want_line() { grep -oE '[0-9a-f]{40}' | head -1; }
else
  REF="refs/tags/v${PIN#v}"
  want_line() { head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1; }
fi
WANT="${PIN#v}"

if [ -x "${NIMBLE_BIN}" ]; then
  have=$("${NIMBLE_BIN}" --version 2>/dev/null | want_line || true)
  if [ "${have}" = "${WANT}" ]; then
    echo "Nimble ${PIN} already installed, skipping."
    exit 0
  fi
fi

mkdir -p "${NIMBLE_DIR}"

# Clone the ref; build with the Nim on PATH.
NIM_BIN="$(command -v nim)"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "Cloning nimble ${REF} with submodules..."
git clone --depth=1 --no-checkout https://github.com/nim-lang/nimble.git \
  "${WORK_DIR}/nimble"
git -C "${WORK_DIR}/nimble" fetch --depth=1 origin "${REF}"
git -C "${WORK_DIR}/nimble" checkout --detach FETCH_HEAD
git -C "${WORK_DIR}/nimble" submodule update --init --recursive --depth=1

echo "Building nimble ${PIN} with $("${NIM_BIN}" --version | head -1)..."
cd "${WORK_DIR}/nimble"
# nim.cfg and config.nims in the clone add the vendored module paths. A
# private --nimcache keeps two runs from clobbering each other.
"${NIM_BIN}" c -d:release --path:src --nimcache:"${WORK_DIR}/nimcache" \
  -o:"${WORK_DIR}/nimble_new" src/nimble.nim

# Rename over the target; never write it in place.
cp "${WORK_DIR}/nimble_new" "${NIMBLE_BIN}.new.$$"
mv -f "${NIMBLE_BIN}.new.$$" "${NIMBLE_BIN}"

echo "Nimble ${PIN} installed to ${NIMBLE_BIN}"

#!/usr/bin/env bash
# Build the pinned Nimble from source into <dir>/nimble, by default
# ~/.local/nimble-<pin>/bin. <pin> is a git revision (40 hex digits) or a
# release version such as 0.25.0, built from tag v<version>. The directory is
# outside ~/.nimble/bin, which `nimble setup` can rewrite. An executable that
# reports the pin is reused.
#
# make passes $(NIMBLE_TOOLDIR) as <dir>, so the binary lands where make
# invokes it. On the Windows CI runners make is a native build whose HOME is
# the Windows profile, while this script runs in an MSYS2 shell whose HOME
# is the MSYS2 home; a default derived here would miss make's directory.

set -e

PIN="${1:-}"
if [ -z "${PIN}" ]; then
  echo "Usage: $0 <nimble-revision-or-version> [install-dir]" >&2
  exit 1
fi

NIMBLE_DIR="${2:-${HOME}/.local/nimble-${PIN}/bin}"
# A Windows path from a native make, such as C:\Users\me/.local/..., becomes
# a path this shell can create and test.
if command -v cygpath >/dev/null 2>&1; then
  NIMBLE_DIR="$(cygpath -u "${NIMBLE_DIR}")"
fi
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

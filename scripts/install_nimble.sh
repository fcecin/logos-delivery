#!/usr/bin/env bash
# Installs a specific nimble version without using `nimble install nimble`.
#
# Install the selected executable under:
#
#   ~/.local/nimble-<version>/bin
#
# The Makefile places this directory before ~/.nimble/bin on PATH. Nimble
# may update package links under ~/.nimble/bin during setup, including the
# `nimble` link when Nimble is installed as a package. Installing the
# selected executable outside that directory avoids writing through that
# link.
#
# Procedure:
#   1. Reuse an executable already reporting the requested version.
#   2. On a recognized platform, try the version-specific GitHub release
#      asset.
#   3. If download, extraction, or execution fails, build the requested
#      tag from source with the Nim compiler on PATH.

set -e

NIMBLE_VERSION="${1:-}"
if [ -z "${NIMBLE_VERSION}" ]; then
  echo "Usage: $0 <nimble-version>" >&2
  exit 1
fi

NIMBLE_DIR="${HOME}/.local/nimble-${NIMBLE_VERSION}/bin"
NIMBLE_BIN="${NIMBLE_DIR}/nimble"

# Step 1: reuse the executable if it reports the requested version.
if [ -x "${NIMBLE_BIN}" ]; then
  nimble_ver=$("${NIMBLE_BIN}" --version 2>/dev/null \
    | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
  if [ "${nimble_ver}" = "${NIMBLE_VERSION}" ]; then
    echo "Nimble ${NIMBLE_VERSION} already installed, skipping."
    exit 0
  fi
fi

mkdir -p "${NIMBLE_DIR}"

# Step 2: try the version-specific prebuilt release asset.
#
# The URL identifies a release under github.com/nim-lang/nimble. After
# extraction, the script checks that the binary can execute --version.
# It does not independently verify an archive checksum. A failed
# download, extraction, or execution falls through to the source build.
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)  ASSET="linux_x64" ;;
  Linux-aarch64) ASSET="linux_aarch64" ;;
  Darwin-arm64)  ASSET="macosx_aarch64" ;;
  Darwin-x86_64) ASSET="macosx_x64" ;;
  MINGW*-x86_64|MSYS*-x86_64) ASSET="windows_x64" ;;
  *)             ASSET="" ;;
esac
if [ -n "${ASSET}" ]; then
  URL="https://github.com/nim-lang/nimble/releases/download/v${NIMBLE_VERSION}/nimble-${ASSET}.tar.gz"
  echo "Downloading prebuilt nimble ${NIMBLE_VERSION} (${ASSET})..."
  if curl -fsSL "${URL}" | tar -xz -C "${NIMBLE_DIR}"; then
    if "${NIMBLE_BIN}" --version >/dev/null 2>&1; then
      "${NIMBLE_BIN}" --version | head -1
      echo "Nimble ${NIMBLE_VERSION} installed to ${NIMBLE_BIN}"
      exit 0
    fi
    echo "Prebuilt binary does not run, falling back to source build." >&2
  else
    echo "Prebuilt download failed, falling back to source build." >&2
  fi
fi

# Step 3: clone the requested version tag and build it with the Nim
# compiler resolved from PATH.
NIM_BIN="$(command -v nim)"

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "${WORK_DIR}"' EXIT

echo "Cloning nimble v${NIMBLE_VERSION} with submodules..."
git clone --depth=1 --branch "v${NIMBLE_VERSION}" \
  --recurse-submodules --shallow-submodules \
  https://github.com/nim-lang/nimble.git \
  "${WORK_DIR}/nimble"

echo "Building nimble ${NIMBLE_VERSION} with $("${NIM_BIN}" --version | head -1)..."
cd "${WORK_DIR}/nimble"
# Nim reads nim.cfg and config.nims from the current directory; these
# files add the vendored module paths used by the build.
"${NIM_BIN}" c -d:release --path:src \
  -o:"${WORK_DIR}/nimble_new" src/nimble.nim

# Stage the executable under a separate pathname, then rename it over
# the target. This avoids writing the target executable in place.
cp "${WORK_DIR}/nimble_new" "${NIMBLE_BIN}.new.$$"
mv -f "${NIMBLE_BIN}.new.$$" "${NIMBLE_BIN}"

echo "Nimble ${NIMBLE_VERSION} installed to ${NIMBLE_BIN}"

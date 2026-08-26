#!/usr/bin/env bash
# Installs a specific nimble version into its own versioned directory,
# ~/.local/nimble-<version>/bin, which the Makefile puts first on PATH.
#
# Why not ~/.nimble/bin: `nimble install nimble` is inherently fragile
# (ETXTBSY overwriting the running binary, JSON parse failures across
# versions), and worse, `nimble setup` re-links installed packages' binary
# shims — nimble ships itself as a package, so setup deletes
# ~/.nimble/bin/nimble (even while it is the running executable) and points
# it back at whatever nimble version sits in pkgs2. A versioned directory
# that nimble never manages is immune to all of that.
#
# Strategy:
#   1. If the right version is already in the versioned dir → done.
#   2. Download the official prebuilt release binary for this platform.
#   3. Fallback: clone the nimble git repo and build from source with nim.

set -e

NIMBLE_VERSION="${1:-}"
if [ -z "${NIMBLE_VERSION}" ]; then
  echo "Usage: $0 <nimble-version>" >&2
  exit 1
fi

NIMBLE_DIR="${HOME}/.local/nimble-${NIMBLE_VERSION}/bin"
NIMBLE_BIN="${NIMBLE_DIR}/nimble"

# 1. Already installed at the right version?
if [ -x "${NIMBLE_BIN}" ]; then
  nimble_ver=$("${NIMBLE_BIN}" --version 2>/dev/null \
    | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
  if [ "${nimble_ver}" = "${NIMBLE_VERSION}" ]; then
    echo "Nimble ${NIMBLE_VERSION} already installed, skipping."
    exit 0
  fi
fi

mkdir -p "${NIMBLE_DIR}"

# 2. Prebuilt release binary.
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
    "${NIMBLE_BIN}" --version | head -1
    echo "Nimble ${NIMBLE_VERSION} installed to ${NIMBLE_BIN}"
    exit 0
  fi
  echo "Prebuilt download failed, falling back to source build." >&2
fi

# 3. Build from source.
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
# nim reads nim.cfg / config.nims in the current dir, which sets vendor paths.
"${NIM_BIN}" c -d:release --path:src \
  -o:"${WORK_DIR}/nimble_new" src/nimble.nim

# Atomic rename: avoids ETXTBSY if the old binary at NIMBLE_BIN is running.
cp "${WORK_DIR}/nimble_new" "${NIMBLE_BIN}.new.$$"
mv -f "${NIMBLE_BIN}.new.$$" "${NIMBLE_BIN}"

echo "Nimble ${NIMBLE_VERSION} installed to ${NIMBLE_BIN}"

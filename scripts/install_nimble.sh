#!/usr/bin/env bash
# Installs a specific nimble version without using `nimble install nimble`.
#
# This script installs one Nimble version into its own directory:
# ~/.local/nimble-<version>/bin. The Makefile puts that directory first
# on PATH.
#
# The script does not install into ~/.nimble/bin. Nimble manages that
# directory. During setup, Nimble refreshes the links for installed
# packages. Nimble can be installed as a package itself. In that case,
# the refresh can replace ~/.nimble/bin/nimble with a link to the
# packaged copy, also when that file is the binary that runs. A
# directory that Nimble does not manage does not have this risk.
#
# Steps:
#   1. If the requested version is already installed, stop.
#   2. Download the official prebuilt release binary (best effort).
#   3. If the download does not succeed, build Nimble from source.

set -e

NIMBLE_VERSION="${1:-}"
if [ -z "${NIMBLE_VERSION}" ]; then
  echo "Usage: $0 <nimble-version>" >&2
  exit 1
fi

NIMBLE_DIR="${HOME}/.local/nimble-${NIMBLE_VERSION}/bin"
NIMBLE_BIN="${NIMBLE_DIR}/nimble"

# Step 1: stop if the requested version is already installed.
if [ -x "${NIMBLE_BIN}" ]; then
  nimble_ver=$("${NIMBLE_BIN}" --version 2>/dev/null \
    | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
  if [ "${nimble_ver}" = "${NIMBLE_VERSION}" ]; then
    echo "Nimble ${NIMBLE_VERSION} already installed, skipping."
    exit 0
  fi
fi

mkdir -p "${NIMBLE_DIR}"

# Step 2: download the official prebuilt release binary.
# This step is a best-effort optimization. It saves the time of a source
# build, which is some minutes for each CI job. The step is safe if it
# decays: if the asset name changes, if the platform is not in the table,
# or if the download does not succeed, the script continues to the source
# build in step 3. This step cannot make the result incorrect. It can
# only lose its speed advantage.
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

# Step 3: build Nimble from source with the Nim compiler on PATH.
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
# Nim reads nim.cfg and config.nims from the current directory. These
# files set the vendor paths for the build.
"${NIM_BIN}" c -d:release --path:src \
  -o:"${WORK_DIR}/nimble_new" src/nimble.nim

# Copy first, then rename in one operation. This prevents an ETXTBSY
# error if the old binary runs at this time.
cp "${WORK_DIR}/nimble_new" "${NIMBLE_BIN}.new.$$"
mv -f "${NIMBLE_BIN}.new.$$" "${NIMBLE_BIN}"

echo "Nimble ${NIMBLE_VERSION} installed to ${NIMBLE_BIN}"

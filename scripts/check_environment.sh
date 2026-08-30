#!/usr/bin/env bash
# Prints the Nim and Nimble that make uses and warns when a `nim` or
# `nimble` command typed in a shell is a different one. Changes nothing.
#
# Usage: scripts/check_environment.sh [shell-path]
#
# shell-path is the PATH that shell commands resolve through; make passes
# the PATH from before the Makefile changed it. Default: the current PATH.
#
# Required versions: RequiredNimVersion and RequiredNimbleVersion in
# logos_delivery.nimble. make keeps its Nimble in ~/.local/nimble-<id>/bin,
# written only by scripts/install_nimble.sh; <id> is RequiredNimbleRevision
# when logos_delivery.nimble defines it, else RequiredNimbleVersion.
set -u

required_const() {
  grep -E "^const $1\s*=" logos_delivery.nimble | grep -oE '"[^"]+"' | tr -d '"'
}

nim_version_of() {
  "$1" --version 2>/dev/null | grep -oE 'Version [0-9]+\.[0-9]+\.[0-9]+' | head -1 | cut -d' ' -f2
}
# Nimble can print an Info line before "nimble vX.Y.Z". A source-built
# Nimble also prints "git hash: <sha>"; a release binary does not.
# Probed without NIMBLE_DIR, the environment make guarantees for Nimble.
nimble_version_of() {
  env -u NIMBLE_DIR "$1" --version 2>/dev/null | grep -oE 'nimble v[0-9]+\.[0-9]+\.[0-9]+' | head -1 | cut -c9-
}
nimble_hash_of() {
  env -u NIMBLE_DIR "$1" --version 2>/dev/null | grep -oE 'git hash: [0-9a-f]{40}' | head -1 | cut -c11-
}

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 2
if [ ! -f logos_delivery.nimble ]; then
  echo "check_environment: logos_delivery.nimble not found in $(pwd)" >&2
  exit 2
fi

NIM_VERSION="$(required_const RequiredNimVersion)"
NIMBLE_VERSION="$(required_const RequiredNimbleVersion)"
NIMBLE_REVISION="$(required_const RequiredNimbleRevision)"
nimble_id="${NIMBLE_REVISION:-${NIMBLE_VERSION}}"
project_nimble_dir="${HOME}/.local/nimble-${nimble_id}/bin"
shell_path="${1:-${PATH}}"

# The Makefile's PATH order: the project's Nimble directory, then
# ~/.nimble/bin, then the rest.
lookup_path="${project_nimble_dir}:${HOME}/.nimble/bin:${PATH}"
nim_bin="$(PATH="${lookup_path}" command -v nim || true)"
nimble_bin="$(PATH="${lookup_path}" command -v nimble || true)"
nim_ver="$([ -n "${nim_bin}" ] && nim_version_of "${nim_bin}")"
nimble_ver="$([ -n "${nimble_bin}" ] && nimble_version_of "${nimble_bin}")"
nimble_hash="$([ -n "${nimble_bin}" ] && nimble_hash_of "${nimble_bin}")"

echo "toolchain: nim ${nim_ver:-missing} (${nim_bin:-not found}), nimble ${nimble_ver:-missing}${nimble_hash:+ git ${nimble_hash:0:8}} (${nimble_bin:-not found})"
echo "toolchain: make uses the nimble in ${project_nimble_dir} (scripts/install_nimble.sh). To use it in a shell: export PATH=\"\$HOME/.local/nimble-${nimble_id}/bin:\$PATH\""

status=0
warned=0

if [ "${nim_ver}" != "${NIM_VERSION}" ]; then
  echo "toolchain: ERROR: the project requires nim ${NIM_VERSION}; make would use ${nim_ver:-none}. Run 'make install-nim'." >&2
  status=1
fi
if [ "${nimble_ver}" != "${NIMBLE_VERSION}" ]; then
  echo "toolchain: ERROR: the project requires nimble ${NIMBLE_VERSION}; make would use ${nimble_ver:-none}. Run 'make install-nimble'." >&2
  status=1
elif [ -n "${NIMBLE_REVISION}" ] && [ "${nimble_hash}" != "${NIMBLE_REVISION}" ]; then
  echo "toolchain: ERROR: the project requires nimble built from ${NIMBLE_REVISION:0:8}; make would use ${nimble_hash:-a release binary}. Run 'make install-nimble'." >&2
  status=1
fi

shell_nim="$(PATH="${shell_path}" command -v nim || true)"
if [ -n "${shell_nim}" ] && [ "${shell_nim}" != "${nim_bin}" ]; then
  shell_nim_ver="$(nim_version_of "${shell_nim}")"
  if [ "${shell_nim_ver}" != "${NIM_VERSION}" ]; then
    echo "toolchain: WARNING: a shell 'nim' command runs nim ${shell_nim_ver:-unknown} (${shell_nim}), not the ${NIM_VERSION} that make uses." >&2
    warned=1
  fi
fi

shell_nimble="$(PATH="${shell_path}" command -v nimble || true)"
if [ -n "${shell_nimble}" ] && [ "${shell_nimble}" != "${nimble_bin}" ]; then
  shell_ver="$(nimble_version_of "${shell_nimble}")"
  shell_hash="$(nimble_hash_of "${shell_nimble}")"
  if [ -n "${shell_hash}" ]; then
    shell_desc="${shell_ver:-unknown} git ${shell_hash:0:8}"
  else
    shell_desc="${shell_ver:-unknown}, a release binary"
  fi
  if [ "${shell_ver}" != "${NIMBLE_VERSION}" ] ||
      { [ -n "${NIMBLE_REVISION}" ] && [ "${shell_hash}" != "${NIMBLE_REVISION}" ]; }; then
    echo "toolchain: WARNING: a shell 'nimble' command runs nimble ${shell_desc} (${shell_nimble}), not the ${nimble_ver:-required}${nimble_hash:+ git ${nimble_hash:0:8}} that make uses. It writes to the shared ~/.nimble/pkgcache." >&2
    warned=1
  fi
fi

if [ "${status}" -ne 0 ]; then
  echo "toolchain: environment check failed" >&2
elif [ "${warned}" -ne 0 ]; then
  echo "toolchain: environment check passed with warnings"
else
  echo "toolchain: environment check passed"
fi
exit "${status}"

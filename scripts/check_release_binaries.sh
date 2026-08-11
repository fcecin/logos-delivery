#!/usr/bin/env bash
# usage: check_release_binaries.sh <arch> <dir> <name...>
# Each named file in <dir> must be a binary with the CPU type that
# <arch> (amd64 or arm64) names. The file command reads the bytes, thus
# a missing, zero-size, or truncated file fails, and so does a wrong
# CPU type. The pattern examines only the text after the ":" that the
# file command writes.
set -e
arch="$1"; dir="$2"; shift 2
case "$arch" in
  amd64) want='x86-64' ;;
  arm64) want='arm64|aarch64' ;;
  *) echo "FAIL: unknown arch: $arch"; exit 1 ;;
esac
rc=0
for b in "$@"; do
  line=$(file "$dir/$b" 2>/dev/null || true)
  echo "$line"
  echo "$line" | grep -E "executable|shared object|shared library" | grep -Eq ": .*($want)" \
    || { echo "FAIL: $b is missing, is not a binary, or has a wrong CPU type (want: $want)"; rc=1; }
done
exit $rc

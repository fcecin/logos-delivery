#!/usr/bin/env bash
# Controls for scripts/link_probe.sh: tiny fixtures that the probe must accept
# or reject, built with the same PROBE_CC / PROBE_FLAGS / PROBE_OS /
# PROBE_READELF the real probe steps use. The probe's job is to reject broken
# artifacts, and a green build of the real library cannot show that it does.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
probe=$here/link_probe.sh
read -r -a cc <<< "${PROBE_CC:-cc}"
flags=${PROBE_FLAGS:-}
os=${PROBE_OS:-}
if [ -z "$os" ]; then
  case "$(uname -s)" in
    Linux*) os=linux ;; Darwin*) os=darwin ;; MINGW*|MSYS*|CYGWIN*) os=windows ;;
    *) echo "selftest: set PROBE_OS" >&2; exit 2 ;;
  esac
fi
export PROBE_OS=$os

work=$(mktemp -d)
trap 'rc=$?; rm -rf "$work"; exit $rc' EXIT
cd "$work"

# expect pass|fail <name> [--grep <text>] -- <command...>
expect() {
  local want=$1 name=$2 needle=""; shift 2
  if [ "$1" = --grep ]; then needle=$2; shift 2; fi
  [ "$1" = -- ] && shift
  local out rc
  set +e; out=$("$@" 2>&1); rc=$?; set -e
  if { [ "$want" = pass ] && [ $rc -ne 0 ]; } || { [ "$want" = fail ] && [ $rc -eq 0 ]; }; then
    echo "selftest: FAILED: $name: expected $want, got exit $rc"; printf '%s\n' "$out" | tail -n 25; exit 1
  fi
  if [ -n "$needle" ] && ! printf '%s' "$out" | grep -q -- "$needle"; then
    echo "selftest: FAILED: $name: output lacks '$needle'"; printf '%s\n' "$out" | tail -n 25; exit 1
  fi
  echo "selftest: ok: $name ($want)"
}

printf 'int good(void) { return 1; }\n' > good.c
printf 'int missing_symbol(void); int bad(void) { return missing_symbol(); }\n' > bad.c
printf 'int ffi_rln_new(void); int rln(void) { return ffi_rln_new(); }\n' > rln.c
printf 'int dep(void) { return 2; }\n' > dep.c
printf 'int dep(void); int missing_symbol(void); int top(void) { return dep() + missing_symbol(); }\n' > top.c
for f in good bad rln dep top; do "${cc[@]}" $flags -fPIC -c $f.c -o $f.o; done

# Archives: the same on every platform.
ar rcs libarch_good.a good.o
ar rcs libarch_bad.a good.o bad.o
ar rcs libarch_rln.a good.o rln.o
expect pass "healthy archive" -- "$probe" static libarch_good.a
expect fail "archive with an unused member that has an unresolved symbol" -- "$probe" static libarch_bad.a

case "$os" in
  linux)
    "${cc[@]}" $flags -shared -o libgood.so good.o
    "${cc[@]}" $flags -shared -o libbad.so bad.o
    expect pass "healthy shared library" -- "$probe" shared libgood.so
    expect fail "shared library with an unresolved symbol" --grep missing_symbol -- "$probe" shared libbad.so
    cp libbad.so libsample.so.1; cp libgood.so libsample.so
    expect fail "the requested file, not a healthy sibling" -- "$probe" shared libsample.so.1
    "${cc[@]}" $flags -shared -Wl,-soname,libdep.so -o libdep.so dep.o
    "${cc[@]}" $flags -shared -o libtop.so top.o -L. -Wl,--no-as-needed -ldep
    expect fail "unresolved symbol in a library whose dependency is present" --grep missing_symbol -- "$probe" shared libtop.so
    mv libdep.so libdep.hidden
    expect fail "missing DT_NEEDED input is itself a failure" --grep "cannot resolve DT_NEEDED" -- "$probe" shared libtop.so
    ;;
  darwin)
    "${cc[@]}" $flags -dynamiclib -o libgood.dylib good.o
    expect pass "healthy dylib" -- "$probe" shared libgood.dylib
    allow=$here/link_probe_rln_exports.txt
    expect pass "archive whose only unresolved symbols are approved RLN externals" -- env PROBE_ALLOW_FILE="$allow" "$probe" static libarch_rln.a
    expect fail "same archive without the allow list" -- "$probe" static libarch_rln.a
    expect fail "allow list does not excuse other symbols" --grep missing_symbol -- env PROBE_ALLOW_FILE="$allow" "$probe" static libarch_bad.a
    ;;
  windows)
    "${cc[@]}" $flags -shared -o good.dll good.o
    expect pass "healthy DLL" -- "$probe" shared good.dll
    ;;
esac
expect fail "unknown mode is rejected" -- "$probe" bogus libarch_good.a
expect fail "unknown PROBE_OS is rejected" -- env PROBE_OS=plan9 "$probe" static libarch_good.a
echo "selftest: all controls behaved for $os"

#!/usr/bin/env bash
# Link a consumer against a built liblogosdelivery artifact with the target
# toolchain, so unresolved symbols fail here and not on a consumer's machine.
# Nothing is executed.
#
#   scripts/link_probe.sh shared <lib.so|.dylib|.dll> [extra linker args...]
#   scripts/link_probe.sh static <lib.a>               [extra linker args...]
#
# What a pass proves: the exact file links as a consumer links it, given the
# dependency inputs described here. It does not prove runtime loading or the
# presence of every export.
#
# shared: the file itself is a linker input, never a -l<name> search, so a
#   sibling file cannot stand in for it.
#   ELF: each DT_NEEDED entry of the file is resolved (the file's directory,
#   then the target toolchain through `cc -print-file-name`) and added as an
#   input, because lld reports a shared object's unresolved symbols only when
#   all of its needed libraries are among the inputs. An entry that cannot be
#   resolved fails the probe. --no-as-needed keeps the file on the link, and
#   --no-allow-shlib-undefined makes GNU ld report as well.
#   Mach-O and PE: the file links; both formats resolve imports when built.
# static: the whole archive is force-loaded (--whole-archive, -force_load), so
#   the unresolved symbols of every member must resolve. System libraries a
#   consumer adds come from the platform list below (PROBE_SYSLIBS replaces
#   it). On Mach-O, PROBE_ALLOW_FILE names the symbols a consumer supplies
#   itself, one per line; each becomes -Wl,-U,_<name>. Anything else fails.
#
# Environment:
#   PROBE_CC          compiler driver, may be several words; default cc
#   PROBE_FLAGS       target flags for compile and link (-arch, -isysroot,
#                     -m*-version-min); split on whitespace, no quoting inside
#   PROBE_OS          linux | darwin | windows; default from uname; iOS is darwin
#   PROBE_READELF     ELF: prints dynamic tags; default readelf (NDK: llvm-readelf)
#   PROBE_ALLOW_FILE  Mach-O static: allowed unresolved symbol names, '#' comments
#   PROBE_SYSLIBS     static: replaces the platform system-library list
#   PROBE_CONSUMER    directory holding liblogosdelivery.h: link
#                     scripts/link_probe_consumer.c, which references the public
#                     API, instead of an empty program
set -euo pipefail

usage() { sed -n '2,36p' "$0" >&2; exit 2; }
fail() { echo "link_probe: $*" >&2; exit 2; }

[ $# -ge 2 ] || usage
mode=$1; artifact=$2; shift 2
case "$mode" in shared|static) ;; *) fail "unknown mode: $mode" ;; esac
[ -f "$artifact" ] || fail "no such file: $artifact"

os=${PROBE_OS:-}
if [ -z "$os" ]; then
  case "$(uname -s)" in
    Linux*) os=linux ;;
    Darwin*) os=darwin ;;
    MINGW*|MSYS*|CYGWIN*) os=windows ;;
    *) fail "set PROBE_OS for $(uname -s)" ;;
  esac
fi
case "$os" in linux|darwin|windows) ;; *) fail "unknown PROBE_OS: $os" ;; esac

# PROBE_CC may be several words ("xcrun -sdk iphoneos clang"); split it.
read -r -a cc_cmd <<< "${PROBE_CC:-cc}"
flags=${PROBE_FLAGS:-}
here=$(cd "$(dirname "$0")" && pwd)
dir=$(cd "$(dirname "$artifact")" && pwd)
base=$(basename "$artifact")
artifact=$dir/$base

work=$(mktemp -d)
# Keep the real exit status across the cleanup: bash 3.2 (macOS) otherwise
# reports the status of the last command in the trap, and a script that dies
# under set -u would count as a pass.
trap 'rc=$?; rm -rf "$work"; exit $rc' EXIT

# Source: an empty program, or the consumer fixture against the public header.
src=$work/probe.c
cflags=()
if [ -n "${PROBE_CONSUMER:-}" ]; then
  [ -f "$PROBE_CONSUMER/liblogosdelivery.h" ] || fail "no liblogosdelivery.h in $PROBE_CONSUMER"
  src=$here/link_probe_consumer.c
  cflags=(-I"$PROBE_CONSUMER")
else
  printf 'int main(void) { return 0; }\n' > "$src"
fi
out=$work/probe
[ "$os" = windows ] && out=$work/probe.exe

# ELF shared: resolve the file's DT_NEEDED entries to concrete inputs.
resolve_needed() {
  local readelf=${PROBE_READELF:-readelf} tags name path
  tags=$("$readelf" -d "$artifact") || fail "$readelf could not read $artifact"
  needed=()
  while IFS= read -r name; do
    [ -n "$name" ] || continue
    case "$name" in
      ld-linux*|ld64.so*|ld-musl*|ld-android*) continue ;;  # the loader
    esac
    if [ -f "$dir/$name" ]; then
      path=$dir/$name
    else
      path=$("${cc_cmd[@]}" $flags -print-file-name="$name")
      [ "$path" != "$name" ] && [ -f "$path" ] || fail "cannot resolve DT_NEEDED $name of $base (searched $dir and the toolchain)"
    fi
    needed+=("$path")
  done < <(printf '%s\n' "$tags" | sed -n 's/.*(NEEDED)[^[]*\[\([^]]*\)\].*/\1/p')
  echo "link_probe: DT_NEEDED of $base resolved to: ${needed[*]:-}"
}

allow=()
if [ -n "${PROBE_ALLOW_FILE:-}" ]; then
  [ -f "$PROBE_ALLOW_FILE" ] || fail "no such allow file: $PROBE_ALLOW_FILE"
  while IFS= read -r line; do
    line=${line%%#*}; line=$(printf '%s' "$line" | tr -d '[:space:]')
    [ -n "$line" ] && allow+=("-Wl,-U,_$line")
  done < "$PROBE_ALLOW_FILE"
  echo "link_probe: ${#allow[@]} unresolved symbols allowed by $PROBE_ALLOW_FILE"
fi

syslibs=${PROBE_SYSLIBS-}
case "$mode:$os" in
  shared:linux)
    resolve_needed
    set -- "${cc_cmd[@]}" $flags ${cflags[@]+"${cflags[@]}"} -o "$out" "$src" \
      -Wl,--no-as-needed "$artifact" ${needed[@]+"${needed[@]}"} \
      -Wl,--no-allow-shlib-undefined -Wl,-rpath-link,"$dir" "$@" ;;
  shared:darwin|shared:windows)
    set -- "${cc_cmd[@]}" $flags ${cflags[@]+"${cflags[@]}"} -o "$out" "$src" "$artifact" "$@" ;;
  static:linux)
    [ -n "${PROBE_SYSLIBS+x}" ] || syslibs="-lstdc++ -fopenmp -lpthread -ldl -lm -lrt"
    set -- "${cc_cmd[@]}" $flags ${cflags[@]+"${cflags[@]}"} -o "$out" "$src" \
      -Wl,--whole-archive "$artifact" -Wl,--no-whole-archive "$@" $syslibs ;;
  static:darwin)
    [ -n "${PROBE_SYSLIBS+x}" ] || syslibs="-lc++ -lsqlite3 -lz -lresolv -framework Security -framework CoreFoundation -framework SystemConfiguration"
    set -- "${cc_cmd[@]}" $flags ${cflags[@]+"${cflags[@]}"} -o "$out" "$src" \
      -Wl,-force_load,"$artifact" "$@" ${allow[@]+"${allow[@]}"} $syslibs ;;
  static:windows)
    [ -n "${PROBE_SYSLIBS+x}" ] || syslibs="-lstdc++ -lws2_32 -liphlpapi -lbcrypt -lpthread"
    set -- "${cc_cmd[@]}" $flags ${cflags[@]+"${cflags[@]}"} -o "$out" "$src" \
      -Wl,--whole-archive "$artifact" -Wl,--no-whole-archive "$@" $syslibs ;;
esac

printf 'link_probe:'; printf ' %q' "$@"; printf '\n'
if "$@"; then
  echo "link_probe: OK, $mode $base links"
else
  echo "link_probe: FAILED, the probe link against $mode $base did not succeed (see the output above)" >&2
  exit 1
fi

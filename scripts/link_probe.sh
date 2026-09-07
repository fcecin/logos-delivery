#!/usr/bin/env bash
# Link an empty program against a built liblogosdelivery artifact. Every symbol
# the artifact leaves unresolved must then resolve at link time. Nothing runs.
#
#   scripts/link_probe.sh shared <liblogosdelivery.so|.dylib|.dll> [extra linker args...]
#   scripts/link_probe.sh static <liblogosdelivery.a>               [extra linker args...]
#
# shared: the library is linked as a dependency of the probe. When GNU ld links
#   an executable it reports unresolved symbols of the shared libraries it links
#   (--no-allow-shlib-undefined is its default there); --no-as-needed keeps the
#   library on the link line although the probe references nothing in it.
#   Mach-O dylibs and PE DLLs resolve everything when they are built, so for
#   them the probe proves that a consumer can link the file at all.
# static: the whole archive is force-loaded (--whole-archive on ELF,
#   -force_load on Mach-O), so the unresolved symbols of every member must
#   resolve, including members nothing in the probe references.
#
# Environment:
#   PROBE_CC     compiler driver; default cc. Cross builds pass their own, e.g.
#                the NDK clang or "xcrun -sdk iphoneos clang".
#   PROBE_FLAGS  flags for compile and link, e.g. -arch/-isysroot/-m*-version-min.
#   PROBE_OS     linux | darwin | windows; default from uname. iOS is darwin.
#   PROBE_ALLOW  extended regex of unresolved symbols a consumer supplies itself,
#                for example '^_ffi_' for librln. Mach-O only: each match becomes
#                -Wl,-U,<symbol>. On ELF pass the supplying archive as an extra
#                linker argument instead.
set -euo pipefail

usage() { sed -n '2,25p' "$0" >&2; exit 2; }
[ $# -ge 2 ] || usage
mode=$1; artifact=$2; shift 2
[ -f "$artifact" ] || { echo "link_probe: no such file: $artifact" >&2; exit 2; }

cc_cmd=${PROBE_CC:-cc}
flags=${PROBE_FLAGS:-}
os=${PROBE_OS:-}
if [ -z "$os" ]; then
  case "$(uname -s)" in
    Linux*) os=linux ;;
    Darwin*) os=darwin ;;
    MINGW*|MSYS*|CYGWIN*) os=windows ;;
    *) echo "link_probe: set PROBE_OS for $(uname -s)" >&2; exit 2 ;;
  esac
fi

work=$(mktemp -d)
# Keep the real exit status across the cleanup: bash 3.2 (macOS) otherwise
# reports the status of the last command in the trap, and a script that dies
# under set -u would count as a pass.
trap 'rc=$?; rm -rf "$work"; exit $rc' EXIT
printf 'int main(void) { return 0; }\n' > "$work/probe.c"
out="$work/probe"
[ "$os" = windows ] && out="$work/probe.exe"

dir=$(cd "$(dirname "$artifact")" && pwd)
base=$(basename "$artifact")

case "$mode" in
  shared)
    # lib<name>.so[.N] | lib<name>.dylib | lib<name>.dll  ->  <name>
    name=${base#lib}; name=${name%%.so*}; name=${name%.dylib}; name=${name%.dll}
    case "$os" in
      linux)
        set -- "$cc_cmd" $flags -o "$out" "$work/probe.c" \
          -L"$dir" -Wl,-rpath-link,"$dir" -Wl,--no-as-needed -l"$name" \
          -Wl,--no-allow-shlib-undefined "$@" ;;
      darwin|windows)
        set -- "$cc_cmd" $flags -o "$out" "$work/probe.c" -L"$dir" -l"$name" "$@" ;;
    esac ;;
  static)
    case "$os" in
      linux)
        set -- "$cc_cmd" $flags -o "$out" "$work/probe.c" \
          -Wl,--whole-archive "$artifact" -Wl,--no-whole-archive "$@" \
          -lstdc++ -fopenmp -lpthread -ldl -lm -lrt ;;
      darwin)
        allow=()
        if [ -n "${PROBE_ALLOW:-}" ]; then
          while IFS= read -r sym; do
            [ -n "$sym" ] && allow+=("-Wl,-U,$sym")
          done < <(nm -u "$artifact" 2>/dev/null | awk '{print $NF}' | sort -u | grep -E "$PROBE_ALLOW" || true)
          echo "link_probe: allowing ${#allow[@]} unresolved symbols matching $PROBE_ALLOW"
        fi
        # ${allow[@]+"${allow[@]}"}: an empty array is "unbound" to bash 3.2,
        # which is what macOS runners execute this with.
        set -- "$cc_cmd" $flags -o "$out" "$work/probe.c" \
          -Wl,-force_load,"$artifact" "$@" ${allow[@]+"${allow[@]}"} \
          -lc++ -lsqlite3 -lz -lresolv \
          -framework Security -framework CoreFoundation -framework SystemConfiguration ;;
      windows)
        set -- "$cc_cmd" $flags -o "$out" "$work/probe.c" \
          -Wl,--whole-archive "$artifact" -Wl,--no-whole-archive "$@" \
          -lstdc++ -lws2_32 -liphlpapi -lbcrypt -lpthread ;;
    esac ;;
  *) usage ;;
esac

echo "link_probe: $*"
if "$@"; then
  echo "link_probe: OK, $mode $base links"
else
  echo "link_probe: FAILED, $mode $base leaves symbols unresolved (see the linker output above)" >&2
  exit 1
fi

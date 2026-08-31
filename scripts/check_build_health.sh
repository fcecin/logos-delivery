#!/usr/bin/env bash
# Assert that build configuration reaches the compiler.
#
# Run from anywhere:
#
#   scripts/check_build_health.sh
#
# Every case here asserts a behaviour, not a file's contents: a caller sets a
# knob and the resulting Nim command line must contain what the knob promises.
# The Make variable database answers that without compiling, so this needs no
# Nim, no Nimble, no dependencies and no network.
#
# Each case exists because that exact plumbing was found broken. A value
# crossing a boundary is a string being concatenated or forwarded, so a dropped
# value looks identical to an empty one and nothing can fail. Every defect this
# file covers was silent and exited 0.
#
# A failing case names the knob and prints the expected and actual flags.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

pass=0
fail=0

ok() {
  printf '  PASS  %s\n' "$1"
  pass=$((pass + 1))
}

no() {
  printf '  FAIL  %s\n' "$1"
  shift
  local line
  for line in "$@"; do
    printf '          %s\n' "${line}"
  done
  fail=$((fail + 1))
}

# The fully resolved NIM_PARAMS for a given set of make variables.
nim_params() {
  make -pn "$@" 2>/dev/null | grep -E '^NIM_PARAMS :?=' | head -1
}

# The resolved value of any variable, without the name or surrounding space.
value_of() {
  local name=$1
  shift
  make -pn "$@" 2>/dev/null \
    | grep -E "^${name} :?=" | head -1 \
    | sed -E "s/^${name} :?=[[:space:]]*//; s/[[:space:]]+\$//"
}

# What a recipe actually sees in its environment. The Nimble tasks read
# NIM_PARAMS with getEnv, so only an exported value reaches them, and the make
# database does not report export directives.
exported_value() {
  local name=$1
  shift
  printf 'include Makefile\n_health_probe:\n\t@echo "$$%s"\n' "${name}" \
    | make -s -f - "$@" _health_probe 2>/dev/null
}

# name, needle, make args...
expect_flag() {
  local name=$1 needle=$2
  shift 2
  local got
  got=$(nim_params "$@")
  case "${got}" in
    *"${needle}"*) ok "${name}" ;;
    *) no "${name}" "expected to contain: ${needle}" "actual: ${got:-<no NIM_PARAMS>}" ;;
  esac
}

# name, needle, make args...
reject_flag() {
  local name=$1 needle=$2
  shift 2
  local got
  got=$(nim_params "$@")
  case "${got}" in
    *"${needle}"*) no "${name}" "expected NOT to contain: ${needle}" "actual: ${got}" ;;
    *) ok "${name}" ;;
  esac
}

# name, expected, actual
expect_eq() {
  if [ "$2" = "$3" ]; then
    ok "$1"
  else
    no "$1" "expected: $2" "actual:   $3"
  fi
}

echo "build health"
echo

# --------------------------------------------------------------------------
# NIMFLAGS is the documented public input. It was consumed by nothing between
# the removal of the vendored build system and its repair, so everything the
# README, the workflows, the Jenkins jobs and the Dockerfiles passed was
# discarded.
# --------------------------------------------------------------------------
expect_flag "NIMFLAGS reaches the compiler" \
  "-d:health_sentinel" NIMFLAGS=-d:health_sentinel

# A caller that names a flag expects it to take effect, so NIMFLAGS has to be
# applied after the project's own defaults: Nim keeps the last definition.
ordering=$(nim_params NIMFLAGS=-d:health_sentinel)
if [ -z "${ordering}" ]; then
  no "NIMFLAGS wins over project defaults" "NIM_PARAMS did not resolve"
elif [ "${ordering%%-d:health_sentinel*}" = "${ordering}" ]; then
  no "NIMFLAGS wins over project defaults" "sentinel absent: ${ordering}"
else
  before=${ordering%%-d:health_sentinel*}
  case "${before}" in
    *git_version*) ok "NIMFLAGS wins over project defaults" ;;
    *) no "NIMFLAGS wins over project defaults" \
         "the caller's flag must come after the project defaults" \
         "actual: ${ordering}" ;;
  esac
fi

# NIM_PARAMS is private. Reaching past NIMFLAGS to set it from the environment
# must not work, or the public input can be bypassed silently.
env_bypass=$(NIM_PARAMS=-d:should_be_ignored nim_params NIMFLAGS=-d:health_sentinel)
case "${env_bypass}" in
  *should_be_ignored*) no "ambient NIM_PARAMS cannot bypass NIMFLAGS" \
                          "an environment NIM_PARAMS leaked into the build" \
                          "actual: ${env_bypass}" ;;
  *) ok "ambient NIM_PARAMS cannot bypass NIMFLAGS" ;;
esac

# The Nimble tasks read this with getEnv, so an unexported value would reach
# them empty no matter how correct the make database looks.
seen=$(exported_value NIM_PARAMS NIMFLAGS=-d:health_sentinel)
case "${seen}" in
  *-d:health_sentinel*) ok "nimble tasks receive NIM_PARAMS in the environment" ;;
  *) no "nimble tasks receive NIM_PARAMS in the environment" \
       "a recipe saw: ${seen:-<empty>}" \
       "getEnv(\"NIM_PARAMS\") in logos_delivery.nimble would not see the flags" ;;
esac

# --------------------------------------------------------------------------
# V. Passed as V=1 by the workflows. It selected nothing at all for months.
# --------------------------------------------------------------------------
expect_flag "V=1 sets --verbosity:1"           "--verbosity:1" V=1
expect_flag "V=0 sets --verbosity:0"           "--verbosity:0" V=0
expect_flag "V=0 quiets hints"                 "--hints:off"   V=0
reject_flag "V=1 keeps hints"                  "--hints:off"   V=1

# V also drives sub-make silencing, which is not a Nim flag and so cannot be
# expressed through NIMFLAGS. Nat.mk consumes it.
expect_eq "V=0 sets HANDLE_OUTPUT for Nat.mk" ">/dev/null" "$(value_of HANDLE_OUTPUT V=0)"
expect_eq "V=1 clears HANDLE_OUTPUT"          ""            "$(value_of HANDLE_OUTPUT V=1)"

# --------------------------------------------------------------------------
# LOG_LEVEL. A declared Jenkins build parameter, forwarded by the release and
# lpt jobs as a Docker build argument.
# --------------------------------------------------------------------------
expect_flag "LOG_LEVEL selects the chronicles level" \
  '-d:chronicles_log_level="INFO"' LOG_LEVEL=INFO
reject_flag "an empty LOG_LEVEL is a no-op" \
  "chronicles_log_level" LOG_LEVEL=

# --------------------------------------------------------------------------
# DEBUG. The only knob here whose active value is 0 rather than 1, so an unset
# DEBUG and DEBUG=0 mean opposite things.
# --------------------------------------------------------------------------
expect_flag "DEBUG=0 selects release"          "-d:release"          DEBUG=0
expect_flag "DEBUG=0 keeps link-time optimisation" "-d:lto_incremental" DEBUG=0
expect_flag "DEBUG=0 strips the binary"        "-d:strip"            DEBUG=0
expect_flag "an unset DEBUG stays a debug build" "-d:debug"
reject_flag "an unset DEBUG does not strip"    "-d:strip"

# --------------------------------------------------------------------------
# The remaining knobs, which are case conversions of one word.
# --------------------------------------------------------------------------
expect_flag "POSTGRES=1 enables the postgres driver" "-d:postgres"    POSTGRES=1
reject_flag "POSTGRES unset leaves it out"           "-d:postgres"
expect_flag "DEBUG_DISCV5=1 enables discv5 tracing"  "-d:debugDiscv5" DEBUG_DISCV5=1

echo
echo "  ${pass} passed, ${fail} failed"
[ "${fail}" -eq 0 ]

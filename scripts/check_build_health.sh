#!/usr/bin/env bash
# Check that build configuration reaches the compiler.
#
# Usage:
#
#   scripts/check_build_health.sh
#
# Each case sets a make variable and checks the Nim flags that result. The
# make database supplies the flags. This does not compile. It needs no Nim,
# no Nimble, no dependencies and no network.
#
# A failed case prints the expected and the actual flags.

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

# Return the resolved NIM_PARAMS.
nim_params() {
  make -pn "$@" 2>/dev/null | grep -E '^NIM_PARAMS :?=' | head -1
}

# Return the value of a variable. Remove the name and the spaces.
value_of() {
  local name=$1
  shift
  make -pn "$@" 2>/dev/null \
    | grep -E "^${name} :?=" | head -1 \
    | sed -E "s/^${name} :?=[[:space:]]*//; s/[[:space:]]+\$//"
}

# Return a variable from a recipe environment. The make database does not
# show export directives.
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
# NIMFLAGS is the public input. The README, the workflows, the Jenkins jobs
# and the Dockerfiles use it. NIM_PARAMS is private.
# --------------------------------------------------------------------------
expect_flag "NIMFLAGS reaches the compiler" \
  "-d:health_sentinel" NIMFLAGS=-d:health_sentinel

# Nim uses the last definition of a define. NIMFLAGS must come last.
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

# An environment NIM_PARAMS must not reach the build.
env_bypass=$(NIM_PARAMS=-d:should_be_ignored nim_params NIMFLAGS=-d:health_sentinel)
case "${env_bypass}" in
  *should_be_ignored*) no "ambient NIM_PARAMS cannot bypass NIMFLAGS" \
                          "an environment NIM_PARAMS leaked into the build" \
                          "actual: ${env_bypass}" ;;
  *) ok "ambient NIM_PARAMS cannot bypass NIMFLAGS" ;;
esac

# The Nimble tasks read NIM_PARAMS with getEnv. Make must export it.
seen=$(exported_value NIM_PARAMS NIMFLAGS=-d:health_sentinel)
case "${seen}" in
  *-d:health_sentinel*) ok "nimble tasks receive NIM_PARAMS in the environment" ;;
  *) no "nimble tasks receive NIM_PARAMS in the environment" \
       "a recipe saw: ${seen:-<empty>}" \
       "getEnv(\"NIM_PARAMS\") in logos_delivery.nimble would not see the flags" ;;
esac

# --------------------------------------------------------------------------
# V selects verbosity. The workflows pass V=1.
# --------------------------------------------------------------------------
expect_flag "V=1 sets --verbosity:1"           "--verbosity:1" V=1
expect_flag "V=0 sets --verbosity:0"           "--verbosity:0" V=0
expect_flag "V=0 quiets hints"                 "--hints:off"   V=0
reject_flag "V=1 keeps hints"                  "--hints:off"   V=1

# V also sets HANDLE_OUTPUT. Nat.mk uses it to silence the sub-makes.
expect_eq "V=0 sets HANDLE_OUTPUT for Nat.mk" ">/dev/null" "$(value_of HANDLE_OUTPUT V=0)"
expect_eq "V=1 clears HANDLE_OUTPUT"          ""            "$(value_of HANDLE_OUTPUT V=1)"

# --------------------------------------------------------------------------
# LOG_LEVEL is a Jenkins parameter. The image builds pass it to make.
# --------------------------------------------------------------------------
expect_flag "LOG_LEVEL selects the chronicles level" \
  '-d:chronicles_log_level="INFO"' LOG_LEVEL=INFO
reject_flag "an empty LOG_LEVEL is a no-op" \
  "chronicles_log_level" LOG_LEVEL=

# --------------------------------------------------------------------------
# DEBUG is active at 0, not at 1. An unset DEBUG and DEBUG=0 differ.
# --------------------------------------------------------------------------
expect_flag "DEBUG=0 selects release"          "-d:release"          DEBUG=0
expect_flag "DEBUG=0 keeps link-time optimisation" "-d:lto_incremental" DEBUG=0
expect_flag "DEBUG=0 strips the binary"        "-d:strip"            DEBUG=0
expect_flag "an unset DEBUG stays a debug build" "-d:debug"
reject_flag "an unset DEBUG does not strip"    "-d:strip"

# --------------------------------------------------------------------------
# These knobs are one word in a different case.
# --------------------------------------------------------------------------
expect_flag "POSTGRES=1 enables the postgres driver" "-d:postgres"    POSTGRES=1
reject_flag "POSTGRES unset leaves it out"           "-d:postgres"
expect_flag "DEBUG_DISCV5=1 enables discv5 tracing"  "-d:debugDiscv5" DEBUG_DISCV5=1

echo
echo "  ${pass} passed, ${fail} failed"
[ "${fail}" -eq 0 ]

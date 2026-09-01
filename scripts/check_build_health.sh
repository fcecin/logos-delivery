#!/usr/bin/env bash
# Check that build configuration reaches the compiler.
#
# Usage:
#
#   scripts/check_build_health.sh
#
# The cases check four things:
#
#   the Nim flags a make variable produces
#   the commands make would run for a target
#   the generated constraints against nimble.lock
#   the command a Nimble task emits
#
# A failed case prints the expected and the actual value.
#
# Examples:
#
#   Variable          Effect
#   ----------------  ------------------------------------------------
#   NIMFLAGS=-d:x     adds -d:x, after the defines it may conflict with
#   NIM_PARAMS=-d:x   from the environment, the base the project appends to;
#                     on the make command line it replaces the whole set
#   V=0               adds --verbosity:0 --hints:off, sets HANDLE_OUTPUT
#   V=1               adds --verbosity:1, clears HANDLE_OUTPUT
#   LOG_LEVEL=INFO    adds -d:chronicles_log_level="INFO"
#   LOG_LEVEL empty   nothing
#   DEBUG=0           adds -d:release -d:lto_incremental -d:strip
#   DEBUG unset       adds -d:debug
#   POSTGRES=1        adds -d:postgres
#   DEBUG_DISCV5=1    adds -d:debugDiscv5

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

# name, python program. The program prints "ok" or the reason it failed.
expect_python() {
  local name=$1 prog=$2
  local got
  got=$(python3 -c "${prog}" 2>&1)
  if [ "${got}" = "ok" ]; then
    ok "${name}"
  else
    no "${name}" "${got}"
  fi
}

# Return the commands make would run for a target. -B ignores timestamps.
recipe_of() {
  make -Bn "$@" 2>/dev/null
}

# name, needle, make args...
expect_recipe() {
  local name=$1 needle=$2
  shift 2
  local got
  got=$(recipe_of "$@")
  case "${got}" in
    *"${needle}"*) ok "${name}" ;;
    *) no "${name}" "expected the recipe to contain: ${needle}" ;;
  esac
}

# name, needle, make args...
reject_recipe() {
  local name=$1 needle=$2
  shift 2
  local got
  got=$(recipe_of "$@")
  case "${got}" in
    *"${needle}"*) no "${name}" "expected the recipe NOT to contain: ${needle}" ;;
    *) ok "${name}" ;;
  esac
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

# Return a variable from a recipe environment. "make -pn" does not show
# export directives.
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
# Callers set NIMFLAGS. The README, the workflows, the Jenkins jobs and the
# Dockerfiles use it. Make computes NIM_PARAMS from it.
# --------------------------------------------------------------------------
expect_flag "NIMFLAGS reaches the compiler" \
  "-d:health_sentinel" NIMFLAGS=-d:health_sentinel

# Nim uses the last definition of a define. NIMFLAGS must come after the
# project defines it may conflict with.
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

# NIM_PARAMS from the environment is the base the project appends to. Callers
# outside this repository rely on it, so keep it, and keep NIMFLAGS after it.
env_base=$(NIM_PARAMS=-d:health_base nim_params NIMFLAGS=-d:health_sentinel)
case "${env_base}" in
  *-d:health_base*) ok "an environment NIM_PARAMS still reaches the build" ;;
  *) no "an environment NIM_PARAMS still reaches the build" \
       "expected to contain: -d:health_base" \
       "actual: ${env_base:-<no NIM_PARAMS>}" ;;
esac
case "${env_base%%-d:health_sentinel*}" in
  *-d:health_base*) ok "NIMFLAGS wins over an environment NIM_PARAMS" ;;
  *) no "NIMFLAGS wins over an environment NIM_PARAMS" \
       "the caller's flag must come after the environment's" \
       "actual: ${env_base:-<no NIM_PARAMS>}" ;;
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

# --------------------------------------------------------------------------
# Nimble reads the constraints only from an attached --requires:<value>. A
# separate argument leaves the value empty and Nimble discards nimble.lock.
# --------------------------------------------------------------------------
expect_recipe "setup attaches the constraints" \
  '--requires:"$(cat requires.generated)"' nimbledeps/.nimble-setup
reject_recipe "setup does not pass them as a separate argument" \
  '--requires "' nimbledeps/.nimble-setup
expect_recipe "custom tasks attach the constraints" \
  '--requires:"$(cat requires.generated)"' wakunode2
reject_recipe "custom tasks do not pass them as a separate argument" \
  '--requires "' wakunode2

# The constraints come from nimble.lock through the generator, and the audit
# checks the result against the same lock.
expect_recipe "setup regenerates the constraints first" \
  "gen_requires.nims" nimbledeps/.nimble-setup
expect_recipe "setup audits the result" \
  "audit-deps" nimbledeps/.nimble-setup

# --------------------------------------------------------------------------
# The constraints are generated from nimble.lock. A named constraint must
# give the locked version, a URL constraint the locked revision.
# --------------------------------------------------------------------------
expect_python "the constraints agree with nimble.lock" "import json
lock = json.load(open(\"nimble.lock\"))[\"packages\"]
gen = [c.strip() for c in open(\"requires.generated\").read().split(\";\") if c.strip()]
def norm(u): return u.lower().rstrip(\"/\").removesuffix(\".git\")
byurl = {norm(v[\"url\"]): v for v in lock.values() if \"url\" in v}
bad = []
for c in gen:
    if \" == \" in c:
        n, v = c.split(\" == \")
        if lock.get(n, {}).get(\"version\") != v:
            bad.append(c + \" (lock has \" + str(lock.get(n, {}).get(\"version\")) + \")\")
    elif c.startswith(\"http\") and \"#\" in c:
        u, rev = c.rsplit(\"#\", 1)
        if byurl.get(norm(u), {}).get(\"vcsRevision\") != rev:
            bad.append(c)
    else:
        bad.append(\"unrecognised form: \" + c)
print(\"ok\" if not bad else \"constraints disagree with nimble.lock: \" + \"; \".join(bad[:3]))"

# --------------------------------------------------------------------------
# The Nimble tasks concatenate their own defaults with NIM_PARAMS. The
# caller's value has to come last. An invalid flag stops Nim before it
# compiles, and the command is printed before it runs.
# --------------------------------------------------------------------------
emitted=$(make wakunode2 \
  NIMFLAGS="-d:chronicles_log_level=HEALTHSENTINEL --nonexistent-flag-xyz" 2>&1 \
  | grep -oE 'nim c [^|]*' | head -1)
if [ -z "${emitted}" ]; then
  no "the task puts the caller's flags after its own" "no nim command was emitted"
else
  before=${emitted%%-d:chronicles_log_level=HEALTHSENTINEL*}
  case "${before}" in
    *chronicles_log_level=*) ok "the task puts the caller's flags after its own" ;;
    *) no "the task puts the caller's flags after its own" \
         "the task default did not appear before the caller's value" ;;
  esac
fi

echo
echo "  ${pass} passed, ${fail} failed"
[ "${fail}" -eq 0 ]

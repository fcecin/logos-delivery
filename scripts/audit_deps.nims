# NOTE: this script exists because Nimble 0.24.1 can install content
# that differs from what the requirements name. The file:line
# references in the comments below point into that Nimble version's
# source (https://github.com/nim-lang/nimble).
#
# Compares the installed dependencies with nimble.lock, in both
# directions. Run it after `nimble setup`:
#
#   nim e scripts/audit_deps.nims
#
# It reads the metadata file of each package in nimbledeps/pkgs2 and
# compares the git revision with the revision in nimble.lock.
#
# Exit code 0: every installed package matches the lock.
# Exit code 1: at least one package differs, a lock entry is not
# installed, or a package is installed that the lock does not list.
# The script prints each difference.
#
# The audit does not help the build succeed: it can only fail it.
# It reads files and writes nothing. A build that passes the audit is
# the same build without it.
#
# Why this script exists: Nimble 0.24.1 can install a revision that
# differs from the revision a requirement names, and exit with code 0.
# The requires string from gen_requires.nims asks for the lock
# revisions; only this audit confirms that the build received them.
# References with a reproduction are at the end of this header.
#
# What a failure means: an upstream repository changed a tag, moved a
# version, or released a version that changes how Nimble resolves.
#
# What to do on a failure:
# 1. To accept the new resolution: run `nimble lock`, then regenerate
#    nix/deps.nix with tools/gen-nix-deps.sh, and commit both files.
# 2. To refuse the new resolution: add a "url#commit" pin for the
#    package in logos_delivery.nimble, then do step 1.
# 3. After a change that removes a package: delete nimbledeps/ and
#    run setup again. `nimble setup` does not remove installed
#    packages, and a package that the lock does not list fails this
#    audit.
#
# The "nim" lock entry is not checked: builds use the system Nim
# (--useSystemNim), so Nimble does not install it as a package.
#
# References:
# - Lock matching is by package name: solveLockFileDeps,
#   src/nimblepkg/nimblesat.nim:1226. A requirement that uses a URL
#   does not match its lock entry, so the lock alone does not
#   constrain resolution in this repository.
# - A commit pin can lose against a competing requirement for the
#   same package: normalizeSpecialVersions,
#   src/nimblepkg/nimblesat.nim:663 keeps one special version and
#   drops the others with a warning.
# - The same defect class in nimble 0.22.3, corrected in 0.24.0:
#   https://github.com/nim-lang/nimble/issues/1691 (resolution fails
#   although a correct selection exists) and
#   https://github.com/nim-lang/nimble/issues/1692 (installed content
#   differs from the selected version).
# - Reproduction (upstream state of 2026-08): the requirement
#   "https://github.com/status-im/nim-secp256k1#d8f1288b7c72f00be5fc2c5ea72bf5cae1eafb15"
#   plus nim-eth's name requirement for secp256k1; nimble 0.24.1
#   setup installs f44cff901dff2a24fedcf4ef9e12a6f72355d58f and exits
#   with code 0.

import std/[json, strutils, algorithm, sets, tables]

let root = thisDir() & "/.."

proc normUrl(url: string): string =
  result = url.toLowerAscii()
  result.removeSuffix("/")
  result.removeSuffix(".git")

# The vcsRevision and url from a nimblemeta.json structure. Both the
# top-level form and the nested metaData form occur.
proc metaField(meta: JsonNode, field: string): string =
  if meta.hasKey(field):
    return meta[field].getStr()
  return meta{"metaData", field}.getStr()

# The package name from a pkgs2 directory name, which has the form
# name-version-checksum. The version starts with a digit and contains
# no dash, so two rsplits recover the name.
proc nameFromDir(dir: string): string =
  result = dir
  for _ in 1 .. 2:
    let cut = result.rfind('-')
    if cut < 0:
      return dir
    result = result[0 ..< cut]

proc main() =
  let lock = parseJson(readFile(root & "/nimble.lock"))["packages"]
  let pkgs2 = root & "/nimbledeps/pkgs2"
  if not dirExists(pkgs2):
    echo "audit: no nimbledeps/pkgs2 directory; run setup first"
    quit(1)

  var installedByUrl = initTable[string, (string, string)]()
  var installedByName = initTable[string, (string, string)]()
  var dirs: seq[string]
  for path in listDirs(pkgs2):
    let d = path.split('/')[^1]
    let metaPath = path & "/nimblemeta.json"
    if not fileExists(metaPath):
      continue
    let meta = parseJson(readFile(metaPath))
    let rev = metaField(meta, "vcsRevision")
    let url = normUrl(metaField(meta, "url"))
    dirs.add(d)
    if url.len > 0:
      installedByUrl[url] = (d, rev)
    installedByName[nameFromDir(d)] = (d, rev)

  var names: seq[string]
  for name, _ in lock:
    names.add(name)
  names.sort()

  var ok = 0
  var total = 0
  var bad: seq[string]
  var matchedDirs: HashSet[string]
  for name in names:
    if name == "nim":
      continue
    total += 1
    let entry = lock[name]
    let want = entry["vcsRevision"].getStr()
    var hit: (string, string)
    if normUrl(entry["url"].getStr()) in installedByUrl:
      hit = installedByUrl[normUrl(entry["url"].getStr())]
    elif name in installedByName:
      hit = installedByName[name]
    else:
      bad.add(name & ": in nimble.lock but not installed")
      continue
    matchedDirs.incl(hit[0])
    if hit[1] != want:
      bad.add(name & ": lock has " & want & ", installed " & hit[0] &
              " has " & hit[1])
    else:
      ok += 1

  # The reverse direction: a task or a solver step can install a
  # package that no lock entry names (for example a nim toolchain).
  dirs.sort()
  for d in dirs:
    if d notin matchedDirs:
      bad.add(d & ": installed but not in nimble.lock")

  for b in bad:
    echo "audit: " & b
  echo "audit: " & $ok & "/" & $total & " installed packages match nimble.lock"
  if bad.len > 0:
    quit(1)

#---------------------------------------------------------------------
# Self-test: checks the parsing procs above on each invocation.
#---------------------------------------------------------------------
proc selfTest() =
  doAssert normUrl("https://github.com/NagyZoltanPeter/nim-brokers.git") ==
    "https://github.com/nagyzoltanpeter/nim-brokers"
  # A version contains no dash, so two rsplits recover the name, also
  # when the name itself contains dashes or digits.
  doAssert nameFromDir("nim-2.2.10-17ec440fdb89") == "nim"
  doAssert nameFromDir("secp256k1-0.6.0.3.2-abfc2c1a") == "secp256k1"
  doAssert nameFromDir("bearssl_pkey_decoder-0.1.0-8666edbc") == "bearssl_pkey_decoder"
  doAssert nameFromDir("nodash") == "nodash"
  # Both nimblemeta.json shapes: top-level and nested under metaData.
  doAssert metaField(parseJson(
    """{"vcsRevision": "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"}"""),
    "vcsRevision") == "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"
  doAssert metaField(parseJson(
    """{"metaData": {"vcsRevision": "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"}}"""),
    "vcsRevision") == "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"
  doAssert metaField(parseJson("""{}"""), "vcsRevision") == ""

selfTest()
main()

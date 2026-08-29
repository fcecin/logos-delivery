# Verify installed Nimble dependency metadata against nimble.lock.
# File-and-line references below refer to Nimble 0.24.1:
# https://github.com/nim-lang/nimble
#
# Run after setup or cache restoration:
#
#   make audit-deps
#
# For each non-Nim lock entry, the audit locates an installed package by
# normalized repository URL, falling back to the package name parsed from
# its pkgs2 directory. Its nimblemeta.json vcsRevision must equal the
# revision in nimble.lock. The reverse check rejects installed directories
# that match no lock entry, and directories without nimblemeta.json are
# also rejected. nimble.paths, which the compiler reads, must select every
# lock package at the lock revision and nothing else.
#
# This validates Nimble's installed metadata. It does not hash or
# otherwise compare every file in an installed package working tree.
#
# The audit does not help the build succeed: it can only fail it. It
# reads files and writes nothing. A build that passes the audit is the
# same build without it.
#
# Successful completion prints the matched count and exits with status 0.
# Missing packages, extra packages, missing metadata, or revision
# differences produce diagnostics and a nonzero exit. File and JSON
# errors also propagate as failures.
#
# Rationale: Nimble 0.24.1 has been observed to exit with status 0 after
# installing a revision different from a requested special revision. The
# reproduction is recorded below.
#
# A failure can result from dependency resolution, an outdated or
# incomplete cache, missing metadata, an upstream tag change, or a
# repository or lock edit. Inspect the reported package and installed
# metadata before updating the lock.
#
# Update procedure:
# 1. To accept a newly reviewed resolution, update logos_delivery.nimble
#    as needed, run `nimble lock`, regenerate nix/deps.nix, perform a
#    clean setup, and require this audit to pass.
# 2. To retain the existing revision, add or adjust a constraint
#    compatible with the other requirements for that package, then perform
#    a clean setup and require this audit to pass. A `url#commit`
#    constraint is not assumed to win when a competing name requirement
#    exists.
# 3. After removing a package, delete nimbledeps/ before setup because
#    `nimble setup` does not remove directories for packages no longer
#    selected.
#
# The `nim` lock entry is skipped because these builds pass --useSystemNim
# and do not install Nim under nimbledeps/.
#
# Relevant Nimble behavior:
# - solveLockFileDeps, src/nimblepkg/nimblesat.nim:1226, matches lock
#   entries by package name. URL requirements do not match those entries
#   directly.
# - normalizeSpecialVersions, src/nimblepkg/nimblesat.nim:663, retains one
#   special version for a package and rewrites or removes competing
#   special requirements, with a warning.
# - Related historical resolution defects are documented in issues #1691
#   and #1692; their fixes shipped in Nimble 0.24.0.
# - Reproduction from the upstream state observed in 2026-08: the
#   requirement
#   "https://github.com/status-im/nim-secp256k1#d8f1288b7c72f00be5fc2c5ea72bf5cae1eafb15"
#   plus nim-eth's name requirement for secp256k1 caused Nimble 0.24.1 to
#   record f44cff901dff2a24fedcf4ef9e12a6f72355d58f and exit with
#   status 0.

import std/[json, strutils, algorithm, sets, tables, os]

let root = thisDir() & "/.."

proc normUrl(url: string): string =
  result = url.toLowerAscii()
  result.removeSuffix("/")
  result.removeSuffix(".git")

# Read a metadata field from either nimblemeta.json layout observed in
# this dependency set: top-level or nested under `metaData`.
proc metaField(meta: JsonNode, field: string): string =
  if meta.hasKey(field):
    return meta[field].getStr()
  return meta{"metaData", field}.getStr()

# Fallback parser for a pkgs2 directory name. Remove the final checksum
# and version fields from `name-version-checksum`. This assumes the
# encoded version field contains no hyphen; URL matching is preferred
# when metadata provides it.
proc nameFromDir(dir: string): string =
  result = dir
  for _ in 1 .. 2:
    let cut = result.rfind('-')
    if cut < 0:
      return dir
    result = result[0 ..< cut]

# One path form for comparison: absolute, forward slashes, no trailing
# separator, lowercase on Windows.
proc canon(path: string): string =
  result = normalizedPath(absolutePath(path)).replace('\\', '/')
  while result.len > 1 and result.endsWith("/"):
    result.setLen(result.len - 1)
  if hostOS == "windows":
    result = result.toLowerAscii()

proc main() =
  let lock = parseJson(readFile(root & "/nimble.lock"))["packages"]
  let pkgs2 = root & "/nimbledeps/pkgs2"
  if not dirExists(pkgs2):
    echo "audit: no nimbledeps/pkgs2 directory; run setup first"
    quit(1)

  var installedByUrl = initTable[string, (string, string)]()
  var installedByName = initTable[string, (string, string)]()
  var dirs: seq[string]
  var metaless: seq[string]
  for path in listDirs(pkgs2):
    let d = lastPathPart(path)
    let metaPath = path / "nimblemeta.json"
    if not fileExists(metaPath):
      metaless.add(d)
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
      bad.add(name & ": lock has " & want & ", installed " & hit[0] & " has " & hit[1])
    else:
      ok += 1

  # Reject installed package directories not matched to a lock entry.
  # This also detects packages added by a later task solve, such as a
  # Nim toolchain.
  dirs.sort()
  for d in dirs:
    if d notin matchedDirs:
      bad.add(d & ": installed but not in nimble.lock")
  metaless.sort()
  for d in metaless:
    bad.add(d & ": installed without nimblemeta.json")

  # nimble.paths is what the compiler reads (config.nims includes it). It
  # must exist, disable the global Nimble path, and select every lock
  # package at the lock revision from this tree's pkgs2 and nothing else.
  # Metadata is read from the literal selected path.
  let pathsFile = root & "/nimble.paths"
  if not fileExists(pathsFile):
    bad.add("nimble.paths: missing; run setup")
  else:
    var selected = initHashSet[string]()
    var noNimblePath = false
    var literals: seq[string]
    for line in readFile(pathsFile).splitLines():
      let l = line.strip()
      if l.len == 0 or l.startsWith("#"):
        continue
      if l == "--noNimblePath":
        noNimblePath = true
        continue
      if not (l.startsWith("--path:\"") and l.endsWith("\"")):
        bad.add("nimble.paths contains an unsupported directive: " & l)
        continue
      # Nimble writes each path with strutils.escape, so a Windows path
      # arrives with doubled backslashes. Decode with the inverse; the
      # quotes are part of the encoded value.
      try:
        literals.add(unescape(l[7 .. ^1]))
      except ValueError as exc:
        bad.add(
          "nimble.paths contains a malformed path directive: " & l & ": " & exc.msg
        )

    let pkgs2Abs = canon(pkgs2)
    let rootAbs = canon(root)
    for literal in literals:
      let p = canon(literal)
      if p == rootAbs:
        continue # the project itself
      if not p.startsWith(pkgs2Abs & "/"):
        bad.add("nimble.paths selects " & p & ", outside " & pkgs2Abs)
        continue
      let d = p[pkgs2Abs.len + 1 .. ^1].split('/')[0]
      let name = nameFromDir(d)
      let metaPath = pkgs2 & "/" & d & "/nimblemeta.json"
      if not fileExists(metaPath):
        bad.add("nimble.paths selects " & d & ": no nimblemeta.json")
        continue
      let rev = metaField(parseJson(readFile(metaPath)), "vcsRevision")
      if not lock.hasKey(name):
        bad.add("nimble.paths selects " & d & ": not in nimble.lock")
      elif lock[name]["vcsRevision"].getStr() != rev:
        bad.add(
          "nimble.paths selects " & d & " at " & rev & "; nimble.lock has " &
            lock[name]["vcsRevision"].getStr()
        )
      selected.incl(name)
    if not noNimblePath:
      bad.add(
        "nimble.paths: --noNimblePath missing; the global Nimble path is visible to the compiler"
      )
    for name in names:
      if name != "nim" and name notin selected:
        bad.add(name & ": in nimble.lock but not in nimble.paths")

  for b in bad:
    echo "audit: " & b
  echo "audit: " & $ok & "/" & $total & " installed packages match nimble.lock"
  if bad.len > 0:
    echo "audit: the installed packages do not match nimble.lock. Delete nimbledeps/ and run make again."
    quit(1)

#---------------------------------------------------------------------
# Self-tests for the metadata and directory-name parsers. Executed before main().
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
  doAssert metaField(
    parseJson("""{"vcsRevision": "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"}"""),
    "vcsRevision",
  ) == "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"
  doAssert metaField(
    parseJson(
      """{"metaData": {"vcsRevision": "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"}}"""
    ),
    "vcsRevision",
  ) == "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"
  doAssert metaField(parseJson("""{}"""), "vcsRevision") == ""
  # Nimble escapes the path it writes; a Windows value arrives doubled.
  doAssert unescape("\"D:\\\\a\\\\b\"") == "D:\\a\\b"
  # Containment compares canonical forms; a trailing separator is not a
  # different directory, and a sibling with a longer name is not inside.
  doAssert canon("/a/b/") == canon("/a/b")
  doAssert not canon("/a/bc").startsWith(canon("/a/b") & "/")
  doAssert canon("/a/b/c").startsWith(canon("/a/b") & "/")

selfTest()
main()

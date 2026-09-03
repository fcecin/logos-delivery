# Audit nimble.lock against its three consumers: the URL requirements in
# logos_delivery.nimble, nix/deps.nix, and the packages installed under
# nimbledeps/pkgs2.
#
# Two modes:
#
#   nim e scripts/audit_deps.nims pins   # preflight: pins and nix only, no
#                                        # nimbledeps needed; the Makefile runs
#                                        # it before `nimble setup`
#   nim e scripts/audit_deps.nims        # full: preflight plus the installed
#                                        # packages; runs after setup and after
#                                        # the build and test steps
#
# URL requirements. Nimble decides whether to install from nimble.lock or to
# re-solve the whole dependency graph online by matching every root
# requirement to a lock entry. For a URL requirement the match is
#
#   cmpIgnoreCase(requirement.url, entry.url) == 0  and
#   withinRange(entry.version, requirement.version)
#
# The URL comparison is exact apart from case: "nim-sds.git" and "nim-sds"
# are different URLs to Nimble. A special version (#tag or #commit) is
# within range only of the same string. A plain lock version is within
# range of any special requirement, so Nimble would silently install the
# locked revision under a pin that names a different commit; this audit is
# stricter there and requires the commit to match. Rules, per URL quoted on
# a non-comment line of logos_delivery.nimble:
#
#   form                     lock version   accepted when
#   ----------------------   ------------   ---------------------------------
#   url                      any            always (Nimble: verAny matches)
#   url#frag                 #special       frag == special
#   url#frag                 plain          frag == vcsRevision
#   url == x                 plain          x == version
#   url == x                 #special       never (Nimble: string mismatch)
#   url <other range>        any            never: unsupported here; use
#                                           "== x", "#tag" or "#commit"
#
# A URL that matches no lock entry exactly but matches one after
# normalisation (case, trailing "/", ".git") is reported as such, because
# that is the mistake Nimble does not forgive.
#
# nix/deps.nix. Every git lock entry must appear in nix/deps.nix with the
# same revision, and every nix entry must correspond to a lock entry. URLs
# are compared normalised, because tools/gen-nix-deps.sh writes them that
# way. Regenerate with: tools/gen-nix-deps.sh nimble.lock nix/deps.nix
#
# Installed packages. For each non-Nim lock entry, an installed package is
# located by normalised repository URL, falling back to the package name
# parsed from its pkgs2 directory. Its nimblemeta.json vcsRevision must
# equal the revision in nimble.lock. Installed directories that match no
# lock entry, and directories without nimblemeta.json, are rejected. The
# `nim` lock entry is skipped because these builds pass --useSystemNim.
#
# The audit does not help the build succeed: it can only fail it. It reads
# files and writes nothing. Successful completion prints a summary and
# exits with status 0. Any mismatch prints a diagnostic and exits 1. File
# and JSON errors also propagate as failures.
#
# Update procedure:
# 1. To accept a newly reviewed resolution, update logos_delivery.nimble
#    as needed, run `nimble lock`, regenerate nix/deps.nix, perform a
#    clean setup, and require this audit to pass.
# 2. To change how a package is pinned without changing its revision (a
#    tag pin to a commit pin or back), set the lock entry's "version" to
#    the same string, then perform a clean setup and require this audit to
#    pass.
# 3. After removing a package, delete nimbledeps/ before setup because
#    `nimble setup` does not remove directories for packages no longer
#    selected.
#
# Relevant Nimble behaviour (the pinned revision, see RequiredNimbleRevision):
# - solveLockFileDeps matches a URL requirement to a lock entry by URL
#   (b1b0690) and installs from the locked URL and revision (07caee3).
#   Releases up to 0.24.1 matched by package name only, so URL
#   requirements never matched and every invocation re-solved online.
# - Version equality with a special version on either side is a string
#   comparison (src/nimblepkg/version.nim, `==`).
# - Release 0.24.1 was observed to exit with status 0 after installing a
#   revision different from a requested special revision (nim-secp256k1
#   pinned to d8f1288b7c72f00be5fc2c5ea72bf5cae1eafb15 installed
#   f44cff901dff2a24fedcf4ef9e12a6f72355d58f). The installed-package check
#   exists for that class of failure.

import std/[json, strutils, algorithm, sets, tables, os]

let root = thisDir() & "/.."

proc normUrl(url: string): string =
  result = url.toLowerAscii()
  result.removeSuffix("/")
  result.removeSuffix(".git")

#---------------------------------------------------------------------
# URL requirements in logos_delivery.nimble
#---------------------------------------------------------------------

type UrlReq = tuple[url, frag, range: string]
  ## `url` as written; `frag` is the text after '#' ("" when absent);
  ## `range` is the version constraint after a space ("" when absent),
  ## e.g. ">= 0.5.1".

# Return every quoted URL requirement on a non-comment line.
proc urlReqsFrom(content: string): seq[UrlReq] =
  for line in content.splitLines():
    if line.strip(trailing = false).startsWith("#"):
      continue
    let parts = line.split('"')
    var i = 1
    while i < parts.len:
      let s = parts[i]
      if s.startsWith("http://") or s.startsWith("https://"):
        var url = s
        var frag = ""
        var range = ""
        let spaceAt = url.find(' ')
        if spaceAt >= 0:
          range = url[spaceAt + 1 .. ^1].strip()
          url = url[0 ..< spaceAt]
        let hashAt = url.find('#')
        if hashAt >= 0:
          frag = url[hashAt + 1 .. ^1]
          url = url[0 ..< hashAt]
        result.add((url, frag, range))
      i += 2

# Diagnostics for requirements that would miss their lock entry, or that
# contradict it.
proc pinMismatches(reqs: seq[UrlReq], lock: JsonNode): seq[string] =
  for req in reqs:
    var name = ""
    var entry: JsonNode = nil
    var near = ""
    for n, e in lock:
      if not e.hasKey("url"):
        continue
      let u = e["url"].getStr()
      if cmpIgnoreCase(u, req.url) == 0:
        name = n
        entry = e
        break
      if normUrl(u) == normUrl(req.url):
        near = n & " (" & u & ")"
    if entry.isNil:
      if near.len > 0:
        result.add(req.url & ": no lock entry with this exact URL; nearest is " &
                   near & ". Nimble matches URL requirements to the lock by " &
                   "exact string (case-insensitive), so this misses the lock")
      else:
        result.add(req.url & ": pinned in logos_delivery.nimble but not in nimble.lock")
      continue
    let lockVer = entry["version"].getStr()
    let lockRev = entry["vcsRevision"].getStr()
    if req.frag.len > 0:
      if lockVer.startsWith("#"):
        if req.frag != lockVer[1 .. ^1]:
          result.add(name & ": pin #" & req.frag & " but nimble.lock records version " &
                     lockVer & " (set both to the same tag or commit)")
      elif req.frag != lockRev:
        result.add(name & ": pin #" & req.frag & " but nimble.lock revision is " & lockRev)
    elif req.range.len > 0:
      if req.range.startsWith("== "):
        let want = req.range[3 .. ^1].strip()
        if lockVer.startsWith("#"):
          result.add(name & ": requirement \"== " & want & "\" cannot match the special " &
                     "lock version " & lockVer & "; pin \"#" & lockVer[1 .. ^1] & "\" instead")
        elif want != lockVer:
          result.add(name & ": requirement \"== " & want & "\" but nimble.lock records " &
                     "version " & lockVer)
      else:
        result.add(name & ": ranged URL requirement \"" & req.range & "\" is not " &
                   "supported by this audit; use \"== <version>\", \"#<tag>\" or \"#<commit>\"")

#---------------------------------------------------------------------
# nix/deps.nix
#---------------------------------------------------------------------

# Map normalised repository URLs to revisions from fetchgit blocks.
proc nixEntriesFrom(content: string): Table[string, string] =
  var url = ""
  for line in content.splitLines():
    let l = line.strip()
    if l.startsWith("url = \""):
      url = l.split('"')[1]
    elif l.startsWith("rev = \"") and url.len > 0:
      result[normUrl(url)] = l.split('"')[1]
      url = ""

const nixHint = "regenerate with: tools/gen-nix-deps.sh nimble.lock nix/deps.nix"

proc nixMismatches(lock: JsonNode, nix: Table[string, string]): seq[string] =
  var lockUrls: HashSet[string]
  var names: seq[string]
  for n, _ in lock:
    names.add(n)
  names.sort()
  for n in names:
    let e = lock[n]
    if n == "nim" or e{"downloadMethod"}.getStr() != "git":
      continue
    let u = normUrl(e["url"].getStr())
    lockUrls.incl(u)
    if u notin nix:
      result.add(n & ": in nimble.lock but not in nix/deps.nix (" & nixHint & ")")
    elif nix[u] != e["vcsRevision"].getStr():
      result.add(n & ": nimble.lock has " & e["vcsRevision"].getStr() &
                 ", nix/deps.nix has " & nix[u] & " (" & nixHint & ")")
  var nixUrls: seq[string]
  for u, _ in nix:
    nixUrls.add(u)
  nixUrls.sort()
  for u in nixUrls:
    if u notin lockUrls:
      result.add("nix/deps.nix has " & u & ", but nimble.lock has no entry for it (" &
                 nixHint & ")")

#---------------------------------------------------------------------
# Installed packages
#---------------------------------------------------------------------

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

proc installedMismatches(lock: JsonNode, pkgs2: string, ok: var int, total: var int): seq[string] =
  var installedByUrl = initTable[string, (string, string)]()
  var installedByName = initTable[string, (string, string)]()
  var dirs: seq[string]
  var metaless: seq[string]
  for path in listDirs(pkgs2):
    let d = path.split('/')[^1]
    let metaPath = path & "/nimblemeta.json"
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
      result.add(name & ": in nimble.lock but not installed")
      continue
    matchedDirs.incl(hit[0])
    if hit[1] != want:
      result.add(name & ": lock has " & want & ", installed " & hit[0] & " has " & hit[1])
    else:
      ok += 1

  # Reject installed package directories not matched to a lock entry.
  # This also detects packages added by a later task solve, such as a
  # Nim toolchain or a floated dependency.
  dirs.sort()
  for d in dirs:
    if d notin matchedDirs:
      result.add(d & ": installed but not in nimble.lock")
  metaless.sort()
  for d in metaless:
    result.add(d & ": installed without nimblemeta.json")

#---------------------------------------------------------------------

proc main() =
  let pinsOnly = "pins" in commandLineParams()
  let lock = parseJson(readFile(root & "/nimble.lock"))["packages"]

  var bad: seq[string]
  let reqs = urlReqsFrom(readFile(root & "/logos_delivery.nimble"))
  bad.add pinMismatches(reqs, lock)
  bad.add nixMismatches(lock, nixEntriesFrom(readFile(root & "/nix/deps.nix")))

  var ok = 0
  var total = 0
  if not pinsOnly:
    let pkgs2 = root & "/nimbledeps/pkgs2"
    if not dirExists(pkgs2):
      bad.add("no nimbledeps/pkgs2 directory; run setup first")
    else:
      bad.add installedMismatches(lock, pkgs2, ok, total)

  for b in bad:
    echo "audit: " & b
  if bad.len > 0:
    echo "audit: " & $bad.len & " problem(s) found"
    quit(1)
  if pinsOnly:
    echo "audit: " & $reqs.len & " URL requirements and nix/deps.nix agree with nimble.lock"
  else:
    echo "audit: " & $ok & "/" & $total & " installed packages match nimble.lock, " &
      $reqs.len & " URL requirements checked, nix/deps.nix checked"

#---------------------------------------------------------------------
# Self-tests for the parsers and the matching rules. Executed before main().
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

  # URL requirement parsing: fragment, range, bare, .git kept, comments and
  # names that start with "http" ignored.
  doAssert urlReqsFrom("""  "https://github.com/status-im/nim-websock#387a8eb",""") ==
    @[("https://github.com/status-im/nim-websock", "387a8eb", "")]
  doAssert urlReqsFrom("""requires "https://github.com/logos-messaging/nim-sds.git#b12f5ee"""") ==
    @[("https://github.com/logos-messaging/nim-sds.git", "b12f5ee", "")]
  doAssert urlReqsFrom("""requires "https://github.com/vacp2p/nim-lsquic.git >= 0.5.1"""") ==
    @[("https://github.com/vacp2p/nim-lsquic.git", "", ">= 0.5.1")]
  doAssert urlReqsFrom("""requires "https://github.com/vacp2p/nim-lsquic == 0.5.1"""") ==
    @[("https://github.com/vacp2p/nim-lsquic", "", "== 0.5.1")]
  doAssert urlReqsFrom("""requires "https://github.com/vacp2p/nim-boringssl"""") ==
    @[("https://github.com/vacp2p/nim-boringssl", "", "")]
  doAssert urlReqsFrom("""# v0.4.0: https://github.com/status-im/nim-websock/releases/tag/v0.4.0""").len == 0
  doAssert urlReqsFrom("""  "httputils >= 0.4.1",""").len == 0

  # Matching rules against a lock fixture.
  let lockFixture = parseJson("""{
    "websock": {"version": "#v0.4.0", "vcsRevision": "387a8eb", "url": "https://github.com/status-im/nim-websock"},
    "metrics": {"version": "0.2.2", "vcsRevision": "9f2e1d4a", "url": "https://github.com/status-im/nim-metrics"},
    "sds": {"version": "#b12f5ee", "vcsRevision": "b12f5ee", "url": "https://github.com/logos-messaging/nim-sds.git"}
  }""")
  proc bad(url, frag, range: string): int =
    pinMismatches(@[(url, frag, range)], lockFixture).len
  # special lock version: fragment must be the same string
  doAssert bad("https://github.com/status-im/nim-websock", "v0.4.0", "") == 0
  doAssert bad("https://github.com/status-im/nim-websock", "387a8eb", "") == 1
  doAssert bad("https://github.com/status-im/nim-websock", "", "") == 0      # bare: verAny
  doAssert bad("https://github.com/status-im/nim-websock", "", "== 0.4.0") == 1
  # plain lock version: a commit fragment must be the locked revision
  doAssert bad("https://github.com/status-im/nim-metrics", "9f2e1d4a", "") == 0
  doAssert bad("https://github.com/status-im/nim-metrics", "deadbeef", "") == 1
  doAssert bad("https://github.com/status-im/nim-metrics", "", "") == 0
  doAssert bad("https://github.com/status-im/nim-metrics", "", "== 0.2.2") == 0
  doAssert bad("https://github.com/status-im/nim-metrics", "", "== 0.2.3") == 1
  doAssert bad("https://github.com/status-im/nim-metrics", "", ">= 0.2.0") == 1  # unsupported range
  # URL exactness: case is ignored, ".git" is not
  doAssert bad("https://github.com/Status-IM/nim-metrics", "", "") == 0
  doAssert bad("https://github.com/logos-messaging/nim-sds.git", "b12f5ee", "") == 0
  doAssert bad("https://github.com/logos-messaging/nim-sds", "b12f5ee", "") == 1
  doAssert pinMismatches(@[("https://github.com/logos-messaging/nim-sds", "b12f5ee", "")],
    lockFixture)[0].contains("nearest is sds")
  doAssert bad("https://github.com/nowhere/pkg", "x", "") == 1

  # nix parsing and cross-check in both directions.
  let nix = nixEntriesFrom("""
  chronos = pkgs.fetchgit {
    url = "https://github.com/status-im/nim-chronos";
    rev = "45f43a9ad8bd8bcf5903b42f365c1c879bd54240";
    sha256 = "sha256-000";
    fetchSubmodules = true;
  };
""")
  doAssert nix["https://github.com/status-im/nim-chronos"] ==
    "45f43a9ad8bd8bcf5903b42f365c1c879bd54240"
  let lockNix = parseJson("""{
    "chronos": {"version": "4.2.4", "vcsRevision": "45f43a9ad8bd8bcf5903b42f365c1c879bd54240",
                "url": "https://github.com/status-im/nim-chronos.git", "downloadMethod": "git"},
    "nim": {"version": "2.2.6", "vcsRevision": "", "url": "", "downloadMethod": "git"}
  }""")
  doAssert nixMismatches(lockNix, nix).len == 0
  var nixStale = nix
  nixStale["https://github.com/status-im/nim-chronos"] = "0000000"
  doAssert nixMismatches(lockNix, nixStale).len == 1
  var nixExtra = nix
  nixExtra["https://github.com/example/extra"] = "1111111"
  doAssert nixMismatches(lockNix, nixExtra).len == 1
  doAssert nixMismatches(lockNix, initTable[string, string]()).len == 1

selfTest()
main()

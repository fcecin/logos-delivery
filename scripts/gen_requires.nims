# NOTE: this script exists to satisfy Nimble 0.24.1. The file:line
# references in the comments below point into that Nimble version's
# source (https://github.com/nim-lang/nimble).
#
# Writes requires.generated: one Nimble requires string, built from
# nimble.lock. The Makefile and CI pass the file's content to
# `nimble setup --requires`. This makes the lock binding: Nimble itself
# does not apply the lock when a requirement uses a URL. Its lock
# matching compares package names only (solveLockFileDeps,
# src/nimblepkg/nimblesat.nim:1226).
#
# Run with:
#
#   nim e scripts/gen_requires.nims
#
# The script carries no package knowledge of its own. Its inputs:
# - nimble.lock: the revision and version of each package.
# - logos_delivery.nimble: the packages that are already pinned by URL
#   there. Those packages are not emitted: a second constraint for the
#   same package can change how Nimble merges.
# - The upstream world, observed when the script runs. The lock fixes
#   which content must arrive; the observation selects the requirement
#   form that can deliver that content in the world that `nimble setup`
#   is about to resolve against. Two facts per package:
#     tagged:   the tag that names the locked version points at the
#               locked revision (`git ls-remote --tags` — the command
#               Nimble uses to enumerate versions: getTagsListRemote,
#               src/nimblepkg/download.nim:208).
#     registry: the name is in Nimble's packages.json with the lock's
#               URL (src/nimblepkg/config.nim:25). A name that is
#               absent, or resolves to a different repository, cannot
#               be used as a name.
#
# Emission rule, per lock entry:
# - pinned by URL in logos_delivery.nimble  -> not emitted
# - not tagged, or not in the registry      -> "url#revision"
# - otherwise                               -> "name == version"
#   (The name form merges correctly with range requirements from other
#   packages. Nimble can drop a commit pin in that merge instead:
#   normalizeSpecialVersions, src/nimblepkg/nimblesat.nim:663. For a
#   package that another package requires by name, the url#revision
#   form is therefore not always binding; the audit is the guarantee
#   that the build received the locked content.)
#
# The script also compares nimble.lock with nix/deps.nix in both
# directions: same entries, same revisions. A difference stops the
# build: the two files must agree. A failed
# observation (network, registry unreachable) stops the build: the
# script never guesses.
#
# Outputs, written only by this script:
# - requires.generated (gitignored): the requires string. Written with
#   a rename, and only when every check passes, so a failed run cannot
#   leave a fresh-looking artifact behind.
# - observed.generated (gitignored): the facts observed for each
#   package at the moment of decision. A diagnostic record. Nothing
#   reads it, and it must stay that way: consuming a recorded
#   observation instead of observing is how staleness starts.

import std/[json, strutils, algorithm, sets, tables]

let root = thisDir() & "/.."

const registryMirrors = [
  "https://raw.githubusercontent.com/nim-lang/packages/master/packages.json",
  "https://packages.nim-lang.org/packages.json",
]

const nixHint = "regenerate with: tools/gen-nix-deps.sh nimble.lock nix/deps.nix"

proc normUrl(url: string): string =
  result = url.toLowerAscii()
  result.removeSuffix("/")
  result.removeSuffix(".git")

# The URLs that logos_delivery.nimble requires in URL form. A quoted
# string that starts with http, on a line that is not a comment, is a
# URL requirement; the base URL ends at '#' or at a space.
proc urlPinsFrom(content: string): HashSet[string] =
  for line in content.splitLines():
    if line.strip(trailing = false).startsWith("#"):
      continue
    let parts = line.split('"')
    var i = 1
    while i < parts.len:
      if parts[i].startsWith("http://") or parts[i].startsWith("https://"):
        var base = parts[i]
        let cut = min(
          if base.find('#') >= 0: base.find('#') else: base.len,
          if base.find(' ') >= 0: base.find(' ') else: base.len,
        )
        base = base[0 ..< cut]
        result.incl(normUrl(base))
      i += 2

proc nimbleUrlPins(path: string): HashSet[string] =
  urlPinsFrom(readFile(path))

# {normalized url: rev} from the fetchgit blocks of nix/deps.nix.
proc nixEntriesFrom(content: string): Table[string, string] =
  var url = ""
  for line in content.splitLines():
    let l = line.strip()
    if l.startsWith("url = \""):
      url = l.split('"')[1]
    elif l.startsWith("rev = \"") and url.len > 0:
      result[normUrl(url)] = l.split('"')[1]
      url = ""

proc nixEntries(path: string): Table[string, string] =
  nixEntriesFrom(readFile(path))

# {lowercased name: normalized url} from Nimble's registry, with alias
# entries followed one level, as Nimble does.
proc registryFrom(jsonContent: string): Table[string, string] =
  var byName = initTable[string, JsonNode]()
  for p in parseJson(jsonContent):
    if p.hasKey("name"):
      byName[p["name"].getStr().toLowerAscii()] = p
  for lname, p in byName:
    var entry = p
    if entry.hasKey("alias"):
      entry = byName.getOrDefault(entry["alias"].getStr().toLowerAscii(), newJObject())
    if entry.hasKey("url"):
      result[lname] = normUrl(entry["url"].getStr())

proc registryUrls(): Table[string, string] =
  var lastErr = ""
  for mirror in registryMirrors:
    let (output, code) = gorgeEx("curl -fsSL --max-time 60 " & mirror)
    if code != 0:
      lastErr = "curl exit " & $code & " for " & mirror
      continue
    return registryFrom(output)
  echo "gen_requires: cannot fetch the Nimble registry: " & lastErr
  quit(1)

# Observe: does the tag that names the locked version point at the
# locked revision? Plain and annotated tags both count (the "^{}"
# variant is the target of an annotated tag). The comparison is against
# exact expected lines, so no pattern language is involved. git's own
# low-speed limits bound a stalled connection.
proc tagLineMatches(lsRemoteOutput, version, rev: string): bool =
  var expected: HashSet[string]
  for prefix in ["refs/tags/", "refs/tags/v"]:
    for suffix in ["", "^{}"]:
      expected.incl(rev & "\t" & prefix & version & suffix)
  for line in lsRemoteOutput.splitLines():
    if line in expected:
      return true
  return false

proc versionTagPointsAtRev(url, version, rev: string): bool =
  let (output, code) = gorgeEx(
    "git -c http.lowSpeedLimit=1 -c http.lowSpeedTime=60 ls-remote --tags " & url)
  if code != 0:
    raise newException(CatchableError, "git ls-remote --tags failed for " & url)
  tagLineMatches(output, version, rev)

proc writeAtomic(path, content: string) =
  writeFile(path & ".tmp", content)
  mvFile(path & ".tmp", path)

type LockEntry = tuple[name, version, rev, url: string]

proc main() =
  let lock = parseJson(readFile(root & "/nimble.lock"))["packages"]
  let inFile = nimbleUrlPins(root & "/logos_delivery.nimble")
  let nix = nixEntries(root & "/nix/deps.nix")
  let registry = registryUrls()

  var entries: seq[LockEntry]
  for name, e in lock:
    if name == "nim" or name == "nimble":
      continue
    if e{"downloadMethod"}.getStr() != "git":
      continue
    entries.add((name, e["version"].getStr(), e["vcsRevision"].getStr(),
                 e["url"].getStr()))
  entries.sort(proc(a, b: LockEntry): int = cmp(a.name, b.name))

  var errors: seq[string]
  var lockUrls: HashSet[string]
  for e in entries:
    lockUrls.incl(normUrl(e.url))
  var missing = inFile - lockUrls
  for u in missing:
    errors.add("logos_delivery.nimble pins " & u &
               ", but nimble.lock has no entry for it")

  for url, _ in nix:
    if url notin lockUrls:
      errors.add("nix/deps.nix has " & url &
                 ", but nimble.lock has no entry for it (" & nixHint & ")")

  var candidates: seq[LockEntry]
  for e in entries:
    let url = normUrl(e.url)
    if url notin nix:
      errors.add(e.name & ": not in nix/deps.nix (" & nixHint & ")")
      continue
    if nix[url] != e.rev:
      errors.add(e.name & ": nimble.lock has " & e.rev &
                 ", nix/deps.nix has " & nix[url])
    if url in inFile:
      continue
    candidates.add(e)

  var outParts: seq[string]
  var record = @[
    "# Diagnostic record: the facts scripts/gen_requires.nims observed",
    "# on its last run. Written on every run; read by nothing.",
  ]
  for e in candidates:
    let inRegistry = registry.getOrDefault(e.name.toLowerAscii(), "") == normUrl(e.url)
    var usable = false
    if inRegistry:
      try:
        usable = versionTagPointsAtRev(e.url, e.version, e.rev)
      except CatchableError as ex:
        errors.add(e.name & ": observation failed: " & ex.msg)
        record.add(e.name & " " & e.rev & " observation-failed")
        continue
    let taggedCol = if inRegistry: $ord(usable) else: "-"
    record.add(e.name & " " & e.rev & " tagged=" & taggedCol &
               " registry=" & $ord(inRegistry))
    if usable:
      outParts.add(e.name & " == " & e.version)
    else:
      var base = e.url
      base.removeSuffix("/")
      base.removeSuffix(".git")
      outParts.add(base & "#" & e.rev)

  writeAtomic(root & "/observed.generated", record.join("\n") & "\n")

  if errors.len > 0:
    for e in errors:
      echo "gen_requires: " & e
    quit(1)

  let requires = outParts.join("; ")
  writeAtomic(root & "/requires.generated", requires & "\n")
  echo requires

#---------------------------------------------------------------------
# Self-test: checks the parsing procs above on each invocation.
#---------------------------------------------------------------------
proc selfTest() =
  # Mixed case and ".git" as this repository really uses them.
  doAssert normUrl("https://github.com/NagyZoltanPeter/nim-brokers.git") ==
    "https://github.com/nagyzoltanpeter/nim-brokers"
  doAssert normUrl("https://github.com/vacp2p/nim-boringssl") ==
    "https://github.com/vacp2p/nim-boringssl"
  # A package name that starts with "http" is not a URL requirement.
  doAssert urlPinsFrom("""  "httputils >= 0.4.1",""").len == 0
  # A comment line is not a requirement, also when it quotes a URL.
  doAssert urlPinsFrom("""# v2.0.0: "https://github.com/vacp2p/nim-libp2p"""").len == 0
  doAssert "https://github.com/vacp2p/nim-libp2p" in urlPinsFrom(
    """requires "https://github.com/vacp2p/nim-libp2p.git#c43199378f46d0aaf61be1cad1ee1d63e8f665d6"""")
  doAssert "https://github.com/vacp2p/nim-lsquic" in urlPinsFrom(
    """requires "https://github.com/vacp2p/nim-lsquic.git == 0.5.1"""")
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
  # A plain tag (libp2p v2.0.0) and an annotated tag's "^{}" target
  # (brokers v3.3.0) both match; both lines are real ls-remote output.
  doAssert tagLineMatches(
    "c43199378f46d0aaf61be1cad1ee1d63e8f665d6\trefs/tags/v2.0.0",
    "2.0.0", "c43199378f46d0aaf61be1cad1ee1d63e8f665d6")
  doAssert tagLineMatches(
    "19565dd80621e33f6da396ef3fb07c379d55c324\trefs/tags/v3.3.0^{}",
    "3.3.0", "19565dd80621e33f6da396ef3fb07c379d55c324")
  # A tag that points at a different revision does not match.
  doAssert not tagLineMatches(
    "f44cff901dff2a24fedcf4ef9e12a6f72355d58f\trefs/tags/v0.6.0",
    "0.6.0", "d8f1288b7c72f00be5fc2c5ea72bf5cae1eafb15")
  # Registry parsing: names compare case-insensitively, an alias is
  # followed one level, and an alias to a missing entry yields nothing.
  let reg = registryFrom("""[
    {"name": "Chronos", "url": "https://github.com/status-im/nim-chronos"},
    {"name": "old", "alias": "chronos"},
    {"name": "dead", "alias": "gone"}
  ]""")
  doAssert reg["chronos"] == "https://github.com/status-im/nim-chronos"
  doAssert reg["old"] == "https://github.com/status-im/nim-chronos"
  doAssert "dead" notin reg

selfTest()
main()

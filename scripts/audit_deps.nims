# Audit the generated dependency artifacts against nimble.lock. Reads files,
# writes nothing, exits 1 on any problem.
#
#   nim e scripts/audit_deps.nims preflight   # nix/deps.nix against the lock;
#                                             # no installed packages needed
#   nim e scripts/audit_deps.nims             # the above plus the installed
#                                             # packages; runs after setup
#                                             # and after builds
#
# Whether logos_delivery.nimble matches nimble.lock is Nimble's decision;
# `make audit-deps` asks Nimble for it (`nimble deps --offline --refresh`)
# after this script. Nothing here reproduces Nimble's matching rules.
#
# nix/deps.nix must list every git lock entry at the locked revision and
# nothing else (URLs compared normalised, as tools/gen-nix-deps.sh writes
# them). Installed packages must be the lock entries at their vcsRevision
# and nothing else; `nim` is skipped (--useSystemNim). After removing a
# package, delete nimbledeps/ before setup; `nimble setup` does not remove
# stale directories.

import std/[json, strutils, algorithm, sets, tables, os]

let root = thisDir() & "/.."

proc normUrl(url: string): string =
  result = url.toLowerAscii()
  result.removeSuffix("/")
  result.removeSuffix(".git")

#---------------------------------------------------------------------
# nix/deps.nix
#---------------------------------------------------------------------

# Normalised URL -> revision, from fetchgit blocks.
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

# nimblemeta.json field, top-level or under metaData.
proc metaField(meta: JsonNode, field: string): string =
  if meta.hasKey(field):
    return meta[field].getStr()
  return meta{"metaData", field}.getStr()

# Package name from a pkgs2 directory name (name-version-checksum).
# Assumes no hyphen in the version.
proc nameFromDir(dir: string): string =
  result = dir
  for _ in 1 .. 2:
    let cut = result.rfind('-')
    if cut < 0:
      return dir
    result = result[0 ..< cut]

type Installed = tuple[dir, url, rev: string]

# The installed directory for a lock entry: same URL and revision first, then
# same URL, then same package name. Several directories can share a URL when
# a stale version was not removed; the unmatched ones are reported below.
proc findInstalled(inst: seq[Installed], name, url, rev: string): int =
  for i, p in inst:
    if p.url == url and p.rev == rev: return i
  for i, p in inst:
    if p.url == url: return i
  for i, p in inst:
    if nameFromDir(p.dir) == name: return i
  -1

proc installedMismatches(lock: JsonNode, pkgs2: string, ok: var int, total: var int): seq[string] =
  var inst: seq[Installed]
  var metaless: seq[string]
  for path in listDirs(pkgs2):
    let d = path.split('/')[^1]
    let metaPath = path & "/nimblemeta.json"
    if not fileExists(metaPath):
      metaless.add(d)
      continue
    let meta = parseJson(readFile(metaPath))
    inst.add((d, normUrl(metaField(meta, "url")), metaField(meta, "vcsRevision")))

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
    let i = findInstalled(inst, name, normUrl(entry["url"].getStr()), want)
    if i < 0:
      result.add(name & ": in nimble.lock but not installed")
      continue
    matchedDirs.incl(inst[i].dir)
    if inst[i].rev != want:
      result.add(name & ": lock has " & want & ", installed " & inst[i].dir & " has " & inst[i].rev)
    else:
      ok += 1

  # Installed directories that match no lock entry.
  var dirs: seq[string]
  for p in inst:
    dirs.add(p.dir)
  dirs.sort()
  for d in dirs:
    if d notin matchedDirs:
      result.add(d & ": installed but not in nimble.lock")
  metaless.sort()
  for d in metaless:
    result.add(d & ": installed without nimblemeta.json")

#---------------------------------------------------------------------

proc main() =
  let preflight = "preflight" in commandLineParams()
  let lock = parseJson(readFile(root & "/nimble.lock"))["packages"]

  var bad: seq[string]
  bad.add nixMismatches(lock, nixEntriesFrom(readFile(root & "/nix/deps.nix")))

  var ok = 0
  var total = 0
  if not preflight:
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
  if preflight:
    echo "audit: nix/deps.nix agrees with nimble.lock"
  else:
    echo "audit: " & $ok & "/" & $total & " installed packages match nimble.lock, nix/deps.nix agrees"

#---------------------------------------------------------------------
# Self-tests, run before main().
#---------------------------------------------------------------------
proc selfTest() =
  doAssert normUrl("https://github.com/NagyZoltanPeter/nim-brokers.git") ==
    "https://github.com/nagyzoltanpeter/nim-brokers"
  doAssert nameFromDir("nim-2.2.10-17ec440fdb89") == "nim"
  doAssert nameFromDir("secp256k1-0.6.0.3.2-abfc2c1a") == "secp256k1"
  doAssert nameFromDir("bearssl_pkey_decoder-0.1.0-8666edbc") == "bearssl_pkey_decoder"
  doAssert nameFromDir("nodash") == "nodash"
  doAssert metaField(parseJson(
    """{"vcsRevision": "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"}"""),
    "vcsRevision") == "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"
  doAssert metaField(parseJson(
    """{"metaData": {"vcsRevision": "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"}}"""),
    "vcsRevision") == "d34aa46bf9d0a3ffff810fbd3c4d2fa024eb9368"
  doAssert metaField(parseJson("""{}"""), "vcsRevision") == ""

  # nix parsing and cross-check.
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

  # Two installed directories for one URL: the one at the locked revision matches.
  let two: seq[Installed] = @[("chronos-4.2.4-aaaa", "https://github.com/status-im/nim-chronos", "90f5"),
                              ("chronos-4.2.5-bbbb", "https://github.com/status-im/nim-chronos", "0ab8")]
  doAssert two[findInstalled(two, "chronos", "https://github.com/status-im/nim-chronos", "0ab8")].dir == "chronos-4.2.5-bbbb"
  doAssert two[findInstalled(two, "chronos", "https://github.com/status-im/nim-chronos", "90f5")].dir == "chronos-4.2.4-aaaa"
  doAssert two[findInstalled(two, "chronos", "https://github.com/status-im/nim-chronos", "ffff")].url.len > 0
  doAssert findInstalled(two, "stew", "https://github.com/status-im/nim-stew", "x") == -1

selfTest()
main()

# Audit nimble.lock against the requirements in logos_delivery.nimble,
# nix/deps.nix, and the packages installed under nimbledeps/pkgs2. Reads
# files, runs `nimble dump --json` (offline), writes nothing, exits 1 on any
# mismatch.
#
#   nim e scripts/audit_deps.nims pins   # requirements and nix only;
#                                        # no nimbledeps needed; runs before
#                                        # `nimble setup`
#   nim e scripts/audit_deps.nims        # the preflight plus the installed
#                                        # packages; runs after setup and
#                                        # after builds
#
# Nimble installs from nimble.lock only when every root requirement matches
# a lock entry; otherwise it re-solves the graph online. The requirements
# come from `nimble dump --json`, the same parse Nimble uses. Match rules:
#
#   requirement          lock version   accepted when
#   ------------------   ------------   -----------------------------------
#   url                  any            always (Nimble: verAny matches)
#   url#frag             #special       frag == special
#   url#frag             plain          frag == vcsRevision (stricter than
#                                       Nimble, which installs the locked
#                                       revision under any pin)
#   url == x             plain          x == version
#   url == x             #special       never
#   url <range>          any            never; use "== x", "#tag", "#commit"
#   name                 any            always
#   name <range>         plain          version within range (==, >=, >,
#                                       <=, <, &, ~=, ^=)
#   name <range>         #special       never; pin the URL instead
#
# A URL is matched to a lock entry by exact case-insensitive string, as
# Nimble does; a URL that matches only after normalisation (case, trailing
# "/", ".git") is reported with the nearest entry. A name is matched by
# case-insensitive package name; registry aliases are not resolved here.
#
# nix/deps.nix must list every git lock entry at the locked revision and
# nothing else (URLs compared normalised). Installed packages must be the
# lock entries at their vcsRevision and nothing else; `nim` is skipped
# (--useSystemNim). To re-pin a package at the same revision, set the lock
# entry's "version" to the same string with `nimble lock`. After removing a
# package, delete nimbledeps/ before setup; `nimble setup` does not remove
# stale directories.

import std/[json, strutils, algorithm, sets, tables, os]

let root = thisDir() & "/.."

proc normUrl(url: string): string =
  result = url.toLowerAscii()
  result.removeSuffix("/")
  result.removeSuffix(".git")

proc isUrl(name: string): bool =
  name.startsWith("http://") or name.startsWith("https://")

#---------------------------------------------------------------------
# Version comparison, after Nimble's cmpSemVer: dotted numeric segments,
# missing segments are zero, a "-prerelease" suffix sorts before the release.
#---------------------------------------------------------------------

proc splitVer(v: string): (seq[int], string) =
  var core = v
  var pre = ""
  let dash = v.find('-')
  if dash >= 0:
    core = v[0 ..< dash]
    pre = v[dash + 1 .. ^1]
  var nums: seq[int]
  for part in core.split('.'):
    var n = 0
    for c in part:
      if c in Digits:
        n = n * 10 + (ord(c) - ord('0'))
      else:
        break
    nums.add(n)
  (nums, pre)

proc cmpVer(a, b: string): int =
  let (an, ap) = splitVer(a)
  let (bn, bp) = splitVer(b)
  for i in 0 ..< max(an.len, bn.len):
    let x = if i < an.len: an[i] else: 0
    let y = if i < bn.len: bn[i] else: 0
    if x != y:
      return cmp(x, y)
  if ap.len == 0 and bp.len == 0: 0
  elif ap.len == 0: 1
  elif bp.len == 0: -1
  else: cmp(ap, bp)

# Is plain version `v` within the range node `ran` from `nimble dump --json`?
# Returns "" when it is, or a reason when it is not or cannot be decided.
proc outsideRange(v: string, ran: JsonNode): string =
  let kind = ran["kind"].getStr()
  case kind
  of "verAny": ""
  of "verEq":
    if cmpVer(v, ran["ver"].getStr()) == 0: "" else: "is not " & ran["ver"].getStr()
  of "verEqLater":
    if cmpVer(v, ran["ver"].getStr()) >= 0: "" else: "is below " & ran["ver"].getStr()
  of "verLater":
    if cmpVer(v, ran["ver"].getStr()) > 0: "" else: "is not above " & ran["ver"].getStr()
  of "verEqEarlier":
    if cmpVer(v, ran["ver"].getStr()) <= 0: "" else: "is above " & ran["ver"].getStr()
  of "verEarlier":
    if cmpVer(v, ran["ver"].getStr()) < 0: "" else: "is not below " & ran["ver"].getStr()
  of "verIntersect", "verTilde", "verCaret":
    let l = outsideRange(v, ran["verILeft"])
    if l.len > 0: l else: outsideRange(v, ran["verIRight"])
  else:
    "has an unsupported range kind " & kind

#---------------------------------------------------------------------
# Requirements from `nimble dump --json`
#---------------------------------------------------------------------

proc dumpRequires(): JsonNode =
  let (output, code) = gorgeEx(
    "cd '" & root & "' && nimble dump --json --localdeps --useSystemNim")
  let s = output.find('{')
  let e = output.rfind('}')
  if code != 0 or s < 0 or e < s:
    echo "audit: `nimble dump --json` failed (is the pinned nimble on PATH? " &
      "use `make preflight-deps`)"
    echo output
    quit(1)
  parseJson(output[s .. e])["requires"]

# Diagnostics for requirements that miss or contradict their lock entry.
proc reqMismatches(reqs: JsonNode, lock: JsonNode): seq[string] =
  for r in reqs:
    let name = r["name"].getStr()
    let ran = r["ver"]
    let kind = ran["kind"].getStr()
    let str = r["str"].getStr()
    if name == "nim":
      continue
    var lockName = ""
    var entry: JsonNode = nil
    if name.isUrl:
      var near = ""
      for n, e in lock:
        if not e.hasKey("url"):
          continue
        let u = e["url"].getStr()
        if cmpIgnoreCase(u, name) == 0:
          lockName = n
          entry = e
          break
        if normUrl(u) == normUrl(name):
          near = n & " (" & u & ")"
      if entry.isNil:
        if near.len > 0:
          result.add(name & ": no lock entry with this exact URL (nearest: " & near &
                     "); Nimble compares URLs as exact case-insensitive strings")
        else:
          result.add(name & ": required but not in nimble.lock")
        continue
    else:
      for n, e in lock:
        if cmpIgnoreCase(n, name) == 0:
          lockName = n
          entry = e
          break
      if entry.isNil:
        result.add(name & ": required but not in nimble.lock (registry aliases are " &
                   "not resolved here; use the lock's package name)")
        continue
    let lockVer = entry["version"].getStr()
    let lockRev = entry["vcsRevision"].getStr()
    let special = lockVer.startsWith("#")
    if kind == "verSpecial":
      let frag = ran["spe"].getStr()[1 .. ^1]
      if not name.isUrl:
        result.add(lockName & ": special version \"" & str & "\" on a name requirement; " &
                   "pin the URL instead")
      elif special:
        if frag != lockVer[1 .. ^1]:
          result.add(lockName & ": pin #" & frag & " but nimble.lock records version " &
                     lockVer & " (set both to the same tag or commit)")
      elif frag != lockRev:
        result.add(lockName & ": pin #" & frag & " but nimble.lock revision is " & lockRev)
    elif kind == "verAny":
      discard
    elif name.isUrl:
      if kind == "verEq":
        let want = ran["ver"].getStr()
        if special:
          result.add(lockName & ": requirement \"== " & want & "\" cannot match the special " &
                     "lock version " & lockVer & "; pin \"#" & lockVer[1 .. ^1] & "\" instead")
        elif cmpVer(want, lockVer) != 0:
          result.add(lockName & ": requirement \"== " & want & "\" but nimble.lock records " &
                     "version " & lockVer)
      else:
        result.add(lockName & ": ranged URL requirement \"" & str & "\" is not supported " &
                   "by this audit; use \"== <version>\", \"#<tag>\" or \"#<commit>\"")
    else:
      if special:
        result.add(lockName & ": requirement \"" & str & "\" cannot match the special " &
                   "lock version " & lockVer & "; pin the URL with that string instead")
      else:
        let why = outsideRange(lockVer, ran)
        if why.len > 0:
          result.add(lockName & ": requirement \"" & str & "\" but nimble.lock records " &
                     "version " & lockVer & ", which " & why)

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
  let pinsOnly = "pins" in commandLineParams()
  let lock = parseJson(readFile(root & "/nimble.lock"))["packages"]

  var bad: seq[string]
  let reqs = dumpRequires()
  bad.add reqMismatches(reqs, lock)
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
    echo "audit: " & $(reqs.len - 1) & " requirements and nix/deps.nix agree with nimble.lock"
  else:
    echo "audit: " & $ok & "/" & $total & " installed packages match nimble.lock, " &
      $(reqs.len - 1) & " requirements checked, nix/deps.nix checked"

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

  # Version comparison.
  doAssert cmpVer("4.2.5", "4.2.4") > 0
  doAssert cmpVer("4.2", "4.2.0") == 0
  doAssert cmpVer("0.6.0.3.2", "0.6.0.3.2") == 0
  doAssert cmpVer("4.10.0", "4.9.0") > 0
  doAssert cmpVer("1.0.0-rc1", "1.0.0") < 0

  # Range evaluation on nimble dump's structure.
  let ranges = parseJson("""{
    "any": {"kind": "verAny"},
    "eq": {"kind": "verEq", "ver": "2.3.1"},
    "ge": {"kind": "verEqLater", "ver": "0.4.1"},
    "band": {"kind": "verIntersect", "verILeft": {"kind": "verEqLater", "ver": "4.2.0"},
             "verIRight": {"kind": "verEarlier", "ver": "4.4.0"}},
    "odd": {"kind": "verMystery"}
  }""")
  doAssert outsideRange("2.3.1", ranges["eq"]) == ""
  doAssert outsideRange("2.4.0", ranges["eq"]).len > 0
  doAssert outsideRange("0.5.1", ranges["ge"]) == ""
  doAssert outsideRange("0.4.0", ranges["ge"]).len > 0
  doAssert outsideRange("4.2.5", ranges["band"]) == ""
  doAssert outsideRange("4.4.0", ranges["band"]).len > 0
  doAssert outsideRange("1.0", ranges["any"]) == ""
  doAssert outsideRange("1.0", ranges["odd"]).contains("unsupported")

  # Matching rules against a lock fixture.
  let lockFixture = parseJson("""{
    "websock": {"version": "#v0.4.0", "vcsRevision": "387a8eb", "url": "https://github.com/status-im/nim-websock"},
    "metrics": {"version": "0.2.2", "vcsRevision": "9f2e1d4a", "url": "https://github.com/status-im/nim-metrics"},
    "sds": {"version": "#b12f5ee", "vcsRevision": "b12f5ee", "url": "https://github.com/logos-messaging/nim-sds.git"},
    "libp2p": {"version": "2.3.1", "vcsRevision": "391e403", "url": "https://github.com/vacp2p/nim-libp2p"},
    "chronos": {"version": "4.2.5", "vcsRevision": "0ab802b", "url": "https://github.com/status-im/nim-chronos"},
    "libplum": {"version": "#v0.6.3", "vcsRevision": "189a498", "url": "https://github.com/logos-storage/nim-libplum"}
  }""")
  proc bad(name, str: string, ver: JsonNode): int =
    var r = %*[{"name": name, "str": str, "ver": ver}]
    reqMismatches(r, lockFixture).len
  proc spe(s: string): JsonNode = %*{"kind": "verSpecial", "spe": s}
  proc eq(s: string): JsonNode = %*{"kind": "verEq", "ver": s}
  let anyv = %*{"kind": "verAny"}
  # URL pins
  doAssert bad("https://github.com/status-im/nim-websock", "#v0.4.0", spe("#v0.4.0")) == 0
  doAssert bad("https://github.com/status-im/nim-websock", "#387a8eb", spe("#387a8eb")) == 1
  doAssert bad("https://github.com/status-im/nim-websock", "any version", anyv) == 0
  doAssert bad("https://github.com/status-im/nim-websock", "== 0.4.0", eq("0.4.0")) == 1
  doAssert bad("https://github.com/status-im/nim-metrics", "#9f2e1d4a", spe("#9f2e1d4a")) == 0
  doAssert bad("https://github.com/status-im/nim-metrics", "#deadbeef", spe("#deadbeef")) == 1
  doAssert bad("https://github.com/status-im/nim-metrics", "== 0.2.2", eq("0.2.2")) == 0
  doAssert bad("https://github.com/status-im/nim-metrics", "== 0.2.3", eq("0.2.3")) == 1
  doAssert bad("https://github.com/status-im/nim-metrics", ">= 0.2.0", ranges["ge"]) == 1
  doAssert bad("https://github.com/Status-IM/nim-metrics", "any version", anyv) == 0
  doAssert bad("https://github.com/logos-messaging/nim-sds.git", "#b12f5ee", spe("#b12f5ee")) == 0
  doAssert bad("https://github.com/logos-messaging/nim-sds", "#b12f5ee", spe("#b12f5ee")) == 1
  doAssert reqMismatches(%*[{"name": "https://github.com/logos-messaging/nim-sds", "str": "#b12f5ee",
    "ver": spe("#b12f5ee")}], lockFixture)[0].contains("nearest: sds")
  doAssert bad("https://github.com/nowhere/pkg", "#x", spe("#x")) == 1
  # name requirements
  doAssert bad("libp2p", "== 2.3.1", eq("2.3.1")) == 0
  doAssert bad("libp2p", "== 2.4.0", eq("2.4.0")) == 1
  doAssert bad("LibP2P", "== 2.3.1", eq("2.3.1")) == 0
  doAssert bad("chronos", ">= 4.2.0 & < 4.4.0", ranges["band"]) == 0
  doAssert bad("chronos", ">= 4.3.0", %*{"kind": "verEqLater", "ver": "4.3.0"}) == 1
  doAssert bad("chronos", "any version", anyv) == 0
  doAssert bad("stew", "any version", anyv) == 1          # not in the fixture lock
  doAssert bad("libplum", "any version", anyv) == 0
  doAssert bad("libplum", ">= 0.6.0", %*{"kind": "verEqLater", "ver": "0.6.0"}) == 1  # special lock version
  doAssert bad("libplum", "#v0.6.3", spe("#v0.6.3")) == 1   # special on a name requirement
  doAssert bad("nim", "== 2.2.6", eq("2.2.6")) == 0          # skipped

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

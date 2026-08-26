#!/usr/bin/env python3
# This script prints one Nimble requires string, built from nimble.lock.
# The Makefile and CI pass the output to `nimble setup --requires`.
# This makes the lock binding: Nimble itself does not apply the lock when
# a requirement uses a URL. Its lock matching compares package names only
# (solveLockFileDeps, src/nimblepkg/nimblesat.nim:1226, nimble v0.24.1).
#
# The script carries no package knowledge of its own. Every input is a
# committed artifact:
# - nimble.lock: the revision for each package.
# - logos_delivery.nimble: the packages that are already pinned by URL
#   there. Those packages are not emitted: a second constraint for the
#   same package can change how Nimble merges.
# - nix/deps.nix "# observed:" lines, written by tools/gen-nix-deps.sh
#   from the upstream world at ratification time:
#     tags=0     The repository has no git tags. A version cannot
#                select content there (Nimble enumerates versions from
#                tags: getTagsListRemote, src/nimblepkg/download.nim:208).
#     registry=0 The name is not in the Nimble registry with this URL.
#                A name requirement cannot resolve to this repository.
#
# Emission rule, per lock entry:
# - pinned by URL in logos_delivery.nimble  -> not emitted
# - observed tags=0 or registry=0           -> "url#revision"
# - otherwise                               -> "name == version"
#   (The name form merges correctly with range requirements from other
#   packages. Nimble can drop a commit pin in that merge instead:
#   normalizeSpecialVersions, src/nimblepkg/nimblesat.nim:663.)
#
# The script also compares each lock revision with nix/deps.nix. A
# difference stops the build: the two files must agree. A lock entry
# with no "# observed:" line stops the build: nix/deps.nix predates the
# observer and must be regenerated.

import json
import re
import sys
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

REGEN_HINT = "regenerate with: tools/gen-nix-deps.sh nimble.lock nix/deps.nix"


def norm(url):
    url = url.lower().rstrip("/")
    return url.removesuffix(".git")


def nimble_url_pins(path):
    """Return the normalized URLs that logos_delivery.nimble requires
    in URL form (with a #revision or a version range on the URL)."""
    urls = set()
    for line in open(path):
        if line.lstrip().startswith("#"):
            continue
        for lit in re.findall(r'"(https?://[^"]+)"', line):
            base = re.split(r"#| ", lit, maxsplit=1)[0]
            urls.add(norm(base))
    return urls


def nix_entries(path):
    """Return {normalized url: (rev, tags, registry)} parsed from the
    fetchgit blocks and their observed lines in nix/deps.nix."""
    entries = {}
    if not os.path.exists(path):
        return entries
    text = open(path).read()
    for block in re.findall(r"fetchgit\s*\{(.*?)\}", text, re.S):
        url = re.search(r'url\s*=\s*"([^"]+)"', block)
        rev = re.search(r'rev\s*=\s*"([^"]+)"', block)
        obs = re.search(r"#\s*observed:\s*tags=(\d)\s+registry=(\d)", block)
        if url and rev:
            entries[norm(url.group(1))] = (
                rev.group(1),
                int(obs.group(1)) if obs else None,
                int(obs.group(2)) if obs else None,
            )
    return entries


def main():
    lock = json.load(open(os.path.join(ROOT, "nimble.lock")))["packages"]
    in_file = nimble_url_pins(os.path.join(ROOT, "logos_delivery.nimble"))
    nix = nix_entries(os.path.join(ROOT, "nix", "deps.nix"))

    lock_urls = {norm(e["url"]) for e in lock.values()}
    errors = [
        f"logos_delivery.nimble pins {u}, but nimble.lock has no entry for it"
        for u in sorted(in_file - lock_urls)
    ]

    out = []
    for name, entry in sorted(lock.items()):
        if name == "nim":
            continue
        url = norm(entry["url"])
        rev = entry["vcsRevision"]
        hit = nix.get(url)
        if hit is None:
            errors.append(f"{name}: not in nix/deps.nix ({REGEN_HINT})")
            continue
        nix_rev, tags, registry = hit
        if nix_rev != rev:
            errors.append(
                f"{name}: nimble.lock has {rev}, nix/deps.nix has {nix_rev}"
            )
        if url in in_file:
            continue
        if tags is None:
            errors.append(f"{name}: no observed line in nix/deps.nix ({REGEN_HINT})")
            continue
        if tags == 0 or registry == 0:
            out.append(f"{entry['url'].rstrip('/').removesuffix('.git')}#{rev}")
        else:
            out.append(f"{name} == {entry['version']}")

    if errors:
        for e in errors:
            print("gen_requires: " + e, file=sys.stderr)
        sys.exit(1)
    print("; ".join(out))


if __name__ == "__main__":
    main()

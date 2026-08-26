#!/usr/bin/env python3
# This script compares the installed dependencies with nimble.lock.
# Run it after `nimble setup`. It reads the metadata file of each
# package in nimbledeps/pkgs2 and compares the git revision with the
# revision in nimble.lock.
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
# The requires string from gen_requires.py asks for the lock
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
# References (nimble v0.24.1 source, https://github.com/nim-lang/nimble):
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

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def norm(url):
    return url.rstrip("/").removesuffix(".git")


def meta_revision(meta):
    """Return the vcsRevision from a nimblemeta.json structure."""
    if "vcsRevision" in meta:
        return meta["vcsRevision"]
    return meta.get("metaData", {}).get("vcsRevision")


def meta_url(meta):
    if "url" in meta:
        return meta["url"]
    return meta.get("metaData", {}).get("url", "")


def main():
    lock = json.load(open(os.path.join(ROOT, "nimble.lock")))["packages"]
    pkgs2 = os.path.join(ROOT, "nimbledeps", "pkgs2")
    if not os.path.isdir(pkgs2):
        print("audit: no nimbledeps/pkgs2 directory; run setup first", file=sys.stderr)
        sys.exit(1)

    # Read the metadata of each installed package. The key is the
    # normalized repository URL; the package name is the fallback key.
    installed_by_url = {}
    installed_by_name = {}
    for d in sorted(os.listdir(pkgs2)):
        meta_path = os.path.join(pkgs2, d, "nimblemeta.json")
        if not os.path.isfile(meta_path):
            continue
        meta = json.load(open(meta_path))
        rev = meta_revision(meta)
        url = norm(meta_url(meta))
        # The directory name has the form name-version-checksum.
        name = re.sub(r"-[^-]+-[0-9a-f]+$", "", d)
        if url:
            installed_by_url[url] = (d, rev)
        installed_by_name[name] = (d, rev)

    ok = 0
    bad = []
    matched_dirs = set()
    for name, entry in sorted(lock.items()):
        if name == "nim":
            continue
        want = entry["vcsRevision"]
        hit = installed_by_url.get(norm(entry["url"])) or installed_by_name.get(name)
        if hit is None:
            bad.append(f"{name}: in nimble.lock but not installed")
            continue
        d, have = hit
        matched_dirs.add(d)
        if have != want:
            bad.append(f"{name}: lock has {want}, installed {d} has {have}")
        else:
            ok += 1

    # The reverse direction: a task or a solver step can install a
    # package that no lock entry names (for example a nim toolchain).
    for d, _rev in sorted(installed_by_name.values()):
        if d not in matched_dirs:
            bad.append(f"{d}: installed but not in nimble.lock")

    total = sum(1 for n in lock if n != "nim")
    for b in bad:
        print("audit: " + b, file=sys.stderr)
    print(f"audit: {ok}/{total} installed packages match nimble.lock")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()

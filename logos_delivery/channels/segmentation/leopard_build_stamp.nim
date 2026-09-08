## Second half of `leopard_build_guard`: import it after `segmentation`, so
## it runs once nim-leopard's compile-time cmake step has produced the archive.
## Records the signature that `leopard_build_guard` compares on the next
## compile, and writes the archive path for the packaging steps that merge it
## into static libraries.

{.used.}
  # Imported for its compile-time effect only; nothing here is called at runtime.

import std/os
import ./leopard_build_guard

const leopardArchiveManifest {.strdefine.} = ""
  ## `-d:leopardArchiveManifest=<file>`: write the effective archive path there.
  ## `--app:staticlib` drops the {.passL.} nim-leopard links the archive with,
  ## so the nimble static tasks read this file and merge the archive themselves.

static:
  if fileExists(leopardArchive):
    if leopardLibOverrideUnset():
      writeFile(leopardSignatureFile, leopardBuildSignature())
    if leopardArchiveManifest.len > 0:
      writeFile(leopardArchiveManifest, leopardArchive & "\n")
  else:
    # nim-leopard raises at compile time when cmake fails, so this is only
    # reachable with an override that points at a missing file.
    echo "leopard_build_stamp: Leopard-RS archive not found: " & leopardArchive

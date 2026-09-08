## Compile-time guard for the Leopard-RS archive that nim-leopard builds.
##
## nim-leopard runs cmake from a `static:` block and skips it whenever
## `<nimcache>/vendor_leopard/liblibleopard.a` already exists. It never checks
## the settings that archive was built with, so a checkout switched between
## native and portable flags, or between host and cross toolchains, keeps the
## first archive it built. This module runs before nim-leopard's block: it
## compares a signature of the effective build inputs with the one recorded by
## the last successful build (see `leopard_build_stamp`) and removes the stale
## build directory when they differ. A successful build records the new
## signature; a failed one leaves nothing that a later run would trust.
##
## Import this module before `segmentation` (or anything that reaches
## `leopard`), and `leopard_build_stamp` after it.

{.used.}
  # Imported for its compile-time effect only; nothing here is called at runtime.

import std/[compilesettings, os, strutils]

const
  LeopardBuildRecipe* = 1
    ## Bump when the build recipe below changes in a way that must rebuild.

  # The same strdefines nim-leopard reads. An undefined value is "" here and
  # the wrapper's own default there; both change together when config.nims
  # starts or stops defining them, which is what the signature must notice.
  LeopardCmakeFlags {.strdefine.} = ""
  LeopardExtraCompilerFlags {.strdefine.} = ""
  LeopardExtraLinkerFlags {.strdefine.} = ""
  LeopardLibOverride {.strdefine: "LeopardLib".} = ""
    ## nim-leopard's `LeopardLib`; when a caller points it elsewhere, the
    ## archive is theirs to manage and this guard leaves the cache alone.

  leopardBuildDir* = querySetting(SingleValueSetting.nimcacheDir) / "vendor_leopard"
  leopardArchive* =
    if LeopardLibOverride.len > 0: LeopardLibOverride
    else: leopardBuildDir / "liblibleopard.a"
  leopardSignatureFile* = leopardBuildDir / ".logos_delivery_build_signature"

proc leopardLibOverrideUnset*(): bool {.compileTime.} =
  return LeopardLibOverride.len == 0

proc leopardPackageDir(): string {.compileTime.} =
  ## The installed nim-leopard package, from the search paths. Its directory
  ## name carries the version and checksum, so a bump changes the signature.
  for p in querySettingSeq(MultipleValueSetting.searchPaths):
    if "leopard-" in p.extractFilename or "leopard-" in p.parentDir.extractFilename:
      return p
  return ""

const ccFamily =
  when defined(gcc): "gcc"
  elif defined(clang): "clang"
  elif defined(vcc): "vcc"
  elif defined(tcc): "tcc"
  elif defined(icc): "icc"
  else: "other"

proc leopardBuildSignature*(): string {.compileTime.} =
  ## Everything that shapes the archive nim-leopard builds.
  var fields = @[
    "recipe=" & $LeopardBuildRecipe,
    "os=" & hostOS,
    "cpu=" & hostCPU,
    "cc=" & ccFamily & " " & querySetting(SingleValueSetting.ccompilerPath) & " " & getEnv("CC"),
    "compileOptions=" & querySetting(SingleValueSetting.compileOptions),
    "LeopardCmakeFlags=" & LeopardCmakeFlags,
    "LeopardExtraCompilerFlags=" & LeopardExtraCompilerFlags,
    "LeopardExtraLinkerFlags=" & LeopardExtraLinkerFlags,
    "package=" & leopardPackageDir(),
  ]
  for name in ["IOS_SDK", "IOS_SDK_PATH", "IOS_ARCH", "IOS_DEPLOYMENT_TARGET",
               "ANDROID_TOOLCHAIN_DIR", "ANDROID_COMPILER", "ANDROID_ARCH"]:
    fields.add(name & "=" & getEnv(name))
  return fields.join("\n") & "\n"

static:
  if LeopardLibOverride.len == 0 and dirExists(leopardBuildDir):
    let recorded =
      if fileExists(leopardSignatureFile): staticRead(leopardSignatureFile)
      else: ""
    if recorded != leopardBuildSignature():
      echo "leopard_build_guard: Leopard-RS build inputs changed, removing " &
        leopardBuildDir & " so nim-leopard rebuilds it"
      when defined(windows):
        discard gorge("rmdir /s /q \"" & leopardBuildDir & "\"")
      else:
        discard gorge("rm -rf '" & leopardBuildDir & "'")

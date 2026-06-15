# zerokit rln built from source; overrides the stale vendor cargoHash.
# vendorHash differs per zerokit version (2.0.0 vs 2.0.2 etc.) and must be
# passed in by the caller so the same builder works for both pins.
{ zerokit, system, vendorHash }:
zerokit.packages.${system}.rln.overrideAttrs (old: {
  cargoDeps = old.cargoDeps.overrideAttrs (oldCargoDeps: {
    vendorStaging = oldCargoDeps.vendorStaging.overrideAttrs (_: {
      outputHash = vendorHash;
    });
  });
})

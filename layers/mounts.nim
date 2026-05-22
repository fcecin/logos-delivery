{.push raises: [].}

## Per-(layer, broker-context) mount gate: at most one mount per
## `(layer typedesc T, BrokerContext)`. Does not bind RequestBroker providers.
##
## Per-thread storage (threadvar).

import std/sets
import results
import brokers/broker_context

export results

type LayerKey = tuple[layerName: string, ctxId: uint32]

var layerMounts {.threadvar.}: HashSet[LayerKey]

proc isLayerMounted*(T: typedesc, ctx: BrokerContext): bool =
  let key: LayerKey = ($T, ctx.uint32)
  key in layerMounts

proc mountLayer*(T: typedesc, ctx: BrokerContext): Result[void, string] =
  ## Claim the (T, ctx) instance slot. Errors if already mounted.
  let key: LayerKey = ($T, ctx.uint32)
  if key in layerMounts:
    return err($T & " is already mounted in broker context " & $ctx.uint32)
  layerMounts.incl(key)
  ok()

proc unmountLayer*(T: typedesc, ctx: BrokerContext): Result[void, string] =
  ## Release the (T, ctx) instance slot. Errors if not mounted.
  let key: LayerKey = ($T, ctx.uint32)
  if key notin layerMounts:
    return err($T & " is not mounted in broker context " & $ctx.uint32)
  layerMounts.excl(key)
  ok()

{.pop.}

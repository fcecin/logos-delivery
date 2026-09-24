## Leaf module for the app-level mode / entry-layer enums.
##
## These appear on `WakuNodeConf` (in the leaf `tools/confutils/cli_args`), so
## they must live in a module that `cli_args` can import without a cycle — i.e.
## a module that imports nothing from the config/api layers. `logos_delivery_conf`
## re-exports them so consumers still get them from there.

type LogosDeliveryMode* {.pure.} = enum
  ## Drives the kernel-internal protocol mountings. Applied only for the
  ## `messaging` / `channels` entry layers; ignored when `entryLayer == kernel`.
  Edge # client-only node
  Core # full service node

type EntryLayer* {.pure.} = enum
  ## Selects which API layer `LogosDelivery` instantiates.
  kernel # transport kernel only; ignores `mode` and uses the config as-is
  messaging # kernel + messaging client
  channels # kernel + messaging + reliable channels

type AnonymityLevel* {.pure.} = enum
  ## How a send may use Mix. A mixed send completes on the exit's reply and gets no
  ## store confirmation, so its delivery assurance is weaker. A `Preferred` send
  ## whose Mix attempt gets no reply goes out again in clear after the Mix window.
  None ## Never use Mix. Send over the plain path, relay then lightpush.
  Preferred
    ## Try Mix first. Take the plain path at once when Mix is not mounted, and
    ## after the Mix window when Mix cannot attempt the send or gets no answer.
  Required ## Use Mix only. Never use the plain path.

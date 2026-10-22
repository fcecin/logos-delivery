## Core identifier types for the Reliable Channel API.

import results, chronos
import logos_delivery/api/types as api_types

import ./scalable_data_sync/scalable_data_sync

export scalable_data_sync
export api_types

type ChannelId* = SdsChannelID

type ChannelCryptoFn* = proc(payload: seq[byte]): Future[Result[seq[byte], string]] {.
  async: (raises: []), closure
.}

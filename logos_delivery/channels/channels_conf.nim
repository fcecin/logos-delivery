## Reliable-channels configuration.

import std/options

import logos_delivery/channels/segmentation/segmentation_persistence
  # SegmentationPersistence
import types/persistence # Persistence (nim-sds)

type ChannelsConf* = object
  ## Reliable-channels configuration as an all-`Option` partial. Unset fields fall
  ## back to the defaults used by `createReliableChannel`.
  # Segmentation
  segmentationEnableReedSolomon*: Option[bool]
    ## Add Reed-Solomon parity segments for recovery of lost segments.
  segmentationSegmentSizeBytes*: Option[int]
    ## Maximum segment size in bytes.
  # SDS
  sdsAcknowledgementTimeoutMs*: Option[int]
    ## Time to wait before retransmitting an unacknowledged message.
  sdsMaxRetransmissions*: Option[int]
    ## Maximum retransmission attempts before delivery fails.
  sdsCausalHistorySize*: Option[int]
    ## Number of message ids kept in causal history.
  # Rate limiting
  rateLimitEnabled*: Option[bool]
    ## Enable rate limiting.
  rateLimitEpochPeriodSec*: Option[int]
    ## Rate-limit epoch length in seconds.
  # Pluggable backends (dependency injection)
  segmentationPersistence*: Option[SegmentationPersistence]
    ## Persists partial reassembly state across restarts.
  sdsPersistence*: Option[Persistence]
    ## Persists SDS local history.

import brokers/event_broker
import libp2p/switch

type EventWakuPeerKind* {.pure.} = enum
  EventConnected
  EventDisconnected
  EventIdentified
  EventMetadataUpdated

EventBroker:
  type EventWakuPeer* = object
    peerId*: PeerId
    kind*: EventWakuPeerKind

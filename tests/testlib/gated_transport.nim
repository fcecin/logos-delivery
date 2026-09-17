{.used.}

import chronos
import libp2p/[builders, dialer, stream/connection, muxers/yamux/yamux]
import logos_delivery/waku/waku_core
import ./wakucore

type
  GatedTransport* = ref object of Connection
    blockWrites*: bool
    requestWritten*: AsyncEvent
    blockedWriteSeen*: AsyncEvent
    release*: AsyncEvent

  FixedDialer = ref object of Dialer
    stream: Connection

  GatedPeer* = object
    switch*: Switch
    peer*: RemotePeerInfo
    wire*: GatedTransport
    mux: Yamux
    peerSwitch: Switch

method write*(
    s: GatedTransport, msg: sink seq[byte]
): Future[void] {.async: (raises: [CancelledError, LPStreamError]).} =
  if s.blockWrites:
    s.blockedWriteSeen.fire()
    await s.release.wait()
  else:
    s.requestWritten.fire()

method dial(
    self: FixedDialer,
    peerId: PeerId,
    addrs: seq[MultiAddress],
    protos: seq[string],
    forceDial = false,
): Future[Stream] {.async: (raises: [DialFailedError, CancelledError]).} =
  return self.stream

proc newGatedPeer*(): Future[GatedPeer] {.async.} =
  let clientSwitch = newStandardSwitch()
  let peerSwitch = newStandardSwitch()
  let wire = GatedTransport(
    peerId: peerSwitch.peerInfo.peerId,
    dir: Direction.Out,
    requestWritten: newAsyncEvent(),
    blockedWriteSeen: newAsyncEvent(),
    release: newAsyncEvent(),
  )
  wire.initStream()
  let mux = Yamux.new(wire)
  let stream = await mux.newStream(lazy = true)
  clientSwitch.dialer = FixedDialer(stream: stream)
  return GatedPeer(
    switch: clientSwitch,
    peer: peerSwitch.peerInfo.toRemotePeerInfo(),
    wire: wire,
    mux: mux,
    peerSwitch: peerSwitch,
  )

proc close*(gated: GatedPeer) {.async.} =
  gated.wire.release.fire()
  await gated.mux.close()
  await gated.switch.stop()
  await gated.peerSwitch.stop()

## Waku layer API — debug / info operations.
{.push raises: [].}

import results, chronos, chronicles, metrics
import eth/p2p/discoveryv5/enr
import libp2p/peerid # pull PeerId pretty string formatting

import logos_delivery/waku/waku
import logos_delivery/waku/[waku_core, node/waku_node]

proc version*(self: Waku): Future[Result[string, string]] {.async.} =
  return ok(WakuNodeVersionString)

proc listenAddresses*(self: Waku): Future[Result[seq[string], string]] {.async.} =
  try:
    return ok(self.node.info().listenAddresses)
  except CatchableError as e:
    return err(e.msg)

proc myEnr*(self: Waku): Future[Result[string, string]] {.async.} =
  try:
    return ok(self.node.enr.toURI())
  except CatchableError as e:
    return err(e.msg)

proc myPeerId*(self: Waku): Future[Result[string, string]] {.async.} =
  try:
    return ok($self.node.peerId())
  except CatchableError as e:
    return err(e.msg)

proc mixPoolSize*(self: Waku): Future[Result[int, string]] {.async.} =
  ## The number of mix nodes this node could route a packet through right now.
  ##
  ## A number rather than a verdict, deliberately. `MinMixPoolSize` is the
  ## fewest a path can be built from, but how much anonymity a pool of that
  ## size buys is the consumer's judgement to make, not this layer's: an
  ## adversary who runs part of a small pool sees a large share of the hops.
  ## Reporting the count lets a caller set its own bar; reporting "ready" would
  ## make us pick one threshold for every consumer, forever.
  ##
  ## `err` when mix is not mounted, which is a different thing from a pool of
  ## zero: one is a configuration, the other is a node still finding peers.
  if self.node.wakuMix.isNil():
    return err("mix is not mounted on this node")
  return ok(self.node.getMixNodePoolSize())

proc metrics*(self: Waku): Future[Result[string, string]] {.async.} =
  {.gcsafe.}:
    try:
      return ok(defaultRegistry.toText())
    except CatchableError as e:
      return err(e.msg)

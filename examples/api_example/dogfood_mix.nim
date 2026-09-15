## dogfood_mix.nim -- live mix dogfooding against the logos.dev fleet.
##
## Runs the full messaging stack with anonymityLevel=Required (mix-only send
## path) on a Logos preset whose mix pool is seeded by the #4219 changeset in
## this tree. Every send must cross the mixnet; the proof of a full loop is
## this node receiving its own message back from the network: send()
## auto-subscribes to the content topic, and the only party that publishes
## the message to relay is the mix exit node.
##
## Verdict line at the end:
##   DOGFOOD VERDICT: OK        -- >=1 message went sender->mixnet->exit->relay->back
##   DOGFOOD VERDICT: PARTIAL   -- exit acknowledged (propagated) but never seen on relay
##   DOGFOOD VERDICT: FAIL      -- nothing crossed the mixnet
##
## Usage: dogfood_mix [natStrategy] [runSeconds] [preset]
##   natStrategy  "any" (default) or "none" -- the NAT-vs-mix A/B knob
##   runSeconds   total run time, default 300
##   preset       default "logos.dev"

import std/[os, strutils, times]
import chronos, results
import stew/byteutils
import logos_delivery
import logos_delivery/api/conf/modes
import logos_delivery/api/conf/messaging_conf
import logos_delivery/api/conf/kernel_conf
import logos_delivery/api/conf/logos_delivery_conf
import logos_delivery/waku/api/debug
import logos_delivery/waku/api/publish
import logos_delivery/waku/api/store
import logos_delivery/waku/waku_core
import logos_delivery/waku/waku_store/common as store_common
import logos_delivery/waku/common/logging as ld_logging
import tools/confutils/cli_args

var
  sentCount = 0
  errorCount = 0
  propagatedCount = 0
  selfReceivedCount = 0
var mixProven = false

proc ts(): string =
  times.now().format("HH:mm:ss'.'fff")

proc main() {.async.} =
  # Local so the event closures capture it; a GC'd global would not be gcsafe.
  let runId = "DOGFOOD-MIX-" & $epochTime().int
  let natStrategy =
    if paramCount() >= 1:
      paramStr(1)
    else:
      "any"
  let runSeconds =
    if paramCount() >= 2:
      parseInt(paramStr(2))
    else:
      300
  let preset =
    if paramCount() >= 3:
      paramStr(3)
    else:
      "logos.dev"

  echo ts(),
    " dogfood run ",
    runId,
    " nat=",
    natStrategy,
    " preset=",
    preset,
    " runSeconds=",
    runSeconds

  var conf = defaultWakuNodeConf().valueOr:
    echo "FATAL default conf: ", error
    quit(QuitFailure)
  conf.entryLayer = EntryLayer.messaging
  conf.preset = preset
  conf.mix = Opt.some(true) # mount mix in the kernel; Required uses no other path
  conf.nat = natStrategy
  # Runtime level; the compile-time -d:chronicles_log_level only sets the floor.
  conf.logLevel = ld_logging.LogLevel.DEBUG

  applyMode(conf, LogosDeliveryMode.Core).isOkOr:
    echo "FATAL applyMode: ", error
    quit(QuitFailure)

  # Manual LogosDeliveryConf: the WakuNodeConf overload of LogosDelivery.new
  # cannot carry an anonymity level, so build the per-layer config directly.
  let ldConf = LogosDeliveryConf(
    kernelConf: KernelConf(conf),
    messagingConf:
      Opt.some(MessagingClientConf(anonymityLevel: Opt.some(AnonymityLevel.Required))),
    channelsConf: Opt.none(ReliableChannelManagerConf),
  )

  let node = (await LogosDelivery.new(ldConf)).valueOr:
    echo "FATAL node create: ", error
    quit(QuitFailure)
  echo ts(), " node created"

  (await node.start()).isOkOr:
    echo "FATAL node start: ", error
    quit(QuitFailure)
  echo ts(), " node started"

  let sentListener = MessageSentEvent.listen(
    proc(event: MessageSentEvent) {.async: (raises: []).} =
      sentCount.inc()
      echo ts(), " EVENT sent      req=", event.requestId, " hash=", event.messageHash
  ).valueOr:
    echo "FATAL listen sent: ", error
    quit(QuitFailure)

  let errorListener = MessageErrorEvent.listen(
    proc(event: MessageErrorEvent) {.async: (raises: []).} =
      errorCount.inc()
      echo ts(), " EVENT error     req=", event.requestId, " err=", event.error
  ).valueOr:
    echo "FATAL listen error: ", error
    quit(QuitFailure)

  let propagatedListener = MessagePropagatedEvent.listen(
    proc(event: MessagePropagatedEvent) {.async: (raises: []).} =
      propagatedCount.inc()
      echo ts(), " EVENT propagated req=", event.requestId, " hash=", event.messageHash
  ).valueOr:
    echo "FATAL listen propagated: ", error
    quit(QuitFailure)

  let receivedListener = MessageReceivedEvent.listen(
    proc(event: MessageReceivedEvent) {.async: (raises: []).} =
      let payload = string.fromBytes(event.message.payload)
      if payload.startsWith(runId & "-CTRL"):
        echo ts(), " EVENT self-received CONTROL hash=", event.messageHash
      elif payload.startsWith(runId):
        selfReceivedCount.inc()
        echo ts(), " EVENT SELF-RECEIVED hash=", event.messageHash, " payload=", payload
      else:
        echo ts(), " EVENT received (foreign) hash=", event.messageHash
  ).valueOr:
    echo "FATAL listen received: ", error
    quit(QuitFailure)

  defer:
    await MessageSentEvent.dropListener(sentListener)
    await MessageErrorEvent.dropListener(errorListener)
    await MessagePropagatedEvent.dropListener(propagatedListener)
    await MessageReceivedEvent.dropListener(receivedListener)

  let deadline = Moment.now() + chronos.seconds(runSeconds)
  var counter = 0
  var lastSend = Moment.now() - chronos.seconds(60)
  var lastPool = -1
  var controlSent = false
  let dogfoodShard = "/waku/2/rs/3/5" # what autosharding maps the topic to
  let dogfoodTopic = "/dogfood/1/mix/proto"

  while Moment.now() < deadline:
    # One control message over PLAIN lightpush (same pipe, no mix) after 60 s:
    # proves fleet lightpush + relay + our subscription verification path.
    if not controlSent and counter >= 3:
      controlSent = true
      let ctrlMsg = WakuMessage(
        payload: toBytes(runId & "-CTRL plain lightpush"),
        contentTopic: dogfoodTopic,
        version: 2,
        timestamp: getNowInNanosecondTime(),
      )
      let ctrlRes =
        await node.waku.lightpushPublishToAny(dogfoodShard, ctrlMsg, mixify = false)
      if ctrlRes.isOk():
        echo ts(), " CONTROL plain lightpush accepted, peers=", ctrlRes.get()
      else:
        echo ts(), " CONTROL plain lightpush FAILED: ", $ctrlRes.error()
    let poolRes = await node.waku.mixPoolSize()
    let pool = poolRes.valueOr(-99)
    if pool != lastPool:
      echo ts(), " mixPoolSize=", pool
      lastPool = pool

    if Moment.now() - lastSend >= chronos.seconds(25):
      lastSend = Moment.now()
      counter.inc()
      let envelope = MessageEnvelope.init(
        contentTopic = "/dogfood/1/mix/proto",
        payload = runId & " #" & $counter & " " & ts(),
      )
      let reqId = (await node.messagingClient.send(envelope)).valueOr:
        echo ts(), " send #", counter, " REJECTED: ", error
        continue
      echo ts(), " send #", counter, " accepted req=", reqId

    await sleepAsync(chronos.seconds(5))

  # The decisive question: did ANY of it reach the network? Ask the fleet's
  # store for everything on the run's content topic.
  let storeRes = await node.waku.storeQueryToAny(
    StoreQueryRequest(
      requestId: runId & "-storecheck",
      includeData: true,
      pubsubTopic: Opt.some(dogfoodShard),
      contentTopics: @[dogfoodTopic],
      paginationLimit: Opt.some(100'u64),
    )
  )
  var storedMixed = 0
  var storedControl = 0
  var storedOther = 0
  if storeRes.isOk():
    for kv in storeRes.get().messages:
      let p =
        if kv.message.isSome():
          string.fromBytes(kv.message.get().payload)
        else:
          ""
      if p.startsWith(runId & "-CTRL"):
        storedControl.inc()
      elif p.startsWith(runId):
        storedMixed.inc()
      else:
        storedOther.inc()
    echo ts(),
      " STORE CHECK: mixed=",
      storedMixed,
      " control=",
      storedControl,
      " other=",
      storedOther,
      " (statusCode=",
      storeRes.get().statusCode,
      ")"
  else:
    echo ts(), " STORE CHECK FAILED: ", storeRes.error

  echo ts(), " ---- summary ----"
  echo "sends attempted:   ", counter
  echo "sent events:       ", sentCount
  echo "error events:      ", errorCount
  echo "propagated events: ", propagatedCount
  echo "self-received:     ", selfReceivedCount, " (mixed only; control excluded)"
  echo "stored mixed:      ", storedMixed, "   stored control: ", storedControl
  # A mixed message on a store node (or self-received back from the mesh)
  # proves sender->mixnet->exit->relay end to end. The plain control is only
  # the baseline that the pipe works at all -- never mix evidence. Keying the
  # verdict on it, as the first cut did, reported OK on a run where every mixed
  # send failed (control=1 -> OK).
  let mixReachedRelay = storedMixed > 0 or selfReceivedCount > 0
  let mixAcked = sentCount > 0 or propagatedCount > 0
  if mixReachedRelay:
    mixProven = true
    echo "DOGFOOD VERDICT: OK"
  elif mixAcked:
    echo "DOGFOOD VERDICT: PARTIAL (exit acked, never seen on relay/store)"
  else:
    echo "DOGFOOD VERDICT: FAIL (nothing mixed crossed)"

  (await node.stop()).isOkOr:
    echo ts(), " node stop error: ", error

when isMainModule:
  waitFor main()
  quit(if mixProven: QuitSuccess else: QuitFailure)

## Tasks, processors and network events for the tests of the send service.

import chronos, results, stew/byteutils
import
  logos_delivery/waku/[waku, waku_core],
  logos_delivery/api/types,
  logos_delivery/api/events/kernel_events,
  logos_delivery/messaging/delivery_service/send_service/[send_processor, delivery_task]
import ./futures

proc buildTask*(id: string, admitted = Moment.now()): DeliveryTask =
  ## An admitted task with no RLN.
  ## The id is the payload, so two tasks never have one hash.
  let msg = WakuMessage(
    contentTopic: "/test/1/send-service/proto",
    payload: id.toBytes(),
    timestamp: 1_700_000_000_000_000_000,
  )
  let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
  return DeliveryTask(
    requestId: RequestId(id),
    pubsubTopic: pubsubTopic,
    msg: msg,
    msgHash: computeMessageHash(pubsubTopic, msg),
    state: DeliveryState.Entry,
    firstAdmittedTime: Opt.some(admitted),
  )

proc receiveFromNetwork*(waku: Waku, task: DeliveryTask): Future[bool] {.async.} =
  ## The network gives this node the message of `task`.
  ## It returns true after a listener of the test got the event.
  ## The listener of the service then got it too.
  let delivered = newFuture[void]("copy delivered")
  let listener = MessageSeenEvent
    .listen(
      waku.brokerCtx,
      proc(event: MessageSeenEvent) {.async: (raises: []).} =
        if not delivered.finished():
          delivered.complete()
      ,
    )
    .expect("listen")
  MessageSeenEvent.emit(waku.brokerCtx, task.pubsubTopic, task.msg)
  let wasDelivered = await delivered.withTimeout(FUTURE_TIMEOUT)
  await MessageSeenEvent.dropListener(waku.brokerCtx, listener)
  return wasDelivered

type MixThenFallbackProcessor* = ref object of BaseSendProcessor
  ## A mix attempt that fails, then a hand-over to the plain path at the next pass.
  ## The mix processor does that when the mix window ends after a failed mix attempt.
  calls*: int

method deanonymizes(self: MixThenFallbackProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(
    self: MixThenFallbackProcessor, task: DeliveryTask
): bool {.gcsafe.} =
  return true

method sendImpl(
    self: MixThenFallbackProcessor, task: DeliveryTask
): Future[void] {.async.} =
  inc self.calls
  if self.calls == 1:
    task.anonymized = true
    task.state = DeliveryState.NextRoundRetry
  else:
    task.state = DeliveryState.FallbackRetry

type SlotProcessor* = ref object of BaseSendProcessor
  ## The task "blocker" keeps the send slot until `finish` fires.
  ## Each other task makes a mix attempt with no answer, and `others` counts them.
  finish*: AsyncEvent
  blockerStarted*: bool
  others*: int

method deanonymizes(self: SlotProcessor): bool {.gcsafe.} =
  return false

method isValidProcessor(self: SlotProcessor, task: DeliveryTask): bool {.gcsafe.} =
  return true

method sendImpl(self: SlotProcessor, task: DeliveryTask): Future[void] {.async.} =
  if task.requestId == RequestId("blocker"):
    self.blockerStarted = true
    await self.finish.wait()
  else:
    inc self.others
    task.anonymized = true
  task.state = DeliveryState.NextRoundRetry

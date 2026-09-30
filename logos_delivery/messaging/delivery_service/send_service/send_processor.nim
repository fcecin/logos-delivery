import results, chronos
import brokers/broker_context
import logos_delivery/waku/waku, logos_delivery/waku/api/publish
import ./delivery_task

{.push raises: [].}

type BaseSendProcessor* = ref object of RootObj
  fallbackProcessor*: BaseSendProcessor
  brokerCtx*: BrokerContext

proc chain*(self: BaseSendProcessor, next: BaseSendProcessor) =
  self.fallbackProcessor = next

method isValidProcessor*(
    self: BaseSendProcessor, task: DeliveryTask
): bool {.base, gcsafe.} =
  return false

method sendImpl*(
    self: BaseSendProcessor, task: DeliveryTask
): Future[void] {.async, base.} =
  assert false, "Not implemented"

method canAttempt*(
    self: BaseSendProcessor, task: DeliveryTask
): bool {.base, gcsafe, raises: [].} =
  ## True when this processor can try to send `task` at this time.
  return true

proc chainCanAttempt*(self: BaseSendProcessor, task: DeliveryTask): bool =
  ## True when a processor of the chain can try to send `task` at this time.
  var processor = self
  while not processor.isNil():
    if processor.canAttempt(task):
      return true
    processor = processor.fallbackProcessor
  return false

proc parkForRlnProofRefresh*(task: DeliveryTask, waku: Waku, errorDesc: string) =
  ## The service refused the task's proof as RLN-invalid: its proof went stale
  ## against a moved merkle root. Schedules a background merkle-path refresh and
  ## clears the proof so the next round regenerates one against the refreshed
  ## path; `attachRlnProof` reuses an existing proof, so without the clear the
  ## rejected bytes would be resent. Resetting admission charges the new nonce
  ## the regenerated proof draws. Without a backend refresh hook a retry would
  ## be rejected the same way, so the task fails with `errorDesc`.
  if not waku.onRlnProofRejected():
    task.state = DeliveryState.FailedToDeliver
    task.errorDesc = errorDesc
    task.deliveryTime = Moment.now()
    return
  task.msg.proof = @[]
  task.firstAdmittedTime = Opt.none(Moment)
  task.state = DeliveryState.NextRoundRetry

method process*(
    self: BaseSendProcessor, task: DeliveryTask
): Future[void] {.async, base.} =
  var currentProcessor: BaseSendProcessor = self
  var keepTrying = true
  while not currentProcessor.isNil() and keepTrying:
    if currentProcessor.isValidProcessor(task):
      await currentProcessor.sendImpl(task)
    currentProcessor = currentProcessor.fallbackProcessor
    keepTrying = task.state == DeliveryState.FallbackRetry

  # A task still in `FallbackRetry` exhausted the chain without delivering, and
  # one still in `Entry` was never attempted because no processor had a usable
  # peer yet (e.g. a lightpush peer that finishes registering right after the
  # first send). Both must be queued for the next round so the service loop
  # retries them; otherwise the task would sit untouched until it ages out.
  if task.state == DeliveryState.FallbackRetry or task.state == DeliveryState.Entry:
    task.state = DeliveryState.NextRoundRetry

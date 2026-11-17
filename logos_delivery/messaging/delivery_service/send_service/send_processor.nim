## A send processor runs the step of one transport state of the delivery plan.
## It makes one attempt and returns the result.

import results, chronos
import brokers/broker_context
import logos_delivery/waku/api/publish
import ./[delivery_task, delivery_plan]

{.push raises: [].}

type BaseSendProcessor* = ref object of RootObj
  brokerCtx*: BrokerContext

type SendProcessors* = object ## A nil field is a processor that the node does not have.
  mix*: BaseSendProcessor
  relay*: BaseSendProcessor
  lightpush*: BaseSendProcessor
  resender*: BaseSendProcessor ## Sends a message again after a Store miss.

method checkBeforeAdmission*(
    self: BaseSendProcessor, task: DeliveryTask
): Result[void, StepResult] {.base, gcsafe, raises: [].} =
  ## Refuses a message that can never go out, before it spends budget or a message id.
  ## Only a `Required` node asks, because mix is its only transport.
  return ok()

method checkBeforeAttempt*(
    self: BaseSendProcessor, task: DeliveryTask
): Result[void, StepResult] {.base, gcsafe, raises: [].} =
  ## Refuses an attempt that cannot work now, with no network call.
  return ok()

method attempt*(
    self: BaseSendProcessor, task: DeliveryTask
): Future[StepResult] {.async, base.} =
  raiseAssert "attempt is not implemented"

func transports*(processors: SendProcessors): set[DeliveryState] =
  var states: set[DeliveryState]
  if not processors.mix.isNil():
    states.incl(DeliveryState.Mix)
  if not processors.relay.isNil():
    states.incl(DeliveryState.Relay)
  if not processors.lightpush.isNil():
    states.incl(DeliveryState.Lightpush)
  return states

func processorOf*(processors: SendProcessors, state: DeliveryState): BaseSendProcessor =
  case state
  of DeliveryState.Mix: processors.mix
  of DeliveryState.Relay: processors.relay
  of DeliveryState.Lightpush: processors.lightpush
  of DeliveryState.StoreResend: processors.resender
  else: nil

func lightpushOutcome*(error: ErrorStatus): StepResult =
  ## Only the text tells an RLN rejection apart from another invalid message.
  let desc = error.desc.get($error.code)
  if error.isRlnRejection():
    return StepResult.init(StepOutcome.ProofRejected, desc)
  let outcome =
    case error.code
    of LightPushErrorCode.NO_PEERS_TO_RELAY: StepOutcome.ServiceNoRelayPeers
    of LightPushErrorCode.TOO_MANY_REQUESTS: StepOutcome.ServiceBusy
    of LightPushErrorCode.SERVICE_NOT_AVAILABLE: StepOutcome.ServiceUnavailable
    of LightPushErrorCode.INTERNAL_SERVER_ERROR: StepOutcome.ServiceError
    else: StepOutcome.Rejected
  return StepResult.init(outcome, desc)

func propagationOutcome*(peers: uint32): StepResult =
  if peers > 0:
    StepResult.init(StepOutcome.Propagated)
  else:
    StepResult.init(StepOutcome.ZeroPeers)

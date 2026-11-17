## The states of a send, and the plan that moves a send from state to state.
## The config builds the plan at startup, with a next step for each outcome of a state.
## `deliver` runs the step of the current state, and the outcome picks the next step.

{.push raises: [].}

import std/strutils, results
import logos_delivery/api/conf/modes

type
  DeliveryState* {.pure.} = enum
    Entry ## In the send queue, before `deliver` runs.
    Admission ## Charges the epoch budget once, then attaches the RLN proof.
    Relay
    Lightpush
    Mix
    StoreResend ## Lightpush again, after a Store node did not report the message.
    NextRoundRetry ## Waits for the next tick of the retry loop.
    AwaitingStore ## Waits for a Store node to report the message.
    Stored
    MixDelivered
    Propagated
    NotStored
    NotPropagated

  StepOutcome* {.pure.} = enum
    ## How a step ended, which is also the cause of a failed send.
    None
    Admitted
    OverBudget
    EphemeralDropped
    ProofNotReady
    ProofTransient
    ProofPermanent
    ProofBudgetExhausted
    ProofSizeUnknown
    Propagated
    ZeroPeers ## The publish worked and reached no peer.
    NoRelayPeers
    NoLightpushPeer
    ServiceNoRelayPeers ## The lightpush service has no relay peer.
    ServiceBusy
    ServiceUnavailable
    ServiceError
    ProofRejected
    Rejected ## A peer rejected the message for a reason that a retry cannot clear.
    AttemptTimedOut
    StepRaised
    TooLargeForMix
    MixNotMounted
    MixUnavailable
    MixSelfHop
    MixNoExit
    MixWindowElapsed
    MixNoReply ## The exit did not reply, so it may have published the message.
    RetryTick
    WindowElapsed
    ParkedTooLong
    StoreReported
    StoreMissed
    StoreWindowElapsed

  NextStep* {.pure.} = enum
    ## What comes after an outcome, a state or a rule that reads the task and the clock.
    Unset
    Relay
    Lightpush
    Mix
    NextRoundRetry
    AwaitingStore
    Stored
    NotStored
    NotPropagated
    StartRound
      ## `Admission` when the task needs a charge or a proof, else the first transport.
    AfterPropagation ## `MixDelivered`, `AwaitingStore` or `Propagated`.
    ResendOrWait
      ## `StoreResend` while the task can get one more resend, else `AwaitingStore`.
    GraceOrFail ## `NextRoundRetry` within `MixUnusableGrace`, else `NotPropagated`.
    RefreshOrFail
      ## `NextRoundRetry` when the RLN backend refreshes, else `NotPropagated`.

  StepResult* = object
    outcome*: StepOutcome
    desc*: string
      ## Text from a remote node or a library, for the log lines and the error text.

  DeliveryPlan* = object
    level*: AnonymityLevel
    chain*: seq[DeliveryState] ## The transports, in the order that a round tries them.
    nextSteps: array[DeliveryState, array[StepOutcome, NextStep]]

const
  RoundStates* = {
    DeliveryState.Admission, DeliveryState.Relay, DeliveryState.Lightpush,
    DeliveryState.Mix,
  } ## The states of one round, which ends at a wait or a final state.
  StoreWaitStates* = {DeliveryState.AwaitingStore, DeliveryState.StoreResend}
    ## The states that wait for a Store answer.
  FinalStates* = {
    DeliveryState.Stored, DeliveryState.MixDelivered, DeliveryState.Propagated,
    DeliveryState.NotStored, DeliveryState.NotPropagated,
  }

  RetryableServiceOutcomes = {
    StepOutcome.ZeroPeers, StepOutcome.NoLightpushPeer, StepOutcome.ServiceNoRelayPeers,
    StepOutcome.ServiceBusy, StepOutcome.ServiceUnavailable, StepOutcome.ServiceError,
  } ## No lightpush service could take the message now.
  MixUnusableOutcomes* = {
    StepOutcome.MixNotMounted, StepOutcome.MixUnavailable, StepOutcome.MixSelfHop,
    StepOutcome.MixNoExit,
  }
  LightpushOutcomes* =
    RetryableServiceOutcomes + {
      StepOutcome.Propagated, StepOutcome.ProofRejected, StepOutcome.Rejected,
      StepOutcome.StepRaised,
    } ## The outcomes of a lightpush attempt, also the attempt of a resend.
  StoreResendOutcomes = LightpushOutcomes + {StepOutcome.AttemptTimedOut}
  AdmissionOutcomes = {
    StepOutcome.Admitted, StepOutcome.OverBudget, StepOutcome.EphemeralDropped,
    StepOutcome.ProofNotReady, StepOutcome.ProofTransient, StepOutcome.ProofPermanent,
    StepOutcome.ProofBudgetExhausted, StepOutcome.ProofSizeUnknown,
    StepOutcome.TooLargeForMix, StepOutcome.StepRaised,
  }
  RelayOutcomes = {
    StepOutcome.Propagated, StepOutcome.ZeroPeers, StepOutcome.NoRelayPeers,
    StepOutcome.ProofRejected, StepOutcome.Rejected, StepOutcome.StepRaised,
    StepOutcome.WindowElapsed,
  }
  NonErrorOutcomes = {
    StepOutcome.None, StepOutcome.Admitted, StepOutcome.Propagated,
    StepOutcome.RetryTick, StepOutcome.WindowElapsed, StepOutcome.ParkedTooLong,
    StepOutcome.StoreReported, StepOutcome.StoreMissed, StepOutcome.StoreWindowElapsed,
  } ## The outcomes that do not tell why an attempt failed.

func init*(T: type StepResult, outcome: StepOutcome, desc = ""): T =
  T(outcome: outcome, desc: desc)

func isFinal*(state: DeliveryState): bool =
  state in FinalStates

func isError*(outcome: StepOutcome): bool =
  ## True when `outcome` tells why a step did not propagate the message.
  outcome notin NonErrorOutcomes

func nextStep*(
    plan: DeliveryPlan, state: DeliveryState, outcome: StepOutcome
): NextStep =
  plan.nextSteps[state][outcome]

func setNext(
    plan: var DeliveryPlan,
    state: DeliveryState,
    outcomes: set[StepOutcome],
    next: NextStep,
) =
  for outcome in outcomes:
    plan.nextSteps[state][outcome] = next

func nextStepOf(transport: DeliveryState): NextStep =
  case transport
  of DeliveryState.Relay: NextStep.Relay
  of DeliveryState.Lightpush: NextStep.Lightpush
  else: NextStep.Mix

func fallback(plan: DeliveryPlan, state: DeliveryState, afterLast: NextStep): NextStep =
  ## The transport after `state` in the chain, or `afterLast` when `state` is the last.
  let at = plan.chain.find(state)
  if at < 0 or at + 1 >= plan.chain.len:
    return afterLast
  return nextStepOf(plan.chain[at + 1])

func setTransportSteps(plan: var DeliveryPlan, state: DeliveryState) =
  plan.setNext(state, {StepOutcome.Propagated}, NextStep.AfterPropagation)
  plan.setNext(state, {StepOutcome.ProofRejected}, NextStep.RefreshOrFail)
  plan.setNext(
    state, {StepOutcome.Rejected, StepOutcome.WindowElapsed}, NextStep.NotPropagated
  )
  plan.setNext(state, {StepOutcome.StepRaised}, NextStep.NextRoundRetry)

func setMixSteps(plan: var DeliveryPlan, level: AnonymityLevel) =
  plan.setTransportSteps(DeliveryState.Mix)
  plan.setNext(
    DeliveryState.Mix,
    RetryableServiceOutcomes + {StepOutcome.MixNoReply},
    NextStep.NextRoundRetry,
  )
  if level == AnonymityLevel.Preferred:
    plan.setNext(
      DeliveryState.Mix,
      MixUnusableOutcomes + {StepOutcome.TooLargeForMix, StepOutcome.MixWindowElapsed},
      plan.fallback(DeliveryState.Mix, NextStep.NextRoundRetry),
    )
  else:
    plan.setNext(
      DeliveryState.Mix,
      {StepOutcome.TooLargeForMix, StepOutcome.MixNotMounted},
      NextStep.NotPropagated,
    )
    plan.setNext(
      DeliveryState.Mix,
      MixUnusableOutcomes - {StepOutcome.MixNotMounted},
      NextStep.GraceOrFail,
    )

func setAdmissionSteps(plan: var DeliveryPlan) =
  plan.setNext(
    DeliveryState.Admission, {StepOutcome.Admitted}, nextStepOf(plan.chain[0])
  )
  plan.setNext(
    DeliveryState.Admission,
    {
      StepOutcome.OverBudget, StepOutcome.ProofNotReady, StepOutcome.ProofTransient,
      StepOutcome.StepRaised,
    },
    NextStep.NextRoundRetry,
  )
  plan.setNext(
    DeliveryState.Admission,
    {
      StepOutcome.EphemeralDropped, StepOutcome.ProofPermanent,
      StepOutcome.ProofBudgetExhausted, StepOutcome.ProofSizeUnknown,
      StepOutcome.TooLargeForMix,
    },
    NextStep.NotPropagated,
  )

func setWaitSteps(plan: var DeliveryPlan) =
  plan.setNext(
    DeliveryState.NextRoundRetry, {StepOutcome.RetryTick}, NextStep.StartRound
  )
  plan.setNext(
    DeliveryState.NextRoundRetry,
    {StepOutcome.WindowElapsed, StepOutcome.ParkedTooLong},
    NextStep.NotPropagated,
  )
  plan.setNext(
    DeliveryState.AwaitingStore, {StepOutcome.StoreReported}, NextStep.Stored
  )
  plan.setNext(
    DeliveryState.AwaitingStore, {StepOutcome.StoreMissed}, NextStep.ResendOrWait
  )
  plan.setNext(
    DeliveryState.AwaitingStore, {StepOutcome.StoreWindowElapsed}, NextStep.NotStored
  )

func setResendSteps(plan: var DeliveryPlan) =
  plan.setNext(DeliveryState.StoreResend, StoreResendOutcomes, NextStep.AwaitingStore)
  plan.setNext(DeliveryState.StoreResend, {StepOutcome.StoreReported}, NextStep.Stored)
  plan.setNext(
    DeliveryState.StoreResend, {StepOutcome.StoreWindowElapsed}, NextStep.NotStored
  )

func buildDeliveryPlan*(
    level: AnonymityLevel, transports: set[DeliveryState], resend: bool
): Result[DeliveryPlan, string] =
  ## Builds the plan of `level` for the mounted `transports`.
  ## With `resend`, a message that Store does not report goes out again over lightpush.
  var plan = DeliveryPlan(level: level)
  case level
  of AnonymityLevel.Required:
    if DeliveryState.Mix notin transports:
      return err("The Required anonymity level needs mix")
    plan.chain = @[DeliveryState.Mix]
  of AnonymityLevel.Preferred:
    if DeliveryState.Mix notin transports:
      return err("The Preferred anonymity level needs mix")
    plan.chain.add(DeliveryState.Mix)
  of AnonymityLevel.None:
    discard
  if level != AnonymityLevel.Required:
    for state in [DeliveryState.Relay, DeliveryState.Lightpush]:
      if state in transports:
        plan.chain.add(state)
  if plan.chain.len == 0:
    return err("No valid send processor found for the delivery task")

  plan.setAdmissionSteps()
  plan.setWaitSteps()
  for state in plan.chain:
    case state
    of DeliveryState.Mix:
      plan.setMixSteps(level)
    of DeliveryState.Relay:
      plan.setTransportSteps(state)
      plan.setNext(
        state,
        {StepOutcome.NoRelayPeers, StepOutcome.ZeroPeers},
        plan.fallback(state, NextStep.NotPropagated),
      )
    of DeliveryState.Lightpush:
      plan.setTransportSteps(state)
      plan.setNext(state, RetryableServiceOutcomes, NextStep.NextRoundRetry)
    else:
      discard

  if resend and level != AnonymityLevel.Required:
    plan.setResendSteps()
  return ok(plan)

func hasResend*(plan: DeliveryPlan): bool =
  plan.nextSteps[DeliveryState.StoreResend][StepOutcome.StoreWindowElapsed] !=
    NextStep.Unset

func stepOutcomes*(plan: DeliveryPlan, state: DeliveryState): set[StepOutcome] =
  ## The outcomes that the step of `state` can return in `plan`.
  case state
  of DeliveryState.Admission:
    AdmissionOutcomes
  of DeliveryState.Relay:
    RelayOutcomes
  of DeliveryState.Lightpush:
    LightpushOutcomes + {StepOutcome.WindowElapsed}
  of DeliveryState.Mix:
    let mixOutcomes =
      LightpushOutcomes + MixUnusableOutcomes +
      {StepOutcome.TooLargeForMix, StepOutcome.MixNoReply, StepOutcome.WindowElapsed}
    if plan.level == AnonymityLevel.Preferred:
      mixOutcomes + {StepOutcome.MixWindowElapsed}
    else:
      mixOutcomes
  of DeliveryState.StoreResend:
    StoreResendOutcomes + {StepOutcome.StoreReported, StepOutcome.StoreWindowElapsed}
  of DeliveryState.NextRoundRetry:
    {StepOutcome.RetryTick, StepOutcome.WindowElapsed, StepOutcome.ParkedTooLong}
  of DeliveryState.AwaitingStore:
    {StepOutcome.StoreReported, StepOutcome.StoreMissed, StepOutcome.StoreWindowElapsed}
  else:
    {}

func `$`*(plan: DeliveryPlan): string =
  ## One line for each state and next step, with the outcomes that lead there.
  var lines: seq[string]
  for state in DeliveryState:
    for next in NextStep:
      if next == NextStep.Unset:
        continue
      var outcomes: seq[string]
      for outcome in StepOutcome:
        if plan.nextSteps[state][outcome] == next:
          outcomes.add($outcome)
      if outcomes.len > 0:
        lines.add($state & " " & outcomes.join(", ") & " -> " & $next)
  return lines.join("\n")

func afterPropagation*(overMix, storeConfirms: bool): DeliveryState =
  ## A mixed send ends at the reply of the exit, as a Store query links it to this node.
  if not storeConfirms:
    DeliveryState.Propagated
  elif overMix:
    DeliveryState.MixDelivered
  else:
    DeliveryState.AwaitingStore

func fixedState*(next: NextStep): Opt[DeliveryState] =
  ## The state of a next step that does not depend on the task.
  case next
  of NextStep.Relay:
    Opt.some(DeliveryState.Relay)
  of NextStep.Lightpush:
    Opt.some(DeliveryState.Lightpush)
  of NextStep.Mix:
    Opt.some(DeliveryState.Mix)
  of NextStep.NextRoundRetry:
    Opt.some(DeliveryState.NextRoundRetry)
  of NextStep.AwaitingStore:
    Opt.some(DeliveryState.AwaitingStore)
  of NextStep.Stored:
    Opt.some(DeliveryState.Stored)
  of NextStep.NotStored:
    Opt.some(DeliveryState.NotStored)
  of NextStep.NotPropagated:
    Opt.some(DeliveryState.NotPropagated)
  else:
    Opt.none(DeliveryState)

{.used.}

import results, testutils/unittests

import
  logos_delivery/api/conf/modes,
  logos_delivery/messaging/delivery_service/send_service/delivery_plan

## Tests of the plans that the configs build.
## `deliver` follows these next steps, and an outcome with no next step fails the send.

proc planOf(
    level: AnonymityLevel, transports: set[DeliveryState], resend = true
): DeliveryPlan =
  buildDeliveryPlan(level, transports, resend).expect("delivery plan")

const
  PlainTransports = {DeliveryState.Relay, DeliveryState.Lightpush}
  AllTransports = {DeliveryState.Mix, DeliveryState.Relay, DeliveryState.Lightpush}

suite "Delivery plan":
  test "each outcome that a state can return has a next step in each plan":
    for level in AnonymityLevel:
      for transports in [
        {DeliveryState.Relay},
        {DeliveryState.Lightpush},
        PlainTransports,
        {DeliveryState.Mix},
        AllTransports,
        {DeliveryState.Mix, DeliveryState.Lightpush},
      ]:
        for resend in [false, true]:
          let built = buildDeliveryPlan(level, transports, resend)
          if built.isErr():
            check:
              if level == AnonymityLevel.None:
                transports * PlainTransports == {}
              else:
                DeliveryState.Mix notin transports
            continue
          let plan = built.get()
          var states = plan.chain
          states.add(
            [
              DeliveryState.Admission, DeliveryState.NextRoundRetry,
              DeliveryState.AwaitingStore,
            ]
          )
          if plan.hasResend():
            states.add(DeliveryState.StoreResend)
          for state in states:
            for outcome in plan.stepOutcomes(state):
              checkpoint($level & " " & $transports & " " & $state & " " & $outcome)
              check plan.nextStep(state, outcome) != NextStep.Unset

  test "a final state has no next step":
    let plan = planOf(AnonymityLevel.Preferred, AllTransports)
    for state in FinalStates:
      for outcome in StepOutcome:
        check plan.nextStep(state, outcome) == NextStep.Unset

  test "the config sets the chain":
    check:
      planOf(AnonymityLevel.None, PlainTransports).chain ==
        @[DeliveryState.Relay, DeliveryState.Lightpush]
      planOf(AnonymityLevel.None, {DeliveryState.Relay}).chain == @[DeliveryState.Relay]
      planOf(AnonymityLevel.None, {DeliveryState.Lightpush}).chain ==
        @[DeliveryState.Lightpush]
      planOf(AnonymityLevel.Preferred, AllTransports).chain ==
        @[DeliveryState.Mix, DeliveryState.Relay, DeliveryState.Lightpush]
      planOf(AnonymityLevel.Required, AllTransports).chain == @[DeliveryState.Mix]
      buildDeliveryPlan(AnonymityLevel.Required, PlainTransports, true).isErr()
      buildDeliveryPlan(AnonymityLevel.Preferred, PlainTransports, true).isErr()
      buildDeliveryPlan(AnonymityLevel.None, {}, true).isErr()

  test "Required has no next step to the plain path and no resend":
    let plan = planOf(AnonymityLevel.Required, AllTransports)
    check not plan.hasResend()
    for state in DeliveryState:
      for outcome in StepOutcome:
        check plan.nextStep(state, outcome) notin {NextStep.Relay, NextStep.Lightpush}

  test "Required waits for an unusable mix and fails a message too large for mix":
    let plan = planOf(AnonymityLevel.Required, {DeliveryState.Mix})
    for outcome in MixUnusableOutcomes - {StepOutcome.MixNotMounted}:
      check plan.nextStep(DeliveryState.Mix, outcome) == NextStep.GraceOrFail
    check:
      plan.nextStep(DeliveryState.Mix, StepOutcome.MixNotMounted) ==
        NextStep.NotPropagated
      plan.nextStep(DeliveryState.Mix, StepOutcome.TooLargeForMix) ==
        NextStep.NotPropagated
      plan.nextStep(DeliveryState.Admission, StepOutcome.TooLargeForMix) ==
        NextStep.NotPropagated

  test "Preferred hands the task to the plain path when mix cannot take it":
    let plan = planOf(AnonymityLevel.Preferred, AllTransports)
    for outcome in MixUnusableOutcomes +
        {StepOutcome.TooLargeForMix, StepOutcome.MixWindowElapsed}:
      check plan.nextStep(DeliveryState.Mix, outcome) == NextStep.Relay
    check plan.nextStep(DeliveryState.Mix, StepOutcome.MixNoReply) ==
      NextStep.NextRoundRetry

  test "relay with no peers falls back to lightpush and fails without lightpush":
    let both = planOf(AnonymityLevel.None, PlainTransports)
    let relayOnly = planOf(AnonymityLevel.None, {DeliveryState.Relay}, resend = false)
    check:
      both.nextStep(DeliveryState.Relay, StepOutcome.NoRelayPeers) == NextStep.Lightpush
      relayOnly.nextStep(DeliveryState.Relay, StepOutcome.NoRelayPeers) ==
        NextStep.NotPropagated

  test "a retry starts a new round and the end of the window fails the task":
    let plan = planOf(AnonymityLevel.None, PlainTransports)
    check:
      plan.nextStep(DeliveryState.NextRoundRetry, StepOutcome.RetryTick) ==
        NextStep.StartRound
      plan.nextStep(DeliveryState.Lightpush, StepOutcome.ServiceBusy) ==
        NextStep.NextRoundRetry
      plan.nextStep(DeliveryState.Relay, StepOutcome.WindowElapsed) ==
        NextStep.NotPropagated
      plan.nextStep(DeliveryState.NextRoundRetry, StepOutcome.ParkedTooLong) ==
        NextStep.NotPropagated

  test "a Store miss gives a resend and each resend outcome waits for Store again":
    let plan = planOf(AnonymityLevel.None, PlainTransports)
    check plan.nextStep(DeliveryState.AwaitingStore, StepOutcome.StoreMissed) ==
      NextStep.ResendOrWait
    for outcome in LightpushOutcomes:
      check plan.nextStep(DeliveryState.StoreResend, outcome) == NextStep.AwaitingStore
    check:
      plan.nextStep(DeliveryState.StoreResend, StepOutcome.StoreReported) ==
        NextStep.Stored
      plan.nextStep(DeliveryState.StoreResend, StepOutcome.StoreWindowElapsed) ==
        NextStep.NotStored
      not planOf(AnonymityLevel.None, PlainTransports, resend = false).hasResend()

  test "only a plain message that a Store node confirms waits for Store":
    check:
      afterPropagation(overMix = false, storeConfirms = true) ==
        DeliveryState.AwaitingStore
      afterPropagation(overMix = true, storeConfirms = true) ==
        DeliveryState.MixDelivered
      afterPropagation(overMix = false, storeConfirms = false) ==
        DeliveryState.Propagated
      afterPropagation(overMix = true, storeConfirms = false) == DeliveryState.Propagated

  test "the plan of a node with relay and lightpush":
    check $planOf(AnonymityLevel.None, PlainTransports) == """
Admission Admitted -> Relay
Admission OverBudget, ProofNotReady, ProofTransient, StepRaised -> NextRoundRetry
Admission EphemeralDropped, ProofPermanent, ProofBudgetExhausted, ProofSizeUnknown, TooLargeForMix -> NotPropagated
Relay ZeroPeers, NoRelayPeers -> Lightpush
Relay StepRaised -> NextRoundRetry
Relay Rejected, WindowElapsed -> NotPropagated
Relay Propagated -> AfterPropagation
Relay ProofRejected -> RefreshOrFail
Lightpush ZeroPeers, NoLightpushPeer, ServiceNoRelayPeers, ServiceBusy, ServiceUnavailable, ServiceError, StepRaised -> NextRoundRetry
Lightpush Rejected, WindowElapsed -> NotPropagated
Lightpush Propagated -> AfterPropagation
Lightpush ProofRejected -> RefreshOrFail
StoreResend Propagated, ZeroPeers, NoLightpushPeer, ServiceNoRelayPeers, ServiceBusy, ServiceUnavailable, ServiceError, ProofRejected, Rejected, AttemptTimedOut, StepRaised -> AwaitingStore
StoreResend StoreReported -> Stored
StoreResend StoreWindowElapsed -> NotStored
NextRoundRetry WindowElapsed, ParkedTooLong -> NotPropagated
NextRoundRetry RetryTick -> StartRound
AwaitingStore StoreReported -> Stored
AwaitingStore StoreWindowElapsed -> NotStored
AwaitingStore StoreMissed -> ResendOrWait"""

  test "the plan of a Preferred node":
    check $planOf(AnonymityLevel.Preferred, AllTransports) == """
Admission Admitted -> Mix
Admission OverBudget, ProofNotReady, ProofTransient, StepRaised -> NextRoundRetry
Admission EphemeralDropped, ProofPermanent, ProofBudgetExhausted, ProofSizeUnknown, TooLargeForMix -> NotPropagated
Relay ZeroPeers, NoRelayPeers -> Lightpush
Relay StepRaised -> NextRoundRetry
Relay Rejected, WindowElapsed -> NotPropagated
Relay Propagated -> AfterPropagation
Relay ProofRejected -> RefreshOrFail
Lightpush ZeroPeers, NoLightpushPeer, ServiceNoRelayPeers, ServiceBusy, ServiceUnavailable, ServiceError, StepRaised -> NextRoundRetry
Lightpush Rejected, WindowElapsed -> NotPropagated
Lightpush Propagated -> AfterPropagation
Lightpush ProofRejected -> RefreshOrFail
Mix TooLargeForMix, MixNotMounted, MixUnavailable, MixSelfHop, MixNoExit, MixWindowElapsed -> Relay
Mix ZeroPeers, NoLightpushPeer, ServiceNoRelayPeers, ServiceBusy, ServiceUnavailable, ServiceError, StepRaised, MixNoReply -> NextRoundRetry
Mix Rejected, WindowElapsed -> NotPropagated
Mix Propagated -> AfterPropagation
Mix ProofRejected -> RefreshOrFail
StoreResend Propagated, ZeroPeers, NoLightpushPeer, ServiceNoRelayPeers, ServiceBusy, ServiceUnavailable, ServiceError, ProofRejected, Rejected, AttemptTimedOut, StepRaised -> AwaitingStore
StoreResend StoreReported -> Stored
StoreResend StoreWindowElapsed -> NotStored
NextRoundRetry WindowElapsed, ParkedTooLong -> NotPropagated
NextRoundRetry RetryTick -> StartRound
AwaitingStore StoreReported -> Stored
AwaitingStore StoreWindowElapsed -> NotStored
AwaitingStore StoreMissed -> ResendOrWait"""

  test "the plan of a Required node":
    check $planOf(AnonymityLevel.Required, {DeliveryState.Mix}) == """
Admission Admitted -> Mix
Admission OverBudget, ProofNotReady, ProofTransient, StepRaised -> NextRoundRetry
Admission EphemeralDropped, ProofPermanent, ProofBudgetExhausted, ProofSizeUnknown, TooLargeForMix -> NotPropagated
Mix ZeroPeers, NoLightpushPeer, ServiceNoRelayPeers, ServiceBusy, ServiceUnavailable, ServiceError, StepRaised, MixNoReply -> NextRoundRetry
Mix Rejected, TooLargeForMix, MixNotMounted, WindowElapsed -> NotPropagated
Mix Propagated -> AfterPropagation
Mix MixUnavailable, MixSelfHop, MixNoExit -> GraceOrFail
Mix ProofRejected -> RefreshOrFail
NextRoundRetry WindowElapsed, ParkedTooLong -> NotPropagated
NextRoundRetry RetryTick -> StartRound
AwaitingStore StoreReported -> Stored
AwaitingStore StoreWindowElapsed -> NotStored
AwaitingStore StoreMissed -> ResendOrWait"""

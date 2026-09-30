{.used.}

import results, chronos, testutils/unittests

import logos_delivery/waku/waku_core
import logos_delivery/messaging/delivery_service/send_service/delivery_task

const MaxTime = chronos.minutes(1)

proc taskWith(admitted, propagated: Opt[Moment]): DeliveryTask =
  ## Builds a DeliveryTask directly (bypassing `new`, which needs a broker).
  return DeliveryTask(firstAdmittedTime: admitted, firstPropagatedTime: propagated)

suite "DeliveryTask - delivery-timeout reaping":
  test "a task parked for budget (never admitted) is exempt, however old":
    ## Budget-parked tasks must survive to the epoch roll, not be aged out.
    let task = taskWith(Opt.none(Moment), Opt.none(Moment))
    check not task.isDeliveryTimedOut(MaxTime)

  test "an admitted, never-propagated task past the window times out":
    let task = taskWith(Opt.some(Moment.now() - chronos.minutes(2)), Opt.none(Moment))
    check task.isDeliveryTimedOut(MaxTime)

  test "an admitted task still within the window does not time out":
    let task = taskWith(Opt.some(Moment.now()), Opt.none(Moment))
    check not task.isDeliveryTimedOut(MaxTime)

  test "a propagated task is never timed out here (store validation owns it)":
    let task =
      taskWith(Opt.some(Moment.now() - chronos.minutes(2)), Opt.some(Moment.now()))
    check not task.isDeliveryTimedOut(MaxTime)

  test "the timeout clock runs from admission, not message creation":
    ## A just-admitted task has a fresh clock, even after a long budget wait.
    let task = taskWith(Opt.some(Moment.now()), Opt.none(Moment))
    check task.admissionAge() < MaxTime
    check not task.isDeliveryTimedOut(MaxTime)

suite "DeliveryTask - new timestamp before a send attempt":
  const MaxAge = chronos.seconds(10)

  proc taskAged(age: timer.Duration): DeliveryTask =
    let msg = WakuMessage(
      contentTopic: "/test/1/restamp/proto",
      payload: @[byte 1, 2, 3],
      timestamp: getNowInNanosecondTime() - age.nanoseconds,
      proof: @[byte 9, 9],
    )
    let pubsubTopic = PubsubTopic("/waku/2/rs/3/0")
    return DeliveryTask(
      pubsubTopic: pubsubTopic, msg: msg, msgHash: computeMessageHash(pubsubTopic, msg)
    )

  test "an old message that never propagated gets a new timestamp and hash":
    let task = taskAged(chronos.seconds(30))
    let oldHash = task.msgHash
    check:
      task.restampIfOld(MaxAge)
      task.messageAge() < MaxAge
      task.msg.proof.len == 0
      task.msgHash != oldHash
      task.msgHash == computeMessageHash(task.pubsubTopic, task.msg)

  test "a recent message keeps its timestamp":
    let task = taskAged(chronos.seconds(1))
    let oldTimestamp = task.msg.timestamp
    check:
      not task.restampIfOld(MaxAge)
      task.msg.timestamp == oldTimestamp

  test "a message that propagated keeps its timestamp":
    let task = taskAged(chronos.seconds(30))
    task.firstPropagatedTime = Opt.some(Moment.now())
    let oldHash = task.msgHash
    check:
      not task.restampIfOld(MaxAge)
      task.msgHash == oldHash

  test "a message whose send attempt got no answer keeps its timestamp":
    let task = taskAged(chronos.seconds(30))
    task.outcomeUnknown = true
    let oldHash = task.msgHash
    check:
      not task.restampIfOld(MaxAge)
      task.msgHash == oldHash

  test "a message that went to a mix exit keeps its timestamp":
    let task = taskAged(chronos.seconds(30))
    task.anonymized = true
    let oldHash = task.msgHash
    check:
      not task.restampIfOld(MaxAge)
      task.msgHash == oldHash

suite "DeliveryTask - max parked age":
  test "the age counts from the call to send, not from the message timestamp":
    let old = DeliveryTask(
      createdAt: Opt.some(Moment.now() - chronos.seconds(60)),
      msg: WakuMessage(timestamp: getNowInNanosecondTime()),
    )
    let fresh = DeliveryTask(
      createdAt: Opt.some(Moment.now()),
      msg: WakuMessage(
        timestamp: getNowInNanosecondTime() - chronos.seconds(60).nanoseconds
      ),
    )
    check:
      old.isParkedExpired(chronos.seconds(30))
      not fresh.isParkedExpired(chronos.seconds(30))

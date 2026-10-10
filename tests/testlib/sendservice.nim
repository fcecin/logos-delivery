import chronos

import logos_delivery/messaging/delivery_service/send_service/send_service
import ./futures

proc runUntilIdleInTime*(service: SendService): Future[bool] {.async.} =
  ## Runs `runUntilIdle` with a time limit. So a scheduler that never comes to
  ## rest fails the test, and does not hang it.
  return await service.runUntilIdle().withTimeout(FUTURE_TIMEOUT_MEDIUM)

type EventCounter* = ref object
  ## Counts events. The future of `waitCount` completes when the count gets to
  ## its value.
  value*: int
  waiters: seq[tuple[count: int, fut: Future[void]]]

proc inc*(counter: EventCounter) =
  ## Adds one, and completes each waiter whose count it reaches.
  inc counter.value
  for waiter in counter.waiters:
    if counter.value >= waiter.count and not waiter.fut.finished():
      waiter.fut.complete()

proc waitCount*(counter: EventCounter, count: int): Future[void] =
  ## Completes when the count gets to `count`. Each call gives a new future, so
  ## the time limit of a negative check cancels only its own future.
  let fut = newFuture[void]("wait-count")
  counter.waiters.add((count: count, fut: fut))
  if counter.value >= count:
    fut.complete()
  return fut

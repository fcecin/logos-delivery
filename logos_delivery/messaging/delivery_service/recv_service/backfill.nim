## Store catch-up of missed messages after a stop.
{.push raises: [].}

import std/[algorithm, sets, tables]
import chronos, chronicles, results, libp2p/protobuf/minprotobuf
import
  logos_delivery/waku/[waku_core, waku_store/common],
  logos_delivery/waku/common/paging,
  logos_delivery/waku/persistency/persistency
from logos_delivery/waku/waku_archive/archive import MaxMessageTimestampVariance

logScope:
  topics = "recv backfill"

const
  BackfillCategory* = "recv.backfill.v1"
    ## the receive service's category in the messaging layer's Persistency job
  LastOnlineKey* = key("last-online")
  TopicKeyTag = "topic"
  TopicKeyPrefix = key(TopicKeyTag)
  BackfillOverlap* = 2 * MaxMessageTimestampVariance
    ## Start the query 40 seconds before the saved time. Allow 20 seconds for
    ## messages that reach the archive late and 20 seconds for differences
    ## between this node's clock and the archive's clock.

type
  BackfillTopic* = tuple[pubsubTopic: PubsubTopic, contentTopic: ContentTopic]

  BackfillQuery* = proc(
    request: StoreQueryRequest
  ): Future[Result[StoreQueryResponse, string]] {.gcsafe, raises: [].}
  BackfillDeliver* =
    proc(pubsubTopic: PubsubTopic, message: WakuMessage): bool {.gcsafe, raises: [].}
    ## True accepts the message, duplicates included. False ends the topic.

  TopicRecord* = object
    ## The catch-up state of one (shard, content topic) that the node
    ## subscribed. A live record shows that the topic is subscribed since `at`,
    ## and that the recovery hint covers it after `at`. An owed record shows
    ## that the app did not get the messages of the topic from `at`.
    at*: Timestamp
    owed*: bool

  SubscriptionChange* = object
    ## A subscription or an unsubscription, in the order of the kernel events.
    topic*: BackfillTopic
    subscribed*: bool
    at*: Timestamp ## the time of the change
    lastReceivedAt*: Timestamp
      ## the local time when the node last received a message, before the change

proc backfillTopics*(
    subscriptions: seq[(PubsubTopic, HashSet[ContentTopic])]
): seq[BackfillTopic] =
  ## The subscribed (shard, content topic) pairs, sorted.
  var topics: seq[BackfillTopic]
  for (pubsubTopic, contentTopics) in subscriptions:
    for contentTopic in contentTopics:
      topics.add((pubsubTopic, contentTopic))
  topics.sort()
  return topics

proc encodeTimestamp(at: Timestamp): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, uint64(at))
  pb.finish()
  return pb.buffer

proc decodeTimestamp(bytes: seq[byte]): Result[Timestamp, string] =
  let pb = initProtoBuffer(bytes)
  var raw: uint64
  let present = pb.getField(1, raw).valueOr:
    return err("timestamp: " & $error)
  if not present or raw == 0 or raw > uint64(int64.high):
    return err("timestamp is missing or out of range")
  return ok(Timestamp(raw))

func atStart*(record: TopicRecord, hint: Opt[Timestamp]): TopicRecord =
  ## The record at a service start. A live topic becomes owed from the later of
  ## its own `at` and the hint, because the hint covers it only after `at`. An
  ## owed topic keeps its `at`.
  if record.owed:
    return record
  return TopicRecord(at: max(record.at, hint.get(record.at)), owed: true)

func afterUnsubscribe*(record: TopicRecord, lastReceivedAt: Timestamp): TopicRecord =
  ## The record after the app unsubscribes the topic. A live record becomes
  ## owed from the later of its `at` and `lastReceivedAt`, so a later
  ## subscription gets the gap. An owed record does not change.
  if record.owed:
    return record
  return TopicRecord(at: max(record.at, lastReceivedAt), owed: true)

func topicKey(topic: BackfillTopic): Opt[Key] =
  ## None when a name is too long for a key.
  if topic.pubsubTopic.len > StringLenMax or topic.contentTopic.len > StringLenMax:
    return Opt.none(Key)
  return Opt.some(key(TopicKeyTag, topic.pubsubTopic, topic.contentTopic))

func decodeTopicKey(recordKey: Key): Opt[BackfillTopic] =
  ## The topic in a key from `topicKey`. None for a key of another form.
  let parts = recordKey.stringParts().valueOr:
    return Opt.none(BackfillTopic)
  if parts.len != 3 or parts[0] != TopicKeyTag or parts[1].len == 0 or parts[2].len == 0:
    return Opt.none(BackfillTopic)
  return Opt.some((parts[1], parts[2]))

func encodeTopicRecord(record: TopicRecord): seq[byte] =
  var pb = initProtoBuffer()
  pb.write(1, uint64(record.at))
  pb.write(2, uint64(ord(record.owed)))
  pb.finish()
  return pb.buffer

func decodeTopicRecord(bytes: seq[byte]): Result[TopicRecord, string] =
  let pb = initProtoBuffer(bytes)
  var at, owed: uint64
  let hasAt = pb.getField(1, at).valueOr:
    return err("topic record: " & $error)
  let hasOwed = pb.getField(2, owed).valueOr:
    return err("topic record: " & $error)
  if not hasAt or not hasOwed or at == 0 or at > uint64(int64.high) or owed > 1:
    return err("topic record is incomplete or out of range")
  return ok(TopicRecord(at: Timestamp(at), owed: owed == 1))

func topicRecordOp*(topic: BackfillTopic, record: TopicRecord): Opt[TxOp] =
  ## The write of a topic record. None for a topic whose names are too long
  ## for a key. Such a topic has no record, so it is new at each start.
  let recordKey = topicKey(topic).valueOr:
    return Opt.none(TxOp)
  return Opt.some(
    TxOp(
      category: BackfillCategory,
      key: recordKey,
      kind: txPut,
      payload: encodeTopicRecord(record),
    )
  )

func topicRecordOps(
    records: Table[BackfillTopic, TopicRecord], topics: openArray[BackfillTopic]
): seq[TxOp] =
  ## The writes of the records of `topics`, one for each topic.
  var seen: HashSet[BackfillTopic]
  var ops: seq[TxOp]
  for topic in topics:
    if topic in seen:
      continue
    seen.incl(topic)
    let op = topicRecordOp(topic, records.getOrDefault(topic))
    if op.isSome():
      ops.add(op.get())
  return ops

func recordsAtStart*(
    stored: openArray[(BackfillTopic, TopicRecord)], hint: Opt[Timestamp]
): (Table[BackfillTopic, TopicRecord], seq[TxOp]) =
  ## The records at a service start (see `atStart`), and the writes of the
  ## records that changed.
  var records: Table[BackfillTopic, TopicRecord]
  var changed: seq[BackfillTopic]
  for (topic, record) in stored:
    let started = record.atStart(hint)
    records[topic] = started
    if started != record:
      changed.add(topic)
  return (records, topicRecordOps(records, changed))

func applyChanges*(
    records: var Table[BackfillTopic, TopicRecord],
    subscribed: var HashSet[BackfillTopic],
    changes: openArray[SubscriptionChange],
    upgradeHint: Opt[Timestamp],
): seq[TxOp] =
  ## Applies the subscription changes in order to `records` and `subscribed`,
  ## and returns the writes of the records that changed. A topic with no
  ## record is new. Its subscription makes a live record from the time of the
  ## change, or an owed record from `upgradeHint` when it is set. An
  ## unsubscription applies `afterUnsubscribe`.
  var changed: seq[BackfillTopic]
  for change in changes:
    if change.subscribed:
      subscribed.incl(change.topic)
      if change.topic in records:
        continue
      records[change.topic] =
        if upgradeHint.isSome():
          TopicRecord(at: upgradeHint.get(), owed: true)
        else:
          TopicRecord(at: change.at, owed: false) # no history
      changed.add(change.topic)
    else:
      subscribed.excl(change.topic)
      if change.topic notin records:
        continue
      let record = records.getOrDefault(change.topic)
      let unsubscribed = record.afterUnsubscribe(change.lastReceivedAt)
      if unsubscribed != record:
        records[change.topic] = unsubscribed
        changed.add(change.topic)
  return topicRecordOps(records, changed)

func setLive*(
    records: var Table[BackfillTopic, TopicRecord],
    topics: openArray[BackfillTopic],
    at: Timestamp,
): seq[TxOp] =
  ## Makes the records of `topics` live from `at`, and returns their writes.
  for topic in topics:
    records[topic] = TopicRecord(at: at, owed: false)
  return topicRecordOps(records, topics)

proc readRecoveryHint*(
    job: persistency.Job
): Future[Result[Opt[Timestamp], string]] {.async: (raises: [CancelledError]).} =
  ## The stored recovery hint. An unreadable record logs a warning and reads
  ## as none.
  if job.isNil() or not job.running:
    return err("backfill persistency job is closed")
  let stored =
    try:
      (await job.get(BackfillCategory, LastOnlineKey)).valueOr:
        return err("read recovery hint: " & $error)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      return err("read recovery hint: " & e.msg)
  if stored.isNone():
    return ok(Opt.none(Timestamp))
  let at = decodeTimestamp(stored.get()).valueOr:
    warn "Failed to decode the Store catch-up recovery hint", error
    return ok(Opt.none(Timestamp))
  return ok(Opt.some(at))

proc writeRecoveryHint*(
    job: persistency.Job, at: Timestamp
) {.async: (raises: [CancelledError]).} =
  ## Fire-and-forget, as the Persistency write API is. A lost write costs
  ## extra history at the next start.
  try:
    await job.persistPut(BackfillCategory, LastOnlineKey, encodeTimestamp(at))
  except CancelledError as e:
    raise e
  except CatchableError as e:
    warn "Failed to write the Store catch-up recovery hint", error = e.msg

proc readTopicRecords*(
    job: persistency.Job, hint = Opt.none(Timestamp)
): Future[Result[seq[(BackfillTopic, TopicRecord)], string]] {.
    async: (raises: [CancelledError])
.} =
  ## The stored topic records. A record whose value does not decode logs a
  ## warning and counts as owed from `hint`. With no hint, or a key that does
  ## not decode, it is left out, and its topic is new at its next subscription.
  if job.isNil() or not job.running:
    return err("backfill persistency job is closed")
  let rows =
    try:
      (await job.scanPrefix(BackfillCategory, TopicKeyPrefix)).valueOr:
        return err("read topic records: " & $error)
    except CancelledError as e:
      raise e
    except CatchableError as e:
      return err("read topic records: " & e.msg)
  var records: seq[(BackfillTopic, TopicRecord)]
  for row in rows:
    let topic = decodeTopicKey(row.key).valueOr:
      warn "Failed to decode the key of a Store catch-up topic record"
      continue
    let record = decodeTopicRecord(row.payload).valueOr:
      warn "Failed to decode a Store catch-up topic record",
        pubsubTopic = topic.pubsubTopic, contentTopic = topic.contentTopic, error
      if hint.isNone():
        continue
      TopicRecord(at: hint.get(), owed: true)
    records.add((topic, record))
  return ok(records)

proc writeTopicRecords*(
    job: persistency.Job, ops: seq[TxOp]
) {.async: (raises: [CancelledError]).} =
  ## Fire-and-forget, as one transaction. The writes of a job apply in order.
  if ops.len == 0:
    return
  try:
    await job.persist(ops)
  except CancelledError as e:
    raise e
  except CatchableError as e:
    warn "Failed to write the Store catch-up topic records", error = e.msg

proc queryPage(
    query: BackfillQuery, request: StoreQueryRequest, queryTimeout: Duration
): Future[Result[StoreQueryResponse, string]] {.async: (raises: [CancelledError]).} =
  try:
    let pending = query(request)
    if not await pending.withTimeout(queryTimeout): # cancels the query
      return err("store query timed out")
    return pending.read()
  except CancelledError as e:
    raise e
  except CatchableError as e:
    return err("store query: " & e.msg)

proc acceptPage(
    topic: BackfillTopic,
    queryStart, queryStop: Timestamp,
    response: StoreQueryResponse,
    deliver: BackfillDeliver,
): Result[Opt[Timestamp], string] =
  ## Delivers one page in order. Returns the timestamp of the last message,
  ## or none when the range has no more rows. A bad row fails the page, and
  ## the rows before it stay delivered.
  var last = queryStart
  for row in response.messages:
    let message = row.message.valueOr:
      return err("store row without a message")
    if message.timestamp < last or message.timestamp >= queryStop:
      return err("store row out of order or outside the range")
    if not deliver(topic.pubsubTopic, message):
      return err("delivery declined")
    last = message.timestamp
  if response.paginationCursor.isNone():
    return ok(Opt.none(Timestamp)) # the range is exhausted
  if response.messages.len == 0:
    return err("store page is empty but claims more")
  if last == queryStart:
    # Every message in the page shares the query start, so a query from that
    # instant returns this same page. Step past the instant. The catch-up does
    # not deliver the messages at that instant beyond this page.
    debug "Store catch-up steps past a timestamp that fills a page",
      pubsubTopic = topic.pubsubTopic,
      contentTopic = topic.contentTopic,
      timestamp = last
    return ok(Opt.some(last + 1))
  return ok(Opt.some(last))

proc setStartIfMissing*(
    starts: TableRef[BackfillTopic, Timestamp],
    topic: BackfillTopic,
    record: TopicRecord,
) =
  ## Gives `topic` the start `record.at - BackfillOverlap` when it has no
  ## start. A topic that failed after its first page keeps its next page start.
  if topic notin starts:
    starts[topic] = record.at - BackfillOverlap

proc catchUpTopic*(
    topic: BackfillTopic,
    starts: TableRef[BackfillTopic, Timestamp],
    cutoff: Timestamp,
    queryTimeout: Duration,
    query: BackfillQuery,
    deliver: BackfillDeliver,
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Queries `topic` over `[starts[topic], cutoff)`, in windows no longer than
  ## the Store's `MaxQueryTimeRange` and page by page, until it has no more
  ## rows or fails. True when the topic has no more rows. A failed topic keeps
  ## its next page start in `starts` and continues from there in the next
  ## pass. A topic with no start has nothing to query.
  var start = starts.getOrDefault(topic, cutoff)
  while start < cutoff:
    let windowStop = min(cutoff, start + MaxQueryTimeRange)
    let request = StoreQueryRequest(
      includeData: true,
      pubsubTopic: Opt.some(topic.pubsubTopic),
      contentTopics: @[topic.contentTopic],
      startTime: Opt.some(start),
      endTime: Opt.some(windowStop), # exclusive on the wire
      paginationForward: PagingDirection.FORWARD,
      paginationLimit: Opt.some(MaxPageSize),
    )
    let response = await queryPage(query, request, queryTimeout)
    let accepted =
      if response.isOk():
        acceptPage(topic, start, windowStop, response.get(), deliver)
      else:
        Result[Opt[Timestamp], string].err(response.error)
    let next = accepted.valueOr:
      debug "Store catch-up query failed, the topic retries next pass",
        pubsubTopic = topic.pubsubTopic, contentTopic = topic.contentTopic, error
      starts[topic] = start
      return false
    start = next.valueOr:
      windowStop # the window is exhausted, move to the next one
  starts.del(topic)
  return true

{.pop.}

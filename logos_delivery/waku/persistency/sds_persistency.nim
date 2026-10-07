## Adapter that materialises the SDS `Persistence` contract (nim-sds 0.3.0,
## snapshot model) on top of a waku-persistency `Job`. One `Job` (== one
## SQLite file, one worker thread) services all channels for a given SDS
## context; rows are namespaced by category and the channelId is the first
## key component so per-channel prefix scans stay cheap.
##
## ## Snapshot contract (nim-sds 0.3.0)
##
## The fine-grained per-row callbacks of 0.2.4 are gone. SDS now persists via
## five procs, all `Future[Result[void, string]]` (load returns
## `Result[ChannelData, string]`), `{.async: (raises: []), gcsafe.}`:
##
##  * **`saveChannelMeta`** — the complete fast-changing per-channel state
##    (lamport clock, outgoing/incoming buffers, both SDS-R repair buffers)
##    as ONE blob. Idempotent; a missed write self-heals on the next save.
##  * **`updateHistory`** — append newly-delivered messages / evict the
##    oldest past the cap, applied as one transactional batch.
##  * **`loadChannel`** — bootstrap: returns the prior `ChannelData`
##    (meta + ordered message history) or an empty one. Surfaces errors.
##  * **`dropChannel`** — wipe all state for a channel. Surfaces errors.
##  
## Failure policy mirrors the interface docs: save/update/hint are non-fatal
## (we log and still return the error string); load/drop are durability-intent
## and propagate their error to the caller.
##
## ## Codec
##
## The blob transform is owned by nim-sds: `ChannelMeta` round-trips through
## `sds/snapshot_codec` (protobuf, schema-versioned — refuses unknown
## versions), and each persisted `SdsMessage` log row through the SDS wire
## codec in `sds/protobuf`. We do not maintain a second codec for these
## shapes (the previous `payload_codec`/`BlobCodec` path is retired).
##
## ## Retrieval hints
##
## `setRetrievalHint` is intentionally a no-op: persisted hints are never read
## back — `loadChannel` returns `ChannelData` (meta + messageHistory) with no
## hint field, and `ChannelMeta` carries none. Hints are supplied live via the
## `onRetrievalHint` provider, so persisting them would be write-only dead
## data. The closure still exists because the field is required by the
## `Persistence` object (SDS calls it from `getRecentHistoryEntries`).
##
## ## Storage layout
##
## | Category      | Key                      | Value                                  |
## |---------------|--------------------------|----------------------------------------|
## | `sds.meta`    | `key(channelId)`         | `ChannelMeta` (snapshot_codec protobuf)|
## | `sds.log`     | `key(channelId, msgId)`  | `SdsMessage` (sds wire protobuf)       |
## | `sds.migration` | `key(channelId, "wire-v0.4")` | marker row, the channel has no row of the format before `v0.4` |
##
## `messageHistory` is reconstructed in memory by sorting on
## `(lamportTimestamp, messageId)` — the same total order SDS uses for
## delivery (see sds/sds_utils.nim).
##
## ## Migration of old rows
##
## The nim-sds version `b12f5ee` wrote field 3 of an SDS message as nested
## history entries. The current nim-sds reads such a row with no error, but
## with wrong causal history ids. So, before a closure reads or writes the
## rows of a channel, it deletes the `sds.log` and `sds.meta` rows of that
## channel that have the old format, one time. nim-sds makes the first call
## at the first load of the reliable channel. One transaction deletes the
## rows and writes the marker row of the channel in `sds.migration`. When the
## marker row of a channel exists, the rows of the channel are not read
## again. A migration that fails runs again at the next call.
##
## The deletion loses the channel state of an old node. Reliable channels are
## not released, so no deployed state is lost.

{.push raises: [].}

import std/[algorithm, sequtils, sets, tables]
import chronos, chronicles, results, stew/byteutils
import ./persistency
import ./keys
import types/persistence
import snapshot_codec
import protobuf

export persistence, persistency

logScope:
  topics = "sds-persistency"

const
  CatMeta* = "sds.meta"
  CatLog* = "sds.log"
  CatMigration* = "sds.migration"
  MigrationMarkerTag = "wire-v0.4"
    ## The last key component of the marker row of each channel in
    ## `CatMigration`. `v0.4` is the nim-sds release that changed field 3 of
    ## the SDS message. If the row of a channel exists, no row of the channel
    ## in `CatLog` and `CatMeta` has the format before `v0.4`.
  MigrationMarkerPayload = @[1'u8] ## The value of a marker row.
  MigrationCommitTimeout = 30.seconds
    ## The longest wait for the marker row after the migration batch.
    ## `persist` puts the batch in a queue and does not wait for its commit.
  MigrationPollInterval = 10.milliseconds
    ## The time between two reads of the marker row during that wait.

# ── Migration of old rows ───────────────────────────────────────────────

proc isOldCausalId(id: SdsMessageID): bool =
  ## True when `id` is a history entry with a message id. The old nim-sds
  ## wrote a history entry in field 3 of an SDS message, where the current
  ## nim-sds writes a message id. So the current decoder reads the bytes of
  ## the entry as the id. A message id of logos-delivery is hex, so it has no
  ## tag byte of the message id field, and it does not decode as an entry.
  let entry = deserializeHistoryEntry(id.toBytes).valueOr:
    return false
  entry.messageId.len > 0

proc hasOldFormat(msg: SdsMessage): bool =
  msg.causalHistory.anyIt(it.messageId.isOldCausalId)

proc logRowHasOldFormat*(payload: seq[byte]): bool =
  ## True when an `sds.log` row has the format before `v0.4`. A row with no
  ## causal history has the same bytes in both formats.
  let msg = deserializeMessage(payload).valueOr:
    return false
  msg.hasOldFormat

proc metaRowHasOldFormat*(payload: seq[byte]): bool =
  ## True when an `sds.meta` row has the format before `v0.4`: a message in
  ## one of its buffers has it.
  let meta = ChannelMeta.decode(payload).valueOr:
    return false
  if meta.outgoingBuffer.anyIt(it.message.hasOldFormat):
    return true
  if meta.incomingBuffer.anyIt(it.message.hasOldFormat):
    return true
  for kv in meta.incomingRepairBuffer:
    let cached = deserializeMessage(kv.entry.cachedMessage).valueOr:
      continue
    if cached.hasOldFormat:
      return true
  false

proc migrationMarkerKey*(channelId: SdsChannelID): Key =
  ## The key of the marker row of `channelId` in `CatMigration`.
  key(channelId, MigrationMarkerTag)

proc waitForMarker*(
    job: Job, markerKey: Key
): Future[Result[void, string]] {.async: (raises: []).} =
  ## Waits until the marker row `markerKey` of `CatMigration` exists.
  ## `persist` does not wait for the commit. The marker is in the same
  ## transaction as the rows, so the rows are committed when it exists. The
  ## wait ends with an error when a read of the marker fails or after
  ## `MigrationCommitTimeout`. A read on a closed job fails at once.
  let deadline = Moment.now() + MigrationCommitTimeout
  try:
    while true:
      let present = (await job.exists(CatMigration, markerKey)).valueOr:
        return err("read marker: " & $error)
      if present:
        return ok()
      if Moment.now() > deadline:
        return err("the migration did not commit in " & $MigrationCommitTimeout)
      await sleepAsync(MigrationPollInterval)
  except CatchableError as e:
    return err(e.msg)

proc runMigration(
    job: Job, channelId: SdsChannelID
): Future[Result[void, string]] {.async: (raises: []).} =
  ## Deletes the rows of `channelId` that have the old nim-sds format, one
  ## time. All deletes of the channel and its marker row go in one
  ## transaction.
  let markerKey = migrationMarkerKey(channelId)
  let chanKey = toKey(channelId)
  try:
    let done = (await job.exists(CatMigration, markerKey)).valueOr:
      return err("read marker: " & $error)
    if done:
      return ok()

    var ops: seq[TxOp]
    let rows = (await job.scanPrefix(CatLog, chanKey)).valueOr:
      return err("scan " & CatLog & ": " & $error)
    for row in rows:
      if row.payload.logRowHasOldFormat():
        ops.add TxOp(category: CatLog, key: row.key, kind: txDelete)
    let meta = (await job.get(CatMeta, chanKey)).valueOr:
      return err("read " & CatMeta & ": " & $error)
    if meta.isSome() and meta.get().metaRowHasOldFormat():
      ops.add TxOp(category: CatMeta, key: chanKey, kind: txDelete)
    let deleted = ops.len
    ops.add TxOp(
      category: CatMigration,
      key: markerKey,
      kind: txPut,
      payload: MigrationMarkerPayload,
    )
    await job.persist(ops)
    ?(await waitForMarker(job, markerKey))
    if deleted > 0:
      info "Deleted the rows of the old nim-sds format", channelId, deleted
    return ok()
  except CatchableError as e:
    return err(e.msg)

proc runMigrationAndLog(
    job: Job, channelId: SdsChannelID, firstRun: bool
): Future[Result[void, string]] {.async: (raises: []).} =
  ## Runs the migration of a channel. Only the failure of the first run is a
  ## warning, so a store that stays broken does not log a warning at each
  ## call.
  let res = await runMigration(job, channelId)
  if res.isErr():
    if firstRun:
      warn "The migration of old SDS rows failed, the next call tries again",
        channelId, error = res.error
    else:
      debug "The migration of old SDS rows failed again", channelId, error = res.error
  return res

type ChannelMigration = object
  job: Job ## Keeps the job alive, so that no other job gets its address.
  migration: Future[Result[void, string]].Raising([])

var channelMigrations {.threadvar.}: Table[(pointer, string), ChannelMigration]
  ## One migration for each channel of a job. All adapters of the job share
  ## it. A job runs only on the thread of its node, so each thread has a table.
  ## An entry of a closed job stays until the next migration starts.

proc migrationFailed(migration: Future[Result[void, string]].Raising([])): bool =
  ## True when the migration finished with an error.
  migration.finished() and not (migration.completed() and migration.value().isOk())

proc removeClosedJobMigrations() =
  ## Removes the entries of the jobs that closed.
  var closed: seq[(pointer, string)]
  for id, entry in channelMigrations:
    if not entry.job.running:
      closed.add(id)
  for id in closed:
    channelMigrations.del(id)

proc migrationTableJobCount*(): int =
  ## The number of jobs that have an entry in the migration table of this
  ## thread. It is a diagnostic for the tests.
  var jobs: HashSet[pointer]
  for id in channelMigrations.keys:
    jobs.incl(id[0])
  jobs.len

proc getOrStartMigration(
    job: Job, channelId: SdsChannelID
): Future[Result[void, string]].Raising([]) =
  ## The migration of `channelId` on `job`. It starts one when the table has
  ## none. A migration that failed is not reused, so the next call starts a
  ## new one, which reads the marker first.
  let id = (cast[pointer](job), channelId)
  var firstRun = true
  channelMigrations.withValue(id, entry):
    if not entry.migration.migrationFailed():
      return entry.migration
    firstRun = false
  removeClosedJobMigrations()
  let migration = runMigrationAndLog(job, channelId, firstRun)
  channelMigrations[id] = ChannelMigration(job: job, migration: migration)
  return migration

# ── Public factory ──────────────────────────────────────────────────────

proc newSdsPersistence*(job: Job): Persistence {.gcsafe, raises: [].} =
  ## Build an SDS `Persistence` value backed by ``job``. One Job services
  ## all channels — channelId is part of every key.
  ##
  ## The closures capture ``job`` by ref. nim-sds calls them on the chronos
  ## loop of the node, where the reliable channels run.
  doAssert not job.isNil, "newSdsPersistence: job is nil"

  # Built field-by-field via assignment rather than an object literal: every
  # field is an async closure whose body uses `await`/`return` statements,
  # which cannot be followed by the `,` field separator a `Persistence(..)`
  # literal would require. Assignments have no separator, so bodies stay plain.
  var persistence = Persistence()

  # Each call waits for the migration of its channel before it reads or
  # writes a row. A cancelled call does not cancel the migration.
  proc ensureMigrated(
      channelId: SdsChannelID
  ): Future[Result[void, string]] {.async: (raises: []).} =
    return await noCancel(getOrStartMigration(job, channelId))

  persistence.saveChannelMeta = proc(
      channelId: SdsChannelID, meta: ChannelMeta
  ): Future[Result[void, string]] {.async: (raises: []), gcsafe.} =
    ?(await ensureMigrated(channelId))
    try:
      await job.persistPut(CatMeta, toKey(channelId), encode(meta).buffer)
      return ok()
    except CatchableError as e:
      warn "sds-persistency: saveChannelMeta failed", channelId, err = e.msg
      return err(e.msg)

  persistence.updateHistory = proc(
      channelId: SdsChannelID, update: HistoryUpdate
  ): Future[Result[void, string]] {.async: (raises: []), gcsafe.} =
    if update.isEmpty:
      return ok()
    ?(await ensureMigrated(channelId))
    # One transactional batch: append rows (txPut) and evictions (txDelete).
    var ops = newSeq[TxOp]()
    for m in update.append:
      let payload = serializeMessage(m).valueOr:
        return err("updateHistory: encode message: " & $error)
      ops.add TxOp(
        category: CatLog,
        key: key(channelId, m.messageId),
        kind: txPut,
        payload: payload,
      )
    for id in update.evict:
      ops.add TxOp(category: CatLog, key: key(channelId, id), kind: txDelete)
    try:
      await job.persist(ops)
      return ok()
    except CatchableError as e:
      warn "sds-persistency: updateHistory failed",
        channelId, appended = update.append.len, evicted = update.evict.len, err = e.msg
      return err(e.msg)

  persistence.loadChannel = proc(
      channelId: SdsChannelID
  ): Future[Result[ChannelData, string]] {.async: (raises: []), gcsafe.} =
    (await ensureMigrated(channelId)).isOkOr:
      return err("loadChannel: migration of old rows: " & error)
    let chanKey = toKey(channelId)
    var data = ChannelData.init()
    try:
      block meta:
        let opt = (await job.get(CatMeta, chanKey)).valueOr:
          return err("loadChannel: get meta: " & $error)
        if opt.isSome:
          # schema-versioned decode; refuses unknown versions loudly.
          data.meta = ChannelMeta.decode(opt.get).valueOr:
            return err("loadChannel: corrupt or unsupported ChannelMeta blob")

      block history:
        let rows = (await job.scanPrefix(CatLog, chanKey)).valueOr:
          return err("loadChannel: scan log: " & $error)
        var msgs = newSeq[SdsMessage]()
        for row in rows:
          let m = deserializeMessage(row.payload).valueOr:
            warn "sds-persistency: skipping undecodable log row", channelId
            continue
          msgs.add(m)
        msgs.sort do(a, b: SdsMessage) -> int:
          result = cmp(a.lamportTimestamp, b.lamportTimestamp)
          if result == 0:
            result = cmp(a.messageId, b.messageId)
        data.messageHistory = msgs

      return ok(data)
    except CatchableError as e:
      return err("loadChannel: " & e.msg)

  persistence.dropChannel = proc(
      channelId: SdsChannelID
  ): Future[Result[void, string]] {.async: (raises: []), gcsafe.} =
    ?(await ensureMigrated(channelId))
    let chanKey = toKey(channelId)
    try:
      await job.persist(
        @[
          TxOp(category: CatLog, key: chanKey, kind: txDeletePrefix),
          TxOp(category: CatMeta, key: chanKey, kind: txDelete),
        ]
      )
      return ok()
    except CatchableError as e:
      error "sds-persistency: dropChannel failed", channelId, err = e.msg
      return err(e.msg)

  return persistence

{.pop.}

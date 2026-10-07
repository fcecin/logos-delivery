{.used.}

## Tests of the one-time deletion of the SDS rows that the earlier nim-sds
## pin (`b12f5ee`) wrote. The fixtures are rows that nim-sds `b12f5ee`
## encoded. `LegacyLogRow` is an `sds.log` row (message `msg-1`), and
## `LegacyMetaRow` is an `sds.meta` row of channel `chan-1` with one message
## in each of its four buffers.

import std/[os, sets, times]
import chronos, results, stew/byteutils
import testutils/unittests
import logos_delivery/waku/persistency/persistency
import logos_delivery/waku/persistency/keys
import logos_delivery/waku/persistency/sds_persistency
import logos_delivery/waku/persistency/backend_comm
import sds/protobuf as sds_wire
import sds/snapshot_codec

const
  LegacyLogRow =
    "0a056d73672d3110071a160a056465702d6112030102031a0873656e6465722d611a070a05" &
    "6465702d6222066368616e2d312a030909093201bf3a0873656e6465722d316a110a056465" &
    "702d631a0873656e6465722d63"
  LegacyMetaRow =
    "0801102a1a620a570a056d73672d3110071a160a056465702d6112030102031a0873656e64" &
    "65722d611a070a056465702d6222066368616e2d312a030909093201bf3a0873656e646572" &
    "2d316a110a056465702d631a0873656e6465722d631080d095ffbc31180222350a2c0a056d" &
    "73672d3210081a140a056d73672d311201041a0873656e6465722d3122066368616e2d312a" &
    "0101320012056465702d782a230a056465702d63121a0a110a056465702d631a0873656e64" &
    "65722d6310a0dd9bffbc31327f0a056d73672d3112760a140a056d73672d311201051a0873" &
    "656e6465722d3112570a056d73672d3110071a160a056465702d6112030102031a0873656e" &
    "6465722d611a070a056465702d6222066368616e2d312a030909093201bf3a0873656e6465" &
    "722d316a110a056465702d631a0873656e6465722d6318c0eaa1ffbc31"
  # The hex of a hash, as logos-delivery makes a message id.
  HexId = "6c1f0a3e9d2b4c5a8e7f6d5c4b3a29180716253443526170819fa0b1c2d3e4f5"
  # A hex id that decodes as a history entry with unknown fields only. Each
  # `0a` pair is the tag of field 6 as a varint, and its value.
  OddHexId = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a"

proc tmpRoot(label: string): string =
  let p = getTempDir() / ("sds_migration_test_" & label & "_" & $epochTime().int)
  removeDir(p)
  p

proc pollExists(
    job: Job, category: string, k: Key, timeoutMs = 1000
): Future[bool] {.async.} =
  ## Polls until the row exists, because a write of the adapter does not wait
  ## for its commit.
  let deadline = epochTime() + (timeoutMs.float / 1000.0)
  while epochTime() < deadline:
    if (await job.exists(category, k)).get(false):
      return true
    await sleepAsync(chronos.milliseconds(2))
  return false

proc pollGone(
    job: Job, category: string, k: Key, timeoutMs = 1000
): Future[bool] {.async.} =
  ## Polls until the row is gone, because a write of the adapter does not wait
  ## for its commit.
  let deadline = epochTime() + (timeoutMs.float / 1000.0)
  while epochTime() < deadline:
    if not (await job.exists(category, k)).get(true):
      return true
    await sleepAsync(chronos.milliseconds(2))
  return false

proc currentMsg(messageId: string, causalIds: seq[string] = @[]): SdsMessage =
  ## A message of the current format with `causalIds` as its causal history.
  var causal: seq[HistoryEntry]
  for id in causalIds:
    causal.add(HistoryEntry.init(id, @[byte 4], "sender-1".SdsParticipantID))
  SdsMessage.init(
    messageId = messageId,
    lamportTimestamp = 9,
    causalHistory = causal,
    channelId = "chan-1",
    content = @[byte 1],
    bloomFilter = @[],
    repairRequest = @[HistoryEntry.init(HexId, @[], "sender-c".SdsParticipantID)],
  )

proc currentMeta(cachedMessage: seq[byte]): ChannelMeta =
  ## A channel meta of the current format with one entry in each buffer.
  ## `cachedMessage` is the cached message of the incoming repair entry.
  var meta = ChannelMeta.init()
  meta.lamportTimestamp = 42
  meta.outgoingBuffer.add(
    UnacknowledgedMessage.init(
      currentMsg("msg-1", @[HexId]), fromUnix(1_700_000_000), 2
    )
  )
  meta.incomingBuffer.add(
    IncomingMessage.init(currentMsg("msg-2", @[HexId]), toHashSet(@[HexId]))
  )
  meta.outgoingRepairBuffer.add(
    OutgoingRepairKV(
      messageId: HexId,
      entry: OutgoingRepairEntry.init(HistoryEntry.init(HexId), fromUnix(1_700_000_100)),
    )
  )
  meta.incomingRepairBuffer.add(
    IncomingRepairKV(
      messageId: "msg-1",
      entry: IncomingRepairEntry.init(
        HistoryEntry.init("msg-1"), cachedMessage, fromUnix(1_700_000_200)
      ),
    )
  )
  meta

proc logRow(msg: SdsMessage): seq[byte] =
  sds_wire.serializeMessage(msg).get()

proc metaRow(meta: ChannelMeta): seq[byte] =
  snapshot_codec.serialize(meta).get()

proc putLegacyRows(job: Job) {.async.} =
  let channelId = "chan-1".SdsChannelID
  check (
    await job.putAcked(CatLog, key(channelId, "msg-1"), hexToSeqByte(LegacyLogRow))
  ).isOk
  check (await job.putAcked(CatMeta, toKey(channelId), hexToSeqByte(LegacyMetaRow))).isOk

proc hasMarker(job: Job, channelId: SdsChannelID): Future[bool] {.async.} =
  return (await job.exists(CatMigration, migrationMarkerKey(channelId))).get(false)

proc legacyLogRowStays(job: Job): Future[bool] {.async.} =
  ## True when the `sds.log` row of `putLegacyRows` has its old bytes.
  let stored = (await job.get(CatLog, key("chan-1".SdsChannelID, "msg-1"))).get(
    Opt.none(seq[byte])
  )
  return stored == Opt.some(hexToSeqByte(LegacyLogRow))

suite "SDS persistency - detection of old rows":
  test "a row of the old format is detected":
    check:
      logRowHasOldFormat(hexToSeqByte(LegacyLogRow))
      metaRowHasOldFormat(hexToSeqByte(LegacyMetaRow))

  test "a row of the current format is not detected":
    let current = currentMeta(logRow(currentMsg("msg-3", @[HexId])))
    check:
      not logRowHasOldFormat(logRow(currentMsg("msg-3", @[HexId])))
      not logRowHasOldFormat(logRow(currentMsg("msg-3", @[OddHexId])))
      not logRowHasOldFormat(logRow(currentMsg("msg-3")))
      not metaRowHasOldFormat(metaRow(current))
      not metaRowHasOldFormat(metaRow(ChannelMeta.init()))

  test "a row that does not decode is not detected":
    # Field 1 with a length of 5 and only 1 byte of data.
    check:
      not logRowHasOldFormat(@[byte 0x0a, 0x05, 0x31])
      not metaRowHasOldFormat(@[byte 0x0a, 0x05, 0x31])

  test "a meta row with an old cached message is detected":
    check metaRowHasOldFormat(metaRow(currentMeta(hexToSeqByte(LegacyLogRow))))

suite "SDS persistency - migration of old rows":
  asyncTest "old rows are deleted before loadChannel reads them":
    let root = tmpRoot("delete")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    let channelId = "chan-1".SdsChannelID
    await job.putLegacyRows()
    let msg = currentMsg("msg-3", @[HexId])
    check (await job.putAcked(CatLog, key(channelId, "msg-3"), logRow(msg))).isOk

    let data = (await newSdsPersistence(job).loadChannel(channelId)).valueOr:
      check false
      return
    # The old rows are gone, and the current row stays.
    check:
      data.messageHistory == @[msg]
      data.meta == ChannelMeta.init()
      await job.hasMarker(channelId)
      not (await job.exists(CatLog, key(channelId, "msg-1"))).get(true)
      not (await job.exists(CatMeta, toKey(channelId))).get(true)

  asyncTest "rows of the current format are not deleted":
    let root = tmpRoot("keep")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    let channelId = "chan-1".SdsChannelID
    let first = currentMsg("msg-1")
    let second = currentMsg("msg-2", @[HexId, OddHexId])
    let meta = currentMeta(logRow(second))
    check:
      (await job.putAcked(CatLog, key(channelId, "msg-1"), logRow(first))).isOk
      (await job.putAcked(CatLog, key(channelId, "msg-2"), logRow(second))).isOk
      (await job.putAcked(CatMeta, toKey(channelId), metaRow(meta))).isOk

    let data = (await newSdsPersistence(job).loadChannel(channelId)).valueOr:
      check false
      return
    check:
      data.messageHistory == @[first, second]
      data.meta == meta
      await job.hasMarker(channelId)

  asyncTest "the migration runs one time":
    let root = tmpRoot("once")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let channelId = "chan-1".SdsChannelID
    block firstStart:
      let job = p.openJob("sds").get()
      await job.putLegacyRows()
      check (await newSdsPersistence(job).loadChannel(channelId)).isOk
      # An old row written after the migration.
      await job.putLegacyRows()
      p.closeJob("sds")

    # A new job on the same file is the next start of the node. The marker
    # row exists, so its migration deletes no row.
    let job = p.openJob("sds").get()
    check:
      (await newSdsPersistence(job).loadChannel(channelId)).isOk
      await job.legacyLogRowStays()
      (await job.exists(CatMeta, toKey(channelId))).get(false)

  asyncTest "a write before the first read also waits for the migration":
    let root = tmpRoot("write-first")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    await job.putLegacyRows()

    let persistence = newSdsPersistence(job)
    let channelId = "chan-1".SdsChannelID
    let msg = currentMsg("msg-3", @[HexId])
    var upd = HistoryUpdate.init()
    upd.append = @[msg]
    check (await persistence.updateHistory(channelId, upd)).isOk

    # The migration committed before the write. The write is fire-and-forget,
    # so poll until the new row is visible.
    check:
      await job.hasMarker(channelId)
      await job.pollExists(CatLog, key(channelId, "msg-3"))
      not (await job.exists(CatLog, key(channelId, "msg-1"))).get(true)

    let data = (await persistence.loadChannel(channelId)).valueOr:
      check false
      return
    check data.messageHistory == @[msg]

  asyncTest "a fresh database gets only the marker":
    let root = tmpRoot("fresh")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()

    let persistence = newSdsPersistence(job)
    let data = (await persistence.loadChannel("nope".SdsChannelID)).valueOr:
      check false
      return
    check:
      data.messageHistory.len == 0
      await job.hasMarker("nope".SdsChannelID)

  asyncTest "all adapters of a job share one migration":
    let root = tmpRoot("shared")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    let channelId = "chan-1".SdsChannelID
    check (await newSdsPersistence(job).loadChannel(channelId)).isOk

    # Without the marker, a migration of a new job would run and delete the
    # old row. The migration of this job is done, so a new adapter does not
    # run one.
    check (await job.deleteAcked(CatMigration, migrationMarkerKey(channelId))).get(
      false
    )
    await job.putLegacyRows()
    check:
      (await newSdsPersistence(job).loadChannel(channelId)).isOk
      await job.legacyLogRowStays()
      not (await job.hasMarker(channelId))

  asyncTest "a second channel migrates at its own first use":
    let root = tmpRoot("per-channel")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    let chan1 = "chan-1".SdsChannelID
    let chan2 = "chan-2".SdsChannelID
    let legacy = hexToSeqByte(LegacyLogRow)
    check:
      (await job.putAcked(CatLog, key(chan1, "msg-1"), legacy)).isOk
      (await job.putAcked(CatLog, key(chan2, "msg-1"), legacy)).isOk

    let persistence = newSdsPersistence(job)
    check (await persistence.loadChannel(chan1)).isOk

    # The first channel is migrated, and the second one is not.
    check:
      await job.hasMarker(chan1)
      not (await job.hasMarker(chan2))
      not (await job.exists(CatLog, key(chan1, "msg-1"))).get(true)
      (await job.get(CatLog, key(chan2, "msg-1"))).get(Opt.none(seq[byte])) ==
        Opt.some(legacy)

    let data = (await persistence.loadChannel(chan2)).valueOr:
      check false
      return
    check:
      data.messageHistory.len == 0
      await job.hasMarker(chan2)
      not (await job.exists(CatLog, key(chan2, "msg-1"))).get(true)

  asyncTest "dropChannel migrates the channel before it deletes its rows":
    let root = tmpRoot("drop")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    await job.putLegacyRows()
    let persistence = newSdsPersistence(job)
    let channelId = "chan-1".SdsChannelID
    check (await persistence.dropChannel(channelId)).isOk

    # `dropChannel` returns after the migration, so the marker row exists.
    check await job.hasMarker(channelId)
    # The drop is fire-and-forget, so poll until its deletes are visible.
    check:
      await job.pollGone(CatLog, key(channelId, "msg-1"))
      await job.pollGone(CatMeta, toKey(channelId))

    # An old row written after the drop stays, because the marker row of the
    # first migration stops the next one.
    await job.putLegacyRows()
    check:
      (await persistence.loadChannel(channelId)).isOk
      await job.legacyLogRowStays()

  asyncTest "a stop and a new start leave no entry of the old job":
    let root = tmpRoot("restart")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let channelId = "chan-1".SdsChannelID
    block firstStart:
      let job = p.openJob("sds").get()
      check (await newSdsPersistence(job).loadChannel(channelId)).isOk
      p.closeJob("sds")

    # The first migration call of the next start removes the entries of the
    # closed job.
    let job = p.openJob("sds").get()
    check:
      (await newSdsPersistence(job).loadChannel(channelId)).isOk
      migrationTableJobCount() == 1

  asyncTest "a save on a closed job starts no new migration":
    let root = tmpRoot("closedsave")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    let persistence = newSdsPersistence(job)
    let channelId = "chan-1".SdsChannelID
    check (await persistence.loadChannel(channelId)).isOk
    p.closeJob("sds")

    # The migration of the channel is done. A late save finds it in the table,
    # so it does not run a new one on the closed job, and the write itself is
    # fire-and-forget.
    check:
      (await persistence.saveChannelMeta(channelId, ChannelMeta.init())).isOk
      (await persistence.saveChannelMeta(channelId, ChannelMeta.init())).isOk

  asyncTest "waitForMarker ends at once when the job is closed":
    let root = tmpRoot("closed")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    p.closeJob("sds")

    # A read on a closed job fails at once, and the failed read ends the wait.
    let start = Moment.now()
    let res = await waitForMarker(job, migrationMarkerKey("chan-1".SdsChannelID))
    check:
      res.isErr()
      Moment.now() - start < chronos.seconds(5)

  asyncTest "a failed migration runs again at the next call":
    let root = tmpRoot("retry")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    await job.putLegacyRows()
    let persistence = newSdsPersistence(job)
    let channelId = "chan-1".SdsChannelID

    # The first read of the marker times out, so the first migration fails.
    let timeout = KvExists.requestTimeout()
    KvExists.setRequestTimeout(chronos.nanoseconds(1))
    let first = await persistence.loadChannel(channelId)
    KvExists.setRequestTimeout(timeout)
    check:
      first.isErr()
      await job.legacyLogRowStays()

    let data = (await persistence.loadChannel(channelId)).valueOr:
      check false
      return
    check:
      data.messageHistory.len == 0
      await job.hasMarker(channelId)
      not (await job.exists(CatLog, key(channelId, "msg-1"))).get(true)

  asyncTest "a cancelled call does not cancel the migration":
    let root = tmpRoot("cancel")
    defer:
      removeDir(root)
    let p = Persistency.new(root).get()
    defer:
      p.close()
    let job = p.openJob("sds").get()
    await job.putLegacyRows()
    let persistence = newSdsPersistence(job)
    let channelId = "chan-1".SdsChannelID

    # Two loads of one channel wait for one migration. The migration waits for
    # the storage thread, so the cancel of the first load comes while it runs.
    let first = persistence.loadChannel(channelId)
    let second = persistence.loadChannel(channelId)
    await first.cancelAndWait()

    # The second load gets the result of the same migration.
    let data = (await second).valueOr:
      check false
      return
    check:
      data.messageHistory.len == 0
      await job.hasMarker(channelId)
      not (await job.exists(CatLog, key(channelId, "msg-1"))).get(true)

## Tracks sent messages considered not properly delivered, archiving them in a
## local sqlite database. A message is considered delivered once received by any
## store node.

import results
import
  waku/common/databases/db_sqlite,
  waku/waku_core/message/message,
  ./migrations

const NotDeliveredMessagesDbUrl = "not-delivered-messages.db"

type NotDeliveredStorage* = ref object
  database: SqliteDatabase

type TrackedWakuMessage = object
  msg: WakuMessage
  numTrials: uint
    ## number of times the node has tried to publish it

proc new*(T: type NotDeliveredStorage): Result[T, string] =
  let db = ?SqliteDatabase.new(NotDeliveredMessagesDbUrl)

  ?migrate(db)

  return ok(NotDeliveredStorage(database: db))

proc archiveMessage*(
    self: NotDeliveredStorage, msg: WakuMessage
): Result[void, string] =
  ## Archives a waku message so it survives an app restart.
  return ok()

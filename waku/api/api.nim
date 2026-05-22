import chronicles, chronos, results

import waku/factory/waku
import ../../tools/confutils/cli_args

export cli_args

logScope:
  topics = "api"

proc createNode*(conf: WakuNodeConf): Future[Result[Waku, string]] {.async.} =
  let wakuConf = conf.toWakuConf().valueOr:
    return err("Failed to handle the configuration: " & error)

  ## We are not defining app callbacks at node creation
  let wakuRes = (await Waku.new(wakuConf)).valueOr:
    error "waku initialization failed", error = error
    return err("Failed setting up Waku: " & $error)

  return ok(wakuRes)

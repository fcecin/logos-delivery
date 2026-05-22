{.push raises: [].}

## FFI helpers for kernel construction. JSON conf parsing + Waku build,
## shared by `kernel_ffi.nim` (waku_new) and `messaging_ffi.nim`
## (messaging_client_new_with_conf).

import std/[json, strutils, tables]
import chronos, chronicles, results, confutils, confutils/std/net
import waku/factory/waku
import waku/api/api
import tools/confutils/cli_args

proc createWakuFromJson*(
    configJson: cstring
): Future[Result[Waku, string]] {.async.} =
  ## Parse a JSON `WakuNodeConf` blob (case-insensitive, unknown-fields-rejected)
  ## and construct a `Waku`. Returns the new Waku ref or an error.

  var conf = defaultWakuNodeConf().valueOr:
    return err("Failed creating default conf: " & error)

  var jsonNode: JsonNode
  try:
    jsonNode = parseJson($configJson)
  except Exception:
    let exceptionMsg = getCurrentExceptionMsg()
    error "Failed to parse config JSON",
      error = exceptionMsg, configJson = $configJson
    return err(
      "Failed to parse config JSON: " & exceptionMsg & " configJson string: " &
        $configJson
    )

  var jsonFields: Table[string, (string, JsonNode)]
  for key, value in jsonNode:
    let lowerKey = key.toLowerAscii()
    if jsonFields.hasKey(lowerKey):
      error "Duplicate configuration option found when normalized to lowercase",
        key = key
      return err(
        "Duplicate configuration option found when normalized to lowercase: '" & key &
          "'"
      )
    jsonFields[lowerKey] = (key, value)

  for confField, confValue in fieldPairs(conf):
    let lowerField = confField.toLowerAscii()
    if jsonFields.hasKey(lowerField):
      let (jsonKey, jsonValue) = jsonFields[lowerField]
      let formattedString = ($jsonValue).strip(chars = {'\"'})
      try:
        confValue = parseCmdArg(typeof(confValue), formattedString)
      except Exception:
        return err(
          "Failed to parse field '" & confField & "' from JSON key '" & jsonKey & "': " &
            getCurrentExceptionMsg() & ". Value: " & formattedString
        )
      jsonFields.del(lowerField)

  if jsonFields.len > 0:
    var unknownKeys = newSeq[string]()
    for _, (jsonKey, _) in pairs(jsonFields):
      unknownKeys.add(jsonKey)
    error "Unrecognized configuration option(s) found", option = unknownKeys
    return err("Unrecognized configuration option(s) found: " & $unknownKeys)

  let waku = (await api.createNode(conf)).valueOr:
    return err($error)

  return ok(waku)

{.pop.}

import std/[json, macros]

type JsonEvent*[T] = ref object
  eventType*: string
  payload*: T

macro toFlatJson*(event: JsonEvent): JsonNode =
  ## Serialize JsonEvent[T] to flat JSON: eventType first, then T's payload fields.
  result = quote:
    var jsonObj = newJObject()
    jsonObj["eventType"] = %`event`.eventType

    # Flatten payload fields into the same object.
    let payloadJson = %`event`.payload
    for key, val in payloadJson.pairs:
      jsonObj[key] = val

    jsonObj

proc `$`*[T](event: JsonEvent[T]): string =
  $toFlatJson(event)

proc newJsonEvent*[T](eventType: string, payload: T): JsonEvent[T] =
  ## New JsonEvent with the given eventType and payload.
  JsonEvent[T](eventType: eventType, payload: payload)

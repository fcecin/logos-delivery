{.used.}

## FFI-level check of the order of a send, through the real C ABI. The driver
## `dlopen`s `liblogosdelivery` and calls the exported `logosdelivery_*` entry
## points, as a module host does.
##
## The callback of `logosdelivery_send` must get the request id before the
## event thread gets an event of that request. The node has a rate-limit budget
## of 0, so each send emits an event with no network call. A durable message
## gets `onMessageQueued`, and an ephemeral one gets `onMessageError`.
##
## The send callback waits 100 ms before it records the id. The callback runs
## on the FFI thread, and the send service runs on that thread too. In the
## correct order, the send service makes the first attempt only after the
## callback returns, so no event of the request comes during the wait. If the
## attempt runs before the answer, the event thread delivers the event during
## the wait, and the case fails. Without the wait, the two threads race, and the
## case can pass on broken code.
##
## The case runs in a child process with its output captured to a file, so a
## crash is an exit code rather than a dead test binary.
##
## Requires the shared library. Build it with:
##   make liblogosdelivery
## Override the location with LIBLOGOSDELIVERY=<path>.
##
## It is in no aggregate, because it needs the shared library built first (see
## above). CI runs it in the `liblogosdelivery-default` job on Linux. Run it with:
##   make test tests/ffi/test_ffi_send_order.nim

import std/[atomics, base64, dynlib, json, os, osproc, strutils]
import testutils/unittests
import ffi/[cbor_serial, ret_codes]

const
  CaseSendOrder = "--case-send-order"

  OrderTopic = "/ffi-send-order/1/probe/proto"
  Sends = 6 ## Durable and ephemeral in turn.
  CallbackWaitMs = 100

  MaxEvents = 64
  EventBytes = 1024

  LibSuffix =
    when defined(macosx):
      ".dylib"
    elif defined(windows):
      ".dll"
    else:
      ".so"

# ── C ABI surface ───────────────────────────────────────────────────────

type
  FfiCallback = proc(callerRet: cint, msg: ptr cchar, len: csize_t, userData: pointer) {.
    cdecl, gcsafe, raises: []
  .}

  CreateNodeFn = proc(
    req: ptr byte, reqLen: csize_t, cb: FfiCallback, userData: pointer
  ): pointer {.cdecl, gcsafe.}
  CtxFn = proc(
    ctx: pointer, cb: FfiCallback, userData: pointer, req: ptr byte, reqLen: csize_t
  ): cint {.cdecl, gcsafe.}
  DestroyFn = proc(ctx: pointer): cint {.cdecl, gcsafe.}
    ## The `{.ffiDtor.}` export takes no callback. It blocks until teardown ends.
  AddListenerFn = proc(
    ctx: pointer, eventName: cstring, cb: FfiCallback, userData: pointer
  ): uint64 {.cdecl, gcsafe.}

  Api = object
    createNode: CreateNodeFn
    startNode: CtxFn
    stopNode: CtxFn
    send: CtxFn
    destroy: DestroyFn
    addListener: AddListenerFn

  CreateNodeReq = object
    configJson: string

  SendReq = object
    messageJson: string

  EmptyReq = object

  Reply = object ## The answer that a request callback writes.
    done: Atomic[int]
    ret: Atomic[int]
    len: Atomic[int]
    buf: array[2048, char]

  EventRecord = object
    ready: Atomic[int] ## Set last, when the other fields hold the event.
    idKnown: int ## 1 when the send callback had recorded its id before this event.
    len: int
    buf: array[EventBytes, char]

  CallbackState = object
    ## The memory that the callbacks share. They run on threads of the library, so
    ## nothing that the GC owns may cross them.
    reply: Reply
    idKnown: Atomic[int] ## Set by the send callback after its wait.
    eventCount: Atomic[int]
    events: array[MaxEvents, EventRecord]

proc libPath(): string =
  let fromEnv = getEnv("LIBLOGOSDELIVERY")
  if fromEnv.len > 0:
    return fromEnv
  return getCurrentDir() / "build" / ("liblogosdelivery" & LibSuffix)

proc copyReply(r: ptr Reply, callerRet: cint, msg: ptr cchar, len: csize_t) =
  var n = int(len)
  if n > r.buf.len:
    n = r.buf.len
  if n > 0 and not msg.isNil():
    copyMem(addr r.buf[0], msg, n)
  r.len.store(n)
  r.ret.store(int(callerRet))

proc onReply(
    callerRet: cint, msg: ptr cchar, len: csize_t, userData: pointer
) {.cdecl, gcsafe, raises: [].} =
  if callerRet == RET_STALE_WARN:
    return
  let s = cast[ptr CallbackState](userData)
  copyReply(addr s.reply, callerRet, msg, len)
  s.reply.done.store(1)

proc onSendReply(
    callerRet: cint, msg: ptr cchar, len: csize_t, userData: pointer
) {.cdecl, gcsafe, raises: [].} =
  ## Waits before it records the id. An event that the event thread delivers in
  ## this time came before its id.
  if callerRet == RET_STALE_WARN:
    return
  sleep(CallbackWaitMs)
  let s = cast[ptr CallbackState](userData)
  copyReply(addr s.reply, callerRet, msg, len)
  s.idKnown.store(1)
  s.reply.done.store(1)

proc onEvent(
    callerRet: cint, msg: ptr cchar, len: csize_t, userData: pointer
) {.cdecl, gcsafe, raises: [].} =
  let s = cast[ptr CallbackState](userData)
  let i = s.eventCount.fetchAdd(1)
  if i >= MaxEvents:
    return
  let record = addr s.events[i]
  record.idKnown = s.idKnown.load()
  var n = int(len)
  if n > record.buf.len:
    n = record.buf.len
  if n > 0 and not msg.isNil():
    copyMem(addr record.buf[0], msg, n)
  record.len = n
  record.ready.store(1)

proc clearReply(s: ptr CallbackState) =
  s.reply.done.store(0)
  s.reply.ret.store(-1)
  s.reply.len.store(0)

proc awaitReply(
    s: ptr CallbackState, timeoutMs = 60_000
): tuple[ok: bool, ret: int, msg: string] =
  var waited = 0
  while s.reply.done.load() == 0 and waited < timeoutMs:
    sleep(10)
    waited += 10
  if s.reply.done.load() == 0:
    return (false, -1, "timeout after " & $timeoutMs & "ms")
  let n = s.reply.len.load()
  var m = newString(n)
  if n > 0:
    copyMem(addr m[0], addr s.reply.buf[0], n)
  return (true, s.reply.ret.load(), m)

proc eventText(s: ptr CallbackState, i: int): string =
  let record = addr s.events[i]
  var text = newString(record.len)
  if record.len > 0:
    copyMem(addr text[0], addr record.buf[0], record.len)
  return text

proc need(lib: LibHandle, name: string): pointer =
  let p = lib.symAddr(name)
  if p.isNil():
    quit("missing symbol " & name & " in " & libPath(), 2)
  return p

proc loadApi(): Api =
  let lib = loadLib(libPath())
  if lib.isNil():
    quit("cannot load " & libPath(), 2)

  Api(
    createNode: cast[CreateNodeFn](lib.need("logosdelivery_create_node")),
    startNode: cast[CtxFn](lib.need("logosdelivery_start_node")),
    stopNode: cast[CtxFn](lib.need("logosdelivery_stop_node")),
    send: cast[CtxFn](lib.need("logosdelivery_send")),
    destroy: cast[DestroyFn](lib.need("logosdelivery_destroy")),
    addListener: cast[AddListenerFn](lib.need("logosdelivery_add_event_listener")),
  )

# ── driver helpers ──────────────────────────────────────────────────────

var failed = false

proc fail(msg: string) =
  echo "  FAIL: ", msg
  failed = true

proc expectOk(step: string, r: tuple[ok: bool, ret: int, msg: string]) =
  echo "  [", step, "] ok=", r.ok, " ret=", r.ret
  if not r.ok or r.ret != int(RET_OK):
    fail(step & " expected RET_OK, got: " & r.msg)

proc call[T](
    api: Api,
    s: ptr CallbackState,
    fn: CtxFn,
    ctx: pointer,
    req: T,
    cb: FfiCallback = onReply,
): tuple[ok: bool, ret: int, msg: string] =
  let bytes = cborEncode(req)
  clearReply(s)
  discard fn(ctx, cb, s, unsafeAddr bytes[0], csize_t(bytes.len))
  return awaitReply(s)

proc nodeConfig(storagePath: string): string =
  ## No preset, so the node dials no fleet. Autosharding gives each content
  ## topic a shard, which a send needs.
  $(
    %*{
      "mode": "Core",
      "messagingOverrides": {
        "log-level": "INFO",
        "local-storage-path": storagePath,
        "tcp-port": "0",
        "discv5-udp-port": "0",
        "discv5-discovery": "false",
        "nat": "none",
        "cluster-id": "198",
        "num-shards-in-network": "1",
        "rate-limit-enabled": "true",
        "rate-limit-messages-per-epoch": "0",
      },
    }
  )

proc messageJson(index: int): string =
  $(
    %*{
      "contentTopic": OrderTopic,
      "payload": encode("send-order-" & $index),
      "ephemeral": index mod 2 == 1,
    }
  )

proc caseRoot(): string =
  getTempDir() / "ffi_send_order"

# ── case ────────────────────────────────────────────────────────────────

proc runSendOrder(api: Api, s: ptr CallbackState) =
  let createReq = cborEncode(CreateNodeReq(configJson: nodeConfig(caseRoot())))
  clearReply(s)
  let ctx = api.createNode(unsafeAddr createReq[0], csize_t(createReq.len), onReply, s)
  if ctx.isNil():
    fail("create_node returned nil")
    return
  expectOk("create_node", awaitReply(s))
  if failed:
    return

  for name in ["onMessageQueued", "onMessageError"]:
    if api.addListener(ctx, name.cstring, onEvent, s) == 0:
      fail("add_event_listener " & name & " returned 0")
  expectOk("start_node", api.call(s, api.startNode, ctx, EmptyReq()))
  if failed:
    discard api.destroy(ctx)
    return

  for i in 0 ..< Sends:
    s.idKnown.store(0)
    let first = s.eventCount.load()
    let r =
      api.call(s, api.send, ctx, SendReq(messageJson: messageJson(i)), onSendReply)
    expectOk("send " & $i, r)
    if failed:
      break
    let id = cborDecode(r.msg.toOpenArrayByte(0, r.msg.high), string).valueOr:
      fail("send " & $i & " reply is not a CBOR string: " & error)
      break

    # Wait for the event of this request.
    var seen = false
    var waited = 0
    while not seen and waited < 10_000:
      for j in first ..< min(s.eventCount.load(), MaxEvents):
        if s.events[j].ready.load() == 1 and s.eventText(j).contains(id):
          seen = true
          if s.events[j].idKnown == 0:
            fail(
              "send " & $i & ": an event came before its request id: " & s.eventText(j)
            )
      if not seen:
        sleep(10)
        waited += 10
    echo "  [send ", i, "] id=", id, " event seen=", seen
    if not seen:
      fail("send " & $i & ": no event for request " & id)
      break

  expectOk("stop_node", api.call(s, api.stopNode, ctx, EmptyReq()))
  echo "  [destroy] ret=", api.destroy(ctx)

if paramCount() >= 1 and paramStr(1) == CaseSendOrder:
  runSendOrder(loadApi(), createShared(CallbackState))
  quit(if failed: 1 else: 0)

# ── parent ──────────────────────────────────────────────────────────────

proc runCase(flag: string): tuple[code: int, output: string] =
  let logFile = getTempDir() / ("ffi_send_order" & flag & ".log")
  discard tryRemoveFile(logFile)
  let cmd =
    quoteShell(getAppFilename()) & " " & flag & " > " & quoteShell(logFile) & " 2>&1"

  let child = startProcess("/bin/sh", args = @["-c", cmd], options = {})
  let code = child.waitForExit(timeout = 300_000)
  child.close()

  let output =
    try:
      readFile(logFile)
    except IOError:
      ""
  discard tryRemoveFile(logFile)
  return (code, output)

suite "FFI - the request id of a send comes before its events":
  test "no event of a send reaches a listener before the send callback returns":
    if not fileExists(libPath()):
      echo "skipped: no ", libPath()
      skip()
    else:
      removeDir(caseRoot())
      let r = runCase(CaseSendOrder)
      echo "--- ", CaseSendOrder, " (exit ", r.code, ") ---"
      for line in r.output.splitLines():
        if line.contains("  [") or line.contains("FAIL:"):
          echo line
      removeDir(caseRoot())

      check r.code == 0

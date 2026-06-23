{.used.}

import std/options, results, testutils/unittests
import std/nativesockets # Port

import logos_delivery/api/types # WakuMode
import logos_delivery/api/messaging_conf
  # MessagingConf, toKernelConf, defaultWakuNodeConf
import logos_delivery/api/messaging_conf_preset # resolvePreset, merge

suite "MessagingConf - toKernelConf inference":
  test "Core mode enables relay and service protocols":
    let conf = MessagingConf().toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check:
      conf.relay == true
      conf.filter == true
      conf.lightpush == true
      conf.discv5Discovery == some(true)
      conf.peerExchange == true
      conf.rendezvous == true

  test "Edge mode disables relay and service protocols":
    let conf = MessagingConf().toKernelConf(WakuMode.Edge).valueOr:
      raiseAssert error
    check:
      conf.relay == false
      conf.filter == false
      conf.lightpush == false
      conf.store == false
      conf.peerExchange == true

  test "clusterId is applied only when set":
    let setConf = MessagingConf(clusterId: some(7'u16)).toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check setConf.clusterId == some(7'u16)

    let kernelDefault = defaultWakuNodeConf().valueOr:
      raiseAssert error
    let unsetConf = MessagingConf().toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check unsetConf.clusterId == kernelDefault.clusterId

  test "p2pTcpPort sets the kernel tcpPort":
    let conf = MessagingConf(p2pTcpPort: some(Port(60123))).toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check conf.tcpPort.uint16 == 60123'u16

  test "reliabilityEnabled is applied to the kernel":
    let conf = MessagingConf(reliabilityEnabled: some(true)).toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check conf.reliabilityEnabled == some(true)

  test "rlnContractAddress sets the contract and enables rlnRelay":
    let conf = MessagingConf(rlnContractAddress: some("0xabc")).toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check:
      conf.rlnRelayEthContractAddress == "0xabc"
      conf.rlnRelay == some(true)

  test "ethRpcEndpoints maps to the kernel ethClientUrls":
    let mc = MessagingConf(ethRpcEndpoints: some(@["http://node:8545"]))
    let conf = mc.toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check:
      conf.ethClientUrls.len == 1
      string(conf.ethClientUrls[0]) == "http://node:8545"

  test "rlnChainId and rlnEpochSizeSec map to the kernel":
    let mc = MessagingConf(rlnChainId: some(5'u), rlnEpochSizeSec: some(600'u))
    let conf = mc.toKernelConf(WakuMode.Core).valueOr:
      raiseAssert error
    check:
      conf.rlnRelayChainId == 5'u
      conf.rlnEpochSizeSec == some(600'u64)

suite "MessagingConf - preset resolution and merge":
  test "resolvePreset twn populates the network fields":
    let m = resolvePreset("twn").valueOr:
      raiseAssert error
    check:
      m.clusterId == some(1'u16)
      m.numShardsInCluster == some(8'u16)
      m.reliabilityEnabled == some(false)
      m.rlnContractAddress.isSome()

  test "resolvePreset logosdev sets cluster 2 and reliability":
    let m = resolvePreset("logosdev").valueOr:
      raiseAssert error
    check:
      m.clusterId == some(2'u16)
      m.reliabilityEnabled == some(true)
      m.rlnContractAddress.isNone()

  test "resolvePreset empty is a no-op":
    let m = resolvePreset("").valueOr:
      raiseAssert error
    check:
      m.clusterId.isNone()
      m.reliabilityEnabled.isNone()

  test "merge: overrides win over the preset base, base kept otherwise":
    let base = resolvePreset("twn").valueOr:
      raiseAssert error
    let merged = base.merge(MessagingConf(clusterId: some(99'u16)))
    check:
      merged.clusterId == some(99'u16)
      merged.numShardsInCluster == some(8'u16)

## Preset resolution and override merge for the Messaging layer.
##
## `resolvePreset` turns a network preset name into the Messaging-layer fields it
## implies; `merge` applies a caller's overrides on top. `startAsClient` uses both
## before inferring the kernel config. The kernel still resolves the preset itself
## for the node-local fields a preset does not carry.

import std/options
import results
import stint # UInt256.truncate

import logos_delivery/api/messaging_conf
  # MessagingConf, ConfResult, toNetworkPresetConf
import logos_delivery/waku/factory/networks_config # NetworkPresetConf, AutoSharding

proc merge*(base, overrides: MessagingConf): MessagingConf =
  ## Combine two messaging configs field by field: a set `overrides` field wins;
  ## otherwise the `base` value is kept.
  result = base
  if overrides.clusterId.isSome(): result.clusterId = overrides.clusterId
  if overrides.numShardsInCluster.isSome():
    result.numShardsInCluster = overrides.numShardsInCluster
  if overrides.p2pTcpPort.isSome(): result.p2pTcpPort = overrides.p2pTcpPort
  if overrides.discv5UdpPort.isSome(): result.discv5UdpPort = overrides.discv5UdpPort
  if overrides.listenIpv4.isSome(): result.listenIpv4 = overrides.listenIpv4
  if overrides.maxMessageSize.isSome(): result.maxMessageSize = overrides.maxMessageSize
  if overrides.entryNodes.isSome(): result.entryNodes = overrides.entryNodes
  if overrides.ethRpcEndpoints.isSome():
    result.ethRpcEndpoints = overrides.ethRpcEndpoints
  if overrides.rlnContractAddress.isSome():
    result.rlnContractAddress = overrides.rlnContractAddress
  if overrides.rlnChainId.isSome(): result.rlnChainId = overrides.rlnChainId
  if overrides.rlnEpochSizeSec.isSome():
    result.rlnEpochSizeSec = overrides.rlnEpochSizeSec
  if overrides.reliabilityEnabled.isSome():
    result.reliabilityEnabled = overrides.reliabilityEnabled

proc resolvePreset*(preset: string): ConfResult[MessagingConf] =
  ## Resolve a network preset name into the Messaging-layer fields it implies.
  ## Node-local fields (ports, bind address, RPC endpoints) are not preset-defined
  ## and stay unset. An empty preset resolves to an empty config.
  let npcOpt = ?toNetworkPresetConf(preset, none(uint16))
  if npcOpt.isNone():
    return ok(MessagingConf())
  let npc = npcOpt.get()
  var m = MessagingConf()
  m.clusterId = some(npc.clusterId)
  if npc.shardingConf.kind == AutoSharding:
    m.numShardsInCluster = some(npc.shardingConf.numShardsInCluster)
  m.maxMessageSize = some(npc.maxMessageSize)
  if npc.entryNodes.len > 0:
    m.entryNodes = some(npc.entryNodes)
  if npc.rlnRelay and npc.rlnRelayEthContractAddress.len > 0:
    m.rlnContractAddress = some(npc.rlnRelayEthContractAddress)
    m.rlnChainId = some(npc.rlnRelayChainId.truncate(uint))
    m.rlnEpochSizeSec = some(npc.rlnEpochSizeSec.uint)
  m.reliabilityEnabled = some(npc.p2pReliability)
  ok(m)

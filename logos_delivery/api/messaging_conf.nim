## Messaging-layer configuration.

import std/options
import std/sequtils # mapIt
import results
import std/nativesockets # Port
import std/net # IpAddress

import logos_delivery/api/types # WakuMode
import logos_delivery/api/kernel_conf # KernelConf, ConfResult, defaultWakuNodeConf, EthRpcUrl
export kernel_conf

type MessagingConf* = object
  ## Messaging-layer configuration as an all-`Option` partial. `toKernelConf`
  ## applies each set field to the corresponding `KernelConf` field; unset fields
  ## leave the kernel default unchanged.
  clusterId*: Option[uint16]
    ## Network cluster id.
  numShardsInCluster*: Option[uint16]
    ## Number of shards in the cluster.
  p2pTcpPort*: Option[Port]
    ## TCP listening port.
  discv5UdpPort*: Option[Port]
    ## discv5 UDP port.
  listenIpv4*: Option[IpAddress]
    ## Inbound bind address.
  maxMessageSize*: Option[string]
    ## Maximum accepted message size (e.g. "150 KiB").
  entryNodes*: Option[seq[string]]
    ## Bootstrap / connectivity nodes (enrtree or multiaddr).
  ethRpcEndpoints*: Option[seq[string]]
    ## Ethereum RPC endpoints (required for RLN validation); multiple for fail-over.
  rlnContractAddress*: Option[string]
    ## RLN contract address; when set, RLN validation is enabled.
  rlnChainId*: Option[uint]
    ## Chain id the RLN contract is deployed on.
  rlnEpochSizeSec*: Option[uint]
    ## RLN epoch size, in seconds.
  reliabilityEnabled*: Option[bool]
    ## Enable store-based send reliability.

proc toKernelConf*(m: MessagingConf, mode: WakuMode): ConfResult[KernelConf] =
  ## Build a `KernelConf` from the operation mode and the messaging configuration.
  ## The mode sets the kernel protocol flags; each set field is then written to its
  ## kernel counterpart, and unset fields keep the kernel default.
  var conf = ?defaultWakuNodeConf()

  case mode
  of WakuMode.Core:
    conf.relay = true
    conf.filter = true
    conf.lightpush = true
    conf.discv5Discovery = some(true)
    conf.peerExchange = true
    conf.rendezvous = true
    if conf.rateLimits.len == 0:
      conf.rateLimits = @["filter:100/1s", "lightpush:5/1s", "px:5/1s"]
  of WakuMode.Edge:
    conf.peerExchange = true
    conf.relay = false
    conf.filter = false
    conf.lightpush = false
    conf.store = false

  if m.clusterId.isSome():
    conf.clusterId = m.clusterId
  if m.numShardsInCluster.isSome():
    conf.numShardsInNetwork = m.numShardsInCluster.get()
  if m.p2pTcpPort.isSome():
    conf.tcpPort = m.p2pTcpPort.get()
  if m.discv5UdpPort.isSome():
    conf.discv5UdpPort = m.discv5UdpPort.get()
  if m.listenIpv4.isSome():
    conf.listenAddress = m.listenIpv4.get()
  if m.maxMessageSize.isSome():
    conf.maxMessageSize = m.maxMessageSize.get()
  if m.entryNodes.isSome():
    conf.entryNodes = m.entryNodes.get()
  if m.ethRpcEndpoints.isSome():
    conf.ethClientUrls = m.ethRpcEndpoints.get().mapIt(EthRpcUrl(it))
  if m.rlnContractAddress.isSome():
    conf.rlnRelayEthContractAddress = m.rlnContractAddress.get()
    conf.rlnRelay = some(true)
  if m.rlnChainId.isSome():
    conf.rlnRelayChainId = m.rlnChainId.get()
  if m.rlnEpochSizeSec.isSome():
    conf.rlnEpochSizeSec = some(m.rlnEpochSizeSec.get().uint64)
  if m.reliabilityEnabled.isSome():
    conf.reliabilityEnabled = m.reliabilityEnabled
  ok(conf)

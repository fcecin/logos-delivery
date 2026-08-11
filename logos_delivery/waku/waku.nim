{.push raises: [].}

import
  std/[sequtils, strformat],
  results,
  chronicles,
  chronos,
  libp2p/protocols/connectivity/relay/relay,
  libp2p/protocols/connectivity/relay/client,
  libp2p/crypto/crypto,
  libp2p/protocols/pubsub/gossipsub,
  libp2p/protocols/ping,
  libp2p/services/autorelayservice,
  libp2p/services/hpservice,
  libp2p/peerid,
  eth/keys,
  eth/p2p/discoveryv5/enr,
  presto,
  metrics,
  metrics/chronos_httpserver,
  brokers/broker_context,
  logos_delivery/api/types,
  logos_delivery/api/kernel_api,
  logos_delivery/waku/[
    waku_core,
    waku_node,
    waku_archive,
    rln,
    waku_store,
    waku_filter_v2,
    waku_relay/protocol,
    waku_enr/sharding,
    waku_enr/multiaddr,
    common/logging,
    node/peer_manager,
    node/health_monitor,
    net/net_config,
    net/nat_config,
    node/waku_metrics,
    node/subscription_manager,
    rest_api/message_cache,
    rest_api/endpoint/server,
    rest_api/endpoint/builder as rest_server_builder,
    discovery/waku_dnsdisc,
    discovery/waku_discv5,
    discovery/autonat_service,
    requests/health_requests,
    factory/node_factory,
    factory/internal_config,
    factory/app_callbacks,
    persistency/persistency,
    factory/validator_signed,
    waku_lightpush/client,
    waku_lightpush_legacy/client,
    waku_store/client,
  ],
  ./factory/waku_conf,
  ./factory/waku_state_info

# Surfaces the Kernel API interface to consumers of the Waku layer.
# `MessageSeenEvent` now lives in `events/kernel_events` (surfaced by the concentrator).
export kernel_api

logScope:
  topics = "wakunode waku"

# Git version in git describe format (defined at compile time)
const git_version* {.strdefine.} = "n/a"

type Waku* = ref object ## Implements `KernelApi` (ops in `waku/api/*`).
  stateInfo*: WakuStateInfo
  conf*: WakuConf
  rng*: crypto.Rng

  wakuDiscv5*: WakuDiscoveryV5
  dynamicBootstrapNodes*: seq[RemotePeerInfo]
  dnsRetryLoopHandle: Future[void]

  discv5NatRenewLoopHandle: Future[void]
  networkConnLoopHandle: Future[void]

  netConfig: Opt[NetConfig]
    ## The configured NetConfig, captured once by captureNetConf.
    ## syncEnr overrides its ENR fields with live state.
  discv5AnnouncedPort: Opt[Port] ## mapped external discv5 udp port; none = bind port

  node*: WakuNode

  healthMonitor*: NodeHealthMonitor

  restServer*: WakuRestServerRef
  metricsServer*: MetricsHttpServerRef
  appCallbacks*: AppCallbacks

  brokerCtx*: BrokerContext

proc setupSwitchServices(
    waku: Waku, conf: WakuConf, circuitRelay: Relay, rng: crypto.Rng
) =
  proc onReservation(addresses: seq[MultiAddress]) {.gcsafe, raises: [].} =
    info "circuit relay handler new reserve event",
      addrs_before = $(waku.node.announcedAddresses), addrs = $addresses

    waku.node.addressSources.relayReserved = addresses
    waku.node.recomputeAnnouncedAddresses()
    info "waku node announced addresses updated",
      announcedAddresses = waku.node.announcedAddresses

  let autonatService = getAutonatService(rng)
  let newService =
    if conf.circuitRelayClient:
      ## The node assumes it is behind NAT and not directly reachable.
      ## It requests circuit-relay reservations to stay reachable.
      const MaxNumRelayServers = 2
      let autoRelayService = AutoRelayService.new(
        MaxNumRelayServers, RelayClient(circuitRelay), onReservation, rng
      )
      Service(HPService.new(autonatService, autoRelayService))
    else:
      Service(autonatService)

  # Build-time services: the NATService (port mapping) and the wildcard
  # address resolver. Only the NATService stays.
  waku.node.switch.services.keepItIf(it of NATService)
  waku.node.switch.services.add(newService)

  # libp2p runs Service.setup only at build time (SwitchBuilder). This
  # service is attached post-build, so run its setup explicitly.
  try:
    newService.setup(waku.node.switch)
  except ServiceSetupError as e:
    error "failed to set up libp2p switch service", error = e.msg

## Initialisation

proc newCircuitRelay(isRelayClient: bool): Relay =
  # TODO: Does it mean it's a circuit-relay server when it's false?
  if isRelayClient:
    return RelayClient.new()
  return Relay.new()

proc setupAppCallbacks(
    node: WakuNode,
    conf: WakuConf,
    appCallbacks: AppCallbacks,
    healthMonitor: NodeHealthMonitor,
): Result[void, string] =
  if appCallbacks.isNil():
    info "No external callbacks to be set"
    return ok()

  if not appCallbacks.relayHandler.isNil():
    if node.wakuRelay.isNil():
      return err("Cannot configure relayHandler callback without Relay mounted")

    let autoShards =
      if node.wakuAutoSharding.isSome():
        node.getAutoshards(conf.contentTopics).valueOr:
          return err("Could not get autoshards: " & error)
      else:
        @[]

    let confShards = conf.subscribeShards.mapIt(
      RelayShard(clusterId: conf.clusterId, shardId: uint16(it))
    )
    let shards = confShards & autoShards

    let uniqueShards = deduplicate(shards)

    for shard in uniqueShards:
      let topic = $shard
      node.subscribe((kind: PubsubSub, topic: topic), appCallbacks.relayHandler).isOkOr:
        return err(fmt"Could not subscribe {topic}: " & $error)

  if not appCallbacks.topicHealthChangeHandler.isNil():
    if node.wakuRelay.isNil():
      return
        err("Cannot configure topicHealthChangeHandler callback without Relay mounted")
    node.wakuRelay.onTopicHealthChange = appCallbacks.topicHealthChangeHandler

  if not appCallbacks.connectionChangeHandler.isNil():
    if node.peerManager.isNil():
      return
        err("Cannot configure connectionChangeHandler callback with empty peer manager")
    node.peerManager.onConnectionChange = appCallbacks.connectionChangeHandler

  if not appCallbacks.connectionStatusChangeHandler.isNil():
    if healthMonitor.isNil():
      return
        err("Cannot configure connectionStatusChangeHandler with empty health monitor")

    healthMonitor.onConnectionStatusChange = appCallbacks.connectionStatusChangeHandler

  return ok()

proc new*(
    T: type Waku, wakuConf: WakuConf, appCallbacks: AppCallbacks = nil
): Future[Result[Waku, string]] {.async.} =
  let rng = crypto.newRng()
  let brokerCtx = globalBrokerContext()

  logging.setupLog(wakuConf.logLevel, wakuConf.logFormat)

  ?wakuConf.validate()
  wakuConf.logConf()

  let relay = newCircuitRelay(wakuConf.circuitRelayClient)

  let node = (await setupNode(wakuConf, rng, relay)).valueOr:
    error "Failed setting up node", error = $error
    return err("Failed setting up node: " & $error)

  let healthMonitor = NodeHealthMonitor.new(node, wakuConf.dnsAddrsNameServers)

  let restServer: WakuRestServerRef =
    if wakuConf.restServerConf.isSome():
      let restServer = startRestServerEssentials(
        healthMonitor, wakuConf.restServerConf.get()
      ).valueOr:
        error "Starting essential REST server failed", error = $error
        return err("Failed to start essential REST server in Waku.new: " & $error)

      restServer
    else:
      nil

  if not restServer.isNil():
    let boundRestPort = restServer.httpServer.address.port
    node.ports.rest = boundRestPort.uint16
    wakuConf.restServerConf.get().port = boundRestPort

  # Set the extMultiAddrsOnly flag so the node knows not to replace explicit addresses
  node.extMultiAddrsOnly = wakuConf.endpointConf.extMultiAddrsOnly

  node.setupAppCallbacks(wakuConf, appCallbacks, healthMonitor).isOkOr:
    error "Failed setting up app callbacks", error = error
    return err("Failed setting up app callbacks: " & $error)

  var waku = Waku(
    stateInfo: WakuStateInfo.init(node, wakuConf),
    conf: wakuConf,
    rng: rng,
    node: node,
    healthMonitor: healthMonitor,
    appCallbacks: appCallbacks,
    restServer: restServer,
    brokerCtx: brokerCtx,
  )

  waku.setupSwitchServices(wakuConf, relay, rng)

  ok(waku)

proc captureNetConf(waku: Waku): Future[Result[void, string]] {.async.} =
  ## Runs once, at start. Resolves the configured endpoint the ENR is
  ## built around (operator extip / dns4, the ports actually bound) and
  ## sets the configured source of the derivation from it. Config
  ## derivation runs here for the last time. Every later ENR rebuild
  ## applies live state on top of waku.netConfig.
  let conf = waku.conf
  let (tcpPort, websocketPort, quicPort) = getPorts(
    waku.node.switch.peerInfo.listenAddrs
  ).valueOr:
    return err("Could not retrieve ports: " & error)

  ## The resolved dynamic (port 0) ports are written back once, so the
  ## conf holds the ports the node actually listens on.
  if tcpPort.isSome():
    conf.endpointConf.p2pTcpPort = tcpPort.get()

  if websocketPort.isSome() and conf.webSocketConf.isSome():
    conf.webSocketConf.get().port = websocketPort.get()

  if quicPort.isSome() and conf.quicConf.isSome():
    conf.quicConf.get().port = quicPort.get()

  # Rebuild the configured NetConfig from the bound ports already read back
  # into `conf`.
  let netConfig = (
    await networkConfiguration(
      conf.clusterId, conf.endpointConf, conf.discv5Conf, conf.webSocketConf,
      conf.quicConf, conf.wakuFlags, conf.dnsAddrsNameServers,
    )
  ).valueOr:
    return err("Could not resolve the network configuration: " & error)

  waku.netConfig = Opt.some(netConfig)

  waku.node.setConfigAnnouncedAddresses(netConfig.announcedAddresses)
  waku.node.recomputeAnnouncedAddresses(notify = false)

  return ok()

proc syncEnr(waku: Waku): Result[void, string] =
  ## Build a fresh ENR from the live state: waku.netConfig, with
  ## the ENR fields overridden by the NAT mapping in place, the announced
  ## addresses as the multiaddrs, and a higher sequence number. Discv5
  ## peers refetch a record only when its sequence number is higher.
  var netConfig = waku.netConfig.valueOr:
    ## Before the capture nothing is announced. Start runs its own sync
    ## after the capture.
    return ok()

  let mappedPorts = getPorts(waku.node.natMappedExternalAddresses()).valueOr:
    return err("Could not retrieve NAT-mapped ports: " & error)
  let natIp = waku.node.natExternalIp()

  ## The external IP is used only while a mapping is in place.
  if mappedPorts.tcpPort.isSome() and natIp.isSome():
    netConfig.enrIp = natIp
    netConfig.enrPort = mappedPorts.tcpPort

  if waku.discv5AnnouncedPort.isSome():
    netConfig.discv5UdpPort = waku.discv5AnnouncedPort

  netConfig.enrMultiaddrs = waku.node.announcedAddresses

  let record = enrConfiguration(waku.conf, netConfig, waku.node.enr.seqNum + 1).valueOr:
    return err("ENR rebuild failed: " & error)

  if isClusterMismatched(record, waku.conf.clusterId):
    return err("cluster-id mismatch configured shards")

  waku.node.enr = record
  info "Waku node ENR updated successfully", enr = record.toUri(), record = $(record)

  if not waku.wakuDiscv5.isNil():
    waku.wakuDiscv5.protocol.localNode.record = record
    info "Waku discv5 ENR updated successfully",
      enr = record.toUri(), record = $(record)

  return ok()

proc updateWaku(waku: Waku): Future[Result[void, string]] {.async.} =
  (await captureNetConf(waku)).isOkOr:
    return err("captureNetConf failed: " & error)

  ?updateAnnouncedAddrWithPrimaryIpAddr(waku.node)

  ?syncEnr(waku)

  return ok()

proc setDiscv5AnnouncedPort(waku: Waku, port: Opt[Port]) {.gcsafe, raises: [].} =
  waku.discv5AnnouncedPort = port
  syncEnr(waku).isOkOr:
    error "failed to refresh ENR after discv5 NAT port change", error = error

proc startDnsDiscoveryRetryLoop(waku: Waku): Future[void] {.async.} =
  while true:
    await sleepAsync(30.seconds)
    if waku.conf.dnsDiscoveryConf.isSome():
      let dnsDiscoveryConf = waku.conf.dnsDiscoveryConf.get()
      waku.dynamicBootstrapNodes = (
        await waku_dnsdisc.retrieveDynamicBootstrapNodes(
          dnsDiscoveryConf.enrTreeUrl, dnsDiscoveryConf.nameServers
        )
      ).valueOr:
        error "Retrieving dynamic bootstrap nodes failed", error = error
        continue

    if not waku.wakuDiscv5.isNil():
      let dynamicBootstrapEnrs =
        waku.dynamicBootstrapNodes.filterIt(it.hasUdpPort()).mapIt(it.enr.get().toUri())
      var discv5BootstrapEnrs: seq[enr.Record]
      # parse enrURIs from the configuration and add the resulting ENRs to the discv5BootstrapEnrs seq
      for enrUri in dynamicBootstrapEnrs:
        addBootstrapNode(enrUri, discv5BootstrapEnrs)

      waku.wakuDiscv5.updateBootstrapRecords(
        waku.wakuDiscv5.protocol.bootstrapRecords & discv5BootstrapEnrs
      )

    info "Connecting to dynamic bootstrap peers"
    try:
      await connectToNodes(waku.node, waku.dynamicBootstrapNodes, "dynamic bootstrap")
    except CatchableError:
      error "failed to connect to dynamic bootstrap nodes: " & getCurrentExceptionMsg()
    return

proc start*(waku: Waku): Future[Result[void, string]] {.async: (raises: []).} =
  if waku.node.started:
    warn "start: waku node already started"
    return ok()

  info "Retrieve dynamic bootstrap nodes"
  let conf = waku.conf

  if conf.dnsDiscoveryConf.isSome():
    let dnsDiscoveryConf = waku.conf.dnsDiscoveryConf.get()
    let dynamicBootstrapNodesRes =
      try:
        await waku_dnsdisc.retrieveDynamicBootstrapNodes(
          dnsDiscoveryConf.enrTreeUrl, dnsDiscoveryConf.nameServers
        )
      except CatchableError as exc:
        Result[seq[RemotePeerInfo], string].err(
          "Retrieving dynamic bootstrap nodes failed: " & exc.msg
        )

    if dynamicBootstrapNodesRes.isErr():
      error "Retrieving dynamic bootstrap nodes failed",
        error = dynamicBootstrapNodesRes.error
      # Start Dns Discovery retry loop
      waku.dnsRetryLoopHandle = waku.startDnsDiscoveryRetryLoop()
    else:
      waku.dynamicBootstrapNodes = dynamicBootstrapNodesRes.get()

  ## Initialize persistency singleton instance - we don't need the instance itself here,
  ## but this ensures it's initialized before any store job starts.
  discard Persistency.instance(conf.localStoragePath).valueOr:
    error "Failed to initialize persistency instance", error = $error
    return err("Failed to initialize persistency instance: " & $error)

  (await startNode(waku.node, waku.conf, waku.dynamicBootstrapNodes)).isOkOr:
    return err("error while calling startNode: " & $error)

  let bound = getPorts(waku.node.switch.peerInfo.listenAddrs).valueOr:
    return err("failed to read bound ports from switch: " & $error)
  waku.node.ports.tcp = bound.tcpPort.get(Port(0)).uint16
  waku.node.ports.webSocket = bound.websocketPort.get(Port(0)).uint16
  waku.node.ports.quic = bound.quicPort.get(Port(0)).uint16

  ## Discv5
  if conf.discv5Conf.isSome():
    waku.wakuDiscV5 = (
      await waku_discv5.setupAndStartDiscv5(
        waku.node.enr,
        waku.node.peerManager,
        waku.node.topicSubscriptionQueue,
        conf.discv5Conf.get(),
        waku.dynamicBootstrapNodes,
        waku.rng,
        conf.nodeKey,
        conf.endpointConf.p2pListenAddress,
      )
    ).valueOr:
      return err("failed to start waku discovery v5: " & error)

    waku.node.ports.discv5Udp = waku.wakuDiscV5.udpPort.uint16
    waku.conf.discv5Conf.get().udpPort = waku.wakuDiscV5.udpPort

    ## The discv5 socket is outside the switch, so the NATService does not
    ## map it. Map it here and keep the lease alive, so the ENR carries the
    ## granted external port. The keeper owns the mapper and closes it.
    if conf.endpointConf.natStrategy.kind in {NatAny, NatUpnp, NatPmp}:
      let mapperOpt = natPortMapper(conf.endpointConf.natStrategy)
      if mapperOpt.isNone():
        info "discv5 NAT port mapping not available: no port mapper"
      else:
        let mapper = mapperOpt.get()
        let discoveryTimeout =
          conf.endpointConf.natDiscoveryTimeoutMs.int64.milliseconds
        try:
          let mapped = await mapUdpPort(
            mapper, waku.wakuDiscV5.udpPort, waku.wakuDiscV5.udpPort, discoveryTimeout
          )
          if mapped.isOk():
            ## The port goes into the ENR only when the switch also has
            ## an external address. The bind port stays in
            ## waku.node.ports.discv5Udp and waku.wakuDiscV5.udpPort.
            let projected = waku.node.natExternalIp().isSome()
            if projected:
              waku.discv5AnnouncedPort = Opt.some(mapped.get().externalPort)
            else:
              info "discv5 mapped but the switch has no external address; " &
                "keeping the bind port in the ENR"
            waku.discv5NatRenewLoopHandle = keepUdpMappingAlive(
              mapper,
              waku.wakuDiscV5.udpPort,
              mapped.get().externalPort,
              projected,
              proc(): bool {.gcsafe, raises: [].} =
                waku.node.natExternalIp().isSome(),
              proc(port: Opt[Port]) {.gcsafe, raises: [].} =
                waku.setDiscv5AnnouncedPort(port),
              discoveryTimeout = discoveryTimeout,
            )
          else:
            info "discv5 NAT port mapping not available", err = mapped.error
            await mapper.close()
        except CancelledError:
          info "discv5 NAT port mapping cancelled"
          await mapper.close()

  try:
    (await updateWaku(waku)).isOkOr:
      return err("updateWaku failed: " & $error)
  except CatchableError:
    return err("Caught exception in start: " & getCurrentExceptionMsg())

  ## Keep the ENR matching the announced addresses as mappings change.
  waku.node.onAnnouncedAddressesChange = proc() {.gcsafe, raises: [].} =
    syncEnr(waku).isOkOr:
      error "failed to refresh ENR after announced address change", error = error

  waku.node.subscriptionManager.subscribeAllAutoshards().isOkOr:
    return err("failed to auto-subscribe autosharding shards: " & $error)

  ## Health Monitor
  waku.healthMonitor.startHealthMonitor().isOkOr:
    return err("failed to start health monitor: " & $error)

  ## Setup RequestConnectionStatus provider

  RequestConnectionStatus.setProvider(
    globalBrokerContext(),
    proc(): Result[RequestConnectionStatus, string] =
      try:
        let healthReport = waku.healthMonitor.getSyncNodeHealthReport()
        return
          ok(RequestConnectionStatus(connectionStatus: healthReport.connectionStatus))
      except CatchableError:
        err("Failed to read health report: " & getCurrentExceptionMsg()),
  ).isOkOr:
    error "Failed to set RequestConnectionStatus provider", error = error

  ## Setup RequestProtocolHealth provider

  RequestProtocolHealth.setProvider(
    globalBrokerContext(),
    proc(
        protocol: WakuProtocol
    ): Future[Result[RequestProtocolHealth, string]] {.async.} =
      try:
        let protocolHealthStatus =
          await waku.healthMonitor.getProtocolHealthInfo(protocol)
        return ok(RequestProtocolHealth(healthStatus: protocolHealthStatus))
      except CatchableError:
        return err("Failed to get protocol health: " & getCurrentExceptionMsg()),
  ).isOkOr:
    error "Failed to set RequestProtocolHealth provider", error = error

  ## Setup RequestHealthReport provider

  RequestHealthReport.setProvider(
    globalBrokerContext(),
    proc(): Future[Result[RequestHealthReport, string]] {.async.} =
      try:
        let report = await waku.healthMonitor.getNodeHealthReport()
        return ok(RequestHealthReport(healthReport: report))
      except CatchableError:
        return err("Failed to get health report: " & getCurrentExceptionMsg()),
  ).isOkOr:
    error "Failed to set RequestHealthReport provider", error = error

  if conf.restServerConf.isSome():
    rest_server_builder.startRestServerProtocolSupport(
      waku.restServer,
      waku.node,
      waku.wakuDiscv5,
      conf.restServerConf.get(),
      conf.relay,
      conf.lightPush,
      conf.clusterId,
      conf.subscribeShards,
      conf.contentTopics,
    ).isOkOr:
      return err ("Starting protocols support REST server failed: " & $error)

  if conf.metricsServerConf.isSome():
    try:
      let (server, port) = (
        await waku_metrics.startMetricsServerAndLogging(conf.metricsServerConf.get())
      ).valueOr:
        return err("Starting monitoring and external interfaces failed: " & error)
      waku.metricsServer = server
      waku.node.ports.metrics = port.uint16
      waku.conf.metricsServerConf.get().httpPort = port
    except CatchableError:
      return err(
        "Caught exception starting monitoring and external interfaces failed: " &
          getCurrentExceptionMsg()
      )
  waku.healthMonitor.setOverallHealth(HealthStatus.READY)

  return ok()

proc stop*(waku: Waku): Future[Result[void, string]] {.async: (raises: []).} =
  if not waku.node.started:
    warn "stop: attempting to stop node that isn't running"

  try:
    waku.healthMonitor.setOverallHealth(HealthStatus.SHUTTING_DOWN)

    Persistency.reset()

    if not waku.metricsServer.isNil():
      await waku.metricsServer.stop()

    if not waku.wakuDiscv5.isNil():
      await waku.wakuDiscv5.stop()

    if not waku.node.isNil():
      await waku.node.stop()

    if not waku.dnsRetryLoopHandle.isNil():
      await waku.dnsRetryLoopHandle.cancelAndWait()

    if not waku.discv5NatRenewLoopHandle.isNil():
      await waku.discv5NatRenewLoopHandle.cancelAndWait()

    if not waku.healthMonitor.isNil():
      await waku.healthMonitor.stopHealthMonitor()

    ## Clear all providers registered in start() so a later start() can re-set them.
    RequestConnectionStatus.clearProvider(waku.brokerCtx)
    RequestProtocolHealth.clearProvider(waku.brokerCtx)
    RequestHealthReport.clearProvider(waku.brokerCtx)

    if not waku.restServer.isNil():
      await waku.restServer.stop()
  except Exception:
    error "waku stop failed: " & getCurrentExceptionMsg()
    return err("waku stop failed: " & getCurrentExceptionMsg())

  return ok()

{.pop.}

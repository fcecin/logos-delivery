{.push raises: [].}

## The address fields of the node's ENR.
##
## `ip`, `tcp` and `udp` name one host, decided from the current facts:
## the host discv5 learned from its peers, else the first announced IPv4
## TCP endpoint, else the configured baseline. Placeholders never enter.

import std/[net, sequtils]
import results, chronicles
import eth/common/keys, eth/p2p/discoveryv5/enr
import libp2p/[multiaddress, wire], libp2p/crypto/crypto
import ../net/net_config, ../waku_enr

logScope:
  topics = "waku enr"

type
  DiscoveryEndpoint* = tuple[ip: IpAddress, udp: Port]
  EnrBaseline* = tuple[ip: Opt[IpAddress], tcp: Opt[Port]]
  Scalars = tuple[ip: Opt[IpAddress], tcp: Opt[Port], udp: Opt[Port]]
  TcpEndpoint = tuple[ip: IpAddress, tcp: Port]

const ScalarKeys = ["id", "secp256k1", "ip", "ip6", "tcp", "tcp6", "udp", "udp6"]

proc tcpEndpoints(addrs: seq[MultiAddress]): seq[TcpEndpoint] =
  ## IPv4 only: nim-eth writes an IPv6 host as `ip6` but its port as `tcp`,
  ## next to whatever `ip` the record has. IPv6 travels in the field instead.
  var endpoints: seq[TcpEndpoint]
  for ma in addrs:
    if ma.isCircuitRelayMA() or not ma.isP2pTcpAddress() or not ma.isConcreteEndpoint():
      continue
    let ip = ma.getIp().valueOr:
      continue
    if ip.family != IpAddressFamily.IPv4:
      continue
    let address = initTAddress(ma).valueOr:
      continue
    endpoints.add((ip: ip, tcp: address.port))
  return endpoints

proc toEnrKey(key: crypto.PrivateKey): Result[keys.PrivateKey, string] =
  let bytes = key.getRawBytes().valueOr:
    return err("failed to read the node key: " & $error)
  let pk = keys.PrivateKey.fromRaw(bytes).valueOr:
    return err("failed to parse the node key: " & $error)
  return ok(pk)

proc rebuild(
    record: var enr.Record,
    pk: keys.PrivateKey,
    scalars: Scalars,
    fields: seq[FieldPair],
): Result[void, string] =
  ## Rebuilt, not updated: `Record.update` cannot remove a field, and an
  ## address that went away has to go away with it.
  if record.publicKey != pk.toPublicKey():
    return err("the node key does not match the record")
  if record.seqNum == high(uint64):
    return err("maximum ENR sequence number reached")
  let replaced = fields.mapIt(it[0])
  let kept = record.pairs.filterIt(it[0] notin ScalarKeys and it[0] notin replaced)
  let rebuilt = enr.Record.init(
    record.seqNum + 1, pk, scalars.ip, scalars.tcp, scalars.udp, kept & fields
  ).valueOr:
    return err("failed to rebuild the record: " & $error)
  record = rebuilt
  return ok()

proc fitAddresses(
    record: var enr.Record,
    pk: keys.PrivateKey,
    scalars: Scalars,
    addrs: seq[MultiAddress],
): Result[void, string] =
  ## Dropping tail entries only helps when the record is too large.
  for retained in countdown(addrs.len, 0):
    let fields =
      @[toFieldPair(MultiaddrEnrField, encodeMultiaddrs(addrs[0 ..< retained]))]
    if record.rebuild(pk, scalars, fields).isOk():
      debug "ENR addresses updated", retained = retained, total = addrs.len
      return ok()
  return err("failed to update ENR addresses at every prefix")

proc hasDialableAddress*(record: enr.Record): bool =
  let typed = record.toTyped().valueOr:
    return false
  if typed.ip().isSome() or typed.ip6().isSome():
    return true
  let announced = typed.multiaddrs().valueOr:
    return false
  return announced.len > 0

proc updateEnrAddresses*(
    record: var enr.Record,
    key: crypto.PrivateKey,
    addrs: seq[MultiAddress],
    baseline: EnrBaseline,
    learned = Opt.none(DiscoveryEndpoint),
): Result[void, string] =
  ## Write `addrs` into `record`: the multiaddrs field, and one endpoint in
  ## the scalars. A port is written only next to the host it belongs to.
  let pk = ?key.toEnrKey()
  let typed = record.toTyped().valueOr:
    return err("failed to read the record: " & $error)
  let usable = addrs.filterIt(it.isConcreteEndpoint())
  let endpoints = tcpEndpoints(usable)
  let udp =
    if typed.udp.isSome():
      Opt.some(Port(typed.udp.get()))
    else:
      Opt.none(Port)

  let scalars: Scalars =
    if learned.isSome():
      let host = learned.get().ip
      let onHost = endpoints.filterIt(it.ip == host)
      let tcp =
        if onHost.len > 0:
          Opt.some(onHost[0].tcp)
        else:
          baseline.tcp
      (ip: Opt.some(host), tcp: tcp, udp: Opt.some(learned.get().udp))
    elif endpoints.len > 0:
      (ip: Opt.some(endpoints[0].ip), tcp: Opt.some(endpoints[0].tcp), udp: udp)
    else:
      (ip: baseline.ip, tcp: baseline.tcp, udp: udp)

  let sorted =
    usable.filterIt(it.isCircuitRelayMA()) & usable.filterIt(not it.isCircuitRelayMA())
  return record.fitAddresses(pk, scalars, sorted)

proc updateEnrUdpPort*(
    record: var enr.Record, key: crypto.PrivateKey, port: Port
): Result[void, string] =
  ## Write `port` as the `udp` of `record`, next to the `ip` and `tcp` it has.
  ## When the record is too large, this drops multiaddrs from the end of the list.
  let pk = ?key.toEnrKey()
  let typed = record.toTyped().valueOr:
    return err("failed to read the record: " & $error)
  let host =
    if typed.ip.isSome():
      Opt.some(IpAddress(family: IpAddressFamily.IPv4, address_v4: typed.ip.get()))
    elif typed.ip6.isSome():
      Opt.some(IpAddress(family: IpAddressFamily.IPv6, address_v6: typed.ip6.get()))
    else:
      Opt.none(IpAddress)
  let tcp =
    if typed.tcp.isSome():
      Opt.some(Port(typed.tcp.get()))
    else:
      Opt.none(Port)
  let scalars: Scalars = (ip: host, tcp: tcp, udp: Opt.some(port))
  let addrs = typed.multiaddrs().valueOr:
    return record.rebuild(pk, scalars, @[])
  return record.fitAddresses(pk, scalars, addrs)

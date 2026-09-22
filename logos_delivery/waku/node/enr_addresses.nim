{.push raises: [].}

## The address fields of the node's ENR, written from the current facts.
##
## The scalars `ip`, `tcp` and `udp` name one host, decided in this order:
## the host discv5 learned from its peers, else the first announced IPv4 TCP
## endpoint the ENR carries, else the configured baseline (the external ip,
## the resolved name or the concrete bind host, with the bound TCP port). A
## port is written only next to the host it belongs to. Nothing is kept from
## the previous record: a NAT grant that went away goes away with it. Every
## write rebuilds the record (`Record.update` cannot remove a field) and keeps
## every field that is not an address. The multiaddrs field carries the
## announced set the caller passes, without placeholders, relay routes first.
import std/[net, sequtils]
import results, chronicles
import eth/keys, eth/p2p/discoveryv5/enr
import libp2p/[multiaddress, wire], libp2p/crypto/crypto
import ../net/net_config, ../waku_enr

logScope:
  topics = "waku enr"

type
  DiscoveryEndpoint* = tuple[ip: IpAddress, udp: Port]
    ## What discv5 learned from its peers: its host, and its port on that host.
  EnrBaseline* = tuple[ip: Opt[IpAddress], tcp: Opt[Port]]
    ## What the scalars say when no announced endpoint and no learned host
    ## decides them: the configured host, and the bound TCP port.
  Scalars = tuple[ip: Opt[IpAddress], tcp: Opt[Port], udp: Opt[Port]]
    ## The address fields nim-eth writes: one host and its ports.

const ScalarKeys = ["id", "secp256k1", "ip", "ip6", "tcp", "tcp6", "udp", "udp6"]
  ## The fields nim-eth writes itself. A rebuild passes them as arguments.

type TcpEndpoint = tuple[ip: IpAddress, tcp: Port]

proc tcpEndpoints(addrs: seq[MultiAddress]): seq[TcpEndpoint] =
  ## The announced IPv4 TCP endpoints a peer can dial, in the announced
  ## order. An IPv6 endpoint travels in the multiaddrs field only: nim-eth
  ## writes its host as `ip6` but its port as `tcp`, next to whatever `ip`
  ## the record has.
  for ma in addrs:
    if ma.isCircuitRelayMA() or not ma.isP2pTcpAddress() or not ma.isDialableMA():
      continue
    let ip = ma.getIp().valueOr:
      continue
    if ip.family != IpAddressFamily.IPv4:
      continue
    let address = initTAddress(ma).valueOr:
      continue
    result.add((ip: ip, tcp: address.port))

proc toEnrKey(key: crypto.PrivateKey): Result[keys.PrivateKey, string] =
  ## The node key as nim-eth sees it: the key the record is signed with.
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
  ## A new record with the given scalars only, the other fields of the old
  ## one (those in `fields` replaced) and the next sequence number.
  if record.publicKey != pk.toPublicKey():
    return err("the node key does not match the record")
  if record.seqNum == high(uint64):
    return err("maximum ENR sequence number reached")
  let replaced = fields.mapIt(it[0])
  let kept = record.pairs.filterIt(it[0] notin ScalarKeys and it[0] notin replaced)
  record = enr.Record.init(
    record.seqNum + 1, pk, scalars.ip, scalars.tcp, scalars.udp, kept & fields
  ).valueOr:
    return err($error)
  return ok()

proc updateEnrAddresses*(
    record: var enr.Record,
    key: crypto.PrivateKey,
    addrs: seq[MultiAddress],
    baseline: EnrBaseline,
    learned = Opt.none(DiscoveryEndpoint),
): Result[void, string] =
  ## Write the announced `addrs` into `record`: the multiaddrs field, and one
  ## host in the scalars. With `learned`, discv5 owns the host: `tcp` is the
  ## port of the first announced endpoint on that host, and absent when there
  ## is none. Else the first announced IPv4 TCP endpoint decides `ip` and
  ## `tcp`. Else the `baseline` does. `udp` is what discv5 learned, else what
  ## the record has. Entries with a placeholder host or port do not go into
  ## the field. Dropping tail entries of the field only helps when the record
  ## is too large. An empty set writes an empty field.
  let pk = ?key.toEnrKey()
  let typed = record.toTyped().valueOr:
    return err("failed to read the record: " & $error)
  let usable = addrs.filterIt(it.isDialableMA())
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
          Opt.none(Port)
      (ip: Opt.some(host), tcp: tcp, udp: Opt.some(learned.get().udp))
    elif endpoints.len > 0:
      (ip: Opt.some(endpoints[0].ip), tcp: Opt.some(endpoints[0].tcp), udp: udp)
    else:
      (ip: baseline.ip, tcp: baseline.tcp, udp: udp)

  let sorted =
    usable.filterIt(it.isCircuitRelayMA()) & usable.filterIt(not it.isCircuitRelayMA())
  for retained in countdown(sorted.len, 0):
    let fields =
      @[toFieldPair(MultiaddrEnrField, encodeMultiaddrs(sorted[0 ..< retained]))]
    if record.rebuild(pk, scalars, fields).isOk():
      debug "ENR addresses updated", retained = retained, total = sorted.len
      return ok()
  return err("failed to update ENR addresses at every prefix")

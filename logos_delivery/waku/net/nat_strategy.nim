{.push raises: [].}

## NatStrategy holds the user intent of --nat.
## nat_config maps a strategy onto the NATService.

import std/[net, strutils]
import results

type
  NatStrategyKind* {.pure.} = enum
    None
    Any
    Upnp
    Pmp
    ExtIp

  NatStrategy* = object
    case kind*: NatStrategyKind
    of ExtIp:
      extIp*: IpAddress
    else:
      discard

func `$`*(strategy: NatStrategy): string =
  case strategy.kind
  of None:
    return "none"
  of Any:
    return "any"
  of Upnp:
    return "upnp"
  of Pmp:
    return "pmp"
  of ExtIp:
    return "extip:" & $strategy.extIp

func parseNatStrategy*(value: string): Result[NatStrategy, string] =
  let normalized = value.strip().toLowerAscii()
  case normalized
  of "any":
    return ok(NatStrategy(kind: Any))
  of "none":
    return ok(NatStrategy(kind: None))
  of "upnp":
    return ok(NatStrategy(kind: Upnp))
  of "pmp":
    return ok(NatStrategy(kind: Pmp))
  else:
    const ExtIpPrefix = "extip:"
    if not normalized.startsWith(ExtIpPrefix):
      return err("not a valid NAT mechanism: " & value)

    let ipString = normalized[ExtIpPrefix.len ..^ 1]
    let ip =
      try:
        parseIpAddress(ipString)
      except ValueError:
        return err("not a valid IP address: " & ipString)

    ok(NatStrategy(kind: ExtIp, extIp: ip))

const DefaultNatDiscoveryTimeoutMs* = 1000'u32
  ## Node start awaits gateway discovery. miniupnpc waits the full timeout
  ## per SSDP round, so one second bounds the worst stall.

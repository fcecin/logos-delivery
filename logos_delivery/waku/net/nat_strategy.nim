{.push raises: [].}

## Logos Delivery's public NAT strategy vocabulary.
##
## `NatStrategy` describes user intent and preserves the `--nat` CLI
## contract. `nat_config` maps a strategy onto libp2p's NATService.

import std/[net, strutils]
import results

type
  NatStrategyKind* = enum
    NatNone
    NatAny
    NatUpnp
    NatPmp
    NatExtIp

  NatStrategy* = object
    case kind*: NatStrategyKind
    of NatExtIp:
      extIp*: IpAddress
    else:
      discard

func `$`*(strategy: NatStrategy): string =
  case strategy.kind
  of NatNone:
    "none"
  of NatAny:
    "any"
  of NatUpnp:
    "upnp"
  of NatPmp:
    "pmp"
  of NatExtIp:
    "extip:" & $strategy.extIp

func parseNatStrategy*(value: string): Result[NatStrategy, string] =
  let normalized = value.toLowerAscii()
  case normalized
  of "any":
    ok(NatStrategy(kind: NatAny))
  of "none":
    ok(NatStrategy(kind: NatNone))
  of "upnp":
    ok(NatStrategy(kind: NatUpnp))
  of "pmp":
    ok(NatStrategy(kind: NatPmp))
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

    ok(NatStrategy(kind: NatExtIp, extIp: ip))

const DefaultNatDiscoveryTimeoutMs* = 1000'u32
  ## Default time limit for the gateway discovery that the NATService
  ## performs during switch start. Node start awaits discovery.
  ## miniupnpc repeats its SSDP probe rounds, and each round waits the
  ## full timeout. On networks without a gateway the start stall is
  ## several times this value per mechanism (observed: 44s at libp2p's
  ## 10-second default, 20s at 3 seconds). One second keeps the
  ## worst-case stall in single digits.

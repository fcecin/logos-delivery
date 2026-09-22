# Configure QUIC transport

QUIC is a UDP-based transport. Enabling it allows peers to connect to your node over QUIC, in addition to the default TCP transport.

To enable QUIC, use the `--quic-support` option.
Note, the default port for QUIC is 60000.

```shell
logosdeliverynode --quic-support=true
```

To listen on a different UDP port, use `--quic-port`:

```shell
logosdeliverynode --quic-support=true --quic-port=<port>
```

QUIC runs alongside the existing TCP transport. The node keeps listening on TCP and announces a `/udp/<port>/quic-v1` address to the peers it connects to, so peers that support QUIC can connect over it while others continue to use TCP. The ENR carries the QUIC address when its host is one a peer can use from outside: an `--ext-ip`, a `--dns4-domain-name`, a concrete `--listen-address`, an `--ext-multiaddr`, or a NAT mapping. A node bound to the wildcard host without any of those announces its primary interface to connected peers, but its ENR omits that address, along with the `ip` field. To advertise a LAN endpoint on purpose, bind to that address or pass it as `--ext-multiaddr`.

If you restrict the node's announced addresses with `--ext-multiaddr-only`, the QUIC address is no longer announced automatically. In that case, include the QUIC multiaddr in `--ext-multiaddr` yourself, for example `/ip4/<ip>/udp/<port>/quic-v1`.

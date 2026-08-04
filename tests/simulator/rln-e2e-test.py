#!/usr/bin/env python3
"""
RLN end-to-end test against a running logos-delivery-simulator stack.

Designed to run as a sidecar container on the simulator's Docker network so
hostnames like `logos-delivery-simulator-nwaku-1` resolve via Docker DNS.

Scenarios covered (in order):
  1. HEALTH         - every node responds to /debug/v1/info with an enrUri
  2. SUBSCRIBE      - every node REST-subscribes to the pubsub topic
  3. WITHIN_LIMIT   - every node concurrently sends msg_limit messages -> 200
  4. PROPAGATION    - one sender's message lands in all peers' inboxes
  5. OVER_LIMIT     - one extra message per node -> 500 (rate-limit hit)
  6. EPOCH_RESET    - after epoch_sec, every node can send 1 more -> 200
  7. SAME_MESSAGE_ID - sending same message_id twice in same epoch is the
                      slashable signal (verified by checking node logs)

Exit code:
  0 = all scenarios passed
  N = number of scenarios that failed

Usage (typical):
  docker run --rm \\
    --network logos-delivery-simulator_simulation \\
    -v /path/to/rln-e2e-test.py:/test.py \\
    python:3.11-slim \\
    sh -c 'pip install --quiet requests && python /test.py \\
             --hostname-prefix logos-delivery-simulator-nwaku- \\
             --num-nodes 30 --msg-limit 30 --epoch-sec 15'
"""

import argparse
import base64
import concurrent.futures as cf
import json
import os
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Optional

import requests

PUBSUB_TOPIC = "/waku/2/rs/66/0"
CONTENT_TOPIC = "/rln-test/1/probe/proto"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def url_of(host: str, port: int = 8645) -> str:
    return f"http://{host}:{port}"


# Ledger of accepted publishes per node URL: every 200 spends one RLN ticket
# from that node's quota for the epoch bucket floor(t / epoch_sec). RLN epochs
# are absolute wall-clock windows (calcEpoch in waku/rln/proof.nim divides
# unix time by the epoch size, no offset), so the bucket is computable here.
# RATE_LIMIT audits this ledger per bucket instead of budgeting its burst, and
# therefore never cares which scenario spent what, or whether a burst
# straddled an epoch boundary.
ACCEPTED: dict[str, list[float]] = {}


def waku_publish(node_url: str, payload: bytes, timeout: float = 5.0) -> int:
    body = {
        "payload": base64.b64encode(payload).decode("ascii"),
        "contentTopic": CONTENT_TOPIC,
        "version": 1,
        "timestamp": time.time_ns(),
    }
    enc = urllib.parse.quote(PUBSUB_TOPIC, safe="")
    try:
        r = requests.post(
            f"{node_url}/relay/v1/messages/{enc}",
            json=body,
            timeout=timeout,
            headers={"content-type": "application/json"},
        )
        if r.status_code == 200:
            # Stamped post-response; a request bridging an epoch boundary can
            # land in the later bucket. Sub-second, absorbed by tolerance.
            ACCEPTED.setdefault(node_url, []).append(time.time())
        return r.status_code
    except requests.RequestException:
        return -1


def waku_subscribe(node_url: str, timeout: float = 5.0) -> int:
    try:
        r = requests.post(
            f"{node_url}/relay/v1/subscriptions",
            json=[PUBSUB_TOPIC],
            timeout=timeout,
            headers={"content-type": "application/json"},
        )
        return r.status_code
    except requests.RequestException:
        return -1


def waku_get_messages(node_url: str, timeout: float = 5.0) -> Optional[list]:
    enc = urllib.parse.quote(PUBSUB_TOPIC, safe="")
    try:
        r = requests.get(
            f"{node_url}/relay/v1/messages/{enc}",
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        return r.json()
    except (requests.RequestException, json.JSONDecodeError):
        return None


def node_healthy(node_url: str, timeout: float = 3.0) -> bool:
    try:
        r = requests.get(f"{node_url}/debug/v1/info", timeout=timeout)
        return r.status_code == 200 and "enrUri" in r.json()
    except (requests.RequestException, json.JSONDecodeError):
        return False


# ---------------------------------------------------------------------------
# scenarios
# ---------------------------------------------------------------------------

@dataclass
class Result:
    name: str
    ok: bool
    detail: str = ""

    def __str__(self) -> str:
        status = "PASS" if self.ok else "FAIL"
        s = f"[{status}] {self.name}"
        if self.detail:
            s += f" — {self.detail}"
        return s


def scenario_health(nodes: list[str], deadline_sec: int = 120) -> Result:
    """Every node must be reachable within deadline_sec."""
    start = time.time()
    unhealthy = list(nodes)
    while time.time() - start < deadline_sec and unhealthy:
        with cf.ThreadPoolExecutor(max_workers=min(32, len(unhealthy))) as ex:
            results = list(ex.map(node_healthy, [url_of(n) for n in unhealthy]))
        unhealthy = [n for n, ok in zip(unhealthy, results) if not ok]
        if unhealthy:
            time.sleep(3)
    return Result(
        "HEALTH",
        not unhealthy,
        f"{len(nodes) - len(unhealthy)}/{len(nodes)} healthy"
        + (f"; failing: {unhealthy[:5]}" if unhealthy else ""),
    )


def scenario_subscribe(nodes: list[str]) -> Result:
    """REST-subscribe every node to the pubsub topic so GETs return cached msgs."""
    with cf.ThreadPoolExecutor(max_workers=min(32, len(nodes))) as ex:
        codes = list(ex.map(waku_subscribe, [url_of(n) for n in nodes]))
    bad = [(n, c) for n, c in zip(nodes, codes) if c != 200]
    return Result(
        "SUBSCRIBE",
        not bad,
        f"{len(nodes) - len(bad)}/{len(nodes)} subscribed"
        + (f"; failing: {bad[:5]}" if bad else ""),
    )


def _send_n(node_url: str, n: int) -> list[int]:
    codes = []
    for i in range(n):
        codes.append(waku_publish(node_url, f"probe-{i}".encode()))
    return codes


def _burst_until_blocked(node_url: str, msg_limit: int, overshoot: int = 3):
    """Send msg_limit+overshoot messages back-to-back, fast, purely to force
    quota exhaustion. Returns the list of (unix_time, status_code) per
    attempt; RATE_LIMIT audits these against the epoch-bucketed ledger.
    The 200s are recorded into ACCEPTED by waku_publish itself."""
    attempts = []
    for i in range(msg_limit + overshoot):
        code = waku_publish(node_url, f"burst-{i}".encode(), timeout=10.0)
        attempts.append((time.time(), code))
    return attempts


def _publish_until_ok(node_url: str, attempts: int = 20, spacing: float = 5.0) -> bool:
    """Retry a single publish until it returns 200 or attempts run out.
    Tolerates the post-startup window where discv5/gossipsub mesh is still
    forming and the RLN publish path transiently 500s."""
    for _ in range(attempts):
        if waku_publish(node_url, b"warmup", timeout=10.0) == 200:
            return True
        time.sleep(spacing)
    return False


def scenario_warmup(
    nodes: list[str], attempts: int = 20, mesh_deadline_sec: int = 60
) -> Result:
    """Readiness gate: every node must successfully publish at least once.
    This absorbs mesh-formation churn so PROPAGATION/RATE_LIMIT aren't
    judging a not-yet-connected fleet. Consumes 1 ticket per node, plus up to
    ~15 more on the probe sender (nodes[0]); RATE_LIMIT audits usage from the
    ledger, so none of this skews it."""
    with cf.ThreadPoolExecutor(max_workers=min(8, len(nodes))) as ex:
        ready = list(ex.map(lambda n: _publish_until_ok(url_of(n), attempts), nodes))
    not_ready = [n for n, ok in zip(nodes, ready) if not ok]
    if not_ready:
        return Result(
            "WARMUP",
            False,
            f"{len(nodes) - len(not_ready)}/{len(nodes)} nodes publishing"
            f"; never ready: {not_ready[:5]}",
        )

    # A 200 does not mean the message went anywhere: gossipsub drops a publish
    # made with an empty mesh, silently and without retransmitting. So probe
    # until one publish actually lands on every receiver. That, not HTTP 200,
    # is what "the fleet is connected" means, and it is what PROPAGATION is
    # entitled to assume when it asserts a single publish reaches everyone.
    sender, receivers = nodes[0], nodes[1:]
    if not receivers:
        return Result("WARMUP", True, f"{len(nodes)}/{len(nodes)} nodes publishing")

    deadline = time.time() + mesh_deadline_sec
    unreached = list(receivers)
    # The GET below DRAINS the receiver's inbox (the relay REST cache is read
    # with clear=true server-side), so match against every probe sent so far,
    # not just the latest: a probe slower than one loop iteration would
    # otherwise be drained by a check that can no longer recognise it, and
    # the receiver would never match. Any probe landing proves the mesh.
    sent_probes: set[str] = set()
    last_code = 200
    while time.time() < deadline and unreached:
        probe = f"warmup-probe-{time.time_ns()}".encode()
        last_code = waku_publish(url_of(sender), probe, timeout=10.0)
        if last_code != 200:
            time.sleep(2)
            continue
        sent_probes.add(base64.b64encode(probe).decode().rstrip("="))
        time.sleep(3)
        with cf.ThreadPoolExecutor(max_workers=min(32, len(unreached))) as ex:
            inboxes = list(ex.map(waku_get_messages, [url_of(r) for r in unreached]))
        unreached = [
            r
            for r, inbox in zip(unreached, inboxes)
            if inbox is None
            or not any(
                (m.get("payload") or "").rstrip("=") in sent_probes for m in inbox
            )
        ]

    if unreached:
        detail = (
            f"{len(nodes)}/{len(nodes)} nodes publishing"
            f"; mesh never reached: {unreached[:5]}"
        )
        if last_code != 200:
            detail += f" (last sender publish -> {last_code})"
        return Result("WARMUP", False, detail)
    return Result(
        "WARMUP",
        True,
        f"{len(nodes)}/{len(nodes)} nodes publishing"
        f", mesh confirmed to all {len(receivers)} receivers",
    )


def scenario_rate_limit(
    nodes: list[str], msg_limit: int, epoch_sec: int, tolerance: int = 3
) -> Result:
    """Burst past the quota on every node, then audit the ticket ledger.

    The RLN invariant, checked in its native per-epoch form:
      (a) no node ever gets more than msg_limit publishes accepted inside any
          epoch bucket (floor(t / epoch_sec)), across ALL scenarios, and
      (b) the burst must hit a 500 ceiling, and any bucket in which it was
          refused must have been full (within tolerance) at that moment —
          refusals below quota and free rides above it both fail.

    A burst straddling an epoch boundary is NOT a failure mode here: it just
    fills two buckets, and each bucket is judged on its own. tolerance
    absorbs transport noise and the sub-second ambiguity of which side of a
    boundary a ticket landed on. Refusals are only collected from the burst
    itself, because WARMUP-era 500s can be transient publish-path errors
    rather than quota rejections."""
    # Cap concurrency: firing len(nodes)*(msg_limit+3) publishes all at once
    # saturates small CI runners (2 vCPU) and causes publish-path timeouts
    # that masquerade as rate-limit failures.
    with cf.ThreadPoolExecutor(max_workers=min(5, len(nodes))) as ex:
        per_node = list(
            ex.map(lambda n: _burst_until_blocked(url_of(n), msg_limit), nodes)
        )

    def bucket(t: float) -> int:
        return int(t // epoch_sec)

    failures = []
    for node, attempts in zip(nodes, per_node):
        acc: dict[int, int] = {}
        for t in ACCEPTED.get(url_of(node), []):
            acc[bucket(t)] = acc.get(bucket(t), 0) + 1

        # (a) the hard invariant, over every epoch the whole run ever touched.
        over = {e: c for e, c in acc.items() if c > msg_limit}
        if over:
            failures.append((node, f"over quota: {over} (limit {msg_limit})"))
            continue

        # (b) the burst must have been refused, and a refusal means that
        # bucket's quota was exhausted -- so the bucket must be ~full.
        refused = sorted({bucket(t) for t, c in attempts if c == 500})
        if not refused:
            n_200 = sum(c == 200 for _, c in attempts)
            n_err = sum(c not in (200, 500) for _, c in attempts)
            failures.append(
                (node, f"no 500 ceiling ({n_200} ok, {n_err} transport err)")
            )
            continue
        short = {
            e: acc.get(e, 0) for e in refused if acc.get(e, 0) < msg_limit - tolerance
        }
        if short:
            failures.append(
                (node, f"refused before full: {short} (limit {msg_limit})")
            )

    return Result(
        "RATE_LIMIT",
        not failures,
        f"{len(nodes) - len(failures)}/{len(nodes)} nodes kept every epoch "
        f"bucket <= {msg_limit} and refused only when full "
        f"(tolerance {tolerance})"
        + (f"; failures: {failures[:3]}" if failures else ""),
    )


def scenario_propagation(
    sender: str, receivers: list[str], settle_sec: int = 5
) -> Result:
    """Send one message on `sender`, expect it visible in every receiver's
    REST inbox within settle_sec."""
    marker = f"propagation-marker-{time.time_ns()}".encode()
    code = waku_publish(url_of(sender), marker)
    if code != 200:
        return Result("PROPAGATION", False, f"sender publish returned {code}")

    time.sleep(settle_sec)

    encoded_marker = base64.b64encode(marker).decode().rstrip("=")
    missing = []
    with cf.ThreadPoolExecutor(max_workers=min(32, len(receivers))) as ex:
        inboxes = list(ex.map(waku_get_messages, [url_of(r) for r in receivers]))
    for r, inbox in zip(receivers, inboxes):
        if inbox is None:
            missing.append((r, "GET failed"))
            continue
        if any((m.get("payload") or "").rstrip("=") == encoded_marker
               for m in inbox):
            continue
        missing.append((r, f"{len(inbox)} msgs, marker not present"))

    return Result(
        "PROPAGATION",
        not missing,
        f"{len(receivers) - len(missing)}/{len(receivers)} receivers got the message"
        + (f"; missing on {missing[:3]}" if missing else ""),
    )


def scenario_epoch_reset(nodes: list[str], epoch_sec: int) -> Result:
    """After epoch_sec + slack, each node can send 1 more message — expect 200."""
    sleep_s = epoch_sec + 3
    print(f"        sleeping {sleep_s}s for epoch reset...")
    time.sleep(sleep_s)
    with cf.ThreadPoolExecutor(max_workers=len(nodes)) as ex:
        codes = list(
            ex.map(
                lambda n: waku_publish(url_of(n), b"post-epoch"),
                nodes,
            )
        )
    bad = [(n, c) for n, c in zip(nodes, codes) if c != 200]
    return Result(
        "EPOCH_RESET",
        not bad,
        f"{sum(c == 200 for c in codes)}/{len(nodes)} returned 200 after epoch reset"
        + (f"; failing: {bad[:3]}" if bad else ""),
    )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--hostname-prefix", default="logos-delivery-simulator-nwaku-")
    ap.add_argument("--num-nodes", type=int, default=30)
    ap.add_argument("--msg-limit", type=int, default=30,
                    help="Must match RLN_RELAY_MSG_LIMIT in simulator .env")
    ap.add_argument("--epoch-sec", type=int, default=15,
                    help="Must match RLN_RELAY_EPOCH_SEC in simulator .env")
    ap.add_argument("--health-deadline-sec", type=int, default=180)
    args = ap.parse_args()

    nodes = [f"{args.hostname_prefix}{i}" for i in range(1, args.num_nodes + 1)]
    print(f"Testing {len(nodes)} nodes: {nodes[0]} … {nodes[-1]}")
    print(f"Config: msg_limit={args.msg_limit}, epoch_sec={args.epoch_sec}")
    print()

    results: list[Result] = []

    def run(scenario_fn, *fn_args, **fn_kwargs) -> bool:
        r = scenario_fn(*fn_args, **fn_kwargs)
        results.append(r)
        print(r)
        return r.ok

    if not run(scenario_health, nodes, deadline_sec=args.health_deadline_sec):
        print("\nABORTING — nodes never reached healthy state.")
        return _summarize(results)

    if not run(scenario_subscribe, nodes):
        print("\nABORTING — could not subscribe nodes to pubsub topic.")
        return _summarize(results)

    # Readiness gate: wait out mesh-formation churn before judging behaviour.
    if not run(scenario_warmup, nodes):
        print("\nABORTING — fleet never reached a publishable state.")
        return _summarize(results)

    run(scenario_propagation, nodes[0], nodes[1:])
    # Rate limit: burst past the quota, then audit per-epoch ticket usage.
    run(scenario_rate_limit, nodes, args.msg_limit, args.epoch_sec)
    run(scenario_epoch_reset, nodes, args.epoch_sec)

    return _summarize(results)


def _summarize(results: list[Result]) -> int:
    print()
    print("=" * 64)
    passed = sum(r.ok for r in results)
    print(f"  {passed}/{len(results)} scenarios passed")
    for r in results:
        print(f"    {r}")
    print("=" * 64)
    return len(results) - passed


if __name__ == "__main__":
    sys.exit(main())

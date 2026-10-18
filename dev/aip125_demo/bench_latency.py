# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
[DO NOT MERGE] AIP-125: how long a sender waits for the answer to a delivery that starts a Dag run.

Sends ``--count`` signed hmac_json deliveries, each with a delivery id of its own, ``--concurrency`` at a
time, and reports the latency percentiles against how long senders wait before they count a delivery
as failed: Slack 3 s, GitHub 10 s. A first batch of ``--warmup`` deliveries pays the webhooks process's
one-time costs, such as reading the connection, importing the source type and opening the pool's
database connections; it is reported on its own line and left out of the percentiles, which would
otherwise show that batch rather than the steady state. The latencies are measured over loopback on
the API server's host, without network, TLS or proxy time, so they are a lower bound of what a real
sender waits. With the defaults it sends ``payloads/hmac_json_deployed.json`` to the ``demo`` webhook of
``demo.sh``, to which the ``smoke_test_billing`` Dag subscribes, so each delivery records a topic event
and queues a run, which the scheduler works through afterwards.

It needs the processes of ``demo.sh``, so run it through ``demo.sh bench``, which passes it
``BENCH_COUNT`` and ``BENCH_CONCURRENCY``::

    breeze run --backend postgres bash -c \
        "BENCH_COUNT=200 BENCH_CONCURRENCY=4 bash /opt/airflow/dev/aip125_demo/demo.sh bench"
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import math
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

BUDGETS = {"Slack": 3.0, "GitHub": 10.0}
DEFAULT_BODY = Path(__file__).parent / "payloads" / "hmac_json_deployed.json"


def send(url: str, body: bytes, headers: dict[str, str]) -> tuple[float, int, str | None]:
    """Send one delivery; return the seconds until the whole answer arrived, its status and outcome."""
    started = time.monotonic()
    try:
        with urlopen(Request(url, data=body, headers=headers, method="POST"), timeout=60) as response:
            status, answer = response.status, response.read()
    except HTTPError as e:
        status, answer = e.code, e.read()
    elapsed = time.monotonic() - started
    try:
        outcome = json.loads(answer).get("outcome")
    except (ValueError, AttributeError):
        outcome = None
    return elapsed, status, outcome


def get_percentile(values: list[float], fraction: float) -> float:
    """Return the nearest-rank percentile of ``values``."""
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--url", default="http://localhost:8081/webhooks/demo")
    parser.add_argument("--secret", default="demo-secret", help="the password of connection webhook_<name>")
    parser.add_argument("--signature-header", default="X-Signature")
    parser.add_argument("--delivery-id-header", default="X-Demo-Delivery")
    parser.add_argument("--body", type=Path, default=DEFAULT_BODY, help="file with the JSON body to send")
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument(
        "--warmup",
        type=int,
        default=None,
        help="deliveries sent first and left out of the percentiles; default --concurrency",
    )
    parser.add_argument(
        "--expect-outcome", default="accepted", help="the outcome every delivery must get; empty for any"
    )
    args = parser.parse_args()

    body = args.body.read_bytes()
    signature = hmac.new(args.secret.encode(), body, hashlib.sha256).hexdigest()
    run = uuid.uuid4().hex[:8]

    def send_one(index: int) -> tuple[float, int, str | None]:
        headers = {
            "Content-Type": "application/json",
            args.signature_header: signature,
            args.delivery_id_header: f"latency-{run}-{index}",
        }
        return send(args.url, body, headers)

    warmup = args.concurrency if args.warmup is None else args.warmup
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        cold = list(pool.map(send_one, range(args.count, args.count + warmup)))
        started = time.monotonic()
        results = list(pool.map(send_one, range(args.count)))
        wall = time.monotonic() - started

    latencies = [elapsed for elapsed, _, _ in results]
    answers = Counter(f"{status} {outcome}" for _, status, outcome in results)
    if cold:
        cold_max = max(elapsed for elapsed, _, _ in cold)
        print(f"first {warmup} deliveries, which pay one-time costs, left out below: ", end="")
        print(f"max {cold_max * 1000:.0f} ms")
    print(f"{args.count} deliveries to {args.url}, {args.concurrency} at a time, in {wall:.1f} s")
    print("answers: " + ", ".join(f"{answer} x{count}" for answer, count in answers.most_common()))
    p50, p99 = get_percentile(latencies, 0.5), get_percentile(latencies, 0.99)
    print(f"latency: p50 {p50 * 1000:.0f} ms, p99 {p99 * 1000:.0f} ms, max {max(latencies) * 1000:.0f} ms")
    print("measured over loopback, without network, TLS or proxy time: a lower bound for real senders")
    for sender, budget in BUDGETS.items():
        verdict = "within" if p99 < budget else "OVER"
        print(f"{sender} waits {budget:.0f} s: p99 is {verdict} it")

    unexpected = [
        (status, outcome)
        for _, status, outcome in cold + results
        if not 200 <= status < 300 or (args.expect_outcome and outcome != args.expect_outcome)
    ]
    if unexpected:
        raise SystemExit(f"{len(unexpected)} deliveries got another answer than 2xx {args.expect_outcome}")


if __name__ == "__main__":
    main()

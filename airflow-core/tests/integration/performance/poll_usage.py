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
Sidecar of the performance harness: every few seconds, push over OTLP the CPU and memory of each
process tree given as ``role=pid``, and the worker slots Celery reports as held next to the budget
the harness configured. Exits on its own once every sampled process is gone.

The breeze OTel stack has no host or container metrics receiver, and every Airflow process is a
subprocess of the harness inside one container, so nothing else would measure what they cost.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import urllib.request

import psutil
from celery import Celery

from airflow.configuration import conf

log = logging.getLogger(__name__)

OTLP_METRICS_URL = os.environ.get(
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://breeze-otel-collector:4318/v1/metrics"
)
SERVICE_NAME = "perf-harness"


def parse_roles(pairs: list[str]) -> dict[str, int]:
    roles: dict[str, int] = {}
    for pair in pairs:
        role, _, pid = pair.partition("=")
        if not role or not pid.isdigit():
            raise SystemExit(f"expected ROLE=PID, got {pair!r}")
        roles[role] = int(pid)
    return roles


def sample_tree(pid: int) -> tuple[float, int] | None:
    """Return ``(cpu_seconds, rss_bytes)`` summed over ``pid`` and its live descendants."""
    try:
        root = psutil.Process(pid)
        processes = [root, *root.children(recursive=True)]
    except psutil.NoSuchProcess:
        return None
    cpu_seconds = 0.0
    rss_bytes = 0
    for process in processes:
        try:
            times = process.cpu_times()
            # cpu_times deltas rather than Process.cpu_percent: Celery and the task supervisor fork
            # per task, so the tree changes between samples and a new child's first cpu_percent
            # always reads zero. The children_* fields count reaped children through their parent,
            # so a task process that lived and died between two samples is not lost.
            cpu_seconds += times.user + times.system + times.children_user + times.children_system
            # Summed over the tree, so pages shared with forked children count more than once: it
            # tracks growth and compares test cases, it is not an absolute footprint.
            rss_bytes += process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
            continue
    return cpu_seconds, rss_bytes


def sample_roles(roles: dict[str, int]) -> dict[str, tuple[float, int]]:
    samples: dict[str, tuple[float, int]] = {}
    for role, pid in roles.items():
        if (sample := sample_tree(pid)) is not None:
            samples[role] = sample
    return samples


def count_occupied_slots(app: Celery) -> int | None:
    """Celery's own count of held slots across all workers, or None when no worker answered."""
    # Not pool.running_slots from the database: that only covers a task's execution phase, about
    # half of the time a task holds a slot; the rest is fork plus supervisor start and teardown.
    try:
        active = app.control.inspect(timeout=2.0).active()
    except Exception:
        log.exception("celery inspect failed")
        return None
    if active is None:
        return None
    return sum(len(tasks) for tasks in active.values())


def data_point(value: float | int, now_ns: int, role: str | None = None) -> dict:
    point: dict = {"timeUnixNano": str(now_ns), "attributes": []}
    if role is not None:
        point["attributes"].append({"key": "role", "value": {"stringValue": role}})
    if isinstance(value, float):
        point["asDouble"] = value
    else:
        point["asInt"] = str(value)
    return point


def gauge(name: str, points: list[dict]) -> dict:
    return {"name": name, "gauge": {"dataPoints": points}}


def build_payload(gauges: list[dict]) -> dict:
    return {
        "resourceMetrics": [
            {
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": SERVICE_NAME}}]},
                "scopeMetrics": [{"scope": {"name": "perf.poller"}, "metrics": gauges}],
            }
        ]
    }


def push(payload: dict) -> None:
    request = urllib.request.Request(
        OTLP_METRICS_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=5).read()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Push CPU, memory and worker slot usage of the harness processes to the OTel collector."
    )
    parser.add_argument("roles", nargs="+", metavar="ROLE=PID")
    parser.add_argument(
        "--slot-budget",
        type=int,
        required=True,
        help="worker slots the harness configured, pushed next to the occupied count",
    )
    parser.add_argument("--interval", type=float, default=5.0, help="seconds between samples")
    parser.add_argument(
        "--once", action="store_true", help="print one sample as JSON to stdout and exit without pushing"
    )
    args = parser.parse_args(argv)
    roles = parse_roles(args.roles)
    app = Celery(broker=conf.get("celery", "broker_url"))

    previous_at = time.monotonic()
    previous = sample_roles(roles)
    interval = 1.0 if args.once else args.interval
    while True:
        time.sleep(interval)
        now_at = time.monotonic()
        current = sample_roles(roles)
        elapsed = now_at - previous_at
        cpu_percent = {
            role: max(0.0, (cpu - previous[role][0]) / elapsed * 100.0)
            for role, (cpu, _) in current.items()
            if role in previous
        }
        rss_bytes = {role: rss for role, (_, rss) in current.items()}
        slots_occupied = count_occupied_slots(app)
        if args.once:
            sample = {
                "cpu_percent": cpu_percent,
                "rss_bytes": rss_bytes,
                "slots_occupied": slots_occupied,
                "slot_budget": args.slot_budget,
            }
            json.dump(sample, sys.stdout, indent=2)
            print()
            return

        now_ns = time.time_ns()
        gauges = [
            gauge(
                "perf.process.cpu_percent",
                [data_point(value, now_ns, role) for role, value in cpu_percent.items()],
            ),
            gauge(
                "perf.process.rss_bytes",
                [data_point(value, now_ns, role) for role, value in rss_bytes.items()],
            ),
            gauge("perf.worker.slot_budget", [data_point(args.slot_budget, now_ns)]),
        ]
        if slots_occupied is not None:
            gauges.append(gauge("perf.worker.slots_occupied", [data_point(slots_occupied, now_ns)]))
        try:
            push(build_payload([g for g in gauges if g["gauge"]["dataPoints"]]))
        except Exception:
            log.exception("push to the OTel collector failed; retrying next interval")
        if not current:
            log.info("every sampled process is gone; exiting")
            return
        previous, previous_at = current, now_at


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

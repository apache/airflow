#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "cachetools>=6.0.0",
# ]
# ///
"""
Measure the per-lookup overhead ``DBDagBag`` caching adds to the scheduler.

The scheduler calls ``DBDagBag.get_dag_for_run`` once per Dag run per scheduling
loop. Today its ``_dags`` is a plain dict guarded by ``nullcontext``. Giving the
scheduler a bounded cache swaps that for a cachetools mapping guarded by an
``RLock``, so this isolates the two costs that change:

* mapping bookkeeping -- ``dict`` vs ``LRUCache`` vs ``TTLCache``
* lock -- ``nullcontext`` vs an uncontended ``RLock``

It replays ``_get_dag``'s cache-hit path (the overwhelmingly common case): a
guarded ``.get()``, a ``time.monotonic()`` freshness comparison, and -- once the
revalidation window has elapsed -- a guarded write-back. No database, no
deserialization; those dominate a real miss and would drown the signal.

Run it with ``uv run``, which resolves ``cachetools`` from the inline script metadata above::

    uv run dev/airflow_perf/dag_bag_cache_overhead.py
    uv run dev/airflow_perf/dag_bag_cache_overhead.py --versions 5000 --lookups 200000
"""

from __future__ import annotations

import argparse
import statistics
import time
from contextlib import nullcontext
from threading import RLock
from typing import TYPE_CHECKING, Any, NamedTuple

from cachetools import LRUCache, TTLCache

if TYPE_CHECKING:
    from collections.abc import MutableMapping


class _CacheEntry(NamedTuple):
    """Mirror of ``airflow.models.dagbag._CacheEntry`` (a real Dag stands in as ``object()``)."""

    dag: Any
    dag_hash: str
    last_validated: float


def _replay_hit_path(
    dags: MutableMapping[str, _CacheEntry],
    lock: Any,
    version_ids: list[str],
    lookups: int,
    revalidation_interval: int,
) -> None:
    """Replay ``_get_dag``'s cache-hit path ``lookups`` times, round-robin over ``version_ids``."""
    n = len(version_ids)
    for i in range(lookups):
        version_id = version_ids[i % n]
        with lock:
            cached = dags.get(version_id)
        if cached is None:
            continue
        now = time.monotonic()
        if now - cached.last_validated < revalidation_interval:
            continue
        # Past the revalidation window: _get_dag re-confirms the hash (a DB round-trip we
        # deliberately skip) and writes the entry back. On a TTLCache this __setitem__ also
        # resets the entry's expiry, which is why hot entries never age out.
        with lock:
            current = dags.get(version_id)
            if current is not None:
                dags[version_id] = current._replace(last_validated=now)


def _build(kind: str, versions: int) -> MutableMapping[str, _CacheEntry]:
    cache: MutableMapping[str, _CacheEntry]
    if kind == "dict":
        cache = {}
    elif kind == "lru":
        cache = LRUCache(maxsize=versions)
    elif kind == "ttl":
        cache = TTLCache[str, _CacheEntry](maxsize=versions, ttl=3600)
    elif kind == "ttl-uncapped":
        cache = TTLCache[str, _CacheEntry](maxsize=float("inf"), ttl=3600)
    else:
        raise ValueError(f"unknown mapping kind: {kind}")
    return cache


def _time_once(
    kind: str, locked: bool, versions: int, lookups: int, revalidation_interval: int, stale: bool
) -> float:
    dags = _build(kind, versions)
    lock = RLock() if locked else nullcontext()
    version_ids = [f"version-{i}" for i in range(versions)]
    for version_id in version_ids:
        dags[version_id] = _CacheEntry(object(), "hash", time.monotonic())

    # Seeding ``last_validated`` in the past would only make the FIRST visit to each version
    # stale, because the write-back refreshes it -- 1 write-back per version, not per lookup.
    # A zero-length revalidation window keeps every lookup past the window instead.
    effective_interval = 0 if stale else revalidation_interval
    start = time.perf_counter()
    _replay_hit_path(dags, lock, version_ids, lookups, effective_interval)
    return time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--versions", type=int, default=1000, help="distinct Dag versions held")
    parser.add_argument("--lookups", type=int, default=200_000, help="get_dag_for_run calls to replay")
    parser.add_argument("--repeat", type=int, default=5, help="timed runs per configuration")
    parser.add_argument(
        "--revalidation-interval",
        type=int,
        default=30,
        help="[core] min_serialized_dag_update_interval",
    )
    args = parser.parse_args()

    configs = [
        ("dict + nullcontext  (scheduler today)", "dict", False),
        ("dict + RLock", "dict", True),
        ("LRUCache + RLock", "lru", True),
        ("TTLCache + RLock", "ttl", True),
        ("TTLCache uncapped + RLock  (proposed)", "ttl-uncapped", True),
        ("TTLCache uncapped + nullcontext", "ttl-uncapped", False),
    ]

    for stale in (False, True):
        branch = "write-back on every lookup" if stale else "all within revalidation window"
        print(f"\n{branch} -- {args.lookups:,} lookups over {args.versions:,} versions")
        print(f"{'configuration':<40} {'median':>10} {'ns/lookup':>12}")
        print("-" * 64)
        baseline_ns = None
        for label, kind, locked in configs:
            timings = [
                _time_once(kind, locked, args.versions, args.lookups, args.revalidation_interval, stale)
                for _ in range(args.repeat)
            ]
            median = statistics.median(timings)
            per_lookup_ns = median / args.lookups * 1e9
            if baseline_ns is None:
                baseline_ns = per_lookup_ns
                delta = "baseline"
            else:
                delta = f"{per_lookup_ns - baseline_ns:+.0f} ns"
            print(f"{label:<40} {median:>9.3f}s {per_lookup_ns:>9.0f} ns  {delta}")


if __name__ == "__main__":
    main()

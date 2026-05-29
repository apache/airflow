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
Shared helpers for the DAG-processing benchmark scripts.

These scripts establish *baselines* for the bottlenecks documented in
``README.md``. They are not A/B harnesses — the goal is to show where time is
spent and how it scales with input size, so later changes have something to
beat.

Benchmarking note (see the ``alert-on-weird-benchmarks`` lesson): absolute
numbers are machine- and load-dependent. We therefore report the **min** of
several samples (the least-contended run, most stable across machines) plus the
**median**, and we focus on *relative scaling across sizes* rather than the raw
milliseconds.
"""

from __future__ import annotations

import gc
import statistics
import time
from collections.abc import Callable
from typing import NamedTuple


class Timing(NamedTuple):
    """Result of timing a callable repeatedly."""

    best: float
    median: float
    samples: list[float]

    @property
    def best_ms(self) -> float:
        return self.best * 1_000

    @property
    def median_ms(self) -> float:
        return self.median * 1_000


def bench(fn: Callable[[], object], *, repeat: int = 7, warmup: int = 2, number: int = 1) -> Timing:
    """
    Time ``fn`` ``repeat`` times (after ``warmup`` untimed runs) and return min/median.

    :param number: how many calls make up a single timed sample (averaged). Use
        >1 for very cheap callables so the timer resolution is not the limit.
    """
    for _ in range(warmup):
        fn()

    gc_was_enabled = gc.isenabled()
    gc.disable()
    samples: list[float] = []
    try:
        for _ in range(repeat):
            start = time.perf_counter()
            for _ in range(number):
                fn()
            samples.append((time.perf_counter() - start) / number)
    finally:
        if gc_was_enabled:
            gc.enable()

    return Timing(best=min(samples), median=statistics.median(samples), samples=samples)


def print_header(title: str) -> None:
    line = "=" * 78
    print(f"\n{line}\n{title}\n{line}")


def print_row(*cells: object, widths: tuple[int, ...]) -> None:
    print("  ".join(str(c).rjust(w) for c, w in zip(cells, widths)))

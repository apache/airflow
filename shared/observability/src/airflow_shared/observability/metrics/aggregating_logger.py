#
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
"""Stats backend that folds metric calls into in-memory totals until a consumer drains them."""

from __future__ import annotations

import datetime
import random
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, NamedTuple

from .protocols import Timer

if TYPE_CHECKING:
    from .protocols import DeltaType

# Bounds on a single batch: exceeding either hands the batch to the consumer early instead of growing it.
MAX_METRICS = 1_000
MAX_TIMING_VALUES = 1_000

MetricKey = tuple[str, tuple[tuple[str, str], ...]]


class AggregatedMetrics(NamedTuple):
    """Everything an :class:`AggregatingLogger` accumulated since it was last drained."""

    counters: dict[MetricKey, int]
    gauges: dict[MetricKey, tuple[float, bool]]
    """Gauge value and whether it is a delta to apply to the previous reading."""
    timings: dict[MetricKey, list[float]]
    """Every observation in milliseconds, so a histogram sees each one."""


def _build_key(stat: str, tags: dict[str, Any] | None) -> MetricKey:
    if not tags:
        return stat, ()
    # Tag values become strings so the key hashes and so they survive any typed transport unchanged.
    return stat, tuple(sorted((k, str(v)) for k, v in tags.items()))


def _skip_due_to_rate(rate: int | float) -> bool:
    return rate < 1 and random.random() > rate


class _AggregatingTimer(Timer):
    def __init__(self, logger: AggregatingLogger, name: str | None, tags: dict[str, Any] | None):
        super().__init__()
        self._logger = logger
        self._name = name
        self._tags = tags

    def stop(self, send: bool = True) -> None:
        super().stop(send)
        if self._name and send and self.duration is not None:
            self._logger.timing(self._name, self.duration, tags=self._tags)


class AggregatingLogger:
    """
    Aggregate metrics in memory instead of exporting them.

    Counters sum their increments, gauges keep their latest value and timings keep every
    observation. :meth:`drain` hands the totals to whoever consumes them and starts over.
    ``on_large_batch`` is called when a batch grows past the size bounds.
    """

    def __init__(self, on_large_batch: Callable[[], None] | None = None):
        self._on_large_batch = on_large_batch
        self._lock = threading.Lock()
        self._counters: dict[MetricKey, int] = {}
        self._gauges: dict[MetricKey, tuple[float, bool]] = {}
        self._timings: dict[MetricKey, list[float]] = {}

    def incr(
        self, stat: str, count: int = 1, rate: int | float = 1, *, tags: dict[str, Any] | None = None
    ) -> None:
        if _skip_due_to_rate(rate):
            return
        key = _build_key(stat, tags)
        with self._lock:
            self._counters[key] = self._counters.get(key, 0) + count
            is_large = self._is_large()
        if is_large:
            self._notify_large_batch()

    def decr(
        self, stat: str, count: int = 1, rate: int | float = 1, *, tags: dict[str, Any] | None = None
    ) -> None:
        self.incr(stat, -count, rate, tags=tags)

    def gauge(
        self,
        stat: str,
        value: float,
        rate: int | float = 1,
        delta: bool = False,
        *,
        tags: dict[str, Any] | None = None,
    ) -> None:
        if _skip_due_to_rate(rate):
            return
        key = _build_key(stat, tags)
        with self._lock:
            previous = self._gauges.get(key)
            if delta and previous is not None:
                # Fold the increment into what is already recorded; the result is still a delta
                # only if nothing absolute was recorded before it.
                value += previous[0]
                delta = previous[1]
            self._gauges[key] = (value, delta)
            is_large = self._is_large()
        if is_large:
            self._notify_large_batch()

    def timing(self, stat: str, dt: DeltaType | None, *, tags: dict[str, Any] | None = None) -> None:
        if dt is None:
            return
        if isinstance(dt, datetime.timedelta):
            dt = dt.total_seconds() * 1000.0
        key = _build_key(stat, tags)
        with self._lock:
            values = self._timings.setdefault(key, [])
            values.append(float(dt))
            is_large = len(values) >= MAX_TIMING_VALUES or self._is_large()
        if is_large:
            self._notify_large_batch()

    def timer(self, stat: str | None = None, *args, tags: dict[str, Any] | None = None, **kwargs) -> Timer:
        return _AggregatingTimer(self, stat, tags)

    def drain(self) -> AggregatedMetrics:
        """Return everything accumulated so far and start over."""
        with self._lock:
            drained = AggregatedMetrics(self._counters, self._gauges, self._timings)
            self._counters = {}
            self._gauges = {}
            self._timings = {}
        return drained

    def _is_large(self) -> bool:
        return len(self._counters) + len(self._gauges) + len(self._timings) >= MAX_METRICS

    def _notify_large_batch(self) -> None:
        if self._on_large_batch is not None:
            self._on_large_batch()

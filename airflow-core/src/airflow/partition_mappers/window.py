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
from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar

from airflow._shared.module_loading import import_string

if TYPE_CHECKING:
    from collections.abc import Iterable


def _require_day_one(dt: datetime, window_cls: type) -> None:
    """
    Raise ``ValueError`` if *dt* is not on day 1 of its month.

    Month-aligned windows expand by month-step arithmetic, which is only
    safe when the period starts on day 1 (otherwise e.g.
    ``replace(month=feb)`` on Jan 31 raises ``ValueError: day is out of
    range for month``). Built-in temporal upstream mappers normalise to
    day 1, but a custom ``PartitionMapper.decode_downstream`` could return
    any day — checking here turns a confusing scheduler-tick crash into
    an explicit signal about the upstream-mapper contract.
    """
    if dt.day != 1:
        raise ValueError(
            f"{window_cls.__name__} expects a period start on day 1 of the month, "
            f"got {dt.isoformat()}. The paired upstream mapper's decode_downstream "
            "must normalise to day 1."
        )


def _shift_months(dt: datetime, months: int) -> datetime:
    """
    Return *dt* shifted forward by *months*, wrapping the year as needed.

    Caller is responsible for ensuring *dt* is on day 1 (see
    :func:`_require_day_one`) so that ``replace(month=...)`` is always valid.
    """
    total = dt.month - 1 + months
    return dt.replace(year=dt.year + total // 12, month=total % 12 + 1)


class Window(ABC):
    """
    Describes a rollup window: which decoded upstream items make up one decoded downstream period.

    Paired with a upstream mapper inside a :class:`RollupMapper`. The window
    operates purely in the upstream mapper's *decoded* form (``datetime`` for
    temporal mappers, domain-specific types for future segment / runtime
    mappers). It does not touch key strings, timezones, or formats — those
    belong to the upstream mapper. ``RollupMapper`` orchestrates the three:
    decode the downstream key, expand via the window, encode each upstream.

    The shipped temporal windows describe contiguous, non-overlapping periods
    in which each upstream key feeds exactly one downstream key. Sliding /
    overlapping semantics — one upstream key contributing to multiple adjacent
    downstream periods, e.g. a rolling 7-day window or the
    ``modifies-past-2-hours`` example from the AIP-76 spec — cannot be
    expressed by subclassing ``Window`` alone: ``Window.to_upstream`` only
    enumerates which upstream keys one downstream needs. The complementary
    direction (which downstreams an upstream key feeds) lives on the paired
    :meth:`PartitionMapper.to_downstream`, which already supports returning an
    iterable to fan out one source key across multiple target APDRs. Sliding
    therefore requires customizing **both** ``Window.to_upstream`` and the
    paired ``PartitionMapper.to_downstream`` consistently, so the schema
    invariant ``upstream_key in window.to_upstream(D) ⇔ D in
    mapper.to_downstream(upstream_key)`` holds.
    """

    #: Type that ``to_upstream`` expects as its ``decoded_downstream`` argument.
    #: ``RollupMapper.__init__`` uses this to reject pairings where the upstream
    #: mapper's ``decode_downstream`` leaves the value as ``str`` (base identity)
    #: but the window needs a different type. Temporal windows declare ``datetime``.
    expected_decoded_type: ClassVar[type] = str

    @abstractmethod
    def to_upstream(self, decoded_downstream: Any) -> Iterable[Any]:
        """Yield each decoded upstream item composing *decoded_downstream*."""

    def serialize(self) -> dict[str, Any]:
        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Window:
        return cls()


class HourWindow(Window):
    """Sixty consecutive minute period-starts making up one hour."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return (period_start + timedelta(minutes=i) for i in range(60))


class DayWindow(Window):
    """
    Twenty-four consecutive hourly period-starts making up one day.

    Arithmetic is done on naive datetime steps so the 24-hour stride is
    unambiguous across DST transitions; the upstream mapper handles timezone
    awareness when it encodes each upstream member back to a key string.

    .. warning:: **DST edge cases with local-timezone upstream mappers**

        ``DayWindow`` always yields exactly 24 steps regardless of the local
        calendar date. When the upstream mapper uses a local timezone
        (e.g. ``StartOfDayMapper(timezone="America/New_York")``), DST gaps
        and folds can cause a mismatch:

        - **Spring-forward (clock skips ahead)**: the local day has fewer than
          24 real hours. One naive step falls in the gap (e.g. 02:00 ET on
          spring-forward day does not exist), so the upstream mapper encodes it
          to the *next* local hour. That key (e.g. ``"2024-03-10T03"``) does
          not match any upstream event — the rollup window can never be fully
          satisfied.
        - **Fall-back (clock repeats)**: the local day has 25 real hours, but
          ``DayWindow`` only enumerates 24 steps. The extra hour's upstream
          events are never included in the expected set, so those events do not
          contribute to any rollup.

        **Mitigation**: use UTC ``input_format`` (e.g. ``%Y-%m-%dT%H%z``) and
        ensure upstream producers emit UTC partition keys so local-clock
        ambiguity never arises.
    """

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return (period_start + timedelta(hours=i) for i in range(24))


class WeekWindow(Window):
    """Seven consecutive daily period-starts making up one week."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        return (period_start + timedelta(days=i) for i in range(7))


class MonthWindow(Window):
    """
    All daily period-starts making up one calendar month.

    Requires *period_start* to be on day 1 of the month (built-in temporal
    upstream mappers normalise to day 1). Iterates from day 1 to the last
    day of that calendar month, so the yielded count varies between 28 and
    31 depending on the month. A non-day-1 input raises ``ValueError``.
    """

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        next_month = period_start.month % 12 + 1
        next_year = period_start.year + (1 if period_start.month == 12 else 0)
        next_start = period_start.replace(year=next_year, month=next_month)
        days = (next_start - period_start).days
        return (period_start + timedelta(days=i) for i in range(days))


class QuarterWindow(Window):
    """Three monthly period-starts making up one calendar quarter (e.g. Jan/Feb/Mar for Q1)."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        return (_shift_months(period_start, i) for i in range(3))


class YearWindow(Window):
    """Twelve consecutive monthly period-starts making up one calendar year."""

    expected_decoded_type: ClassVar[type] = datetime

    def to_upstream(self, period_start: datetime) -> Iterable[datetime]:
        _require_day_one(period_start, type(self))
        return (_shift_months(period_start, i) for i in range(12))


class SegmentWindow(Window):
    """
    A fixed categorical rollup window.

    Represents a static set of segment keys (e.g. regions, tenants, variants) that must
    all be present before the downstream Dag run is released.

    Unlike temporal windows, ``SegmentWindow`` ignores the downstream anchor completely —
    ``to_upstream`` always returns the same declared segment set regardless of the key passed
    in. Pair with ``IdentityMapper`` inside a ``RollupMapper`` so that both the upstream and
    downstream keys are plain strings and no encode/decode conversion is needed.

    Example::

        from airflow.sdk import RollupMapper, IdentityMapper, SegmentWindow

        mapper = RollupMapper(
            upstream_mapper=IdentityMapper(),
            window=SegmentWindow(["us-east", "eu-west", "ap-south"]),
        )

    The scheduler holds the downstream Dag run until every segment key in the declared
    set has emitted an upstream asset event. This only works with ``WAIT_FOR_ALL``
    trigger semantics (the default). Dynamic or threshold-based semantics are out of
    scope.

    :param segments: Non-empty iterable of non-empty string segment keys. Duplicates are
        silently de-duplicated; the resulting set order is deterministic across processes.
    :raises ValueError: if *segments* is empty, contains a non-``str`` element, or
        contains an empty-string key.
    """

    expected_decoded_type: ClassVar[type] = str

    def __init__(self, segments: Iterable[str]) -> None:
        collected: list[str] = list(segments)
        if not collected:
            raise ValueError("SegmentWindow requires at least one segment key; got an empty iterable.")
        for i, item in enumerate(collected):
            if not isinstance(item, str):
                raise ValueError(
                    f"SegmentWindow segment keys must be str; "
                    f"got {type(item).__name__!r} at index {i}: {item!r}"
                )
            if item == "":
                raise ValueError(
                    f"SegmentWindow segment keys must be non-empty strings; got an empty string at index {i}."
                )
        self._segments: frozenset[str] = frozenset(collected)

    def to_upstream(self, decoded_downstream: Any) -> frozenset[str]:
        """Return the complete set of declared segment keys, ignoring the downstream anchor."""
        return self._segments

    def serialize(self) -> dict[str, Any]:
        return {"segments": sorted(self._segments)}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> SegmentWindow:
        return cls(data["segments"])


def _validate_resolver(fn: Callable) -> str:
    """
    Validate that *fn* is a module-level function and return its dotted import path.

    Rejects lambdas, closures/nested functions, bound methods, and anything not a
    plain function. Also verifies the function round-trips via ``import_string`` so
    the scheduler can re-import it.

    :raises TypeError: if *fn* is not a plain function.
    :raises ValueError: if *fn* is a lambda, closure, nested function, or non-importable.
    """
    if not inspect.isfunction(fn):
        raise TypeError(
            f"DynamicSegmentWindow resolver must be a plain function, got {type(fn).__name__!r}. "
            "Must be a module-level function so it is serializable as a dotted path and "
            "re-importable by the scheduler. Bound methods are rejected."
        )
    if fn.__name__ == "<lambda>":
        raise ValueError(
            "DynamicSegmentWindow resolver must be a module-level function so it is "
            "serializable as a dotted path. Lambdas are rejected at construction."
        )
    if "<locals>" in fn.__qualname__:
        raise ValueError(
            "DynamicSegmentWindow resolver must be a module-level function so it is "
            "serializable as a dotted path and re-importable by the scheduler. "
            "Closures and nested functions are rejected at construction."
        )
    dotted_path = f"{fn.__module__}.{fn.__qualname__}"
    try:
        imported = import_string(dotted_path)
    except ImportError as exc:
        raise ValueError(
            f"DynamicSegmentWindow resolver {dotted_path!r} is not importable via its dotted "
            f"path; the scheduler stores the path and re-imports at scheduling time. "
            f"Ensure the function is defined at module level and exported from its module. "
            f"ImportError: {exc}"
        ) from exc
    if imported is not fn:
        raise ValueError(
            f"DynamicSegmentWindow resolver {dotted_path!r} does not round-trip via import: "
            f"import_string({dotted_path!r}) returned a different object. "
            "The function must be accessible at its dotted path without any re-binding."
        )
    return dotted_path


class DynamicSegmentWindow(Window):
    """
    A runtime-resolved categorical rollup window.

    Unlike :class:`SegmentWindow`, the expected set of segment keys is produced at
    scheduling time by calling a **module-level callable** (the *resolver*). The
    scheduler re-imports the callable from its dotted path, invokes it with no
    arguments, and expects an iterable of non-empty ``str`` segment keys.

    Like :class:`SegmentWindow`, the downstream anchor is ignored completely —
    ``to_upstream`` always returns the full set that the resolver produces,
    regardless of the downstream key. Pair with :class:`~airflow.partition_mappers.identity.IdentityMapper`
    inside a :class:`~airflow.partition_mappers.base.RollupMapper`::

        from airflow.sdk import RollupMapper, IdentityMapper, DynamicSegmentWindow


        def list_active_regions() -> list[str]:
            # Could read from a config file, environment variable, etc.
            return ["us-east", "eu-west", "ap-south"]


        mapper = RollupMapper(
            upstream_mapper=IdentityMapper(),
            window=DynamicSegmentWindow(list_active_regions),
        )

    The downstream Dag run is held until every segment key returned by the resolver
    has emitted an upstream asset event. This only works with ``WAIT_FOR_ALL`` trigger
    semantics (the default).

    .. warning:: **Resolver is called on every scheduler tick (commit 1 behavior)**

        In this implementation, ``to_upstream`` re-invokes the resolver on every
        scheduler tick. This means the expected segment set can **drift between ticks**
        if the resolver is non-deterministic. A follow-up commit will add a per-period
        snapshot to freeze the resolved set at the moment the Dag run is created.

        Until that snapshot is in place:

        - The resolver **must be deterministic and side-effect-free** across calls.
          Returning different values on different calls may cause the scheduler to
          simultaneously hold and release a Dag run, or never release it.
        - The resolver is **invoked by the scheduler process**, not by user task code.
          Do not use logic that requires task-side context (XCom, task parameters, etc.).

    **Serialization**: the scheduler stores the resolver's dotted import path
    (``module.function``) in the Dag serialization. At scheduling time the scheduler
    re-imports the function from that path. The callable must therefore be:

    - A **module-level function** (not a lambda, closure, or nested function).
    - Accessible at its dotted path without re-binding (i.e.
      ``import_string("module.function")`` must return the same object).

    :param resolver: A module-level callable with signature ``() -> Iterable[str]``.
        Must not be a lambda, closure/nested function, or bound method.
    :raises TypeError: if *resolver* is not a plain function.
    :raises ValueError: if *resolver* is a lambda, closure, nested function, or not
        importable via its dotted path at construction time.
    """

    expected_decoded_type: ClassVar[type] = str

    def __init__(self, resolver: Callable[[], Any]) -> None:
        self._resolver_path: str = _validate_resolver(resolver)
        self._resolver: Callable[[], Any] = resolver

    def to_upstream(self, decoded_downstream: Any) -> frozenset[str]:
        """
        Invoke the resolver and return the resulting segment set, ignoring the downstream anchor.

        The resolver is called with no arguments and must return an iterable of
        non-empty ``str`` values. The downstream anchor is intentionally ignored —
        every downstream key maps to the same resolver-produced set.

        :raises ValueError: if the resolver returns a non-str or empty-string element.
        """
        results: list[str] = list(self._resolver())
        for i, item in enumerate(results):
            if not isinstance(item, str):
                raise ValueError(
                    f"DynamicSegmentWindow resolver {self._resolver_path!r} returned a non-str "
                    f"value at index {i}: got {type(item).__name__!r} ({item!r}). "
                    "The resolver must return an iterable of non-empty str segment keys."
                )
            if item == "":
                raise ValueError(
                    f"DynamicSegmentWindow resolver {self._resolver_path!r} returned an empty "
                    f"string at index {i}. Segment keys must be non-empty strings."
                )
        return frozenset(results)

    def serialize(self) -> dict[str, Any]:
        return {"resolver": self._resolver_path}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> DynamicSegmentWindow:
        resolver = import_string(data["resolver"])
        return cls(resolver)

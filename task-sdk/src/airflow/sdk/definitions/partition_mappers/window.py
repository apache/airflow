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
from collections.abc import Callable
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.sdk._shared.module_loading import import_string

if TYPE_CHECKING:
    from collections.abc import Iterable


class Window:
    """
    Describes a rollup window: which upstream keys make up one downstream key.

    Paired with a ``upstream_mapper`` :class:`PartitionMapper` inside a
    :class:`RollupMapper`. The upstream_mapper normalizes upstream keys to the
    downstream granularity; the window enumerates the complete set of
    upstream keys that roll up into one downstream key. Runtime logic
    lives in ``airflow.partition_mappers.window`` on the scheduler side.

    The shipped temporal windows describe contiguous, non-overlapping periods
    in which each upstream key feeds exactly one downstream key. Sliding /
    overlapping semantics — e.g. a rolling 7-day window, or the
    ``modifies-past-2-hours`` example from the AIP-76 spec — cannot be
    expressed by subclassing ``Window`` alone, because ``Window.to_upstream``
    only enumerates the upstream keys one downstream needs. The complementary
    direction (which downstreams an upstream key feeds) lives on the paired
    :meth:`PartitionMapper.to_downstream`, which supports returning an iterable
    to fan out one source key across multiple target partitions. Sliding
    therefore requires customizing **both** sides consistently so the
    invariant ``upstream_key in window.to_upstream(D) ⇔ D in
    mapper.to_downstream(upstream_key)`` holds.
    """

    #: Decoded type the window iterates in; ``RollupMapper.__init__`` uses this
    #: to reject pairings where the upstream mapper decodes to a different type.
    #: Default ``str`` matches the identity mapper; temporal windows declare
    #: ``datetime``. Mirrors the same attribute on the core ``Window``.
    expected_decoded_type: ClassVar[type] = str
    #: Whether this window's ``to_upstream`` ignores the downstream anchor and
    #: returns the same upstream set for every downstream key. Such windows only
    #: roll up correctly when paired with a mapper that collapses every upstream
    #: key onto one downstream partition (see
    #: :attr:`PartitionMapper.collapses_to_constant`); ``RollupMapper.__init__``
    #: enforces this. Segment windows set this to ``True``.
    requires_collapsing_mapper: ClassVar[bool] = False


class HourWindow(Window):
    """Sixty consecutive minute keys making up one hour."""

    expected_decoded_type: ClassVar[type] = datetime


class DayWindow(Window):
    """Twenty-four consecutive hourly keys making up one day."""

    expected_decoded_type: ClassVar[type] = datetime


class WeekWindow(Window):
    """Seven consecutive daily keys making up one week."""

    expected_decoded_type: ClassVar[type] = datetime


class MonthWindow(Window):
    """All daily keys making up one calendar month."""

    expected_decoded_type: ClassVar[type] = datetime


class QuarterWindow(Window):
    """Three consecutive monthly keys making up one calendar quarter."""

    expected_decoded_type: ClassVar[type] = datetime


class YearWindow(Window):
    """Twelve consecutive monthly keys making up one calendar year."""

    expected_decoded_type: ClassVar[type] = datetime


class SegmentWindow(Window):
    """
    A fixed categorical rollup window.

    Represents a static set of segment keys (e.g. regions, tenants, variants). Pair with
    :class:`~airflow.sdk.definitions.partition_mappers.constant.ConstantMapper` inside a
    ``RollupMapper`` so that every segment event collapses onto one downstream partition
    and the downstream Dag run is held until every declared segment has emitted an
    upstream asset event.

    Unlike temporal windows, ``SegmentWindow`` ignores the downstream anchor completely —
    the runtime ``to_upstream`` on the core side always returns the same declared segment
    set regardless of the downstream key. Because the same set is expected for every
    downstream key, all upstream events must land in a single downstream partition; pairing
    with a non-collapsing mapper (e.g. ``IdentityMapper``) fans each segment into its own
    partition so the rollup never completes — ``RollupMapper.__init__`` rejects that pairing.
    Runtime logic lives in ``airflow.partition_mappers.window.SegmentWindow`` on the
    scheduler side.

    :param segments: Non-empty iterable of non-empty string segment keys. Duplicates are
        silently de-duplicated.
    :raises ValueError: if *segments* is empty, contains a non-``str`` element, or
        contains an empty-string key.
    """

    expected_decoded_type: ClassVar[type] = str
    requires_collapsing_mapper: ClassVar[bool] = True

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


def _validate_resolver(fn: Callable) -> str:
    """
    Validate that *fn* is a module-level function and return its dotted import path.

    Mirrors the same helper in ``airflow.partition_mappers.window`` so parse-time
    validation on the SDK side is identical to what the scheduler side enforces.

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

    Like :class:`SegmentWindow`, the downstream anchor is ignored completely. Pair with
    :class:`~airflow.sdk.definitions.partition_mappers.constant.ConstantMapper` inside a
    :class:`~airflow.sdk.definitions.partition_mappers.base.RollupMapper` so every segment
    event collapses onto one downstream partition::

        from airflow.sdk import RollupMapper, ConstantMapper, DynamicSegmentWindow


        def list_active_regions() -> list[str]:
            return ["us-east", "eu-west", "ap-south"]


        mapper = RollupMapper(
            upstream_mapper=ConstantMapper("all_regions"),
            window=DynamicSegmentWindow(list_active_regions),
        )

    Pairing with a non-collapsing mapper (e.g. ``IdentityMapper``) fans each segment into
    its own partition so the rollup never completes; ``RollupMapper.__init__`` rejects that
    pairing. Runtime logic (``to_upstream``) lives in
    ``airflow.partition_mappers.window.DynamicSegmentWindow`` on the scheduler side.

    .. warning:: **Resolver is called on every scheduler tick (commit 1 behavior)**

        In this implementation, ``to_upstream`` re-invokes the resolver on every
        scheduler tick. The resolver must be deterministic and side-effect-free across
        calls. A follow-up commit will add a per-period snapshot to freeze the resolved
        set.

    :param resolver: A module-level callable with signature ``() -> Iterable[str]``.
        Must not be a lambda, closure/nested function, or bound method.
    :raises TypeError: if *resolver* is not a plain function.
    :raises ValueError: if *resolver* is a lambda, closure, nested function, or not
        importable via its dotted path at construction time.
    """

    expected_decoded_type: ClassVar[type] = str
    requires_collapsing_mapper: ClassVar[bool] = True

    def __init__(self, resolver: Callable[[], Any]) -> None:
        self._resolver_path: str = _validate_resolver(resolver)
        self._resolver: Callable[[], Any] = resolver

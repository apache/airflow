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

from datetime import datetime
from typing import ClassVar


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

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
Declarative OTel histogram bucket views keyed by metric-name pattern.

PR #64207 standardised timer histograms on
:class:`~opentelemetry.sdk.metrics.view.ExponentialBucketHistogramAggregation`
at the instrument-type level.  This module extends that idea to non-timer
histograms (``*_count``, ``*_duration``, ``*_delay`` and similar families):
bucket shape is declared once, here, instead of being chosen per call site.

The mapping is intentionally narrow.  Patterns are simple suffix globs and
each one maps to a single named aggregation.  Deployments that need a
different bucket layout can pass an override dict to
:func:`build_views_for_patterns`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    ExponentialBucketHistogramAggregation,
    View,
)

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics._internal.aggregation import Aggregation

# Latency-shaped metrics span milliseconds to minutes; exponential buckets
# adapt without hand-tuned boundaries.
LATENCY_BUCKETS: Aggregation = ExponentialBucketHistogramAggregation()

# Small unbounded counts (queue depths, retry attempts, fan-out sizes).
COUNT_BUCKETS: Aggregation = ExplicitBucketHistogramAggregation(
    boundaries=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000),
)

# Large-range delays (schedule lag, dependency wait); seconds to hours.
DELAY_BUCKETS: Aggregation = ExplicitBucketHistogramAggregation(
    boundaries=(1, 5, 15, 60, 300, 900, 1800, 3600, 7200, 21600),
)

DEFAULT_PATTERN_BUCKETS: dict[str, Aggregation] = {
    "*_duration": LATENCY_BUCKETS,
    "*_delay": DELAY_BUCKETS,
    "*_count": COUNT_BUCKETS,
}


def build_views_for_patterns(
    pattern_buckets: dict[str, Aggregation] | None = None,
) -> list[View]:
    """
    Return one OTel ``View`` per ``(instrument-name pattern, aggregation)`` entry.

    :param pattern_buckets: Optional override of the default pattern map.  When
        ``None``, :data:`DEFAULT_PATTERN_BUCKETS` is used.  Each key is a
        ``View.instrument_name`` glob (e.g. ``"*_duration"``) and each value
        is the aggregation to apply.
    :returns: A list of :class:`~opentelemetry.sdk.metrics.view.View` objects
        suitable for passing to ``MeterProvider(views=...)``.
    """
    mapping = DEFAULT_PATTERN_BUCKETS if pattern_buckets is None else pattern_buckets
    return [
        View(instrument_name=pattern, aggregation=aggregation) for pattern, aggregation in mapping.items()
    ]

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

import pytest
from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    ExponentialBucketHistogramAggregation,
    View,
)

from airflow_shared.observability.metrics.histogram_buckets import (
    COUNT_BUCKETS,
    DEFAULT_PATTERN_BUCKETS,
    DELAY_BUCKETS,
    LATENCY_BUCKETS,
    build_views_for_patterns,
)


class TestHistogramBuckets:
    def test_default_patterns_cover_expected_families(self):
        assert set(DEFAULT_PATTERN_BUCKETS) == {"*_count", "*_duration", "*_delay"}

    @pytest.mark.parametrize(
        ("pattern", "expected"),
        [
            ("*_duration", LATENCY_BUCKETS),
            ("*_delay", DELAY_BUCKETS),
            ("*_count", COUNT_BUCKETS),
        ],
    )
    def test_pattern_maps_to_expected_aggregation(self, pattern, expected):
        assert DEFAULT_PATTERN_BUCKETS[pattern] is expected

    def test_latency_buckets_is_exponential(self):
        assert isinstance(LATENCY_BUCKETS, ExponentialBucketHistogramAggregation)

    def test_count_and_delay_buckets_are_explicit(self):
        assert isinstance(COUNT_BUCKETS, ExplicitBucketHistogramAggregation)
        assert isinstance(DELAY_BUCKETS, ExplicitBucketHistogramAggregation)

    def test_build_views_returns_one_view_per_pattern(self):
        views = build_views_for_patterns()

        assert len(views) == len(DEFAULT_PATTERN_BUCKETS)
        assert all(isinstance(v, View) for v in views)

        names = {v._instrument_name for v in views}
        assert names == set(DEFAULT_PATTERN_BUCKETS)

    def test_build_views_resolves_duration_pattern_to_latency_buckets(self):
        views = build_views_for_patterns()
        duration_view = next(v for v in views if v._instrument_name == "*_duration")

        assert duration_view._aggregation is LATENCY_BUCKETS

    def test_build_views_accepts_custom_pattern_mapping(self):
        custom = {"*_custom": ExplicitBucketHistogramAggregation(boundaries=(1, 2, 3))}

        views = build_views_for_patterns(custom)

        assert len(views) == 1
        assert views[0]._instrument_name == "*_custom"
        assert views[0]._aggregation is custom["*_custom"]

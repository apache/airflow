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

import logging
from unittest import mock

import pytest
from opentelemetry.metrics import MeterProvider
from pytest import param

from airflow.exceptions import InvalidStatsNameException
from airflow.metrics.otel_logger import (
    METRIC_NAME_PREFIX,
    OTEL_NAME_MAX_LENGTH,
    UP_DOWN_COUNTERS,
    SafeOtelLogger,
    _generate_key_name,
    _is_up_down_counter,
)
from airflow.metrics.validators import BACK_COMPAT_METRIC_NAMES, MetricNameLengthExemptionWarning

INVALID_STAT_NAME_CASES = [
    (None, "can not be None"),
    (42, "is not a string"),
    ("X" * OTEL_NAME_MAX_LENGTH, "too long"),
    ("test/$tats", "contains invalid characters"),
]


def full_name(name: str):
    return f"{METRIC_NAME_PREFIX}{name}"


@pytest.fixture
def name():
    return "test_stats_run"


class TestOtelMetrics:
    def setup_method(self):
        self.meter = mock.Mock(MeterProvider)
        self.stats = SafeOtelLogger(otel_provider=self.meter)
        self.map = self.stats.metrics_map.map
        self.logger = logging.getLogger(__name__)

    def test_is_up_down_counter_positive(self):
        udc = next(iter(UP_DOWN_COUNTERS))

        assert _is_up_down_counter(udc)

    def test_is_up_down_counter_negative(self):
        assert not _is_up_down_counter("this_is_not_a_udc")

    def test_exemption_list_has_not_grown(self):
        assert len(BACK_COMPAT_METRIC_NAMES) <= 23, (
            "This test exists solely to ensure that nobody is adding names to the exemption list. "
            "There are 23 names which are potentially too long for OTel and that number should "
            "only ever go down as these names are deprecated.  If this test is failing, please "
            "adjust your new stat's name; do not add as exemption without a very good reason."
        )

    @pytest.mark.parametrize(
        "invalid_stat_combo",
        [
            *[param(("prefix", name), id=f"Stat name {msg}.") for (name, msg) in INVALID_STAT_NAME_CASES],
            *[param((prefix, "name"), id=f"Stat prefix {msg}.") for (prefix, msg) in INVALID_STAT_NAME_CASES],
        ],
    )
    def test_invalid_stat_names_are_caught(self, invalid_stat_combo):
        prefix = invalid_stat_combo[0]
        name = invalid_stat_combo[1]
        self.stats.prefix = prefix

        with pytest.raises(InvalidStatsNameException):
            self.stats.incr(name)

        self.meter.assert_not_called()

    def test_old_name_exception_works(self, caplog):
        name = "task_instance_created-OperatorNameWhichIsSuperLongAndExceedsTheOpenTelemetryCharacterLimit"
        assert len(name) > OTEL_NAME_MAX_LENGTH

        with pytest.warns(MetricNameLengthExemptionWarning):
            self.stats.incr(name)

        self.meter.get_meter().create_counter.assert_called_once_with(
            name=(full_name(name)[:OTEL_NAME_MAX_LENGTH])
        )

    def test_incr_new_metric(self, name):
        self.stats.incr(name)

        self.meter.get_meter().create_counter.assert_called_once_with(name=full_name(name))

    def test_incr_new_metric_with_tags(self, name):
        tags = {"hello": "world"}
        key = _generate_key_name(full_name(name), tags)

        self.stats.incr(name, tags=tags)

        self.meter.get_meter().create_counter.assert_called_once_with(name=full_name(name))
        self.map[key].add.assert_called_once_with(1, attributes=tags)

    def test_incr_existing_metric(self, name):
        # Create the metric and set value to 1
        self.stats.incr(name)
        # Increment value to 2
        self.stats.incr(name)

        assert self.map[full_name(name)].add.call_count == 2

    @mock.patch("random.random", side_effect=[0.1, 0.9])
    def test_incr_with_rate_limit_works(self, mock_random, name):
        # Create the counter and set the value to 1
        self.stats.incr(name, rate=0.5)
        # This one should not increment because random() will return a value higher than `rate`
        self.stats.incr(name, rate=0.5)
        # This one should raise an exception for a negative `rate` value
        with pytest.raises(ValueError):
            self.stats.incr(name, rate=-0.5)

        assert mock_random.call_count == 2
        assert self.map[full_name(name)].add.call_count == 1

    def test_decr_existing_metric(self, name):
        expected_calls = [
            mock.call(1, attributes=None),
            mock.call(-1, attributes=None),
        ]
        # Create the metric and set value to 1
        self.stats.incr(name)

        # Decrement value to 0
        self.stats.decr(name)

        self.map[full_name(name)].add.assert_has_calls(expected_calls)
        assert self.map[full_name(name)].add.call_count == len(expected_calls)

    @mock.patch("random.random", side_effect=[0.1, 0.9])
    def test_decr_with_rate_limit_works(self, mock_random, name):
        expected_calls = [
            mock.call(1, attributes=None),
            mock.call(-1, attributes=None),
        ]
        # Create the metric and set value to 1
        self.stats.incr(name)

        # Decrement the counter to 0
        self.stats.decr(name, rate=0.5)
        # This one should not decrement because random() will return a value higher than `rate`
        self.stats.decr(name, rate=0.5)
        # This one should raise an exception for a negative `rate` value
        with pytest.raises(ValueError):
            self.stats.decr(name, rate=-0.5)

        assert mock_random.call_count == 2
        # add() is called once in the initial stats.incr and once for the decr that passed the rate check.
        self.map[full_name(name)].add.assert_has_calls(expected_calls)
        self.map[full_name(name)].add.call_count == 2

    @mock.patch("warnings.warn")
    def test_timer_warns_not_implemented(self, mock_warn):
        with self.stats.timer():
            mock_warn.assert_called_once_with("OpenTelemetry Timers are not yet implemented.")

    @mock.patch("warnings.warn")
    def test_gauge_warns_not_implemented(self, mock_warn):
        self.stats.gauge("test_gauge", 1)

        mock_warn.assert_called_once_with("OpenTelemetry Gauges are not yet implemented.")

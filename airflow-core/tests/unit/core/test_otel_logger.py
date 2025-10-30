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
import time
from unittest import mock

import pytest
from opentelemetry.metrics import MeterProvider

from airflow.exceptions import InvalidStatsNameException
from airflow.metrics.otel_logger import (
    OTEL_NAME_MAX_LENGTH,
    UP_DOWN_COUNTERS,
    MetricsMap,
    SafeOtelLogger,
    _generate_key_name,
    _is_up_down_counter,
    full_name,
)
from airflow.metrics.validators import BACK_COMPAT_METRIC_NAMES, MetricNameLengthExemptionWarning

INVALID_STAT_NAME_CASES = [
    (None, "can not be None"),
    (42, "is not a string"),
    ("X" * OTEL_NAME_MAX_LENGTH, "too long"),
    ("test/$tats", "contains invalid characters"),
]

RATE_MUST_BE_POSITIVE_MSG = "rate must be a positive value"


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
        assert len(BACK_COMPAT_METRIC_NAMES) <= 26, (
            "This test exists solely to ensure that nobody is adding names to the exemption list. "
            "There are 26 names which are potentially too long for OTel and that number should "
            "only ever go down as these names are deprecated.  If this test is failing, please "
            "adjust your new stat's name; do not add as exemption without a very good reason."
        )

    @pytest.mark.parametrize(
        "invalid_stat_combo",
        [
            *[
                pytest.param(("prefix", name), id=f"Stat name {msg}.")
                for (name, msg) in INVALID_STAT_NAME_CASES
            ],
            *[
                pytest.param((prefix, "name"), id=f"Stat prefix {msg}.")
                for (prefix, msg) in INVALID_STAT_NAME_CASES
            ],
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
        name = "task_instance_created_OperatorNameWhichIsSuperLongAndExceedsTheOpenTelemetryCharacterLimit/task_instance_created_OperatorNameWhichIsSuperLongAndExceedsTheOpenTelemetryCharacterLimit/task_instance_created_OperatorNameWhichIsSuperLongAndExceedsTheOpenTelemetryCharacterLimit"

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
        self.meter.get_meter().create_counter.assert_called_once_with(name=full_name(name))

    @mock.patch("random.random", side_effect=[0.1, 0.9])
    def test_incr_with_rate_limit_works(self, mock_random, name):
        # Create the counter and set the value to 1
        self.stats.incr(name, rate=0.5)
        # This one should not increment because random() will return a value higher than `rate`
        self.stats.incr(name, rate=0.5)
        # This one should raise an exception for a negative `rate` value
        with pytest.raises(ValueError, match=RATE_MUST_BE_POSITIVE_MSG):
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
        with pytest.raises(ValueError, match=RATE_MUST_BE_POSITIVE_MSG):
            self.stats.decr(name, rate=-0.5)

        assert mock_random.call_count == 2
        # add() is called once in the initial stats.incr and once for the decr that passed the rate check.
        self.map[full_name(name)].add.assert_has_calls(expected_calls)
        self.map[full_name(name)].add.call_count == 2

    def test_gauge_new_metric(self, name):
        self.stats.gauge(name, value=1)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        assert self.map[full_name(name)].value == 1

    def test_gauge_new_metric_with_tags(self, name):
        tags = {"hello": "world"}
        key = _generate_key_name(full_name(name), tags)

        self.stats.gauge(name, value=1, tags=tags)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        self.map[key].attributes == tags

    def test_gauge_existing_metric(self, name):
        self.stats.gauge(name, value=1)
        self.stats.gauge(name, value=2)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        assert self.map[full_name(name)].value == 2

    def test_gauge_existing_metric_with_delta(self, name):
        self.stats.gauge(name, value=1)
        self.stats.gauge(name, value=2, delta=True)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        assert self.map[full_name(name)].value == 3

    @mock.patch("random.random", side_effect=[0.1, 0.9])
    @mock.patch.object(MetricsMap, "set_gauge_value")
    def test_gauge_with_rate_limit_works(self, mock_set_value, mock_random, name):
        # Create the gauge and set the value to 1
        self.stats.gauge(name, value=1, rate=0.5)
        # This one should not increment because random() will return a value higher than `rate`
        self.stats.gauge(name, value=1, rate=0.5)

        with pytest.raises(ValueError, match=RATE_MUST_BE_POSITIVE_MSG):
            self.stats.gauge(name, value=1, rate=-0.5)

        assert mock_random.call_count == 2
        assert mock_set_value.call_count == 1

    def test_gauge_value_is_correct(self, name):
        self.stats.gauge(name, value=1)

        assert self.map[full_name(name)].value == 1

    def test_timing_new_metric(self, name):
        import datetime

        self.stats.timing(name, dt=datetime.timedelta(seconds=123))

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        expected_value = 123000.0
        assert self.map[full_name(name)].value == expected_value

    def test_timing_new_metric_with_tags(self, name):
        tags = {"hello": "world"}
        key = _generate_key_name(full_name(name), tags)

        self.stats.timing(name, dt=1, tags=tags)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        self.map[key].attributes == tags

    def test_timing_existing_metric(self, name):
        self.stats.timing(name, dt=1)
        self.stats.timing(name, dt=2)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        assert self.map[full_name(name)].value == 2

    # For the four test_timer_foo tests below:
    #   time.perf_count() is called once to get the starting timestamp and again
    #   to get the end timestamp.  timer() should return the difference as a float.

    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_with_name_returns_float_and_stores_value(self, mock_time, name):
        with self.stats.timer(name) as timer:
            pass

        assert isinstance(timer.duration, float)
        expected_duration = 3140.0
        assert timer.duration == expected_duration
        assert mock_time.call_count == 2
        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))

    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_no_name_returns_float_but_does_not_store_value(self, mock_time, name):
        with self.stats.timer() as timer:
            pass

        assert isinstance(timer.duration, float)
        expected_duration = 3140.0
        assert timer.duration == expected_duration
        assert mock_time.call_count == 2
        self.meter.get_meter().create_gauge.assert_not_called()

    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_start_and_stop_manually_send_false(self, mock_time, name):
        timer = self.stats.timer(name)
        timer.start()
        # Perform some task
        timer.stop(send=False)

        assert isinstance(timer.duration, float)
        expected_value = 3140.0
        assert timer.duration == expected_value
        assert mock_time.call_count == 2
        self.meter.get_meter().create_gauge.assert_not_called()

    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_start_and_stop_manually_send_true(self, mock_time, name):
        timer = self.stats.timer(name)
        timer.start()
        # Perform some task
        timer.stop(send=True)

        assert isinstance(timer.duration, float)
        expected_value = 3140.0
        assert timer.duration == expected_value
        assert mock_time.call_count == 2
        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))

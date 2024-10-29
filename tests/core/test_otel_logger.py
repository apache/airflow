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
from unittest.mock import ANY

import pytest
from opentelemetry.metrics import MeterProvider

from airflow.exceptions import InvalidStatsNameException
from airflow.metrics import otel_logger, protocols
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
        name = "task_instance_created_OperatorNameWhichIsSuperLongAndExceedsTheOpenTelemetryCharacterLimit"
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

    def test_gauge_new_metric(self, name):
        self.stats.gauge(name, value=1)

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        assert self.map[full_name(name)].value == 1

    def test_gauge_new_metric_with_tags(self, name):
        tags = {"hello": "world"}
        key = _generate_key_name(full_name(name), tags)

        self.stats.gauge(name, value=1, tags=tags)

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        self.map[key].attributes == tags

    def test_gauge_existing_metric(self, name):
        self.stats.gauge(name, value=1)
        self.stats.gauge(name, value=2)

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        assert self.map[full_name(name)].value == 2

    def test_gauge_existing_metric_with_delta(self, name):
        self.stats.gauge(name, value=1)
        self.stats.gauge(name, value=2, delta=True)

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        assert self.map[full_name(name)].value == 3

    @mock.patch("random.random", side_effect=[0.1, 0.9])
    @mock.patch.object(MetricsMap, "set_gauge_value")
    def test_gauge_with_rate_limit_works(self, mock_set_value, mock_random, name):
        # Create the gauge and set the value to 1
        self.stats.gauge(name, value=1, rate=0.5)
        # This one should not increment because random() will return a value higher than `rate`
        self.stats.gauge(name, value=1, rate=0.5)

        with pytest.raises(ValueError):
            self.stats.gauge(name, value=1, rate=-0.5)

        assert mock_random.call_count == 2
        assert mock_set_value.call_count == 1

    def test_gauge_value_is_correct(self, name):
        self.stats.gauge(name, value=1)

        assert self.map[full_name(name)].value == 1

    @pytest.mark.parametrize(
        "metrics_consistency_on",
        [True, False],
    )
    def test_timing_new_metric(self, metrics_consistency_on, name):
        import datetime

        otel_logger.metrics_consistency_on = metrics_consistency_on

        self.stats.timing(name, dt=datetime.timedelta(seconds=123))

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        expected_value = 123000.0 if metrics_consistency_on else 123
        assert self.map[full_name(name)].value == expected_value

    def test_timing_new_metric_with_tags(self, name):
        tags = {"hello": "world"}
        key = _generate_key_name(full_name(name), tags)

        self.stats.timing(name, dt=1, tags=tags)

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        self.map[key].attributes == tags

    def test_timing_existing_metric(self, name):
        self.stats.timing(name, dt=1)
        self.stats.timing(name, dt=2)

        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )
        assert self.map[full_name(name)].value == 2

    # For the four test_timer_foo tests below:
    #   time.perf_count() is called once to get the starting timestamp and again
    #   to get the end timestamp.  timer() should return the difference as a float.

    @pytest.mark.parametrize(
        "metrics_consistency_on",
        [True, False],
    )
    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_with_name_returns_float_and_stores_value(self, mock_time, metrics_consistency_on, name):
        protocols.metrics_consistency_on = metrics_consistency_on
        with self.stats.timer(name) as timer:
            pass

        assert isinstance(timer.duration, float)
        expected_duration = 3140.0 if metrics_consistency_on else 3.14
        assert timer.duration == expected_duration
        assert mock_time.call_count == 2
        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )

    @pytest.mark.parametrize(
        "metrics_consistency_on",
        [True, False],
    )
    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_no_name_returns_float_but_does_not_store_value(
        self, mock_time, metrics_consistency_on, name
    ):
        protocols.metrics_consistency_on = metrics_consistency_on
        with self.stats.timer() as timer:
            pass

        assert isinstance(timer.duration, float)
        expected_duration = 3140.0 if metrics_consistency_on else 3.14
        assert timer.duration == expected_duration
        assert mock_time.call_count == 2
        self.meter.get_meter().create_observable_gauge.assert_not_called()

    @pytest.mark.parametrize(
        "metrics_consistency_on",
        [
            True,
            False,
        ],
    )
    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_start_and_stop_manually_send_false(self, mock_time, metrics_consistency_on, name):
        protocols.metrics_consistency_on = metrics_consistency_on

        timer = self.stats.timer(name)
        timer.start()
        # Perform some task
        timer.stop(send=False)

        assert isinstance(timer.duration, float)
        expected_value = 3140.0 if metrics_consistency_on else 3.14
        assert timer.duration == expected_value
        assert mock_time.call_count == 2
        self.meter.get_meter().create_observable_gauge.assert_not_called()

    @pytest.mark.parametrize(
        "metrics_consistency_on",
        [
            True,
            False,
        ],
    )
    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_start_and_stop_manually_send_true(self, mock_time, metrics_consistency_on, name):
        protocols.metrics_consistency_on = metrics_consistency_on
        timer = self.stats.timer(name)
        timer.start()
        # Perform some task
        timer.stop(send=True)

        assert isinstance(timer.duration, float)
        expected_value = 3140.0 if metrics_consistency_on else 3.14
        assert timer.duration == expected_value
        assert mock_time.call_count == 2
        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(name), callbacks=ANY
        )

    def test_incr_counter(self):
        metric_name = "test_metric"
        count = 5
        tags = {"env": "prod"}

        self.stats.incr(metric_name, count=count, tags=tags)

        counter = self.stats.metrics_map.get_counter(
            full_name(prefix=self.stats.prefix, name=metric_name), attributes=tags
        )
        counter.add.assert_called_with(count, attributes=tags)

    def test_decr_counter(self):
        metric_name = "test_metric"
        count = 3
        tags = {"env": "prod"}

        self.stats.decr(metric_name, count=count, tags=tags)

        counter = self.stats.metrics_map.get_counter(
            full_name(prefix=self.stats.prefix, name=metric_name), attributes=tags
        )
        counter.add.assert_called_with(-count, attributes=tags)

    @pytest.mark.parametrize("expected_duration", [2.5, 1.0, 3.0])
    def test_timing(self, expected_duration):
        metric_name = "test_metric"
        tags = {"env": "prod"}

        # Mocking time.perf_counter to simulate timing
        with mock.patch.object(time, "perf_counter", side_effect=[0.0, expected_duration]):
            with self.stats.timer(metric_name, tags=tags) as timer:
                pass

        acceptable_margin = 0.1
        assert isinstance(timer.duration, float)
        assert timer.duration >= expected_duration - acceptable_margin
        assert timer.duration <= expected_duration + acceptable_margin
        self.meter.get_meter().create_observable_gauge.assert_called_once_with(
            name=full_name(prefix=self.stats.prefix, name=metric_name), callbacks=ANY
        )

    @pytest.mark.parametrize(
        "metric_name, initial_value, delta, expected_value, tags",
        [
            ("test_metric", 42, False, 42, {"env": "prod"}),
            ("test_metric", 10, True, 10, {"env": "prod"}),
            ("test_metric", 5, True, 0, {"env": "prod"}),
        ],
    )
    def test_gauge_operations(self, metric_name, initial_value, delta, expected_value, tags):
        if delta and expected_value == 0:
            self.stats.gauge(metric_name, initial_value, delta=True, tags=tags)
            current_value_after_increment = self.stats.metrics_map.poke_gauge(
                full_name(prefix=self.stats.prefix, name=metric_name), tags
            )
            assert current_value_after_increment == initial_value

            self.stats.gauge(metric_name, -initial_value, delta=True, tags=tags)
        else:
            self.stats.gauge(metric_name, initial_value, delta=delta, tags=tags)

        current_value = self.stats.metrics_map.poke_gauge(
            full_name(prefix=self.stats.prefix, name=metric_name), tags
        )
        assert current_value == expected_value

    def test_get_name_invalid_cases(self):
        invalid_name = "invalid/metric/name"

        # Expect the method to raise InvalidStatsNameException for invalid characters
        with pytest.raises(InvalidStatsNameException):
            self.stats.get_name(invalid_name)

    def test_get_name_too_long(self):
        # Edge case: Name exceeds max length
        long_name = "a" * (OTEL_NAME_MAX_LENGTH + 1)
        with pytest.raises(InvalidStatsNameException, match="Invalid stat name.*Please see"):
            self.stats.get_name(long_name)

    def test_get_name_special_characters(self):
        # Edge case: Name contains invalid special characters
        invalid_name = "invalid@name!"
        with pytest.raises(
            InvalidStatsNameException, match="composed of ASCII alphabets, numbers, or the underscore"
        ):
            self.stats.get_name(invalid_name)

    def test_get_name_empty_string(self):
        # Edge case: Empty string as name
        empty_name = ""
        with pytest.raises(
            InvalidStatsNameException, match="The stat name cannot be None or an empty string."
        ):
            self.stats.get_name(empty_name)

    def test_get_name_empty_string_no_tags(self):
        # Edge case: Empty string as name, no tags provided
        empty_name = ""
        with pytest.raises(
            InvalidStatsNameException, match="The stat name cannot be None or an empty string."
        ):
            self.stats.get_name(empty_name)

    def test_get_name_empty_string_with_tags(self):
        # Edge case: Empty string as name, with tags provided
        empty_name = ""
        tags = {"key": "value"}
        with pytest.raises(
            InvalidStatsNameException, match="The stat name cannot be None or an empty string."
        ):
            self.stats.get_name(empty_name, tags=tags)

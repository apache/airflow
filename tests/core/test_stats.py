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
from __future__ import annotations

import importlib
from unittest import mock
from unittest.mock import Mock

import pytest

import airflow
from airflow.exceptions import InvalidStatsNameException
from airflow.metrics.datadog_logger import SafeDogStatsdLogger
from airflow.metrics.validators import (
    AllowListValidator,
    BlockListValidator,
)
from tests.test_utils.config import conf_vars


class TestDogStats:
    def setup_method(self):
        pytest.importorskip("datadog")
        from datadog import DogStatsd

        self.dogstatsd_client = Mock(spec=DogStatsd)
        self.dogstatsd = SafeDogStatsdLogger(self.dogstatsd_client)

    def test_increment_counter_with_valid_name_with_dogstatsd(self):
        self.dogstatsd.incr("test_stats_run")
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="test_stats_run", sample_rate=1, tags=[], value=1
        )

    def test_stat_name_must_be_a_string_with_dogstatsd(self):
        self.dogstatsd.incr([])
        self.dogstatsd_client.assert_not_called()

    def test_stat_name_must_not_exceed_max_length_with_dogstatsd(self):
        self.dogstatsd.incr("X" * 300)
        self.dogstatsd_client.assert_not_called()

    def test_stat_name_must_only_include_allowed_characters_with_dogstatsd(self):
        self.dogstatsd.incr("test/$tats")
        self.dogstatsd_client.assert_not_called()

    def test_does_send_stats_using_dogstatsd_when_dogstatsd_on(self):
        self.dogstatsd.incr("empty_key")
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=[], value=1
        )

    def test_does_send_stats_using_dogstatsd_with_tags_without_enabled_metrics_tags(self):
        self.dogstatsd.incr("empty_key", 1, 1, tags={"key1": "value1", "key2": "value2"})
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=[], value=1
        )

    def test_does_send_stats_using_dogstatsd_when_statsd_and_dogstatsd_both_on(self):
        # ToDo: Figure out why it identical to test_does_send_stats_using_dogstatsd_when_dogstatsd_on
        self.dogstatsd.incr("empty_key")
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=[], value=1
        )

    def test_timer(self):
        with self.dogstatsd.timer("empty_timer"):
            pass
        self.dogstatsd_client.timed.assert_called_once_with("empty_timer", tags=[])

    def test_empty_timer(self):
        with self.dogstatsd.timer():
            pass
        self.dogstatsd_client.timed.assert_not_called()

    def test_timing(self):
        import datetime

        self.dogstatsd.timing("empty_timer", 123)
        self.dogstatsd_client.timing.assert_called_once_with(metric="empty_timer", value=123, tags=[])

        self.dogstatsd.timing("empty_timer", datetime.timedelta(seconds=123))
        self.dogstatsd_client.timing.assert_called_with(metric="empty_timer", value=123.0, tags=[])

    def test_gauge(self):
        self.dogstatsd.gauge("empty", 123)
        self.dogstatsd_client.gauge.assert_called_once_with(metric="empty", sample_rate=1, value=123, tags=[])

    def test_decr(self):
        self.dogstatsd.decr("empty")
        self.dogstatsd_client.decrement.assert_called_once_with(
            metric="empty", sample_rate=1, value=1, tags=[]
        )

    def test_enabled_by_config(self):
        """Test that enabling this sets the right instance properties"""
        from datadog import DogStatsd

        with conf_vars(
            {("metrics", "statsd_datadog_enabled"): "True", ("metrics", "metrics_use_pattern_match"): "True"}
        ):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.stats.Stats, "statsd")
        # Avoid side-effects
        importlib.reload(airflow.stats)

    def test_does_not_send_stats_using_statsd_when_statsd_and_dogstatsd_both_on(self):
        from datadog import DogStatsd

        with conf_vars(
            {
                ("metrics", "statsd_datadog_enabled"): "True",
                ("metrics", "metrics_use_pattern_match"): "True",
            }
        ):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.stats.Stats, "statsd")
        importlib.reload(airflow.stats)


class TestDogStatsWithAllowList:
    def setup_method(self):
        pytest.importorskip("datadog")
        from datadog import DogStatsd

        self.dogstatsd_client = Mock(speck=DogStatsd)
        self.dogstats = SafeDogStatsdLogger(self.dogstatsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.dogstats.incr("stats_one")
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="stats_one", sample_rate=1, tags=[], value=1
        )

    def test_increment_counter_with_allowed_prefix(self):
        self.dogstats.incr("stats_two.bla")
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="stats_two.bla", sample_rate=1, tags=[], value=1
        )

    def test_not_increment_counter_if_not_allowed(self):
        self.dogstats.incr("stats_three")
        self.dogstatsd_client.assert_not_called()


class TestDogStatsWithMetricsTags:
    def setup_method(self):
        pytest.importorskip("datadog")
        from datadog import DogStatsd

        self.dogstatsd_client = Mock(speck=DogStatsd)
        self.dogstatsd = SafeDogStatsdLogger(self.dogstatsd_client, metrics_tags=True)

    def test_does_send_stats_using_dogstatsd_with_tags(self):
        self.dogstatsd.incr("empty_key", 1, 1, tags={"key1": "value1", "key2": "value2"})
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=["key1:value1", "key2:value2"], value=1
        )


class TestDogStatsWithDisabledMetricsTags:
    def setup_method(self):
        pytest.importorskip("datadog")
        from datadog import DogStatsd

        self.dogstatsd_client = Mock(speck=DogStatsd)
        self.dogstatsd = SafeDogStatsdLogger(
            self.dogstatsd_client,
            metrics_tags=True,
            metric_tags_validator=BlockListValidator("key1"),
        )

    def test_does_send_stats_using_dogstatsd_with_tags(self):
        self.dogstatsd.incr("empty_key", 1, 1, tags={"key1": "value1", "key2": "value2"})
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=["key2:value2"], value=1
        )


def always_invalid(stat_name):
    raise InvalidStatsNameException(f"Invalid name: {stat_name}")


def always_valid(stat_name):
    return stat_name


class TestCustomStatsName:
    @conf_vars(
        {
            ("metrics", "statsd_datadog_enabled"): "True",
            ("metrics", "metrics_use_pattern_match"): "True",
            ("metrics", "stat_name_handler"): "tests.core.test_stats.always_invalid",
        }
    )
    @mock.patch("datadog.DogStatsd")
    def test_does_not_send_stats_using_dogstatsd_when_the_name_is_not_valid(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("empty_key")
        mock_dogstatsd.return_value.assert_not_called()

    @conf_vars(
        {
            ("metrics", "statsd_datadog_enabled"): "True",
            ("metrics", "metrics_use_pattern_match"): "True",
            ("metrics", "stat_name_handler"): "tests.core.test_stats.always_valid",
        }
    )
    @mock.patch("datadog.DogStatsd")
    def test_does_send_stats_using_dogstatsd_when_the_name_is_valid(self, mock_dogstatsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("empty_key")
        mock_dogstatsd.return_value.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=[], value=1
        )

    def teardown_method(self) -> None:
        # To avoid side-effect
        importlib.reload(airflow.stats)

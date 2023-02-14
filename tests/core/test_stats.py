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
import re
from unittest import mock
from unittest.mock import Mock

import pytest
import statsd

import airflow
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.stats import AllowListValidator, SafeDogStatsdLogger, SafeStatsdLogger
from tests.test_utils.config import conf_vars


class CustomStatsd(statsd.StatsClient):
    pass


class InvalidCustomStatsd:
    """
    This custom Statsd class is invalid because it does not subclass
    statsd.StatsClient.
    """

    def __init__(self, host=None, port=None, prefix=None):
        pass


class TestStats:
    def setup_method(self):
        self.statsd_client = Mock(spec=statsd.StatsClient)
        self.stats = SafeStatsdLogger(self.statsd_client)

    def test_increment_counter_with_valid_name(self):
        self.stats.incr("test_stats_run")
        self.statsd_client.incr.assert_called_once_with("test_stats_run", 1, 1)

    def test_stat_name_must_be_a_string(self):
        self.stats.incr([])
        self.statsd_client.assert_not_called()

    def test_stat_name_must_not_exceed_max_length(self):
        self.stats.incr("X" * 300)
        self.statsd_client.assert_not_called()

    def test_stat_name_must_only_include_allowed_characters(self):
        self.stats.incr("test/$tats")
        self.statsd_client.assert_not_called()

    def test_timer(self):
        with self.stats.timer("empty_timer"):
            pass
        self.statsd_client.timer.assert_called_once_with("empty_timer")

    def test_empty_timer(self):
        with self.stats.timer():
            pass
        self.statsd_client.timer.assert_not_called()

    def test_timing(self):
        self.stats.timing("empty_timer", 123)
        self.statsd_client.timing.assert_called_once_with("empty_timer", 123)

    def test_gauge(self):
        self.stats.gauge("empty", 123)
        self.statsd_client.gauge.assert_called_once_with("empty", 123, 1, False)

    def test_decr(self):
        self.stats.decr("empty")
        self.statsd_client.decr.assert_called_once_with("empty", 1, 1)

    def test_enabled_by_config(self):
        """Test that enabling this sets the right instance properties"""
        with conf_vars({("metrics", "statsd_on"): "True"}):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.statsd, statsd.StatsClient)
            assert not hasattr(airflow.stats.Stats, "dogstatsd")
        # Avoid side-effects
        importlib.reload(airflow.stats)

    def test_load_custom_statsd_client(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "statsd_custom_client_path"): f"{__name__}.CustomStatsd",
            }
        ):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.statsd, CustomStatsd)
        # Avoid side-effects
        importlib.reload(airflow.stats)

    def test_load_invalid_custom_stats_client(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "statsd_custom_client_path"): f"{__name__}.InvalidCustomStatsd",
            }
        ), pytest.raises(
            AirflowConfigException,
            match=re.escape(
                "Your custom StatsD client must extend the statsd."
                "StatsClient in order to ensure backwards compatibility."
            ),
        ):
            importlib.reload(airflow.stats)
            airflow.stats.Stats.incr("empty_key")
        importlib.reload(airflow.stats)


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

        with conf_vars({("metrics", "statsd_datadog_enabled"): "True"}):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.stats.Stats, "statsd")
        # Avoid side-effects
        importlib.reload(airflow.stats)

    def test_does_not_send_stats_using_statsd_when_statsd_and_dogstatsd_both_on(self):
        from datadog import DogStatsd

        with conf_vars({("metrics", "statsd_on"): "True", ("metrics", "statsd_datadog_enabled"): "True"}):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.stats.Stats, "statsd")
        importlib.reload(airflow.stats)


class TestStatsWithAllowList:
    def setup_method(self):
        self.statsd_client = Mock(spec=statsd.StatsClient)
        self.stats = SafeStatsdLogger(self.statsd_client, AllowListValidator("stats_one, stats_two"))

    def test_increment_counter_with_allowed_key(self):
        self.stats.incr("stats_one")
        self.statsd_client.incr.assert_called_once_with("stats_one", 1, 1)

    def test_increment_counter_with_allowed_prefix(self):
        self.stats.incr("stats_two.bla")
        self.statsd_client.incr.assert_called_once_with("stats_two.bla", 1, 1)

    def test_not_increment_counter_if_not_allowed(self):
        self.stats.incr("stats_three")
        self.statsd_client.assert_not_called()


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


class TestStatsWithInfluxDBEnabled:
    def setup_method(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "statsd_influxdb_enabled"): "True",
            }
        ):
            self.statsd_client = Mock(spec=statsd.StatsClient)
            self.stats = SafeStatsdLogger(self.statsd_client, influxdb_tags_enabled=True)

    def test_increment_counter(self):
        self.stats.incr(
            "test_stats_run.delay",
        )
        self.statsd_client.incr.assert_called_once_with("test_stats_run.delay", 1, 1)

    def test_increment_counter_with_tags(self):
        self.stats.incr(
            "test_stats_run.delay",
            tags={"key0": "val0", "key1": "val1", "key2": "val2"},
        )
        self.statsd_client.incr.assert_called_once_with(
            "test_stats_run.delay,key0=val0,key1=val1,key2=val2", 1, 1
        )

    def test_does_not_increment_counter_drops_invalid_tags(self):
        self.stats.incr(
            "test_stats_run.delay",
            tags={"key0,": "val0", "key1": "val1", "key2": "val2"},
        )
        self.statsd_client.incr.assert_called_once_with("test_stats_run.delay,key1=val1,key2=val2", 1, 1)


def always_invalid(stat_name):
    raise InvalidStatsNameException(f"Invalid name: {stat_name}")


def always_valid(stat_name):
    return stat_name


class TestCustomStatsName:
    @conf_vars(
        {
            ("metrics", "statsd_on"): "True",
            ("metrics", "stat_name_handler"): "tests.core.test_stats.always_invalid",
        }
    )
    @mock.patch("statsd.StatsClient")
    def test_does_not_send_stats_using_statsd_when_the_name_is_not_valid(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("empty_key")
        mock_statsd.return_value.assert_not_called()

    @conf_vars(
        {
            ("metrics", "statsd_datadog_enabled"): "True",
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
            ("metrics", "statsd_on"): "True",
            ("metrics", "stat_name_handler"): "tests.core.test_stats.always_valid",
        }
    )
    @mock.patch("statsd.StatsClient")
    def test_does_send_stats_using_statsd_when_the_name_is_valid(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("empty_key")
        mock_statsd.return_value.incr.assert_called_once_with("empty_key", 1, 1)

    @conf_vars(
        {
            ("metrics", "statsd_datadog_enabled"): "True",
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

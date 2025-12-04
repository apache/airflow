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
import logging
import re
import time
from unittest import mock
from unittest.mock import Mock

import pytest
import statsd

import airflow
from airflow.exceptions import AirflowConfigException, InvalidStatsNameException
from airflow.metrics.datadog_logger import SafeDogStatsdLogger
from airflow.metrics.statsd_logger import SafeStatsdLogger
from airflow.metrics.validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker


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
        with self.stats.timer("empty_timer") as t:
            pass
        self.statsd_client.timer.assert_called_once_with("empty_timer")
        assert isinstance(t.duration, float)

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
        ):
            importlib.reload(airflow.stats)
            error_message = re.escape(
                "Your custom StatsD client must extend the statsd."
                "StatsClient in order to ensure backwards compatibility."
            )
            with pytest.raises(AirflowConfigException, match=error_message):
                airflow.stats.Stats.incr("empty_key")
        importlib.reload(airflow.stats)

    def test_load_allow_list_validator(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "metrics_allow_list"): "name1,name2",
            }
        ):
            importlib.reload(airflow.stats)
            assert type(airflow.stats.Stats.metrics_validator) is PatternAllowListValidator
            assert airflow.stats.Stats.metrics_validator.validate_list == ("name1", "name2")
        # Avoid side-effects
        importlib.reload(airflow.stats)

    def test_load_block_list_validator(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "metrics_block_list"): "name1,name2",
            }
        ):
            importlib.reload(airflow.stats)
            assert type(airflow.stats.Stats.metrics_validator) is PatternBlockListValidator
            assert airflow.stats.Stats.metrics_validator.validate_list == ("name1", "name2")
        # Avoid side-effects
        importlib.reload(airflow.stats)

    def test_load_allow_and_block_list_validator_loads_only_allow_list_validator(self):
        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "metrics_allow_list"): "name1,name2",
                ("metrics", "metrics_block_list"): "name1,name2",
            }
        ):
            importlib.reload(airflow.stats)
            assert type(airflow.stats.Stats.metrics_validator) is PatternAllowListValidator
            assert airflow.stats.Stats.metrics_validator.validate_list == ("name1", "name2")
        # Avoid side-effects
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
        """Test that dogstatsd works when both statsd and dogstatsd are enabled (dogstatsd takes precedence)."""
        self.dogstatsd.incr("empty_key")
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=[], value=1
        )

    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 100.0])
    def test_timer(self, time_mock):
        with self.dogstatsd.timer("empty_timer") as timer:
            pass
        self.dogstatsd_client.timed.assert_called_once_with("empty_timer", tags=[])
        expected_duration = 1000.0 * 100.0
        assert expected_duration == timer.duration
        assert time_mock.call_count == 2

    def test_empty_timer(self):
        with self.dogstatsd.timer():
            pass
        self.dogstatsd_client.timed.assert_not_called()

    def test_timing(self):
        import datetime

        self.dogstatsd.timing("empty_timer", 123)
        self.dogstatsd_client.timing.assert_called_once_with(metric="empty_timer", value=123, tags=[])

        self.dogstatsd.timing("empty_timer", datetime.timedelta(seconds=123))
        self.dogstatsd_client.timing.assert_called_with(metric="empty_timer", value=123000.0, tags=[])

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

        with conf_vars(
            {
                ("metrics", "statsd_on"): "True",
                ("metrics", "statsd_datadog_enabled"): "True",
            }
        ):
            importlib.reload(airflow.stats)
            assert isinstance(airflow.stats.Stats.dogstatsd, DogStatsd)
            assert not hasattr(airflow.stats.Stats, "statsd")
        importlib.reload(airflow.stats)


class TestStatsAllowAndBlockLists:
    @pytest.mark.parametrize(
        ("validator", "stat_name", "expect_incr"),
        [
            (PatternAllowListValidator, "stats_one", True),
            (PatternAllowListValidator, "stats_two.bla", True),
            (PatternAllowListValidator, "stats_three.foo", True),
            (PatternAllowListValidator, "stats_foo_three", True),
            (PatternAllowListValidator, "stats_three", False),
            (PatternBlockListValidator, "stats_one", False),
            (PatternBlockListValidator, "stats_two.bla", False),
            (PatternBlockListValidator, "stats_three.foo", False),
            (PatternBlockListValidator, "stats_foo_three", False),
            (PatternBlockListValidator, "stats_foo", False),
            (PatternBlockListValidator, "stats_three", True),
        ],
    )
    def test_allow_and_block_list(self, validator, stat_name, expect_incr):
        statsd_client = Mock(spec=statsd.StatsClient)
        stats = SafeStatsdLogger(statsd_client, validator("stats_one, stats_two, foo"))

        stats.incr(stat_name)

        if expect_incr:
            statsd_client.incr.assert_called_once_with(stat_name, 1, 1)
        else:
            statsd_client.assert_not_called()

    @pytest.mark.parametrize(
        ("match_pattern", "expect_incr"),
        [
            ("^stat", True),  # Match: Regex Startswith
            ("a.{4}o", True),  # Match: RegEx Pattern
            ("foo", True),  # Match: Any substring
            ("stat", True),  # Match: Substring Startswith
            ("^banana", False),  # No match
        ],
    )
    def test_regex_matches(self, match_pattern, expect_incr):
        stat_name = "stats_foo_one"
        validator = PatternAllowListValidator

        statsd_client = Mock(spec=statsd.StatsClient)
        stats = SafeStatsdLogger(statsd_client, validator(match_pattern))

        stats.incr(stat_name)

        if expect_incr:
            statsd_client.incr.assert_called_once_with(stat_name, 1, 1)
        else:
            statsd_client.assert_not_called()


class TestPatternValidatorConfigOption:
    def teardown_method(self):
        # Avoid side-effects
        importlib.reload(airflow.stats)

    stats_on = {("metrics", "statsd_on"): "True"}
    allow_list = {("metrics", "metrics_allow_list"): "foo,bar"}
    block_list = {("metrics", "metrics_block_list"): "foo,bar"}

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            pytest.param(
                {**stats_on},
                PatternAllowListValidator,
                id="pattern_allow_by_default",
            ),
            pytest.param(
                {**stats_on, **allow_list},
                PatternAllowListValidator,
                id="pattern_allow_list_provided",
            ),
            pytest.param(
                {**stats_on, **block_list},
                PatternBlockListValidator,
                id="pattern_block_list_provided",
            ),
            pytest.param(
                {**stats_on, **allow_list, **block_list},
                PatternAllowListValidator,
                id="pattern_block_list_provided",
            ),
        ],
    )
    def test_pattern_picker(self, config, expected):
        with conf_vars(config):
            importlib.reload(airflow.stats)

            assert isinstance(airflow.stats.Stats.statsd, statsd.StatsClient)
            assert type(airflow.stats.Stats.instance.metrics_validator) is expected

    @conf_vars({**stats_on, **block_list, ("metrics", "metrics_allow_list"): "baz,qux"})
    def test_setting_allow_and_block_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            importlib.reload(airflow.stats)

            assert isinstance(airflow.stats.Stats.statsd, statsd.StatsClient)
            assert type(airflow.stats.Stats.instance.metrics_validator) is PatternAllowListValidator
            assert "Ignoring metrics_block_list" in caplog.text


class TestDogStatsWithAllowList:
    def setup_method(self):
        pytest.importorskip("datadog")
        from datadog import DogStatsd

        self.dogstatsd_client = Mock(spec=DogStatsd)
        self.dogstats = SafeDogStatsdLogger(
            self.dogstatsd_client, PatternAllowListValidator("stats_one, stats_two")
        )

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

        self.dogstatsd_client = Mock(spec=DogStatsd)
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

        self.dogstatsd_client = Mock(spec=DogStatsd)
        self.dogstatsd = SafeDogStatsdLogger(
            self.dogstatsd_client,
            metrics_tags=True,
            metric_tags_validator=PatternBlockListValidator("key1"),
        )

    def test_does_send_stats_using_dogstatsd_with_tags(self):
        self.dogstatsd.incr("empty_key", 1, 1, tags={"key1": "value1", "key2": "value2"})
        self.dogstatsd_client.increment.assert_called_once_with(
            metric="empty_key", sample_rate=1, tags=["key2:value2"], value=1
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
            self.stats = SafeStatsdLogger(
                self.statsd_client,
                influxdb_tags_enabled=True,
                metric_tags_validator=PatternBlockListValidator("key2,key3"),
            )

    def test_increment_counter(self):
        self.stats.incr(
            "test_stats_run.delay",
        )
        self.statsd_client.incr.assert_called_once_with("test_stats_run.delay", 1, 1)

    def test_increment_counter_with_tags(self):
        self.stats.incr(
            "test_stats_run.delay",
            tags={"key0": 0, "key1": "val1", "key2": "val2"},
        )
        self.statsd_client.incr.assert_called_once_with("test_stats_run.delay,key0=0,key1=val1", 1, 1)

    def test_increment_counter_with_tags_and_forward_slash(self):
        self.stats.incr("test_stats_run.dag", tags={"path": "/some/path/dag.py"})
        self.statsd_client.incr.assert_called_once_with("test_stats_run.dag,path=/some/path/dag.py", 1, 1)

    def test_does_not_increment_counter_drops_invalid_tags(self):
        self.stats.incr(
            "test_stats_run.delay",
            tags={"key0,": "val0", "key1": "val1", "key2": "val2", "key3": "val3"},
        )
        self.statsd_client.incr.assert_called_once_with("test_stats_run.delay,key1=val1", 1, 1)


def always_invalid(stat_name):
    raise InvalidStatsNameException(f"Invalid name: {stat_name}")


def always_valid(stat_name):
    return stat_name


class TestCustomStatsName:
    @conf_vars(
        {
            ("metrics", "statsd_on"): "True",
            ("metrics", "stat_name_handler"): "unit.core.test_stats.always_invalid",
        }
    )
    @mock.patch("statsd.StatsClient")
    def test_does_not_send_stats_using_statsd_when_the_name_is_not_valid(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("empty_key")
        mock_statsd.return_value.assert_not_called()

    @skip_if_force_lowest_dependencies_marker
    @conf_vars(
        {
            ("metrics", "statsd_datadog_enabled"): "True",
            ("metrics", "stat_name_handler"): "unit.core.test_stats.always_invalid",
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
            ("metrics", "stat_name_handler"): "unit.core.test_stats.always_valid",
        }
    )
    @mock.patch("statsd.StatsClient")
    def test_does_send_stats_using_statsd_when_the_name_is_valid(self, mock_statsd):
        importlib.reload(airflow.stats)
        airflow.stats.Stats.incr("empty_key")
        mock_statsd.return_value.incr.assert_called_once_with("empty_key", 1, 1)

    @skip_if_force_lowest_dependencies_marker
    @conf_vars(
        {
            ("metrics", "statsd_datadog_enabled"): "True",
            ("metrics", "stat_name_handler"): "unit.core.test_stats.always_valid",
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

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
import time
from collections.abc import Callable
from unittest import mock
from unittest.mock import Mock

import pytest
import statsd

import airflow_shared
import airflow_shared.observability.metrics.stats
import airflow_shared.observability.metrics.validators
from airflow_shared.observability.exceptions import InvalidStatsNameException
from airflow_shared.observability.metrics import datadog_logger, statsd_logger
from airflow_shared.observability.metrics.base_stats_logger import StatsLogger
from airflow_shared.observability.metrics.datadog_logger import SafeDogStatsdLogger
from airflow_shared.observability.metrics.statsd_logger import SafeStatsdLogger
from airflow_shared.observability.metrics.validators import (
    PatternAllowListValidator,
    PatternBlockListValidator,
)

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker


class CustomStatsd(statsd.StatsClient):
    pass


def get_statsd_logger_factory(
    stats_class,
    metrics_allow_list: str | None = None,
    metrics_block_list: str | None = None,
    stat_name_handler: Callable[[str], str] | None = None,
):
    return lambda: statsd_logger.get_statsd_logger(
        stats_class=stats_class,
        host="localhost",
        port="1234",
        prefix="airflow",
        metrics_allow_list=metrics_allow_list,
        metrics_block_list=metrics_block_list,
        stat_name_handler=stat_name_handler,
    )


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
        importlib.reload(airflow_shared.observability.metrics.stats)
        stats = airflow_shared.observability.metrics.stats
        stats.initialize(
            factory=get_statsd_logger_factory(stats_class=statsd.StatsClient), export_legacy_names=True
        )
        backend = stats._get_backend()
        assert isinstance(backend.statsd, statsd.StatsClient)
        assert not hasattr(backend, "dogstatsd")
        # Avoid side-effects
        importlib.reload(airflow_shared.observability.metrics.stats)

    def test_load_custom_statsd_client(self):
        importlib.reload(airflow_shared.observability.metrics.stats)
        stats = airflow_shared.observability.metrics.stats
        stats.initialize(
            factory=get_statsd_logger_factory(stats_class=CustomStatsd), export_legacy_names=True
        )
        assert isinstance(stats._get_backend().statsd, CustomStatsd)
        # Avoid side-effects
        importlib.reload(airflow_shared.observability.metrics.stats)

    def test_load_allow_list_validator(self):
        importlib.reload(airflow_shared.observability.metrics.stats)
        stats = airflow_shared.observability.metrics.stats
        stats.initialize(
            factory=get_statsd_logger_factory(
                stats_class=statsd.StatsClient,
                metrics_allow_list="name1,name2",
            ),
            export_legacy_names=True,
        )
        backend = stats._get_backend()
        assert isinstance(backend.metrics_validator, PatternAllowListValidator)
        assert backend.metrics_validator.validate_list == ("name1", "name2")
        # Avoid side-effects
        importlib.reload(airflow_shared.observability.metrics.stats)

    def test_load_block_list_validator(self):
        importlib.reload(airflow_shared.observability.metrics.stats)
        stats = airflow_shared.observability.metrics.stats
        stats.initialize(
            factory=get_statsd_logger_factory(
                stats_class=statsd.StatsClient,
                metrics_block_list="name1,name2",
            ),
            export_legacy_names=True,
        )
        backend = stats._get_backend()
        assert isinstance(backend.metrics_validator, PatternBlockListValidator)
        assert backend.metrics_validator.validate_list == ("name1", "name2")
        # Avoid side-effects
        importlib.reload(airflow_shared.observability.metrics.stats)

    def test_load_allow_and_block_list_validator_loads_only_allow_list_validator(self):
        importlib.reload(airflow_shared.observability.metrics.stats)
        stats = airflow_shared.observability.metrics.stats
        stats.initialize(
            factory=get_statsd_logger_factory(
                stats_class=statsd.StatsClient,
                metrics_allow_list="name1,name2",
                metrics_block_list="name1,name2",
            ),
            export_legacy_names=True,
        )
        backend = stats._get_backend()
        assert isinstance(backend.metrics_validator, PatternAllowListValidator)
        assert backend.metrics_validator.validate_list == ("name1", "name2")
        # Avoid side-effects
        importlib.reload(airflow_shared.observability.metrics.stats)


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
        importlib.reload(airflow_shared.observability.metrics.stats)

    @pytest.mark.parametrize(
        ("allow_list", "block_list", "expected"),
        [
            pytest.param(
                None,
                None,
                PatternAllowListValidator,
                id="pattern_allow_by_default",
            ),
            pytest.param(
                "foo,bar",
                None,
                PatternAllowListValidator,
                id="pattern_allow_list_provided",
            ),
            pytest.param(
                None,
                "foo,bar",
                PatternBlockListValidator,
                id="pattern_block_list_provided",
            ),
            pytest.param(
                "foo,bar",
                "foo,bar",
                PatternAllowListValidator,
                id="pattern_both_lists_provided",
            ),
        ],
    )
    def test_pattern_picker(self, allow_list, block_list, expected):
        importlib.reload(airflow_shared.observability.metrics.stats)
        airflow_shared.observability.metrics.stats.initialize(
            factory=get_statsd_logger_factory(
                stats_class=statsd.StatsClient,
                metrics_allow_list=allow_list,
                metrics_block_list=block_list,
            ),
            export_legacy_names=True,
        )

        backend = airflow_shared.observability.metrics.stats._get_backend()
        assert isinstance(backend.statsd, statsd.StatsClient)
        assert isinstance(backend.metrics_validator, expected)

    def test_setting_allow_and_block_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            importlib.reload(airflow_shared.observability.metrics.stats)
            airflow_shared.observability.metrics.stats.initialize(
                factory=get_statsd_logger_factory(
                    stats_class=statsd.StatsClient,
                    metrics_allow_list="baz,qux",
                    metrics_block_list="foo,bar",
                ),
                export_legacy_names=True,
            )

            backend = airflow_shared.observability.metrics.stats._get_backend()
            assert isinstance(backend.statsd, statsd.StatsClient)
            assert isinstance(backend.metrics_validator, PatternAllowListValidator)
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
        self.statsd_client = Mock(spec=statsd.StatsClient)
        self.stats = SafeStatsdLogger(
            self.statsd_client,
            influxdb_tags_enabled=True,
            metric_tags_validator=PatternBlockListValidator("key2,key3"),
            statsd_influxdb_enabled=True,
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


class TestStatsHelpers:
    """Tests for internal stats helper functions, used for exporting legacy metric names."""

    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            pytest.param({"x": 1}, {"x": 1}, id="number"),
            pytest.param({"x": False}, {"x": False}, id="boolean"),
            pytest.param({"x": None}, {}, id="None"),
            pytest.param({"x": {}}, {}, id="empty_dict"),
        ],
    )
    def test_defined_filters_values(self, kwargs, expected):
        from airflow_shared.observability.metrics.stats import _defined

        assert _defined(**kwargs) == expected

    @pytest.mark.parametrize(
        ("count", "rate", "delta", "tags", "expected"),
        [
            pytest.param(
                1,
                1,
                False,
                {},
                {"count": 1, "rate": 1, "delta": False},
                id="all_params_empty_tags",
            ),
            pytest.param(
                1,
                1,
                False,
                {"test": True},
                {"count": 1, "rate": 1, "delta": False, "tags": {"test": True}},
                id="provide_tags",
            ),
            pytest.param(
                None,
                1,
                False,
                {},
                {"rate": 1, "delta": False},
                id="no_count",
            ),
            pytest.param(
                1,
                None,
                False,
                {},
                {"count": 1, "delta": False},
                id="no_rate",
            ),
            pytest.param(
                1,
                1,
                None,
                {},
                {"count": 1, "rate": 1},
                id="no_delta",
            ),
            pytest.param(
                1,
                1,
                False,
                None,
                {"count": 1, "rate": 1, "delta": False},
                id="no_tags",
            ),
        ],
    )
    def test_defined_with_stat_args(
        self,
        count: int | None,
        rate: int | None,
        delta: bool | None,
        tags: dict | None,
        expected: dict,
    ):
        from airflow_shared.observability.metrics.stats import _defined

        assert _defined(count=count, rate=rate, delta=delta, tags=tags) == expected

    @pytest.mark.parametrize(
        ("stat", "tags", "expected_result"),
        [
            pytest.param(
                "operator_failures",
                {"operator_name": "exec1"},
                ("operator_failures_exec1", {}),
                id="name_vars_only_no_extra_tags",
            ),
            pytest.param(
                "operator_failures",
                {"operator_name": "exec1", "dag_id": "my_dag"},
                ("operator_failures_exec1", {"dag_id": "my_dag"}),
                id="name_vars_with_extra_tags",
            ),
            pytest.param(
                "missing_metric",
                {},
                (None, {}),
                id="missing_metric_returns_none",
            ),
        ],
    )
    def test_get_legacy_stat_name_and_tags(
        self,
        stat: str,
        tags: dict,
        expected_result: tuple[str | None, dict],
    ):
        from airflow_shared.observability.metrics.stats import _get_legacy_stat_name_and_tags

        assert _get_legacy_stat_name_and_tags(stat, tags) == expected_result

    def test_get_legacy_stat_name_and_tags_missing_variables_raises(self):
        from airflow_shared.observability.metrics.stats import _get_legacy_stat_name_and_tags

        with pytest.raises(ValueError, match="Missing required variables for metric"):
            _get_legacy_stat_name_and_tags("operator_failures", {})


class TestLegacyExport:
    """
    Tests for the dual stats export behavior driven by the contents of the YAML registry.

    When _export_legacy_names is True, and a metric has a legacy name in the registry,
    both the legacy stat (with name variables consumed from tags) and the modern stat
    (with all tags) are emitted.
    """

    @pytest.fixture
    def mock_backend(self):
        with mock.patch("airflow_shared.observability.metrics.stats._get_backend") as m:
            backend = mock.MagicMock(spec=StatsLogger)
            m.return_value = backend
            yield backend

    def test_incr_emits_both_when_legacy_name_exists(self, mock_backend):
        airflow_shared.observability.metrics.stats.incr(
            "operator_failures",
            tags={"operator_name": "EmptyOperator", "dag_id": "test_dag"},
        )
        assert mock_backend.incr.call_count == 2
        mock_backend.incr.assert_any_call("operator_failures_EmptyOperator", tags={"dag_id": "test_dag"})
        mock_backend.incr.assert_any_call(
            "operator_failures",
            tags={"operator_name": "EmptyOperator", "dag_id": "test_dag"},
        )

    def test_incr_no_extra_tags_legacy_has_no_tags(self, mock_backend):
        airflow_shared.observability.metrics.stats.incr(
            "operator_failures",
            tags={"operator_name": "EmptyOperator"},
        )
        assert mock_backend.incr.call_count == 2
        mock_backend.incr.assert_any_call("operator_failures_EmptyOperator")
        mock_backend.incr.assert_any_call(
            "operator_failures",
            tags={"operator_name": "EmptyOperator"},
        )

    def test_incr_legacy_export_disabled_emits_once(self, mock_backend, monkeypatch):
        monkeypatch.setattr(airflow_shared.observability.metrics.stats, "_export_legacy_names", False)
        airflow_shared.observability.metrics.stats.incr(
            "operator_failures",
            tags={"operator_name": "EmptyOperator", "dag_id": "test_dag"},
        )
        mock_backend.incr.assert_called_once_with(
            "operator_failures",
            tags={"operator_name": "EmptyOperator", "dag_id": "test_dag"},
        )

    def test_decr_emits_both_when_legacy_name_exists(self, mock_backend):
        airflow_shared.observability.metrics.stats.decr(
            "operator_failures",
            tags={"operator_name": "EmptyOperator", "dag_id": "test_dag"},
        )
        assert mock_backend.decr.call_count == 2
        mock_backend.decr.assert_any_call("operator_failures_EmptyOperator", tags={"dag_id": "test_dag"})
        mock_backend.decr.assert_any_call(
            "operator_failures",
            tags={"operator_name": "EmptyOperator", "dag_id": "test_dag"},
        )

    def test_gauge_emits_both_when_legacy_name_exists(self, mock_backend):
        airflow_shared.observability.metrics.stats.gauge(
            "ti.scheduled",
            42,
            tags={"queue": "default", "dag_id": "test_dag", "task_id": "test_task"},
        )
        assert mock_backend.gauge.call_count == 2
        mock_backend.gauge.assert_any_call("ti.scheduled.default.test_dag.test_task", 42)
        mock_backend.gauge.assert_any_call(
            "ti.scheduled",
            42,
            tags={"queue": "default", "dag_id": "test_dag", "task_id": "test_task"},
        )

    def test_timing_emits_both_when_legacy_name_exists(self, mock_backend):
        airflow_shared.observability.metrics.stats.timing(
            "operator_failures",
            1.5,
            tags={"operator_name": "EmptyOperator"},
        )
        assert mock_backend.timing.call_count == 2
        mock_backend.timing.assert_any_call("operator_failures_EmptyOperator", 1.5)
        mock_backend.timing.assert_any_call("operator_failures", 1.5, tags={"operator_name": "EmptyOperator"})

    def test_timer_times_both_when_legacy_name_exists(self, mock_backend):
        with airflow_shared.observability.metrics.stats.timer(
            "operator_failures",
            tags={"operator_name": "EmptyOperator"},
        ):
            pass
        assert mock_backend.timer.call_count == 2
        mock_backend.timer.assert_any_call("operator_failures_EmptyOperator")
        mock_backend.timer.assert_any_call("operator_failures", tags={"operator_name": "EmptyOperator"})


class TestCustomStatsName:
    def test_does_not_send_stats_using_statsd_when_the_name_is_not_valid(self):
        with mock.patch("statsd.StatsClient") as mock_statsd:
            importlib.reload(airflow_shared.observability.metrics.stats)
            airflow_shared.observability.metrics.stats.initialize(
                factory=get_statsd_logger_factory(
                    stats_class=mock_statsd,
                    stat_name_handler=always_invalid,
                ),
                export_legacy_names=True,
            )
            airflow_shared.observability.metrics.stats.incr("empty_key")
            mock_statsd.return_value.assert_not_called()

    @skip_if_force_lowest_dependencies_marker
    def test_does_not_send_stats_using_dogstatsd_when_the_name_is_not_valid(self):
        with mock.patch("datadog.DogStatsd") as mock_dogstatsd:
            importlib.reload(airflow_shared.observability.metrics.stats)
            airflow_shared.observability.metrics.stats.initialize(
                factory=lambda: datadog_logger.get_dogstatsd_logger(
                    host="localhost",
                    port="1234",
                    namespace="airflow",
                    stat_name_handler=always_invalid,
                ),
                export_legacy_names=True,
            )
            airflow_shared.observability.metrics.stats.incr("empty_key")
            mock_dogstatsd.return_value.assert_not_called()

    def test_does_send_stats_using_statsd_when_the_name_is_valid(self):
        with mock.patch("statsd.StatsClient") as mock_statsd:
            importlib.reload(airflow_shared.observability.metrics.stats)
            airflow_shared.observability.metrics.stats.initialize(
                factory=get_statsd_logger_factory(
                    stats_class=mock_statsd,
                    stat_name_handler=always_valid,
                ),
                export_legacy_names=True,
            )
            airflow_shared.observability.metrics.stats.incr("empty_key")
            mock_statsd.return_value.incr.assert_called_once_with("empty_key", 1, 1)

    @skip_if_force_lowest_dependencies_marker
    def test_does_send_stats_using_dogstatsd_when_the_name_is_valid(self):
        with mock.patch("datadog.DogStatsd") as mock_dogstatsd:
            importlib.reload(airflow_shared.observability.metrics.stats)
            airflow_shared.observability.metrics.stats.initialize(
                factory=lambda: datadog_logger.get_dogstatsd_logger(
                    host="localhost",
                    port="1234",
                    namespace="airflow",
                    stat_name_handler=always_valid,
                ),
                export_legacy_names=True,
            )

            airflow_shared.observability.metrics.stats.incr("empty_key")
            mock_dogstatsd.return_value.increment.assert_called_once_with(
                metric="empty_key", sample_rate=1, tags=[], value=1
            )

    def teardown_method(self) -> None:
        # To avoid side-effect
        importlib.reload(airflow_shared.observability.metrics.stats)

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
import os
import subprocess
import sys
import threading
import time
from unittest import mock

import pytest
from opentelemetry import metrics
from opentelemetry.metrics import MeterProvider
from opentelemetry.sdk.metrics import MeterProvider as SDKMeterProvider
from opentelemetry.sdk.metrics.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    ExponentialBucketHistogramAggregation,
    View,
)

from airflow_shared.observability.common import get_otel_data_exporter
from airflow_shared.observability.metrics import otel_logger as otel_logger_module
from airflow_shared.observability.metrics.otel_logger import (
    OTEL_NAME_MAX_LENGTH,
    UP_DOWN_COUNTERS,
    MetricsMap,
    SafeOtelLogger,
    _generate_key_name,
    _is_up_down_counter,
    full_name,
    get_otel_logger,
)
from airflow_shared.observability.metrics.validators import (
    BACK_COMPAT_METRIC_NAMES,
    MetricNameLengthExemptionWarning,
)
from airflow_shared.observability.otel_env_config import load_metrics_env_config

from tests_common.test_utils.config import env_vars

# Long enough that only the shutdown flush exports, so an exported metric name appears in the
# output once per flush.
NO_PERIODIC_EXPORT_INTERVAL_MS = 600000

# Short enough that an inherited reader fires several times while the child is alive.
CHILD_EXPORT_INTERVAL_MS = 500

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


@pytest.fixture(autouse=True)
def reset_process_provider():
    """Clear the per-process provider cache so tests never inherit one another's pipeline."""

    def discard() -> None:
        # Stop the pipeline rather than just dropping the reference: its exporter threads would
        # otherwise stay alive for the rest of the session.
        if otel_logger_module._provider is not None:
            otel_logger_module._stop_inherited_pipeline(otel_logger_module._provider)
        otel_logger_module._provider = None

    discard()
    yield
    discard()


@pytest.fixture
def reset_meter_provider():
    """Let a test install its own global MeterProvider, then restore the previous one.

    ``set_meter_provider`` is guarded by a process-wide ``Once``, so tests that install a
    provider have to clear it the same way ``get_otel_logger`` does after a fork.
    """
    import opentelemetry.metrics._internal as metrics_internal

    def clear() -> None:
        metrics_internal._METER_PROVIDER_SET_ONCE._done = False
        metrics_internal._METER_PROVIDER = None

    previous = metrics_internal._METER_PROVIDER
    clear()
    yield
    clear()
    metrics_internal._METER_PROVIDER = previous


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
        assert len(BACK_COMPAT_METRIC_NAMES) <= 25, (
            "This test exists solely to ensure that nobody is adding names to the exemption list. "
            "There are 25 names which are potentially too long for OTel and that number should "
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
    def test_invalid_stat_names_are_skipped(self, invalid_stat_combo):
        prefix = invalid_stat_combo[0]
        name = invalid_stat_combo[1]
        self.stats.prefix = prefix

        result = self.stats.incr(name)

        assert result is None
        self.meter.get_meter().create_counter.assert_not_called()

    @pytest.mark.parametrize(
        "stat",
        [
            "dag.my_dag.preço_task.scheduled_duration",
            "dag.my_dag.tâche_principale.duration",
            "dag.my_dag.aufgäbe.duration",
        ],
    )
    def test_non_ascii_stat_names_are_skipped_without_raising(self, stat):
        result = self.stats.incr(stat)

        assert result is None
        self.meter.get_meter().create_counter.assert_not_called()

    @pytest.mark.parametrize(
        "stat",
        [
            "dag_processing.last_run.seconds_ago.PBI_SKU_Performance copy",  # space in filename
            "dag_processing.last_run.seconds_ago.mein_däg_file",  # non-ASCII in filename
        ],
    )
    def test_gauge_with_invalid_stat_names_skipped_without_raising(self, stat):
        self.stats.gauge(stat, value=1)

        self.meter.get_meter().create_gauge.assert_not_called()

    @pytest.mark.parametrize(
        "stat",
        [
            "dag.my_dag.preço_task.duration",  # non-ASCII
            "dag.my_dag.task copy.duration",  # space
        ],
    )
    def test_timer_with_invalid_stat_name_does_not_record(self, stat):
        with self.stats.timer(stat):
            pass

        self.meter.get_meter().create_histogram.assert_not_called()

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
        assert self.map[full_name(name)].add.call_count == 2

    def test_gauge_new_metric(self, name):
        self.stats.gauge(name, value=1)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        assert self.map[full_name(name)].value == 1

    def test_gauge_new_metric_with_tags(self, name):
        tags = {"hello": "world"}
        key = _generate_key_name(full_name(name), tags)

        self.stats.gauge(name, value=1, tags=tags)

        self.meter.get_meter().create_gauge.assert_called_once_with(name=full_name(name))
        assert self.map[key].attributes == tags

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

        self.meter.get_meter().create_histogram.assert_called_once_with(name=full_name(name), unit="ms")
        self.meter.get_meter().create_histogram.return_value.record.assert_called_once_with(
            123000.0, attributes=None
        )

    def test_timing_new_metric_with_tags(self, name):
        tags = {"hello": "world"}

        self.stats.timing(name, dt=1, tags=tags)

        self.meter.get_meter().create_histogram.assert_called_once_with(name=full_name(name), unit="ms")
        self.meter.get_meter().create_histogram.return_value.record.assert_called_once_with(
            1.0, attributes=tags
        )

    def test_timing_existing_metric(self, name):
        self.stats.timing(name, dt=1)
        self.stats.timing(name, dt=2)

        # histogram created only once, but both observations are recorded
        self.meter.get_meter().create_histogram.assert_called_once_with(name=full_name(name), unit="ms")
        assert self.meter.get_meter().create_histogram.return_value.record.call_count == 2

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
        self.meter.get_meter().create_histogram.assert_called_once_with(name=full_name(name), unit="ms")

    @mock.patch.object(time, "perf_counter", side_effect=[0.0, 3.14])
    def test_timer_no_name_returns_float_but_does_not_store_value(self, mock_time, name):
        with self.stats.timer() as timer:
            pass

        assert hasattr(timer, "duration")
        assert isinstance(timer.duration, float)
        expected_duration = 3140.0
        assert timer.duration == expected_duration
        assert mock_time.call_count == 2
        self.meter.get_meter().create_histogram.assert_not_called()

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
        self.meter.get_meter().create_histogram.assert_not_called()

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
        self.meter.get_meter().create_histogram.assert_called_once_with(name=full_name(name), unit="ms")

    @pytest.mark.parametrize(
        (
            "provided_env_vars",
            "airflow_conf_host",
            "airflow_conf_port",
            "expected_endpoint",
            "expected_exporter_module",
        ),
        [
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
                },
                "breeze-otel-collector",
                "4318",
                "localhost:1234",
                "grpc",
                id="env_vars_with_grpc",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
                },
                "breeze-otel-collector",
                "4318",
                "http://breeze-otel-collector:4318/v1/metrics",
                "http",
                id="protocol_is_ignored_if_no_env_endpoint",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                },
                "breeze-otel-collector",
                "4318",
                "http://localhost:1234/v1/metrics",
                "http",
                id="for_http_with_env_vars_otel_builds_full_url",
            ),
            pytest.param(
                {},
                "breeze-otel-collector",
                "4318",
                "http://breeze-otel-collector:4318/v1/metrics",
                "http",
                id="use_airflow_config",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                },
                None,
                None,
                "http://localhost:1234/v1/metrics",
                "http",
                id="only_env_vars",
            ),
            pytest.param(
                {
                    "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:1234",
                    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://localhost:2222",
                    "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL": "grpc",
                },
                None,
                None,
                "localhost:2222",
                "grpc",
                id="type_specific_vars_take_precedence",
            ),
            pytest.param(
                {},
                "::1",
                "4318",
                "http://[::1]:4318/v1/metrics",
                "http",
                id="airflow_config_ipv6_loopback_is_bracketed",
            ),
            pytest.param(
                {},
                "2001:db8::1",
                "4318",
                "http://[2001:db8::1]:4318/v1/metrics",
                "http",
                id="airflow_config_ipv6_literal_is_bracketed",
            ),
            pytest.param(
                {},
                "[::1]",
                "4318",
                "http://[::1]:4318/v1/metrics",
                "http",
                id="airflow_config_already_bracketed_ipv6_is_preserved",
            ),
            pytest.param(
                {},
                "10.0.0.1",
                "4318",
                "http://10.0.0.1:4318/v1/metrics",
                "http",
                id="airflow_config_ipv4_literal_passes_through_unchanged",
            ),
        ],
    )
    def test_config_priorities(
        self,
        provided_env_vars,
        airflow_conf_host,
        airflow_conf_port,
        expected_endpoint,
        expected_exporter_module,
    ):
        with env_vars(provided_env_vars):
            otel_env_config = load_metrics_env_config()

            otel_metric_exporter = get_otel_data_exporter(
                otel_env_config=otel_env_config,
                host=airflow_conf_host,
                port=airflow_conf_port,
            )

            assert otel_metric_exporter._endpoint == expected_endpoint

            assert (
                otel_metric_exporter.__class__.__module__
                == f"opentelemetry.exporter.otlp.proto.{expected_exporter_module}.metric_exporter"
            )

    @mock.patch("airflow_shared.observability.metrics.otel_logger.metrics")
    @mock.patch("airflow_shared.observability.metrics.otel_logger.MeterProvider")
    def test_get_otel_logger_uses_exponential_histogram_view(self, mock_provider, mock_metrics):
        get_otel_logger(host="localhost", port=4318)

        call_kwargs = mock_provider.call_args.kwargs
        views = call_kwargs["views"]
        assert len(views) == 1
        view = views[0]
        assert isinstance(view, View)
        assert isinstance(view._aggregation, ExponentialBucketHistogramAggregation)

    def test_declaratively_configured_provider_is_not_replaced(self, reset_meter_provider):
        """A MeterProvider built from OTEL_CONFIG_FILE must survive get_otel_logger().

        The declarative configuration spec makes that file the sole source of SDK
        construction, so replacing its provider silently drops the deployment's views.
        See https://github.com/apache/airflow/issues/64690 for why the provider is
        otherwise force-replaced.
        """
        declarative_view = View(
            instrument_name="*_duration",
            aggregation=ExplicitBucketHistogramAggregation(boundaries=(0.5, 1, 2, 4, 8)),
        )
        configured_provider = SDKMeterProvider(views=[declarative_view], shutdown_on_exit=False)
        metrics.set_meter_provider(configured_provider)

        with env_vars({"OTEL_CONFIG_FILE": "/tmp/otel-config.yaml"}):
            logger = get_otel_logger(host="localhost", port=4318)

        assert logger.otel is configured_provider
        assert metrics.get_meter_provider() is configured_provider
        assert list(configured_provider._sdk_config.views) == [declarative_view]

    def test_provider_is_replaced_without_declarative_config(self, reset_meter_provider):
        """Without OTEL_CONFIG_FILE, Airflow still installs its own provider."""
        pre_existing = SDKMeterProvider(shutdown_on_exit=False)
        metrics.set_meter_provider(pre_existing)

        logger = get_otel_logger(host="localhost", port=4318)

        assert logger.otel is not pre_existing

    def test_atexit_flush_on_process_exit(self):
        """
        Run a process that initializes a logger, creates a stat and then exits.

        The logger initialization registers an atexit hook.
        Test that the hook runs and flushes the created stat at shutdown.
        """
        proc = run_service_helper("mock_service_run")

        assert "my_test_stat" in proc.stdout, (
            "Expected the metric name to be present in the stdout but it wasn't.\n"
            f"stdout:\n{proc.stdout}\n"
            f"stderr:\n{proc.stderr}"
        )

    @pytest.mark.parametrize(
        ("runner", "metric_name"),
        [
            ("mock_service_fork_child_without_reinit", "parent_stat"),
            ("mock_service_fork_child_with_reinit", "child_stat"),
        ],
    )
    def test_forked_child_does_not_duplicate_the_shutdown_flush(self, runner, metric_name):
        """A fork must not turn one shutdown flush into two.

        The child inherits the parent's atexit hook along with its MeterProvider, so an
        inherited hook either re-exports everything the parent accumulated (child that never
        initializes its own logger) or runs twice over the child's own provider (child that does).
        """
        proc = run_service_helper(runner)

        exports = proc.stdout.count(f'"name": "airflow.{metric_name}"')
        assert exports == 1, (
            f"Expected 'airflow.{metric_name}' to be exported exactly once but it was "
            f"exported {exports} times.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )

    @pytest.mark.parametrize(
        ("runner", "own_metric"),
        [
            ("mock_service_fork_idle_child", None),
            ("mock_service_fork_reinitializing_child", "child_stat"),
        ],
    )
    def test_forked_child_stops_the_pipeline_it_inherited(self, tmp_path, runner, own_metric):
        """A child must not keep exporting the metrics its parent accumulated.

        ``PeriodicExportingMetricReader`` registers its own ``after_in_child`` hook that restarts
        the export thread, so without stopping the inherited provider the child republishes the
        parent's cumulative totals on every interval for as long as it lives. A child that builds
        its own pipeline — the case every worker that emits metrics reaches — must go on exporting
        through that one on the same interval.
        """
        child_output = tmp_path / "child-metrics.json"
        run_service_helper(runner, child_output)

        exported_in_child = child_output.read_text()
        assert '"name": "airflow.parent_stat"' not in exported_in_child, (
            "The forked child kept exporting the parent's metrics from the inherited pipeline.\n"
            f"child output:\n{exported_in_child}"
        )
        if own_metric:
            assert f'"name": "airflow.{own_metric}"' in exported_in_child, (
                "The forked child stopped exporting through the pipeline it built for itself.\n"
                f"child output:\n{exported_in_child}"
            )

    def test_forked_child_does_not_flush_a_provider_it_did_not_build(self, tmp_path):
        """A provider supplied by the SDK brings its own atexit shutdown; a child must not run it.

        ``shutdown_on_exit=True`` is the SDK default for a ``MeterProvider`` built from
        ``OTEL_CONFIG_FILE`` or by an instrumentation agent, and a fork hands that hook to the
        child together with everything the parent accumulated.
        """
        child_output = tmp_path / "child-metrics.json"
        run_service_helper("mock_service_fork_child_under_foreign_provider", child_output)

        assert "foreign_counter" not in child_output.read_text(), (
            "The child ran the inherited provider's shutdown hook and exported the parent's state."
        )

    def test_forked_child_leaves_other_pipelines_alone(self, tmp_path):
        """Stopping the inherited pipeline must not touch a provider Airflow did not build.

        ``MeterProvider._all_metric_readers`` is a class attribute shared by every provider in the
        process, so reaching for that instead of this provider's own readers would silence an
        ``opentelemetry-instrument`` pipeline in every forked child.
        """
        child_output = tmp_path / "child-metrics.json"
        run_service_helper("mock_service_fork_idle_child_beside_foreign_provider", child_output)

        exported_in_child = child_output.read_text()
        assert "foreign_counter" in exported_in_child, (
            f"The child stopped a pipeline Airflow does not own.\nchild output:\n{exported_in_child}"
        )
        assert '"name": "airflow.parent_stat"' not in exported_in_child

    def test_reinit_reuses_the_process_pipeline(self, reset_meter_provider):
        """A second call must not leave a second pipeline exporting alongside the first.

        Every ``MeterProvider`` owns a ``PeriodicExportingMetricReader`` whose thread exports on
        its own interval, and ``shutdown_on_exit=False`` means nothing reaps one that gets
        replaced: its instruments stop being recorded to, so it republishes frozen cumulative
        totals alongside the live stream for the same series.
        """
        first = get_otel_logger(host="localhost", port=4318, conf_interval=NO_PERIODIC_EXPORT_INTERVAL_MS)
        readers_after_first = count_live_readers()

        second = get_otel_logger(host="localhost", port=4318, conf_interval=NO_PERIODIC_EXPORT_INTERVAL_MS)

        assert second.otel is first.otel
        assert count_live_readers() == readers_after_first


TEST_MODULE = "tests.observability.metrics.test_otel_logger"


def run_service_helper(runner: str, child_out=None) -> subprocess.CompletedProcess:
    """Run one of this module's ``mock_service_*`` helpers in a fresh interpreter."""
    env = os.environ.copy()
    if child_out is not None:
        env["CHILD_METRICS_OUT"] = str(child_out)

    proc = subprocess.run(
        [sys.executable, "-c", f"import {TEST_MODULE} as m; m.{runner}()"],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, f"{runner} failed\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    return proc


def count_live_readers() -> int:
    """Number of running periodic exporter threads (a pipeline has one per configured reader)."""
    return sum(1 for t in threading.enumerate() if t.name == "OtelPeriodicExportingMetricReader")


def mock_service_run():
    logger = get_otel_logger(debug=True)
    logger.incr("my_test_stat")


def mock_service_fork_child_without_reinit():
    """Emit a metric, then fork a child that exits without initializing its own logger."""
    get_otel_logger(debug=True, conf_interval=NO_PERIODIC_EXPORT_INTERVAL_MS).incr("parent_stat")
    if os.fork() == 0:
        sys.exit(0)
    os.wait()


def mock_service_fork_child_with_reinit():
    """Emit a metric, then fork a child that initializes its own logger and emits its own metric.

    The child also checks that it installed its own provider globally: the parent's
    ``_METER_PROVIDER_SET_ONCE`` guard is inherited as already-done, so without the reset in
    ``get_otel_logger()`` the child's ``set_meter_provider()`` silently leaves the parent's
    provider in place for anything that reads the global one. See
    https://github.com/apache/airflow/issues/64690.
    """
    get_otel_logger(debug=True, conf_interval=NO_PERIODIC_EXPORT_INTERVAL_MS).incr("parent_stat")
    if os.fork() == 0:
        child = get_otel_logger(debug=True, conf_interval=NO_PERIODIC_EXPORT_INTERVAL_MS)
        child.incr("child_stat")
        if metrics.get_meter_provider() is not child.otel:
            print("child did not install its own MeterProvider globally", file=sys.stderr)
            sys.exit(1)
        sys.exit(0)
    os.wait()


def _fork_child_capturing_its_own_output(on_start=None):
    """Fork a child that redirects its output to ``CHILD_METRICS_OUT`` and idles, then reap it."""
    if os.fork() == 0:
        fd = os.open(os.environ["CHILD_METRICS_OUT"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.dup2(fd, 1)
        os.dup2(fd, 2)
        if on_start is not None:
            on_start()
        time.sleep(CHILD_EXPORT_INTERVAL_MS / 1000 * 5)
        os._exit(0)
    os.wait()


def mock_service_fork_idle_child():
    """Emit a metric, then fork a child that only idles while the export interval elapses."""
    get_otel_logger(debug=True, conf_interval=CHILD_EXPORT_INTERVAL_MS).incr("parent_stat")
    _fork_child_capturing_its_own_output()


def mock_service_fork_reinitializing_child():
    """Emit a metric, then fork a child that builds its own pipeline and idles alongside it."""
    get_otel_logger(debug=True, conf_interval=CHILD_EXPORT_INTERVAL_MS).incr("parent_stat")
    _fork_child_capturing_its_own_output(
        on_start=lambda: get_otel_logger(debug=True, conf_interval=CHILD_EXPORT_INTERVAL_MS).incr(
            "child_stat"
        )
    )


def mock_service_fork_idle_child_beside_foreign_provider():
    """Emit a metric with a provider Airflow does not own also running, then fork an idle child."""
    foreign = SDKMeterProvider(
        metric_readers=[
            PeriodicExportingMetricReader(
                ConsoleMetricExporter(), export_interval_millis=CHILD_EXPORT_INTERVAL_MS
            )
        ],
        shutdown_on_exit=False,
    )
    foreign.get_meter("foreign").create_counter("foreign_counter").add(7)
    get_otel_logger(debug=True, conf_interval=CHILD_EXPORT_INTERVAL_MS).incr("parent_stat")
    _fork_child_capturing_its_own_output()


def mock_service_fork_child_under_foreign_provider():
    """Record into an SDK-built provider that keeps its own atexit shutdown, then fork and exit."""
    foreign = SDKMeterProvider(
        metric_readers=[
            PeriodicExportingMetricReader(
                ConsoleMetricExporter(), export_interval_millis=NO_PERIODIC_EXPORT_INTERVAL_MS
            )
        ],
    )
    metrics.set_meter_provider(foreign)
    foreign.get_meter("foreign").create_counter("foreign_counter").add(7)
    if os.fork() == 0:
        fd = os.open(os.environ["CHILD_METRICS_OUT"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.dup2(fd, 1)
        os.dup2(fd, 2)
        sys.exit(0)  # normal exit: atexit hooks run
    os.wait()

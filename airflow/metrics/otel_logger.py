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
import random
import warnings
from typing import Callable

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.util.types import Attributes

from airflow.configuration import conf
from airflow.metrics.protocols import DeltaType, Timer, TimerProtocol
from airflow.metrics.validators import (
    OTEL_NAME_MAX_LENGTH,
    AllowListValidator,
    stat_name_otel_handler,
    validate_stat,
)

log = logging.getLogger(__name__)


# "airflow.dag_processing.processes" is currently the only UDC used in Airflow.  If more are added,
# we should add a better system for this.
#
# Generally in OTel a Counter is monotonic (can only go up) and there is an UpDownCounter which,
# as you can guess, is non-monotonic; it can go up or down. The choice here is to either drop
# this one metric and implement the rest as monotonic Counters, implement all counters as
# UpDownCounters, or add a bit of logic to do it intelligently. The catch is that the Collector
# which transmits these metrics to the upstream dashboard tools (Prometheus, Grafana, etc.) assigns
# the type of Gauge to any UDC instead of Counter. Adding this logic feels like the best compromise
# where normal Counters still get typed correctly, and we don't lose an existing metric.
# See:
# https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#counter-creation
# https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/api.md#updowncounter
UP_DOWN_COUNTERS = {"airflow.dag_processing.processes"}

METRIC_NAME_PREFIX = "airflow."


def _is_up_down_counter(name):
    return name in UP_DOWN_COUNTERS


def _generate_key_name(name: str, attributes: Attributes = None):
    if attributes:
        key = name
        for item in attributes.items():
            key += f"_{item[0]}_{item[1]}"
    else:
        key = name

    return key


def name_is_otel_safe(prefix: str, name: str) -> bool:
    """
    Returns True if the provided name and prefix would result in a name that meets the OpenTelemetry standard.

    Legal names are defined here:
    https://opentelemetry.io/docs/reference/specification/metrics/api/#instrument-name-syntax
    """
    return bool(stat_name_otel_handler(prefix, name, max_length=OTEL_NAME_MAX_LENGTH))


class SafeOtelLogger:
    """Otel Logger."""

    def __init__(self, otel_provider, prefix: str = "airflow", allow_list_validator=AllowListValidator()):
        self.otel: Callable = otel_provider
        self.prefix: str = prefix
        self.metrics_validator = allow_list_validator
        self.meter = otel_provider.get_meter(__name__)
        self.metrics_map = MetricsMap(self.meter)

    def incr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        tags: Attributes = None,
    ):
        """
        Increment stat by count.

        :param stat: The name of the stat to increment.
        :param count: A positive integer to add to the current value of stat.
        :param rate: value between 0 and 1 that represents the sampled rate at
            which the metric is going to be emitted.
        :param tags: Tags to append to the stat.
        """
        if (count < 0) or (rate < 0):
            raise ValueError("count and rate must both be positive values.")
        if rate < 1 and random.random() > rate:
            return

        if self.metrics_validator.test(stat) and name_is_otel_safe(self.prefix, stat):
            counter = self.metrics_map.get_counter(f"{self.prefix}.{stat}", attributes=tags)
            counter.add(count, attributes=tags)
            return counter

    def decr(
        self,
        stat: str,
        count: int = 1,
        rate: float = 1,
        tags: Attributes = None,
    ):
        """
        Decrement stat by count.

        :param stat: The name of the stat to decrement.
        :param count: A positive integer to subtract from current value of stat.
        :param rate: value between 0 and 1 that represents the sampled rate at
            which the metric is going to be emitted.
        :param tags: Tags to append to the stat.
        """
        if (count < 0) or (rate < 0):
            raise ValueError("count and rate must both be positive values.")
        if rate < 1 and random.random() > rate:
            return

        if self.metrics_validator.test(stat) and name_is_otel_safe(self.prefix, stat):
            counter = self.metrics_map.get_counter(f"{self.prefix}.{stat}")
            counter.add(-count, attributes=tags)
            return counter

    @validate_stat
    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: Attributes = None,
    ) -> None:
        warnings.warn("OpenTelemetry Gauges are not yet implemented.")
        return None

    @validate_stat
    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: Attributes = None,
    ) -> None:
        warnings.warn("OpenTelemetry Timers are not yet implemented.")
        return None

    @validate_stat
    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: Attributes = None,
        **kwargs,
    ) -> TimerProtocol:
        warnings.warn("OpenTelemetry Timers are not yet implemented.")
        return Timer()


class MetricsMap:
    """Stores Otel Instruments."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    def _create_counter(self, name):
        """Creates a new counter or up_down_counter for the provided name."""
        otel_safe_name = name[:OTEL_NAME_MAX_LENGTH]
        if name != otel_safe_name:
            warnings.warn(
                f"Metric name `{name}` exceeds OpenTelemetry's name length limit of "
                f"{OTEL_NAME_MAX_LENGTH} characters and will be truncated to `{otel_safe_name}`."
            )

        if _is_up_down_counter(name):
            counter = self.meter.create_up_down_counter(name=otel_safe_name)
        else:
            counter = self.meter.create_counter(name=otel_safe_name)

        counter_type = str(type(counter)).split(".")[-1][:-2]
        logging.debug("Created %s as type: %s", otel_safe_name, counter_type)
        return counter

    def get_counter(self, name: str, attributes: Attributes = None):
        """
        Returns the counter; creates a new one if it did not exist.

        :param name: The name of the counter to fetch or create.
        :param attributes:  Counter attributes, used to generate a unique key to store the counter.
        """
        key = _generate_key_name(name, attributes)
        if key in self.map.keys():
            return self.map[key]
        else:
            new_counter = self._create_counter(name)
            self.map[key] = new_counter
            return new_counter

    def del_counter(self, name: str, attributes: Attributes = None) -> None:
        """
        Deletes a counter.

        :param name: The name of the counter to delete.
        :param attributes: Counter attributes which were used to generate a unique key to store the counter.
        """
        key = _generate_key_name(name, attributes)
        if key in self.map.keys():
            del self.map[key]


def get_otel_logger(cls) -> SafeOtelLogger:
    host = conf.get("metrics", "otel_host")  # ex: "breeze-otel-collector"
    port = conf.getint("metrics", "otel_port")  # ex: 4318
    prefix = conf.get("metrics", "otel_prefix")  # ex: "airflow"
    interval = conf.getint("metrics", "otel_interval_milliseconds")  # ex: 30000
    debug = conf.getboolean("metrics", "otel_debugging_on")

    allow_list = conf.get("metrics", "metrics_allow_list", fallback=None)
    allow_list_validator = AllowListValidator(allow_list)

    resource = Resource(attributes={SERVICE_NAME: "Airflow"})
    # TODO:  figure out https instead of http ??
    endpoint = f"http://{host}:{port}/v1/metrics"

    logging.info("[Metric Exporter] Connecting to OpenTelemetry Collector at %s", endpoint)
    readers = [
        PeriodicExportingMetricReader(
            OTLPMetricExporter(
                endpoint=endpoint,
                headers={"Content-Type": "application/json"},
            ),
            export_interval_millis=interval,
        )
    ]

    if debug:
        export_to_console = PeriodicExportingMetricReader(ConsoleMetricExporter())
        readers.append(export_to_console)

    metrics.set_meter_provider(
        MeterProvider(
            resource=resource,
            metric_readers=readers,
            shutdown_on_exit=False,
        ),
    )

    return SafeOtelLogger(metrics.get_meter_provider(), prefix, allow_list_validator)

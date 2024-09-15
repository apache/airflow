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

import datetime
import logging
import random
import warnings
from functools import partial
from typing import TYPE_CHECKING, Callable, Iterable, Union

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import ConsoleMetricExporter, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from airflow.configuration import conf
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.metrics.protocols import Timer
from airflow.metrics.validators import (
    OTEL_NAME_MAX_LENGTH,
    AllowListValidator,
    ListValidator,
    get_validator,
    stat_name_otel_handler,
)

if TYPE_CHECKING:
    from opentelemetry.metrics import Instrument
    from opentelemetry.util.types import Attributes

    from airflow.metrics.protocols import DeltaType, TimerProtocol

log = logging.getLogger(__name__)

GaugeValues = Union[int, float]

DEFAULT_GAUGE_VALUE = 0.0

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

DEFAULT_METRIC_NAME_PREFIX = "airflow"
# Delimiter is placed between the universal metric prefix and the unique metric name.
DEFAULT_METRIC_NAME_DELIMITER = "."

metrics_consistency_on = conf.getboolean("metrics", "metrics_consistency_on", fallback=True)
if not metrics_consistency_on:
    warnings.warn(
        "Timer and timing metrics publish in seconds were deprecated. It is enabled by default from Airflow 3 onwards. Enable metrics consistency to publish all the timer and timing metrics in milliseconds.",
        AirflowProviderDeprecationWarning,
        stacklevel=2,
    )


def full_name(name: str, *, prefix: str = DEFAULT_METRIC_NAME_PREFIX) -> str:
    """Assembles the prefix, delimiter, and name and returns it as a string."""
    return f"{prefix}{DEFAULT_METRIC_NAME_DELIMITER}{name}"


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
    Return True if the provided name and prefix would result in a name that meets the OpenTelemetry standard.

    Legal names are defined here:
    https://opentelemetry.io/docs/reference/specification/metrics/api/#instrument-name-syntax
    """
    return bool(stat_name_otel_handler(prefix, name, max_length=OTEL_NAME_MAX_LENGTH))


def _type_as_str(obj: Instrument) -> str:
    """
    Given an OpenTelemetry Instrument, returns the type of the instrument as a string.

    :param obj: An OTel Instrument or subclass
    :returns: The type() of the Instrument without all the nested class info
    """
    # type().__name__ will return something like: '_Counter',
    # this drops the leading underscore for cleaner logging.

    return type(obj).__name__[1:]


def _get_otel_safe_name(name: str) -> str:
    """
    Verify that the provided name does not exceed OpenTelemetry's maximum length for metric names.

    :param name: The original metric name
    :returns: The name, truncated to an OTel-acceptable length if required.
    """
    otel_safe_name = name[:OTEL_NAME_MAX_LENGTH]
    if name != otel_safe_name:
        warnings.warn(
            f"Metric name `{name}` exceeds OpenTelemetry's name length limit of "
            f"{OTEL_NAME_MAX_LENGTH} characters and will be truncated to `{otel_safe_name}`.",
            category=UserWarning,
            stacklevel=2,
        )
    return otel_safe_name


def _skip_due_to_rate(rate: float) -> bool:
    if rate < 0:
        raise ValueError("rate must be a positive value.")
    return rate < 1 and random.random() > rate


class _OtelTimer(Timer):
    """
    An implementation of Stats.Timer() which records the result in the OTel Metrics Map.

    OpenTelemetry does not have a native timer, we will store the values as a Gauge.

    :param name: The name of the timer.
    :param tags: Tags to append to the timer.
    """

    def __init__(self, otel_logger: SafeOtelLogger, name: str | None, tags: Attributes):
        super().__init__()
        self.otel_logger = otel_logger
        self.name = name
        self.tags = tags

    def stop(self, send: bool = True) -> None:
        super().stop(send)
        if self.name and send:
            self.otel_logger.metrics_map.set_gauge_value(
                full_name(prefix=self.otel_logger.prefix, name=self.name), self.duration, False, self.tags
            )


class SafeOtelLogger:
    """Otel Logger."""

    def __init__(
        self,
        otel_provider,
        prefix: str = DEFAULT_METRIC_NAME_PREFIX,
        metrics_validator: ListValidator = AllowListValidator(),
    ):
        self.otel: Callable = otel_provider
        self.prefix: str = prefix
        self.metrics_validator = metrics_validator
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
        :param rate: value between 0 and 1 that represents the sample rate at
            which the metric is going to be emitted.
        :param tags: Tags to append to the stat.
        """
        if _skip_due_to_rate(rate):
            return
        if count < 0:
            raise ValueError("count must be a positive value.")

        if self.metrics_validator.test(stat) and name_is_otel_safe(self.prefix, stat):
            counter = self.metrics_map.get_counter(full_name(prefix=self.prefix, name=stat), attributes=tags)
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
        :param rate: value between 0 and 1 that represents the sample rate at
            which the metric is going to be emitted.
        :param tags: Tags to append to the stat.
        """
        if _skip_due_to_rate(rate):
            return
        if count < 0:
            raise ValueError("count must be a positive value.")

        if self.metrics_validator.test(stat) and name_is_otel_safe(self.prefix, stat):
            counter = self.metrics_map.get_counter(full_name(prefix=self.prefix, name=stat))
            counter.add(-count, attributes=tags)
            return counter

    def gauge(
        self,
        stat: str,
        value: int | float,
        rate: float = 1,
        delta: bool = False,
        *,
        tags: Attributes = None,
        back_compat_name: str = "",
    ) -> None:
        """
        Record a new value for a Gauge.

        :param stat: The name of the stat to update.
        :param value: The new value of stat, either a float or an int.
        :param rate: value between 0 and 1 that represents the sample rate at
            which the metric is going to be emitted.
        :param delta: If true, the provided value will be added to the previous value.
            If False the new value will override the previous.
        :param tags: Tags to append to the stat.
        :param back_compat_name:  If an alternative name is provided, the
            stat will be emitted using both names if possible.
        """
        if _skip_due_to_rate(rate):
            return

        if back_compat_name and self.metrics_validator.test(back_compat_name):
            self.metrics_map.set_gauge_value(
                full_name(prefix=self.prefix, name=back_compat_name), value, delta, tags
            )

        if self.metrics_validator.test(stat):
            self.metrics_map.set_gauge_value(full_name(prefix=self.prefix, name=stat), value, delta, tags)

    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: Attributes = None,
    ) -> None:
        """OTel does not have a native timer, stored as a Gauge whose value is number of seconds elapsed."""
        if self.metrics_validator.test(stat) and name_is_otel_safe(self.prefix, stat):
            if isinstance(dt, datetime.timedelta):
                if metrics_consistency_on:
                    dt = dt.total_seconds() * 1000.0
                else:
                    dt = dt.total_seconds()
            self.metrics_map.set_gauge_value(full_name(prefix=self.prefix, name=stat), float(dt), False, tags)

    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: Attributes = None,
        **kwargs,
    ) -> TimerProtocol:
        """Timer context manager returns the duration and can be cancelled."""
        return _OtelTimer(self, stat, tags)


class MetricsMap:
    """Stores Otel Instruments."""

    def __init__(self, meter):
        self.meter = meter
        self.map = {}

    def clear(self) -> None:
        self.map.clear()

    def _create_counter(self, name):
        """Create a new counter or up_down_counter for the provided name."""
        otel_safe_name = _get_otel_safe_name(name)

        if _is_up_down_counter(name):
            counter = self.meter.create_up_down_counter(name=otel_safe_name)
        else:
            counter = self.meter.create_counter(name=otel_safe_name)

        log.debug("Created %s as type: %s", otel_safe_name, _type_as_str(counter))
        return counter

    def get_counter(self, name: str, attributes: Attributes = None):
        """
        Return the counter; creates a new one if it did not exist.

        :param name: The name of the counter to fetch or create.
        :param attributes:  Counter attributes, used to generate a unique key to store the counter.
        """
        key = _generate_key_name(name, attributes)
        if key not in self.map:
            self.map[key] = self._create_counter(name)
        return self.map[key]

    def del_counter(self, name: str, attributes: Attributes = None) -> None:
        """
        Delete a counter.

        :param name: The name of the counter to delete.
        :param attributes: Counter attributes which were used to generate a unique key to store the counter.
        """
        key = _generate_key_name(name, attributes)
        if key in self.map.keys():
            del self.map[key]

    def set_gauge_value(self, name: str, value: float | None, delta: bool, tags: Attributes):
        """
        Override the last reading for a Gauge with a new value.

        :param name: The name of the gauge to record.
        :param value: The new reading to record.
        :param delta: If True, value is added to the previous reading, else it overrides.
        :param tags: Gauge attributes which were used to generate a unique key to store the counter.
        :returns: None
        """
        key: str = _generate_key_name(name, tags)
        new_value = value or DEFAULT_GAUGE_VALUE
        old_value = self.poke_gauge(name, tags)
        if delta:
            new_value += old_value
        # If delta is true, add the new value to the last reading otherwise overwrite it.
        self.map[key] = Observation(new_value, tags)

    def _create_gauge(self, name: str, attributes: Attributes = None):
        """
        Create a new Observable Gauge with the provided name and the default value.

        :param name: The name of the gauge to fetch or create.
        :param attributes:  Gauge attributes, used to generate a unique key to store the gauge.
        """
        otel_safe_name = _get_otel_safe_name(name)
        key = _generate_key_name(name, attributes)

        gauge = self.meter.create_observable_gauge(
            name=otel_safe_name,
            callbacks=[partial(self.read_gauge, _generate_key_name(name, attributes))],
        )
        self.map[key] = Observation(DEFAULT_GAUGE_VALUE, attributes)

        return gauge

    def read_gauge(self, key: str, *args) -> Iterable[Observation]:
        """Return the Observation for the provided key; callback for the Observable Gauges."""
        yield self.map[key]

    def poke_gauge(self, name: str, attributes: Attributes = None) -> GaugeValues:
        """
        Return the value of the gauge; creates a new one with the default value if it did not exist.

        :param name: The name of the gauge to fetch or create.
        :param attributes:  Gauge attributes, used to generate a unique key to store the gauge.
        :returns:  The integer or float value last recorded for the provided Gauge name.
        """
        key = _generate_key_name(name, attributes)
        if key not in self.map:
            self._create_gauge(name, attributes)

        return self.map[key].value


def get_otel_logger(cls) -> SafeOtelLogger:
    host = conf.get("metrics", "otel_host")  # ex: "breeze-otel-collector"
    port = conf.getint("metrics", "otel_port")  # ex: 4318
    prefix = conf.get("metrics", "otel_prefix")  # ex: "airflow"
    ssl_active = conf.getboolean("metrics", "otel_ssl_active")
    # PeriodicExportingMetricReader will default to an interval of 60000 millis.
    interval = conf.getint("metrics", "otel_interval_milliseconds", fallback=None)  # ex: 30000
    debug = conf.getboolean("metrics", "otel_debugging_on")
    service_name = conf.get("metrics", "otel_service")

    resource = Resource(attributes={SERVICE_NAME: service_name})

    protocol = "https" if ssl_active else "http"
    endpoint = f"{protocol}://{host}:{port}/v1/metrics"

    log.info("[Metric Exporter] Connecting to OpenTelemetry Collector at %s", endpoint)
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

    return SafeOtelLogger(metrics.get_meter_provider(), prefix, get_validator())

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
from importlib.metadata import entry_points
from typing import TYPE_CHECKING

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.metrics.view import ExponentialBucketHistogramAggregation, View
from opentelemetry.sdk.resources import SERVICE_NAME, Resource

from ..common import _format_url_host
from .otel_logger import (
    DEFAULT_METRIC_NAME_PREFIX,
    SafeOtelLogger,
    atexit_register_metrics_flush,
)
from .validators import get_validator

if TYPE_CHECKING:
    from configparser import ConfigParser

    from opentelemetry.sdk.metrics._internal.export import MetricExporter

log = logging.getLogger(__name__)


def _get_backcompat_config(
    conf: ConfigParser,
) -> tuple[str | None, float | None, Resource | None]:
    """
    Possibly get deprecated Airflow configs for otel metrics.

    Ideally we return ``(None, None, None)`` here. But if the old configuration
    is there, then we will use it.
    """
    resource = None
    if not os.environ.get("OTEL_SERVICE_NAME") and not os.environ.get("OTEL_RESOURCE_ATTRIBUTES"):
        service_name = conf.get("metrics", "otel_service", fallback=None)
        if service_name:
            resource = Resource.create(attributes={SERVICE_NAME: service_name})

    endpoint = None
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") and not os.environ.get(
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
    ):
        host = conf.get("metrics", "otel_host", fallback=None)
        port = conf.get("metrics", "otel_port", fallback=None)
        ssl_active = conf.getboolean("metrics", "otel_ssl_active", fallback=False)
        if host and port:
            scheme = "https" if ssl_active else "http"
            endpoint = f"{scheme}://{_format_url_host(host)}:{port}/v1/metrics"

    interval_ms: float | None = None
    if not os.environ.get("OTEL_METRIC_EXPORT_INTERVAL") and conf.has_option(
        "metrics", "otel_interval_milliseconds"
    ):
        interval_ms = conf.getfloat("metrics", "otel_interval_milliseconds")

    return endpoint, interval_ms, resource


def _load_exporter_from_env() -> MetricExporter:
    """
    Load a metric exporter using the ``OTEL_METRICS_EXPORTER`` env var.

    Mirrors the entry-point mechanism used by the OTEL SDK auto-instrumentation
    configurator. Supported values (from installed packages):
      - ``otlp`` (default) — OTLP/gRPC
      - ``otlp_proto_http`` — OTLP/HTTP
      - ``console`` — stdout (useful for debugging)
    """
    exporter_name = os.environ.get("OTEL_METRICS_EXPORTER", "otlp")
    eps = entry_points(group="opentelemetry_metrics_exporter", name=exporter_name)
    ep = next(iter(eps), None)
    if ep is None:
        raise RuntimeError(
            f"No metric exporter found for OTEL_METRICS_EXPORTER={exporter_name!r}. "
            f"Available: {[e.name for e in entry_points(group='opentelemetry_metrics_exporter')]}"
        )
    return ep.load()()


def configure_otel(conf: ConfigParser) -> SafeOtelLogger | None:
    """
    Configure the OpenTelemetry metrics pipeline from Airflow conf.

    Mirrors ``airflow_shared.observability.traces.configure_otel``: a single
    conf-driven entry point that bridges deprecated Airflow-specific options
    into the standard OTel environment variables, loads the exporter via the
    SDK's entry-point mechanism, and installs the global meter provider.

    Returns the user-facing :class:`SafeOtelLogger` so callers (Stats) can
    wrap their metrics. Returns ``None`` when ``metrics.otel_on`` is false.
    """
    otel_on = conf.getboolean("metrics", "otel_on", fallback=False)
    if not otel_on:
        return None

    # Ideally all three are None here.
    # They would only be something other than None if the user is still using
    # the deprecated Airflow-defined otel configs.
    backcompat_endpoint, backcompat_interval_ms, resource = _get_backcompat_config(conf)

    # Backcompat: bridge deprecated configs into the OTel env vars so the
    # exporter (loaded below via entry points) picks them up automatically.
    if backcompat_endpoint and not (
        os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
    ):
        os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = backcompat_endpoint

    if backcompat_interval_ms is not None and not os.environ.get("OTEL_METRIC_EXPORT_INTERVAL"):
        os.environ["OTEL_METRIC_EXPORT_INTERVAL"] = str(int(backcompat_interval_ms))

    interval_str = os.environ.get("OTEL_METRIC_EXPORT_INTERVAL")
    interval_ms = float(interval_str) if interval_str else None

    debug = conf.getboolean("metrics", "otel_debugging_on", fallback=False) or (
        os.environ.get("OTEL_METRICS_EXPORTER") == "console"
    )

    readers: list[PeriodicExportingMetricReader] = [
        PeriodicExportingMetricReader(
            exporter=_load_exporter_from_env(),  # type: ignore[arg-type]
            export_interval_millis=interval_ms,  # type: ignore[arg-type]
        )
    ]
    if debug:
        readers.append(
            PeriodicExportingMetricReader(
                ConsoleMetricExporter(),
                export_interval_millis=interval_ms,  # type: ignore[arg-type]
            )
        )

    # Reset the OTel SDK's Once() guard so set_meter_provider() can succeed.
    # This is necessary when configure_otel() is called after a process fork:
    # the parent's _METER_PROVIDER_SET_ONCE._done = True is inherited by the
    # child, causing set_meter_provider() to silently fail with "Overriding of
    # current MeterProvider is not allowed". The child then uses the parent's
    # stale provider whose PeriodicExportingMetricReader thread is dead after
    # fork. On first call (no fork), _done is already False so this is a
    # no-op. See: https://github.com/apache/airflow/issues/64690
    try:
        import opentelemetry.metrics._internal as _metrics_internal

        _metrics_internal._METER_PROVIDER_SET_ONCE._done = False
        _metrics_internal._METER_PROVIDER = None
    except (ImportError, AttributeError):
        pass

    metrics.set_meter_provider(
        MeterProvider(
            resource=resource,
            metric_readers=readers,
            views=[
                View(
                    instrument_type=metrics.Histogram,
                    aggregation=ExponentialBucketHistogramAggregation(),
                )
            ],
            shutdown_on_exit=False,
        ),
    )

    # Register a hook that flushes any in-memory metrics at shutdown.
    atexit_register_metrics_flush()

    # ``getimport`` is an Airflow ``AirflowConfigParser`` extension; plain
    # ``ConfigParser`` instances used in tests don't have it. Fall back to
    # ``None`` so test fixtures that pass a plain ``ConfigParser`` still work.
    stat_name_handler = None
    if hasattr(conf, "getimport"):
        stat_name_handler = conf.getimport("metrics", "stat_name_handler", fallback=None)

    return SafeOtelLogger(
        metrics.get_meter_provider(),
        conf.get("metrics", "otel_prefix", fallback=DEFAULT_METRIC_NAME_PREFIX),
        get_validator(
            conf.get("metrics", "metrics_allow_list", fallback=None),
            conf.get("metrics", "metrics_block_list", fallback=None),
        ),
        stat_name_handler,
        conf.getboolean("metrics", "statsd_influxdb_enabled", fallback=False),
    )

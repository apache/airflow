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

import inspect
import logging
import os
import random
from typing import TYPE_CHECKING, Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import HOST_NAME, SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace import NonRecordingSpan, TraceFlags

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.metrics.otel_logger import SafeOtelLogger
from airflow.providers.opentelemetry.models import (
    EMPTY_SPAN,
    EMPTY_TIMER,
)
from airflow.providers.opentelemetry.util import (
    gen_span_id,
    gen_trace_id,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname

if TYPE_CHECKING:
    from opentelemetry.trace import Span, Tracer
    from opentelemetry.util.types import Attributes

    from airflow.metrics.protocols import DeltaType, TimerProtocol
    from airflow.models import TaskInstance
    from airflow.providers.opentelemetry.models import EmptySpan

log = logging.getLogger(__name__)


def is_otel_traces_enabled() -> bool:
    """Check whether either core otel traces is enabled."""
    return conf.has_option("traces", "otel_on") and conf.getboolean("traces", "otel_on") is True


def is_otel_metrics_enabled() -> bool:
    """Check whether either core otel metrics is enabled."""
    return conf.has_option("metrics", "otel_on") and conf.getboolean("metrics", "otel_on") is True


def is_listener_enabled() -> bool:
    """Check whether otel listener is disabled."""
    return is_otel_traces_enabled() is not True and os.getenv("OTEL_LISTENER_DISABLED", "false").lower() == "false"


OTEL_CONN_ID = "OTEL_CONN_ID"
DEFAULT_SERVICE_NAME = "Airflow"


class AirflowOtelIdGenerator(IdGenerator):
    """
    ID Generator for span id and trace id.

    The specific purpose of this ID generator is to generate a given span_id when the
    generate_span_id is called for the FIRST time. Any subsequent calls to the generate_span_id()
    will then fall back into producing random ones. As for the trace_id, the class is designed
    to produce the provided trace id (and not anything random)
    """

    def __init__(self, span_id=None, trace_id=None):
        super().__init__()
        self.span_id = span_id
        self.trace_id = trace_id

    def generate_span_id(self) -> int:
        if self.span_id is not None:
            id = self.span_id
            self.span_id = None
            return id
        else:
            new_id = random.getrandbits(64)
            return new_id

    def generate_trace_id(self) -> int:
        if self.trace_id is not None:
            id = self.trace_id
            return id
        else:
            new_id = random.getrandbits(128)
            return new_id


class OtelHook(BaseHook, LoggingMixin):
    """
    Uses OpenTelemetry API to send metrics, traces to the OpenTelemetry endpoint.

    :param otel_conn_id: The connection to Otel endpoint, containing metadata for api keys.
    """

    conn_name_attr = "otel_conn_id"
    default_conn_name = "otel_default"
    conn_type = "opentelemetry"
    hook_name = "OtelHook"

    def __init__(self, otel_conn_id: str = "otel_default", *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.otel_conn_id = otel_conn_id
        self.ready = True
        # if the otel core already has traces.otel_on, then we do not have to
        # initialize the new tracer provider.
        # this initialization will only kick in when:
        # 1. traces.otel_on does NOT exist or is False
        # 2. valid connection configuration (otel_default, etc.) exists.
        # if otel trace is enabled, the hook automatically uses what's provided from
        # the existing otel config.
        if is_otel_traces_enabled() is False:
            try:
                conn = self._get_conn()
                self.api_key = conn.password
                self.url = conn.host
                self.header_name = conn.login
                self.interval = conn.port
                if self.url is None:
                    raise AirflowException("Please provide valid URL of the OTEL endpoint.")
                self.headers = {"Content-Type": "application/json"}
                if self.api_key is not None and self.header_name is not None:
                    self.headers[self.header_name] = self.api_key
                self.resource = Resource(attributes={HOST_NAME: get_hostname(), SERVICE_NAME: DEFAULT_SERVICE_NAME})
                self.provider = TracerProvider(resource=self.resource)
                self.processor = SimpleSpanProcessor(
                    span_exporter=OTLPSpanExporter(endpoint=f"{self.url}/v1/traces", headers=self.headers)
                )
                self.provider.add_span_processor(self.processor)
                trace.set_tracer_provider(self.provider)

            except Exception:
                self.ready = False
                self.log.debug(
                    "Failed to retrieve connection using %s.",
                    "Please make sure to setup the appropriate OTEL connection in Airflow and specify",
                    "its name in OTEL_CONN_ID env variable",
                    otel_conn_id,
                )
        else:
            protocol = "https" if conf.getboolean("traces", "otel_ssl_active") else "http"
            self.url = f"{protocol}://{conf.get('traces', 'otel_host')}:{conf.get('traces', 'otel_port')}"
            self.headers = {"Content-Type": "application/json"}
            self.resource = Resource(attributes={HOST_NAME: get_hostname(), SERVICE_NAME: conf.get("traces", "otel_service")})
            self.processor = SimpleSpanProcessor(
                span_exporter=OTLPSpanExporter(endpoint=f"{self.url}/v1/traces", headers=self.headers)
            )
            self.provider = TracerProvider(resource=self.resource)
            self.provider.add_span_processor(self.processor)
            trace.set_tracer_provider(self.provider)

        if self.ready is True:
            self.log.debug("OTEL HOOK tracer initialized.")
        
        if is_otel_metrics_enabled() is False:
            try:
                conn = self._get_conn()
                self.api_key = conn.password
                self.url = conn.host
                self.header_name = conn.login
                self.interval = conn.port
                if self.url is None:
                    raise AirflowException("Please provide valid URL of the OTEL endpoint.")
                self.headers = {"Content-Type": "application/json"}
                if self.api_key is not None and self.header_name is not None:
                    self.headers[self.header_name] = self.api_key
                self.resource = Resource(attributes={HOST_NAME: get_hostname(), SERVICE_NAME: DEFAULT_SERVICE_NAME})
                readers = [
                    PeriodicExportingMetricReader(
                        OTLPMetricExporter(endpoint=f"{self.url}/v1/metrics", headers=self.headers),
                        export_interval_millis=int(self.interval),
                    )
                ]
                metrics.set_meter_provider(
                    MeterProvider(resource=self.resource, metric_readers=readers, shutdown_on_exit=False)
                )
            
            except Exception:
                self.ready = False
                self.log.debug(
                    "Failed to retrieve connection using %s.",
                    "Please make sure to setup the appropriate OTEL connection in Airflow and specify",
                    "its name in OTEL_CONN_ID env variable",
                    otel_conn_id,
                )

        if self.ready is True:
            self.metric_logger = SafeOtelLogger(metrics.get_meter_provider(), "airflow")
            self.log.debug("OTEL HOOK metrics initialized.")


    def _get_conn(self):
        conn = self.get_connection(self.otel_conn_id)
        return conn

    def span(self, func):
        """Decorate a function with span."""

        def wrapper(*args, **kwargs):
            func_name = func.__name__

            if self.ready is True:
                tracer = self._get_tracer(library_name="python_function")
                log.info(f"otel_trace_enabled:{is_otel_traces_enabled()} and is_listener_enabled: {is_listener_enabled()}")
                if "task_instance" in kwargs and (
                    is_otel_traces_enabled() is True or is_listener_enabled() is True
                ):
                    task_instance: TaskInstance = kwargs["task_instance"]
                    dag_run = task_instance.dag_run
                    trace_id = gen_trace_id(dag_run=dag_run)
                    span_id = gen_span_id(ti=task_instance)
                    span_ctx = trace.SpanContext(
                        trace_id=int(trace_id, 16),
                        span_id=int(span_id, 16),
                        is_remote=True,
                        trace_flags=TraceFlags(0x01),
                    )
                    ctx = trace.set_span_in_context(NonRecordingSpan(span_ctx))
                    with tracer.start_as_current_span(func_name, context=ctx):
                        return (
                            func(*args, **kwargs) if len(inspect.signature(func).parameters) > 0 else func()
                        )

                else:
                    with tracer.start_as_current_span(func_name):
                        return (
                            func(*args, **kwargs) if len(inspect.signature(func).parameters) > 0 else func()
                        )
            else:
                return func(*args, **kwargs) if len(inspect.signature(func).parameters) > 0 else func()

        return wrapper

    def _get_tracer(
        self,
        library_name: str | None = None,
        library_version: str | None = None,
        schema_url: str | None = None,
        trace_id: int | None = None,
        span_id: int | None = None,
    ) -> Tracer:
        """Get tracer."""
        if self.ready is True:
            if library_name is None:
                _library_name = __name__
            else:
                _library_name = library_name
            if trace_id or span_id:
                tracer_provider = TracerProvider(
                    resource=self.resource,
                    id_generator=AirflowOtelIdGenerator(span_id=span_id, trace_id=trace_id),
                )

                tracer_provider.add_span_processor(self.processor)
                return tracer_provider.get_tracer(
                    instrumenting_module_name=_library_name,
                    instrumenting_library_version=library_version,
                    schema_url=schema_url,
                )
            else:
                return trace.get_tracer(
                    instrumenting_module_name=_library_name,
                    instrumenting_library_version=library_version,
                    schema_url=schema_url,
                )
        raise Exception(
            "OtelHook was unable to get tracer due to it not being ready.",
            "Possible reason is that the connection information is not setup properly.",
            "Please check whether you have an airflow connection by the name %s.",
            self.otel_conn_id,
        )

    def incr(self, stat: str, count: int = 1, rate: float = 1, tags: Attributes = None):
        """Increase a counter by given count."""
        if self.ready is True:
            self.metric_logger.incr(stat=stat, count=count, rate=rate, tags=tags)

    def decr(self, stat: str, count: int = 1, rate: float = 1, tags: Attributes = None):
        """Decrease a counter by given count."""
        if self.ready is True:
            self.metric_logger.decr(stat=stat, count=count, rate=rate, tags=tags)

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
        """Set a reading to a gauge."""
        if self.ready is True:
            self.metric_logger.gauge(
                stat=stat, value=value, rate=rate, delta=delta, tags=tags, back_compat_name=back_compat_name
            )

    def timing(
        self,
        stat: str,
        dt: DeltaType,
        *,
        tags: Attributes = None,
    ) -> None:
        """Start a timing."""
        if self.ready is True:
            self.metric_logger.timing(stat=stat, dt=dt, tags=tags)

    def timer(
        self,
        stat: str | None = None,
        *args,
        tags: Attributes = None,
        **kwargs,
    ) -> TimerProtocol:
        """Return the duration and can be cancelled."""
        if self.ready is True:
            self.metric_logger.timer()
            return self.metric_logger.timer(stat, *args, tags=tags, **kwargs)
        else:
            return EMPTY_TIMER

    def start_span(
        self,
        name: str,
        library_name: str | None = None,
        library_version: str | None = None,
        trace_id: int | None = None,
        span_id: int | None = None,
        *args,
        **kwargs,
    ) -> Span | EmptySpan:
        """Start a span, which is not attached to current trace context."""
        if self.ready is True:
            tracer = self._get_tracer(
                library_name=library_name,
                library_version=library_version,
                trace_id=trace_id,
                span_id=span_id,
            )
            return tracer.start_span(name, *args, **kwargs)
        else:
            return EMPTY_SPAN

    def start_as_current_span(
        self,
        name: str,
        library_name: str | None = None,
        library_version: str | None = None,
        trace_id: int | None = None,
        span_id: int | None = None,
        *args,
        **kwargs,
    ) -> Any[Span]:
        """Start a span, as current span."""
        if self.ready is True:
            tracer = self._get_tracer(
                library_name=library_name,
                library_version=library_version,
                trace_id=trace_id,
                span_id=span_id,
            )
            return tracer.start_as_current_span(name, *args, **kwargs)
        else:
            return EMPTY_SPAN

    def is_ready(self) -> bool:
        """Indicate whether hook is ready or not."""
        return self.ready

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {
                "host": "OTEL endpoint URL",
                "login": "HTTP Header Name for API Key",
                "password": "API Key",
                "port": "Export interval in ms",
            },
            "placeholders": {
                "host": "http://<host>:<port>",
                "login": "(Optional) HTTP header name",
                "password": "(Optional) API key",
                "port": "5000",
            },
        }

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

from typing import TYPE_CHECKING

import structlog

from .otel_env_config import OtelDataType, OtelEnvConfig

if TYPE_CHECKING:
    from opentelemetry.sdk.metrics._internal.export import MetricExporter
    from opentelemetry.sdk.trace.export import SpanExporter

log = structlog.getLogger(__name__)


def get_otel_data_exporter(
    *,
    otel_env_config: OtelEnvConfig,
    host: str | None = None,
    port: int | None = None,
    ssl_active: bool = False,
) -> SpanExporter | MetricExporter:
    protocol = "https" if ssl_active else "http"

    # According to the OpenTelemetry Spec, specific config options like 'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT'
    # take precedence over generic ones like 'OTEL_EXPORTER_OTLP_ENDPOINT'.
    env_exporter_protocol = (
        otel_env_config.type_specific_exporter_protocol or otel_env_config.exporter_protocol
    )
    env_endpoint = otel_env_config.type_specific_endpoint or otel_env_config.base_endpoint

    # If the protocol env var isn't set, then it will be None,
    # and it will default to an http/protobuf exporter.
    if env_endpoint and env_exporter_protocol == "grpc":
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    else:
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    if env_endpoint:
        if host is not None and port is not None:
            log.warning(
                "Both the standard OpenTelemetry environment variables and "
                "the Airflow OpenTelemetry configs have been provided. "
                "Using the OpenTelemetry environment variables. "
                "The Airflow configs have been deprecated and will be removed in the future."
            )

        endpoint_str = env_endpoint
        # The SDK will pick up all the values from the environment.
        if otel_env_config.data_type == OtelDataType.TRACES:
            exporter = OTLPSpanExporter()
        else:
            exporter = OTLPMetricExporter()
    else:
        if host is None or port is None:
            # Since the configs have been deprecated, host and port could be None.
            # Log a warning to steer the user towards configuring the environment variables
            # and deliberately let it fail here without providing fallbacks.
            log.warning(
                "OpenTelemetry %s have been enabled but the endpoint settings haven't been configured. "
                "The Airflow configs have been deprecated and will be removed in the future. "
                "Configure the standard OpenTelemetry environment variables instead. "
                "For more info, check the docs.",
                otel_env_config.data_type.value,
            )
        else:
            log.warning(
                "The Airflow OpenTelemetry configs have been deprecated and will be removed in the future. "
                "OpenTelemetry is advised to be configured using the standard environment variables. "
                "For more info, check the docs."
            )
        # If the environment endpoint isn't set, then assume that the airflow config is used
        # where protocol isn't specified, and it's always http/protobuf.
        # In that case it should default to the full 'url_path' and set it directly.

        endpoint_suffix = "traces" if otel_env_config.data_type == OtelDataType.TRACES else "metrics"

        endpoint_str = f"{protocol}://{host}:{port}/v1/{endpoint_suffix}"
        if otel_env_config.data_type == OtelDataType.TRACES:
            exporter = OTLPSpanExporter(endpoint=endpoint_str)
        else:
            exporter = OTLPMetricExporter(endpoint=endpoint_str)

    exporter_name = (
        "OTLPSpanExporter" if otel_env_config.data_type == OtelDataType.TRACES else "OTLPMetricExporter"
    )

    log.info("[%s] Connecting to OpenTelemetry Collector at %s", exporter_name, endpoint_str)

    return exporter

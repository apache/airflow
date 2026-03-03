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

import os
from dataclasses import dataclass
from enum import Enum

import structlog

log = structlog.getLogger(__name__)


def _parse_kv_str_to_dict(str_var: str | None) -> dict[str, str]:
    """
    Convert a string of key-value pairs to a dictionary.

    Environment variables like 'OTEL_RESOURCE_ATTRIBUTES' or 'OTEL_EXPORTER_OTLP_HEADERS'
    accept values with the format "key1=value1,key2=value2,..."
    """
    configs = {}
    if str_var:
        for pair in str_var.split(","):
            if "=" in pair:
                k, v = pair.split("=", 1)
                configs[k.strip()] = v.strip()
    return configs


class OtelDataType(str, Enum):
    """Enum with the different telemetry data types."""

    TRACES = "traces"
    METRICS = "metrics"


@dataclass(frozen=True)
class OtelEnvConfig:
    """Immutable class for holding OTel config environment variables."""

    data_type: OtelDataType  # traces | metrics
    base_endpoint: str | None  # base url
    type_specific_endpoint: str | None  # traces | metrics specific url
    exporter_protocol: str | None  # "grpc" | "http/protobuf"
    type_specific_exporter_protocol: str | None  # traces | metrics specific protocol
    exporter: str | None  # OTEL_TRACES_EXPORTER | OTEL_METRICS_EXPORTER
    service_name: str | None
    headers_kv_str: str | None
    headers: dict[str, str]
    resource_attributes_kv_str: str | None
    resource_attributes: dict[str, str]
    interval_ms: float | None


def load_otel_env_config(data_type: OtelDataType) -> OtelEnvConfig:
    """Read OTel config env vars and return an OtelEnvConfig object."""
    exporter_protocol = os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL")
    service_name = os.getenv("OTEL_SERVICE_NAME")
    headers_kv_str = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")
    resource_attributes_kv_str = os.getenv("OTEL_RESOURCE_ATTRIBUTES")
    base_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

    if data_type == OtelDataType.TRACES:
        type_specific_endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        type_specific_exporter_protocol = os.getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
        exporter = os.getenv("OTEL_TRACES_EXPORTER")
        interval_ms = None
    else:
        type_specific_endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        type_specific_exporter_protocol = os.getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL")
        exporter = os.getenv("OTEL_METRICS_EXPORTER")
        # Instead of directly providing a default value of float,
        # use a value of str and convert to float to get rid of a static-code check error.
        interval = os.getenv("OTEL_METRIC_EXPORT_INTERVAL")
        interval_ms = float(interval) if interval else None

    return OtelEnvConfig(
        data_type=data_type,
        base_endpoint=base_endpoint,
        type_specific_endpoint=type_specific_endpoint,
        exporter_protocol=exporter_protocol,
        type_specific_exporter_protocol=type_specific_exporter_protocol,
        exporter=exporter,
        service_name=service_name,
        headers_kv_str=headers_kv_str,
        headers=_parse_kv_str_to_dict(headers_kv_str),
        resource_attributes_kv_str=resource_attributes_kv_str,
        resource_attributes=_parse_kv_str_to_dict(resource_attributes_kv_str),
        interval_ms=interval_ms,
    )


def load_traces_env_config() -> OtelEnvConfig:
    return load_otel_env_config(OtelDataType.TRACES)


def load_metrics_env_config() -> OtelEnvConfig:
    return load_otel_env_config(OtelDataType.METRICS)

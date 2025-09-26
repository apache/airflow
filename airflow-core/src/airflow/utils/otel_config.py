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

import logging
import os
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache

log = logging.getLogger(__name__)


def _parse_kv_str_to_dict(str_var: str) -> dict[str, str]:
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
    LOGS = "logs"


@dataclass(frozen=True)
class OtelConfig:
    """Immutable class for holding and validating OTel config environment variables."""

    data_type: OtelDataType  # traces | metrics
    endpoint: str  # url
    protocol: str  # "grpc" or "http/protobuf"
    exporter: str  # OTEL_TRACES_EXPORTER | OTEL_METRICS_EXPORTER
    service_name: str  # default "Airflow"
    headers_kv_str: str
    headers: dict[str, str]
    resource_attributes_kv_str: str
    resource_attributes: dict[str, str]
    interval_ms: int

    def __post_init__(self):
        """Validate the environment variables where necessary."""
        endpoint_type_specific = (
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
            if self.data_type == OtelDataType.TRACES
            else "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
        )

        if not self.endpoint:
            raise OSError(
                f"Missing required environment variable: 'OTEL_EXPORTER_OTLP_ENDPOINT' or {endpoint_type_specific}"
            )

        stripped_protocol = (self.protocol or "").strip().strip('"').strip("'").lower()
        if stripped_protocol not in ("grpc", "http/protobuf"):
            raise ValueError(f"Invalid value for OTEL_EXPORTER_OTLP_PROTOCOL: {self.protocol}")

        # If the protocol is http, then the endpoint url should end with '/v1/<traces|metrics>'.
        if stripped_protocol == "http/protobuf":
            suffix = "/v1/traces" if self.data_type == OtelDataType.TRACES else "/v1/metrics"
            if not self.endpoint.rstrip("/").endswith(suffix):
                # No need for a fatal error, the OTel code will fail.
                # Just log an error to help the user understand the issue.
                log.error(
                    "Invalid value for config 'OTEL_EXPORTER_OTLP_ENDPOINT' or '%s' with protocol value '%s': %s",
                    endpoint_type_specific,
                    stripped_protocol,
                    self.endpoint,
                )


def _env_vars_snapshot(data_type: OtelDataType) -> tuple[str | None, ...]:
    """
    Return a tuple of the env values, representing a snapshot.

    If any of the values changes, then the snapshot will be new, and the cache entry will be invalidated.
    """
    # Common.
    common = (
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
        os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
        os.getenv("OTEL_SERVICE_NAME"),
        os.getenv("OTEL_EXPORTER_OTLP_HEADERS"),
        os.getenv("OTEL_RESOURCE_ATTRIBUTES"),
    )

    # Traces.
    if data_type == OtelDataType.TRACES:
        traces_specific = (
            os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
            os.getenv("OTEL_TRACES_EXPORTER"),
        )
        return data_type.value, *traces_specific, *common

    # Metrics.
    metrics_specific = (
        os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
        os.getenv("OTEL_METRICS_EXPORTER"),
        os.getenv("OTEL_METRIC_EXPORT_INTERVAL"),
    )
    return data_type.value, *metrics_specific, *common


@lru_cache(maxsize=3)
def load_otel_config(data_type: OtelDataType, vars_snapshot: tuple | None = None) -> OtelConfig:
    """
    Read and validate OTel config env vars once per unique snapshot.

    `_env_vars_snapshot()` is passed as the argument whenever this function is called.
    If the env changes, then the snapshot changes, and so this recomputes and validates the vars.
    """
    protocol = os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    service_name = os.getenv("OTEL_SERVICE_NAME", "Airflow")
    headers_kv_str = os.getenv("OTEL_EXPORTER_OTLP_HEADERS", "")
    resource_attributes_kv_str = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")

    if data_type == OtelDataType.TRACES:
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        if endpoint is None:
            # If it's still None, give it a default empty value to avoid a static-code check error.
            endpoint = os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") or ""
        exporter = os.getenv("OTEL_TRACES_EXPORTER", "otlp")
        interval_ms = 0
    else:
        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
        if endpoint is None:
            # If it's still None, give it a default empty value to avoid a static-code check error.
            endpoint = os.getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") or ""
        exporter = os.getenv("OTEL_METRICS_EXPORTER", "otlp")
        # Instead of directly providing a default value of int,
        # use a value of str and convert to int to get rid of a static-code check error.
        interval_ms = int(os.getenv("OTEL_METRIC_EXPORT_INTERVAL", "60000"))

    return OtelConfig(
        data_type=data_type,
        endpoint=endpoint,
        protocol=protocol,
        exporter=exporter,
        service_name=service_name,
        headers_kv_str=headers_kv_str,
        headers=_parse_kv_str_to_dict(headers_kv_str),
        resource_attributes_kv_str=resource_attributes_kv_str,
        resource_attributes=_parse_kv_str_to_dict(resource_attributes_kv_str),
        interval_ms=interval_ms,
    )


def load_traces_config() -> OtelConfig:
    return load_otel_config(OtelDataType.TRACES, _env_vars_snapshot(OtelDataType.TRACES))


def load_metrics_config() -> OtelConfig:
    return load_otel_config(OtelDataType.METRICS, _env_vars_snapshot(OtelDataType.METRICS))

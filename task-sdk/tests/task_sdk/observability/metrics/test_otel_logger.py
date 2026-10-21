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
from unittest import mock

import pytest
from opentelemetry.sdk.metrics import Counter, Histogram, MeterProvider
from opentelemetry.sdk.metrics.export import AggregationTemporality, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import SERVICE_INSTANCE_ID

from airflow.sdk.observability.metrics.otel_logger import get_otel_logger

TEMPORALITY_ENV_VAR = "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE"
RESOURCE_ATTRIBUTES = "OTEL_RESOURCE_ATTRIBUTES"
SHARED_ID = "task-worker"

DELTA = AggregationTemporality.DELTA
CUMULATIVE = AggregationTemporality.CUMULATIVE


@pytest.mark.parametrize(
    (
        "short_lived",
        "env_temporality",
        "env_resource_attributes",
        "expected_temporality",
        "expected_instance_id",
    ),
    [
        pytest.param(True, None, None, DELTA, SHARED_ID, id="short_lived_defaults"),
        pytest.param(True, "CUMULATIVE", None, CUMULATIVE, None, id="short_lived_user_cumulative"),
        pytest.param(True, "DELTA", None, DELTA, SHARED_ID, id="short_lived_user_delta"),
        pytest.param(True, "LOWMEMORY", None, DELTA, SHARED_ID, id="short_lived_user_lowmemory"),
        pytest.param(True, "delta", None, DELTA, SHARED_ID, id="short_lived_user_lowercase_delta"),
        pytest.param(True, "FOO", None, CUMULATIVE, None, id="short_lived_user_unrecognized_value"),
        pytest.param(
            True,
            None,
            "service.instance.id=my-id",
            DELTA,
            "my-id",
            id="short_lived_user_instance_id",
        ),
        pytest.param(
            True,
            None,
            "deployment.environment=prod,service.instance.id=my-id",
            DELTA,
            "my-id",
            id="short_lived_user_instance_id_among_other_attributes",
        ),
        pytest.param(
            True,
            "CUMULATIVE",
            "service.instance.id=my-id",
            CUMULATIVE,
            "my-id",
            id="short_lived_user_cumulative_and_instance_id",
        ),
        pytest.param(False, None, None, CUMULATIVE, None, id="long_lived_defaults"),
        pytest.param(False, "DELTA", None, DELTA, None, id="long_lived_user_delta"),
        pytest.param(
            False,
            None,
            "service.instance.id=my-id",
            CUMULATIVE,
            "my-id",
            id="long_lived_user_instance_id",
        ),
    ],
)
def test_temporality_and_service_instance_id(
    short_lived: bool,
    env_temporality: str | None,
    env_resource_attributes: str | None,
    expected_temporality: AggregationTemporality,
    expected_instance_id: str | None,
):
    """The short-lived defaults, delta values and a shared id, apply only where the user left the env unset."""
    env_dict = {
        key: value
        for key, value in os.environ.items()
        if key not in (TEMPORALITY_ENV_VAR, RESOURCE_ATTRIBUTES)
    }
    env_dict["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = "http://collector:4318/v1/metrics"
    if env_temporality:
        env_dict[TEMPORALITY_ENV_VAR] = env_temporality
    if env_resource_attributes:
        env_dict[RESOURCE_ATTRIBUTES] = env_resource_attributes
    # patch.dict restores the whole mapping, so the variable the wrapper adds with setdefault does not leak.
    with mock.patch.dict(os.environ, env_dict, clear=True):
        otel_logger = get_otel_logger(short_lived=short_lived)

    provider = otel_logger.otel
    assert isinstance(provider, MeterProvider)
    reader = provider._metric_readers[0]
    assert isinstance(reader, PeriodicExportingMetricReader)
    assert reader._exporter._preferred_temporality[Counter] == expected_temporality
    assert reader._exporter._preferred_temporality[Histogram] == expected_temporality

    instance_id = provider._sdk_config.resource.attributes[SERVICE_INSTANCE_ID]
    if expected_instance_id is None:
        assert instance_id != SHARED_ID
    else:
        assert instance_id == expected_instance_id

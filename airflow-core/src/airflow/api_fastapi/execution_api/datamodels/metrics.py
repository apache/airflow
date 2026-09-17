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

from enum import Enum

from airflow.api_fastapi.core_api.base import StrictBaseModel


class MetricKind(str, Enum):
    """Type of stats operation of a forwarded metric."""

    COUNTER = "counter"
    GAUGE = "gauge"
    TIMING = "timing"


class ForwardMetric(StrictBaseModel):
    """
    One metric aggregated by the Task SDK.

    Forwarded to a long-lived process like the API server and exported by its stats backend.
    """

    kind: MetricKind
    name: str
    tags: dict[str, str] | None = None
    # Counter: sum of increments (negative for decrements).
    # Gauge: the value, or an increment when delta.
    value: int | float | None = None
    delta: bool = False
    # Timing: every observation, in milliseconds, so the backend's histogram gets all of them.
    values: list[float] | None = None


class ForwardMetricsBody(StrictBaseModel):
    """Metrics the Task SDK accumulated since its previous batch."""

    metrics: list[ForwardMetric]

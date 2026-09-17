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

from fastapi import APIRouter, status

from airflow._shared.observability.metrics import stats
from airflow.api_fastapi.execution_api.datamodels.metrics import ForwardMetricsBody, MetricKind

router = APIRouter()


@router.post(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Unauthorized"}},
)
def forward_metrics(body: ForwardMetricsBody) -> None:
    """Export metrics forwarded by the Task SDK."""
    # Use the configured backend directly instead of the ``stats`` module. The Task SDK is
    # configured to use a forwarding logger but the actual metric calls are all routed through the
    # ``stats`` module functions. ``stats.X`` runs ``backend.X`` once for the new name and once
    # for the legacy name. The ``backend`` is the forwarding logger that aggregates everything in
    # memory and therefore the metrics list below holds both the new name and the legacy name.
    backend = stats._get_backend()
    for metric in body.metrics:
        if metric.kind == MetricKind.COUNTER:
            count = int(metric.value or 0)
            if count > 0:
                backend.incr(metric.name, count, tags=metric.tags)
            elif count < 0:
                backend.decr(metric.name, -count, tags=metric.tags)
        elif metric.kind == MetricKind.GAUGE:
            backend.gauge(metric.name, float(metric.value or 0), delta=metric.delta, tags=metric.tags)
        else:
            for value in metric.values or ():
                backend.timing(metric.name, value, tags=metric.tags)

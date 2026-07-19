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

from unittest import mock

from airflow.providers.common.compat.sdk import Stats
from airflow.providers.edge3.models.edge_worker import EdgeWorkerState, set_metrics

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

stats_reference = f"{Stats.__module__}.Stats"


def test_set_metrics():
    worker_name = "test_worker1"
    if AIRFLOW_V_3_3_PLUS:
        with mock.patch("airflow.sdk._shared.observability.metrics.stats._get_backend") as mock_get_backend:
            mock_backend = mock.MagicMock()
            mock_get_backend.return_value = mock_backend

            set_metrics(
                worker_name=worker_name,
                state=EdgeWorkerState.IDLE,
                jobs_active=0,
                concurrency=1,
                free_concurrency=1,
                queues=None,
                sysinfo={"status": 1},
            )

            metric_names = [call.args[0] for call in mock_backend.gauge.call_args_list]
    else:
        with mock.patch(f"{stats_reference}.gauge") as mock_gauge:
            set_metrics(
                worker_name=worker_name,
                state=EdgeWorkerState.IDLE,
                jobs_active=0,
                concurrency=1,
                free_concurrency=1,
                queues=None,
                sysinfo={"status": 1},
            )

            metric_names = [call.args[0] for call in mock_gauge.call_args_list]

    assert "edge_worker.status" in metric_names

    legacy_metric_name = f"edge_worker.status.{worker_name}"
    assert legacy_metric_name in metric_names

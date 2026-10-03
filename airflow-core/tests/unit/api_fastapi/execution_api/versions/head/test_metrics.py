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

import pytest

from airflow._shared.observability.metrics.base_stats_logger import StatsLogger

pytestmark = pytest.mark.db_test


class TestForwardMetrics:
    @pytest.mark.parametrize(
        ("metric", "expected_calls"),
        [
            pytest.param(
                {"kind": "counter", "name": "ti_successes", "tags": {"dag_id": "dag"}, "value": 2},
                [mock.call.incr("ti_successes", 2, tags={"dag_id": "dag"})],
                id="counter",
            ),
            pytest.param(
                {"kind": "counter", "name": "pool.running_slots", "value": -3},
                [mock.call.decr("pool.running_slots", 3, tags=None)],
                id="negative_counter",
            ),
            pytest.param(
                {"kind": "counter", "name": "ti_successes", "value": 0},
                [],
                id="zero_counter",
            ),
            pytest.param(
                {"kind": "gauge", "name": "pool.open_slots", "value": 4.5, "delta": True},
                [mock.call.gauge("pool.open_slots", 4.5, delta=True, tags=None)],
                id="gauge",
            ),
            pytest.param(
                {"kind": "timing", "name": "task.duration", "tags": {"task_id": "t"}, "values": [12.0, 30.0]},
                [
                    mock.call.timing("task.duration", 12.0, tags={"task_id": "t"}),
                    mock.call.timing("task.duration", 30.0, tags={"task_id": "t"}),
                ],
                id="timing",
            ),
        ],
    )
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_metrics_are_replayed_into_the_backend(self, mock_get_backend, client, metric, expected_calls):
        backend = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = backend

        response = client.post("/execution/metrics", json={"metrics": [metric]})

        assert response.status_code == 204
        assert backend.mock_calls == expected_calls

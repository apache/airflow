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

from airflow.exceptions import DagRunNotFound

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize(
    ("version", "expected_status"),
    [
        pytest.param("2026-06-30", 500, id="previous-version"),
        pytest.param("2026-10-30", 404, id="new-version"),
    ],
)
@mock.patch("airflow.api_fastapi.execution_api.routes.task_state_store.get_state_backend", autospec=True)
def test_missing_dagrun_response_by_version(
    mock_get_backend, client, create_task_instance, monkeypatch, version, expected_status
):
    ti = create_task_instance()
    mock_get_backend.return_value.set.side_effect = DagRunNotFound("DagRun not found")
    client.headers["Airflow-API-Version"] = version
    monkeypatch.setattr(client._transport, "raise_server_exceptions", False)

    response = client.put(f"/execution/store/ti/{ti.id}/job_id", json={"value": "spark_001"})

    assert response.status_code == expected_status
    if expected_status == 404:
        assert response.json()["detail"] == {
            "reason": "not_found",
            "message": f"DagRun with dag_id={ti.dag_id} and run_id={ti.run_id} not found",
        }

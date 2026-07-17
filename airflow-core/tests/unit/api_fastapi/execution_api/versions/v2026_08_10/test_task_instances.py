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

import pytest

from airflow.utils.state import State

from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_logs()
    clear_db_runs()
    clear_db_serialized_dags()
    clear_db_dags()
    clear_db_assets()
    yield
    clear_db_logs()
    clear_db_runs()
    clear_db_serialized_dags()
    clear_db_dags()
    clear_db_assets()


def test_task_run_state_conflict_error_uses_legacy_shape_for_older_versions(
    client, session, create_task_instance
):
    client.headers["Airflow-API-Version"] = "2026-06-30"
    ti = create_task_instance(task_id="test_ti_run_state_conflict_legacy_error", state=State.SUCCESS)
    session.commit()

    response = client.patch(
        f"/execution/task-instances/{ti.id}/run",
        json={
            "state": "running",
            "hostname": "random-hostname",
            "unixname": "random-unixname",
            "pid": 100,
            "start_date": "2024-10-31T12:00:00Z",
        },
    )

    assert response.status_code == 409
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {
        "detail": {
            "message": "TI was not in a state where it could be marked as running",
            "previous_state": State.SUCCESS,
            "reason": "invalid_state",
        }
    }

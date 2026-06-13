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

from uuid import uuid4

import pytest

from airflow._shared.timezones import timezone
from airflow.models import TaskReschedule
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


@pytest.fixture
def old_ver_client(client):
    """Last released execution API before first_task_reschedule_start_date was added."""
    client.headers["Airflow-API-Version"] = "2026-06-16"
    return client


@pytest.fixture(autouse=True)
def setup_teardown():
    clear_db_runs()
    clear_db_serialized_dags()
    clear_db_dags()
    yield
    clear_db_runs()
    clear_db_serialized_dags()
    clear_db_dags()


def test_first_task_reschedule_start_date_removed_from_previous_version(
    old_ver_client,
    session,
    create_task_instance,
):
    ti = create_task_instance(
        task_id="test_first_task_reschedule_start_date_removed_from_previous_version",
        state=State.QUEUED,
        dagrun_state=DagRunState.RUNNING,
        session=session,
        start_date=timezone.datetime(2024, 9, 30, 12),
        dag_id=str(uuid4()),
    )
    session.add(
        TaskReschedule(
            ti_id=ti.id,
            start_date=timezone.datetime(2024, 9, 30, 10),
            end_date=timezone.datetime(2024, 9, 30, 10, 1),
            reschedule_date=timezone.datetime(2024, 9, 30, 10, 2),
        )
    )
    session.commit()

    response = old_ver_client.patch(
        f"/execution/task-instances/{ti.id}/run",
        json={
            "state": "running",
            "hostname": "random-hostname",
            "unixname": "random-unixname",
            "pid": 100,
            "start_date": "2024-09-30T12:00:00Z",
        },
    )

    assert response.status_code == 200
    result = response.json()
    assert result["task_reschedule_count"] == 1
    assert "first_task_reschedule_start_date" not in result

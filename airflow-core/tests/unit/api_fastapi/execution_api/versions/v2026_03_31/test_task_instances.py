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

from airflow._shared.timezones import timezone
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"
TIMESTAMP = timezone.parse(TIMESTAMP_STR)

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "test-hostname",
    "unixname": "test-user",
    "pid": 12345,
    "start_date": TIMESTAMP_STR,
}


@pytest.fixture
def old_ver_client(client):
    """Client configured to use API version before start_date nullable change."""
    client.headers["Airflow-API-Version"] = "2025-12-08"
    return client


class TestDagRunStartDateNullableBackwardCompat:
    """Test that older API versions get a non-null start_date fallback."""

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_gets_run_after_when_start_date_is_null(
        self,
        old_ver_client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_nullable",
            state=State.QUEUED,
            dagrun_state=DagRunState.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.start_date = None  # DagRun has not started yet
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is not None
        assert dag_run["start_date"] == dag_run["run_after"]

    def test_head_version_allows_null_start_date(
        self,
        client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_null_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.QUEUED,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.start_date = None  # DagRun has not started yet
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is None

    def test_old_version_preserves_real_start_date(
        self,
        old_ver_client,
        session,
        create_task_instance,
    ):
        ti = create_task_instance(
            task_id="test_start_date_preserved",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        assert ti.dag_run.start_date == TIMESTAMP  # DagRun has already started
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        dag_run = response.json()["dag_run"]

        assert response.status_code == 200
        assert dag_run["start_date"] is not None, "start_date should not be None when DagRun has started"
        assert dag_run["start_date"] == TIMESTAMP.isoformat().replace("+00:00", "Z")

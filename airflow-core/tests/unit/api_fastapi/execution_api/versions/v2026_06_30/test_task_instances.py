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
    "hostname": "h",
    "unixname": "u",
    "pid": 1,
    "start_date": TIMESTAMP_STR,
}


@pytest.fixture
def old_ver_client(client):
    """Execution API version immediately before ``queued_dttm`` was added."""
    client.headers["Airflow-API-Version"] = "2026-06-16"
    return client


class TestQueuedDttmFieldBackwardCompat:
    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_strips_queued_dttm(self, old_ver_client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_queued_dttm_downgrade",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.queued_dttm = TIMESTAMP
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert "queued_dttm" not in response.json()

    def test_head_version_includes_queued_dttm_when_set(self, client, session, create_task_instance):
        queued_at = timezone.parse("2024-09-30T11:55:00Z")
        ti = create_task_instance(
            task_id="test_queued_dttm_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.queued_dttm = queued_at
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert response.json()["queued_dttm"] == "2024-09-30T11:55:00Z"

    def test_head_version_omits_queued_dttm_when_not_set(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_queued_dttm_head_null",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.queued_dttm = None
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)

        assert response.status_code == 200
        assert "queued_dttm" not in response.json()

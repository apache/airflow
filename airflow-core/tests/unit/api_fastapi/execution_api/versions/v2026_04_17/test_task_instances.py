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

from tests_common.test_utils.config import conf_vars
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
    """Execution API version immediately before ``team_name`` was added."""
    client.headers["Airflow-API-Version"] = "2026-04-06"
    return client


class TestTeamNameFieldBackwardCompat:
    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_strips_team_name_even_when_set(self, old_ver_client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_team_name_downgrade",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        session.commit()
        with conf_vars({("core", "multi_team"): "False"}):
            response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert "team_name" not in response.json()["dag_run"]

    def test_head_version_includes_team_name_field(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_team_name_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        session.commit()
        with conf_vars({("core", "multi_team"): "False"}):
            response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        assert response.json()["dag_run"]["team_name"] is None

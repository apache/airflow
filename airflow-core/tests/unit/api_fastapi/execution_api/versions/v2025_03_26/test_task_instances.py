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

from airflow.utils import timezone
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_assets, clear_db_runs

pytestmark = pytest.mark.db_test


DEFAULT_START_DATE = timezone.parse("2024-10-31T11:00:00Z")
DEFAULT_END_DATE = timezone.parse("2024-10-31T12:00:00Z")


@pytest.fixture
def ver_client(client):
    client.headers["Airflow-API-Version"] = "2025-03-26"
    return client


class TestTIUpdateState:
    def setup_method(self):
        clear_db_assets()
        clear_db_runs()

    def teardown_method(self):
        clear_db_assets()
        clear_db_runs()

    def test_ti_run(self, ver_client, session, create_task_instance, time_machine):
        """
        Test that this version of the endpoint works.

        Later versions add a consumed_asset_events field.
        """
        instant_str = "2024-09-30T12:00:00Z"
        instant = timezone.parse(instant_str)
        time_machine.move_to(instant, tick=False)

        ti = create_task_instance(
            task_id="test_ti_run_state_to_running",
            state=State.QUEUED,
            session=session,
            start_date=instant,
        )
        session.commit()

        response = ver_client.patch(
            f"/execution/task-instances/{ti.id}/run",
            json={
                "state": "running",
                "hostname": "random-hostname",
                "unixname": "random-unixname",
                "pid": 100,
                "start_date": instant_str,
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "dag_run": {
                "dag_id": "dag",
                "run_id": "test",
                "clear_number": 0,
                "logical_date": instant_str,
                "data_interval_start": instant.subtract(days=1).to_iso8601_string(),
                "data_interval_end": instant_str,
                "run_after": instant_str,
                "start_date": instant_str,
                "end_date": None,
                "run_type": "manual",
                "conf": {},
            },
            "task_reschedule_count": 0,
            "max_tries": 0,
            "should_retry": False,
            "variables": [],
            "connections": [],
            "xcom_keys_to_clear": [],
        }

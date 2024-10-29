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

from datetime import datetime, timezone

import pytest

from airflow.models.log import Log
from airflow.utils.session import provide_session

from tests_common.test_utils.db import clear_db_logs, clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "TEST_DAG_ID"
DAG_RUN_ID = "TEST_DAG_RUN_ID"
TASK_ID = "TEST_TASK_ID"
DAG_EXECUTION_DATE = datetime(2024, 6, 15, 0, 0, tzinfo=timezone.utc)
OWNER = "TEST_OWNER"
OWNER_DISPLAY_NAME = "Test Owner"
OWNER_AIRFLOW = "airflow"
TASK_INSTANCE_EVENT = "TASK_INSTANCE_EVENT"
TASK_INSTANCE_OWNER = "TASK_INSTANCE_OWNER"
TASK_INSTANCE_OWNER_DISPLAY_NAME = "Task Instance Owner"


EVENT_NORMAL = "NORMAL_EVENT"
EVENT_WITH_OWNER = "EVENT_WITH_OWNER"
EVENT_WITH_TASK_INSTANCE = "EVENT_WITH_TASK_INSTANCE"
EVENT_WITH_OWNER_AND_TASK_INSTANCE = "EVENT_WITH_OWNER_AND_TASK_INSTANCE"
EVENT_NON_EXISTED_ID = 9999


class TestEventLogsEndpoint:
    """Common class for /public/eventLogs related unit tests."""

    @staticmethod
    def _clear_db():
        clear_db_logs()
        clear_db_runs()

    @pytest.fixture(autouse=True)
    @provide_session
    def setup(self, create_task_instance, session=None) -> dict[str, Log]:
        """
        Setup event logs for testing.
        :return: Dictionary with event log keys and their corresponding IDs.
        """
        self._clear_db()
        # create task instances for testing
        task_instance = create_task_instance(
            session=session,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            run_id=DAG_RUN_ID,
            execution_date=DAG_EXECUTION_DATE,
        )
        normal_log = Log(
            event=EVENT_NORMAL,
        )
        log_with_owner = Log(
            event=EVENT_WITH_OWNER,
            owner=OWNER,
            owner_display_name=OWNER_DISPLAY_NAME,
        )
        log_with_task_instance = Log(
            event=TASK_INSTANCE_EVENT,
            task_instance=task_instance,
        )
        log_with_owner_and_task_instance = Log(
            event=EVENT_WITH_OWNER_AND_TASK_INSTANCE,
            owner=OWNER,
            owner_display_name=OWNER_DISPLAY_NAME,
            task_instance=task_instance,
        )
        session.add_all(
            [normal_log, log_with_owner, log_with_task_instance, log_with_owner_and_task_instance]
        )
        session.commit()
        return {
            EVENT_NORMAL: normal_log,
            EVENT_WITH_OWNER: log_with_owner,
            TASK_INSTANCE_EVENT: log_with_task_instance,
            EVENT_WITH_OWNER_AND_TASK_INSTANCE: log_with_owner_and_task_instance,
        }

    def teardown_method(self) -> None:
        self._clear_db()


class TestGetEventLog(TestEventLogsEndpoint):
    @pytest.mark.parametrize(
        "event_log_key, expected_status_code, expected_body",
        [
            (
                EVENT_NORMAL,
                200,
                {
                    "event": EVENT_NORMAL,
                },
            ),
            (
                EVENT_WITH_OWNER,
                200,
                {
                    "event": EVENT_WITH_OWNER,
                    "owner": OWNER,
                },
            ),
            (
                TASK_INSTANCE_EVENT,
                200,
                {
                    "dag_id": DAG_ID,
                    "event": TASK_INSTANCE_EVENT,
                    "map_index": -1,
                    "owner": OWNER_AIRFLOW,
                    "run_id": DAG_RUN_ID,
                    "task_id": TASK_ID,
                },
            ),
            (
                EVENT_WITH_OWNER_AND_TASK_INSTANCE,
                200,
                {
                    "dag_id": DAG_ID,
                    "event": EVENT_WITH_OWNER_AND_TASK_INSTANCE,
                    "map_index": -1,
                    "owner": OWNER,
                    "run_id": DAG_RUN_ID,
                    "task_id": TASK_ID,
                    "try_number": 0,
                },
            ),
            ("not_existed_event_log_key", 404, {}),
        ],
    )
    def test_get_event_log(self, test_client, setup, event_log_key, expected_status_code, expected_body):
        event_log: Log | None = setup.get(event_log_key, None)
        event_log_id = event_log.id if event_log else EVENT_NON_EXISTED_ID
        response = test_client.get(f"/public/eventLogs/{event_log_id}")
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        expected_json = {
            "event_log_id": event_log_id,
            "when": event_log.dttm.isoformat().replace("+00:00", "Z") if event_log.dttm else None,
            "dag_id": expected_body.get("dag_id"),
            "task_id": expected_body.get("task_id"),
            "run_id": expected_body.get("run_id"),
            "map_index": event_log.map_index,
            "try_number": event_log.try_number,
            "event": expected_body.get("event"),
            "logical_date": event_log.execution_date.isoformat().replace("+00:00", "Z")
            if event_log.execution_date
            else None,
            "owner": expected_body.get("owner"),
            "extra": expected_body.get("extra"),
        }

        assert response.json() == expected_json

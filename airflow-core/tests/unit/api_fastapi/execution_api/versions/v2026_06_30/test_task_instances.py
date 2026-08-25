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
from airflow.models.task_instance_launch import TaskInstanceLaunch, TaskInstanceLaunchState
from airflow.utils.state import DagRunState, State

from tests_common.test_utils.db import clear_db_runs, clear_db_task_instance_launches

pytestmark = pytest.mark.db_test

TIMESTAMP_STR = "2024-09-30T12:00:00Z"
TIMESTAMP = timezone.parse(TIMESTAMP_STR)
PARTITION_DATE = timezone.parse("2026-05-20T01:00:00")

RUN_PATCH_BODY = {
    "state": "running",
    "hostname": "h",
    "unixname": "u",
    "pid": 1,
    "start_date": TIMESTAMP_STR,
}


@pytest.fixture
def old_ver_client(client):
    """Execution API version immediately before ``partition_date`` was added."""
    client.headers["Airflow-API-Version"] = "2026-06-16"
    return client


class TestPartitionDateFieldBackwardCompat:
    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_old_version_strips_partition_date_from_dag_run(
        self, old_ver_client, session, create_task_instance
    ):
        ti = create_task_instance(
            task_id="test_partition_date_downgrade",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.partition_key = "2026-05-20"
        ti.dag_run.partition_date = PARTITION_DATE
        session.commit()

        response = old_ver_client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        dag_run = response.json()["dag_run"]
        assert dag_run["partition_key"] == "2026-05-20"
        assert "partition_date" not in dag_run

    def test_head_version_includes_partition_date_field(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_partition_date_head",
            state=State.QUEUED,
            dagrun_state=DagRunState.RUNNING,
            session=session,
            start_date=TIMESTAMP,
        )
        ti.dag_run.partition_key = "2026-05-20"
        ti.dag_run.partition_date = PARTITION_DATE
        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/run", json=RUN_PATCH_BODY)
        assert response.status_code == 200
        dag_run = response.json()["dag_run"]
        assert dag_run["partition_key"] == "2026-05-20"
        assert dag_run["partition_date"] == PARTITION_DATE.isoformat().replace("+00:00", "Z")


class TestExternalExecutorIdFieldBackwardCompat:
    """Durable-launch fencing (typed 409) is opt-in via the ``2026-06-30`` version.

    ``external_executor_id`` was added to ``TIEnterRunningPayload`` in ``2026-06-30``
    (``AddTaskInstanceExternalExecutorIdField``). A Task SDK pinned to an earlier Execution
    API version has no such field in its request schema, so it never carries a launch token;
    with no token the ``/run`` fencing branch is skipped and the client keeps observing the
    legacy ``404`` for a missing task instance, never the new ``409 stale_executor_launch``.
    A head-version client that does carry the token gets the typed ``409``.
    """

    @pytest.fixture(autouse=True)
    def _freeze_time(self, time_machine):
        time_machine.move_to(TIMESTAMP_STR, tick=False)

    def setup_method(self):
        clear_db_runs()
        clear_db_task_instance_launches()

    def teardown_method(self):
        clear_db_runs()
        clear_db_task_instance_launches()

    def _add_terminal_launch(self, session, token):
        session.add(
            TaskInstanceLaunch(
                token=token,
                task_instance_id="deleted-ti",
                dag_id="dag",
                task_id="task",
                run_id="run",
                map_index=-1,
                try_number=1,
                executor="executor",
                state=TaskInstanceLaunchState.SUPERSEDED,
            )
        )
        session.commit()

    def test_old_version_client_without_token_returns_404_for_stale_launch(self, old_ver_client, session):
        """Old client cannot send a token (field absent from its schema) -> plain 404, never 409."""
        self._add_terminal_launch(session, "stale-token")

        # A genuine pre-2026-06-30 client has no ``external_executor_id`` field, so it omits it.
        response = old_ver_client.patch(
            f"/execution/task-instances/{uuid4()}/run",
            json=RUN_PATCH_BODY,
        )

        assert response.status_code == 404
        assert response.json()["detail"]["reason"] == "not_found"

    def test_head_version_returns_409_for_stale_launch(self, client, session):
        """Head client carries the token -> known-terminal launch yields typed 409."""
        token = "stale-token"
        self._add_terminal_launch(session, token)

        response = client.patch(
            f"/execution/task-instances/{uuid4()}/run",
            json={**RUN_PATCH_BODY, "external_executor_id": token},
        )

        assert response.status_code == 409
        assert response.json()["detail"]["reason"] == "stale_executor_launch"

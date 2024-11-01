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
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.state import State

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


DEFAULT_START_DATE = timezone.parse("2024-10-31T11:00:00Z")
DEFAULT_END_DATE = timezone.parse("2024-10-31T12:00:00Z")


class TestTIUpdateState:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_ti_update_state_to_running(self, client, session, create_task_instance):
        """
        Test that the Task Instance state is updated to running when the Task Instance is in a state where it can be
        marked as running.
        """

        ti = create_task_instance(
            task_id="test_ti_update_state_to_running",
            state=State.QUEUED,
            session=session,
        )

        session.commit()

        response = client.patch(
            f"/execution/task_instance/{ti.id}/state",
            json={
                "state": "running",
                "hostname": "random-hostname",
                "unixname": "random-unixname",
                "pid": 100,
                "start_date": "2024-10-31T12:00:00Z",
            },
        )

        assert response.status_code == 204
        assert response.text == ""

        # Refresh the Task Instance from the database so that we can check the updated values
        session.refresh(ti)
        assert ti.state == State.RUNNING
        assert ti.hostname == "random-hostname"
        assert ti.unixname == "random-unixname"
        assert ti.pid == 100
        assert ti.start_date.isoformat() == "2024-10-31T12:00:00+00:00"

    def test_ti_update_state_conflict_if_not_queued(self, client, session, create_task_instance):
        """
        Test that a 409 error is returned when the Task Instance is not in a state where it can be marked as
        running. In this case, the Task Instance is first in NONE state so it cannot be marked as running.
        """
        ti = create_task_instance(
            task_id="test_ti_update_state_conflict_if_not_queued",
            state=State.NONE,
        )
        session.commit()

        response = client.patch(
            f"/execution/task_instance/{ti.id}/state",
            json={
                "state": "running",
                "hostname": "random-hostname",
                "unixname": "random-unixname",
                "pid": 100,
                "start_date": "2024-10-31T12:00:00Z",
            },
        )

        assert response.status_code == 409
        assert response.json() == {
            "detail": {
                "message": "TI was not in a state where it could be marked as running",
                "previous_state": State.NONE,
                "reason": "invalid_state",
            }
        }

        assert session.scalar(select(TaskInstance.state).where(TaskInstance.id == ti.id)) == State.NONE

    @pytest.mark.parametrize(
        ("state", "end_date", "expected_state", "expected_end_date"),
        [
            # For SUCCESS & FAILED states, end_date is required
            (State.SUCCESS, DEFAULT_END_DATE, State.SUCCESS, DEFAULT_END_DATE),
            (State.FAILED, DEFAULT_END_DATE, State.FAILED, DEFAULT_END_DATE),
            # When state is SKIPPED, end_date is optional because
            # 1) a user can run a task
            #   and raise AirflowSkipException within the task without specifying the end_date
            # 2) a task can be skipped by the scheduler without specifying the end_date
            (State.SKIPPED, DEFAULT_END_DATE, State.SKIPPED, DEFAULT_END_DATE),
            (State.SKIPPED, None, State.SKIPPED, None),
            # For UPSTREAM_FAILED & REMOVED states, end_date is not required
            (State.UPSTREAM_FAILED, None, State.UPSTREAM_FAILED, None),
            (State.REMOVED, None, State.REMOVED, None),
        ],
    )
    def test_ti_update_state_to_terminal(
        self, client, session, create_task_instance, state, end_date, expected_state, expected_end_date
    ):
        ti = create_task_instance(
            task_id="test_ti_update_state_to_terminal",
            start_date=DEFAULT_START_DATE,
            state=State.RUNNING,
        )
        session.commit()

        payload = {"state": state}
        if end_date:
            payload["end_date"] = end_date.isoformat()

        response = client.patch(
            f"/execution/task_instance/{ti.id}/state",
            json=payload,
        )

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        ti = session.get(TaskInstance, ti.id)
        assert ti.state == expected_state
        assert ti.end_date == expected_end_date

    def test_ti_update_state_to_terminal_raises_when_no_end_date(self, client, session, create_task_instance):
        """
        Test that a 409 error is returned when the TI state is updated to a terminal state with
        no end date.
        """
        ti = create_task_instance(
            task_id="test_ti_update_state_to_terminal_raises_when_no_end_date",
            start_date=DEFAULT_START_DATE,
            state=State.RUNNING,
        )
        session.commit()

        response = client.patch(
            f"/execution/task_instance/{ti.id}/state",
            json={
                "state": State.FAILED,
                "end_date": None,
            },
        )

        assert response.status_code == 409
        assert response.json() == {
            "detail": {
                "message": "End date is required for this state",
                "reason": "missing_end_date",
            }
        }

    def test_ti_update_state_not_found(self, client, session):
        """
        Test that a 404 error is returned when the Task Instance does not exist.
        """
        task_instance_id = "0182e924-0f1e-77e6-ab50-e977118bc139"

        # Pre-condition: the Task Instance does not exist
        assert session.scalar(select(TaskInstance.id).where(TaskInstance.id == task_instance_id)) is None

        payload = {"state": "success", "end_date": "2024-10-31T12:30:00Z"}

        response = client.patch(f"/execution/task_instance/{task_instance_id}/state", json=payload)
        assert response.status_code == 404
        assert response.json()["detail"] == {
            "reason": "not_found",
            "message": "Task Instance not found",
        }

    def test_ti_update_state_database_error(self, client, session, create_task_instance):
        """
        Test that a database error is handled correctly when updating the Task Instance state.
        """
        ti = create_task_instance(
            task_id="test_ti_update_state_database_error",
            state=State.QUEUED,
        )
        session.commit()
        payload = {
            "state": "running",
            "hostname": "random-hostname",
            "unixname": "random-unixname",
            "pid": 100,
            "start_date": "2024-10-31T12:00:00Z",
        }

        with mock.patch(
            "airflow.api_fastapi.execution_api.routes.task_instance.Session.execute",
            side_effect=[
                mock.Mock(one=lambda: ("queued",)),  # First call returns "queued"
                SQLAlchemyError("Database error"),  # Second call raises an error
            ],
        ):
            response = client.patch(f"/execution/task_instance/{ti.id}/state", json=payload)
            assert response.status_code == 500
            assert response.json()["detail"] == "Database error occurred"

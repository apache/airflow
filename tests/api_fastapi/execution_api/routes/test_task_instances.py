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

from datetime import datetime
from unittest import mock

import pytest
import uuid6
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from airflow.models import RenderedTaskInstanceFields, Trigger
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.db import clear_db_runs, clear_rendered_ti_fields

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
            f"/execution/task-instances/{ti.id}/state",
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

    @pytest.mark.parametrize("initial_ti_state", [s for s in TaskInstanceState if s != State.QUEUED])
    def test_ti_update_state_conflict_if_not_queued(
        self, client, session, create_task_instance, initial_ti_state
    ):
        """
        Test that a 409 error is returned when the Task Instance is not in a state where it can be marked as
        running. In this case, the Task Instance is first in NONE state so it cannot be marked as running.
        """
        ti = create_task_instance(
            task_id="test_ti_update_state_conflict_if_not_queued",
            state=initial_ti_state,
        )
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
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
                "previous_state": initial_ti_state,
                "reason": "invalid_state",
            }
        }

        assert session.scalar(select(TaskInstance.state).where(TaskInstance.id == ti.id)) == initial_ti_state

    @pytest.mark.parametrize(
        ("state", "end_date", "expected_state"),
        [
            (State.SUCCESS, DEFAULT_END_DATE, State.SUCCESS),
            (State.FAILED, DEFAULT_END_DATE, State.FAILED),
            (State.SKIPPED, DEFAULT_END_DATE, State.SKIPPED),
        ],
    )
    def test_ti_update_state_to_terminal(
        self, client, session, create_task_instance, state, end_date, expected_state
    ):
        ti = create_task_instance(
            task_id="test_ti_update_state_to_terminal",
            start_date=DEFAULT_START_DATE,
            state=State.RUNNING,
        )
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
            json={
                "state": state,
                "end_date": end_date.isoformat(),
            },
        )

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        ti = session.get(TaskInstance, ti.id)
        assert ti.state == expected_state
        assert ti.end_date == end_date

    def test_ti_update_state_not_found(self, client, session):
        """
        Test that a 404 error is returned when the Task Instance does not exist.
        """
        task_instance_id = "0182e924-0f1e-77e6-ab50-e977118bc139"

        # Pre-condition: the Task Instance does not exist
        assert session.scalar(select(TaskInstance.id).where(TaskInstance.id == task_instance_id)) is None

        payload = {"state": "success", "end_date": "2024-10-31T12:30:00Z"}

        response = client.patch(f"/execution/task-instances/{task_instance_id}/state", json=payload)
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
            "airflow.api_fastapi.common.db.common.Session.execute",
            side_effect=[
                mock.Mock(one=lambda: ("queued",)),  # First call returns "queued"
                SQLAlchemyError("Database error"),  # Second call raises an error
            ],
        ):
            response = client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)
            assert response.status_code == 500
            assert response.json()["detail"] == "Database error occurred"

    def test_ti_update_state_to_deferred(self, client, session, create_task_instance, time_machine):
        """
        Test that tests if the transition to deferred state is handled correctly.
        """

        ti = create_task_instance(
            task_id="test_ti_update_state_to_deferred",
            state=State.RUNNING,
            session=session,
        )
        session.commit()

        instant = timezone.datetime(2024, 11, 22)
        time_machine.move_to(instant, tick=False)

        payload = {
            "state": "deferred",
            "trigger_kwargs": {"key": "value"},
            "classpath": "my-classpath",
            "next_method": "execute_callback",
            "trigger_timeout": "P1D",  # 1 day
        }

        response = client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        tis = session.query(TaskInstance).all()
        assert len(tis) == 1

        assert tis[0].state == TaskInstanceState.DEFERRED
        assert tis[0].next_method == "execute_callback"
        assert tis[0].next_kwargs == {"key": "value"}
        assert tis[0].trigger_timeout == timezone.make_aware(datetime(2024, 11, 23), timezone=timezone.utc)

        t = session.query(Trigger).all()
        assert len(t) == 1
        assert t[0].created_date == instant
        assert t[0].classpath == "my-classpath"
        assert t[0].kwargs == {"key": "value"}


class TestTIHealthEndpoint:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.mark.parametrize(
        ("hostname", "pid", "expected_status_code", "expected_detail"),
        [
            # Case: Successful heartbeat
            ("random-hostname", 1789, 204, None),
            # Case: Conflict due to hostname mismatch
            (
                "wrong-hostname",
                1789,
                409,
                {
                    "reason": "running_elsewhere",
                    "message": "TI is already running elsewhere",
                    "current_hostname": "random-hostname",
                    "current_pid": 1789,
                },
            ),
            # Case: Conflict due to pid mismatch
            (
                "random-hostname",
                1054,
                409,
                {
                    "reason": "running_elsewhere",
                    "message": "TI is already running elsewhere",
                    "current_hostname": "random-hostname",
                    "current_pid": 1789,
                },
            ),
        ],
    )
    def test_ti_heartbeat(
        self,
        client,
        session,
        create_task_instance,
        hostname,
        pid,
        expected_status_code,
        expected_detail,
        time_machine,
    ):
        """Test the TI heartbeat endpoint for various scenarios including conflicts."""
        time_now = timezone.parse("2024-10-31T12:00:00Z")

        # Freeze time to a specific time
        time_machine.move_to(time_now, tick=False)

        ti = create_task_instance(
            task_id="test_ti_heartbeat",
            state=State.RUNNING,
            hostname="random-hostname",
            pid=1789,
            session=session,
        )
        session.commit()
        task_instance_id = ti.id

        # Pre-condition: TI heartbeat is NONE
        assert ti.last_heartbeat_at is None

        response = client.put(
            f"/execution/task-instances/{task_instance_id}/heartbeat",
            json={"hostname": hostname, "pid": pid},
        )

        assert response.status_code == expected_status_code

        if expected_status_code == 204:
            # If successful, ensure last_heartbeat_at is updated
            session.refresh(ti)
            assert ti.last_heartbeat_at == time_now
            assert response.text == ""
        else:
            # If there's an error, check the error detail
            assert response.json()["detail"] == expected_detail

    def test_ti_heartbeat_non_existent_task(self, client, session, create_task_instance):
        """Test that a 404 error is returned when the Task Instance does not exist."""

        task_instance_id = "0182e924-0f1e-77e6-ab50-e977118bc139"

        # Pre-condition: the Task Instance does not exist
        assert session.scalar(select(TaskInstance.id).where(TaskInstance.id == task_instance_id)) is None

        response = client.put(
            f"/execution/task-instances/{task_instance_id}/heartbeat",
            json={"hostname": "random-hostname", "pid": 1547},
        )

        assert response.status_code == 404
        assert response.json()["detail"] == {
            "reason": "not_found",
            "message": "Task Instance not found",
        }

    @pytest.mark.parametrize(
        "ti_state",
        [State.SUCCESS, State.FAILED],
    )
    def test_ti_heartbeat_when_task_not_running(self, client, session, create_task_instance, ti_state):
        """Test that a 409 error is returned when the Task Instance is not in RUNNING state."""

        ti = create_task_instance(
            task_id="test_ti_heartbeat_when_task_not_running",
            state=ti_state,
            hostname="random-hostname",
            pid=1547,
            session=session,
        )
        session.commit()
        task_instance_id = ti.id

        response = client.put(
            f"/execution/task-instances/{task_instance_id}/heartbeat",
            json={"hostname": "random-hostname", "pid": 1547},
        )

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "reason": "not_running",
            "message": "TI is no longer in the running state and task should terminate",
            "current_state": ti_state,
        }

    def test_ti_heartbeat_update(self, client, session, create_task_instance, time_machine):
        """Test that the Task Instance heartbeat is updated when the Task Instance is running."""

        # Set initial time for the test
        time_now = timezone.parse("2024-10-31T12:00:00Z")
        time_machine.move_to(time_now, tick=False)

        ti = create_task_instance(
            task_id="test_ti_heartbeat_update",
            state=State.RUNNING,
            hostname="random-hostname",
            pid=1547,
            last_heartbeat_at=time_now,
            session=session,
        )
        session.commit()
        task_instance_id = ti.id

        # Pre-condition: TI heartbeat is set
        assert ti.last_heartbeat_at == time_now, "Initial last_heartbeat_at should match time_now"

        # Move time forward by 10 minutes
        new_time = time_now.add(minutes=10)
        time_machine.move_to(new_time, tick=False)

        response = client.put(
            f"/execution/task-instances/{task_instance_id}/heartbeat",
            json={"hostname": "random-hostname", "pid": 1547},
        )

        assert response.status_code == 204

        # If successful, ensure last_heartbeat_at is updated
        session.refresh(ti)
        assert ti.last_heartbeat_at == time_now.add(minutes=10)


class TestTIPutRTIF:
    def setup_method(self):
        clear_db_runs()
        clear_rendered_ti_fields()

    def teardown_method(self):
        clear_db_runs()
        clear_rendered_ti_fields()

    def test_ti_put_rtif_success(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_ti_put_rtif_success",
            state=State.RUNNING,
            session=session,
        )
        session.commit()

        payload = {"field1": "rendered_value1", "field2": "rendered_value2"}

        response = client.put(f"/execution/task-instances/{ti.id}/rtif", json=payload)
        assert response.status_code == 201
        assert response.json() == {"message": "Rendered task instance fields successfully set"}

        session.expire_all()

        rtifs = session.query(RenderedTaskInstanceFields).all()
        assert len(rtifs) == 1

        assert rtifs[0].dag_id == "dag"
        assert rtifs[0].run_id == "test"
        assert rtifs[0].task_id == "test_ti_put_rtif_success"
        assert rtifs[0].map_index == -1
        assert rtifs[0].rendered_fields == payload

    def test_ti_put_rtif_missing_ti(self, client, session, create_task_instance):
        create_task_instance(
            task_id="test_ti_put_rtif_missing_ti",
            state=State.RUNNING,
            session=session,
        )
        session.commit()

        payload = {"field1": "rendered_value1", "field2": "rendered_value2"}

        random_id = uuid6.uuid7()
        response = client.put(f"/execution/task-instances/{random_id}/rtif", json=payload)
        assert response.status_code == 404
        assert response.json()["detail"] == "Not Found"

    def test_ti_put_rtif_extra_fields(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_ti_put_rtif_missing_ti",
            state=State.RUNNING,
            session=session,
        )
        session.commit()

        payload = {
            "field1": "rendered_value1",
            "field2": "rendered_value2",
            "invalid_key": {"field3": "rendered_value3"},
        }

        response = client.put(f"/execution/task-instances/{ti.id}/rtif", json=payload)
        assert response.status_code == 422
        assert response.json()["detail"] == [
            {
                "input": {"field3": "rendered_value3"},
                "loc": ["body", "invalid_key"],
                "msg": "Input should be a valid string",
                "type": "string_type",
            }
        ]

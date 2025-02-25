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
from sqlalchemy import select, update
from sqlalchemy.exc import SQLAlchemyError

from airflow.exceptions import AirflowInactiveAssetInInletOrOutletException
from airflow.models import RenderedTaskInstanceFields, TaskReschedule, Trigger
from airflow.models.asset import AssetActive, AssetAliasModel, AssetEvent, AssetModel
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.definitions.asset import AssetUniqueKey
from airflow.utils import timezone
from airflow.utils.state import State, TaskInstanceState, TerminalTIState

from tests_common.test_utils.db import clear_db_assets, clear_db_runs, clear_rendered_ti_fields

pytestmark = pytest.mark.db_test


DEFAULT_START_DATE = timezone.parse("2024-10-31T11:00:00Z")
DEFAULT_END_DATE = timezone.parse("2024-10-31T12:00:00Z")


def _create_asset_aliases(session, num: int = 2) -> None:
    asset_aliases = [
        AssetAliasModel(
            id=i,
            name=f"simple{i}",
            group="alias",
        )
        for i in range(1, 1 + num)
    ]
    session.add_all(asset_aliases)
    session.commit()


class TestTIRunState:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_ti_run_state_to_running(self, client, session, create_task_instance, time_machine):
        """
        Test that the Task Instance state is updated to running when the Task Instance is in a state where it can be
        marked as running.
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

        response = client.patch(
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
            "variables": [],
            "connections": [],
        }

        # Refresh the Task Instance from the database so that we can check the updated values
        session.refresh(ti)
        assert ti.state == State.RUNNING
        assert ti.hostname == "random-hostname"
        assert ti.unixname == "random-unixname"
        assert ti.pid == 100

    @pytest.mark.parametrize("initial_ti_state", [s for s in TaskInstanceState if s != State.QUEUED])
    def test_ti_run_state_conflict_if_not_queued(
        self, client, session, create_task_instance, initial_ti_state
    ):
        """
        Test that a 409 error is returned when the Task Instance is not in a state where it can be marked as
        running. In this case, the Task Instance is first in NONE state so it cannot be marked as running.
        """
        ti = create_task_instance(
            task_id="test_ti_run_state_conflict_if_not_queued",
            state=initial_ti_state,
        )
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
        assert response.json() == {
            "detail": {
                "message": "TI was not in a state where it could be marked as running",
                "previous_state": initial_ti_state,
                "reason": "invalid_state",
            }
        }

        assert session.scalar(select(TaskInstance.state).where(TaskInstance.id == ti.id)) == initial_ti_state

    def test_xcom_cleared_when_ti_runs(self, client, session, create_task_instance, time_machine):
        """
        Test that the xcoms are cleared when the Task Instance state is updated to running.
        """
        instant_str = "2024-09-30T12:00:00Z"
        instant = timezone.parse(instant_str)
        time_machine.move_to(instant, tick=False)

        ti = create_task_instance(
            task_id="test_xcom_cleared_when_ti_runs",
            state=State.QUEUED,
            session=session,
            start_date=instant,
        )
        session.commit()

        # Lets stage a xcom push
        ti.xcom_push(key="key", value="value")

        response = client.patch(
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
        # Once the task is running, we can check if xcom is cleared
        assert ti.xcom_pull(task_ids="test_xcom_cleared_when_ti_runs", key="key") is None

    def test_xcom_not_cleared_for_deferral(self, client, session, create_task_instance, time_machine):
        """
        Test that the xcoms are not cleared when the Task Instance state is re-running after deferral.
        """
        instant_str = "2024-09-30T12:00:00Z"
        instant = timezone.parse(instant_str)
        time_machine.move_to(instant, tick=False)

        ti = create_task_instance(
            task_id="test_xcom_not_cleared_for_deferral",
            state=State.RUNNING,
            session=session,
            start_date=instant,
        )
        session.commit()

        # Move this task to deferred
        payload = {
            "state": "deferred",
            "trigger_kwargs": {"key": "value", "moment": "2024-12-18T00:00:00Z"},
            "classpath": "my-classpath",
            "next_method": "execute_callback",
            "trigger_timeout": "P1D",  # 1 day
        }

        response = client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)
        assert response.status_code == 204
        assert response.text == ""
        session.expire_all()

        # Deferred -> Queued so that we can run it again
        query = update(TaskInstance).where(TaskInstance.id == ti.id).values(state="queued")
        session.execute(query)
        session.commit()

        # Lets stage a xcom push
        ti.xcom_push(key="key", value="value")

        response = client.patch(
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
        assert ti.xcom_pull(task_ids="test_xcom_not_cleared_for_deferral", key="key") == "value"


class TestTIUpdateState:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

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

    @pytest.mark.parametrize(
        ("task_outlets", "outlet_events"),
        [
            (
                [{"name": "s3://bucket/my-task", "uri": "s3://bucket/my-task", "asset_type": "Asset"}],
                [
                    {
                        "key": {"name": "s3://bucket/my-task", "uri": "s3://bucket/my-task"},
                        "extra": {},
                        "asset_alias_events": [],
                    }
                ],
            ),
            (
                [{"asset_type": "AssetAlias"}],
                [
                    {
                        "source_alias_name": "example-alias",
                        "dest_asset_key": {"name": "s3://bucket/my-task", "uri": "s3://bucket/my-task"},
                        "extra": {},
                    }
                ],
            ),
        ],
    )
    def test_ti_update_state_to_success_with_asset_events(
        self, client, session, create_task_instance, task_outlets, outlet_events
    ):
        clear_db_assets()
        clear_db_runs()

        asset = AssetModel(
            id=1,
            name="s3://bucket/my-task",
            uri="s3://bucket/my-task",
            group="asset",
            extra={},
        )
        asset_active = AssetActive.for_asset(asset)
        session.add_all([asset, asset_active])
        asset_type = task_outlets[0]["asset_type"]
        if asset_type == "AssetAlias":
            _create_asset_aliases(session, num=1)
            asset_alias = session.query(AssetAliasModel).all()
            assert len(asset_alias) == 1
            assert asset_alias == [AssetAliasModel(name="simple1")]

        ti = create_task_instance(
            task_id="test_ti_update_state_to_success_with_asset_events",
            start_date=DEFAULT_START_DATE,
            state=State.RUNNING,
        )
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
            json={
                "state": "success",
                "end_date": DEFAULT_END_DATE.isoformat(),
                "task_outlets": task_outlets,
                "outlet_events": outlet_events,
            },
        )

        assert response.status_code == 204
        assert response.text == ""
        session.expire_all()

        # check if asset was created properly
        asset = session.query(AssetModel).all()
        assert len(asset) == 1
        assert asset == [AssetModel(name="s3://bucket/my-task", uri="s3://bucket/my-task", extra={})]

        event = session.query(AssetEvent).all()
        assert len(event) == 1
        assert event[0].asset_id == 1
        assert event[0].asset == AssetModel(name="s3://bucket/my-task", uri="s3://bucket/my-task", extra={})
        assert event[0].extra == {}
        if asset_type == "AssetAlias":
            assert event[0].source_aliases == [AssetAliasModel(name="example-alias")]

    def test_ti_update_state_not_found(self, client, session):
        """
        Test that a 404 error is returned when the Task Instance does not exist.
        """
        task_instance_id = "0182e924-0f1e-77e6-ab50-e977118bc139"

        # Pre-condition: the Task Instance does not exist
        assert session.get(TaskInstance, task_instance_id) is None

        payload = {"state": "success", "end_date": "2024-10-31T12:30:00Z"}

        response = client.patch(f"/execution/task-instances/{task_instance_id}/state", json=payload)
        assert response.status_code == 404
        assert response.json()["detail"] == {
            "reason": "not_found",
            "message": "Task Instance not found",
        }

    def test_ti_update_state_running_errors(self, client, session, create_task_instance, time_machine):
        """
        Test that a 422 error is returned when the Task Instance state is RUNNING in the payload.

        Task should be set to Running state via the /execution/task-instances/{task_instance_id}/run endpoint.
        """

        ti = create_task_instance(
            task_id="test_ti_update_state_running_errors",
            state=State.QUEUED,
            session=session,
            start_date=DEFAULT_START_DATE,
        )

        session.commit()

        response = client.patch(f"/execution/task-instances/{ti.id}/state", json={"state": "running"})

        assert response.status_code == 422

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
            "state": "success",
            "end_date": "2024-10-31T12:00:00Z",
        }

        with (
            mock.patch(
                "airflow.api_fastapi.common.db.common.Session.execute",
                side_effect=[
                    mock.Mock(one=lambda: ("running", 1, 0)),  # First call returns "queued"
                    mock.Mock(one=lambda: ("running", 1, 0)),  # Second call returns "queued"
                    SQLAlchemyError("Database error"),  # Last call raises an error
                ],
            ),
            mock.patch(
                "airflow.models.taskinstance.TaskInstance.register_asset_changes_in_db",
            ) as mock_register_asset_changes_in_db,
        ):
            mock_register_asset_changes_in_db.return_value = None
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
            "trigger_kwargs": {"key": "value", "moment": "2024-12-18T00:00:00Z"},
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
        assert tis[0].next_kwargs == {
            "key": "value",
            "moment": datetime(2024, 12, 18, 00, 00, 00, tzinfo=timezone.utc),
        }
        assert tis[0].trigger_timeout == timezone.make_aware(datetime(2024, 11, 23), timezone=timezone.utc)

        t = session.query(Trigger).all()
        assert len(t) == 1
        assert t[0].created_date == instant
        assert t[0].classpath == "my-classpath"
        assert t[0].kwargs == {
            "key": "value",
            "moment": datetime(2024, 12, 18, 00, 00, 00, tzinfo=timezone.utc),
        }

    def test_ti_update_state_to_reschedule(self, client, session, create_task_instance, time_machine):
        """
        Test that tests if the transition to reschedule state is handled correctly.
        """

        instant = timezone.datetime(2024, 10, 30)
        time_machine.move_to(instant, tick=False)

        ti = create_task_instance(
            task_id="test_ti_update_state_to_reschedule",
            state=State.RUNNING,
            session=session,
        )
        ti.start_date = instant
        session.commit()

        payload = {
            "state": "up_for_reschedule",
            "reschedule_date": "2024-10-31T11:03:00+00:00",
            "end_date": DEFAULT_END_DATE.isoformat(),
        }

        response = client.patch(f"/execution/task-instances/{ti.id}/state", json=payload)

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        tis = session.query(TaskInstance).all()
        assert len(tis) == 1
        assert tis[0].state == TaskInstanceState.UP_FOR_RESCHEDULE
        assert tis[0].next_method is None
        assert tis[0].next_kwargs is None
        assert tis[0].duration == 129600

        trs = session.query(TaskReschedule).all()
        assert len(trs) == 1
        assert trs[0].dag_id == "dag"
        assert trs[0].task_id == "test_ti_update_state_to_reschedule"
        assert trs[0].run_id == "test"
        assert trs[0].try_number == 0
        assert trs[0].start_date == instant
        assert trs[0].end_date == DEFAULT_END_DATE
        assert trs[0].reschedule_date == timezone.parse("2024-10-31T11:03:00+00:00")
        assert trs[0].map_index == -1
        assert trs[0].duration == 129600

    @pytest.mark.parametrize(
        ("retries", "expected_state"),
        [
            (0, State.FAILED),
            (None, State.FAILED),
            (3, State.UP_FOR_RETRY),
        ],
    )
    def test_ti_update_state_to_failed_with_retries(
        self, client, session, create_task_instance, retries, expected_state
    ):
        ti = create_task_instance(
            task_id="test_ti_update_state_to_retry",
            state=State.RUNNING,
        )

        if retries is not None:
            ti.max_tries = retries
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
            json={
                "state": TerminalTIState.FAILED,
                "end_date": DEFAULT_END_DATE.isoformat(),
            },
        )

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        ti = session.get(TaskInstance, ti.id)
        assert ti.state == expected_state
        assert ti.next_method is None
        assert ti.next_kwargs is None

    def test_ti_update_state_when_ti_is_restarting(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_ti_update_state_when_ti_is_restarting",
            state=State.RUNNING,
        )
        # update state to restarting
        ti.state = State.RESTARTING
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
            json={
                "state": TerminalTIState.FAILED,
                "end_date": DEFAULT_END_DATE.isoformat(),
            },
        )

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        ti = session.get(TaskInstance, ti.id)
        # restarting is always retried
        assert ti.state == State.UP_FOR_RETRY
        assert ti.next_method is None
        assert ti.next_kwargs is None

    def test_ti_update_state_when_ti_has_higher_tries_than_retries(
        self, client, session, create_task_instance
    ):
        ti = create_task_instance(
            task_id="test_ti_update_state_when_ti_has_higher_tries_than_retries",
            state=State.RUNNING,
        )
        # two maximum tries defined, but third try going on
        ti.max_tries = 2
        ti.try_number = 3
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
            json={
                "state": TerminalTIState.FAILED,
                "end_date": DEFAULT_END_DATE.isoformat(),
            },
        )

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        ti = session.get(TaskInstance, ti.id)
        # all retries exhausted, marking as failed
        assert ti.state == State.FAILED
        assert ti.next_method is None
        assert ti.next_kwargs is None

    def test_ti_update_state_to_failed_without_retry_table_check(self, client, session, create_task_instance):
        # we just want to fail in this test, no need to retry
        ti = create_task_instance(
            task_id="test_ti_update_state_to_failed_table_check",
            state=State.RUNNING,
        )
        ti.start_date = DEFAULT_START_DATE
        session.commit()

        response = client.patch(
            f"/execution/task-instances/{ti.id}/state",
            json={
                "state": TerminalTIState.FAIL_WITHOUT_RETRY,
                "end_date": DEFAULT_END_DATE.isoformat(),
            },
        )

        assert response.status_code == 204
        assert response.text == ""

        session.expire_all()

        ti = session.get(TaskInstance, ti.id)
        assert ti.state == State.FAILED
        assert ti.next_method is None
        assert ti.next_kwargs is None
        assert ti.duration == 3600.00

    @pytest.mark.parametrize(
        ("state", "expected_status_code"),
        [
            (State.RUNNING, 204),
            (State.SUCCESS, 409),
            (State.QUEUED, 409),
            (State.FAILED, 409),
        ],
    )
    def test_ti_runtime_checks_success(
        self, client, session, create_task_instance, state, expected_status_code
    ):
        ti = create_task_instance(
            task_id="test_ti_runtime_checks",
            state=state,
        )
        session.commit()

        with mock.patch(
            "airflow.models.taskinstance.TaskInstance.validate_inlet_outlet_assets_activeness"
        ) as mock_validate_inlet_outlet_assets_activeness:
            mock_validate_inlet_outlet_assets_activeness.return_value = None
            response = client.post(
                f"/execution/task-instances/{ti.id}/runtime-checks",
                json={
                    "inlets": [],
                    "outlets": [],
                },
            )

            assert response.status_code == expected_status_code

        session.expire_all()

    def test_ti_runtime_checks_failure(self, client, session, create_task_instance):
        ti = create_task_instance(
            task_id="test_ti_runtime_checks_failure",
            state=State.RUNNING,
        )
        session.commit()

        with mock.patch(
            "airflow.models.taskinstance.TaskInstance.validate_inlet_outlet_assets_activeness"
        ) as mock_validate_inlet_outlet_assets_activeness:
            mock_validate_inlet_outlet_assets_activeness.side_effect = (
                AirflowInactiveAssetInInletOrOutletException([AssetUniqueKey(name="abc", uri="something")])
            )
            response = client.post(
                f"/execution/task-instances/{ti.id}/runtime-checks",
                json={
                    "inlets": [],
                    "outlets": [],
                },
            )

            assert response.status_code == 400

        session.expire_all()


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
        assert session.get(TaskInstance, task_instance_id) is None

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

    @pytest.mark.parametrize(
        "payload",
        [
            # string value
            {"field1": "string_value", "field2": "another_string"},
            # dictionary value
            {"field1": {"nested_key": "nested_value"}},
            # string lists value
            {"field1": ["123"], "field2": ["a", "b", "c"]},
            # list of JSON values
            {"field1": [1, "string", 3.14, True, None, {"nested": "dict"}]},
            # nested dictionary with mixed types in lists
            {
                "field1": {"nested_dict": {"key1": 123, "key2": "value"}},
                "field2": [3.14, {"sub_key": "sub_value"}, [1, 2]],
            },
        ],
    )
    def test_ti_put_rtif_success(self, client, session, create_task_instance, payload):
        ti = create_task_instance(
            task_id="test_ti_put_rtif_success",
            state=State.RUNNING,
            session=session,
        )
        session.commit()
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


class TestPreviousDagRun:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    def test_ti_previous_dag_run(self, client, session, create_task_instance, dag_maker):
        """Test that the previous dag run is returned correctly for a task instance."""
        ti = create_task_instance(
            task_id="test_ti_previous_dag_run",
            dag_id="test_dag",
            logical_date=timezone.datetime(2025, 1, 19),
            state=State.RUNNING,
            start_date=timezone.datetime(2024, 1, 17),
            session=session,
        )
        session.commit()

        # Create 2 DagRuns for the same DAG to verify that the correct DagRun (last) is returned
        dr1 = dag_maker.create_dagrun(
            run_id="test_run_id_1",
            logical_date=timezone.datetime(2025, 1, 17),
            run_type="scheduled",
            state=State.SUCCESS,
            session=session,
        )
        dr1.end_date = timezone.datetime(2025, 1, 17, 1, 0, 0)

        dr2 = dag_maker.create_dagrun(
            run_id="test_run_id_2",
            logical_date=timezone.datetime(2025, 1, 18),
            run_type="scheduled",
            state=State.SUCCESS,
            session=session,
        )

        dr2.end_date = timezone.datetime(2025, 1, 18, 1, 0, 0)

        session.commit()

        response = client.get(f"/execution/task-instances/{ti.id}/previous-successful-dagrun")
        assert response.status_code == 200
        assert response.json() == {
            "data_interval_start": "2025-01-18T00:00:00Z",
            "data_interval_end": "2025-01-19T00:00:00Z",
            "start_date": "2024-01-17T00:00:00Z",
            "end_date": "2025-01-18T01:00:00Z",
        }

    def test_ti_previous_dag_run_not_found(self, client, session):
        ti_id = "0182e924-0f1e-77e6-ab50-e977118bc139"

        assert session.get(TaskInstance, ti_id) is None

        response = client.get(f"/execution/task-instances/{ti_id}/previous-successful-dagrun")
        assert response.status_code == 200
        assert response.json() == {
            "data_interval_start": None,
            "data_interval_end": None,
            "start_date": None,
            "end_date": None,
        }

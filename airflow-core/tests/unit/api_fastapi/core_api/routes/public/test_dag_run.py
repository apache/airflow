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

from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import time_machine
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagModel, DagRun, Log
from airflow.models.asset import AssetEvent, AssetModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.param import Param
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.api_fastapi import _check_dag_run_note, _check_last_log
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_connections,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu, from_datetime_to_zulu_without_ms

if TYPE_CHECKING:
    from airflow.models.dag_version import DagVersion
    from airflow.timetables.base import DataInterval

pytestmark = pytest.mark.db_test


class CustomTimetable(CronDataIntervalTimetable):
    """Custom timetable that generates custom run IDs."""

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        run_after,
        data_interval: DataInterval | None,
        **kwargs,
    ) -> str:
        if data_interval:
            return f"custom_{data_interval.start.strftime('%Y%m%d%H%M%S')}"
        return f"custom_manual_{run_after.strftime('%Y%m%d%H%M%S')}"


@pytest.fixture
def custom_timetable_plugin(monkeypatch):
    """Fixture to register CustomTimetable for serialization."""
    from airflow import plugins_manager
    from airflow.utils.module_loading import qualname

    timetable_class_name = qualname(CustomTimetable)
    existing_timetables = getattr(plugins_manager, "timetable_classes", None) or {}

    monkeypatch.setattr(plugins_manager, "initialize_timetables_plugins", lambda: None)
    monkeypatch.setattr(
        plugins_manager,
        "timetable_classes",
        {**existing_timetables, timetable_class_name: CustomTimetable},
    )


DAG1_ID = "test_dag1"
DAG1_DISPLAY_NAME = "test_dag1"
DAG2_ID = "test_dag2"
DAG1_RUN1_ID = "dag_run_1"
DAG1_RUN2_ID = "dag_run_2"
DAG2_RUN1_ID = "dag_run_3"
DAG2_RUN2_ID = "dag_run_4"
DAG1_RUN1_STATE = DagRunState.SUCCESS
DAG1_RUN2_STATE = DagRunState.FAILED
DAG2_RUN1_STATE = DagRunState.SUCCESS
DAG2_RUN2_STATE = DagRunState.SUCCESS
DAG1_RUN1_RUN_TYPE = DagRunType.MANUAL
DAG1_RUN2_RUN_TYPE = DagRunType.SCHEDULED
DAG2_RUN1_RUN_TYPE = DagRunType.BACKFILL_JOB
DAG2_RUN2_RUN_TYPE = DagRunType.ASSET_TRIGGERED
DAG1_RUN1_TRIGGERED_BY = DagRunTriggeredByType.UI
DAG1_RUN2_TRIGGERED_BY = DagRunTriggeredByType.ASSET
DAG2_RUN1_TRIGGERED_BY = DagRunTriggeredByType.CLI
DAG2_RUN2_TRIGGERED_BY = DagRunTriggeredByType.REST_API
START_DATE1 = datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE1 = datetime(2024, 2, 16, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE2 = datetime(2024, 2, 20, 0, 0, tzinfo=timezone.utc)
RUN_AFTER1 = datetime(2024, 2, 16, 0, 0, tzinfo=timezone.utc)
RUN_AFTER2 = datetime(2024, 2, 20, 0, 0, tzinfo=timezone.utc)
START_DATE2 = datetime(2024, 4, 15, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE3 = datetime(2024, 5, 16, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE4 = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
DAG1_RUN1_NOTE = "test_note"
DAG2_PARAM = {"validated_number": Param(1, minimum=1, maximum=10)}

DAG_RUNS_LIST = [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID]


@pytest.fixture(autouse=True)
@provide_session
def setup(request, dag_maker, session=None):
    clear_db_connections()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()
    clear_db_logs()

    if "no_setup" in request.keywords:
        return

    with dag_maker(DAG1_ID, schedule=None, start_date=START_DATE1, serialized=True):
        task1 = EmptyOperator(task_id="task_1")
        task2 = EmptyOperator(task_id="task_2")

    dag_run1 = dag_maker.create_dagrun(
        run_id=DAG1_RUN1_ID,
        state=DAG1_RUN1_STATE,
        run_type=DAG1_RUN1_RUN_TYPE,
        triggered_by=DAG1_RUN1_TRIGGERED_BY,
        logical_date=LOGICAL_DATE1,
    )
    # Set triggering_user_name for testing
    dag_run1.triggering_user_name = "alice_admin"
    dag_run1.note = (DAG1_RUN1_NOTE, "not_test")
    # Set end_date for testing duration filter
    dag_run1.end_date = dag_run1.start_date + timedelta(seconds=101)
    # Set conf for testing conf_contains filter (values ordered for predictable sorting)
    dag_run1.conf = {"env": "development", "version": "1.0"}

    for i, task in enumerate([task1, task2], start=1):
        ti = dag_run1.get_task_instance(task_id=task.task_id)
        ti.task = task
        ti.state = State.SUCCESS
        session.merge(ti)
        ti.xcom_push("return_value", f"result_{i}")

    dag_run2 = dag_maker.create_dagrun(
        run_id=DAG1_RUN2_ID,
        state=DAG1_RUN2_STATE,
        run_type=DAG1_RUN2_RUN_TYPE,
        triggered_by=DAG1_RUN2_TRIGGERED_BY,
        logical_date=LOGICAL_DATE2,
    )
    # Set triggering_user_name for testing
    dag_run2.triggering_user_name = "bob_service"
    # Set end_date for testing duration filter
    dag_run2.end_date = dag_run2.start_date + timedelta(seconds=201)
    # Set conf for testing conf_contains filter
    dag_run2.conf = {"env": "production", "debug": True}

    ti1 = dag_run2.get_task_instance(task_id=task1.task_id)
    ti1.task = task1
    ti1.state = State.SUCCESS

    ti2 = dag_run2.get_task_instance(task_id=task2.task_id)
    ti2.task = task2
    ti2.state = State.FAILED

    with dag_maker(DAG2_ID, schedule=None, start_date=START_DATE2, params=DAG2_PARAM, serialized=True):
        EmptyOperator(task_id="task_2")

    dag_run3 = dag_maker.create_dagrun(
        run_id=DAG2_RUN1_ID,
        state=DAG2_RUN1_STATE,
        run_type=DAG2_RUN1_RUN_TYPE,
        triggered_by=DAG2_RUN1_TRIGGERED_BY,
        logical_date=LOGICAL_DATE3,
    )
    # Set triggering_user_name for testing
    dag_run3.triggering_user_name = "service_account"
    # Set end_date for testing duration filter
    dag_run3.end_date = dag_run3.start_date + timedelta(seconds=51)
    # Set conf for testing conf_contains filter
    dag_run3.conf = {"env": "staging", "test_mode": True}

    dag_run4 = dag_maker.create_dagrun(
        run_id=DAG2_RUN2_ID,
        state=DAG2_RUN2_STATE,
        run_type=DAG2_RUN2_RUN_TYPE,
        triggered_by=DAG2_RUN2_TRIGGERED_BY,
        logical_date=LOGICAL_DATE4,
    )
    # Leave triggering_user_name as None for testing
    dag_run4.triggering_user_name = None
    # Set end_date for testing duration filter
    dag_run4.end_date = dag_run4.start_date + timedelta(seconds=150)
    # Set conf for testing conf_contains filter
    dag_run4.conf = {"env": "testing", "mode": "ci"}

    dag_maker.sync_dagbag_to_db()
    dag_maker.dag_model.has_task_concurrency_limits = True
    session.merge(ti1)
    session.merge(ti2)
    session.merge(dag_maker.dag_model)
    session.commit()


def get_dag_versions_dict(dag_versions: list[DagVersion]) -> list[dict]:
    return [
        # must set mode="json" or the created_at and id will be python datetime and UUID instead of string
        DagVersionResponse.model_validate(dag_version, from_attributes=True).model_dump(mode="json")
        for dag_version in dag_versions
    ]


def get_dag_run_dict(run: DagRun):
    return {
        "bundle_version": None,
        "dag_display_name": run.dag_model.dag_display_name,
        "dag_run_id": run.run_id,
        "dag_id": run.dag_id,
        "logical_date": from_datetime_to_zulu_without_ms(run.logical_date) if run.logical_date else None,
        "queued_at": from_datetime_to_zulu(run.queued_at) if run.queued_at else None,
        "run_after": from_datetime_to_zulu_without_ms(run.run_after),
        "start_date": from_datetime_to_zulu_without_ms(run.start_date) if run.start_date else None,
        "end_date": from_datetime_to_zulu_without_ms(run.end_date) if run.end_date else None,
        "duration": run.duration,
        "data_interval_start": from_datetime_to_zulu_without_ms(run.data_interval_start)
        if run.data_interval_start
        else None,
        "data_interval_end": from_datetime_to_zulu_without_ms(run.data_interval_end)
        if run.data_interval_end
        else None,
        "last_scheduling_decision": (
            from_datetime_to_zulu(run.last_scheduling_decision) if run.last_scheduling_decision else None
        ),
        "run_type": run.run_type,
        "state": run.state,
        "triggered_by": run.triggered_by.value if run.triggered_by else None,
        "triggering_user_name": run.triggering_user_name,
        "conf": run.conf,
        "note": run.note,
        "dag_versions": get_dag_versions_dict(run.dag_versions),
        "partition_key": None,
    }


class TestGetDagRun:
    @pytest.mark.parametrize(
        ("dag_id", "run_id", "state", "run_type", "triggered_by", "dag_run_note"),
        [
            (
                DAG1_ID,
                DAG1_RUN1_ID,
                DAG1_RUN1_STATE,
                DAG1_RUN1_RUN_TYPE,
                DAG1_RUN1_TRIGGERED_BY,
                DAG1_RUN1_NOTE,
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                DAG1_RUN2_STATE,
                DAG1_RUN2_RUN_TYPE,
                DAG1_RUN2_TRIGGERED_BY,
                None,
            ),
            (
                DAG2_ID,
                DAG2_RUN1_ID,
                DAG2_RUN1_STATE,
                DAG2_RUN1_RUN_TYPE,
                DAG2_RUN1_TRIGGERED_BY,
                None,
            ),
            (
                DAG2_ID,
                DAG2_RUN2_ID,
                DAG2_RUN2_STATE,
                DAG2_RUN2_RUN_TYPE,
                DAG2_RUN2_TRIGGERED_BY,
                None,
            ),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_get_dag_run(self, test_client, dag_id, run_id, state, run_type, triggered_by, dag_run_note):
        response = test_client.get(f"/dags/{dag_id}/dagRuns/{run_id}")
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == dag_id
        assert body["dag_run_id"] == run_id
        assert body["state"] == state
        assert body["run_type"] == run_type
        assert body["triggered_by"] == triggered_by.value
        assert body["note"] == dag_run_note

    def test_get_dag_run_not_found(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 403


class TestGetDagRuns:
    @pytest.mark.parametrize(
        ("dag_id", "total_entries"),
        [(DAG1_ID, 2), (DAG2_ID, 2), ("~", 4)],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_get_dag_runs(self, test_client, session, dag_id, total_entries):
        response = test_client.get(f"/dags/{dag_id}/dagRuns")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == total_entries
        for each in body["dag_runs"]:
            run = (
                session.query(DagRun)
                .where(DagRun.dag_id == each["dag_id"], DagRun.run_id == each["dag_run_id"])
                .one()
            )
            assert each == get_dag_run_dict(run)

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_get_dag_runs_not_found(self, test_client):
        response = test_client.get("/dags/invalid/dagRuns")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The Dag with ID: `invalid` was not found"

    def test_invalid_order_by_raises_400(self, test_client):
        response = test_client.get("/dags/test_dag1/dagRuns?order_by=invalid")
        assert response.status_code == 400
        body = response.json()
        assert (
            body["detail"]
            == "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        )

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/test_dag1/dagRuns?order_by=invalid")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/test_dag1/dagRuns?order_by=invalid")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("order_by", "expected_order"),
        [
            pytest.param("id", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_id"),
            pytest.param("state", [DAG1_RUN2_ID, DAG1_RUN1_ID], id="order_by_state"),
            pytest.param("dag_id", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_dag_id"),
            pytest.param("logical_date", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_logical_date"),
            pytest.param("dag_run_id", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_dag_run_id"),
            pytest.param("start_date", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_start_date"),
            pytest.param("end_date", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_end_date"),
            pytest.param("updated_at", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_updated_at"),
            pytest.param("conf", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_conf"),
            pytest.param("duration", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_duration"),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_return_correct_results_with_order_by(self, test_client, order_by, expected_order):
        # Test ascending order

        with assert_queries_count(7):
            response = test_client.get("/dags/test_dag1/dagRuns", params={"order_by": order_by})

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_order

        # Test descending order
        response = test_client.get("/dags/test_dag1/dagRuns", params={"order_by": f"-{order_by}"})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_order[::-1]

    @pytest.mark.parametrize(
        ("query_params", "expected_dag_id_order"),
        [
            ({}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            ({"limit": 1}, [DAG1_RUN1_ID]),
            ({"limit": 3}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            ({"offset": 1}, [DAG1_RUN2_ID]),
            ({"offset": 2}, []),
            ({"limit": 1, "offset": 1}, [DAG1_RUN2_ID]),
            ({"limit": 1, "offset": 2}, []),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_limit_and_offset(self, test_client, query_params, expected_dag_id_order):
        response = test_client.get("/dags/test_dag1/dagRuns", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_order

    @pytest.mark.parametrize(
        ("query_params", "expected_detail"),
        [
            (
                {"limit": 1, "offset": -1},
                [
                    {
                        "type": "greater_than_equal",
                        "loc": ["query", "offset"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": "-1",
                        "ctx": {"ge": 0},
                    }
                ],
            ),
            (
                {"limit": -1, "offset": 1},
                [
                    {
                        "type": "greater_than_equal",
                        "loc": ["query", "limit"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": "-1",
                        "ctx": {"ge": 0},
                    }
                ],
            ),
            (
                {"limit": -1, "offset": -1},
                [
                    {
                        "type": "greater_than_equal",
                        "loc": ["query", "offset"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": "-1",
                        "ctx": {"ge": 0},
                    },
                    {
                        "type": "greater_than_equal",
                        "loc": ["query", "limit"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": "-1",
                        "ctx": {"ge": 0},
                    },
                ],
            ),
        ],
    )
    def test_bad_limit_and_offset(self, test_client, query_params, expected_detail):
        response = test_client.get("/dags/test_dag1/dagRuns", params=query_params)
        assert response.status_code == 422
        assert response.json()["detail"] == expected_detail

    @pytest.mark.parametrize(
        ("dag_id", "query_params", "expected_dag_id_list"),
        [
            (
                DAG1_ID,
                {"logical_date_gte": LOGICAL_DATE1.isoformat()},
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (DAG2_ID, {"logical_date_lte": LOGICAL_DATE3.isoformat()}, [DAG2_RUN1_ID]),
            (
                "~",
                {
                    "start_date_gte": START_DATE1.isoformat(),
                    "start_date_lte": (START_DATE2 - timedelta(days=1)).isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                "~",
                {
                    "start_date_gt": START_DATE1.isoformat(),
                    "start_date_lt": (START_DATE2 - timedelta(days=1)).isoformat(),
                },
                [],
            ),
            (
                "~",
                {
                    "start_date_gt": (START_DATE1 - timedelta(hours=1)).isoformat(),
                    "start_date_lt": (START_DATE2 - timedelta(days=1)).isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                DAG1_ID,
                {
                    "end_date_gte": START_DATE2.isoformat(),  # 2024-04-15
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                # DAG1 runs have end_date based on START_DATE1 (2024-01-15), so all < 2024-04-15
                [],
            ),
            (
                DAG1_ID,
                {
                    "end_date_gt": START_DATE2.isoformat(),  # 2024-04-15
                    "end_date_lt": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                # DAG1 runs have end_date based on START_DATE1 (2024-01-15), so all < 2024-04-15
                [],
            ),
            (
                DAG1_ID,
                {
                    "end_date_gt": (
                        START_DATE1 + timedelta(seconds=50)
                    ).isoformat(),  # Between the two end dates
                    "end_date_lt": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],  # Both should match as their end_date > start + 50s
            ),
            (
                DAG1_ID,
                {
                    "logical_date_gte": LOGICAL_DATE1.isoformat(),
                    "logical_date_lte": LOGICAL_DATE2.isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                DAG1_ID,
                {
                    "logical_date_gt": LOGICAL_DATE1.isoformat(),
                    "logical_date_lt": LOGICAL_DATE2.isoformat(),
                },
                [],
            ),
            (
                DAG1_ID,
                {
                    "logical_date_gt": (LOGICAL_DATE1 - timedelta(hours=1)).isoformat(),
                    "logical_date_lt": LOGICAL_DATE2.isoformat(),
                },
                [DAG1_RUN1_ID],
            ),
            (
                DAG1_ID,
                {
                    "run_after_gte": RUN_AFTER1.isoformat(),
                    "run_after_lte": RUN_AFTER2.isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                DAG1_ID,
                {
                    "run_after_gt": RUN_AFTER1.isoformat(),
                    "run_after_lt": RUN_AFTER2.isoformat(),
                },
                [],
            ),
            (
                DAG1_ID,
                {
                    "run_after_gt": (RUN_AFTER1 - timedelta(hours=1)).isoformat(),
                    "run_after_lt": (RUN_AFTER2 + timedelta(hours=1)).isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                DAG2_ID,
                {
                    "start_date_gte": START_DATE2.isoformat(),
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                [DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),
            (
                DAG2_ID,
                {
                    "start_date_gt": START_DATE2.isoformat(),
                    "end_date_lt": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                [],
            ),
            (
                DAG2_ID,
                {
                    "start_date_gt": (START_DATE2 - timedelta(hours=1)).isoformat(),
                    "end_date_lt": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                [DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),
            (DAG1_ID, {"state": DagRunState.SUCCESS.value}, [DAG1_RUN1_ID]),
            (DAG2_ID, {"state": DagRunState.FAILED.value}, []),
            (
                DAG1_ID,
                {
                    "state": DagRunState.SUCCESS.value,
                    "logical_date_gte": LOGICAL_DATE1.isoformat(),
                },
                [DAG1_RUN1_ID],
            ),
            (
                DAG1_ID,
                {
                    "state": DagRunState.FAILED.value,
                    "start_date_gte": START_DATE1.isoformat(),
                },
                [DAG1_RUN2_ID],
            ),
            (DAG1_ID, {"run_id_pattern": DAG1_RUN1_ID}, [DAG1_RUN1_ID]),
            (DAG2_ID, {"run_id_pattern": DAG2_RUN1_ID}, [DAG2_RUN1_ID]),
            ("~", {"run_id_pattern": DAG1_RUN1_ID}, [DAG1_RUN1_ID]),
            ("~", {"run_id_pattern": "non_existent_run_id"}, []),
            (DAG1_ID, {"run_id_pattern": "run_1"}, [DAG1_RUN1_ID]),
            (DAG1_ID, {"run_id_pattern": "dag_%_1"}, [DAG1_RUN1_ID]),
            ("~", {"run_id_pattern": "dag_run_"}, [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID]),
            (
                DAG1_ID,
                {
                    "run_id_pattern": DAG1_RUN1_ID,
                    "state": DagRunState.SUCCESS.value,
                },
                [DAG1_RUN1_ID],
            ),
            # Test triggering_user_name_pattern filter
            (DAG1_ID, {"triggering_user_name_pattern": "alice_admin"}, [DAG1_RUN1_ID]),
            (DAG1_ID, {"triggering_user_name_pattern": "bob_service"}, [DAG1_RUN2_ID]),
            (DAG2_ID, {"triggering_user_name_pattern": "service_account"}, [DAG2_RUN1_ID]),
            ("~", {"triggering_user_name_pattern": "alice%"}, [DAG1_RUN1_ID]),
            ("~", {"triggering_user_name_pattern": "%service%"}, [DAG1_RUN2_ID, DAG2_RUN1_ID]),
            ("~", {"triggering_user_name_pattern": "nonexistent"}, []),
            (
                DAG1_ID,
                {
                    "triggering_user_name_pattern": "alice%",
                    "state": DagRunState.SUCCESS.value,
                },
                [DAG1_RUN1_ID],
            ),
            # Test dag_id_pattern filter
            ("~", {"dag_id_pattern": "test_dag1"}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            ("~", {"dag_id_pattern": "test_dag2"}, [DAG2_RUN1_ID, DAG2_RUN2_ID]),
            ("~", {"dag_id_pattern": "test_%"}, [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID]),
            ("~", {"dag_id_pattern": "%_dag1"}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            ("~", {"dag_id_pattern": "%_dag2"}, [DAG2_RUN1_ID, DAG2_RUN2_ID]),
            ("~", {"dag_id_pattern": "test_dag_"}, [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID]),
            ("~", {"dag_id_pattern": "nonexistent"}, []),
            (
                "~",
                {
                    "dag_id_pattern": "test_dag1",
                    "state": DagRunState.SUCCESS.value,
                },
                [DAG1_RUN1_ID],
            ),
            # Test dag_version filter
            (
                DAG1_ID,
                {"dag_version": [1]},
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),  # Version 1 should match all DAG1 runs
            (
                DAG2_ID,
                {"dag_version": [1]},
                [DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),  # Version 1 should match all DAG2 runs
            (
                "~",
                {"dag_version": [1]},
                [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),  # Version 1 should match all runs
            ("~", {"dag_version": [999]}, []),  # Non-existent version should match no runs
            (
                DAG1_ID,
                {"dag_version": [1, 999]},
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),  # Multiple versions, only existing ones match
            # Test duration filters
            ("~", {"duration_gte": 200}, [DAG1_RUN2_ID]),  # Test >= 200 seconds
            ("~", {"duration_lt": 100}, [DAG2_RUN1_ID]),  # Test < 100 seconds
            (
                "~",
                {"duration_gte": 100, "duration_lte": 150},
                [DAG1_RUN1_ID, DAG2_RUN2_ID],
            ),  # Test between 100 and 150 (inclusive)
            # Test conf_contains filter
            ("~", {"conf_contains": "development"}, [DAG1_RUN1_ID]),  # Test for "development" env
            (
                "~",
                {"conf_contains": "debug"},
                [DAG1_RUN2_ID],
            ),  # Test for debug key
            ("~", {"conf_contains": "version"}, [DAG1_RUN1_ID]),  # Test for the key "version"
            ("~", {"conf_contains": "non_existent_key"}, []),  # Test for a key that doesn't exist
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_filters(self, test_client, dag_id, query_params, expected_dag_id_list):
        response = test_client.get(f"/dags/{dag_id}/dagRuns", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_list

    def test_bad_filters(self, test_client):
        query_params = {
            "logical_date_gte": "invalid",
            "start_date_gte": "invalid",
            "end_date_gte": "invalid",
            "run_after_gte": "invalid",
            "logical_date_lte": "invalid",
            "start_date_lte": "invalid",
            "end_date_lte": "invalid",
            "run_after_lte": "invalid",
        }
        expected_detail = [
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "run_after_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "run_after_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "logical_date_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "logical_date_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "start_date_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "start_date_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "end_date_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["query", "end_date_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
        ]
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns", params=query_params)
        assert response.status_code == 422
        body = response.json()
        assert body["detail"] == expected_detail

    def test_invalid_state(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns", params={"state": ["invalid"]})
        assert response.status_code == 422
        assert (
            response.json()["detail"] == f"Invalid value for state. Valid values are {', '.join(DagRunState)}"
        )

    def test_invalid_dag_version(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns", params={"dag_version": ["invalid"]})
        assert response.status_code == 422
        body = response.json()
        assert body["detail"][0]["type"] == "int_parsing"
        assert "dag_version" in body["detail"][0]["loc"]


class TestListDagRunsBatch:
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_list_dag_runs_return_200(self, test_client, session):
        with assert_queries_count(5):
            response = test_client.post("/dags/~/dagRuns/list", json={})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        for each in body["dag_runs"]:
            run = session.query(DagRun).where(DagRun.run_id == each["dag_run_id"]).one()
            expected = get_dag_run_dict(run)
            assert each == expected

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/dags/~/dagRuns/list", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/dags/~/dagRuns/list", json={})
        assert response.status_code == 403

    def test_list_dag_runs_with_invalid_dag_id(self, test_client):
        response = test_client.post("/dags/invalid/dagRuns/list", json={})
        assert response.status_code == 422
        body = response.json()
        assert body["detail"] == [
            {
                "type": "literal_error",
                "loc": ["path", "dag_id"],
                "msg": "Input should be '~'",
                "input": "invalid",
                "ctx": {"expected": "'~'"},
            }
        ]

    @pytest.mark.parametrize(
        ("dag_ids", "status_code", "expected_dag_id_list"),
        [
            ([], 200, DAG_RUNS_LIST),
            ([DAG1_ID], 200, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            [["invalid"], 200, []],
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_list_dag_runs_with_dag_ids_filter(self, test_client, dag_ids, status_code, expected_dag_id_list):
        with assert_queries_count(5):
            response = test_client.post("/dags/~/dagRuns/list", json={"dag_ids": dag_ids})
        assert response.status_code == status_code
        assert set([each["dag_run_id"] for each in response.json()["dag_runs"]]) == set(expected_dag_id_list)

    def test_invalid_order_by_raises_400(self, test_client):
        response = test_client.post("/dags/~/dagRuns/list", json={"order_by": "invalid"})
        assert response.status_code == 400
        body = response.json()
        assert (
            body["detail"]
            == "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        )

    @pytest.mark.parametrize(
        ("order_by", "expected_order"),
        [
            pytest.param("id", DAG_RUNS_LIST, id="order_by_id"),
            pytest.param(
                "state", [DAG1_RUN2_ID, DAG1_RUN1_ID, DAG2_RUN1_ID, DAG2_RUN2_ID], id="order_by_state"
            ),
            pytest.param("dag_id", DAG_RUNS_LIST, id="order_by_dag_id"),
            pytest.param("run_after", DAG_RUNS_LIST, id="order_by_run_after"),
            pytest.param("logical_date", DAG_RUNS_LIST, id="order_by_logical_date"),
            pytest.param("dag_run_id", DAG_RUNS_LIST, id="order_by_dag_run_id"),
            pytest.param("start_date", DAG_RUNS_LIST, id="order_by_start_date"),
            pytest.param("end_date", DAG_RUNS_LIST, id="order_by_end_date"),
            pytest.param("updated_at", DAG_RUNS_LIST, id="order_by_updated_at"),
            pytest.param("conf", DAG_RUNS_LIST, id="order_by_conf"),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_dag_runs_ordering(self, test_client, order_by, expected_order):
        # Test ascending order
        response = test_client.post("/dags/~/dagRuns/list", json={"order_by": order_by})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [run["dag_run_id"] for run in body["dag_runs"]] == expected_order

        # Test descending order
        response = test_client.post("/dags/~/dagRuns/list", json={"order_by": f"-{order_by}"})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [run["dag_run_id"] for run in body["dag_runs"]] == expected_order[::-1]

    @pytest.mark.parametrize(
        ("post_body", "expected_dag_id_order"),
        [
            ({}, DAG_RUNS_LIST),
            ({"page_limit": 1}, DAG_RUNS_LIST[:1]),
            ({"page_limit": 3}, DAG_RUNS_LIST[:3]),
            ({"page_offset": 1}, DAG_RUNS_LIST[1:]),
            ({"page_offset": 5}, []),
            ({"page_limit": 1, "page_offset": 1}, DAG_RUNS_LIST[1:2]),
            ({"page_limit": 1, "page_offset": 2}, DAG_RUNS_LIST[2:3]),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_limit_and_offset(self, test_client, post_body, expected_dag_id_order):
        response = test_client.post("/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_order

    @pytest.mark.parametrize(
        ("post_body", "expected_detail"),
        [
            (
                {"page_limit": 1, "page_offset": -1},
                [
                    {
                        "type": "greater_than_equal",
                        "loc": ["body", "page_offset"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": -1,
                        "ctx": {"ge": 0},
                    }
                ],
            ),
            (
                {"page_limit": -1, "page_offset": 1},
                [
                    {
                        "type": "greater_than_equal",
                        "loc": ["body", "page_limit"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": -1,
                        "ctx": {"ge": 0},
                    }
                ],
            ),
            (
                {"page_limit": -1, "page_offset": -1},
                [
                    {
                        "type": "greater_than_equal",
                        "loc": ["body", "page_offset"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": -1,
                        "ctx": {"ge": 0},
                    },
                    {
                        "type": "greater_than_equal",
                        "loc": ["body", "page_limit"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": -1,
                        "ctx": {"ge": 0},
                    },
                ],
            ),
        ],
    )
    def test_bad_limit_and_offset(self, test_client, post_body, expected_detail):
        response = test_client.post("/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 422
        assert response.json()["detail"] == expected_detail

    @pytest.mark.parametrize(
        ("post_body", "expected_dag_id_list"),
        [
            (
                {"logical_date_gte": LOGICAL_DATE1.isoformat()},
                DAG_RUNS_LIST,
            ),
            ({"logical_date_lte": LOGICAL_DATE3.isoformat()}, DAG_RUNS_LIST[:3]),
            (
                {
                    "start_date_gte": START_DATE1.isoformat(),
                    "start_date_lte": (START_DATE2 - timedelta(days=1)).isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                {
                    "end_date_gte": START_DATE2.isoformat(),  # 2024-04-15
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                # Only DAG2 runs match: their start_date is 2024-04-15, so end_date >= 2024-04-15
                # DAG1 runs have start_date 2024-01-15, so end_date < 2024-04-15
                [DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),
            (
                {
                    "logical_date_gte": LOGICAL_DATE1.isoformat(),
                    "logical_date_lte": LOGICAL_DATE2.isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),
            (
                {
                    "start_date_gte": START_DATE2.isoformat(),
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                [DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),
            (
                {"states": [DagRunState.SUCCESS.value]},
                [DAG1_RUN1_ID, DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),
            ({"states": [DagRunState.FAILED.value]}, [DAG1_RUN2_ID]),
            (
                {
                    "states": [DagRunState.SUCCESS.value],
                    "logical_date_gte": LOGICAL_DATE2.isoformat(),
                },
                DAG_RUNS_LIST[2:],
            ),
            (
                {
                    "states": [DagRunState.FAILED.value],
                    "start_date_gte": START_DATE1.isoformat(),
                },
                [DAG1_RUN2_ID],
            ),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_filters(self, test_client, post_body, expected_dag_id_list):
        response = test_client.post("/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 200
        body = response.json()
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_list

    def test_bad_filters(self, test_client):
        post_body = {
            "logical_date_gte": "invalid",
            "start_date_gte": "invalid",
            "end_date_gte": "invalid",
            "logical_date_lte": "invalid",
            "start_date_lte": "invalid",
            "end_date_lte": "invalid",
            "dag_ids": "invalid",
        }
        expected_detail = [
            {
                "input": "invalid",
                "loc": ["body", "dag_ids"],
                "msg": "Input should be a valid list",
                "type": "list_type",
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["body", "logical_date_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["body", "logical_date_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["body", "start_date_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["body", "start_date_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["body", "end_date_gte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
            {
                "type": "datetime_from_date_parsing",
                "loc": ["body", "end_date_lte"],
                "msg": "Input should be a valid datetime or date, input is too short",
                "input": "invalid",
                "ctx": {"error": "input is too short"},
            },
        ]
        response = test_client.post("/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 422
        body = response.json()
        assert body["detail"] == expected_detail

    @pytest.mark.parametrize(
        ("post_body", "expected_response"),
        [
            (
                {"states": ["invalid"]},
                [
                    {
                        "type": "enum",
                        "loc": ["body", "states", 0],
                        "msg": "Input should be 'queued', 'running', 'success' or 'failed'",
                        "input": "invalid",
                        "ctx": {"expected": "'queued', 'running', 'success' or 'failed'"},
                    }
                ],
            ),
            (
                {"states": "invalid"},
                [
                    {
                        "type": "list_type",
                        "loc": ["body", "states"],
                        "msg": "Input should be a valid list",
                        "input": "invalid",
                    }
                ],
            ),
        ],
    )
    def test_invalid_state(self, test_client, post_body, expected_response):
        response = test_client.post("/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 422
        assert response.json()["detail"] == expected_response


class TestPatchDagRun:
    @pytest.mark.parametrize(
        ("dag_id", "run_id", "patch_body", "response_body", "note_data"),
        [
            (
                DAG1_ID,
                DAG1_RUN1_ID,
                {"state": DagRunState.FAILED, "note": "new_note2"},
                {"state": DagRunState.FAILED, "note": "new_note2"},
                {"user_id": "test", "content": "new_note2"},
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                {"state": DagRunState.SUCCESS},
                {"state": DagRunState.SUCCESS, "note": None},
                None,
            ),
            (
                DAG2_ID,
                DAG2_RUN1_ID,
                {"state": DagRunState.QUEUED},
                {"state": DagRunState.QUEUED, "note": None},
                None,
            ),
            (
                DAG1_ID,
                DAG1_RUN1_ID,
                {"note": "updated note"},
                {"state": DagRunState.SUCCESS, "note": "updated note"},
                {"user_id": "test", "content": "updated note"},
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                {"note": "new note", "state": DagRunState.FAILED},
                {"state": DagRunState.FAILED, "note": "new note"},
                {"user_id": "test", "content": "new note"},
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                {"note": None},
                {"state": DagRunState.FAILED, "note": None},
                {"user_id": "test", "content": None},
            ),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_patch_dag_run(self, test_client, dag_id, run_id, patch_body, response_body, note_data, session):
        response = test_client.patch(f"/dags/{dag_id}/dagRuns/{run_id}", json=patch_body)
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == dag_id
        assert body["dag_run_id"] == run_id
        assert body.get("state") == response_body.get("state")
        assert body.get("note") == response_body.get("note")

        _check_dag_run_note(session, run_id, note_data)
        _check_last_log(session, dag_id=dag_id, event="patch_dag_run", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch("/dags/dag_1/dagRuns/run_1", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch("/dags/dag_1/dagRuns/run_1", json={})
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("query_params", "patch_body", "response_body", "expected_status_code", "note_data"),
        [
            (
                {"update_mask": ["state"]},
                {"state": DagRunState.SUCCESS},
                {"state": "success"},
                200,
                {"user_id": "not_test", "content": "test_note"},
            ),
            (
                {"update_mask": ["note"]},
                {"state": DagRunState.FAILED, "note": "new_note1"},
                {"note": "new_note1", "state": "success"},
                200,
                {"user_id": "test", "content": "new_note1"},
            ),
            (
                {},
                {"state": DagRunState.FAILED, "note": "new_note2"},
                {"note": "new_note2", "state": "failed"},
                200,
                {"user_id": "test", "content": "new_note2"},
            ),
            (
                {"update_mask": ["note"]},
                {},
                {"state": "success", "note": "test_note"},
                200,
                {"user_id": "not_test", "content": "test_note"},
            ),
            (
                {"update_mask": ["note"]},
                {"note": None},
                {"state": "success", "note": None},
                200,
                {"user_id": "test", "content": None},
            ),
            (
                {"update_mask": ["random"]},
                {"state": DagRunState.FAILED},
                {"state": "success", "note": "test_note"},
                200,
                {"user_id": "not_test", "content": "test_note"},
            ),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_patch_dag_run_with_update_mask(
        self, test_client, query_params, patch_body, response_body, expected_status_code, note_data, session
    ):
        response = test_client.patch(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}",
            params=query_params,
            json=patch_body,
        )
        response_json = response.json()
        assert response.status_code == expected_status_code
        for key, value in response_body.items():
            assert response_json.get(key) == value
        _check_dag_run_note(session, DAG1_RUN1_ID, note_data)

    def test_patch_dag_run_not_found(self, test_client):
        response = test_client.patch(
            f"/dags/{DAG1_ID}/dagRuns/invalid",
            json={"state": DagRunState.SUCCESS},
        )
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"

    def test_patch_dag_run_bad_request(self, test_client):
        response = test_client.patch(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}", json={"state": "running"})
        assert response.status_code == 422
        body = response.json()
        assert body["detail"][0]["msg"] == "Input should be 'queued', 'success' or 'failed'"

    @pytest.fixture(autouse=True)
    def clean_listener_manager(self):
        get_listener_manager().clear()
        yield
        get_listener_manager().clear()

    @pytest.mark.parametrize(
        ("state", "listener_state"),
        [
            ("queued", []),
            ("success", [DagRunState.SUCCESS]),
            ("failed", [DagRunState.FAILED]),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_patch_dag_run_notifies_listeners(self, test_client, state, listener_state):
        from unit.listeners.class_listener import ClassBasedListener

        listener = ClassBasedListener()
        get_listener_manager().add_listener(listener)
        response = test_client.patch(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}", json={"state": state})
        assert response.status_code == 200
        assert listener.state == listener_state


class TestDeleteDagRun:
    def test_delete_dag_run(self, test_client, session):
        response = test_client.delete(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}")
        assert response.status_code == 204
        _check_last_log(session, dag_id=DAG1_ID, event="delete_dag_run", logical_date=None)

    def test_delete_dag_run_not_found(self, test_client):
        response = test_client.delete(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 403

    def test_delete_dag_run_running_state(self, test_client, dag_maker, session):
        """Should not allow deleting a dag run in RUNNING state."""
        with dag_maker(DAG1_ID, schedule=None, start_date=START_DATE1, serialized=True):
            EmptyOperator(task_id="task_1")
        dag_run = dag_maker.create_dagrun(
            run_id="run_running",
            state=DagRunState.RUNNING,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.UI,
            logical_date=LOGICAL_DATE1,
        )
        session.flush()
        response = test_client.delete(f"/dags/{DAG1_ID}/dagRuns/run_running")
        assert response.status_code == 400
        assert "Cannot delete DagRun in state" in response.json()["detail"]

    def test_delete_dag_run_allowed_states(self, test_client, dag_maker, session):
        """Should allow deleting dag runs in allowed states (QUEUED, SUCCESS, FAILED)."""
        allowed_states = [DagRunState.QUEUED, DagRunState.SUCCESS, DagRunState.FAILED]
        for state in allowed_states:
            run_id = f"run_{state}"
            with dag_maker(DAG1_ID, schedule=None, start_date=START_DATE1, serialized=True):
                EmptyOperator(task_id="task_1")
            dag_run = dag_maker.create_dagrun(
                run_id=run_id,
                state=state,
                run_type=DagRunType.MANUAL,
                triggered_by=DagRunTriggeredByType.UI,
                logical_date=LOGICAL_DATE1,
            )
            session.flush()
            response = test_client.delete(f"/dags/{DAG1_ID}/dagRuns/{run_id}")
            assert response.status_code == 204


class TestGetDagRunAssetTriggerEvents:
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200(self, test_client, dag_maker, session):
        asset1 = Asset(name="ds1", uri="file:///da1")

        with dag_maker(dag_id="source_dag", start_date=START_DATE1, session=session):
            EmptyOperator(task_id="task", outlets=[asset1])
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]

        asset1_id = session.query(AssetModel.id).filter_by(uri=asset1.uri).scalar()
        event = AssetEvent(
            asset_id=asset1_id,
            source_task_id=ti.task_id,
            source_dag_id=ti.dag_id,
            source_run_id=ti.run_id,
            source_map_index=ti.map_index,
        )
        session.add(event)

        with dag_maker(dag_id="TEST_DAG_ID", start_date=START_DATE1, session=session):
            pass
        dr = dag_maker.create_dagrun(run_id="TEST_DAG_RUN_ID", run_type=DagRunType.ASSET_TRIGGERED)
        dr.consumed_asset_events.append(event)

        session.commit()
        assert event.timestamp

        with assert_queries_count(3):
            response = test_client.get(
                "/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamAssetEvents",
            )
        assert response.status_code == 200
        expected_response = {
            "asset_events": [
                {
                    "timestamp": from_datetime_to_zulu(event.timestamp),
                    "asset_id": asset1_id,
                    "uri": "file:///da1",
                    "extra": {},
                    "id": event.id,
                    "group": "asset",
                    "name": "ds1",
                    "source_dag_id": ti.dag_id,
                    "source_map_index": ti.map_index,
                    "source_run_id": ti.run_id,
                    "source_task_id": ti.task_id,
                    "created_dagruns": [
                        {
                            "dag_id": "TEST_DAG_ID",
                            "run_id": "TEST_DAG_RUN_ID",
                            "data_interval_end": from_datetime_to_zulu_without_ms(dr.data_interval_end),
                            "data_interval_start": from_datetime_to_zulu_without_ms(dr.data_interval_start),
                            "end_date": None,
                            "logical_date": from_datetime_to_zulu_without_ms(dr.logical_date),
                            "start_date": from_datetime_to_zulu_without_ms(dr.start_date),
                            "state": "running",
                        }
                    ],
                    "partition_key": None,
                }
            ],
            "total_entries": 1,
        }
        assert response.json() == expected_response

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamAssetEvents",
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamAssetEvents"
        )
        assert response.status_code == 403

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            "/dags/invalid-id/dagRuns/invalid-run-id/upstreamAssetEvents",
        )
        assert response.status_code == 404
        assert (
            response.json()["detail"]
            == "The DagRun with dag_id: `invalid-id` and run_id: `invalid-run-id` was not found"
        )


class TestClearDagRun:
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run(self, test_client, session):
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == DAG1_ID
        assert body["dag_run_id"] == DAG1_RUN1_ID
        assert body["state"] == "queued"
        _check_last_log(
            session,
            dag_id=DAG1_ID,
            event="clear_dag_run",
            logical_date=None,
        )

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": False},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": False},
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("body", "dag_run_id", "expected_state"),
        [
            [{"dry_run": True}, DAG1_RUN1_ID, ["success", "success"]],
            [{}, DAG1_RUN1_ID, ["success", "success"]],
            [{}, DAG1_RUN2_ID, ["success", "failed"]],
            [{"only_failed": True}, DAG1_RUN2_ID, ["failed"]],
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_dry_run(self, test_client, session, body, dag_run_id, expected_state):
        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns/{dag_run_id}/clear", json=body)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == len(expected_state)
        for index, each in enumerate(sorted(body["task_instances"], key=lambda x: x["task_id"])):
            assert each["state"] == expected_state[index]
        dag_run = session.scalar(select(DagRun).filter_by(dag_id=DAG1_ID, run_id=DAG1_RUN1_ID))
        assert dag_run.state == DAG1_RUN1_STATE

        logs = (
            session.query(Log)
            .filter(Log.dag_id == DAG1_ID, Log.run_id == dag_run_id, Log.event == "clear_dag_run")
            .count()
        )
        assert logs == 0

    def test_clear_dag_run_not_found(self, test_client):
        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns/invalid/clear", json={"dry_run": False})
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"

    def test_clear_dag_run_unprocessable_entity(self, test_client):
        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear")
        assert response.status_code == 422
        body = response.json()
        assert body["detail"][0]["msg"] == "Field required"
        assert body["detail"][0]["loc"][0] == "body"


class TestTriggerDagRun:
    def _dags_for_trigger_tests(self, session=None):
        inactive_dag = DagModel(
            dag_id="inactive",
            bundle_name="testing",
            fileloc="/tmp/dag_del_1.py",
            timetable_summary="2 2 * * *",
            is_stale=True,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        import_errors_dag = DagModel(
            dag_id="import_errors",
            bundle_name="testing",
            fileloc="/tmp/dag_del_2.py",
            timetable_summary="2 2 * * *",
            is_stale=False,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        import_errors_dag.has_import_errors = True

        session.add(inactive_dag)
        session.add(import_errors_dag)
        session.commit()

    @time_machine.travel(timezone.utcnow(), tick=False)
    @pytest.mark.parametrize(
        ("dag_run_id", "note", "data_interval_start", "data_interval_end", "note_data"),
        [
            ("dag_run_5", "test-note", None, None, {"user_id": "test", "content": "test-note"}),
            (
                "dag_run_6",
                "test-note",
                "2024-01-03T00:00:00+00:00",
                "2024-01-04T05:00:00+00:00",
                {"user_id": "test", "content": "test-note"},
            ),
            (None, None, None, None, None),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200(
        self, test_client, dag_run_id, note, data_interval_start, data_interval_end, note_data, session
    ):
        fixed_now = timezone.utcnow().isoformat()

        request_json = {"note": note, "logical_date": fixed_now}
        if dag_run_id is not None:
            request_json["dag_run_id"] = dag_run_id
        if data_interval_start is not None:
            request_json["data_interval_start"] = data_interval_start
        if data_interval_end is not None:
            request_json["data_interval_end"] = data_interval_end
        request_json["logical_date"] = fixed_now
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json=request_json,
        )
        assert response.status_code == 200

        if dag_run_id is None:
            expected_dag_run_id = f"manual__{fixed_now}"
        else:
            expected_dag_run_id = dag_run_id

        expected_data_interval_start = fixed_now.replace("+00:00", "Z")
        expected_data_interval_end = fixed_now.replace("+00:00", "Z")
        if data_interval_start is not None and data_interval_end is not None:
            expected_data_interval_start = data_interval_start.replace("+00:00", "Z")
            expected_data_interval_end = data_interval_end.replace("+00:00", "Z")
        expected_logical_date = fixed_now.replace("+00:00", "Z")

        run = (
            session.query(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == expected_dag_run_id).one()
        )

        expected_response_json = {
            "bundle_version": None,
            "conf": {},
            "dag_display_name": DAG1_DISPLAY_NAME,
            "dag_id": DAG1_ID,
            "dag_run_id": expected_dag_run_id,
            "dag_versions": get_dag_versions_dict(run.dag_versions),
            "end_date": None,
            "logical_date": expected_logical_date,
            "run_after": fixed_now.replace("+00:00", "Z"),
            "start_date": None,
            "duration": None,
            "run_type": "manual",
            "state": "queued",
            "data_interval_end": expected_data_interval_end,
            "data_interval_start": expected_data_interval_start,
            "queued_at": fixed_now.replace("+00:00", "Z"),
            "last_scheduling_decision": None,
            "note": note,
            "triggered_by": "rest_api",
            "triggering_user_name": "test",
            "partition_key": None,
        }

        assert response.json() == expected_response_json

        _check_dag_run_note(session, expected_dag_run_id, note_data)

        _check_last_log(session, dag_id=DAG1_ID, event="trigger_dag_run", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={},
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("post_body", "expected_detail"),
        [
            (
                {"executiondate": "2020-11-10T08:25:56Z"},
                {
                    "detail": [
                        {
                            "input": "2020-11-10T08:25:56Z",
                            "loc": ["body", "executiondate"],
                            "msg": "Extra inputs are not permitted",
                            "type": "extra_forbidden",
                        }
                    ]
                },
            ),
            (
                {"data_interval_start": "2020-11-10T08:25:56"},
                {
                    "detail": [
                        {
                            "input": "2020-11-10T08:25:56",
                            "loc": ["body", "data_interval_start"],
                            "msg": "Input should have timezone info",
                            "type": "timezone_aware",
                        }
                    ]
                },
            ),
            (
                {"data_interval_end": "2020-11-10T08:25:56"},
                {
                    "detail": [
                        {
                            "input": "2020-11-10T08:25:56",
                            "loc": ["body", "data_interval_end"],
                            "msg": "Input should have timezone info",
                            "type": "timezone_aware",
                        }
                    ]
                },
            ),
            (
                {"dag_run_id": 20},
                {
                    "detail": [
                        {
                            "input": 20,
                            "loc": ["body", "dag_run_id"],
                            "msg": "Input should be a valid string",
                            "type": "string_type",
                        }
                    ]
                },
            ),
            (
                {"note": 20},
                {
                    "detail": [
                        {
                            "input": 20,
                            "loc": ["body", "note"],
                            "msg": "Input should be a valid string",
                            "type": "string_type",
                        }
                    ]
                },
            ),
            (
                {"conf": 20},
                {
                    "detail": [
                        {
                            "input": 20,
                            "loc": ["body", "conf"],
                            "msg": "Input should be a valid dictionary",
                            "type": "dict_type",
                        }
                    ]
                },
            ),
        ],
    )
    def test_invalid_data(self, test_client, post_body, expected_detail):
        now = timezone.utcnow().isoformat()
        post_body["logical_date"] = now
        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns", json=post_body)
        assert response.status_code == 422
        assert response.json() == expected_detail

    def test_post_dag_runs_with_empty_payload(self, test_client):
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns", data={}, headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422
        body = response.json()
        assert body["detail"] == [
            {
                "input": None,
                "loc": ["body"],
                "msg": "Field required",
                "type": "missing",
            },
        ]

    @mock.patch("airflow.serialization.serialized_objects.SerializedDAG.create_dagrun")
    def test_dagrun_creation_exception_is_handled(self, mock_create_dagrun, test_client):
        now = timezone.utcnow().isoformat()
        error_message = "Encountered Error"

        mock_create_dagrun.side_effect = ValueError(error_message)

        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns", json={"logical_date": now})
        assert response.status_code == 400
        assert response.json() == {"detail": error_message}

    def test_should_respond_404_if_a_dag_is_inactive(self, test_client, session, testing_dag_bundle):
        now = timezone.utcnow().isoformat()
        self._dags_for_trigger_tests(session)
        response = test_client.post("/dags/inactive/dagRuns", json={"logical_date": now})
        assert response.status_code == 404
        assert response.json()["detail"] == "DAG with dag_id: 'inactive' not found"

    def test_should_respond_400_if_a_dag_has_import_errors(self, test_client, session, testing_dag_bundle):
        now = timezone.utcnow().isoformat()
        self._dags_for_trigger_tests(session)
        response = test_client.post("/dags/import_errors/dagRuns", json={"logical_date": now})
        assert response.status_code == 400
        assert (
            response.json()["detail"]
            == "DAG with dag_id: 'import_errors' has import errors and cannot be triggered"
        )

    @time_machine.travel("2025-10-02 12:00:00", tick=False)
    @pytest.mark.usefixtures("custom_timetable_plugin")
    def test_custom_timetable_generate_run_id_for_manual_trigger(self, dag_maker, test_client, session):
        """Test that custom timetable's generate_run_id is used for manual triggers (issue #55908)."""
        custom_dag_id = "test_custom_timetable_dag"
        with dag_maker(
            dag_id=custom_dag_id,
            schedule=CustomTimetable("0 0 * * *", timezone="UTC"),
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="test_task")

        session.commit()

        logical_date = datetime(2025, 10, 1, 0, 0, 0, tzinfo=timezone.utc)
        response = test_client.post(
            f"/dags/{custom_dag_id}/dagRuns",
            json={"logical_date": logical_date.isoformat()},
        )
        assert response.status_code == 200
        run_id_with_logical_date = response.json()["dag_run_id"]
        assert run_id_with_logical_date.startswith("custom_")

        run = session.query(DagRun).filter(DagRun.run_id == run_id_with_logical_date).one()
        assert run.dag_id == custom_dag_id

        response = test_client.post(
            f"/dags/{custom_dag_id}/dagRuns",
            json={"logical_date": None},
        )
        assert response.status_code == 200
        run_id_without_logical_date = response.json()["dag_run_id"]
        assert run_id_without_logical_date.startswith("custom_manual_")

        run = session.query(DagRun).filter(DagRun.run_id == run_id_without_logical_date).one()
        assert run.dag_id == custom_dag_id


class TestWaitDagRun:
    # The way we init async engine does not work well with FastAPI app init.
    # Creating the engine implicitly creates an event loop, which Airflow does
    # once for the entire process; creating the FastAPI app also does, but our
    # test setup does it once for each test. I don't know how to properly fix
    # this without rewriting how Airflow does db; re-configuring the db for each
    # test at least makes the tests run correctly.
    @pytest.fixture(autouse=True)
    def reconfigure_async_db_engine(self):
        from airflow.settings import _configure_async_session

        _configure_async_session()

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait",
            params={"interval": "1"},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait",
            params={"interval": "1"},
        )
        assert response.status_code == 403

    def test_should_respond_404(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns/does-not-exist/wait", params={"interval": "1"})
        assert response.status_code == 404

    def test_should_respond_422_without_interval_param(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait")
        assert response.status_code == 422

    @pytest.mark.parametrize(
        ("run_id", "state"),
        [(DAG1_RUN1_ID, DAG1_RUN1_STATE), (DAG1_RUN2_ID, DAG1_RUN2_STATE)],
    )
    def test_should_respond_200_immediately_for_finished_run(self, test_client, run_id, state):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns/{run_id}/wait", params={"interval": "100"})
        assert response.status_code == 200
        data = response.json()
        assert data == {"state": state}

    def test_collect_task(self, test_client):
        response = test_client.get(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait", params={"interval": "1", "result": "task_1"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data == {"state": DagRunState.SUCCESS, "results": {"task_1": '"result_1"'}}

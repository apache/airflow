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

import math
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import time_machine
from fastapi.testclient import TestClient
from sqlalchemy import func, select, update

from airflow import plugins_manager
from airflow._shared.module_loading import qualname
from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity, DagDetails
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.common.dagbag import resolve_run_on_latest_version
from airflow.api_fastapi.core_api.datamodels.dag_versions import DagVersionResponse
from airflow.exceptions import ParamValidationError
from airflow.models import DagModel, DagRun, Log
from airflow.models.asset import AssetEvent, AssetModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Param, result, task
from airflow.settings import _configure_async_session
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.timetables.simple import PartitionedAssetTimetable, PartitionedAtRuntime
from airflow.timetables.trigger import CronPartitionTimetable
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.api_fastapi import _check_dag_run_note, _check_last_log
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_connections,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu, from_datetime_to_zulu_without_ms
from tests_common.test_utils.taskinstance import run_task_instance
from unit.listeners.class_listener import ClassBasedListener

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
    timetable_class_name = qualname(CustomTimetable)
    existing_timetables = getattr(plugins_manager, "timetable_classes", None) or {}

    monkeypatch.setattr(
        plugins_manager,
        "get_timetables_plugins",
        lambda: {**existing_timetables, timetable_class_name: CustomTimetable},
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
def setup(request, dag_maker, *, session=None):
    clear_db_connections()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_logs()
    clear_db_assets()

    if "no_setup" in request.keywords:
        return

    with dag_maker(DAG1_ID, schedule=None, start_date=START_DATE1, serialized=True):
        task1 = EmptyOperator(task_id="task_1")
        task2 = EmptyOperator(task_id="task_2")
        dag_maker.dag.add_result(task2.output)

    dag_run1 = dag_maker.create_dagrun(
        run_id=DAG1_RUN1_ID,
        state=DAG1_RUN1_STATE,
        run_type=DAG1_RUN1_RUN_TYPE,
        triggered_by=DAG1_RUN1_TRIGGERED_BY,
        logical_date=LOGICAL_DATE1,
    )
    # Set triggering_user_name for testing
    dag_run1.triggering_user_name = "alice_admin"
    # Set partition_key for testing partition_key_pattern / partition_key_prefix_pattern filters.
    # The value uses the ProductMapper default delimiter (|) to form a composite key so we can
    # verify that the filter treats | as a literal character, not an OR separator.
    dag_run1.partition_key = "2026-01-01|us"
    # Set a real partition_date so the GET/list responses exercise the serialized
    # (non-None) partition_date path, not just the None case.
    dag_run1.partition_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    dag_run1.note = (DAG1_RUN1_NOTE, "not_test")
    # Set end_date for testing duration filter
    dag_run1.end_date = dag_run1.start_date + timedelta(seconds=101)
    # Set conf for testing conf_contains filter (values ordered for predictable sorting)
    dag_run1.conf = {"env": "development", "version": "1.0"}

    for i, t in enumerate([task1, task2], start=1):
        ti = dag_run1.get_task_instance(task_id=t.task_id)
        ti.task = t
        ti.state = State.SUCCESS
        session.merge(ti)
        XComModel.set(
            key="return_value",
            value=f"result_{i}",
            task_id=ti.task_id,
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            dag_result=t.returns_dag_result,
            session=session,
        )

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
    # Set partition_key for testing: plain single-dimension key to pair with dag_run1's composite key.
    dag_run3.partition_key = "us"
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

    asset1 = AssetModel(name="sales", uri="s3://bucket/sales")
    asset2 = AssetModel(name="customer", uri="s3://bucket/customer")
    session.add_all([asset1, asset2])
    session.flush()

    event1 = AssetEvent(
        asset_id=asset1.id,
        source_dag_id="source_dag",
        source_run_id="source_run",
        source_task_id="source_task",
    )
    event2 = AssetEvent(
        asset_id=asset2.id,
        source_dag_id="source_dag",
        source_run_id="source_run",
        source_task_id="source_task",
    )
    session.add_all([event1, event2])
    session.flush()

    dag_run1 = session.scalar(select(DagRun).filter(DagRun.id == dag_run1.id))
    dag_run2 = session.scalar(select(DagRun).filter(DagRun.id == dag_run2.id))

    dag_run1.consumed_asset_events.append(event1)
    dag_run2.consumed_asset_events.append(event2)

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
        "partition_key": run.partition_key,
        "partition_date": from_datetime_to_zulu_without_ms(run.partition_date)
        if run.partition_date
        else None,
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
            run = session.scalars(
                select(DagRun).where(DagRun.dag_id == each["dag_id"], DagRun.run_id == each["dag_run_id"])
            ).one()
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
                        "loc": ["query", "limit"],
                        "msg": "Input should be greater than or equal to 0",
                        "input": "-1",
                        "ctx": {"ge": 0},
                    },
                    {
                        "type": "greater_than_equal",
                        "loc": ["query", "offset"],
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
        "order_by",
        [
            "id",
            "dag_run_id",
            "logical_date",
            "-run_after",
        ],  # test with multiple ordering fields (alias, non-alias, datetime, non-datetime)
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_cursor_pagination_first_two_page(self, test_client, order_by):
        """First page with cursor='' and second page fetched via the returned next_cursor."""
        response = test_client.get(
            "/dags/~/dagRuns",
            params={"limit": 2, "order_by": order_by, "cursor": ""},
        )
        assert response.status_code == 200, response.json()
        body = response.json()
        assert body["next_cursor"] is not None
        assert body["previous_cursor"] is None
        assert body["total_entries"] is None
        assert len(body["dag_runs"]) == 2

        response2 = test_client.get(
            "/dags/~/dagRuns",
            params={"limit": 2, "order_by": order_by, "cursor": body["next_cursor"]},
        )
        assert response2.status_code == 200, response2.json()
        body2 = response2.json()
        assert body2["previous_cursor"] is not None
        assert body2["total_entries"] is None
        assert len(body2["dag_runs"]) == 2
        first_page_ids = {(r["dag_id"], r["dag_run_id"]) for r in body["dag_runs"]}
        second_page_ids = {(r["dag_id"], r["dag_run_id"]) for r in body2["dag_runs"]}
        assert first_page_ids.isdisjoint(second_page_ids)

    @pytest.mark.parametrize(
        "order_by",
        ["id", "dag_run_id", "logical_date", "-run_after"],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_cursor_pagination_returns_cursor_response(self, test_client, order_by):
        """When cursor param is provided, response has cursor fields and no total_entries."""
        response1 = test_client.get(
            "/dags/~/dagRuns",
            params={"limit": 2, "order_by": order_by, "cursor": ""},
        )
        assert response1.status_code == 200
        body1 = response1.json()
        assert body1["total_entries"] is None
        assert len(body1["dag_runs"]) == 2
        next_cursor = body1["next_cursor"]
        assert next_cursor is not None

        # Second (last) page using next_cursor from first page — only 2 dag runs remain
        response2 = test_client.get(
            "/dags/~/dagRuns",
            params={"limit": 100, "cursor": next_cursor, "order_by": order_by},
        )
        assert response2.status_code == 200, response2.json()
        body2 = response2.json()
        assert body2["next_cursor"] is None
        assert body2["previous_cursor"] is not None
        assert body2["total_entries"] is None

    @pytest.mark.parametrize(
        "order_by",
        ["id", "dag_run_id", "logical_date", "-run_after"],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_cursor_pagination_forward_and_backward_consistency(self, test_client, order_by):
        """Walk all pages forward via next_cursor, then backward via previous_cursor, and compare."""
        total_runs = 4  # 4 dag runs are created by the setup fixture
        page_size = 2
        max_pages = math.ceil(total_runs / page_size)

        forward_ids: list[tuple[str, str]] = []
        forward_pages: list[dict] = []
        cursor_token = ""
        for _ in range(max_pages):
            response = test_client.get(
                "/dags/~/dagRuns",
                params={"limit": page_size, "order_by": order_by, "cursor": cursor_token},
            )
            assert response.status_code == 200, response.json()
            body = response.json()
            assert body["total_entries"] is None
            forward_pages.append(body)
            forward_ids.extend((r["dag_id"], r["dag_run_id"]) for r in body["dag_runs"])

            cursor_token = body.get("next_cursor")
            if cursor_token is None:
                break

        assert len(forward_ids) == total_runs
        assert len(forward_ids) == len(set(forward_ids)), "Forward pages should not overlap"
        assert len(forward_pages) == max_pages

        assert forward_pages[0]["previous_cursor"] is None
        assert forward_pages[-1]["next_cursor"] is None

        backward_ids: list[tuple[str, str]] = []
        cursor_token = forward_pages[-1]["previous_cursor"]
        assert cursor_token is not None

        for _ in range(max_pages):
            response = test_client.get(
                "/dags/~/dagRuns",
                params={"limit": page_size, "order_by": order_by, "cursor": cursor_token},
            )
            assert response.status_code == 200, response.json()
            body = response.json()
            backward_ids = [(r["dag_id"], r["dag_run_id"]) for r in body["dag_runs"]] + backward_ids

            cursor_token = body.get("previous_cursor")
            if cursor_token is None:
                break

        all_backward = backward_ids + [(r["dag_id"], r["dag_run_id"]) for r in forward_pages[-1]["dag_runs"]]
        assert all_backward == forward_ids

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_cursor_pagination_invalid_token(self, test_client):
        response = test_client.get(
            "/dags/~/dagRuns",
            params={"cursor": "this-is-not-valid", "order_by": "id"},
        )
        assert response.status_code == 400

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_cursor_pagination_nullable_sort_column_returns_all_rows(self, test_client, session):
        """Cursor pagination sorted by a nullable column must not silently drop rows.

        With a NULL in the sort column, the keyset predicate and the ORDER BY can disagree
        on NULL placement and drop every row on one side of the NULL/non-NULL boundary.
        """
        # Null out one run's start_date so the NULL/non-NULL boundary is crossed mid-walk.
        run = session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID))
        run.start_date = None
        session.commit()

        full = test_client.get("/dags/~/dagRuns", params={"limit": 100})
        assert full.status_code == 200, full.json()
        full_ids = {(r["dag_id"], r["dag_run_id"]) for r in full.json()["dag_runs"]}
        assert len(full_ids) == 4

        collected: list[tuple[str, str]] = []
        cursor_token: str | None = ""
        for _ in range(20):
            resp = test_client.get(
                "/dags/~/dagRuns",
                params={"limit": 1, "order_by": "start_date", "cursor": cursor_token},
            )
            assert resp.status_code == 200, resp.json()
            body = resp.json()
            collected.extend((r["dag_id"], r["dag_run_id"]) for r in body["dag_runs"])
            cursor_token = body.get("next_cursor")
            if cursor_token is None:
                break

        assert len(collected) == len(set(collected)), "cursor pages overlapped"
        assert set(collected) == full_ids, "cursor pagination dropped rows across the NULL boundary"

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
            # Pipe (OR) operator returns results matching either term
            ("~", {"run_id_pattern": f"{DAG1_RUN1_ID}|{DAG1_RUN2_ID}"}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            # Trailing/leading pipe should not leak into the LIKE pattern
            ("~", {"run_id_pattern": f"{DAG1_RUN1_ID}|"}, [DAG1_RUN1_ID]),
            ("~", {"run_id_pattern": f"|{DAG1_RUN1_ID}"}, [DAG1_RUN1_ID]),
            (
                DAG1_ID,
                {
                    "run_id_pattern": DAG1_RUN1_ID,
                    "state": DagRunState.SUCCESS.value,
                },
                [DAG1_RUN1_ID],
            ),
            # run_id_prefix_pattern counterpart
            (DAG1_ID, {"run_id_prefix_pattern": DAG1_RUN1_ID}, [DAG1_RUN1_ID]),
            ("~", {"run_id_prefix_pattern": f"{DAG1_RUN1_ID}|{DAG1_RUN2_ID}"}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            ("~", {"run_id_prefix_pattern": "nonexistent"}, []),
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
            # triggering_user_name_prefix_pattern counterpart
            (DAG1_ID, {"triggering_user_name_prefix_pattern": "alice_admin"}, [DAG1_RUN1_ID]),
            ("~", {"triggering_user_name_prefix_pattern": "alice"}, [DAG1_RUN1_ID]),
            ("~", {"triggering_user_name_prefix_pattern": "nonexistent"}, []),
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
            # dag_id_prefix_pattern counterpart
            ("~", {"dag_id_prefix_pattern": "test_dag1"}, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            (
                "~",
                {"dag_id_prefix_pattern": "test_"},
                [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID],
            ),
            ("~", {"dag_id_prefix_pattern": "nonexistent"}, []),
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
            ("~", {"conf_contains": "nonexistent_key"}, []),  # Test for a key that doesn't exist
            # Test consuming_asset_pattern filter
            ("~", {"consuming_asset_pattern": "sales"}, [DAG1_RUN1_ID]),  # Filter by asset name
            ("~", {"consuming_asset_pattern": "s3://bucket/sales"}, [DAG1_RUN1_ID]),  # Filter by asset URI
            ("~", {"consuming_asset_pattern": "customer"}, [DAG1_RUN2_ID]),  # Filter by another asset
            (
                "~",
                {"consuming_asset_pattern": "s3://bucket/customer"},
                [DAG1_RUN2_ID],
            ),  # Filter by customer URI
            (
                "~",
                {"consuming_asset_pattern": "s3://bucket"},
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
            ),  # Partial URI match
            ("~", {"consuming_asset_pattern": "nonexistent_asset"}, []),  # Non-existent asset returns empty
            # Test partition_key_pattern filter.
            # dag_run1 has partition_key="2026-01-01|us" (composite key with | as ProductMapper delimiter).
            # dag_run3 has partition_key="us" (plain single-dimension key).
            # dag_run2 and dag_run4 have partition_key=None.
            #
            # Composite-key literal match: the full "2026-01-01|us" matches only dag_run1 and NOT dag_run3.
            # This verifies that | is NOT treated as an OR separator.
            ("~", {"partition_key_pattern": "2026-01-01|us"}, [DAG1_RUN1_ID]),
            # Substring "us" matches both dag_run1 (contains "|us") and dag_run3 (equals "us").
            ("~", {"partition_key_pattern": "us"}, [DAG1_RUN1_ID, DAG2_RUN1_ID]),
            # Substring "2026-01-01" matches only dag_run1 — NOT split on | to also match dag_run3.
            ("~", {"partition_key_pattern": "2026-01-01"}, [DAG1_RUN1_ID]),
            # No match → empty result.
            ("~", {"partition_key_pattern": "nonexistent"}, []),
            # dag_id_pattern still uses | as OR (other search params unaffected by this fix).
            ("~", {"dag_id_pattern": f"{DAG1_ID}|{DAG2_ID}"}, DAG_RUNS_LIST),
            # Test partition_key_prefix_pattern filter.
            # Exact prefix "2026-01-01|us" matches dag_run1.
            ("~", {"partition_key_prefix_pattern": "2026-01-01|us"}, [DAG1_RUN1_ID]),
            # Prefix "us" matches only dag_run3 (dag_run1 starts with "2026", not "us").
            ("~", {"partition_key_prefix_pattern": "us"}, [DAG2_RUN1_ID]),
            # Prefix "2026" matches dag_run1 (starts with "2026-01-01|us").
            ("~", {"partition_key_prefix_pattern": "2026"}, [DAG1_RUN1_ID]),
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

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @pytest.mark.parametrize(
        ("dag_id", "query_params", "expected_dag_run_ids"),
        [
            ("dag_with_multiple_versions", {"bundle_version": "some_commit_hash1"}, ["run1"]),
            ("dag_with_multiple_versions", {"bundle_version": "some_commit_hash2"}, ["run2"]),
            ("dag_with_multiple_versions", {"bundle_version": "some_commit_hash3"}, ["run3"]),
            ("~", {"bundle_version": "some_commit_hash2"}, ["run2"]),
            ("~", {"bundle_version": "does_not_exist"}, []),
        ],
    )
    def test_filter_by_bundle_version(self, test_client, dag_id, query_params, expected_dag_run_ids):
        response = test_client.get(f"/dags/{dag_id}/dagRuns", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_run_ids

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
            run = session.scalars(select(DagRun).where(DagRun.run_id == each["dag_run_id"])).one()
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
                DAG1_RUN1_ID,
                {"note": ""},
                {"state": DagRunState.SUCCESS, "note": None},
                None,
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

    @pytest.mark.parametrize(
        ("state", "listener_state"),
        [
            ("queued", []),
            ("success", [DagRunState.SUCCESS]),
            ("failed", [DagRunState.FAILED]),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_patch_dag_run_notifies_listeners(self, test_client, state, listener_state, listener_manager):
        listener = ClassBasedListener()
        listener_manager(listener)
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

    def test_delete_dag_run_in_running_state(self, test_client, dag_maker, session):
        with dag_maker(dag_id="test_running_dag"):
            EmptyOperator(task_id="t1")

        dag_maker.create_dagrun(
            run_id="test_running",
            state=DagRunState.RUNNING,
        )
        session.commit()
        response = test_client.delete("/dags/test_running_dag/dagRuns/test_running")
        assert response.status_code == 409
        body = response.json()
        assert body["detail"] == (
            "The DagRun with dag_id: `test_running_dag` and run_id: `test_running` "
            "cannot be deleted in running state"
        )

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(f"/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 403


class TestGetDagRunAssetTriggerEvents:
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    @pytest.mark.parametrize(
        "partition_key",
        ["test_partition_key", None],
        ids=["partitioned", "non-partitioned"],
    )
    def test_should_respond_200(self, partition_key, test_client, dag_maker, session):
        asset1 = Asset(name="ds1", uri="file:///da1")

        # Use PartitionedAtRuntime for partitioned cases so the partition_key gate does not reject the key.
        source_schedule = PartitionedAtRuntime() if partition_key is not None else timedelta(days=1)
        with dag_maker(
            dag_id="source_dag", start_date=START_DATE1, schedule=source_schedule, session=session
        ):
            EmptyOperator(task_id="task", outlets=[asset1])
        dr = dag_maker.create_dagrun(partition_key=partition_key)
        ti = dr.task_instances[0]

        asset1_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset1.uri))
        event = AssetEvent(
            asset_id=asset1_id,
            source_task_id=ti.task_id,
            source_dag_id=ti.dag_id,
            source_run_id=ti.run_id,
            source_map_index=ti.map_index,
            partition_key=partition_key,
        )
        session.add(event)

        trigger_schedule = PartitionedAtRuntime() if partition_key is not None else timedelta(days=1)
        with dag_maker(
            dag_id="TEST_DAG_ID", start_date=START_DATE1, schedule=trigger_schedule, session=session
        ):
            pass
        create_dagrun_kwargs: dict = {
            "run_id": "TEST_DAG_RUN_ID",
            "run_type": DagRunType.ASSET_TRIGGERED,
            "partition_key": partition_key,
        }
        if partition_key is not None:
            # PartitionedAtRuntime is a null-timetable with no scheduled runs; supply logical_date=None
            # explicitly so dag_maker does not try to infer it via next_dagrun_info (which returns None).
            create_dagrun_kwargs["logical_date"] = None
        dr = dag_maker.create_dagrun(**create_dagrun_kwargs)
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
                            "partition_key": partition_key,
                        }
                    ],
                    "partition_key": partition_key,
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
        ("body", "expected_note"),
        [
            ({"dry_run": False, "note": "cleared by test"}, "cleared by test"),
            ({"dry_run": False, "note": ""}, None),
            ({"dry_run": False, "note": None}, "test_note"),
            ({"dry_run": False}, "test_note"),
        ],
        ids=[
            "set-new-note",
            "empty-note-removes-existing",
            "explicit-null-leaves-existing",
            "omit-leaves-existing",
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_applies_note(self, test_client, session, body, expected_note):
        """``note`` in the clear body writes to the Dag Run; ``None`` / unset leaves it alone."""
        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear", json=body)
        assert response.status_code == 200
        assert response.json()["note"] == expected_note
        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == DAG1_RUN1_ID)
        )
        assert dag_run.note == expected_note

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_dry_run_does_not_apply_note(self, test_client, session):
        """``note`` is ignored on dry-run (no side effects)."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": True, "note": "ignored"},
        )
        assert response.status_code == 200
        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == DAG1_RUN1_ID)
        )
        assert dag_run.note == "test_note"

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
        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == DAG1_RUN1_ID)
        )
        assert dag_run.state == DAG1_RUN1_STATE

        logs = session.scalar(
            select(func.count())
            .select_from(Log)
            .where(Log.dag_id == DAG1_ID, Log.run_id == dag_run_id, Log.event == "clear_dag_run")
        )
        assert logs == 0

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_dry_run_response_has_full_task_instance_fields(self, test_client):
        """Regression test: dry-run response must include all TaskInstanceResponse fields.

        Previously, dag.clear(dry_run=True) returned raw ORM objects without eager-loaded
        relationships, so Pydantic could not populate fields like dag_display_name (requires
        dag_run.dag_model) and the serialization silently failed, causing the UI modal to
        show an empty task list.
        """
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": True, "only_failed": False, "only_new": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2

        for ti in body["task_instances"]:
            # Fields that require dag_run → dag_model join (previously missing)
            assert ti["dag_display_name"] == DAG1_DISPLAY_NAME
            # run_id is serialised under the alias dag_run_id
            assert ti["dag_run_id"] == DAG1_RUN1_ID
            assert ti["dag_id"] == DAG1_ID
            assert ti["task_id"] is not None
            assert ti["state"] is not None
            # rendered_fields must be present (defaults to {})
            assert "rendered_fields" in ti

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_dry_run_only_failed_returns_only_failed_tasks_with_full_fields(self, test_client):
        """Regression test: only_failed=True dry-run must return only failed TIs with full fields.

        Verifies that:
        1. Only FAILED / UPSTREAM_FAILED task instances are included (not SUCCESS).
        2. All TaskInstanceResponse fields (dag_display_name, dag_run_id, rendered_fields)
           are fully populated — the same eager-loading requirement as the general dry-run path.
        """
        # DAG1_RUN2_ID has task_1=SUCCESS, task_2=FAILED — only task_2 should be returned.
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN2_ID}/clear",
            json={"dry_run": True, "only_failed": True, "only_new": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1

        (ti,) = body["task_instances"]
        assert ti["state"] == "failed"
        assert ti["dag_display_name"] == DAG1_DISPLAY_NAME
        assert ti["dag_run_id"] == DAG1_RUN2_ID
        assert ti["dag_id"] == DAG1_ID
        assert "rendered_fields" in ti

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

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_only_new_dry_run(self, test_client, session):
        """Test that only_new dry_run returns 0 new tasks when all tasks already have TIs.

        The new implementation uses TI-existence checks rather than DAG version comparison.
        DAG1_RUN1_ID already has TIs for every task in the latest DAG version, so there are
        no new tasks to queue and dag.clear() is not called for the dry-run path.
        """
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": True, "only_new": True},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["task_instances"] == []
        assert body["total_entries"] == 0
        logs = session.scalar(
            select(func.count())
            .select_from(Log)
            .where(Log.dag_id == DAG1_ID, Log.run_id == DAG1_RUN1_ID, Log.event == "clear_dag_run")
        )
        assert logs == 0

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.clear")
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_dag_run_only_new_non_dry_run(self, mock_clear, test_client, session):
        """Test that only_new non-dry_run clears and returns a DAGRunResponse."""
        mock_clear.return_value = 2
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": False, "only_new": True},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == DAG1_ID
        assert body["dag_run_id"] == DAG1_RUN1_ID
        mock_clear.assert_called_once_with(
            run_id=DAG1_RUN1_ID,
            task_ids=None,
            only_new=True,
            only_failed=False,
            run_on_latest_version=False,
            session=mock.ANY,
        )
        _check_last_log(
            session,
            dag_id=DAG1_ID,
            event="clear_dag_run",
            logical_date=None,
        )

    def test_clear_dag_run_only_new_and_only_failed_mutually_exclusive(self, test_client):
        """Test that only_new and only_failed cannot both be True."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": True, "only_new": True, "only_failed": True},
        )
        assert response.status_code == 422


class TestBulkClearDagRuns:
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_specific_dag(self, test_client, session):
        """Specific dag_id in URL, dag_run_id in body — clears both runs and queues them."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": False,
                "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}, {"dag_run_id": DAG1_RUN2_ID}],
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        returned_run_ids = sorted(run["dag_run_id"] for run in body["dag_runs"])
        assert returned_run_ids == sorted([DAG1_RUN1_ID, DAG1_RUN2_ID])
        for run in body["dag_runs"]:
            assert run["state"] == "queued"
            assert run["dag_id"] == DAG1_ID

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_wildcard_across_dags(self, test_client, session):
        """``~`` URL with per-entity dag_id — clears runs across Dags in one call."""
        response = test_client.post(
            "/dags/~/clearDagRuns",
            json={
                "dry_run": False,
                "dag_runs": [
                    {"dag_id": DAG1_ID, "dag_run_id": DAG1_RUN1_ID},
                    {"dag_id": DAG2_ID, "dag_run_id": DAG2_RUN1_ID},
                ],
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        pairs = sorted((run["dag_id"], run["dag_run_id"]) for run in body["dag_runs"])
        assert pairs == sorted([(DAG1_ID, DAG1_RUN1_ID), (DAG2_ID, DAG2_RUN1_ID)])
        for run in body["dag_runs"]:
            assert run["state"] == "queued"

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_dry_run_collects_affected_tis_across_runs(self, test_client, session):
        """Dry-run returns the union of affected TIs across the listed runs without mutating state."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": True,
                "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}, {"dag_run_id": DAG1_RUN2_ID}],
            },
        )
        assert response.status_code == 200
        body = response.json()
        # Both DAG1 runs have two task instances each.
        assert body["total_entries"] == 4
        run_ids_in_response = {ti["dag_run_id"] for ti in body["task_instances"]}
        assert run_ids_in_response == {DAG1_RUN1_ID, DAG1_RUN2_ID}
        # No state changes — dry_run never writes.
        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == DAG1_RUN1_ID)
        )
        assert dag_run.state == DAG1_RUN1_STATE

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_dry_run_only_failed_filters(self, test_client):
        """``only_failed=True`` shrinks the dry-run preview to failed TIs only."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": True,
                "only_failed": True,
                "dag_runs": [{"dag_run_id": DAG1_RUN2_ID}],
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert all(ti["state"] == "failed" for ti in body["task_instances"])
        assert body["total_entries"] == 1

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_applies_note_to_each_run(self, test_client, session):
        """``note`` in the body is applied to every cleared run in the same transaction."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": False,
                "note": "bulk cleared by test",
                "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}, {"dag_run_id": DAG1_RUN2_ID}],
            },
        )
        assert response.status_code == 200
        for run_id in (DAG1_RUN1_ID, DAG1_RUN2_ID):
            dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == run_id))
            assert dag_run.note == "bulk cleared by test"

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_wildcard_rejects_missing_dag_id(self, test_client):
        """``~`` URL requires every entry to carry a concrete dag_id; 400 otherwise."""
        response = test_client.post(
            "/dags/~/clearDagRuns",
            json={
                "dry_run": False,
                "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}],
            },
        )
        assert response.status_code == 400
        assert DAG1_RUN1_ID in response.json()["detail"]

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_specific_url_rejects_mismatched_dag_id(self, test_client):
        """When the URL has a specific dag_id, mismatched per-entity dag_id is rejected."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": False,
                "dag_runs": [{"dag_id": DAG2_ID, "dag_run_id": DAG2_RUN1_ID}],
            },
        )
        assert response.status_code == 400

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_missing_run_returns_404(self, test_client):
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": False,
                "dag_runs": [{"dag_run_id": "does_not_exist"}],
            },
        )
        assert response.status_code == 404

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_rejects_only_new_with_only_failed(self, test_client):
        """``only_new`` and ``only_failed`` are mutually exclusive at the body validator level."""
        response = test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={
                "dry_run": True,
                "only_new": True,
                "only_failed": True,
                "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}],
            },
        )
        assert response.status_code == 422

    def test_bulk_clear_unauthenticated_returns_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={"dry_run": False, "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}]},
        )
        assert response.status_code == 401

    def test_bulk_clear_unauthorized_returns_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            f"/dags/{DAG1_ID}/clearDagRuns",
            json={"dry_run": False, "dag_runs": [{"dag_run_id": DAG1_RUN1_ID}]},
        )
        assert response.status_code == 403

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_bulk_clear_rejects_unauthorized_dag_ids_from_request_body(self, test_client, session):
        """A 403 at the route level if any entry references a Dag the user can't access; nothing is cleared."""
        restricted_bundle_name = "restricted-bundle-clear"
        restricted_team_name = "restricted-team-clear"
        restricted_bundle = DagBundleModel(name=restricted_bundle_name)
        restricted_team = Team(name=restricted_team_name)
        restricted_bundle.teams.append(restricted_team)
        session.add_all([restricted_bundle, restricted_team])
        session.flush()
        # Restrict DAG2 by attaching it to a team-scoped bundle the limited user has no access to.
        session.execute(
            update(DagModel).where(DagModel.dag_id == DAG2_ID).values(bundle_name=restricted_bundle_name)
        )
        session.commit()

        states_before = {
            run_id: session.scalar(select(DagRun.state).where(DagRun.run_id == run_id))
            for run_id in (DAG1_RUN1_ID, DAG2_RUN1_ID)
        }

        auth_manager = test_client.app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(
                SimpleAuthManagerUser(username="limited-user", role="user", teams=[]),
            )
        )
        with (
            mock.patch("airflow.models.revoked_token.RevokedToken.is_revoked", return_value=False),
            TestClient(
                test_client.app,
                headers={"Authorization": f"Bearer {token}"},
                base_url=str(test_client.base_url),
            ) as limited_test_client,
        ):
            response = limited_test_client.post(
                "/dags/~/clearDagRuns",
                json={
                    "dry_run": False,
                    "dag_runs": [
                        {"dag_id": DAG1_ID, "dag_run_id": DAG1_RUN1_ID},
                        {"dag_id": DAG2_ID, "dag_run_id": DAG2_RUN1_ID},
                    ],
                },
            )

        assert response.status_code == 403
        # The batched auth check rejects the whole request, so the authorized Dag's run is not cleared either.
        session.expire_all()
        for run_id, state_before in states_before.items():
            assert session.scalar(select(DagRun.state).where(DagRun.run_id == run_id)) == state_before


class TestClearDagRunOnlyNew:
    """Integration tests for only_new=True using a real two-version DAG.

    These tests use real serialised DAG versions to confirm that:
      - the dry-run preview lists the correct new task IDs (TI-existence check), and
      - the actual action creates the new TI in the task_instance table.
    """

    @pytest.fixture
    def dag_two_versions(self, dag_maker, configure_git_connection_for_dag_bundle, session):
        """
        Two-version DAG with one run on v1.

        v1: task_a only
        v2: task_a + task_b   (task_b is the "new" task)

        The v1 run has a TI for task_a only; task_b has no TI yet.
        """
        dag_id = "dag_only_new_test"

        # --- v1 ---
        with dag_maker(dag_id, session=session, serialized=True):
            EmptyOperator(task_id="task_a")
        run = dag_maker.create_dagrun(
            run_id="run_v1",
            logical_date=datetime(2024, 3, 1, tzinfo=timezone.utc),
            state=DagRunState.SUCCESS,
            session=session,
        )
        session.flush()
        ti_a = run.get_task_instance(task_id="task_a", session=session)
        ti_a.state = State.SUCCESS
        session.merge(ti_a)

        # --- v2: task_b added ---
        with dag_maker(dag_id, session=session, serialized=True):
            EmptyOperator(task_id="task_a")
            EmptyOperator(task_id="task_b")
        session.commit()

        return {"dag_id": dag_id, "run_id": "run_v1"}

    def test_only_new_dry_run_identifies_new_task(self, test_client, dag_two_versions):
        """Dry-run with only_new=True must identify tasks added in the latest version."""
        dag_id = dag_two_versions["dag_id"]
        run_id = dag_two_versions["run_id"]

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns/{run_id}/clear",
            json={"dry_run": True, "only_new": True},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        assert body["task_instances"][0]["task_id"] == "task_b"

    def test_only_new_creates_task_instance_in_db(self, test_client, session, dag_two_versions):
        """Non-dry-run with only_new=True must create a TI for task_b in the DB."""
        dag_id = dag_two_versions["dag_id"]
        run_id = dag_two_versions["run_id"]

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns/{run_id}/clear",
            json={"dry_run": False, "only_new": True},
        )
        assert response.status_code == 200
        assert response.json()["dag_run_id"] == run_id

        session.expire_all()
        task_ids = {
            ti.task_id
            for ti in session.scalars(
                select(TaskInstance).where(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == run_id,
                )
            ).all()
        }
        assert "task_b" in task_ids, "task_b TI was not created after only_new clear"

    def test_only_new_skips_task_that_already_has_ti(self, test_client, dag_two_versions):
        """Tasks with an existing TI must NOT appear in the only_new preview, regardless of version.

        This verifies the TI-existence check: even though task_b was added in v2, once its TI
        exists in the run it must not be returned as "new". We create the TI by running the
        non-dry-run endpoint first, then confirm the dry-run preview shows 0 new tasks.
        """
        dag_id = dag_two_versions["dag_id"]
        run_id = dag_two_versions["run_id"]

        # Create task_b's TI by executing the actual only_new clear (non-dry-run)
        resp = test_client.post(
            f"/dags/{dag_id}/dagRuns/{run_id}/clear",
            json={"dry_run": False, "only_new": True},
        )
        assert resp.status_code == 200

        # Now the dry-run preview should show 0 new tasks — task_b already has a TI
        response = test_client.post(
            f"/dags/{dag_id}/dagRuns/{run_id}/clear",
            json={"dry_run": True, "only_new": True},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0, (
            f"Expected 0 new tasks but got {body['total_entries']}: {body['task_instances']}"
        )


PARTITION_DAG_ID = "partition_test_dag"


class TestBulkClearDagRunsPartitionSelector:
    """
    Tests for the partition-selector extensions to clearDagRuns (Part A).

    These tests cover: partition_key selector, partition_date window, dry_run,
    mutual-exclusion validation, wildcard rejection, and authz bypass fix (B1).
    """

    @pytest.fixture
    def partition_dag(self, dag_maker, configure_git_connection_for_dag_bundle, session):
        """Dag with two runs carrying partition_key and partition_date."""
        with dag_maker(
            PARTITION_DAG_ID,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            serialized=True,
        ):
            task_1 = EmptyOperator(task_id="task_1")

        run_specs = [
            ("partition_run_a", "2026-01-01|us", datetime(2026, 1, 1, tzinfo=timezone.utc)),
            ("partition_run_b", "2026-01-02|us", datetime(2026, 1, 2, tzinfo=timezone.utc)),
            ("partition_run_c", "2026-01-03|us", datetime(2026, 1, 3, tzinfo=timezone.utc)),
        ]
        for run_id, partition_key, partition_date in run_specs:
            run = dag_maker.create_dagrun(
                run_id=run_id,
                state=DagRunState.SUCCESS,
                run_type=DagRunType.MANUAL,
                triggered_by=DagRunTriggeredByType.REST_API,
                logical_date=partition_date,
            )
            run.partition_key = partition_key
            run.partition_date = partition_date
            ti = run.get_task_instance(task_id="task_1")
            ti.task = task_1
            ti.state = State.SUCCESS
            session.merge(ti)

        dag_maker.sync_dagbag_to_db()
        session.flush()
        return {
            "dag_id": PARTITION_DAG_ID,
            "run_a_id": "partition_run_a",
            "run_b_id": "partition_run_b",
            "run_c_id": "partition_run_c",
        }

    def test_partition_key_selector_clears_matching_run(self, test_client, session, partition_dag):
        """partition_key selector resolves the matching run and clears it (state → queued)."""
        dag_id = partition_dag["dag_id"]
        response = test_client.post(
            f"/dags/{dag_id}/clearDagRuns",
            json={"dry_run": False, "partition_key": "2026-01-01|us"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        assert body["dag_runs"][0]["dag_run_id"] == partition_dag["run_a_id"]
        assert body["dag_runs"][0]["state"] == "queued"

        # run_b and run_c must be untouched
        session.expire_all()
        for run_id in (partition_dag["run_b_id"], partition_dag["run_c_id"]):
            state = session.scalar(select(DagRun.state).where(DagRun.run_id == run_id))
            assert state == DagRunState.SUCCESS

    def test_partition_date_window_inclusive_end(self, test_client, session, partition_dag):
        """
        Window [Jan 1, Jan 2] must include runs with partition_date on Jan 1 and Jan 2
        but exclude Jan 3 (cap-boundary pair: end==Jan 2 included, end+1==Jan 3 excluded).
        """
        dag_id = partition_dag["dag_id"]
        response = test_client.post(
            f"/dags/{dag_id}/clearDagRuns",
            json={
                "dry_run": False,
                "partition_date_start": "2026-01-01T00:00:00Z",
                "partition_date_end": "2026-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        body = response.json()
        cleared_run_ids = sorted(r["dag_run_id"] for r in body["dag_runs"])
        assert cleared_run_ids == sorted([partition_dag["run_a_id"], partition_dag["run_b_id"]])
        for run in body["dag_runs"]:
            assert run["state"] == "queued"

        # Jan 3 run must be untouched
        session.expire_all()
        state_c = session.scalar(select(DagRun.state).where(DagRun.run_id == partition_dag["run_c_id"]))
        assert state_c == DagRunState.SUCCESS

    def test_partition_date_end_boundary_excludes_next_day(self, test_client, session, partition_dag):
        """Upper bound is inclusive: run on end datetime is included, run after end is not."""
        dag_id = partition_dag["dag_id"]
        # Window: start=Jan 1, end=Jan 1T00:00Z → only run_a selected (run_b on Jan 2 excluded)
        response = test_client.post(
            f"/dags/{dag_id}/clearDagRuns",
            json={
                "dry_run": True,
                "partition_date_start": "2026-01-01T00:00:00Z",
                "partition_date_end": "2026-01-01T00:00:00Z",
            },
        )
        assert response.status_code == 200
        body = response.json()
        affected_run_ids = {ti["dag_run_id"] for ti in body["task_instances"]}
        assert affected_run_ids == {partition_dag["run_a_id"]}

    def test_partition_selector_dry_run_does_not_write(self, test_client, session, partition_dag):
        """dry_run=True returns affected TIs without modifying run state."""
        dag_id = partition_dag["dag_id"]
        response = test_client.post(
            f"/dags/{dag_id}/clearDagRuns",
            json={"dry_run": True, "partition_key": "2026-01-01|us"},
        )
        assert response.status_code == 200
        body = response.json()
        # dry_run returns ClearTaskInstanceCollectionResponse
        assert "task_instances" in body
        assert len(body["task_instances"]) > 0, "dry_run must return at least one affected TI"
        session.expire_all()
        state = session.scalar(select(DagRun.state).where(DagRun.run_id == partition_dag["run_a_id"]))
        assert state == DagRunState.SUCCESS

    @pytest.mark.parametrize(
        "build_body",
        [
            pytest.param(
                lambda d: {
                    "dry_run": True,
                    "dag_runs": [{"dag_run_id": d["run_a_id"]}],
                    "partition_key": "2026-01-01|us",
                },
                id="dag_runs_and_partition_key",
            ),
            pytest.param(
                lambda d: {
                    "dry_run": True,
                    "partition_key": "2026-01-01|us",
                    "partition_date_start": "2026-01-01T00:00:00Z",
                },
                id="two_partition_selectors",
            ),
            pytest.param(
                lambda d: {"dry_run": True},
                id="no_selector",
            ),
            pytest.param(
                lambda d: {
                    "dry_run": True,
                    "partition_date_start": "2026-01-03T00:00:00Z",
                    "partition_date_end": "2026-01-01T00:00:00Z",
                },
                id="start_after_end",
            ),
        ],
    )
    def test_invalid_selector_combination_returns_422(self, test_client, partition_dag, build_body):
        """Invalid selector combinations must be rejected with 422."""
        response = test_client.post(
            f"/dags/{partition_dag['dag_id']}/clearDagRuns",
            json=build_body(partition_dag),
        )
        assert response.status_code == 422

    def test_wildcard_dag_id_with_partition_selector_returns_400(self, test_client, partition_dag):
        """'~' dag_id + partition selector must be rejected with 400 (timetable unknown)."""
        response = test_client.post(
            "/dags/~/clearDagRuns",
            json={"dry_run": True, "partition_key": "2026-01-01|us"},
        )
        assert response.status_code == 400

    def test_partition_selector_unauthenticated_returns_401(self, unauthenticated_test_client, partition_dag):
        """Unauthenticated request with partition selector must return 401."""
        response = unauthenticated_test_client.post(
            f"/dags/{partition_dag['dag_id']}/clearDagRuns",
            json={"dry_run": True, "partition_key": "2026-01-01|us"},
        )
        assert response.status_code == 401

    def test_partition_selector_unauthorized_returns_403(self, unauthorized_test_client, partition_dag):
        """Unauthorized user with partition selector must return 403 (authz bypass fix B1)."""
        response = unauthorized_test_client.post(
            f"/dags/{partition_dag['dag_id']}/clearDagRuns",
            json={"dry_run": True, "partition_key": "2026-01-01|us"},
        )
        assert response.status_code == 403

    def test_partition_key_no_match_returns_200_empty(self, test_client, session, partition_dag):
        """A partition_key matching no run returns 200 with an empty result, not 404."""
        dag_id = partition_dag["dag_id"]
        response = test_client.post(
            f"/dags/{dag_id}/clearDagRuns",
            json={"dry_run": False, "partition_key": "9999-12-31|none"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 0
        assert body["dag_runs"] == []

        # All runs must be untouched.
        session.expire_all()
        for run_id in (partition_dag["run_a_id"], partition_dag["run_b_id"], partition_dag["run_c_id"]):
            state = session.scalar(select(DagRun.state).where(DagRun.run_id == run_id))
            assert state == DagRunState.SUCCESS

    @pytest.mark.parametrize(
        ("window", "expected_run_keys"),
        [
            pytest.param(
                {"partition_date_start": "2026-01-02T00:00:00Z"},
                ("run_b_id", "run_c_id"),
                id="start-only-includes-from-start-onward",
            ),
            pytest.param(
                {"partition_date_end": "2026-01-02T00:00:00Z"},
                ("run_a_id", "run_b_id"),
                id="end-only-includes-up-to-and-including-end",
            ),
        ],
    )
    def test_partition_date_single_bound_window(
        self, test_client, session, partition_dag, window, expected_run_keys
    ):
        """A window with only one bound is open-ended on the missing side."""
        dag_id = partition_dag["dag_id"]
        response = test_client.post(
            f"/dags/{dag_id}/clearDagRuns",
            json={"dry_run": False, **window},
        )
        assert response.status_code == 200
        body = response.json()
        cleared_run_ids = sorted(r["dag_run_id"] for r in body["dag_runs"])
        assert cleared_run_ids == sorted(partition_dag[k] for k in expected_run_keys)


class TestClearPartitions:
    """
    Tests for the new clearPartitions endpoint (Part B/C).

    Covers: run_id selector, partition_key selector, partition_date window,
    clear_task_instances, dry_run, mutual-exclusion validation, and authz.
    """

    @pytest.fixture
    def partitioned_dag_with_runs(self, dag_maker, configure_git_connection_for_dag_bundle, session):
        """Dag with three runs carrying partition fields and task instances."""
        dag_id = "clear_partitions_test_dag"
        with dag_maker(
            dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            serialized=True,
        ):
            task_x = EmptyOperator(task_id="task_x")
            task_y = EmptyOperator(task_id="task_y")

        run_specs = [
            ("cp_run_a", "key-a", datetime(2026, 1, 1, tzinfo=timezone.utc)),
            ("cp_run_b", "key-b", datetime(2026, 1, 2, tzinfo=timezone.utc)),
            ("cp_run_c", "key-c", datetime(2026, 1, 3, tzinfo=timezone.utc)),
        ]
        runs = {}
        for run_id, partition_key, partition_date in run_specs:
            run = dag_maker.create_dagrun(
                run_id=run_id,
                state=DagRunState.SUCCESS,
                run_type=DagRunType.MANUAL,
                triggered_by=DagRunTriggeredByType.REST_API,
                logical_date=partition_date,
            )
            run.partition_key = partition_key
            run.partition_date = partition_date
            runs[run_id] = run

        # Only run_a carries task instances, for the clear_task_instances tests
        for op in (task_x, task_y):
            ti = runs["cp_run_a"].get_task_instance(task_id=op.task_id)
            ti.task = op
            ti.state = State.SUCCESS
            session.merge(ti)

        dag_maker.sync_dagbag_to_db()
        session.flush()
        return {
            "dag_id": dag_id,
            "run_a_id": "cp_run_a",
            "run_b_id": "cp_run_b",
            "run_c_id": "cp_run_c",
        }

    def test_run_id_selector_clears_partition_fields(self, test_client, session, partitioned_dag_with_runs):
        """run_id selector resets partition fields to None on the matching run."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"run_id": info["run_a_id"], "dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 1
        assert body["task_instances_cleared"] == 0
        assert body["dry_run"] is False

        session.expire_all()
        run = session.scalar(select(DagRun).where(DagRun.run_id == info["run_a_id"]))
        assert run.partition_key is None
        assert run.partition_date is None
        # Other runs untouched
        run_b = session.scalar(select(DagRun).where(DagRun.run_id == info["run_b_id"]))
        assert run_b.partition_key == "key-b"

    def test_partition_key_selector_clears_partition_fields(
        self, test_client, session, partitioned_dag_with_runs
    ):
        """partition_key selector resets partition fields to None on matching run."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"partition_key": "key-b", "dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 1
        assert body["dry_run"] is False

        session.expire_all()
        run_b = session.scalar(select(DagRun).where(DagRun.run_id == info["run_b_id"]))
        assert run_b.partition_key is None
        assert run_b.partition_date is None

    def test_partition_date_window_clears_fields_within_range(
        self, test_client, session, partitioned_dag_with_runs
    ):
        """partition_date window [Jan 1, Jan 2] clears run_a and run_b, leaves run_c."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={
                "partition_date_start": "2026-01-01T00:00:00Z",
                "partition_date_end": "2026-01-02T00:00:00Z",
                "dry_run": False,
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 2
        assert body["dry_run"] is False

        session.expire_all()
        for run_id in (info["run_a_id"], info["run_b_id"]):
            run = session.scalar(select(DagRun).where(DagRun.run_id == run_id))
            assert run.partition_key is None
            assert run.partition_date is None
        # run_c (Jan 3) must be untouched
        run_c = session.scalar(select(DagRun).where(DagRun.run_id == info["run_c_id"]))
        assert run_c.partition_key == "key-c"

    def test_dry_run_returns_counts_without_writing(self, test_client, session, partitioned_dag_with_runs):
        """dry_run=True reports the would-be count but does not modify the DB."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"partition_key": "key-a", "dry_run": True},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 1
        assert body["dry_run"] is True

        session.expire_all()
        run_a = session.scalar(select(DagRun).where(DagRun.run_id == info["run_a_id"]))
        assert run_a.partition_key == "key-a"  # unchanged

    def test_clear_task_instances_non_dry_run(self, test_client, session, partitioned_dag_with_runs):
        """clear_task_instances=True clears TIs and reports the count."""
        info = partitioned_dag_with_runs
        # run_a has 2 TIs (task_x and task_y)
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"run_id": info["run_a_id"], "clear_task_instances": True, "dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 1
        assert body["task_instances_cleared"] == 2
        assert body["dry_run"] is False

    def test_clear_task_instances_dry_run_counts_tis(self, test_client, session, partitioned_dag_with_runs):
        """dry_run + clear_task_instances reports TI count without writing."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"run_id": info["run_a_id"], "clear_task_instances": True, "dry_run": True},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["task_instances_cleared"] == 2
        assert body["dry_run"] is True

        session.expire_all()
        run_a = session.scalar(select(DagRun).where(DagRun.run_id == info["run_a_id"]))
        assert run_a.partition_key == "key-a"  # not cleared (dry_run)

    @pytest.mark.parametrize(
        "build_body",
        [
            pytest.param(
                lambda d: {"run_id": d["run_a_id"], "partition_key": "key-a", "dry_run": True},
                id="run_id_and_partition_key",
            ),
            pytest.param(
                lambda d: {"dry_run": True},
                id="no_selector",
            ),
            pytest.param(
                lambda d: {
                    "partition_date_start": "2026-01-03T00:00:00Z",
                    "partition_date_end": "2026-01-01T00:00:00Z",
                    "dry_run": True,
                },
                id="start_after_end",
            ),
        ],
    )
    def test_invalid_selector_combination_returns_422(
        self, test_client, partitioned_dag_with_runs, build_body
    ):
        """Invalid selector combinations must be rejected with 422."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json=build_body(info),
        )
        assert response.status_code == 422

    def test_unauthenticated_returns_401(self, unauthenticated_test_client, partitioned_dag_with_runs):
        """Unauthenticated request must return 401."""
        info = partitioned_dag_with_runs
        response = unauthenticated_test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"partition_key": "key-a", "dry_run": True},
        )
        assert response.status_code == 401

    def test_unauthorized_returns_403(self, unauthorized_test_client, partitioned_dag_with_runs):
        """Unauthorized user must return 403."""
        info = partitioned_dag_with_runs
        response = unauthorized_test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"partition_key": "key-a", "dry_run": True},
        )
        assert response.status_code == 403

    def test_partition_key_no_match_returns_200_zero_count(
        self, test_client, session, partitioned_dag_with_runs
    ):
        """A partition_key matching no run returns 200 with zero counts, not 404."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"partition_key": "nonexistent-key", "dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 0
        assert body["task_instances_cleared"] == 0

        # Existing runs keep their partition fields.
        session.expire_all()
        run_a = session.scalar(select(DagRun).where(DagRun.run_id == info["run_a_id"]))
        assert run_a.partition_key == "key-a"

    @pytest.mark.parametrize(
        ("window", "expected_run_keys"),
        [
            pytest.param(
                {"partition_date_start": "2026-01-02T00:00:00Z"},
                ("run_b_id", "run_c_id"),
                id="start-only-includes-from-start-onward",
            ),
            pytest.param(
                {"partition_date_end": "2026-01-02T00:00:00Z"},
                ("run_a_id", "run_b_id"),
                id="end-only-includes-up-to-and-including-end",
            ),
        ],
    )
    def test_partition_date_single_bound_window(
        self, test_client, session, partitioned_dag_with_runs, window, expected_run_keys
    ):
        """A window with only one bound is open-ended on the missing side."""
        info = partitioned_dag_with_runs
        response = test_client.post(
            f"/dags/{info['dag_id']}/clearPartitions",
            json={"dry_run": False, **window},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 2

        session.expire_all()
        cleared_ids = {info[k] for k in expected_run_keys}
        for run_id in (info["run_a_id"], info["run_b_id"], info["run_c_id"]):
            run = session.scalar(select(DagRun).where(DagRun.run_id == run_id))
            if run_id in cleared_ids:
                assert run.partition_key is None
                assert run.partition_date is None
            else:
                assert run.partition_key is not None

    def test_cross_dag_run_id_collision_does_not_clear_other_dag(
        self, test_client, session, dag_maker, configure_git_connection_for_dag_bundle
    ):
        """Clearing by run_id only affects the target Dag; a second Dag with the same run_id is untouched."""
        shared_run_id = "shared_run_id"

        # Build target Dag with the shared run_id.
        dag_id_target = "cp_cross_dag_target"
        with dag_maker(
            dag_id_target,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            serialized=True,
        ):
            task_a = EmptyOperator(task_id="task_a")

        run_target = dag_maker.create_dagrun(
            run_id=shared_run_id,
            state=DagRunState.SUCCESS,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.REST_API,
            logical_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        run_target.partition_key = "key-target"
        run_target.partition_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ti_target = run_target.get_task_instance(task_id=task_a.task_id)
        ti_target.task = task_a
        ti_target.state = State.SUCCESS
        session.merge(ti_target)

        # Build bystander Dag with the same run_id.
        dag_id_bystander = "cp_cross_dag_bystander"
        with dag_maker(
            dag_id_bystander,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            serialized=True,
        ):
            task_b = EmptyOperator(task_id="task_b")

        run_bystander = dag_maker.create_dagrun(
            run_id=shared_run_id,
            state=DagRunState.SUCCESS,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.REST_API,
            logical_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        run_bystander.partition_key = "key-bystander"
        run_bystander.partition_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ti_bystander = run_bystander.get_task_instance(task_id=task_b.task_id)
        ti_bystander.task = task_b
        ti_bystander.state = State.SUCCESS
        session.merge(ti_bystander)

        dag_maker.sync_dagbag_to_db()
        session.flush()

        response = test_client.post(
            f"/dags/{dag_id_target}/clearPartitions",
            json={"run_id": shared_run_id, "clear_task_instances": True, "dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_runs_cleared"] == 1
        assert body["task_instances_cleared"] == 1

        # Target Dag's run and TI are cleared.
        session.expire_all()
        run_t = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag_id_target, DagRun.run_id == shared_run_id)
        )
        assert run_t.partition_key is None
        assert run_t.partition_date is None

        # Bystander Dag's run and TI are completely untouched.
        run_b = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag_id_bystander, DagRun.run_id == shared_run_id)
        )
        assert run_b.partition_key == "key-bystander"
        assert run_b.partition_date is not None

        ti_b_after = session.scalar(
            select(TaskInstance).where(
                TaskInstance.dag_id == dag_id_bystander, TaskInstance.run_id == shared_run_id
            )
        )
        assert ti_b_after.state == State.SUCCESS

    def test_cross_dag_run_id_collision_dry_run_counts_only_target_dag(
        self, test_client, session, dag_maker, configure_git_connection_for_dag_bundle
    ):
        """dry_run=True TI count only includes the target Dag's TIs, not a bystander sharing the same run_id."""
        shared_run_id = "shared_dry_run_id"

        dag_id_target = "cp_cross_dag_dry_target"
        with dag_maker(
            dag_id_target,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            serialized=True,
        ):
            task_a = EmptyOperator(task_id="task_a")
            task_b = EmptyOperator(task_id="task_b")

        run_target = dag_maker.create_dagrun(
            run_id=shared_run_id,
            state=DagRunState.SUCCESS,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.REST_API,
            logical_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        run_target.partition_key = "key-target-dry"
        run_target.partition_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for op in (task_a, task_b):
            ti = run_target.get_task_instance(task_id=op.task_id)
            ti.task = op
            ti.state = State.SUCCESS
            session.merge(ti)

        dag_id_bystander = "cp_cross_dag_dry_bystander"
        with dag_maker(
            dag_id_bystander,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            serialized=True,
        ):
            task_c = EmptyOperator(task_id="task_c")
            task_d = EmptyOperator(task_id="task_d")
            task_e = EmptyOperator(task_id="task_e")

        run_bystander = dag_maker.create_dagrun(
            run_id=shared_run_id,
            state=DagRunState.SUCCESS,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.REST_API,
            logical_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        run_bystander.partition_key = "key-bystander-dry"
        run_bystander.partition_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for op in (task_c, task_d, task_e):
            ti = run_bystander.get_task_instance(task_id=op.task_id)
            ti.task = op
            ti.state = State.SUCCESS
            session.merge(ti)

        dag_maker.sync_dagbag_to_db()
        session.flush()

        response = test_client.post(
            f"/dags/{dag_id_target}/clearPartitions",
            json={"run_id": shared_run_id, "clear_task_instances": True, "dry_run": True},
        )
        assert response.status_code == 200
        body = response.json()
        # Only the 2 TIs from the target Dag count; the 3 bystander TIs must not be included.
        assert body["task_instances_cleared"] == 2
        assert body["dry_run"] is True

        # Neither run is written to (dry_run=True).
        session.expire_all()
        run_t = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag_id_target, DagRun.run_id == shared_run_id)
        )
        assert run_t.partition_key == "key-target-dry"
        run_b = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag_id_bystander, DagRun.run_id == shared_run_id)
        )
        assert run_b.partition_key == "key-bystander-dry"


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

        allowed_scheduled_dag = DagModel(
            dag_id="allowed_scheduled",
            bundle_name="testing",
            fileloc="/tmp/dag_del_3.py",
            timetable_summary="2 2 * * *",
            is_stale=False,
            owners="test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            allowed_run_types=["scheduled"],
        )

        session.add(inactive_dag)
        session.add(import_errors_dag)
        session.add(allowed_scheduled_dag)
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

        run = session.scalars(
            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == expected_dag_run_id)
        ).one()

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
            "partition_date": None,
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
            (
                [],
                {
                    "detail": [
                        {
                            "type": "model_attributes_type",
                            "loc": ["body"],
                            "msg": "Input should be a valid dictionary or object to extract fields from",
                            "input": [],
                        }
                    ]
                },
            ),
        ],
    )
    def test_invalid_data(self, test_client, post_body, expected_detail):
        if isinstance(post_body, dict):
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

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.create_dagrun")
    def test_dagrun_creation_param_validation_error_returns_400(self, mock_create_dagrun, test_client):
        now = timezone.utcnow().isoformat()
        error_message = "Invalid input for param x"
        mock_create_dagrun.side_effect = ParamValidationError(error_message)

        response = test_client.post(f"/dags/{DAG1_ID}/dagRuns", json={"logical_date": now})
        assert response.status_code == 400
        assert response.json() == {"detail": error_message}

    @mock.patch("airflow.serialization.definitions.dag.SerializedDAG.create_dagrun")
    def test_dagrun_creation_non_validation_error_propagates(self, mock_create_dagrun, test_client):
        """
        Non-ParamValidationError exceptions from create_dagrun() must not be swallowed.

        TestClient's default raise_server_exceptions=True surfaces server-side
        exceptions to the caller; in production these would become a 500. The
        regression we are guarding against is the old behavior where any
        ValueError got silently converted to 400.
        """
        now = timezone.utcnow().isoformat()
        mock_create_dagrun.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            test_client.post(f"/dags/{DAG1_ID}/dagRuns", json={"logical_date": now})

    def test_should_respond_404_if_a_dag_is_inactive(self, test_client, session, testing_dag_bundle):
        now = timezone.utcnow().isoformat()
        self._dags_for_trigger_tests(session)
        response = test_client.post("/dags/inactive/dagRuns", json={"logical_date": now})
        assert response.status_code == 404
        assert response.json()["detail"] == "Dag with dag_id: 'inactive' not found"

    def test_should_respond_400_if_a_dag_has_import_errors(self, test_client, session, testing_dag_bundle):
        now = timezone.utcnow().isoformat()
        self._dags_for_trigger_tests(session)
        response = test_client.post("/dags/import_errors/dagRuns", json={"logical_date": now})
        assert response.status_code == 400
        assert (
            response.json()["detail"]
            == "Dag with dag_id: 'import_errors' has import errors and cannot be triggered"
        )

    def test_should_respond_400_if_manual_runs_denied(self, test_client, session, dag_maker):
        now = timezone.utcnow().isoformat()
        dag_id = "allowed_scheduled"
        with dag_maker(
            dag_id=dag_id,
            schedule="@daily",
            allowed_run_types=[DagRunType.SCHEDULED],
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")
        session.commit()

        response = test_client.post(f"/dags/{dag_id}/dagRuns", json={"logical_date": now})
        assert response.status_code == 400
        assert response.json()["detail"] == f"Dag with dag_id: '{dag_id}' does not allow manual runs"

    @time_machine.travel(timezone.utcnow(), tick=False)
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_response_409_for_duplicate_logical_date(self, test_client):
        RUN_ID_1 = "random_1"
        RUN_ID_2 = "random_2"
        now = from_datetime_to_zulu(timezone.utcnow())
        note = "duplicate logical date test"
        response_1 = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={"dag_run_id": RUN_ID_1, "note": note, "logical_date": now},
        )
        response_2 = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={"dag_run_id": RUN_ID_2, "note": note, "logical_date": now},
        )

        assert response_1.status_code == 200
        assert response_1.json() == {
            "bundle_version": None,
            "dag_display_name": DAG1_DISPLAY_NAME,
            "dag_run_id": RUN_ID_1,
            "dag_id": DAG1_ID,
            "dag_versions": mock.ANY,
            "logical_date": now,
            "queued_at": now,
            "start_date": None,
            "end_date": None,
            "duration": None,
            "run_after": now,
            "data_interval_start": now,
            "data_interval_end": now,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "state": "queued",
            "triggered_by": "rest_api",
            "triggering_user_name": "test",
            "conf": {},
            "note": note,
            "partition_key": None,
            "partition_date": None,
        }

        assert response_2.status_code == 409

    @pytest.mark.parametrize(
        ("data_interval_start", "data_interval_end"),
        [
            (
                LOGICAL_DATE1.isoformat(),
                None,
            ),
            (
                None,
                LOGICAL_DATE1.isoformat(),
            ),
        ],
    )
    def test_should_response_422_for_missing_start_date_or_end_date(
        self, test_client, data_interval_start, data_interval_end
    ):
        now = timezone.utcnow().isoformat()
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={
                "data_interval_start": data_interval_start,
                "data_interval_end": data_interval_end,
                "logical_date": now,
            },
        )
        assert response.status_code == 422
        assert (
            response.json()["detail"][0]["msg"]
            == "Value error, Either both data_interval_start and data_interval_end must be provided or both must be None"
        )

    def test_raises_validation_error_for_invalid_params(self, test_client):
        now = timezone.utcnow().isoformat()
        response = test_client.post(
            f"/dags/{DAG2_ID}/dagRuns",
            json={"conf": {"validated_number": 5000}, "logical_date": now},
        )
        assert response.status_code == 400
        assert "Invalid input for param validated_number" in response.json()["detail"]

    def test_response_404(self, test_client):
        now = timezone.utcnow().isoformat()
        response = test_client.post("/dags/randoms/dagRuns", json={"logical_date": now})
        assert response.status_code == 404
        assert response.json()["detail"] == "Dag with dag_id: 'randoms' not found"

    def test_response_409(self, test_client):
        now = timezone.utcnow().isoformat()
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns", json={"dag_run_id": DAG1_RUN1_ID, "logical_date": now}
        )
        assert response.status_code == 409
        response_json = response.json()
        assert "detail" in response_json
        assert list(response_json["detail"].keys()) == ["reason", "statement", "orig_error", "message"]

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_with_null_logical_date(self, test_client):
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={"logical_date": None},
        )
        assert response.status_code == 200
        assert response.json() == {
            "bundle_version": None,
            "dag_display_name": DAG1_DISPLAY_NAME,
            "dag_run_id": mock.ANY,
            "dag_id": DAG1_ID,
            "dag_versions": mock.ANY,
            "logical_date": None,
            "queued_at": mock.ANY,
            "run_after": mock.ANY,
            "start_date": None,
            "end_date": None,
            "duration": None,
            "data_interval_start": mock.ANY,
            "data_interval_end": mock.ANY,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "state": "queued",
            "triggered_by": "rest_api",
            "triggering_user_name": "test",
            "conf": {},
            "note": None,
            "partition_key": None,
            "partition_date": None,
        }

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_generate_unique_run_id_for_scheduled_dag(self, dag_maker, test_client, session):
        "Ensure manual triggers on scheduled DAGs don't conflict on run_id"
        scheduled_dag_id = "test_scheduled_dag"
        with dag_maker(
            dag_id=scheduled_dag_id,
            schedule="@daily",
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="test_task")

        session.commit()

        response_1 = test_client.post(
            f"/dags/{scheduled_dag_id}/dagRuns",
            json={
                "logical_date": "2025-12-11T16:00:00+00:00",
                "run_after": "2025-12-11T16:00:00+00:00",
            },
        )
        assert response_1.status_code == 200

        response_2 = test_client.post(
            f"/dags/{scheduled_dag_id}/dagRuns",
            json={
                "logical_date": "2025-12-11T16:01:00+00:00",
                "run_after": "2025-12-11T16:01:00+00:00",
            },
        )
        assert response_2.status_code == 200

        assert response_1.json()["dag_run_id"] != response_2.json()["dag_run_id"]

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

        run = session.scalars(select(DagRun).where(DagRun.run_id == run_id_with_logical_date)).one()
        assert run.dag_id == custom_dag_id

        response = test_client.post(
            f"/dags/{custom_dag_id}/dagRuns",
            json={"logical_date": None},
        )
        assert response.status_code == 200
        run_id_without_logical_date = response.json()["dag_run_id"]
        assert run_id_without_logical_date.startswith("custom_manual_")

        run = session.scalars(select(DagRun).where(DagRun.run_id == run_id_without_logical_date)).one()
        assert run.dag_id == custom_dag_id

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_trigger_dag_run_with_bundle_version(self, test_client, session, dag_maker):
        """Test triggering a DAG run with a specific bundle version."""
        from tests_common.test_utils.dag import sync_dag_to_db

        dag_id = "test_bundle_version_dag"
        bundle_name = "testing_bundle"

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v1",
            session=session,
        ) as dag1:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag1, bundle_name=bundle_name, bundle_version="v1")

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v2",
            session=session,
        ) as dag2:
            EmptyOperator(task_id="task_1")
            EmptyOperator(task_id="task_2")
        sync_dag_to_db(dag2, bundle_name=bundle_name, bundle_version="v2")

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns", json={"logical_date": "2024-01-01T00:00:00Z", "bundle_version": "v1"}
        )
        assert response.status_code == 200
        assert response.json()["dag_versions"][0]["bundle_version"] == "v1"
        run_id_v1 = response.json()["dag_run_id"]
        dr_v1 = session.scalars(select(DagRun).where(DagRun.run_id == run_id_v1)).one()
        assert {ti.task_id for ti in dr_v1.task_instances} == {"task_1"}

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={
                "logical_date": "2024-01-02T00:00:00Z",
            },
        )
        assert response.status_code == 200
        assert response.json()["dag_versions"][0]["bundle_version"] == "v2"
        run_id_v2 = response.json()["dag_run_id"]
        dr_v2 = session.scalars(select(DagRun).where(DagRun.run_id == run_id_v2)).one()
        assert {ti.task_id for ti in dr_v2.task_instances} == {"task_1", "task_2"}

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={"logical_date": "2024-01-03T00:00:00Z", "bundle_version": "invalid_version"},
        )
        assert response.status_code == 404
        assert (
            f"DAG with dag_id: '{dag_id}' does not have a version for bundle_version 'invalid_version'"
            in response.json()["detail"]
        )

        dag2.disable_bundle_versioning = True
        sync_dag_to_db(dag2, bundle_name=bundle_name)

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns", json={"logical_date": "2024-01-04T00:00:00Z", "bundle_version": "v1"}
        )
        assert response.status_code == 400
        assert f"DAG with dag_id: '{dag_id}' does not support bundle versioning" in response.json()["detail"]

    def test_trigger_dag_run_bundle_version_validates_against_old_param_schema(
        self, test_client, session, dag_maker
    ):
        """Conf is validated against the requested bundle version's param schema, not the live dag's."""
        from tests_common.test_utils.dag import sync_dag_to_db

        dag_id = "test_bundle_param_schema_dag"
        bundle_name = "param_schema_bundle"

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v1",
            session=session,
            params={"env": Param("staging", type="string", enum=["staging", "prod"])},
        ) as dag1:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag1, bundle_name=bundle_name, bundle_version="v1")

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v2",
            session=session,
            params={"env": Param("dev", type="string", enum=["dev", "staging", "prod"])},
        ) as dag2:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag2, bundle_name=bundle_name, bundle_version="v2")

        # "dev" is valid for v2 but not for v1's enum — triggering v1 should reject it.
        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={"logical_date": "2024-02-01T00:00:00Z", "bundle_version": "v1", "conf": {"env": "dev"}},
        )
        assert response.status_code == 400

        # "staging" is valid for both v1 and v2 — triggering v1 should accept it.
        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={
                "logical_date": "2024-02-02T00:00:00Z",
                "bundle_version": "v1",
                "conf": {"env": "staging"},
            },
        )
        assert response.status_code == 200

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_trigger_dag_run_bundle_version_uses_v1_timetable(self, test_client, session, dag_maker):
        """Triggering with bundle_version='v1' must derive data_interval from v1's timetable, not v2's."""
        from tests_common.test_utils.dag import sync_dag_to_db

        dag_id = "test_bundle_timetable_dag"
        bundle_name = "timetable_bundle"

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v1",
            schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
            session=session,
        ) as dag1:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag1, bundle_name=bundle_name, bundle_version="v1")

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v2",
            schedule=None,
            session=session,
        ) as dag2:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag2, bundle_name=bundle_name, bundle_version="v2")

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={"logical_date": "2024-01-01T00:00:00Z", "bundle_version": "v1"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["dag_versions"][0]["bundle_version"] == "v1"
        # data_interval must come from v1's daily cron timetable, not v2's null timetable.
        # For a "0 0 * * *" cron, logical_date is the interval END, so interval is [prev_day, logical_date].
        assert data["data_interval_start"] == "2023-12-31T00:00:00Z"
        assert data["data_interval_end"] == "2024-01-01T00:00:00Z"

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_trigger_dag_run_allowed_run_types_from_requested_version(self, test_client, session, dag_maker):
        """allowed_run_types is enforced from the requested bundle version, not the latest."""
        from tests_common.test_utils.dag import sync_dag_to_db

        dag_id = "test_bundle_allowed_run_types_dag"
        bundle_name = "allowed_run_types_bundle"

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v1",
            schedule="@daily",
            allowed_run_types=[DagRunType.MANUAL, DagRunType.SCHEDULED],
            session=session,
        ) as dag1:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag1, bundle_name=bundle_name, bundle_version="v1")

        with dag_maker(
            dag_id=dag_id,
            bundle_name=bundle_name,
            bundle_version="v2",
            schedule="@daily",
            allowed_run_types=[DagRunType.SCHEDULED],
            session=session,
        ) as dag2:
            EmptyOperator(task_id="task_1")
        sync_dag_to_db(dag2, bundle_name=bundle_name, bundle_version="v2")

        # Latest (v2) disallows manual runs; v1 allows them. Triggering v1 must succeed.
        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={"logical_date": "2024-02-01T00:00:00Z", "bundle_version": "v1"},
        )
        assert response.status_code == 200

        # Without bundle_version the latest (v2) governs and rejects the manual run.
        response = test_client.post(
            f"/dags/{dag_id}/dagRuns",
            json={"logical_date": "2024-02-02T00:00:00Z"},
        )
        assert response.status_code == 400
        assert response.json()["detail"] == f"Dag with dag_id: '{dag_id}' does not allow manual runs"

    def test_should_respond_400_when_partition_key_given_for_non_partitioned_dag(self, test_client):
        """Passing partition_key to a non-partitioned Dag via REST trigger must return 400, not 500.

        The validation happens in TriggerDAGRunPostBody.validate_context(), which is now called
        inside the try/except block that converts ValueError to HTTP 400.
        """
        now = timezone.utcnow().isoformat()
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={"logical_date": now, "partition_key": "some-partition"},
        )
        assert response.status_code == 400
        assert "not a partitioned Dag" in response.json()["detail"]

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_when_partition_key_given_for_partitioned_dag(
        self, dag_maker, test_client, session
    ):
        """partition_key on a genuinely partitioned Dag must not be rejected (happy-path guard).

        Uses CronPartitionTimetable (partitioned=True) to confirm the reject path does not
        fire for legitimate partitioned Dags.
        """
        partitioned_dag_id = "test_partitioned_dag_trigger"
        with dag_maker(
            dag_id=partitioned_dag_id,
            schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{partitioned_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "2025-01-01T00:00:00"},
        )
        assert response.status_code == 200

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_when_partition_key_given_for_partitioned_at_runtime_dag(
        self, dag_maker, test_client, session
    ):
        """partition_key on a PartitionedAtRuntime Dag must also be accepted (deferred validation).

        partitioned_at_runtime=True means the Dag accepts runtime-discovered partition keys, so
        the REST layer must not reject it even though timetable.partitioned is False.
        """
        runtime_dag_id = "test_partitioned_at_runtime_dag_trigger"
        with dag_maker(
            dag_id=runtime_dag_id,
            schedule=PartitionedAtRuntime(),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{runtime_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "runtime-key"},
        )
        assert response.status_code == 200

    def test_should_respond_400_when_empty_partition_key(self, test_client):
        """An empty partition_key must return 400, not 500."""
        now = timezone.utcnow().isoformat()
        response = test_client.post(
            f"/dags/{DAG1_ID}/dagRuns",
            json={"logical_date": now, "partition_key": ""},
        )
        assert response.status_code == 400
        assert (
            response.json()["detail"]
            == "Dag 'test_dag1' is not a partitioned Dag and does not accept a partition_key."
        )

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_400_when_over_length_partition_key(self, dag_maker, test_client, session):
        """A partition_key exceeding 250 characters must return 400, not 500."""
        partitioned_dag_id = "test_over_length_partition_key"
        with dag_maker(
            dag_id=partitioned_dag_id,
            schedule=CronPartitionTimetable("0 * * * *", timezone="UTC"),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{partitioned_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "a" * 251},
        )
        assert response.status_code == 400
        assert "at most 250 characters" in response.json()["detail"]

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_respond_200_when_exactly_max_length_partition_key(self, dag_maker, test_client, session):
        """A partition_key of exactly 250 characters must be accepted."""
        partitioned_dag_id = "test_max_length_partition_key"
        with dag_maker(
            dag_id=partitioned_dag_id,
            schedule=PartitionedAssetTimetable(assets=Asset("test")),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{partitioned_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "a" * 250},
        )
        assert response.status_code == 200

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_trigger_partitioned_dag_populates_partition_date(self, dag_maker, test_client, session):
        """Triggering a CronPartitionTimetable Dag with a valid key populates partition_date on the run.

        Regression guard: before this fix partition_date was NULL for manually triggered runs even
        when partition_key was supplied, making partition-date-based filtering (e.g.
        ``airflow dags clear --partition-date-*``) silently skip those runs.
        """
        partitioned_dag_id = "test_trigger_populates_partition_date"
        with dag_maker(
            dag_id=partitioned_dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{partitioned_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "2025-06-01T00:00:00"},
        )
        assert response.status_code == 200

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == partitioned_dag_id))
        assert dag_run is not None
        assert dag_run.partition_key == "2025-06-01T00:00:00"
        assert dag_run.partition_date == datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_trigger_partitioned_dag_invalid_key_returns_400(self, dag_maker, test_client, session):
        """An invalid partition_key for a CronPartitionTimetable Dag must return HTTP 400."""
        partitioned_dag_id = "test_trigger_invalid_partition_key"
        with dag_maker(
            dag_id=partitioned_dag_id,
            schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{partitioned_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "not-a-valid-date"},
        )
        assert response.status_code == 400

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_trigger_partitioned_at_runtime_dag_leaves_partition_date_none(
        self, dag_maker, test_client, session
    ):
        """PartitionedAtRuntime Dag with an arbitrary key must produce partition_date=None."""
        runtime_dag_id = "test_trigger_partitioned_at_runtime_none_date"
        with dag_maker(
            dag_id=runtime_dag_id,
            schedule=PartitionedAtRuntime(),
            start_date=START_DATE1,
            session=session,
            serialized=True,
        ):
            EmptyOperator(task_id="task")

        session.commit()

        response = test_client.post(
            f"/dags/{runtime_dag_id}/dagRuns",
            json={"logical_date": None, "partition_key": "arbitrary-key"},
        )
        assert response.status_code == 200

        dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == runtime_dag_id))
        assert dag_run is not None
        assert dag_run.partition_key == "arbitrary-key"
        assert dag_run.partition_date is None


class TestResolveRunOnLatestVersion:
    @pytest.mark.parametrize("explicit_value", [True, False])
    def test_explicit_value_takes_precedence(self, explicit_value, dag_maker, session):
        """Explicit value always wins, regardless of DAG or global config."""

        with dag_maker("test_resolver_explicit", serialized=True, session=session):
            ...

        result = resolve_run_on_latest_version(explicit_value, "test_resolver_explicit", session)
        assert result is explicit_value

    def test_dag_level_takes_precedence_over_global(self, dag_maker, session):
        """DAG-level rerun_with_latest_version=True takes precedence over global False."""

        with dag_maker("test_resolver_dag", serialized=True, session=session, rerun_with_latest_version=True):
            ...

        result = resolve_run_on_latest_version(None, "test_resolver_dag", session)
        assert result is True

    def test_global_config_used_when_dag_not_set(self, dag_maker, session):
        """Falls back to global config when DAG doesn't set rerun_with_latest_version."""

        with dag_maker("test_resolver_global", serialized=True, session=session):
            ...

        with mock.patch("airflow.configuration.conf.getboolean", return_value=True):
            result = resolve_run_on_latest_version(None, "test_resolver_global", session)
        assert result is True

    def test_default_is_false(self, dag_maker, session):
        """Returns False when no explicit value, no DAG config, no global config."""

        with dag_maker("test_resolver_default", serialized=True, session=session):
            ...

        result = resolve_run_on_latest_version(None, "test_resolver_default", session)
        assert result is False

    def test_fallback_true_for_backfills(self, dag_maker, session):
        """Backfill callers pass fallback=True to preserve historical default."""

        with dag_maker("test_resolver_fallback_true", serialized=True, session=session):
            ...

        # With no DAG config and no global config set, the fallback kicks in
        result = resolve_run_on_latest_version(None, "test_resolver_fallback_true", session, fallback=True)
        assert result is True

    def test_dag_level_false_overrides_fallback_true(self, dag_maker, session):
        """DAG-level False takes precedence over a True fallback (backfill case)."""

        with dag_maker(
            "test_resolver_dag_false",
            serialized=True,
            session=session,
            rerun_with_latest_version=False,
        ):
            ...

        result = resolve_run_on_latest_version(None, "test_resolver_dag_false", session, fallback=True)
        assert result is False

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_clear_endpoint_invokes_resolver_when_field_omitted(self, test_client):
        """Clearing without run_on_latest_version triggers the server-side resolver."""
        with mock.patch(
            "airflow.api_fastapi.core_api.services.public.dag_run.resolve_run_on_latest_version",
            return_value=False,
        ) as mock_resolver:
            response = test_client.post(
                f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
                json={"dry_run": False},
            )
        assert response.status_code == 200
        mock_resolver.assert_called_once()
        # First positional arg should be None (omitted from request body)
        assert mock_resolver.call_args.args[0] is None


class TestWaitDagRun:
    # The way we init async engine does not work well with FastAPI app init.
    # Creating the engine implicitly creates an event loop, which Airflow does
    # once for the entire process; creating the FastAPI app also does, but our
    # test setup does it once for each test. I don't know how to properly fix
    # this without rewriting how Airflow does db; re-configuring the db for each
    # test at least makes the tests run correctly.
    @pytest.fixture(autouse=True)
    def reconfigure_async_db_engine(self):
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
        ("run_id", "expected"),
        [
            pytest.param(
                DAG1_RUN1_ID,
                {"state": DAG1_RUN1_STATE, "results": {"task_2": '"result_2"'}},
                id="return-result-task",
            ),
            pytest.param(
                DAG1_RUN2_ID,
                {"state": DAG1_RUN2_STATE},
                id="no-result-task",
            ),
        ],
    )
    def test_should_respond_200_with_implicit_return_value(self, test_client, run_id, expected):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns/{run_id}/wait", params={"interval": "100"})
        assert response.status_code == 200
        data = response.json()
        assert data == expected

    @pytest.mark.parametrize(
        ("requested", "results"),
        [
            pytest.param("task_1", {"task_1": '"result_1"'}, id="only-non-result"),
            pytest.param("task_2", {"task_2": '"result_2"'}, id="only-result"),
        ],
    )
    def test_should_respond_200_with_explicit_return_value(self, test_client, requested, results):
        response = test_client.get(
            f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait",
            params={"interval": "1", "result": requested},
        )
        assert response.status_code == 200
        data = response.json()
        assert data == {"state": DagRunState.SUCCESS, "results": results}

    def test_collect_authored_task_results(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait", params={"interval": "1"})
        assert response.status_code == 200
        data = response.json()
        assert data == {"state": DagRunState.SUCCESS, "results": {"task_2": '"result_2"'}}

    def test_should_respond_403_when_user_lacks_xcom_permission(self, test_client):
        with mock.patch(
            "airflow.api_fastapi.core_api.routes.public.dag_run.get_auth_manager",
            autospec=True,
        ) as mock_get_auth_manager:
            mock_get_auth_manager.return_value.is_authorized_dag.return_value = False

            response = test_client.get(
                f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait",
                params={"interval": "1", "result": "task_1"},
            )

            assert response.status_code == 403
            mock_get_auth_manager.return_value.is_authorized_dag.assert_called_once_with(
                method="GET",
                access_entity=DagAccessEntity.XCOM,
                details=DagDetails(id=DAG1_ID),
                user=mock.ANY,
            )

    def test_should_respond_200_without_result_when_user_lacks_xcom_permission(self, test_client):
        """Waiting without result parameter should not require XCom permissions."""
        with mock.patch(
            "airflow.api_fastapi.core_api.routes.public.dag_run.get_auth_manager",
            autospec=True,
        ) as mock_get_auth_manager:
            mock_get_auth_manager.return_value.is_authorized_dag.return_value = False

            response = test_client.get(
                f"/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/wait",
                params={"interval": "1"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data == {"state": DagRunState.SUCCESS}

    def test_collect_mapped_task_dag_result(self, test_client, dag_maker, session):
        """XComs from a mapped @result task are aggregated into a list ordered by map_index."""
        with dag_maker("dag_mapped_result"):

            @result
            @task(task_id="a")
            def double(v):
                return v * 2

            mapped = double.expand(v=[1, 2])

        mapped_op = mapped.operator  # MappedOperator with returns_dag_result=True

        dag_run = dag_maker.create_dagrun(
            run_id="mapped_run_1",
            state=DagRunState.SUCCESS,
            run_type=DagRunType.MANUAL,
            triggered_by=DagRunTriggeredByType.UI,
            logical_date=LOGICAL_DATE1,
        )
        for ti in dag_run.task_instances:
            run_task_instance(ti, mapped_op, session=session)
        session.commit()

        response = test_client.get(
            f"/dags/dag_mapped_result/dagRuns/{dag_run.run_id}/wait",
            params={"interval": "1"},
        )
        assert response.status_code == 200
        assert response.json() == {"state": DagRunState.SUCCESS, "results": {"a": [2, 4]}}


class TestBulkDagRuns:
    ENDPOINT_URL = f"/dags/{DAG1_ID}/dagRuns"
    WILDCARD_ENDPOINT = "/dags/~/dagRuns"

    def test_bulk_delete(self, test_client, session):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [DAG1_RUN1_ID, DAG1_RUN2_ID],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert sorted(body["delete"]["success"]) == sorted(
            [f"{DAG1_ID}.{DAG1_RUN1_ID}", f"{DAG1_ID}.{DAG1_RUN2_ID}"]
        )
        session.expire_all()
        remaining = session.scalars(select(DagRun).where(DagRun.dag_id == DAG1_ID)).all()
        assert remaining == []

    def test_bulk_delete_with_entity_object(self, test_client, session):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [{"dag_run_id": DAG1_RUN1_ID}],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["delete"]["success"] == [f"{DAG1_ID}.{DAG1_RUN1_ID}"]
        session.expire_all()
        dr = session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID))
        assert dr is None

    def test_bulk_delete_rejects_running_state(self, test_client, dag_maker, session):
        """Mirror the single-run DELETE: a RUNNING Dag Run can't be bulk-deleted (409)."""
        with dag_maker(dag_id="test_running_bulk_dag"):
            EmptyOperator(task_id="t1")
        dag_maker.create_dagrun(run_id="running_run", state=DagRunState.RUNNING)
        session.commit()

        response = test_client.patch(
            "/dags/test_running_bulk_dag/dagRuns",
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": ["running_run"],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["delete"]["success"] == []
        assert body["delete"]["errors"] == [
            {
                "error": (
                    "The DagRun with dag_id: `test_running_bulk_dag` and run_id: `running_run` "
                    "cannot be deleted in running state"
                ),
                "status_code": 409,
            }
        ]
        session.expire_all()
        assert session.scalar(select(DagRun).where(DagRun.run_id == "running_run")) is not None

    def test_bulk_delete_not_found_fails(self, test_client, session):
        """FAIL semantics: a single missing run fails the whole action and nothing is deleted."""
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [DAG1_RUN1_ID, "non_existent_run", "another_missing_run"],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["delete"]["success"] == []
        errors = body["delete"]["errors"]
        assert len(errors) == 1
        assert errors[0]["status_code"] == 404
        assert "non_existent_run" in errors[0]["error"]
        assert "another_missing_run" in errors[0]["error"]
        session.expire_all()
        # The matched run must not be deleted when another entity is missing.
        assert session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID)) is not None

    def test_bulk_delete_not_found_skip(self, test_client, session):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "action_on_non_existence": "skip",
                        "entities": [DAG1_RUN1_ID, "non_existent_run"],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["delete"]["success"] == [f"{DAG1_ID}.{DAG1_RUN1_ID}"]
        assert body["delete"]["errors"] == []

    def test_bulk_delete_across_dags_with_wildcard(self, test_client, session):
        response = test_client.patch(
            self.WILDCARD_ENDPOINT,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [
                            {"dag_id": DAG1_ID, "dag_run_id": DAG1_RUN1_ID},
                            {"dag_id": DAG2_ID, "dag_run_id": DAG2_RUN1_ID},
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert sorted(body["delete"]["success"]) == sorted(
            [f"{DAG1_ID}.{DAG1_RUN1_ID}", f"{DAG2_ID}.{DAG2_RUN1_ID}"]
        )
        session.expire_all()
        assert session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID)) is None
        assert session.scalar(select(DagRun).where(DagRun.run_id == DAG2_RUN1_ID)) is None

    def test_bulk_delete_wildcard_requires_dag_id_in_body(self, test_client):
        response = test_client.patch(
            self.WILDCARD_ENDPOINT,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [DAG1_RUN1_ID],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["delete"]["success"] == []
        assert len(body["delete"]["errors"]) == 1
        assert body["delete"]["errors"][0]["status_code"] == 400

    def test_bulk_create_not_supported(self, test_client):
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "create",
                        "entities": [{"dag_run_id": "brand_new_run"}],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["create"]["success"] == []
        assert len(body["create"]["errors"]) == 1
        assert body["create"]["errors"][0]["status_code"] == 405

    def test_bulk_update_marks_state(self, test_client, session):
        """Bulk update marks the selected Dag Runs to the requested state in a single call."""
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {"dag_run_id": DAG1_RUN1_ID, "state": "failed"},
                            {"dag_run_id": DAG1_RUN2_ID, "state": "failed"},
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert sorted(body["update"]["success"]) == sorted(
            [f"{DAG1_ID}.{DAG1_RUN1_ID}", f"{DAG1_ID}.{DAG1_RUN2_ID}"]
        )
        assert body["update"]["errors"] == []
        session.expire_all()
        for run_id in (DAG1_RUN1_ID, DAG1_RUN2_ID):
            dag_run = session.scalar(select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == run_id))
            assert dag_run.state == DagRunState.FAILED

    def test_bulk_update_across_dags_with_wildcard(self, test_client, session):
        """``~`` URL with per-entity dag_id marks runs across Dags in one call."""
        response = test_client.patch(
            self.WILDCARD_ENDPOINT,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {"dag_id": DAG1_ID, "dag_run_id": DAG1_RUN1_ID, "state": "success"},
                            {"dag_id": DAG2_ID, "dag_run_id": DAG2_RUN1_ID, "state": "success"},
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert sorted(body["update"]["success"]) == sorted(
            [f"{DAG1_ID}.{DAG1_RUN1_ID}", f"{DAG2_ID}.{DAG2_RUN1_ID}"]
        )
        session.expire_all()
        for run_id in (DAG1_RUN1_ID, DAG2_RUN1_ID):
            assert session.scalar(select(DagRun).where(DagRun.run_id == run_id)).state == DagRunState.SUCCESS

    def test_bulk_update_note_only(self, test_client, session):
        """A bulk update may set only the note, without a target state."""
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [{"dag_run_id": DAG1_RUN1_ID, "note": "bulk note"}],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["update"]["success"] == [f"{DAG1_ID}.{DAG1_RUN1_ID}"]
        assert body["update"]["errors"] == []
        session.expire_all()
        dag_run = session.scalar(
            select(DagRun).where(DagRun.dag_id == DAG1_ID, DagRun.run_id == DAG1_RUN1_ID)
        )
        assert dag_run.note == "bulk note"
        assert dag_run.state == DAG1_RUN1_STATE

    def test_bulk_update_not_found_fails(self, test_client, session):
        """FAIL semantics: a single missing run fails the whole action and nothing is updated."""
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {"dag_run_id": DAG1_RUN1_ID, "state": "failed"},
                            {"dag_run_id": "non_existent_run", "state": "failed"},
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["update"]["success"] == []
        assert len(body["update"]["errors"]) == 1
        assert body["update"]["errors"][0]["status_code"] == 404
        assert "non_existent_run" in body["update"]["errors"][0]["error"]
        session.expire_all()
        # The matched run must keep its original state when another entity is missing.
        dag_run = session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID))
        assert dag_run.state == DAG1_RUN1_STATE

    def test_bulk_update_not_found_skip(self, test_client, session):
        """SKIP semantics: missing runs are ignored and matched runs are still updated."""
        response = test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "update",
                        "action_on_non_existence": "skip",
                        "entities": [
                            {"dag_run_id": DAG1_RUN1_ID, "state": "failed"},
                            {"dag_run_id": "non_existent_run", "state": "failed"},
                        ],
                    }
                ]
            },
        )
        assert response.status_code == 200
        body = response.json()
        assert body["update"]["success"] == [f"{DAG1_ID}.{DAG1_RUN1_ID}"]
        assert body["update"]["errors"] == []
        session.expire_all()
        dag_run = session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID))
        assert dag_run.state == DagRunState.FAILED

    def test_bulk_delete_rejects_unauthorized_dag_ids_from_request_body(self, test_client, session):
        """A 403 at the route level if any entity references a Dag the user can't access."""
        restricted_bundle_name = "restricted-bundle-delete"
        restricted_team_name = "restricted-team-delete"
        restricted_bundle = DagBundleModel(name=restricted_bundle_name)
        restricted_team = Team(name=restricted_team_name)
        restricted_bundle.teams.append(restricted_team)
        session.add_all([restricted_bundle, restricted_team])
        session.flush()
        # Restrict DAG2 by attaching it to a team-scoped bundle the limited user has no access to.
        session.execute(
            update(DagModel).where(DagModel.dag_id == DAG2_ID).values(bundle_name=restricted_bundle_name)
        )
        session.commit()

        auth_manager = test_client.app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(
                SimpleAuthManagerUser(username="limited-user", role="user", teams=[]),
            )
        )
        with (
            mock.patch("airflow.models.revoked_token.RevokedToken.is_revoked", return_value=False),
            TestClient(
                test_client.app,
                headers={"Authorization": f"Bearer {token}"},
                base_url=str(test_client.base_url),
            ) as limited_test_client,
        ):
            response = limited_test_client.patch(
                self.WILDCARD_ENDPOINT,
                json={
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [
                                {"dag_id": DAG1_ID, "dag_run_id": DAG1_RUN1_ID},
                                {"dag_id": DAG2_ID, "dag_run_id": DAG2_RUN1_ID},
                            ],
                        }
                    ]
                },
            )

        assert response.status_code == 403
        session.expire_all()
        assert session.scalar(select(DagRun).where(DagRun.run_id == DAG1_RUN1_ID)) is not None
        assert session.scalar(select(DagRun).where(DagRun.run_id == DAG2_RUN1_ID)) is not None

    def test_bulk_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(self.ENDPOINT_URL, json={"actions": []})
        assert response.status_code == 401

    def test_bulk_should_respond_403(self, unauthorized_test_client):
        """An authenticated user with no Dag permissions gets a 403 at the route level."""
        response = unauthorized_test_client.patch(
            self.ENDPOINT_URL,
            json={
                "actions": [
                    {
                        "action": "delete",
                        "entities": [DAG1_RUN1_ID],
                    }
                ]
            },
        )
        assert response.status_code == 403

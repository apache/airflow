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
from unittest import mock

import pytest
import time_machine
from sqlalchemy import select

from airflow.models import DagModel, DagRun
from airflow.models.asset import AssetEvent, AssetModel
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import (
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu, from_datetime_to_zulu_without_ms

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
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
START_DATE2 = datetime(2024, 4, 15, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE3 = datetime(2024, 5, 16, 0, 0, tzinfo=timezone.utc)
LOGICAL_DATE4 = datetime(2024, 5, 25, 0, 0, tzinfo=timezone.utc)
DAG1_RUN1_NOTE = "test_note"
DAG2_PARAM = {"validated_number": Param(1, minimum=1, maximum=10)}

DAG_RUNS_LIST = [DAG1_RUN1_ID, DAG1_RUN2_ID, DAG2_RUN1_ID, DAG2_RUN2_ID]


@pytest.fixture(autouse=True)
@provide_session
def setup(request, dag_maker, session=None):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    if "no_setup" in request.keywords:
        return

    with dag_maker(
        DAG1_ID,
        schedule=None,
        start_date=START_DATE1,
    ):
        task1 = EmptyOperator(task_id="task_1")
    dag_run1 = dag_maker.create_dagrun(
        run_id=DAG1_RUN1_ID,
        state=DAG1_RUN1_STATE,
        run_type=DAG1_RUN1_RUN_TYPE,
        triggered_by=DAG1_RUN1_TRIGGERED_BY,
        logical_date=LOGICAL_DATE1,
    )

    dag_run1.note = (DAG1_RUN1_NOTE, 1)

    ti1 = dag_run1.get_task_instance(task_id="task_1")
    ti1.task = task1
    ti1.state = State.SUCCESS

    dag_maker.create_dagrun(
        run_id=DAG1_RUN2_ID,
        state=DAG1_RUN2_STATE,
        run_type=DAG1_RUN2_RUN_TYPE,
        triggered_by=DAG1_RUN2_TRIGGERED_BY,
        logical_date=LOGICAL_DATE2,
    )

    with dag_maker(DAG2_ID, schedule=None, start_date=START_DATE2, params=DAG2_PARAM):
        EmptyOperator(task_id="task_2")
    dag_maker.create_dagrun(
        run_id=DAG2_RUN1_ID,
        state=DAG2_RUN1_STATE,
        run_type=DAG2_RUN1_RUN_TYPE,
        triggered_by=DAG2_RUN1_TRIGGERED_BY,
        logical_date=LOGICAL_DATE3,
    )
    dag_maker.create_dagrun(
        run_id=DAG2_RUN2_ID,
        state=DAG2_RUN2_STATE,
        run_type=DAG2_RUN2_RUN_TYPE,
        triggered_by=DAG2_RUN2_TRIGGERED_BY,
        logical_date=LOGICAL_DATE4,
    )

    dag_maker.dagbag.sync_to_db()
    dag_maker.dag_model
    dag_maker.dag_model.has_task_concurrency_limits = True
    session.merge(ti1)
    session.merge(dag_maker.dag_model)
    session.commit()


class TestGetDagRun:
    @pytest.mark.parametrize(
        "dag_id, run_id, state, run_type, triggered_by, dag_run_note",
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
    def test_get_dag_run(self, test_client, dag_id, run_id, state, run_type, triggered_by, dag_run_note):
        response = test_client.get(f"/public/dags/{dag_id}/dagRuns/{run_id}")
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == dag_id
        assert body["dag_run_id"] == run_id
        assert body["state"] == state
        assert body["run_type"] == run_type
        assert body["triggered_by"] == triggered_by.value
        assert body["note"] == dag_run_note

    def test_get_dag_run_not_found(self, test_client):
        response = test_client.get(f"/public/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"


class TestGetDagRuns:
    @staticmethod
    def get_dag_run_dict(run: DagRun):
        return {
            "dag_run_id": run.run_id,
            "dag_id": run.dag_id,
            "logical_date": from_datetime_to_zulu_without_ms(run.logical_date),
            "queued_at": from_datetime_to_zulu(run.queued_at) if run.queued_at else None,
            "start_date": from_datetime_to_zulu_without_ms(run.start_date),
            "end_date": from_datetime_to_zulu(run.end_date),
            "data_interval_start": from_datetime_to_zulu_without_ms(run.data_interval_start),
            "data_interval_end": from_datetime_to_zulu_without_ms(run.data_interval_end),
            "last_scheduling_decision": from_datetime_to_zulu(run.last_scheduling_decision)
            if run.last_scheduling_decision
            else None,
            "run_type": run.run_type,
            "state": run.state,
            "external_trigger": run.external_trigger,
            "triggered_by": run.triggered_by.value,
            "conf": run.conf,
            "note": run.note,
        }

    @pytest.mark.parametrize("dag_id, total_entries", [(DAG1_ID, 2), (DAG2_ID, 2), ("~", 4)])
    def test_get_dag_runs(self, test_client, session, dag_id, total_entries):
        response = test_client.get(f"/public/dags/{dag_id}/dagRuns")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == total_entries
        for each in body["dag_runs"]:
            run = (
                session.query(DagRun)
                .where(DagRun.dag_id == each["dag_id"], DagRun.run_id == each["dag_run_id"])
                .one()
            )
            expected = self.get_dag_run_dict(run)
            assert each == expected

    def test_get_dag_runs_not_found(self, test_client):
        response = test_client.get("/public/dags/invalid/dagRuns")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DAG with dag_id: `invalid` was not found"

    def test_invalid_order_by_raises_400(self, test_client):
        response = test_client.get("/public/dags/test_dag1/dagRuns?order_by=invalid")
        assert response.status_code == 400
        body = response.json()
        assert (
            body["detail"]
            == "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        )

    @pytest.mark.parametrize(
        "order_by,expected_order",
        [
            pytest.param("id", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_id"),
            pytest.param("state", [DAG1_RUN2_ID, DAG1_RUN1_ID], id="order_by_state"),
            pytest.param("dag_id", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_dag_id"),
            pytest.param("logical_date", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_logical_date"),
            pytest.param("dag_run_id", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_dag_run_id"),
            pytest.param("start_date", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_start_date"),
            pytest.param("end_date", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_end_date"),
            pytest.param("updated_at", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_updated_at"),
            pytest.param("external_trigger", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_external_trigger"),
            pytest.param("conf", [DAG1_RUN1_ID, DAG1_RUN2_ID], id="order_by_conf"),
        ],
    )
    def test_return_correct_results_with_order_by(self, test_client, order_by, expected_order):
        # Test ascending order
        response = test_client.get("/public/dags/test_dag1/dagRuns", params={"order_by": order_by})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_order

        # Test descending order
        response = test_client.get("/public/dags/test_dag1/dagRuns", params={"order_by": f"-{order_by}"})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_order[::-1]

    @pytest.mark.parametrize(
        "query_params, expected_dag_id_order",
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
    def test_limit_and_offset(self, test_client, query_params, expected_dag_id_order):
        response = test_client.get("/public/dags/test_dag1/dagRuns", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_order

    @pytest.mark.parametrize(
        "query_params, expected_detail",
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
        response = test_client.get("/public/dags/test_dag1/dagRuns", params=query_params)
        assert response.status_code == 422
        assert response.json()["detail"] == expected_detail

    @pytest.mark.parametrize(
        "dag_id, query_params, expected_dag_id_list",
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
                DAG1_ID,
                {
                    "end_date_gte": START_DATE2.isoformat(),
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                [DAG1_RUN1_ID, DAG1_RUN2_ID],
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
                DAG2_ID,
                {
                    "start_date_gte": START_DATE2.isoformat(),
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
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
        ],
    )
    def test_filters(self, test_client, dag_id, query_params, expected_dag_id_list):
        response = test_client.get(f"/public/dags/{dag_id}/dagRuns", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_list

    def test_bad_filters(self, test_client):
        query_params = {
            "logical_date_gte": "invalid",
            "start_date_gte": "invalid",
            "end_date_gte": "invalid",
            "logical_date_lte": "invalid",
            "start_date_lte": "invalid",
            "end_date_lte": "invalid",
        }
        expected_detail = [
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
        response = test_client.get(f"/public/dags/{DAG1_ID}/dagRuns", params=query_params)
        assert response.status_code == 422
        body = response.json()
        assert body["detail"] == expected_detail

    def test_invalid_state(self, test_client):
        response = test_client.get(f"/public/dags/{DAG1_ID}/dagRuns", params={"state": ["invalid"]})
        assert response.status_code == 422
        assert (
            response.json()["detail"] == f"Invalid value for state. Valid values are {', '.join(DagRunState)}"
        )


class TestListDagRunsBatch:
    @staticmethod
    def get_dag_run_dict(run: DagRun):
        return {
            "dag_run_id": run.run_id,
            "dag_id": run.dag_id,
            "logical_date": from_datetime_to_zulu_without_ms(run.logical_date),
            "queued_at": from_datetime_to_zulu_without_ms(run.queued_at) if run.queued_at else None,
            "start_date": from_datetime_to_zulu_without_ms(run.start_date),
            "end_date": from_datetime_to_zulu(run.end_date),
            "data_interval_start": from_datetime_to_zulu_without_ms(run.data_interval_start),
            "data_interval_end": from_datetime_to_zulu_without_ms(run.data_interval_end),
            "last_scheduling_decision": from_datetime_to_zulu_without_ms(run.last_scheduling_decision)
            if run.last_scheduling_decision
            else None,
            "run_type": run.run_type,
            "state": run.state,
            "external_trigger": run.external_trigger,
            "triggered_by": run.triggered_by.value,
            "conf": run.conf,
            "note": run.note,
        }

    def test_list_dag_runs_return_200(self, test_client, session):
        response = test_client.post("/public/dags/~/dagRuns/list", json={})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        for each in body["dag_runs"]:
            run = session.query(DagRun).where(DagRun.run_id == each["dag_run_id"]).one()
            expected = self.get_dag_run_dict(run)
            assert each == expected

    def test_list_dag_runs_with_invalid_dag_id(self, test_client):
        response = test_client.post("/public/dags/invalid/dagRuns/list", json={})
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
        "dag_ids, status_code, expected_dag_id_list",
        [
            ([], 200, DAG_RUNS_LIST),
            ([DAG1_ID], 200, [DAG1_RUN1_ID, DAG1_RUN2_ID]),
            [["invalid"], 200, []],
        ],
    )
    def test_list_dag_runs_with_dag_ids_filter(self, test_client, dag_ids, status_code, expected_dag_id_list):
        response = test_client.post("/public/dags/~/dagRuns/list", json={"dag_ids": dag_ids})
        assert response.status_code == status_code
        assert set([each["dag_run_id"] for each in response.json()["dag_runs"]]) == set(expected_dag_id_list)

    def test_invalid_order_by_raises_400(self, test_client):
        response = test_client.post("/public/dags/~/dagRuns/list", json={"order_by": "invalid"})
        assert response.status_code == 400
        body = response.json()
        assert (
            body["detail"]
            == "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        )

    @pytest.mark.parametrize(
        "order_by,expected_order",
        [
            pytest.param("id", DAG_RUNS_LIST, id="order_by_id"),
            pytest.param(
                "state", [DAG1_RUN2_ID, DAG1_RUN1_ID, DAG2_RUN1_ID, DAG2_RUN2_ID], id="order_by_state"
            ),
            pytest.param("dag_id", DAG_RUNS_LIST, id="order_by_dag_id"),
            pytest.param("logical_date", DAG_RUNS_LIST, id="order_by_logical_date"),
            pytest.param("dag_run_id", DAG_RUNS_LIST, id="order_by_dag_run_id"),
            pytest.param("start_date", DAG_RUNS_LIST, id="order_by_start_date"),
            pytest.param("end_date", DAG_RUNS_LIST, id="order_by_end_date"),
            pytest.param("updated_at", DAG_RUNS_LIST, id="order_by_updated_at"),
            pytest.param("external_trigger", DAG_RUNS_LIST, id="order_by_external_trigger"),
            pytest.param("conf", DAG_RUNS_LIST, id="order_by_conf"),
        ],
    )
    def test_dag_runs_ordering(self, test_client, order_by, expected_order):
        # Test ascending order
        response = test_client.post("/public/dags/~/dagRuns/list", json={"order_by": order_by})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [run["dag_run_id"] for run in body["dag_runs"]] == expected_order

        # Test descending order
        response = test_client.post("/public/dags/~/dagRuns/list", json={"order_by": f"-{order_by}"})
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [run["dag_run_id"] for run in body["dag_runs"]] == expected_order[::-1]

    @pytest.mark.parametrize(
        "post_body, expected_dag_id_order",
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
    def test_limit_and_offset(self, test_client, post_body, expected_dag_id_order):
        response = test_client.post("/public/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 4
        assert [each["dag_run_id"] for each in body["dag_runs"]] == expected_dag_id_order

    @pytest.mark.parametrize(
        "post_body, expected_detail",
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
        response = test_client.post("/public/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 422
        assert response.json()["detail"] == expected_detail

    @pytest.mark.parametrize(
        "post_body, expected_dag_id_list",
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
                    "end_date_gte": START_DATE2.isoformat(),
                    "end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                DAG_RUNS_LIST,
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
    def test_filters(self, test_client, post_body, expected_dag_id_list):
        response = test_client.post("/public/dags/~/dagRuns/list", json=post_body)
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
        response = test_client.post("/public/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 422
        body = response.json()
        assert body["detail"] == expected_detail

    @pytest.mark.parametrize(
        "post_body, expected_response",
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
        response = test_client.post("/public/dags/~/dagRuns/list", json=post_body)
        assert response.status_code == 422
        assert response.json()["detail"] == expected_response


class TestPatchDagRun:
    @pytest.mark.parametrize(
        "dag_id, run_id, patch_body, response_body",
        [
            (
                DAG1_ID,
                DAG1_RUN1_ID,
                {"state": DagRunState.FAILED, "note": "new_note2"},
                {"state": DagRunState.FAILED, "note": "new_note2"},
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                {"state": DagRunState.SUCCESS},
                {"state": DagRunState.SUCCESS, "note": None},
            ),
            (
                DAG2_ID,
                DAG2_RUN1_ID,
                {"state": DagRunState.QUEUED},
                {"state": DagRunState.QUEUED, "note": None},
            ),
            (
                DAG1_ID,
                DAG1_RUN1_ID,
                {"note": "updated note"},
                {"state": DagRunState.SUCCESS, "note": "updated note"},
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                {"note": "new note", "state": DagRunState.FAILED},
                {"state": DagRunState.FAILED, "note": "new note"},
            ),
            (
                DAG1_ID,
                DAG1_RUN2_ID,
                {"note": None},
                {"state": DagRunState.FAILED, "note": None},
            ),
        ],
    )
    def test_patch_dag_run(self, test_client, dag_id, run_id, patch_body, response_body):
        response = test_client.patch(f"/public/dags/{dag_id}/dagRuns/{run_id}", json=patch_body)
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == dag_id
        assert body["dag_run_id"] == run_id
        assert body.get("state") == response_body.get("state")
        assert body.get("note") == response_body.get("note")

    @pytest.mark.parametrize(
        "query_params, patch_body, response_body, expected_status_code",
        [
            (
                {"update_mask": ["state"]},
                {"state": DagRunState.SUCCESS},
                {"state": "success"},
                200,
            ),
            (
                {"update_mask": ["note"]},
                {"state": DagRunState.FAILED, "note": "new_note1"},
                {"note": "new_note1", "state": "success"},
                200,
            ),
            (
                {},
                {"state": DagRunState.FAILED, "note": "new_note2"},
                {"note": "new_note2", "state": "failed"},
                200,
            ),
            ({"update_mask": ["note"]}, {}, {"state": "success", "note": None}, 200),
            (
                {"update_mask": ["random"]},
                {"state": DagRunState.FAILED},
                {"state": "success", "note": "test_note"},
                200,
            ),
        ],
    )
    def test_patch_dag_run_with_update_mask(
        self, test_client, query_params, patch_body, response_body, expected_status_code
    ):
        response = test_client.patch(
            f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}",
            params=query_params,
            json=patch_body,
        )
        response_json = response.json()
        assert response.status_code == expected_status_code
        for key, value in response_body.items():
            assert response_json.get(key) == value

    def test_patch_dag_run_not_found(self, test_client):
        response = test_client.patch(
            f"/public/dags/{DAG1_ID}/dagRuns/invalid",
            json={"state": DagRunState.SUCCESS},
        )
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"

    def test_patch_dag_run_bad_request(self, test_client):
        response = test_client.patch(
            f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}", json={"state": "running"}
        )
        assert response.status_code == 422
        body = response.json()
        assert body["detail"][0]["msg"] == "Input should be 'queued', 'success' or 'failed'"


class TestDeleteDagRun:
    def test_delete_dag_run(self, test_client):
        response = test_client.delete(f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}")
        assert response.status_code == 204

    def test_delete_dag_run_not_found(self, test_client):
        response = test_client.delete(f"/public/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"


class TestGetDagRunAssetTriggerEvents:
    def test_should_respond_200(self, test_client, dag_maker, session):
        asset1 = Asset(uri="ds1")

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

        response = test_client.get(
            "/public/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamAssetEvents",
        )
        assert response.status_code == 200
        expected_response = {
            "asset_events": [
                {
                    "timestamp": from_datetime_to_zulu(event.timestamp),
                    "asset_id": asset1_id,
                    "uri": asset1.uri,
                    "extra": {},
                    "id": event.id,
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
                }
            ],
            "total_entries": 1,
        }
        assert response.json() == expected_response

    def test_should_respond_404(self, test_client):
        response = test_client.get(
            "public/dags/invalid-id/dagRuns/invalid-run-id/upstreamAssetEvents",
        )
        assert response.status_code == 404
        assert (
            "The DagRun with dag_id: `invalid-id` and run_id: `invalid-run-id` was not found"
            == response.json()["detail"]
        )


class TestClearDagRun:
    def test_clear_dag_run(self, test_client):
        response = test_client.post(
            f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear",
            json={"dry_run": False},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == DAG1_ID
        assert body["dag_run_id"] == DAG1_RUN1_ID
        assert body["state"] == "queued"

    @pytest.mark.parametrize(
        "body",
        [{"dry_run": True}, {}],
    )
    def test_clear_dag_run_dry_run(self, test_client, session, body):
        response = test_client.post(f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear", json=body)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 1
        for each in body["task_instances"]:
            assert each["state"] == "success"
        dag_run = session.scalar(select(DagRun).filter_by(dag_id=DAG1_ID, run_id=DAG1_RUN1_ID))
        assert dag_run.state == DAG1_RUN1_STATE

    def test_clear_dag_run_not_found(self, test_client):
        response = test_client.post(f"/public/dags/{DAG1_ID}/dagRuns/invalid/clear", json={"dry_run": False})
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"

    def test_clear_dag_run_unprocessable_entity(self, test_client):
        response = test_client.post(f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear")
        assert response.status_code == 422
        body = response.json()
        assert body["detail"][0]["msg"] == "Field required"
        assert body["detail"][0]["loc"][0] == "body"


class TestTriggerDagRun:
    def _dags_for_trigger_tests(self, session=None):
        inactive_dag = DagModel(
            dag_id="inactive",
            fileloc="/tmp/dag_del_1.py",
            timetable_summary="2 2 * * *",
            is_active=False,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        import_errors_dag = DagModel(
            dag_id="import_errors",
            fileloc="/tmp/dag_del_2.py",
            timetable_summary="2 2 * * *",
            is_active=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        import_errors_dag.has_import_errors = True

        session.add(inactive_dag)
        session.add(import_errors_dag)
        session.commit()

    @time_machine.travel(timezone.utcnow(), tick=False)
    @pytest.mark.parametrize(
        "dag_run_id, note, data_interval_start, data_interval_end",
        [
            ("dag_run_5", "test-note", None, None),
            (
                "dag_run_6",
                "test-note",
                "2024-01-03T00:00:00+00:00",
                "2024-01-04T05:00:00+00:00",
            ),
            (None, None, None, None),
        ],
    )
    def test_should_respond_200(
        self,
        test_client,
        dag_run_id,
        note,
        data_interval_start,
        data_interval_end,
    ):
        fixed_now = timezone.utcnow().isoformat()

        request_json = {"note": note}
        if dag_run_id is not None:
            request_json["dag_run_id"] = dag_run_id
        if data_interval_start is not None:
            request_json["data_interval_start"] = data_interval_start
        if data_interval_end is not None:
            request_json["data_interval_end"] = data_interval_end

        response = test_client.post(
            f"/public/dags/{DAG1_ID}/dagRuns",
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

        expected_response_json = {
            "conf": {},
            "dag_id": DAG1_ID,
            "dag_run_id": expected_dag_run_id,
            "end_date": None,
            "logical_date": fixed_now.replace("+00:00", "Z"),
            "external_trigger": True,
            "start_date": None,
            "state": "queued",
            "data_interval_end": expected_data_interval_end,
            "data_interval_start": expected_data_interval_start,
            "queued_at": fixed_now.replace("+00:00", "Z"),
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": note,
            "triggered_by": "rest_api",
        }

        assert response.json() == expected_response_json

    @pytest.mark.parametrize(
        "post_body, expected_detail",
        [
            # Uncomment these 2 test cases once https://github.com/apache/airflow/pull/44306 is merged
            # (
            #     {"executiondate": "2020-11-10T08:25:56Z"},
            #     {
            #         "detail": [
            #             {
            #                 "input": "2020-11-10T08:25:56Z",
            #                 "loc": ["body", "executiondate"],
            #                 "msg": "Extra inputs are not permitted",
            #                 "type": "extra_forbidden",
            #             }
            #         ]
            #     },
            # ),
            # (
            #     {"logical_date": "2020-11-10T08:25:56"},
            #     {
            #         "detail": [
            #             {
            #                 "input": "2020-11-10T08:25:56",
            #                 "loc": ["body", "logical_date"],
            #                 "msg": "Extra inputs are not permitted",
            #                 "type": "extra_forbidden",
            #             }
            #         ]
            #     },
            # ),
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
        response = test_client.post(f"/public/dags/{DAG1_ID}/dagRuns", json=post_body)
        assert response.status_code == 422
        assert response.json() == expected_detail

    @mock.patch("airflow.models.DAG.create_dagrun")
    def test_dagrun_creation_exception_is_handled(self, mock_create_dagrun, test_client):
        error_message = "Encountered Error"

        mock_create_dagrun.side_effect = ValueError(error_message)

        response = test_client.post(f"/public/dags/{DAG1_ID}/dagRuns", json={})
        assert response.status_code == 400
        assert response.json() == {"detail": error_message}

    def test_should_respond_404_if_a_dag_is_inactive(self, test_client, session):
        self._dags_for_trigger_tests(session)
        response = test_client.post("/public/dags/inactive/dagRuns", json={})
        assert response.status_code == 404
        assert response.json()["detail"] == "DAG with dag_id: 'inactive' not found"

    def test_should_respond_400_if_a_dag_has_import_errors(self, test_client, session):
        self._dags_for_trigger_tests(session)
        response = test_client.post("/public/dags/import_errors/dagRuns", json={})
        assert response.status_code == 400
        assert (
            response.json()["detail"]
            == "DAG with dag_id: 'import_errors' has import errors and cannot be triggered"
        )

    @time_machine.travel(timezone.utcnow(), tick=False)
    def test_should_response_200_for_duplicate_logical_date(self, test_client):
        RUN_ID_1 = "random_1"
        RUN_ID_2 = "random_2"
        now = timezone.utcnow().isoformat().replace("+00:00", "Z")
        note = "duplicate logical date test"
        response_1 = test_client.post(
            f"/public/dags/{DAG1_ID}/dagRuns",
            json={"dag_run_id": RUN_ID_1, "note": note},
        )
        response_2 = test_client.post(
            f"/public/dags/{DAG1_ID}/dagRuns",
            json={"dag_run_id": RUN_ID_2, "note": note},
        )

        assert response_1.status_code == response_2.status_code == 200
        body1 = response_1.json()
        body2 = response_2.json()

        for each_run_id, each_body in [(RUN_ID_1, body1), (RUN_ID_2, body2)]:
            assert each_body == {
                "dag_run_id": each_run_id,
                "dag_id": DAG1_ID,
                "logical_date": now,
                "queued_at": now,
                "start_date": None,
                "end_date": None,
                "data_interval_start": now,
                "data_interval_end": now,
                "last_scheduling_decision": None,
                "run_type": "manual",
                "state": "queued",
                "external_trigger": True,
                "triggered_by": "rest_api",
                "conf": {},
                "note": note,
            }

    @pytest.mark.parametrize(
        "data_interval_start, data_interval_end",
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
        response = test_client.post(
            f"/public/dags/{DAG1_ID}/dagRuns",
            json={"data_interval_start": data_interval_start, "data_interval_end": data_interval_end},
        )
        assert response.status_code == 422
        assert (
            response.json()["detail"][0]["msg"]
            == "Value error, Either both data_interval_start and data_interval_end must be provided or both must be None"
        )

    def test_raises_validation_error_for_invalid_params(self, test_client):
        response = test_client.post(
            f"/public/dags/{DAG2_ID}/dagRuns",
            json={"conf": {"validated_number": 5000}},
        )
        assert response.status_code == 400
        assert "Invalid input for param validated_number" in response.json()["detail"]

    def test_response_404(self, test_client):
        response = test_client.post("/public/dags/randoms/dagRuns", json={})
        assert response.status_code == 404
        assert response.json()["detail"] == "DAG with dag_id: 'randoms' not found"

    def test_response_409(self, test_client):
        response = test_client.post(f"/public/dags/{DAG1_ID}/dagRuns", json={"dag_run_id": DAG1_RUN1_ID})
        assert response.status_code == 409
        assert response.json()["detail"] == "Unique constraint violation"

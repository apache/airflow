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

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_DISPLAY_NAME = "display1"
DAG2_ID = "test_dag2"
DAG2_DISPLAY_NAME = "display2"
DAG3_ID = "test_dag3"
TASK_ID = "op1"


@provide_session
def _create_deactivated_paused_dag(session=None):
    dag_model = DagModel(
        dag_id=DAG3_ID,
        fileloc="/tmp/dag_del_1.py",
        timetable_summary="2 2 * * *",
        is_active=False,
        is_paused=True,
        owners="test_owner,another_test_owner",
        next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    )

    dagrun_failed = DagRun(
        dag_id=DAG3_ID,
        run_id="run1",
        execution_date=datetime(2018, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        start_date=datetime(2018, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.FAILED,
    )

    dagrun_success = DagRun(
        dag_id=DAG3_ID,
        run_id="run2",
        execution_date=datetime(2019, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        start_date=datetime(2019, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        run_type=DagRunType.MANUAL,
        state=DagRunState.SUCCESS,
    )

    session.add(dag_model)
    session.add(dagrun_failed)
    session.add(dagrun_success)


@pytest.fixture(autouse=True)
def setup(dag_maker) -> None:
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    with dag_maker(
        DAG1_ID,
        dag_display_name=DAG1_DISPLAY_NAME,
        schedule=None,
        start_date=datetime(2018, 6, 15, 0, 0, tzinfo=timezone.utc),
        doc_md="details",
        params={"foo": 1},
        tags=["example"],
    ):
        EmptyOperator(task_id=TASK_ID)

    dag_maker.create_dagrun(state=DagRunState.FAILED)

    with dag_maker(
        DAG2_ID,
        dag_display_name=DAG2_DISPLAY_NAME,
        schedule=None,
        start_date=datetime(
            2021,
            6,
            15,
        ),
    ):
        EmptyOperator(task_id=TASK_ID)

    dag_maker.dagbag.sync_to_db()
    _create_deactivated_paused_dag()


@pytest.mark.parametrize(
    "query_params, expected_total_entries, expected_ids",
    [
        # Filters
        ({}, 2, [DAG1_ID, DAG2_ID]),
        ({"limit": 1}, 2, [DAG1_ID]),
        ({"offset": 1}, 2, [DAG2_ID]),
        ({"tags": ["example"]}, 1, [DAG1_ID]),
        ({"only_active": False}, 3, [DAG1_ID, DAG2_ID, DAG3_ID]),
        ({"paused": True, "only_active": False}, 1, [DAG3_ID]),
        ({"paused": False}, 2, [DAG1_ID, DAG2_ID]),
        ({"owners": ["airflow"]}, 2, [DAG1_ID, DAG2_ID]),
        ({"owners": ["test_owner"], "only_active": False}, 1, [DAG3_ID]),
        ({"last_dag_run_state": "success", "only_active": False}, 1, [DAG3_ID]),
        ({"last_dag_run_state": "failed", "only_active": False}, 1, [DAG1_ID]),
        # # Sort
        ({"order_by": "-dag_id"}, 2, [DAG2_ID, DAG1_ID]),
        ({"order_by": "-dag_display_name"}, 2, [DAG2_ID, DAG1_ID]),
        ({"order_by": "dag_display_name"}, 2, [DAG1_ID, DAG2_ID]),
        ({"order_by": "next_dagrun", "only_active": False}, 3, [DAG3_ID, DAG1_ID, DAG2_ID]),
        ({"order_by": "last_run_state", "only_active": False}, 3, [DAG1_ID, DAG3_ID, DAG2_ID]),
        ({"order_by": "-last_run_state", "only_active": False}, 3, [DAG3_ID, DAG1_ID, DAG2_ID]),
        (
            {"order_by": "last_run_start_date", "only_active": False},
            3,
            [DAG1_ID, DAG3_ID, DAG2_ID],
        ),
        (
            {"order_by": "-last_run_start_date", "only_active": False},
            3,
            [DAG3_ID, DAG1_ID, DAG2_ID],
        ),
        # Search
        ({"dag_id_pattern": "1"}, 1, [DAG1_ID]),
        ({"dag_display_name_pattern": "display2"}, 1, [DAG2_ID]),
    ],
)
def test_get_dags(test_client, query_params, expected_total_entries, expected_ids):
    response = test_client.get("/public/dags", params=query_params)

    assert response.status_code == 200
    body = response.json()

    assert body["total_entries"] == expected_total_entries
    assert [dag["dag_id"] for dag in body["dags"]] == expected_ids


@pytest.mark.parametrize(
    "query_params, dag_id, body, expected_status_code, expected_is_paused",
    [
        ({}, "fake_dag_id", {"is_paused": True}, 404, None),
        ({"update_mask": ["field_1", "is_paused"]}, DAG1_ID, {"is_paused": True}, 400, None),
        ({}, DAG1_ID, {"is_paused": True}, 200, True),
        ({}, DAG1_ID, {"is_paused": False}, 200, False),
        ({"update_mask": ["is_paused"]}, DAG1_ID, {"is_paused": True}, 200, True),
        ({"update_mask": ["is_paused"]}, DAG1_ID, {"is_paused": False}, 200, False),
    ],
)
def test_patch_dag(test_client, query_params, dag_id, body, expected_status_code, expected_is_paused):
    response = test_client.patch(f"/public/dags/{dag_id}", json=body, params=query_params)

    assert response.status_code == expected_status_code
    if expected_status_code == 200:
        body = response.json()
        assert body["is_paused"] == expected_is_paused


@pytest.mark.parametrize(
    "query_params, body, expected_status_code, expected_ids, expected_paused_ids",
    [
        ({"update_mask": ["field_1", "is_paused"]}, {"is_paused": True}, 400, None, None),
        (
            {"only_active": False},
            {"is_paused": True},
            200,
            [],
            [],
        ),  # no-op because the dag_id_pattern is not provided
        (
            {"only_active": False, "dag_id_pattern": "~"},
            {"is_paused": True},
            200,
            [DAG1_ID, DAG2_ID, DAG3_ID],
            [DAG1_ID, DAG2_ID, DAG3_ID],
        ),
        (
            {"only_active": False, "dag_id_pattern": "~"},
            {"is_paused": False},
            200,
            [DAG1_ID, DAG2_ID, DAG3_ID],
            [],
        ),
        (
            {"dag_id_pattern": "~"},
            {"is_paused": True},
            200,
            [DAG1_ID, DAG2_ID],
            [DAG1_ID, DAG2_ID],
        ),
        (
            {"dag_id_pattern": "dag1"},
            {"is_paused": True},
            200,
            [DAG1_ID],
            [DAG1_ID],
        ),
    ],
)
def test_patch_dags(test_client, query_params, body, expected_status_code, expected_ids, expected_paused_ids):
    response = test_client.patch("/public/dags", json=body, params=query_params)

    assert response.status_code == expected_status_code
    if expected_status_code == 200:
        body = response.json()
        assert [dag["dag_id"] for dag in body["dags"]] == expected_ids
        paused_dag_ids = [dag["dag_id"] for dag in body["dags"] if dag["is_paused"]]
        assert paused_dag_ids == expected_paused_ids

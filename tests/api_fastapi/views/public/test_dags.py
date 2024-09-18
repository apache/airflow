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

from airflow.models.dag import DAG, DagModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
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
    session.add(dag_model)


@pytest.fixture(autouse=True)
def setup() -> None:
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    with DAG(
        DAG1_ID,
        dag_display_name=DAG1_DISPLAY_NAME,
        schedule=None,
        start_date=datetime(2020, 6, 15),
        doc_md="details",
        params={"foo": 1},
        tags=["example"],
    ) as dag1:
        EmptyOperator(task_id=TASK_ID)

    with DAG(
        DAG2_ID,
        dag_display_name=DAG2_DISPLAY_NAME,
        schedule=None,
        start_date=datetime(
            2020,
            6,
            15,
        ),
    ) as dag2:
        EmptyOperator(task_id=TASK_ID)

    dag1.sync_to_db()
    dag2.sync_to_db()

    _create_deactivated_paused_dag()


@pytest.mark.parametrize(
    "query_params, expected_total_entries, expected_ids",
    [
        # Filters
        ({}, 2, ["test_dag1", "test_dag2"]),
        ({"limit": 1}, 2, ["test_dag1"]),
        ({"offset": 1}, 2, ["test_dag2"]),
        ({"tags": ["example"]}, 1, ["test_dag1"]),
        ({"only_active": False}, 3, ["test_dag1", "test_dag2", "test_dag3"]),
        ({"paused": True, "only_active": False}, 1, ["test_dag3"]),
        ({"paused": False}, 2, ["test_dag1", "test_dag2"]),
        ({"owners": ["airflow"]}, 2, ["test_dag1", "test_dag2"]),
        ({"owners": ["test_owner"], "only_active": False}, 1, ["test_dag3"]),
        # # Sort
        ({"order_by": "-dag_id"}, 2, ["test_dag2", "test_dag1"]),
        ({"order_by": "-dag_display_name"}, 2, ["test_dag2", "test_dag1"]),
        ({"order_by": "dag_display_name"}, 2, ["test_dag1", "test_dag2"]),
        ({"order_by": "next_dagrun", "only_active": False}, 3, ["test_dag3", "test_dag1", "test_dag2"]),
        # # Search
        ({"dag_id_pattern": "1"}, 1, ["test_dag1"]),
        ({"dag_display_name_pattern": "display2"}, 1, ["test_dag2"]),
    ],
)
def test_get_dags(test_client, query_params, expected_total_entries, expected_ids):
    response = test_client.get("/public/dags", params=query_params)

    assert response.status_code == 200
    body = response.json()

    assert body["total_entries"] == expected_total_entries
    assert [dag["dag_id"] for dag in body["dags"]] == expected_ids

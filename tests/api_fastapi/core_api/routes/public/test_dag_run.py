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

from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

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
START_DATE = datetime(2024, 6, 15, 0, 0, tzinfo=timezone.utc)
EXECUTION_DATE = datetime(2024, 6, 16, 0, 0, tzinfo=timezone.utc)
DAG1_NOTE = "test_note"


@pytest.fixture(autouse=True)
@provide_session
def setup(dag_maker, session=None):
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()

    with dag_maker(
        DAG1_ID,
        schedule="@daily",
        start_date=START_DATE,
    ):
        EmptyOperator(task_id="task_1")
    dag_run1 = dag_maker.create_dagrun(
        run_id=DAG1_RUN1_ID,
        state=DAG1_RUN1_STATE,
        run_type=DAG1_RUN1_RUN_TYPE,
        triggered_by=DAG1_RUN1_TRIGGERED_BY,
    )
    dag_run1.note = (DAG1_NOTE, 1)

    dag_maker.create_dagrun(
        run_id=DAG1_RUN2_ID,
        state=DAG1_RUN2_STATE,
        run_type=DAG1_RUN2_RUN_TYPE,
        triggered_by=DAG1_RUN2_TRIGGERED_BY,
        execution_date=EXECUTION_DATE,
    )

    with dag_maker(
        DAG2_ID,
        schedule=None,
        start_date=START_DATE,
    ):
        EmptyOperator(task_id="task_2")
    dag_maker.create_dagrun(
        run_id=DAG2_RUN1_ID,
        state=DAG2_RUN1_STATE,
        run_type=DAG2_RUN1_RUN_TYPE,
        triggered_by=DAG2_RUN1_TRIGGERED_BY,
        execution_date=EXECUTION_DATE,
    )
    dag_maker.create_dagrun(
        run_id=DAG2_RUN2_ID,
        state=DAG2_RUN2_STATE,
        run_type=DAG2_RUN2_RUN_TYPE,
        triggered_by=DAG2_RUN2_TRIGGERED_BY,
        execution_date=EXECUTION_DATE,
    )

    dag_maker.dagbag.sync_to_db()
    dag_maker.dag_model
    dag_maker.dag_model.has_task_concurrency_limits = True
    session.merge(dag_maker.dag_model)
    session.commit()


class TestGetDagRun:
    @pytest.mark.parametrize(
        "dag_id, run_id, state, run_type, triggered_by, dag_run_note",
        [
            (DAG1_ID, DAG1_RUN1_ID, DAG1_RUN1_STATE, DAG1_RUN1_RUN_TYPE, DAG1_RUN1_TRIGGERED_BY, DAG1_NOTE),
            (DAG1_ID, DAG1_RUN2_ID, DAG1_RUN2_STATE, DAG1_RUN2_RUN_TYPE, DAG1_RUN2_TRIGGERED_BY, None),
            (DAG2_ID, DAG2_RUN1_ID, DAG2_RUN1_STATE, DAG2_RUN1_RUN_TYPE, DAG2_RUN1_TRIGGERED_BY, None),
            (DAG2_ID, DAG2_RUN2_ID, DAG2_RUN2_STATE, DAG2_RUN2_RUN_TYPE, DAG2_RUN2_TRIGGERED_BY, None),
        ],
    )
    def test_get_dag_run(self, test_client, dag_id, run_id, state, run_type, triggered_by, dag_run_note):
        response = test_client.get(f"/public/dags/{dag_id}/dagRuns/{run_id}")
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == dag_id
        assert body["run_id"] == run_id
        assert body["state"] == state
        assert body["run_type"] == run_type
        assert body["triggered_by"] == triggered_by.value
        assert body["note"] == dag_run_note

    def test_get_dag_run_not_found(self, test_client):
        response = test_client.get(f"/public/dags/{DAG1_ID}/dagRuns/invalid")
        assert response.status_code == 404
        body = response.json()
        assert body["detail"] == "The DagRun with dag_id: `test_dag1` and run_id: `invalid` was not found"


class TestPatchDagRun:
    @pytest.mark.parametrize(
        "dag_id, run_id, patch_body, response_body",
        [
            (DAG1_ID, DAG1_RUN1_ID, {"state": DagRunState.FAILED}, {"state": DagRunState.FAILED}),
            (DAG1_ID, DAG1_RUN2_ID, {"state": DagRunState.SUCCESS}, {"state": DagRunState.SUCCESS}),
            (DAG2_ID, DAG2_RUN1_ID, {"state": DagRunState.QUEUED}, {"state": DagRunState.QUEUED}),
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
            (DAG1_ID, DAG1_RUN2_ID, {"note": None}, {"state": DagRunState.FAILED, "note": None}),
        ],
    )
    def test_patch_dag_run(self, test_client, dag_id, run_id, patch_body, response_body):
        response = test_client.patch(f"/public/dags/{dag_id}/dagRuns/{run_id}", json=patch_body)
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == dag_id
        assert body["run_id"] == run_id
        assert body.get("state") == response_body.get("state")
        assert body.get("note") == response_body.get("note")

    @pytest.mark.parametrize(
        "query_params,patch_body, expected_status_code",
        [
            ({"update_mask": ["state"]}, {"state": DagRunState.SUCCESS}, 200),
            ({}, {"state": DagRunState.SUCCESS}, 200),
            ({"update_mask": ["random"]}, {"state": DagRunState.SUCCESS}, 400),
        ],
    )
    def test_patch_dag_run_with_update_mask(
        self, test_client, query_params, patch_body, expected_status_code
    ):
        response = test_client.patch(
            f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}", params=query_params, json=patch_body
        )
        assert response.status_code == expected_status_code

    def test_patch_dag_run_not_found(self, test_client):
        response = test_client.patch(
            f"/public/dags/{DAG1_ID}/dagRuns/invalid", json={"state": DagRunState.SUCCESS}
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

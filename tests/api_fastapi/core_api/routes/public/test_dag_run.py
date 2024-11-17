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
from sqlalchemy import select

from airflow import Asset
from airflow.models import DagRun
from airflow.models.asset import AssetEvent, AssetModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State
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
END_DATE = datetime(2024, 6, 15, 0, 0, tzinfo=timezone.utc)
EXECUTION_DATE = datetime(2024, 6, 16, 0, 0, tzinfo=timezone.utc)
DAG1_RUN1_NOTE = "test_note"


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
        task1 = EmptyOperator(task_id="task_1")
    dag_run1 = dag_maker.create_dagrun(
        run_id=DAG1_RUN1_ID,
        state=DAG1_RUN1_STATE,
        run_type=DAG1_RUN1_RUN_TYPE,
        triggered_by=DAG1_RUN1_TRIGGERED_BY,
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
        logical_date=EXECUTION_DATE,
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
        logical_date=EXECUTION_DATE,
    )
    dag_maker.create_dagrun(
        run_id=DAG2_RUN2_ID,
        state=DAG2_RUN2_STATE,
        run_type=DAG2_RUN2_RUN_TYPE,
        triggered_by=DAG2_RUN2_TRIGGERED_BY,
        logical_date=EXECUTION_DATE,
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
        "query_params, patch_body, response_body, expected_status_code",
        [
            ({"update_mask": ["state"]}, {"state": DagRunState.SUCCESS}, {"state": "success"}, 200),
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
            f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}", params=query_params, json=patch_body
        )
        response_json = response.json()
        assert response.status_code == expected_status_code
        for key, value in response_body.items():
            assert response_json.get(key) == value

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


class TestGetDagRunAssetTriggerEvents:
    def test_should_respond_200(self, test_client, dag_maker, session):
        asset1 = Asset(uri="ds1")

        with dag_maker(dag_id="source_dag", start_date=START_DATE, session=session):
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

        with dag_maker(dag_id="TEST_DAG_ID", start_date=START_DATE, session=session):
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
                    "timestamp": event.timestamp.isoformat().replace("+00:00", "Z"),
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
                            "data_interval_end": dr.data_interval_end.isoformat().replace("+00:00", "Z"),
                            "data_interval_start": dr.data_interval_start.isoformat().replace("+00:00", "Z"),
                            "end_date": None,
                            "logical_date": dr.logical_date.isoformat().replace("+00:00", "Z"),
                            "start_date": dr.start_date.isoformat().replace("+00:00", "Z"),
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
            f"/public/dags/{DAG1_ID}/dagRuns/{DAG1_RUN1_ID}/clear", json={"dry_run": False}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["dag_id"] == DAG1_ID
        assert body["run_id"] == DAG1_RUN1_ID
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

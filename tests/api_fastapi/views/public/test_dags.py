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

import pendulum
import pytest

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from dev.tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_DISPLAY_NAME = "display1"
DAG2_ID = "test_dag2"
DAG2_DISPLAY_NAME = "display2"
DAG2_START_DATE = datetime(2021, 6, 15, tzinfo=timezone.utc)
DAG3_ID = "test_dag3"
TASK_ID = "op1"
UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"


@pytest.fixture(autouse=True)
@provide_session
def _create_dags(dag_maker, session=None):
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
        start_date=DAG2_START_DATE,
        doc_md="details",
        params={"foo": 1},
        max_active_tasks=16,
        max_active_runs=16,
    ):
        EmptyOperator(task_id=TASK_ID)

    dag_maker.dagbag.sync_to_db()
    dag_maker.dag_model.has_task_concurrency_limits = True
    session.merge(dag_maker.dag_model)
    session.commit()

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


class TestDagEndpoint:
    @staticmethod
    def clear() -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self.clear()

    def teardown_method(self) -> None:
        self.clear()


class TestGetDags(TestDagEndpoint):
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
    def test_get_dags(self, test_client, query_params, expected_total_entries, expected_ids):
        response = test_client.get("/public/dags", params=query_params)

        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == expected_total_entries
        assert [dag["dag_id"] for dag in body["dags"]] == expected_ids


class TestPatchDag(TestDagEndpoint):
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
    def test_patch_dag(
        self, test_client, query_params, dag_id, body, expected_status_code, expected_is_paused
    ):
        response = test_client.patch(f"/public/dags/{dag_id}", json=body, params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code == 200:
            body = response.json()
            assert body["is_paused"] == expected_is_paused


class TestPatchDags(TestDagEndpoint):
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
    def test_patch_dags(
        self, test_client, query_params, body, expected_status_code, expected_ids, expected_paused_ids
    ):
        response = test_client.patch("/public/dags", json=body, params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code == 200:
            body = response.json()
            assert [dag["dag_id"] for dag in body["dags"]] == expected_ids
            paused_dag_ids = [dag["dag_id"] for dag in body["dags"] if dag["is_paused"]]
            assert paused_dag_ids == expected_paused_ids


class TestDagDetails(TestDagEndpoint):
    @pytest.mark.parametrize(
        "query_params, dag_id, expected_status_code, dag_display_name, start_date",
        [
            ({}, "fake_dag_id", 404, "fake_dag", datetime(2023, 12, 31, tzinfo=timezone.utc)),
            ({}, DAG2_ID, 200, DAG2_DISPLAY_NAME, DAG2_START_DATE),
        ],
    )
    def test_dag_details(
        self, test_client, query_params, dag_id, expected_status_code, dag_display_name, start_date
    ):
        response = test_client.get(f"/public/dags/{dag_id}/details", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        # Match expected and actual responses below.
        res_json = response.json()
        last_parsed = res_json["last_parsed"]
        last_parsed_time = res_json["last_parsed_time"]
        file_token = res_json["file_token"]
        expected = {
            "catchup": True,
            "concurrency": 16,
            "dag_id": dag_id,
            "dag_display_name": dag_display_name,
            "dag_run_timeout": None,
            "dataset_expression": None,
            "default_view": "grid",
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "fileloc": "/opt/airflow/tests/api_fastapi/views/public/test_dags.py",
            "file_token": file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_active": True,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": last_parsed_time,
            "last_pickled": None,
            "max_active_runs": 16,
            "max_active_tasks": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun": None,
            "next_dagrun_create_after": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "orientation": "LR",
            "owners": ["airflow"],
            "params": {
                "foo": {
                    "__class": "airflow.models.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "pickle_id": None,
            "render_template_as_native_obj": False,
            "timetable_summary": None,
            "scheduler_lock": None,
            "start_date": start_date.replace(tzinfo=None).isoformat() + "Z",  # pydantic datetime format
            "tags": [],
            "template_search_path": None,
            "timetable_description": "Never, external triggers only",
            "timezone": UTC_JSON_REPR,
        }
        assert res_json == expected


class TestGetDag(TestDagEndpoint):
    @pytest.mark.parametrize(
        "query_params, dag_id, expected_status_code, dag_display_name",
        [
            ({}, "fake_dag_id", 404, "fake_dag"),
            ({}, DAG2_ID, 200, DAG2_DISPLAY_NAME),
        ],
    )
    def test_get_dag(self, test_client, query_params, dag_id, expected_status_code, dag_display_name):
        response = test_client.get(f"/public/dags/{dag_id}", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        # Match expected and actual responses below.
        res_json = response.json()
        last_parsed_time = res_json["last_parsed_time"]
        file_token = res_json["file_token"]
        expected = {
            "dag_id": dag_id,
            "dag_display_name": dag_display_name,
            "description": None,
            "fileloc": "/opt/airflow/tests/api_fastapi/views/public/test_dags.py",
            "file_token": file_token,
            "is_paused": False,
            "is_active": True,
            "owners": ["airflow"],
            "timetable_summary": None,
            "tags": [],
            "next_dagrun": None,
            "has_task_concurrency_limits": True,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_data_interval_end": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_create_after": None,
            "last_expired": None,
            "max_active_tasks": 16,
            "last_pickled": None,
            "default_view": "grid",
            "last_parsed_time": last_parsed_time,
            "scheduler_lock": None,
            "timetable_description": "Never, external triggers only",
            "has_import_errors": False,
            "pickle_id": None,
        }
        assert res_json == expected

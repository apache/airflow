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

from unittest import mock

import pendulum
import pytest

from airflow._shared.timezones import timezone
from airflow.models import DagModel
from airflow.models.backfill import Backfill, BackfillDagRun
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "op1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"


def _clean_db():
    clear_db_backfills()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_dag_bundles()


@pytest.fixture(autouse=True)
def clean_db():
    _clean_db()
    yield
    _clean_db()


class TestBackfillEndpoint:
    @provide_session
    def _create_dag_models(self, *, count=3, dag_id_prefix="TEST_DAG", is_paused=False, session=None):
        dags = []
        for num in range(1, count + 1):
            dag_model = DagModel(
                dag_id=f"{dag_id_prefix}_{num}",
                bundle_name="testing",
                fileloc=f"/tmp/dag_{num}.py",
                is_stale=False,
                timetable_summary="0 0 * * *",
                is_paused=is_paused,
            )
            session.add(dag_model)
            dags.append(dag_model)
        return dags


class TestListBackfills(TestBackfillEndpoint):
    @pytest.mark.parametrize(
        ("test_params", "response_params", "total_entries"),
        [
            ({}, ["backfill1", "backfill2", "backfill3"], 3),
            ({"active": True}, ["backfill2", "backfill3"], 2),
            ({"active": False}, ["backfill1"], 1),
            ({"dag_id": "", "active": True}, ["backfill2", "backfill3"], 2),
            ({"dag_id": "", "active": False}, ["backfill1"], 1),
            ({"dag_id": ""}, ["backfill1", "backfill2", "backfill3"], 3),
            ({"dag_id": "TEST_DAG_1", "active": True}, [], 0),
            ({"dag_id": "TEST_DAG_1", "active": False}, ["backfill1"], 1),
            ({"dag_id": "TEST_DAG_1"}, ["backfill1"], 1),
        ],
    )
    def test_should_response_200(
        self, test_params, response_params, total_entries, test_client, session, testing_dag_bundle
    ):
        dags = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        completed_at = timezone.utcnow()
        backfill0 = Backfill(
            dag_id=dags[0].dag_id, from_date=from_date, to_date=to_date, completed_at=completed_at
        )
        backfill1 = Backfill(dag_id=dags[1].dag_id, from_date=from_date, to_date=to_date)
        backfill2 = Backfill(dag_id=dags[2].dag_id, from_date=from_date, to_date=to_date, is_paused=True)
        backfills = [backfill0, backfill1, backfill2]
        session.add_all(backfills)
        session.commit()
        backfill_responses = {
            "backfill1": {
                "completed_at": from_datetime_to_zulu(completed_at),
                "created_at": mock.ANY,
                "dag_display_name": "TEST_DAG_1",
                "dag_id": "TEST_DAG_1",
                "dag_run_conf": {},
                "from_date": from_datetime_to_zulu(from_date),
                "id": backfills[0].id,
                "is_paused": False,
                "reprocess_behavior": "none",
                "max_active_runs": 10,
                "running_task_instances": 0,
                "to_date": from_datetime_to_zulu(to_date),
                "updated_at": mock.ANY,
            },
            "backfill2": {
                "completed_at": None,
                "created_at": mock.ANY,
                "dag_display_name": "TEST_DAG_2",
                "dag_id": "TEST_DAG_2",
                "dag_run_conf": {},
                "from_date": from_datetime_to_zulu(from_date),
                "id": backfills[1].id,
                "is_paused": False,
                "reprocess_behavior": "none",
                "max_active_runs": 10,
                "running_task_instances": 0,
                "to_date": from_datetime_to_zulu(to_date),
                "updated_at": mock.ANY,
            },
            "backfill3": {
                "completed_at": None,
                "created_at": mock.ANY,
                "dag_display_name": "TEST_DAG_3",
                "dag_id": "TEST_DAG_3",
                "dag_run_conf": {},
                "from_date": from_datetime_to_zulu(from_date),
                "id": backfills[2].id,
                "is_paused": True,
                "reprocess_behavior": "none",
                "max_active_runs": 10,
                "running_task_instances": 0,
                "to_date": from_datetime_to_zulu(to_date),
                "updated_at": mock.ANY,
            },
        }
        expected_response = []
        for backfill in response_params:
            expected_response.append(backfill_responses[backfill])
        with assert_queries_count(3 if test_params.get("dag_id") is None else 4):
            response = test_client.get("/backfills", params=test_params)
        assert response.status_code == 200
        assert response.json() == {
            "backfills": expected_response,
            "total_entries": total_entries,
        }

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/backfills", params={})
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/backfills", params={})
        assert response.status_code == 403

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_should_only_return_authorized_dag_backfills(
        self, mock_get_authorized_dag_ids, test_client, session, testing_dag_bundle
    ):
        dags = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfills = [
            Backfill(dag_id=dags[0].dag_id, from_date=from_date, to_date=to_date),
            Backfill(dag_id=dags[1].dag_id, from_date=from_date, to_date=to_date),
            Backfill(dag_id=dags[2].dag_id, from_date=from_date, to_date=to_date),
        ]
        session.add_all(backfills)
        session.commit()

        mock_get_authorized_dag_ids.return_value = {"TEST_DAG_2", "TEST_DAG_3"}
        response = test_client.get("/backfills")

        mock_get_authorized_dag_ids.assert_called_once_with(user=mock.ANY, method="GET")
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert {b["dag_id"] for b in body["backfills"]} == {"TEST_DAG_2", "TEST_DAG_3"}

    def test_active_filter_includes_backfill_with_running_task_instances(
        self, test_client, dag_maker, session
    ):
        dag_id = "TEST_DAG_RUNNING_TASKS"
        from_date = pendulum.datetime(2024, 1, 1, tz="UTC")
        to_date = pendulum.datetime(2024, 1, 2, tz="UTC")
        with dag_maker(dag_id=dag_id, schedule="@daily", serialized=True, session=session):
            EmptyOperator(task_id=TASK_ID)

        backfill = Backfill(
            dag_id=dag_id,
            from_date=from_date,
            to_date=to_date,
            completed_at=timezone.utcnow(),
        )
        session.add(backfill)
        session.flush()

        older_run = dag_maker.create_dagrun(
            run_id="backfill_old",
            logical_date=from_date,
            run_type=DagRunType.BACKFILL_JOB,
            state=DagRunState.SUCCESS,
        )
        newer_run = dag_maker.create_dagrun(
            run_id="backfill_new",
            logical_date=to_date,
            run_type=DagRunType.BACKFILL_JOB,
            state=DagRunState.SUCCESS,
        )
        older_run.backfill_id = backfill.id
        newer_run.backfill_id = backfill.id
        session.add_all(
            [
                BackfillDagRun(
                    backfill_id=backfill.id,
                    dag_run_id=older_run.id,
                    logical_date=from_date,
                    sort_ordinal=2,
                ),
                BackfillDagRun(
                    backfill_id=backfill.id,
                    dag_run_id=newer_run.id,
                    logical_date=to_date,
                    sort_ordinal=1,
                ),
            ]
        )
        for task_instance in older_run.task_instances:
            task_instance.state = TaskInstanceState.RUNNING
        for task_instance in newer_run.task_instances:
            task_instance.state = TaskInstanceState.SUCCESS
        session.commit()

        active_response = test_client.get("/backfills", params={"dag_id": dag_id, "active": True})
        inactive_response = test_client.get("/backfills", params={"dag_id": dag_id, "active": False})

        assert active_response.status_code == 200
        assert inactive_response.status_code == 200
        assert active_response.json()["total_entries"] == 1
        assert active_response.json()["backfills"][0]["running_task_instances"] == 1
        assert inactive_response.json() == {"backfills": [], "total_entries": 0}

        older_run.task_instances[0].state = TaskInstanceState.SUCCESS
        session.commit()

        response = test_client.get("/backfills", params={"dag_id": dag_id, "active": False})

        assert response.status_code == 200
        assert response.json()["total_entries"] == 1
        assert response.json()["backfills"][0]["running_task_instances"] == 0

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

from airflow.models import DagRun
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from unit.api_fastapi.core_api.routes.public.test_dags import (
    DAG1_ID,
    DAG2_ID,
    DAG3_ID,
    DAG4_ID,
    DAG5_ID,
    TestDagEndpoint as TestPublicDagEndpoint,
)

pytestmark = pytest.mark.db_test


class TestRecentDagRuns(TestPublicDagEndpoint):
    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, session=None) -> None:
        # Create DAG Runs
        for dag_id in [DAG1_ID, DAG2_ID, DAG3_ID, DAG4_ID, DAG5_ID]:
            dag_runs_count = 5 if dag_id in [DAG1_ID, DAG2_ID] else 2
            for i in range(dag_runs_count):
                start_date = datetime(2021 + i, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                dag_run = DagRun(
                    dag_id=dag_id,
                    run_id=f"run_id_{i + 1}",
                    run_type=DagRunType.MANUAL,
                    start_date=start_date,
                    logical_date=start_date,
                    run_after=start_date,
                    state=(DagRunState.FAILED if i % 2 == 0 else DagRunState.SUCCESS),
                    triggered_by=DagRunTriggeredByType.TEST,
                )
                dag_run.end_date = dag_run.start_date + pendulum.duration(hours=1)
                session.add(dag_run)
        session.commit()

    @pytest.mark.parametrize(
        "query_params, expected_ids,expected_total_dag_runs",
        [
            # Filters
            ({}, [DAG1_ID, DAG2_ID], 11),
            ({"limit": 1}, [DAG1_ID], 2),
            ({"offset": 1}, [DAG1_ID, DAG2_ID], 11),
            ({"tags": ["example"]}, [DAG1_ID], 6),
            ({"only_active": False}, [DAG1_ID, DAG2_ID, DAG3_ID], 15),
            ({"paused": True, "only_active": False}, [DAG3_ID], 4),
            ({"paused": False}, [DAG1_ID, DAG2_ID], 11),
            ({"owners": ["airflow"]}, [DAG1_ID, DAG2_ID], 11),
            ({"owners": ["test_owner"], "only_active": False}, [DAG3_ID], 4),
            ({"dag_ids": [DAG1_ID]}, [DAG1_ID], 6),
            ({"dag_ids": [DAG1_ID, DAG2_ID]}, [DAG1_ID, DAG2_ID], 11),
            ({"last_dag_run_state": "success", "only_active": False}, [DAG1_ID, DAG2_ID, DAG3_ID], 6),
            ({"last_dag_run_state": "failed", "only_active": False}, [DAG1_ID, DAG2_ID, DAG3_ID], 9),
            # Search
            ({"dag_id_pattern": "1"}, [DAG1_ID], 6),
            ({"dag_display_name_pattern": "test_dag2"}, [DAG2_ID], 5),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_return_200(self, test_client, query_params, expected_ids, expected_total_dag_runs):
        response = test_client.get("/dags/recent_dag_runs", params=query_params)
        assert response.status_code == 200
        body = response.json()
        required_dag_run_key = [
            "dag_run_id",
            "dag_id",
            "state",
            "run_after",
            "dag_versions",
        ]
        for recent_dag_runs in body["dags"]:
            dag_runs = recent_dag_runs["latest_dag_runs"]
            # check date ordering
            previous_run_after = None
            for dag_run in dag_runs:
                # validate the response
                for key in required_dag_run_key:
                    assert key in dag_run
                if previous_run_after:
                    assert previous_run_after > dag_run["run_after"]
                previous_run_after = dag_run["run_after"]

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/recent_dag_runs", params={})
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/recent_dag_runs", params={})
        assert response.status_code == 403

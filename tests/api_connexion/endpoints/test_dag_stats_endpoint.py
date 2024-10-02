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

from datetime import timedelta

import pytest

from airflow.models.dag import DAG, DagModel
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,
        username="test",
        role_name="admin",
    )

    create_user(app, username="test_no_permissions", role_name=None)

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


class TestDagStatsEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    def _create_dag(self, dag_id):
        dag_instance = DagModel(dag_id=dag_id)
        dag_instance.is_active = True
        with create_session() as session:
            session.add(dag_instance)
        dag = DAG(dag_id=dag_id, schedule=None)
        self.app.dag_bag.bag_dag(dag)
        return dag_instance

    def test_should_respond_200(self, session):
        self._create_dag("dag_stats_dag")
        self._create_dag("dag_stats_dag_2")
        dag_1_run_1 = DagRun(
            dag_id="dag_stats_dag",
            run_id="test_dag_run_id_1",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="running",
        )
        dag_1_run_2 = DagRun(
            dag_id="dag_stats_dag",
            run_id="test_dag_run_id_2",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time) + timedelta(days=1),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="failed",
        )
        dag_2_run_1 = DagRun(
            dag_id="dag_stats_dag_2",
            run_id="test_dag_2_run_id_1",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="queued",
        )
        session.add_all((dag_1_run_1, dag_1_run_2, dag_2_run_1))
        session.commit()
        exp_payload = {
            "dags": [
                {
                    "dag_id": "dag_stats_dag",
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 1,
                        },
                    ],
                },
                {
                    "dag_id": "dag_stats_dag_2",
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 0,
                        },
                    ],
                },
            ],
            "total_entries": 2,
        }

        dag_ids = "dag_stats_dag,dag_stats_dag_2"
        response = self.client.get(
            f"api/v1/dagStats?dag_ids={dag_ids}", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert len(response.json["dags"]) == 2
        assert sorted(response.json["dags"], key=lambda d: d["dag_id"]) == sorted(
            exp_payload["dags"], key=lambda d: d["dag_id"]
        )
        response.json["total_entries"] == 2

    def test_should_raises_401_unauthenticated(self):
        dag_ids = "dag_stats_dag,dag_stats_dag_2"
        response = self.client.get(
            f"api/v1/dagStats?dag_ids={dag_ids}", environ_overrides={"REMOTE_USER": "no_user"}
        )
        assert response.status_code == 401

    def test_should_raises_403_no_permission(self):
        dag_ids = "dag_stats_dag,dag_stats_dag_2"
        response = self.client.get(
            f"api/v1/dagStats?dag_ids={dag_ids}", environ_overrides={"REMOTE_USER": "test_no_permissions"}
        )
        assert response.status_code == 403

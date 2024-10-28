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

import os
from urllib.parse import quote_plus

import pytest

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DataInterval
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.api_connexion_utils import create_user, delete_user
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS, BaseOperatorLink
from tests_common.test_utils.db import clear_db_runs, clear_db_xcom
from tests_common.test_utils.mock_operators import CustomOperator
from tests_common.test_utils.mock_plugins import mock_plugin_manager

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

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


class TestGetExtraLinks:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app, session) -> None:
        self.default_time = timezone.datetime(2020, 1, 1)

        clear_db_runs()
        clear_db_xcom()

        self.app = configured_app

        self.dag = self._create_dag()

        self.app.dag_bag = DagBag(os.devnull, include_examples=False)
        self.app.dag_bag.dags = {self.dag.dag_id: self.dag}
        self.app.dag_bag.sync_to_db()

        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        self.dag.create_dagrun(
            run_id="TEST_DAG_RUN_ID",
            execution_date=self.default_time,
            run_type=DagRunType.MANUAL,
            state=DagRunState.SUCCESS,
            session=session,
            data_interval=DataInterval(
                timezone.datetime(2020, 1, 1), timezone.datetime(2020, 1, 2)
            ),
            **triggered_by_kwargs,
        )
        session.flush()

        self.client = self.app.test_client()  # type:ignore

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_xcom()

    def _create_dag(self):
        with DAG(
            dag_id="TEST_DAG_ID",
            schedule=None,
            default_args={"start_date": self.default_time},
        ) as dag:
            CustomOperator(task_id="TEST_SINGLE_LINK", bash_command="TEST_LINK_VALUE")
            CustomOperator(
                task_id="TEST_MULTIPLE_LINK",
                bash_command=["TEST_LINK_VALUE_1", "TEST_LINK_VALUE_2"],
            )
        return dag

    @pytest.mark.parametrize(
        "url, expected_title, expected_detail",
        [
            pytest.param(
                "/api/v1/dags/INVALID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_LINK/links",
                "DAG not found",
                'DAG with ID = "INVALID" not found',
                id="missing_dag",
            ),
            pytest.param(
                "/api/v1/dags/TEST_DAG_ID/dagRuns/INVALID/taskInstances/TEST_SINGLE_LINK/links",
                "DAG Run not found",
                'DAG Run with ID = "INVALID" not found',
                id="missing_dag_run",
            ),
            pytest.param(
                "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/INVALID/links",
                "Task not found",
                'Task with ID = "INVALID" not found',
                id="missing_task",
            ),
        ],
    )
    def test_should_respond_404(self, url, expected_title, expected_detail):
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})

        assert 404 == response.status_code
        assert {
            "detail": expected_detail,
            "status": 404,
            "title": expected_title,
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_LINK/links",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200(self):
        XCom.set(
            key="search_query",
            value="TEST_LINK_VALUE",
            task_id="TEST_SINGLE_LINK",
            dag_id=self.dag.dag_id,
            run_id="TEST_DAG_RUN_ID",
        )
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_LINK/links",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code, response.data
        assert {
            "Google Custom": "http://google.com/custom_base_link?search=TEST_LINK_VALUE"
        } == response.json

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200_missing_xcom(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_LINK/links",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code, response.data
        assert {"Google Custom": None} == response.json

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200_multiple_links(self):
        XCom.set(
            key="search_query",
            value=["TEST_LINK_VALUE_1", "TEST_LINK_VALUE_2"],
            task_id="TEST_MULTIPLE_LINK",
            dag_id=self.dag.dag_id,
            run_id="TEST_DAG_RUN_ID",
        )
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_MULTIPLE_LINK/links",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code, response.data
        assert {
            "BigQuery Console #1": "https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_1",
            "BigQuery Console #2": "https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_2",
        } == response.json

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200_multiple_links_missing_xcom(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_MULTIPLE_LINK/links",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code, response.data
        assert {"BigQuery Console #1": None, "BigQuery Console #2": None} == response.json

    def test_should_respond_200_support_plugins(self):
        class GoogleLink(BaseOperatorLink):
            name = "Google"

            def get_link(self, operator, dttm):
                return "https://www.google.com"

        class S3LogLink(BaseOperatorLink):
            name = "S3"
            operators = [CustomOperator]

            def get_link(self, operator, dttm):
                return (
                    f"https://s3.amazonaws.com/airflow-logs/{operator.dag_id}/"
                    f"{operator.task_id}/{quote_plus(dttm.isoformat())}"
                )

        class AirflowTestPlugin(AirflowPlugin):
            name = "test_plugin"
            global_operator_extra_links = [
                GoogleLink(),
            ]
            operator_extra_links = [
                S3LogLink(),
            ]

        with mock_plugin_manager(plugins=[AirflowTestPlugin]):
            response = self.client.get(
                "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_LINK/links",
                environ_overrides={"REMOTE_USER": "test"},
            )

            assert 200 == response.status_code, response.data
            assert {
                "Google Custom": None,
                "Google": "https://www.google.com",
                "S3": (
                    "https://s3.amazonaws.com/airflow-logs/"
                    "TEST_DAG_ID/TEST_SINGLE_LINK/2020-01-01T00%3A00%3A00%2B00%3A00"
                ),
            } == response.json

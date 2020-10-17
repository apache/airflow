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
import unittest
from unittest import mock
from urllib.parse import quote_plus

from parameterized import parameterized

from airflow import DAG
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.security import permissions
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs, clear_db_xcom
from tests.test_utils.mock_plugins import mock_plugin_manager


class TestGetExtraLinks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with mock.patch.dict("os.environ", SKIP_DAGS_PARSING="True"), conf_vars(
            {("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}
        ):
            cls.app = app.create_app(testing=True)  # type:ignore
        create_user(
            cls.app,  # type: ignore
            username="test",
            role_name="Test",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            ],
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        delete_user(cls.app, username="test_no_permissions")  # type: ignore

    @provide_session
    def setUp(self, session) -> None:
        self.default_time = datetime(2020, 1, 1)

        clear_db_runs()
        clear_db_xcom()

        self.dag = self._create_dag()
        self.app.dag_bag.dags = {self.dag.dag_id: self.dag}  # type: ignore  # pylint: disable=no-member
        self.app.dag_bag.sync_to_db()  # type: ignore  # pylint: disable=no-member

        dr = DagRun(
            dag_id=self.dag.dag_id,
            run_id="TEST_DAG_RUN_ID",
            execution_date=self.default_time,
            run_type=DagRunType.MANUAL.value,
        )
        session.add(dr)
        session.commit()

        self.client = self.app.test_client()  # type:ignore

    def tearDown(self) -> None:
        super().tearDown()
        clear_db_runs()
        clear_db_xcom()

    @staticmethod
    def _create_dag():
        with DAG(
            dag_id="TEST_DAG_ID",
            default_args=dict(
                start_date=days_ago(2),
            ),
        ) as dag:
            BigQueryExecuteQueryOperator(task_id="TEST_SINGLE_QUERY", sql="SELECT 1")
            BigQueryExecuteQueryOperator(task_id="TEST_MULTIPLE_QUERY", sql=["SELECT 1", "SELECT 2"])
        return dag

    @parameterized.expand(
        [
            (
                "missing_dag",
                "/api/v1/dags/INVALID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links",
                "DAG not found",
                'DAG with ID = "INVALID" not found',
            ),
            (
                "missing_dag_run",
                "/api/v1/dags/TEST_DAG_ID/dagRuns/INVALID/taskInstances/TEST_SINGLE_QUERY/links",
                "DAG Run not found",
                'DAG Run with ID = "INVALID" not found',
            ),
            (
                "missing_task",
                "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/INVALID/links",
                "Task not found",
                'Task with ID = "INVALID" not found',
            ),
        ]
    )
    def test_should_response_404(self, name, url, expected_title, expected_detail):
        del name
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})

        self.assertEqual(404, response.status_code)
        self.assertEqual(
            {
                "detail": expected_detail,
                "status": 404,
                "title": expected_title,
                "type": EXCEPTIONS_LINK_MAP[404],
            },
            response.json,
        )

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403

    @mock_plugin_manager(plugins=[])
    def test_should_response_200(self):
        XCom.set(
            key="job_id",
            value="TEST_JOB_ID",
            execution_date=self.default_time,
            task_id="TEST_SINGLE_QUERY",
            dag_id=self.dag.dag_id,
        )
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links",
            environ_overrides={'REMOTE_USER': "test"},
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {"BigQuery Console": "https://console.cloud.google.com/bigquery?j=TEST_JOB_ID"}, response.json
        )

    @mock_plugin_manager(plugins=[])
    def test_should_response_200_missing_xcom(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links",
            environ_overrides={'REMOTE_USER': "test"},
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {"BigQuery Console": None},
            response.json,
        )

    @mock_plugin_manager(plugins=[])
    def test_should_response_200_multiple_links(self):
        XCom.set(
            key="job_id",
            value=["TEST_JOB_ID_1", "TEST_JOB_ID_2"],
            execution_date=self.default_time,
            task_id="TEST_MULTIPLE_QUERY",
            dag_id=self.dag.dag_id,
        )
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_MULTIPLE_QUERY/links",
            environ_overrides={'REMOTE_USER': "test"},
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {
                "BigQuery Console #1": "https://console.cloud.google.com/bigquery?j=TEST_JOB_ID_1",
                "BigQuery Console #2": "https://console.cloud.google.com/bigquery?j=TEST_JOB_ID_2",
            },
            response.json,
        )

    @mock_plugin_manager(plugins=[])
    def test_should_response_200_multiple_links_missing_xcom(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_MULTIPLE_QUERY/links",
            environ_overrides={'REMOTE_USER': "test"},
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {"BigQuery Console #1": None, "BigQuery Console #2": None},
            response.json,
        )

    def test_should_response_200_support_plugins(self):
        class GoogleLink(BaseOperatorLink):
            name = "Google"

            def get_link(self, operator, dttm):
                return "https://www.google.com"

        class S3LogLink(BaseOperatorLink):
            name = "S3"
            operators = [BigQueryExecuteQueryOperator]

            def get_link(self, operator, dttm):
                return "https://s3.amazonaws.com/airflow-logs/{dag_id}/{task_id}/{execution_date}".format(
                    dag_id=operator.dag_id,
                    task_id=operator.task_id,
                    execution_date=quote_plus(dttm.isoformat()),
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
                "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links",
                environ_overrides={'REMOTE_USER': "test"},
            )

            self.assertEqual(200, response.status_code, response.data)
            self.assertEqual(
                {
                    "BigQuery Console": None,
                    "Google": "https://www.google.com",
                    "S3": (
                        "https://s3.amazonaws.com/airflow-logs/"
                        "TEST_DAG_ID/TEST_SINGLE_QUERY/2020-01-01T00%3A00%3A00%2B00%3A00"
                    ),
                },
                response.json,
            )

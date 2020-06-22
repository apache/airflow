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

from airflow import DAG
from airflow.models import XCom
from airflow.models.dagrun import DagRun
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.session import provide_session
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from airflow.www import app
from tests.test_utils.db import clear_db_runs, clear_db_xcom


class TestGetExtraLinks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore
        cls.dag = cls._create_dag()
        cls.app.dag_bag.dags = {cls.dag.dag_id: cls.dag}  # pylint: disable=no-member
        cls.now = datetime(2020, 1, 1)

    @provide_session
    def setUp(self, session) -> None:
        clear_db_runs()
        clear_db_xcom()
        self.app.dag_bag.sync_to_db()  # pylint: disable=no-member
        dr = DagRun(
            dag_id=self.dag.dag_id,
            run_id="TEST_DAG_RUN_ID",
            execution_date=self.now,
            run_type=DagRunType.MANUAL.value,
        )
        session.add(dr)
        session.commit()

        self.client = self.app.test_client()  # type:ignore

    @staticmethod
    def _create_dag():
        with DAG(dag_id="TEST_DAG_ID", default_args=dict(start_date=days_ago(2),)) as dag:
            BigQueryExecuteQueryOperator(task_id="TEST_SINGLE_QUERY", sql="SELECT 1")
            BigQueryExecuteQueryOperator(task_id="TEST_MULTIPLE_QUERY", sql=["SELECT 1", "SELECT 2"])
        return dag

    def test_should_response_200(self):
        XCom.set(
            key="job_id",
            value="TEST_JOB_ID",
            execution_date=self.now,
            task_id="TEST_SINGLE_QUERY",
            dag_id=self.dag.dag_id,
        )
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links"
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {
                "BigQuery Console": "https://console.cloud.google.com/bigquery?j=TEST_JOB_ID",
                "airflow": "should_be_overridden",
                "github": "https://github.com/apache/airflow",
            },
            response.json,
        )

    def test_should_response_200_missing_xcom(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_QUERY/links"
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {
                "BigQuery Console": None,
                "airflow": "should_be_overridden",
                "github": "https://github.com/apache/airflow",
            },
            response.json,
        )

    def test_should_response_200_multiple_links(self):
        XCom.set(
            key="job_id",
            value=["TEST_JOB_ID_1", "TEST_JOB_ID_2"],
            execution_date=self.now,
            task_id="TEST_MULTIPLE_QUERY",
            dag_id=self.dag.dag_id,
        )
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_MULTIPLE_QUERY/links"
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {
                "BigQuery Console #1": "https://console.cloud.google.com/bigquery?j=TEST_JOB_ID_1",
                "BigQuery Console #2": "https://console.cloud.google.com/bigquery?j=TEST_JOB_ID_2",
                "airflow": "should_be_overridden",
                "github": "https://github.com/apache/airflow",
            },
            response.json,
        )

    def test_should_response_200_multiple_links_missing_xcom(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_MULTIPLE_QUERY/links"
        )

        self.assertEqual(200, response.status_code, response.data)
        self.assertEqual(
            {
                "BigQuery Console #1": None,
                "BigQuery Console #2": None,
                "airflow": "should_be_overridden",
                "github": "https://github.com/apache/airflow",
            },
            response.json,
        )

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

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS, BaseOperatorLink
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_xcom
from tests_common.test_utils.mock_operators import CustomOperator
from tests_common.test_utils.mock_plugins import mock_plugin_manager

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestExtraLinks:
    dag_id = "TEST_DAG_ID"
    dag_run_id = "TEST_DAG_RUN_ID"
    task_single_link = "TEST_SINGLE_LINK"
    task_multiple_links = "TEST_MULTIPLE_LINKS"
    default_time = timezone.datetime(2020, 1, 1)
    plugin_name = "test_plugin"

    @staticmethod
    def _clear_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()

    @provide_session
    @pytest.fixture(autouse=True)
    def setup(self, test_client, session=None) -> None:
        """
        Setup extra links for testing.
        :return: Dictionary with event extra link names with their corresponding link as the links.
        """
        self._clear_db()

        self.dag = self._create_dag()

        dag_bag = DagBag(os.devnull, include_examples=False)
        dag_bag.dags = {self.dag.dag_id: self.dag}
        test_client.app.state.dag_bag = dag_bag
        dag_bag.sync_to_db()

        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST}

        self.dag.create_dagrun(
            run_id=self.dag_run_id,
            logical_date=self.default_time,
            run_type=DagRunType.MANUAL,
            state=DagRunState.SUCCESS,
            data_interval=(timezone.datetime(2020, 1, 1), timezone.datetime(2020, 1, 2)),
            **triggered_by_kwargs,
        )

    def _create_dag(self):
        with DAG(dag_id=self.dag_id, schedule=None, default_args={"start_date": self.default_time}) as dag:
            CustomOperator(task_id=self.task_single_link, bash_command="TEST_LINK_VALUE")
            CustomOperator(
                task_id=self.task_multiple_links, bash_command=["TEST_LINK_VALUE_1", "TEST_LINK_VALUE_2"]
            )
        return dag


class TestGetExtraLinks(TestExtraLinks):
    @pytest.mark.parametrize(
        "url, expected_status_code, expected_response",
        [
            pytest.param(
                "/public/dags/INVALID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_SINGLE_LINK/links",
                404,
                {"detail": "DAG with ID = INVALID not found"},
                id="missing_dag",
            ),
            pytest.param(
                "/public/dags/TEST_DAG_ID/dagRuns/INVALID/taskInstances/TEST_SINGLE_LINK/links",
                404,
                {"detail": "DAG Run with ID = INVALID not found"},
                id="missing_dag_run",
            ),
            pytest.param(
                "/public/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/INVALID/links",
                404,
                {"detail": "Task with ID = INVALID not found"},
                id="missing_task",
            ),
        ],
    )
    def test_should_respond_404(self, test_client, url, expected_status_code, expected_response):
        response = test_client.get(url)

        assert response.status_code == expected_status_code
        assert response.json() == expected_response

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200(self, test_client):
        XCom.set(
            key="search_query",
            value="TEST_LINK_VALUE",
            task_id=self.task_single_link,
            dag_id=self.dag_id,
            run_id=self.dag_run_id,
        )
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_single_link}/links",
        )

        assert response.status_code == 200
        assert response.json() == {
            "Google Custom": "http://google.com/custom_base_link?search=TEST_LINK_VALUE"
        }

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200_missing_xcom(self, test_client):
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_single_link}/links",
        )

        assert response.status_code == 200
        assert response.json() == {"Google Custom": None}

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200_multiple_links(self, test_client):
        XCom.set(
            key="search_query",
            value=["TEST_LINK_VALUE_1", "TEST_LINK_VALUE_2"],
            task_id=self.task_multiple_links,
            dag_id=self.dag.dag_id,
            run_id=self.dag_run_id,
        )
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_multiple_links}/links",
        )

        assert response.status_code == 200
        assert response.json() == {
            "BigQuery Console #1": "https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_1",
            "BigQuery Console #2": "https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_2",
        }

    @mock_plugin_manager(plugins=[])
    def test_should_respond_200_multiple_links_missing_xcom(self, test_client):
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_multiple_links}/links",
        )

        assert response.status_code == 200
        assert response.json() == {"BigQuery Console #1": None, "BigQuery Console #2": None}

    def test_should_respond_200_support_plugins(self, test_client):
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
            response = test_client.get(
                f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_single_link}/links",
            )

            assert response, response.status_code == 200
            assert response.json() == {
                "Google Custom": None,
                "Google": "https://www.google.com",
                "S3": (
                    "https://s3.amazonaws.com/airflow-logs/"
                    "TEST_DAG_ID/TEST_SINGLE_LINK/2020-01-01T00%3A00%3A00%2B00%3A00"
                ),
            }

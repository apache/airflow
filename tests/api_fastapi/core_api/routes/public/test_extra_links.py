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

import pytest

from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.models.dagbag import DagBag
from airflow.models.xcom import XCom
from airflow.plugins_manager import AirflowPlugin
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import BaseOperatorLink
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_xcom
from tests_common.test_utils.mock_operators import CustomOperator
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test


class GoogleLink(BaseOperatorLink):
    name = "Google"

    def get_link(self, operator, ti_key):
        return "https://www.google.com"


class S3LogLink(BaseOperatorLink):
    name = "S3"
    operators = [CustomOperator]

    def get_link(self, operator, ti_key):
        return f"https://s3.amazonaws.com/airflow-logs/{operator.dag_id}/{operator.task_id}/"


class AirflowPluginWithOperatorLinks(AirflowPlugin):
    name = "test_plugin"
    global_operator_extra_links = [
        GoogleLink(),
    ]
    operator_extra_links = [
        S3LogLink(),
    ]


@pytest.mark.mock_plugin_manager(plugins=[])
class TestGetExtraLinks:
    dag_id = "TEST_DAG_ID"
    dag_run_id = "TEST_DAG_RUN_ID"
    task_single_link = "TEST_SINGLE_LINK"
    task_multiple_links = "TEST_MULTIPLE_LINKS"
    task_mapped = "TEST_MAPPED_TASK"
    default_time = timezone.datetime(2020, 1, 1)
    plugin_name = "test_plugin"

    @staticmethod
    def _clear_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()

    @pytest.fixture(autouse=True)
    def setup(self, test_client, dag_maker, request, session) -> None:
        """
        Setup extra links for testing.
        :return: Dictionary with event extra link names with their corresponding link as the links.
        """
        self._clear_db()

        self.dag = self._create_dag(dag_maker)

        DagBundlesManager().sync_bundles_to_db()
        dag_bag = DagBag(os.devnull, include_examples=False)
        dag_bag.dags = {self.dag.dag_id: self.dag}
        test_client.app.state.dag_bag = dag_bag
        dag_bag.sync_to_db("dags-folder", None)

        self.dag.create_dagrun(
            run_id=self.dag_run_id,
            logical_date=self.default_time,
            run_type=DagRunType.MANUAL,
            state=DagRunState.SUCCESS,
            data_interval=(timezone.datetime(2020, 1, 1), timezone.datetime(2020, 1, 2)),
            run_after=timezone.datetime(2020, 1, 2),
            triggered_by=DagRunTriggeredByType.TEST,
        )

    def teardown_method(self) -> None:
        self._clear_db()

    def _create_dag(self, dag_maker):
        with dag_maker(
            dag_id=self.dag_id, schedule=None, default_args={"start_date": self.default_time}, serialized=True
        ) as dag:
            CustomOperator(task_id=self.task_single_link, bash_command="TEST_LINK_VALUE")
            CustomOperator(
                task_id=self.task_multiple_links, bash_command=["TEST_LINK_VALUE_1", "TEST_LINK_VALUE_2"]
            )
        return dag

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

    def test_should_respond_200(self, dag_maker, test_client):
        XCom.set(
            key="search_query",
            value="TEST_LINK_VALUE",
            task_id=self.task_single_link,
            dag_id=self.dag_id,
            run_id=self.dag_run_id,
        )
        XCom.set(
            key="_link_CustomOpLink",
            value="http://google.com/custom_base_link?search=TEST_LINK_VALUE",
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

    def test_should_respond_200_missing_xcom(self, test_client):
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_single_link}/links",
        )

        assert response.status_code == 200
        assert response.json() == {"Google Custom": None}

    def test_should_respond_200_multiple_links(self, test_client, session):
        XCom.set(
            key="search_query",
            value=["TEST_LINK_VALUE_1", "TEST_LINK_VALUE_2"],
            task_id=self.task_multiple_links,
            dag_id=self.dag.dag_id,
            run_id=self.dag_run_id,
            session=session,
        )
        XCom.set(
            key="bigquery_1",
            value="https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_1",
            task_id=self.task_multiple_links,
            dag_id=self.dag_id,
            run_id=self.dag_run_id,
            session=session,
        )
        XCom.set(
            key="bigquery_2",
            value="https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_2",
            task_id=self.task_multiple_links,
            dag_id=self.dag_id,
            run_id=self.dag_run_id,
            session=session,
        )
        session.commit()

        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_multiple_links}/links",
        )

        assert response.status_code == 200
        assert response.json() == {
            "BigQuery Console #1": "https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_1",
            "BigQuery Console #2": "https://console.cloud.google.com/bigquery?j=TEST_LINK_VALUE_2",
        }

    def test_should_respond_200_multiple_links_missing_xcom(self, test_client):
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_multiple_links}/links",
        )

        assert response.status_code == 200
        assert response.json() == {"BigQuery Console #1": None, "BigQuery Console #2": None}

    @pytest.mark.mock_plugin_manager(plugins=[AirflowPluginWithOperatorLinks])
    def test_should_respond_200_support_plugins(self, test_client):
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_single_link}/links",
        )

        assert response, response.status_code == 200
        assert response.json() == {
            "Google Custom": None,
            "Google": "https://www.google.com",
            "S3": ("https://s3.amazonaws.com/airflow-logs/TEST_DAG_ID/TEST_SINGLE_LINK/"),
        }

    @pytest.mark.xfail(reason="TODO: TaskSDK need to fix this, Extra links should work for mapped operator")
    def test_should_respond_200_mapped_task_instance(self, test_client):
        map_index = 0
        XCom.set(
            key="search_query",
            value="TEST_LINK_VALUE_1",
            task_id=self.task_mapped,
            dag_id=self.dag.dag_id,
            run_id=self.dag_run_id,
            map_index=map_index,
        )
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_mapped}/links",
            params={"map_index": map_index},
        )
        assert response.status_code == 200
        assert response.json() == {
            "Google Custom": "http://google.com/custom_base_link?search=TEST_LINK_VALUE_1"
        }

    def test_should_respond_404_invalid_map_index(self, test_client):
        response = test_client.get(
            f"/public/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_mapped}/links",
            params={"map_index": 4},
        )
        assert response.status_code == 404
        assert response.json() == {"detail": "Task with ID = TEST_MAPPED_TASK not found"}

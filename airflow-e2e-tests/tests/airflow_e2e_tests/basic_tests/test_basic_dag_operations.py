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

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient, TaskSDKClient


class TestBasicDagFunctionality:
    """Test basic DAG functionality using the Airflow REST API."""

    airflow_client = AirflowClient()

    def test_dag_unpause(self):
        self.airflow_client.un_pause_dag(
            "example_xcom_test",
        )

    def test_xcom_value(self):
        resp = self.airflow_client.trigger_dag(
            "example_xcom_test", json={"logical_date": datetime.now(timezone.utc).isoformat()}
        )
        self.airflow_client.wait_for_dag_run(
            dag_id="example_xcom_test",
            run_id=resp["dag_run_id"],
        )
        xcom_value_resp = self.airflow_client.get_xcom_value(
            dag_id="example_xcom_test",
            task_id="bash_push",
            key="manually_pushed_value",
            run_id=resp["dag_run_id"],
        )
        assert xcom_value_resp["value"] == "manually_pushed_value", xcom_value_resp


class TestTaskSDKBasicFunctionality:
    """Test basic functionality of Task SDK using the Task SDK REST API."""

    task_sdk_client = TaskSDKClient()

    def test_task_sdk_health_check(self):
        response = self.task_sdk_client.health_check()
        assert response.status_code == 200

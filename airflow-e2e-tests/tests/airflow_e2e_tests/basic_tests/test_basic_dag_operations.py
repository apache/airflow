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

import pendulum
from airflow_client.client import ApiClient, Configuration, DagRunApi, DagRunState, TriggerDAGRunPostBody

from airflow_e2e_tests.constants import DOCKER_COMPOSE_HOST_PORT
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


class TestBasicDagFunctionality:
    """Test basic DAG functionality using the Airflow REST API."""

    airflow_client = AirflowClient()

    def test_xcom_value(self):
        dag_id = "example_xcom_test"
        self.airflow_client.un_pause_dag(dag_id)
        config = Configuration(
            host=f"http://{DOCKER_COMPOSE_HOST_PORT}", access_token=self.airflow_client.token
        )
        with ApiClient(config) as client:
            runs = DagRunApi(client)
            logical_date = pendulum.now("UTC")
            run = runs.trigger_dag_run(dag_id, TriggerDAGRunPostBody(logical_date=logical_date))
            run_id = run.dag_run_id

            def get_state() -> str:
                observed = runs.get_dag_run(dag_id, run_id, _request_timeout=15.0)
                assert observed.logical_date == logical_date
                return observed.state

            state = self.airflow_client.wait_for_dag_run(dag_id, run_id, get_state=get_state)
            assert state == DagRunState.SUCCESS, run_id

        xcom_value_resp = self.airflow_client.get_xcom_value(
            dag_id=dag_id,
            task_id="bash_push",
            key="manually_pushed_value",
            run_id=run_id,
        )
        assert xcom_value_resp["value"] == "manually_pushed_value", xcom_value_resp

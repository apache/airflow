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

import pytest

from kubernetes_tests.test_base import EXECUTOR, BaseK8STest


# These tests are here because only KubernetesExecutor can run the tests in
# test_kubernetes_executor.py
# Also, the skipping is necessary as there's no gain in running these tests in KubernetesExecutor
@pytest.mark.skipif(EXECUTOR == "KubernetesExecutor", reason="Does not run on KubernetesExecutor")
class TestCeleryAndLocalExecutor(BaseK8STest):
    def test_integration_run_dag(self):
        dag_id = "example_simplest_dag"
        dag_run_id, logical_date = self.start_job_in_kubernetes(dag_id, self.host)
        print(f"Found the job with logical_date {logical_date}")

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="my_task",
            expected_final_state="success",
            timeout=300,
        )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=dag_id,
            expected_final_state="success",
            timeout=300,
        )

    def test_integration_run_dag_with_scheduler_failure(self):
        dag_id = "example_xcom"

        dag_run_id, logical_date = self.start_job_in_kubernetes(dag_id, self.host)

        self._delete_airflow_pod("scheduler")

        # Wait for the scheduler to be recreated
        if EXECUTOR == "CeleryExecutor":
            scheduler_resource_type = "deployment"
        elif EXECUTOR == "LocalExecutor":
            scheduler_resource_type = "statefulset"
        else:
            raise ValueError(f"Unknown executor {EXECUTOR}")
        self.ensure_resource_health("airflow-scheduler", resource_type=scheduler_resource_type)

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="push",
            expected_final_state="success",
            timeout=40,  # This should fail fast if failing
        )

        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="puller",
            expected_final_state="success",
            timeout=40,
        )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=dag_id,
            expected_final_state="success",
            timeout=60,
        )

        assert self._num_pods_in_namespace("test-namespace") == 0, "failed to delete pods in other namespace"

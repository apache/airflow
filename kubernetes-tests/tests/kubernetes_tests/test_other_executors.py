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

        # Make sure the first task has already been handed to the executor before
        # we kill the scheduler. Otherwise the scheduler-kill races with the very
        # first scheduling step, and the post-restart scheduler has to re-pick the
        # task itself — making the post-restart monitor timeouts unreliable.
        self.wait_until_task_in_executor(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="push",
            timeout=60,
        )

        self._delete_airflow_pod("scheduler")

        # Wait for the scheduler to be recreated
        if EXECUTOR == "CeleryExecutor":
            scheduler_resource_type = "deployment"
        elif EXECUTOR == "LocalExecutor":
            scheduler_resource_type = "statefulset"
        else:
            raise ValueError(f"Unknown executor {EXECUTOR}")
        self.ensure_resource_health("airflow-scheduler", resource_type=scheduler_resource_type)

        # `push` is already in the executor at this point, but `kubectl rollout
        # status` returns when the new scheduler pod is *running*, not when the
        # scheduler loop has resumed processing. Give the worker / new scheduler
        # enough headroom to drive push → success and then schedule the
        # downstream puller. 40s used to be the "fail fast" budget — in
        # practice that races with scheduler-loop warm-up.
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="push",
            expected_final_state="success",
            timeout=120,
        )

        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="puller",
            expected_final_state="success",
            timeout=120,
        )

        self.ensure_dag_expected_state(
            host=self.host,
            logical_date=logical_date,
            dag_id=dag_id,
            expected_final_state="success",
            timeout=60,
        )

        assert self._num_pods_in_namespace("test-namespace") == 0, "failed to delete pods in other namespace"

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

from typing import TYPE_CHECKING
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from kubernetes.client import CoreV1Api

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import AirflowKubernetesScheduler
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import FailureDetails

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import KubernetesResults
from kubernetes_tests.test_base import EXECUTOR, BaseK8STest


@pytest.fixture(params=[False, True], ids=["coordinates", "uuid"])
def executor_task_key(request, monkeypatch):
    if request.param and not KubernetesExecutor.supports_task_instance_uuid:
        pytest.skip("Requires executor UUID support")
    monkeypatch.setattr(KubernetesExecutor, "supports_task_instance_uuid", request.param)
    if request.param:
        from airflow.executors.workloads.types import TaskInstanceUuid

        return TaskInstanceUuid(uuid4())
    return TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)


@pytest.mark.skipif(EXECUTOR != "KubernetesExecutor", reason="Only runs on KubernetesExecutor")
class TestKubernetesExecutor(BaseK8STest):
    @pytest.mark.execution_timeout(300)
    def test_integration_run_dag(self):
        dag_id = "example_kubernetes_executor"
        dag_run_id, logical_date = self.start_job_in_kubernetes(dag_id, self.host)
        print(f"Found the job with logical_date {logical_date}")

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="start_task",
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

    @pytest.mark.execution_timeout(300)
    def test_integration_run_dag_task_mapping(self):
        dag_id = "example_task_mapping_second_order"
        dag_run_id, logical_date = self.start_job_in_kubernetes(dag_id, self.host)
        print(f"Found the job with logical_date {logical_date}")

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="get_nums",
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

    @pytest.mark.execution_timeout(500)
    def test_integration_run_dag_with_scheduler_failure(self):
        dag_id = "example_kubernetes_executor"

        dag_run_id, logical_date = self.start_job_in_kubernetes(dag_id, self.host)

        self._delete_airflow_pod("scheduler")
        self.ensure_resource_health("airflow-scheduler")

        # Wait some time for the operator to complete
        self.monitor_task(
            host=self.host,
            dag_run_id=dag_run_id,
            dag_id=dag_id,
            task_id="start_task",
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

        assert self._num_pods_in_namespace("test-namespace") == 0, "failed to delete pods in other namespace"

    @pytest.mark.execution_timeout(300)
    @patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log",
        autospec=True,
    )
    def test_pod_failure_logging_with_container_terminated(self, mock_log, executor_task_key):
        """Test that pod failure information is logged when container is terminated."""

        executor = KubernetesExecutor()

        executor.kube_scheduler = Mock(spec=AirflowKubernetesScheduler)

        failure_details: FailureDetails = {
            "pod_status": "Failed",
            "pod_reason": "PodFailed",
            "pod_message": "Pod execution failed",
            "container_state": "terminated",
            "container_reason": "Error",
            "container_message": "Container failed with exit code 1",
            "exit_code": 1,
            "container_type": "main",
            "container_name": "test-container",
        }

        task_key = executor_task_key

        results = KubernetesResults(
            key=task_key,
            state=TaskInstanceState.FAILED,
            pod_name="test-pod",
            namespace="test-namespace",
            resource_version="123",
            failure_details=failure_details,
        )

        executor._change_state(results)

        mock_log.warning.assert_called_once_with(
            "Task %s failed in pod %s/%s. Pod phase: %s, reason: %s, message: %s, "
            "container_type: %s, container_name: %s, container_state: %s, container_reason: %s, "
            "container_message: %s, exit_code: %s",
            str(task_key),
            "test-namespace",
            "test-pod",
            "Failed",
            "PodFailed",
            "Pod execution failed",
            "main",
            "test-container",
            "terminated",
            "Error",
            "Container failed with exit code 1",
            1,
        )

    @pytest.mark.execution_timeout(300)
    @patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log",
        autospec=True,
    )
    def test_pod_failure_logging_exception_handling(self, mock_log, executor_task_key):
        """Test that failures without details are handled gracefully."""

        executor = KubernetesExecutor()

        executor.kube_scheduler = Mock(spec=AirflowKubernetesScheduler)

        task_key = executor_task_key

        results = KubernetesResults(
            key=task_key,
            state=TaskInstanceState.FAILED,
            pod_name="test-pod",
            namespace="test-namespace",
            resource_version="123",
            failure_details=None,
        )

        executor._change_state(results)

        mock_log.warning.assert_called_once_with(
            "Task %s failed in pod %s/%s (no details available)",
            str(task_key),
            "test-namespace",
            "test-pod",
        )

    @pytest.mark.execution_timeout(300)
    @patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log",
        autospec=True,
    )
    def test_pod_failure_logging_non_failed_state(self, mock_log, executor_task_key):
        """Test that pod failure logging only occurs for FAILED state."""

        executor = KubernetesExecutor()

        executor.kube_client = Mock(spec=CoreV1Api)

        executor.kube_scheduler = Mock(spec=AirflowKubernetesScheduler)

        task_key = executor_task_key

        results = KubernetesResults(
            key=task_key,
            state=TaskInstanceState.SUCCESS,
            pod_name="test-pod",
            namespace="test-namespace",
            resource_version="123",
            failure_details=None,
        )

        executor._change_state(results)

        mock_log.error.assert_not_called()
        mock_log.warning.assert_not_called()

        executor.kube_client.read_namespaced_pod.assert_not_called()

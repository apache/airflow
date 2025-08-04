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

from unittest.mock import Mock, patch

import pytest

from kubernetes_tests.test_base import (
    EXECUTOR,
    BaseK8STest,  # isort:skip (needed to workaround isort bug)
)


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
    @patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log")
    def test_pod_failure_logging_with_container_terminated(self, mock_log):
        """Test that pod failure information is logged when container is terminated."""

        from airflow.models.taskinstancekey import TaskInstanceKey
        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
        from airflow.utils.state import TaskInstanceState

        # Create a mock KubernetesExecutor instance
        executor = KubernetesExecutor()
        executor.kube_client = Mock()
        executor.kube_scheduler = Mock()

        # Create mock pod status with terminated container
        mock_container_state = Mock()
        mock_container_state.terminated = Mock()
        mock_container_state.terminated.reason = "Error"
        mock_container_state.terminated.message = "Container failed with exit code 1"
        mock_container_state.waiting = None

        mock_container_status = Mock()
        mock_container_status.state = mock_container_state

        mock_pod_status = Mock()
        mock_pod_status.phase = "Failed"
        mock_pod_status.reason = "PodFailed"
        mock_pod_status.message = "Pod execution failed"
        mock_pod_status.container_statuses = [mock_container_status]

        mock_pod = Mock()
        mock_pod.status = mock_pod_status

        executor.kube_client.read_namespaced_pod.return_value = mock_pod

        # Create a test task key
        task_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        # Call _change_state with FAILED status
        executor._change_state(
            key=task_key, state=TaskInstanceState.FAILED, pod_name="test-pod", namespace="test-namespace"
        )

        # Verify that the error log was called with expected parameters
        mock_log.error.assert_called_once_with(
            "Pod %s in namespace %s failed. Pod phase: %s, reason: %s, message: %s, container_state: %s, container_reason: %s, container_message: %s",
            "test-pod",
            "test-namespace",
            "Failed",
            "PodFailed",
            "Pod execution failed",
            "terminated",
            "Error",
            "Container failed with exit code 1",
        )

    @pytest.mark.execution_timeout(300)
    @patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log")
    def test_pod_failure_logging_exception_handling(self, mock_log):
        """Test that exceptions during pod status retrieval are handled gracefully."""
        from kubernetes.client.rest import ApiException

        from airflow.models.taskinstancekey import TaskInstanceKey
        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
        from airflow.utils.state import TaskInstanceState

        # Create a mock KubernetesExecutor instance
        executor = KubernetesExecutor()
        executor.kube_client = Mock()
        executor.kube_scheduler = Mock()

        # Make read_namespaced_pod raise an exception
        executor.kube_client.read_namespaced_pod.side_effect = ApiException(status=404, reason="Not Found")

        # Create a test task key
        task_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        # Call _change_state with FAILED status
        executor._change_state(
            key=task_key, state=TaskInstanceState.FAILED, pod_name="test-pod", namespace="test-namespace"
        )

        # Verify that the warning log was called with the correct parameters
        mock_log.warning.assert_called_once()
        call_args = mock_log.warning.call_args[0]
        assert call_args[0] == "Failed to fetch pod failure reason for %s/%s: %s"
        assert call_args[1] == "test-namespace"
        assert call_args[2] == "test-pod"
        # The third argument should be the exception
        assert isinstance(call_args[3], ApiException)

        # Verify that error log was not called
        mock_log.error.assert_not_called()

    @pytest.mark.execution_timeout(300)
    @patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log")
    def test_pod_failure_logging_non_failed_state(self, mock_log):
        """Test that pod failure logging only occurs for FAILED state."""
        from airflow.models.taskinstancekey import TaskInstanceKey
        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
        from airflow.utils.state import TaskInstanceState

        # Create a mock KubernetesExecutor instance
        executor = KubernetesExecutor()
        executor.kube_client = Mock()
        executor.kube_scheduler = Mock()

        # Create a test task key
        task_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        # Call _change_state with SUCCESS status
        executor._change_state(
            key=task_key, state=TaskInstanceState.SUCCESS, pod_name="test-pod", namespace="test-namespace"
        )

        # Verify that no failure logs were called
        mock_log.error.assert_not_called()
        mock_log.warning.assert_not_called()

        # Verify that kube_client methods were not called
        executor.kube_client.read_namespaced_pod.assert_not_called()

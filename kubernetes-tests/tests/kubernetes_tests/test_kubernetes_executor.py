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

import json
import re
import subprocess
import time
from subprocess import check_output
from typing import TYPE_CHECKING
from unittest.mock import Mock, patch

import pytest

if TYPE_CHECKING:
    from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import FailureDetails

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import KubernetesResults
from kubernetes_tests.test_base import EXECUTOR, BaseK8STest


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
        executor.kube_scheduler = Mock()

        # Create test failure details
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

        # Create a test task key
        task_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        # Create KubernetesResults object
        results = KubernetesResults(
            key=task_key,
            state=TaskInstanceState.FAILED,
            pod_name="test-pod",
            namespace="test-namespace",
            resource_version="123",
            failure_details=failure_details,
        )

        # Call _change_state with KubernetesResults object
        executor._change_state(results)

        # Verify that the warning log was called with expected parameters
        mock_log.warning.assert_called_once_with(
            "Task %s failed in pod %s/%s. Pod phase: %s, reason: %s, message: %s, "
            "container_type: %s, container_name: %s, container_state: %s, container_reason: %s, "
            "container_message: %s, exit_code: %s",
            "test_dag.test_task.1",
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
    @patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor.KubernetesExecutor.log")
    def test_pod_failure_logging_exception_handling(self, mock_log):
        """Test that failures without details are handled gracefully."""
        from airflow.models.taskinstancekey import TaskInstanceKey
        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
        from airflow.utils.state import TaskInstanceState

        # Create a mock KubernetesExecutor instance
        executor = KubernetesExecutor()
        executor.kube_scheduler = Mock()

        # Create a test task key
        task_key = TaskInstanceKey(dag_id="test_dag", task_id="test_task", run_id="test_run", try_number=1)

        # Create KubernetesResults object without failure details
        results = KubernetesResults(
            key=task_key,
            state=TaskInstanceState.FAILED,
            pod_name="test-pod",
            namespace="test-namespace",
            resource_version="123",
            failure_details=None,
        )

        # Call _change_state with KubernetesResults object
        executor._change_state(results)

        # Verify that the warning log was called with the correct parameters
        mock_log.warning.assert_called_once_with(
            "Task %s failed in pod %s/%s (no details available)",
            "test_dag.test_task.1",
            "test-namespace",
            "test-pod",
        )

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

        # Create KubernetesResults object with SUCCESS state
        results = KubernetesResults(
            key=task_key,
            state=TaskInstanceState.SUCCESS,
            pod_name="test-pod",
            namespace="test-namespace",
            resource_version="123",
            failure_details=None,
        )

        # Call _change_state with KubernetesResults object
        executor._change_state(results)

        # Verify that no failure logs were called
        mock_log.error.assert_not_called()
        mock_log.warning.assert_not_called()

        # Verify that kube_client methods were not called
        executor.kube_client.read_namespaced_pod.assert_not_called()


@pytest.mark.skipif(EXECUTOR != "KubernetesExecutor", reason="Only runs on KubernetesExecutor")
class TestKubernetesExecutorCallbackSupport(BaseK8STest):
    """
    Integration tests for ExecutorCallback (DeadlineAlert / SyncCallback) support in the
    Kubernetes executor.

    Prerequisites:
      - The ``example_deadline_callback`` and ``example_deadline_callback_slow`` DAGs must
        be loaded in the cluster (they live in airflow-core/src/airflow/example_dags/ and
        are baked into the k8s image via ``breeze k8s build-k8s-image``).
      - The executor must be KubernetesExecutor.
    """

    _FAST_DAG_ID = "example_deadline_callback"
    _SLOW_DAG_ID = "example_deadline_callback_slow"
    _FAILING_DAG_ID = "example_deadline_callback_failing"

    _CALLBACK_LABEL = "airflow-workload-type=callback"
    _CALLBACK_ANNOTATION_KEY = "callback_id"

    @classmethod
    def _get_callback_pods(cls, namespace: str = "airflow") -> list[dict]:
        raw = check_output(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                cls._CALLBACK_LABEL,
                "-o",
                "json",
            ]
        )
        return json.loads(raw)["items"]

    @classmethod
    def _wait_for_callback_pod(cls, run_id: str, namespace: str = "airflow", timeout: int = 120) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for pod in cls._get_callback_pods(namespace):
                annotations = pod.get("metadata", {}).get("annotations", {})
                if annotations.get("run_id") == run_id:
                    return pod
            time.sleep(1)
        raise AssertionError(f"No callback pod for run_id={run_id!r} appeared within {timeout}s")

    @staticmethod
    def _wait_for_pod_phase(
        pod_name: str,
        phases: list[str],
        namespace: str = "airflow",
        timeout: int = 120,
    ) -> str:
        """
        Block until *pod_name* reaches one of *phases*; return the reached phase.

        Returns ``"Deleted"`` if the pod is not found. Include ``"Deleted"`` in *phases*
        when executor-driven deletion counts as success — pods are only removed after they
        succeed (``delete_worker_pods=True`` default).
        """
        deadline = time.monotonic() + timeout
        phase = ""
        while time.monotonic() < deadline:
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pod",
                    pod_name,
                    "-n",
                    namespace,
                    "-o",
                    "jsonpath={.status.phase}",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode != 0 and (
                "NotFound" in result.stderr or "not found" in result.stderr.lower()
            ):
                phase = "Deleted"
            else:
                phase = result.stdout.strip()
            if phase in phases:
                return phase
            time.sleep(2)
        raise AssertionError(
            f"Pod {pod_name!r} did not reach {phases} within {timeout}s (last seen phase: {phase!r})"
        )

    @staticmethod
    def _wait_for_pod_gone(pod_name: str, namespace: str = "airflow", timeout: int = 60) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = subprocess.run(
                ["kubectl", "get", "pod", pod_name, "-n", namespace],
                capture_output=True,
                text=True,
                check=False,
            )
            # kubectl exits non-zero and prints "NotFound" when the pod is gone
            if result.returncode != 0 and "NotFound" in result.stderr:
                return
            time.sleep(5)
        raise AssertionError(f"Pod {pod_name!r} was not deleted within {timeout}s")

    def _trigger_dag_run(self, dag_id: str) -> str:
        result_json = self.start_dag(dag_id=dag_id, host=self.host)
        dag_runs = result_json.get("dag_runs", [])
        matching = [r for r in dag_runs if r["dag_id"] == dag_id]
        assert matching, f"No dag runs returned for dag_id={dag_id!r}"
        newest = max(matching, key=lambda r: r["queued_at"])
        return newest["dag_run_id"]

    @pytest.mark.execution_timeout(300)
    def test_deadline_callback_executes_on_kubernetes(self):
        """
        A DAG with a past deadline fires a SyncCallback that is executed as a Kubernetes pod.
        The pod must reach the Succeeded phase.
        """
        dag_id = self._FAST_DAG_ID
        dag_run_id = self._trigger_dag_run(dag_id)
        print(f"[{dag_id}] dag_run_id={dag_run_id}")

        pod = self._wait_for_callback_pod(dag_run_id, timeout=120)
        pod_name = pod["metadata"]["name"]
        print(f"[{dag_id}] callback pod appeared: {pod_name}")

        # The executor deletes pods immediately after they succeed (delete_worker_pods=True
        # default), so "Deleted" is equally valid evidence of success.
        phase = self._wait_for_pod_phase(pod_name, ["Succeeded", "Failed", "Deleted"], timeout=120)
        assert phase in ("Succeeded", "Deleted"), (
            f"Callback pod {pod_name!r} reached phase {phase!r} instead of Succeeded/Deleted"
        )

    @pytest.mark.execution_timeout(300)
    def test_callback_pod_annotations_and_labels(self):
        """
        The callback pod must carry the expected Airflow annotations (callback_id, dag_id, run_id)
        and labels (airflow-workload-type=callback, kubernetes_executor=True, airflow-worker=…).
        Its container command must invoke execute_workload.
        """
        dag_id = self._FAST_DAG_ID
        dag_run_id = self._trigger_dag_run(dag_id)

        pod = self._wait_for_callback_pod(dag_run_id, timeout=120)
        annotations = pod["metadata"]["annotations"]
        labels = pod["metadata"]["labels"]
        containers = pod["spec"]["containers"]

        assert self._CALLBACK_ANNOTATION_KEY in annotations, (
            f"Annotation {self._CALLBACK_ANNOTATION_KEY!r} missing from pod. Got: {annotations}"
        )
        callback_id = annotations[self._CALLBACK_ANNOTATION_KEY]
        assert re.fullmatch(r"[0-9a-f-]{36}", callback_id), (
            f"callback_id {callback_id!r} does not look like a UUID"
        )
        assert annotations.get("dag_id") == dag_id, (
            f"Expected dag_id annotation {dag_id!r}, got {annotations.get('dag_id')!r}"
        )
        assert "run_id" in annotations, f"run_id annotation missing. Got: {annotations}"
        assert annotations.get("run_id"), "run_id annotation must be non-empty"

        # Task-specific annotations must NOT be present on callback pods.
        for forbidden in ("task_id", "try_number", "map_index"):
            assert forbidden not in annotations, f"Unexpected annotation {forbidden!r} found on callback pod"

        assert labels.get("airflow-workload-type") == "callback", (
            f"Expected label airflow-workload-type=callback, got: {labels}"
        )
        assert labels.get("kubernetes_executor") == "True", (
            f"Expected label kubernetes_executor=True, got: {labels}"
        )
        assert "airflow-worker" in labels, f"airflow-worker label missing. Got: {labels}"

        assert containers, "No containers found in callback pod spec"
        args = containers[0].get("args", []) or []
        cmd = containers[0].get("command", []) or []
        full_cmd = cmd + args
        assert any("execute_workload" in part for part in full_cmd), (
            f"Container command does not include 'execute_workload'. Full command: {full_cmd}"
        )
        assert any("--json-string" in part for part in full_cmd), (
            f"Container command does not include '--json-string'. Full command: {full_cmd}"
        )

    @pytest.mark.execution_timeout(300)
    def test_deadline_callback_pod_failure(self):
        """
        When the callback pod exits with a non-zero code the watcher must emit a FAILED
        state and the executor must not crash.

        Uses ``example_deadline_callback_failing``, whose callback always raises
        ``RuntimeError``, causing ``sys.exit(1)`` in the pod and a ``Failed`` phase.
        Failed pods are NOT auto-deleted (``delete_worker_pods_on_failure=False`` default),
        so the pod stays and we can assert its phase before cleaning it up manually.
        """
        dag_id = self._FAILING_DAG_ID
        dag_run_id = self._trigger_dag_run(dag_id)
        print(f"[{dag_id}] dag_run_id={dag_run_id}")

        pod = self._wait_for_callback_pod(dag_run_id, timeout=120)
        pod_name = pod["metadata"]["name"]
        print(f"[{dag_id}] callback pod appeared: {pod_name}")

        try:
            # Failed pods are NOT auto-deleted (delete_worker_pods_on_failure=False default).
            phase = self._wait_for_pod_phase(pod_name, ["Failed"], timeout=120)
            assert phase == "Failed", f"Expected Failed phase, got {phase!r}"

            # Executor must not have crashed — scheduler pod must still be Running.
            result = subprocess.run(
                [
                    "kubectl",
                    "get",
                    "pod",
                    "-n",
                    "airflow",
                    "-l",
                    "component=scheduler",
                    "-o",
                    "jsonpath={.items[0].status.phase}",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            scheduler_phase = result.stdout.strip()
            assert scheduler_phase == "Running", (
                f"Scheduler pod is in phase {scheduler_phase!r} after callback failure"
            )
        finally:
            # Clean up the failed pod manually (won't be auto-deleted by executor).
            subprocess.run(
                ["kubectl", "delete", "pod", pod_name, "-n", "airflow", "--ignore-not-found"],
                capture_output=True,
                check=False,
            )

    @pytest.mark.execution_timeout(300)
    def test_callback_pod_is_cleaned_up_after_success(self):
        """
        After the callback pod reaches Succeeded, the executor must delete it so no
        orphaned callback pods linger in the namespace.
        """
        dag_id = self._FAST_DAG_ID
        dag_run_id = self._trigger_dag_run(dag_id)

        pod = self._wait_for_callback_pod(dag_run_id, timeout=120)
        pod_name = pod["metadata"]["name"]

        # If the pod is already gone ("Deleted"), that itself is proof of executor-driven
        # cleanup — skip the explicit deletion wait in that case.
        phase = self._wait_for_pod_phase(pod_name, ["Succeeded", "Failed", "Deleted"], timeout=120)
        if phase != "Deleted":
            self._wait_for_pod_gone(pod_name, timeout=60)

    @pytest.mark.execution_timeout(400)
    def test_callback_pod_survives_scheduler_restart(self):
        """
        A callback pod running in Kubernetes must complete even when the scheduler
        is restarted mid-execution. After restart, the new scheduler must re-adopt
        the pod via the watcher label selector and record the terminal state.
        """
        dag_id = self._SLOW_DAG_ID
        dag_run_id = self._trigger_dag_run(dag_id)

        # Wait until the callback pod is actually Running before killing the scheduler.
        pod = self._wait_for_callback_pod(dag_run_id, timeout=120)
        pod_name = pod["metadata"]["name"]
        pre_restart_phase = self._wait_for_pod_phase(
            pod_name, ["Running", "Succeeded", "Failed", "Deleted"], timeout=60
        )

        self._delete_airflow_pod("scheduler")
        self.ensure_resource_health("airflow-scheduler")
        print(f"[{dag_id}] Scheduler restarted; waiting for callback pod to complete.")

        if pre_restart_phase in ("Succeeded", "Deleted"):
            # Pod already completed before the restart — the test goal (pod completion
            # despite scheduler lifecycle) is still met.
            return

        # The slow callback sleeps for 30s; allow plenty of time for completion.
        phase = self._wait_for_pod_phase(pod_name, ["Succeeded", "Failed", "Deleted"], timeout=180)
        assert phase in ("Succeeded", "Deleted"), (
            f"Callback pod {pod_name!r} reached {phase!r} after scheduler restart (expected Succeeded/Deleted)"
        )

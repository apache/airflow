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
import time
from subprocess import check_output
from textwrap import dedent
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


@pytest.mark.skipif(EXECUTOR != "KubernetesExecutor", reason="Only runs on KubernetesExecutor")
class TestKubernetesExecutorCallbacks(BaseK8STest):
    """
    Integration tests for deadline callbacks run as pods on the Kubernetes executor.

    Each test triggers its own Dag run and follows the callback that run created, so other callback pods in
    the namespace cannot satisfy it.  No public API exposes executor callback rows, so the callback is looked
    up in the metadata database from inside the scheduler pod.

    Prerequisites:
      - The ``example_deadline_callback`` Dag must be loaded in the cluster.
    """

    _DAG_ID = "example_deadline_callback"
    _CALLBACK_LABEL = "airflow-workload-type=callback"
    _CALLBACK_ANNOTATION_KEY = "callback_id"
    _TERMINAL_STATES = ("success", "failed")

    _QUERY_CALLBACK_SCRIPT = dedent(
        """\
        import json
        import sys

        from sqlalchemy import select

        from airflow.models.callback import Callback
        from airflow.models.dagrun import DagRun
        from airflow.models.deadline import Deadline
        from airflow.utils.session import create_session

        dag_id, run_id = sys.argv[1:3]
        with create_session() as session:
            row = session.execute(
                select(Callback.id, Callback.state)
                .join(Deadline, Deadline.callback_id == Callback.id)
                .join(DagRun, DagRun.id == Deadline.dagrun_id)
                .where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
            ).first()
        print("CALLBACK_ROW " + json.dumps(None if row is None else [str(row[0]), row[1]]))
        """
    )

    @classmethod
    def _find_callback(cls, dag_id: str, run_id: str, namespace: str = "airflow") -> tuple[str, str] | None:
        """Return ``(callback_id, state)`` of the callback created for a Dag run, if it exists yet."""
        output = check_output(
            [
                "kubectl",
                "exec",
                "-n",
                namespace,
                "deploy/airflow-scheduler",
                "-c",
                "scheduler",
                "--",
                "python",
                "-c",
                cls._QUERY_CALLBACK_SCRIPT,
                dag_id,
                run_id,
            ]
        )
        # Airflow logs to stdout before and after the result, at interpreter exit included, so find it by
        # its marker rather than its position.
        line = next(line for line in output.decode().splitlines() if line.startswith("CALLBACK_ROW "))
        row = json.loads(line.removeprefix("CALLBACK_ROW "))
        return None if row is None else (row[0], row[1])

    @classmethod
    def _wait_for_callback_state(
        cls, dag_id: str, run_id: str, states: tuple[str, ...], timeout: int = 180
    ) -> tuple[str, str]:
        deadline = time.monotonic() + timeout
        found = None
        while time.monotonic() < deadline:
            found = cls._find_callback(dag_id, run_id)
            if found is not None and found[1] in states:
                return found
            time.sleep(2)
        raise AssertionError(
            f"Callback for {dag_id}/{run_id} did not reach one of {states} within {timeout}s (last seen: {found})"
        )

    @classmethod
    def _find_callback_pod(cls, callback_id: str, namespace: str = "airflow") -> dict | None:
        raw = check_output(
            ["kubectl", "get", "pods", "-n", namespace, "-l", cls._CALLBACK_LABEL, "-o", "json"]
        )
        return next(
            (
                pod
                for pod in json.loads(raw)["items"]
                if pod["metadata"].get("annotations", {}).get(cls._CALLBACK_ANNOTATION_KEY) == callback_id
            ),
            None,
        )

    @classmethod
    def _wait_for_callback_pod(cls, callback_id: str, timeout: int = 120) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pod := cls._find_callback_pod(callback_id):
                return pod
            time.sleep(0.5)
        raise AssertionError(f"No pod for callback {callback_id} appeared within {timeout}s")

    @classmethod
    def _wait_for_callback_pod_gone(cls, callback_id: str, timeout: int = 120) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if cls._find_callback_pod(callback_id) is None:
                return
            time.sleep(2)
        raise AssertionError(f"Pod for callback {callback_id} was not deleted within {timeout}s")

    def _trigger_dag_run(self, dag_id: str) -> str:
        result_json = self.start_dag(dag_id=dag_id, host=self.host)
        dag_runs = result_json.get("dag_runs", [])
        matching = [r for r in dag_runs if r["dag_id"] == dag_id]
        assert matching, f"No dag runs returned for dag_id={dag_id!r}"
        newest = max(matching, key=lambda r: r["queued_at"])
        return newest["dag_run_id"]

    @pytest.mark.execution_timeout(300)
    def test_deadline_callback_executes_on_kubernetes(self):
        run_id = self._trigger_dag_run(self._DAG_ID)

        callback_id, state = self._wait_for_callback_state(self._DAG_ID, run_id, self._TERMINAL_STATES)

        assert state == "success", f"Callback {callback_id} ended in state {state!r}"

    @pytest.mark.execution_timeout(300)
    def test_callback_pod_annotations_and_labels(self):
        run_id = self._trigger_dag_run(self._DAG_ID)
        callback_id, _ = self._wait_for_callback_state(
            self._DAG_ID, run_id, ("queued", "running", *self._TERMINAL_STATES)
        )

        pod = self._wait_for_callback_pod(callback_id)
        annotations = pod["metadata"]["annotations"]
        labels = pod["metadata"]["labels"]
        containers = pod["spec"]["containers"]

        assert annotations[self._CALLBACK_ANNOTATION_KEY] == callback_id
        for forbidden in ("dag_id", "run_id", "task_id", "try_number", "map_index"):
            assert forbidden not in annotations, f"Unexpected annotation {forbidden!r} found on callback pod"

        assert labels.get("airflow-workload-type") == "callback", (
            f"Expected label airflow-workload-type=callback, got: {labels}"
        )
        assert labels.get("kubernetes_executor") == "True", (
            f"Expected label kubernetes_executor=True, got: {labels}"
        )
        assert "airflow-worker" in labels, f"airflow-worker label missing. Got: {labels}"

        assert containers, "No containers found in callback pod spec"
        full_cmd = (containers[0].get("command") or []) + (containers[0].get("args") or [])
        assert any("execute_workload" in part for part in full_cmd), (
            f"Container command does not include 'execute_workload'. Full command: {full_cmd}"
        )
        assert any("--json-string" in part for part in full_cmd), (
            f"Container command does not include '--json-string'. Full command: {full_cmd}"
        )

    @pytest.mark.execution_timeout(300)
    def test_callback_pod_is_cleaned_up_after_success(self):
        run_id = self._trigger_dag_run(self._DAG_ID)
        callback_id, state = self._wait_for_callback_state(self._DAG_ID, run_id, self._TERMINAL_STATES)

        assert state == "success", f"Callback {callback_id} ended in state {state!r}"

        self._wait_for_callback_pod_gone(callback_id)

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

from queue import Queue
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
    ADOPTED,
    POD_REVOKED_KEY,
    KubernetesWatch,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
    AirflowKubernetesScheduler,
    KubernetesJobWatcher,
    ResourceVersion,
    _analyze_containers,
    collect_pod_failure_details,
    get_base_pod_from_template,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.utils.state import TaskInstanceState


class TestResourceVersion:
    def test_resource_version_singleton(self):
        rv1 = ResourceVersion()
        rv2 = ResourceVersion()
        assert rv1 is rv2
        rv1.resource_version["default"] = "12345"
        assert rv2.resource_version["default"] == "12345"


class TestGetBasePodFromTemplate:
    @patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.PodGenerator.deserialize_model_file"
    )
    def test_get_base_pod_from_custom_template(self, mock_deserialize):
        mock_pod = MagicMock(spec=k8s.V1Pod)
        mock_deserialize.return_value = mock_pod
        kube_config = MagicMock()
        kube_config.pod_template_file = "/path/to/default.yaml"

        result = get_base_pod_from_template("/path/to/custom.yaml", kube_config)

        mock_deserialize.assert_called_once_with("/path/to/custom.yaml")
        assert result == mock_pod

    @patch(
        "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.PodGenerator.deserialize_model_file"
    )
    def test_get_base_pod_from_default_config_template(self, mock_deserialize):
        mock_pod = MagicMock(spec=k8s.V1Pod)
        mock_deserialize.return_value = mock_pod
        kube_config = MagicMock()
        kube_config.pod_template_file = "/path/to/default.yaml"

        result = get_base_pod_from_template(None, kube_config)

        mock_deserialize.assert_called_once_with("/path/to/default.yaml")
        assert result == mock_pod


class TestCollectPodFailureDetails:
    def test_pod_not_failed_returns_none(self):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.status.phase = "Running"
        logger = MagicMock()

        assert collect_pod_failure_details(pod, logger) is None

    def test_pod_status_none_returns_none(self):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.status = None
        logger = MagicMock()

        assert collect_pod_failure_details(pod, logger) is None

    def test_pod_failed_basic_info(self):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.status.phase = "Failed"
        pod.status.reason = "Evicted"
        pod.status.message = "The node was low on resource: ephemeral-storage."
        pod.status.init_container_statuses = None
        pod.status.container_statuses = None
        logger = MagicMock()

        details = collect_pod_failure_details(pod, logger)
        assert details == {
            "pod_status": "Failed",
            "pod_reason": "Evicted",
            "pod_message": "The node was low on resource: ephemeral-storage.",
        }

    def test_analyze_containers_terminated_container(self):
        container_status = MagicMock(spec=k8s.V1ContainerStatus)
        container_status.name = "base"
        container_status.state.terminated.exit_code = 137
        container_status.state.terminated.reason = "OOMKilled"
        container_status.state.terminated.message = "Container killed by OOM"
        container_status.state.waiting = None

        details = _analyze_containers([container_status], "main")
        assert details == {
            "container_type": "main",
            "failed_container": "base",
            "container_reason": "OOMKilled",
            "container_message": "Container killed by OOM",
            "exit_code": 137,
        }

    def test_analyze_containers_waiting_container(self):
        container_status = MagicMock(spec=k8s.V1ContainerStatus)
        container_status.name = "init"
        container_status.state.terminated = None
        container_status.state.waiting.reason = "CrashLoopBackOff"
        container_status.state.waiting.message = "back-off 5m0s restarting failed container"

        details = _analyze_containers([container_status], "init")
        assert details == {
            "container_type": "init",
            "failed_container": "init",
            "container_reason": "CrashLoopBackOff",
            "container_message": "back-off 5m0s restarting failed container",
            "exit_code": None,
        }


class TestKubernetesJobWatcher:
    @pytest.fixture
    def watcher(self):
        queue = Queue()
        kube_config = MagicMock()
        kube_config.worker_pod_pending_fatal_container_state_reasons = ["CreateContainerConfigError"]
        watcher = KubernetesJobWatcher(
            namespace="default",
            watcher_queue=queue,
            resource_version="100",
            scheduler_job_id="test-job-id",
            kube_config=kube_config,
        )
        return watcher

    def test_process_error_410_resets_resource_version(self, watcher):
        event = {
            "raw_object": {
                "code": 410,
                "reason": "Expired",
                "message": "Resource version too old",
            }
        }
        rv = watcher.process_error(event)
        assert rv == "0"

    def test_process_error_non_410_raises_exception(self, watcher):
        event = {
            "raw_object": {
                "code": 500,
                "reason": "InternalServerError",
                "message": "cluster unavailable",
            }
        }
        with pytest.raises(AirflowException) as exc_info:
            watcher.process_error(event)
        assert "InternalServerError" in str(exc_info.value)
        assert "500" in str(exc_info.value)

    def test_process_status_pod_revoked_ignored(self, watcher):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.metadata.labels = {POD_REVOKED_KEY: "True"}
        event = {"object": pod, "type": "MODIFIED"}

        watcher.process_status(
            pod_name="test-pod",
            namespace="default",
            status="Running",
            annotations={},
            resource_version="101",
            event=event,
        )
        assert watcher.watcher_queue.empty()

    def test_process_status_pod_adopted(self, watcher):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.metadata.labels = {}
        pod.metadata.deletion_timestamp = None
        event = {"object": pod, "type": "DELETED"}

        watcher.process_status(
            pod_name="test-pod",
            namespace="default",
            status="Running",
            annotations={"dag_id": "test_dag", "task_id": "test_task"},
            resource_version="102",
            event=event,
        )
        assert not watcher.watcher_queue.empty()
        item = watcher.watcher_queue.get_nowait()
        assert isinstance(item, KubernetesWatch)
        assert item.pod_name == "test-pod"
        assert item.state == ADOPTED

    def test_process_status_succeeded(self, watcher):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.metadata.labels = {}
        event = {"object": pod, "type": "MODIFIED"}

        watcher.process_status(
            pod_name="test-pod",
            namespace="default",
            status="Succeeded",
            annotations={"dag_id": "test_dag", "task_id": "test_task"},
            resource_version="103",
            event=event,
        )
        assert not watcher.watcher_queue.empty()
        item = watcher.watcher_queue.get_nowait()
        assert item.state is None

    def test_process_status_failed(self, watcher):
        pod = MagicMock(spec=k8s.V1Pod)
        pod.metadata.labels = {}
        pod.status.phase = "Failed"
        pod.status.reason = "Error"
        pod.status.message = "Process exited with code 1"
        pod.status.init_container_statuses = None
        pod.status.container_statuses = None
        event = {"object": pod, "type": "MODIFIED"}

        watcher.process_status(
            pod_name="test-pod",
            namespace="default",
            status="Failed",
            annotations={"dag_id": "test_dag", "task_id": "test_task", "try_number": "1"},
            resource_version="104",
            event=event,
        )
        assert not watcher.watcher_queue.empty()
        item = watcher.watcher_queue.get_nowait()
        assert item.state == TaskInstanceState.FAILED
        assert item.failure_details is not None
        assert item.failure_details["pod_status"] == "Failed"


class TestAirflowKubernetesScheduler:
    @patch("airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils._get_executor_kube_client")
    def test_delete_pod(self, mock_get_kube_client):
        mock_kube_client = MagicMock()
        mock_get_kube_client.return_value = mock_kube_client
        kube_config = MagicMock()
        scheduler = AirflowKubernetesScheduler(
            kube_config=kube_config,
            result_queue=Queue(),
            kube_client=mock_kube_client,
            scheduler_job_id="test-job",
        )

        scheduler.delete_pod(pod_name="test-pod", namespace="default")
        mock_kube_client.delete_namespaced_pod.assert_called_once_with(
            name="test-pod",
            namespace="default",
            body=mock_kube_client.delete_namespaced_pod.call_args.kwargs["body"],
        )

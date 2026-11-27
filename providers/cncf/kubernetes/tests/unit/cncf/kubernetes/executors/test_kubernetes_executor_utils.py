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

import logging
from datetime import UTC
from queue import Queue
from unittest import mock

import pytest
from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import (
    POD_REVOKED_KEY,
)
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
    KubernetesJobWatcher,
    ResourceVersion,
    _analyze_containers,
    _analyze_init_containers,
    _analyze_main_containers,
    collect_pod_failure_details,
    get_base_pod_from_template,
)
from airflow.utils.state import TaskInstanceState

ANNOTATIONS = {
    "dag_id": "test_dag",
    "task_id": "test_task",
    "run_id": "test_run",
    "try_number": "1",
    "execution_date": "2024-01-01T00:00:00+00:00",
}


def _make_watcher(namespace="default", kube_config=None):
    """Build a KubernetesJobWatcher with a real queue but without starting the process."""
    watcher_queue: Queue = Queue()
    if kube_config is None:
        kube_config = mock.MagicMock()
        kube_config.worker_pod_pending_fatal_container_state_reasons = []
        kube_config.kube_client_request_args = {}
    watcher = KubernetesJobWatcher(
        namespace=namespace,
        watcher_queue=watcher_queue,
        resource_version="0",
        scheduler_job_id="scheduler-1",
        kube_config=kube_config,
    )
    return watcher


def _make_pod(
    name="test-pod",
    namespace="default",
    phase="Running",
    labels=None,
    deletion_timestamp=None,
    reason=None,
    resource_version="1",
):
    labels = labels or {}
    pod = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=labels,
            annotations=ANNOTATIONS,
            resource_version=resource_version,
            deletion_timestamp=deletion_timestamp,
        ),
        status=k8s.V1PodStatus(phase=phase, reason=reason),
    )
    return pod


class TestResourceVersion:
    def test_singleton_returns_same_instance(self):
        rv1 = ResourceVersion()
        rv2 = ResourceVersion()
        assert rv1 is rv2

    def test_resource_version_dict_is_shared(self):
        rv1 = ResourceVersion()
        rv1.resource_version["test-ns"] = "42"
        rv2 = ResourceVersion()
        assert rv2.resource_version["test-ns"] == "42"
        # Cleanup
        del rv1.resource_version["test-ns"]


class TestKubernetesJobWatcherProcessError:
    def test_process_error_410_returns_zero(self):
        watcher = _make_watcher()
        event = {
            "raw_object": {
                "code": 410,
                "message": "resource version too old",
                "reason": "Gone",
            }
        }
        result = watcher.process_error(event)
        assert result == "0"

    def test_process_error_non_410_raises(self):
        from airflow.providers.common.compat.sdk import AirflowException

        watcher = _make_watcher()
        event = {
            "raw_object": {
                "code": 500,
                "message": "internal server error",
                "reason": "InternalError",
            }
        }
        with pytest.raises(AirflowException, match="Kubernetes failure"):
            watcher.process_error(event)


class TestKubernetesJobWatcherProcessStatus:
    def test_revoked_pod_is_ignored(self):
        watcher = _make_watcher()
        pod = _make_pod(labels={POD_REVOKED_KEY: "True"})
        event = {"type": "MODIFIED", "object": pod, "raw_object": {}}
        watcher.process_status("test-pod", "default", "Running", ANNOTATIONS, "1", event)
        assert watcher.watcher_queue.empty()

    def test_deleted_pod_without_deletion_timestamp_is_adopted(self):
        watcher = _make_watcher()
        pod = _make_pod(labels={})
        event = {"type": "DELETED", "object": pod, "raw_object": {}}
        watcher.process_status("test-pod", "default", "Running", ANNOTATIONS, "1", event)
        result = watcher.watcher_queue.get_nowait()
        from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_types import ADOPTED

        assert result.state == ADOPTED

    def test_succeeded_pod_is_queued_with_none_state(self):
        watcher = _make_watcher()
        pod = _make_pod(labels={}, phase="Succeeded")
        event = {"type": "MODIFIED", "object": pod, "raw_object": {}}
        watcher.process_status("test-pod", "default", "Succeeded", ANNOTATIONS, "1", event)
        result = watcher.watcher_queue.get_nowait()
        assert result.state is None

    def test_failed_pod_is_queued_with_failed_state(self):
        watcher = _make_watcher()
        pod = _make_pod(labels={}, phase="Failed")
        event = {"type": "MODIFIED", "object": pod, "raw_object": {}}
        with mock.patch(
            "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.collect_pod_failure_details",
            return_value=None,
        ):
            watcher.process_status("test-pod", "default", "Failed", ANNOTATIONS, "1", event)
        result = watcher.watcher_queue.get_nowait()
        assert result.state == TaskInstanceState.FAILED

    def test_running_pod_deleted_with_timestamp_is_failed(self):
        from datetime import datetime

        watcher = _make_watcher()
        deletion_ts = datetime(2024, 1, 1, tzinfo=UTC)
        pod = _make_pod(labels={}, phase="Running", deletion_timestamp=deletion_ts)
        event = {"type": "DELETED", "object": pod, "raw_object": {}}
        watcher.process_status("test-pod", "default", "Running", ANNOTATIONS, "1", event)
        result = watcher.watcher_queue.get_nowait()
        assert result.state == TaskInstanceState.FAILED

    def test_running_pod_without_deletion_does_not_enqueue(self):
        watcher = _make_watcher()
        pod = _make_pod(labels={}, phase="Running")
        event = {"type": "MODIFIED", "object": pod, "raw_object": {}}
        watcher.process_status("test-pod", "default", "Running", ANNOTATIONS, "1", event)
        assert watcher.watcher_queue.empty()

    def test_provider_failed_pod_is_queued_as_failed(self):
        watcher = _make_watcher()
        pod = _make_pod(labels={}, phase="Failed", reason="ProviderFailed")
        event = {"type": "MODIFIED", "object": pod, "raw_object": {}}
        watcher.process_status("test-pod", "default", "Running", ANNOTATIONS, "1", event)
        result = watcher.watcher_queue.get_nowait()
        assert result.state == TaskInstanceState.FAILED

    def test_pending_pod_with_fatal_image_pull_reason_is_failed(self):
        kube_config = mock.MagicMock()
        kube_config.worker_pod_pending_fatal_container_state_reasons = ["ErrImagePull", "OOMKilled"]
        kube_config.kube_client_request_args = {}
        watcher = _make_watcher(kube_config=kube_config)
        pod = _make_pod(labels={}, phase="Pending")
        raw_object = {
            "status": {
                "containerStatuses": [
                    {
                        "name": "base",
                        "state": {
                            "waiting": {
                                "reason": "OOMKilled",
                                "message": "Out of memory",
                            }
                        },
                    }
                ]
            }
        }
        event = {"type": "MODIFIED", "object": pod, "raw_object": raw_object}
        watcher.process_status("test-pod", "default", "Pending", ANNOTATIONS, "1", event)
        result = watcher.watcher_queue.get_nowait()
        assert result.state == TaskInstanceState.FAILED

    def test_pending_pod_with_qps_exceeded_err_image_pull_is_not_failed(self):
        kube_config = mock.MagicMock()
        kube_config.worker_pod_pending_fatal_container_state_reasons = ["ErrImagePull"]
        kube_config.kube_client_request_args = {}
        watcher = _make_watcher(kube_config=kube_config)
        pod = _make_pod(labels={}, phase="Pending")
        raw_object = {
            "status": {
                "containerStatuses": [
                    {
                        "name": "base",
                        "state": {
                            "waiting": {
                                "reason": "ErrImagePull",
                                "message": "pull QPS exceeded",
                            }
                        },
                    }
                ]
            }
        }
        event = {"type": "MODIFIED", "object": pod, "raw_object": raw_object}
        watcher.process_status("test-pod", "default", "Pending", ANNOTATIONS, "1", event)
        # QPS exceeded is explicitly skipped, so no failure should be queued
        assert watcher.watcher_queue.empty()


class TestCollectPodFailureDetails:
    def _make_pod_with_status(
        self, phase, container_statuses=None, init_container_statuses=None, reason=None, message=None
    ):
        pod_status = k8s.V1PodStatus(
            phase=phase,
            reason=reason,
            message=message,
            container_statuses=container_statuses,
            init_container_statuses=init_container_statuses,
        )
        return k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="test-pod"),
            status=pod_status,
        )

    def test_non_failed_pod_returns_none(self):
        pod = self._make_pod_with_status("Running")
        result = collect_pod_failure_details(pod, logging.getLogger())
        assert result is None

    def test_failed_pod_with_no_container_info(self):
        pod = self._make_pod_with_status("Failed", reason="Error", message="Pod failed")
        result = collect_pod_failure_details(pod, logging.getLogger())
        assert result is not None
        assert result["pod_status"] == "Failed"
        assert result["pod_reason"] == "Error"
        assert result["pod_message"] == "Pod failed"

    def test_failed_pod_with_terminated_container(self):
        terminated = k8s.V1ContainerStateTerminated(exit_code=1, reason="Error", message="OOM killed")
        container_status = k8s.V1ContainerStatus(
            name="base",
            state=k8s.V1ContainerState(terminated=terminated),
            image="test",
            image_id="test",
            ready=False,
            restart_count=0,
        )
        pod = self._make_pod_with_status("Failed", container_statuses=[container_status])
        result = collect_pod_failure_details(pod, logging.getLogger())
        assert result is not None
        assert result["container_state"] == "terminated"
        assert result["exit_code"] == 1
        assert result["container_type"] == "main"
        assert result["container_name"] == "base"

    def test_failed_pod_with_completed_container_is_not_a_failure(self):
        terminated = k8s.V1ContainerStateTerminated(exit_code=0, reason="Completed")
        container_status = k8s.V1ContainerStatus(
            name="base",
            state=k8s.V1ContainerState(terminated=terminated),
            image="test",
            image_id="test",
            ready=False,
            restart_count=0,
        )
        pod = self._make_pod_with_status("Failed", container_statuses=[container_status])
        result = collect_pod_failure_details(pod, logging.getLogger())
        # Container exited 0 / "Completed" should not be flagged as a container failure
        assert result is not None
        assert "container_state" not in result

    def test_init_container_failure_takes_priority_over_main(self):
        init_terminated = k8s.V1ContainerStateTerminated(exit_code=1, reason="Error")
        init_cs = k8s.V1ContainerStatus(
            name="init",
            state=k8s.V1ContainerState(terminated=init_terminated),
            image="test",
            image_id="test",
            ready=False,
            restart_count=0,
        )
        main_terminated = k8s.V1ContainerStateTerminated(exit_code=2, reason="Error")
        main_cs = k8s.V1ContainerStatus(
            name="main",
            state=k8s.V1ContainerState(terminated=main_terminated),
            image="test",
            image_id="test",
            ready=False,
            restart_count=0,
        )
        pod = self._make_pod_with_status(
            "Failed", container_statuses=[main_cs], init_container_statuses=[init_cs]
        )
        result = collect_pod_failure_details(pod, logging.getLogger())
        assert result is not None
        assert result["container_type"] == "init"

    def test_waiting_container_state_captured(self):
        waiting = k8s.V1ContainerStateWaiting(reason="ImagePullBackOff", message="Cannot pull image")
        container_status = k8s.V1ContainerStatus(
            name="base",
            state=k8s.V1ContainerState(waiting=waiting),
            image="test",
            image_id="test",
            ready=False,
            restart_count=0,
        )
        pod = self._make_pod_with_status("Failed", container_statuses=[container_status])
        result = collect_pod_failure_details(pod, logging.getLogger())
        assert result is not None
        assert result["container_state"] == "waiting"
        assert result["container_reason"] == "ImagePullBackOff"


class TestAnalyzeContainers:
    def test_none_input_returns_none(self):
        assert _analyze_containers(None, "main") is None

    def test_empty_list_returns_none(self):
        assert _analyze_containers([], "main") is None

    def test_terminated_nonzero_exit_returned(self):
        terminated = k8s.V1ContainerStateTerminated(exit_code=137, reason="OOMKilled")
        cs = k8s.V1ContainerStatus(
            name="main",
            state=k8s.V1ContainerState(terminated=terminated),
            image="i",
            image_id="i",
            ready=False,
            restart_count=0,
        )
        result = _analyze_containers([cs], "main")
        assert result is not None
        assert result["exit_code"] == 137
        assert result["container_type"] == "main"

    def test_zero_exit_code_is_not_failure(self):
        terminated = k8s.V1ContainerStateTerminated(exit_code=0, reason="Completed")
        cs = k8s.V1ContainerStatus(
            name="main",
            state=k8s.V1ContainerState(terminated=terminated),
            image="i",
            image_id="i",
            ready=False,
            restart_count=0,
        )
        result = _analyze_containers([cs], "main")
        assert result is None

    def test_waiting_state_returned_when_no_terminated(self):
        waiting = k8s.V1ContainerStateWaiting(reason="ErrImagePull")
        cs = k8s.V1ContainerStatus(
            name="main",
            state=k8s.V1ContainerState(waiting=waiting),
            image="i",
            image_id="i",
            ready=False,
            restart_count=0,
        )
        result = _analyze_containers([cs], "init")
        assert result is not None
        assert result["container_state"] == "waiting"
        assert result["container_type"] == "init"

    def test_terminated_takes_priority_over_waiting(self):
        """When a terminated (failed) container exists alongside a waiting container, terminated wins."""
        terminated = k8s.V1ContainerStateTerminated(exit_code=1, reason="Error")
        cs_terminated = k8s.V1ContainerStatus(
            name="main",
            state=k8s.V1ContainerState(terminated=terminated),
            image="i",
            image_id="i",
            ready=False,
            restart_count=0,
        )
        waiting = k8s.V1ContainerStateWaiting(reason="ErrImagePull")
        cs_waiting = k8s.V1ContainerStatus(
            name="sidecar",
            state=k8s.V1ContainerState(waiting=waiting),
            image="i",
            image_id="i",
            ready=False,
            restart_count=0,
        )
        result = _analyze_containers([cs_waiting, cs_terminated], "main")
        assert result is not None
        assert result["container_state"] == "terminated"


class TestAnalyzeInitAndMainContainerHelpers:
    def test_analyze_init_containers_delegates_correctly(self):
        pod_status = mock.MagicMock()
        pod_status.init_container_statuses = None
        result = _analyze_init_containers(pod_status)
        assert result is None

    def test_analyze_main_containers_delegates_correctly(self):
        pod_status = mock.MagicMock()
        pod_status.container_statuses = None
        result = _analyze_main_containers(pod_status)
        assert result is None


class TestGetBasePodFromTemplate:
    def test_uses_pod_template_file_when_provided(self):
        with mock.patch(
            "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.PodGenerator.deserialize_model_file"
        ) as mock_deserialize:
            mock_deserialize.return_value = k8s.V1Pod()
            kube_config = mock.MagicMock()
            get_base_pod_from_template("/path/to/template.yaml", kube_config)
            mock_deserialize.assert_called_once_with("/path/to/template.yaml")

    def test_falls_back_to_kube_config_template_when_no_file_given(self):
        with mock.patch(
            "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.PodGenerator.deserialize_model_file"
        ) as mock_deserialize:
            mock_deserialize.return_value = k8s.V1Pod()
            kube_config = mock.MagicMock()
            kube_config.pod_template_file = "/config/template.yaml"
            get_base_pod_from_template(None, kube_config)
            mock_deserialize.assert_called_once_with("/config/template.yaml")

    def test_falls_back_to_kube_config_template_when_empty_string_given(self):
        with mock.patch(
            "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.PodGenerator.deserialize_model_file"
        ) as mock_deserialize:
            mock_deserialize.return_value = k8s.V1Pod()
            kube_config = mock.MagicMock()
            kube_config.pod_template_file = "/config/template.yaml"
            get_base_pod_from_template("", kube_config)
            mock_deserialize.assert_called_once_with("/config/template.yaml")

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

import importlib
import os
from unittest import mock
from unittest.mock import MagicMock, call

import kubernetes
import pytest
from dateutil.parser import parse
from kubernetes.client import models as k8s

from airflow.cli import cli_parser
from airflow.executors import executor_loader
from airflow.providers.cncf.kubernetes.cli import kubernetes_command

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

pytestmark = pytest.mark.db_test


class TestGenerateDagYamlCommand:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "KubernetesExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    def test_generate_dag_yaml(self, tmp_path):
        path = tmp_path / "miscellaneous_test_dag_run_after_loop_2020-11-03T00_00_00_plus_00_00.yml"
        kubernetes_command.generate_pod_yaml(
            self.parser.parse_args(
                [
                    "kubernetes",
                    "generate-dag-yaml",
                    "miscellaneous_test_dag",
                    "--logical-date",
                    "2020-11-03",
                    "--output-path",
                    os.fspath(path.parent),
                ]
                if AIRFLOW_V_3_0_PLUS
                else [
                    "kubernetes",
                    "generate-dag-yaml",
                    "miscellaneous_test_dag",
                    "2020-11-03",
                    "--output-path",
                    os.fspath(path.parent),
                ]
            )
        )
        assert sum(1 for _ in path.parent.iterdir()) == 1
        output_path = path.parent / "airflow_yaml_output"
        assert sum(1 for _ in output_path.iterdir()) == 6
        assert os.path.isfile(output_path / path.name)
        assert (output_path / path.name).stat().st_size > 0


class TestCleanUpPodsCommand:
    label_selector = "dag_id,task_id,try_number,airflow_version"

    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "KubernetesExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @mock.patch("kubernetes.client.CoreV1Api.delete_namespaced_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config.load_incluster_config")
    def test_delete_pod(self, load_incluster_config, delete_namespaced_pod):
        kubernetes_command._delete_pod("dummy", "awesome-namespace")
        delete_namespaced_pod.assert_called_with(body=mock.ANY, name="dummy", namespace="awesome-namespace")
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config.load_incluster_config")
    def test_running_pods_are_not_cleaned(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Running"
        pod1.status.reason = None
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        delete_pod.assert_not_called()
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config.load_incluster_config")
    def test_cleanup_succeeded_pods(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Succeeded"
        pod1.status.reason = None
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        delete_pod.assert_called_with("dummy", "awesome-namespace")
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_no_cleanup_failed_pods_wo_restart_policy_never(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy2"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Failed"
        pod1.status.reason = None
        pod1.spec.restart_policy = "Always"
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        delete_pod.assert_not_called()
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_failed_pods_w_restart_policy_never(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy3"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Failed"
        pod1.status.reason = None
        pod1.spec.restart_policy = "Never"
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        delete_pod.assert_called_with("dummy3", "awesome-namespace")
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_evicted_pods(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy4"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Failed"
        pod1.status.reason = "Evicted"
        pod1.spec.restart_policy = "Never"
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        delete_pod.assert_called_with("dummy4", "awesome-namespace")
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_pending_pods(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy5"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Pending"
        pod1.status.reason = "Unschedulable"
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        delete_pod.assert_called_with("dummy5", "awesome-namespace")
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_api_exception_continue(self, load_incluster_config, list_namespaced_pod, delete_pod):
        delete_pod.side_effect = kubernetes.client.rest.ApiException(status=0)
        pod1 = MagicMock()
        pod1.metadata.name = "dummy"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Succeeded"
        pod1.status.reason = None
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod1]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        list_namespaced_pod.assert_called_once_with(
            namespace="awesome-namespace", limit=500, label_selector=self.label_selector
        )
        load_incluster_config.assert_called_once()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_list_pod_with_continue_token(self, load_incluster_config, list_namespaced_pod, delete_pod):
        pod1 = MagicMock()
        pod1.metadata.name = "dummy"
        pod1.metadata.creation_timestamp = parse("2021-12-20T08:01:07Z")
        pod1.status.phase = "Succeeded"
        pod1.status.reason = None
        pods = MagicMock()
        pods.metadata._continue = "dummy-token"
        pods.items = [pod1]
        next_pods = MagicMock()
        next_pods.metadata._continue = None
        next_pods.items = [pod1]
        list_namespaced_pod.side_effect = [pods, next_pods]
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(["kubernetes", "cleanup-pods", "--namespace", "awesome-namespace"])
        )
        calls = [
            call.first(namespace="awesome-namespace", limit=500, label_selector=self.label_selector),
            call.second(
                namespace="awesome-namespace",
                limit=500,
                label_selector=self.label_selector,
                _continue="dummy-token",
            ),
        ]
        list_namespaced_pod.assert_has_calls(calls)
        delete_pod.assert_called_with("dummy", "awesome-namespace")
        load_incluster_config.assert_called_once()

    # -- min-completed-minutes tests --

    def _make_pod(self, name, phase, finished_at, reason=None, restart_policy="Never"):
        """Build a minimal pod mock for min-completed-minutes tests."""
        pod = MagicMock()
        pod.metadata.name = name
        pod.metadata.creation_timestamp = parse("2021-12-20T08:00:00Z")
        pod.status.phase = phase
        pod.status.reason = reason
        pod.spec.restart_policy = restart_policy
        container_status = MagicMock()
        container_status.state.terminated.finished_at = finished_at
        pod.status.container_statuses = [container_status]
        return pod

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_succeeded_pod_too_young_not_deleted(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        # finished_at far in the future → age is negative → less than 1 min → skip
        pod = self._make_pod("run-o1sxc2on", "Succeeded", parse("2099-12-20T08:01:07Z"))
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(
                [
                    "kubernetes",
                    "cleanup-pods",
                    "--namespace",
                    "awesome-namespace",
                    "--min-completed-minutes",
                    "1",
                ]
            )
        )
        delete_pod.assert_not_called()

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_succeeded_pod_old_enough_deleted(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        # finished_at far in the past → age > 1 min → delete
        pod = self._make_pod("run-oldpod", "Succeeded", parse("2021-12-20T08:01:07Z"))
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(
                [
                    "kubernetes",
                    "cleanup-pods",
                    "--namespace",
                    "awesome-namespace",
                    "--min-completed-minutes",
                    "1",
                ]
            )
        )
        delete_pod.assert_called_with("run-oldpod", "awesome-namespace")

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_min_completed_zero_deletes_immediately(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        # explicit 0 disables the age check: delete Succeeded pods regardless of age
        pod = self._make_pod("run-newpod", "Succeeded", parse("2099-12-20T08:01:07Z"))
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(
                [
                    "kubernetes",
                    "cleanup-pods",
                    "--namespace",
                    "awesome-namespace",
                    "--min-completed-minutes",
                    "0",
                ]
            )
        )
        delete_pod.assert_called_with("run-newpod", "awesome-namespace")

    @mock.patch("airflow.providers.cncf.kubernetes.cli.kubernetes_command._delete_pod")
    @mock.patch("kubernetes.client.CoreV1Api.list_namespaced_pod")
    @mock.patch("kubernetes.config.load_incluster_config")
    def test_cleanup_failed_pod_too_young_not_deleted(
        self, load_incluster_config, list_namespaced_pod, delete_pod
    ):
        # Failed + restart_policy=Never, but finished too recently → skip
        pod = self._make_pod(
            "run-failpod", "Failed", parse("2099-12-20T08:01:07Z"), restart_policy="Never"
        )
        pods = MagicMock()
        pods.metadata._continue = None
        pods.items = [pod]
        list_namespaced_pod.return_value = pods
        kubernetes_command.cleanup_pods(
            self.parser.parse_args(
                [
                    "kubernetes",
                    "cleanup-pods",
                    "--namespace",
                    "awesome-namespace",
                    "--min-completed-minutes",
                    "1",
                ]
            )
        )
        delete_pod.assert_not_called()


class TestGetPodCompletionTime:
    T1 = parse("2024-01-01T10:00:00Z")
    T2 = parse("2024-01-01T10:05:00Z")
    T3 = parse("2024-01-01T09:00:00Z")  # earlier than T1/T2, used as creation_timestamp

    def _cs(self, finished_at=None):
        """Real V1ContainerStatus with optional finished_at."""
        return k8s.V1ContainerStatus(
            name="base",
            ready=False,
            restart_count=0,
            image="img",
            image_id="id",
            state=k8s.V1ContainerState(
                terminated=k8s.V1ContainerStateTerminated(exit_code=0, finished_at=finished_at)
                if finished_at
                else None
            ),
        )

    def _cond(self, last_transition_time):
        return k8s.V1PodCondition(type="Ready", status="False", last_transition_time=last_transition_time)

    def _pod(self, container_statuses=None, init_container_statuses=None, conditions=None):
        pod = MagicMock()
        pod.status.container_statuses = container_statuses
        pod.status.init_container_statuses = init_container_statuses
        pod.status.conditions = conditions
        pod.metadata.creation_timestamp = self.T3
        return pod

    def test_single_main_container(self):
        pod = self._pod(container_statuses=[self._cs(self.T1)])
        assert kubernetes_command._get_pod_completion_time(pod) == self.T1

    def test_single_init_container_no_main(self):
        pod = self._pod(container_statuses=[], init_container_statuses=[self._cs(self.T1)])
        assert kubernetes_command._get_pod_completion_time(pod) == self.T1

    def test_main_and_init_returns_max(self):
        pod = self._pod(
            container_statuses=[self._cs(self.T1)],
            init_container_statuses=[self._cs(self.T2)],
        )
        assert kubernetes_command._get_pod_completion_time(pod) == self.T2

    def test_no_finished_at_falls_back_to_conditions(self):
        # container status present but no finished_at → conditions fallback
        pod = self._pod(
            container_statuses=[self._cs(finished_at=None)],
            conditions=[self._cond(self.T1)],
        )
        assert kubernetes_command._get_pod_completion_time(pod) == self.T1

    def test_no_containers_no_conditions_falls_back_to_creation_timestamp(self):
        pod = self._pod(container_statuses=[], init_container_statuses=[], conditions=[])
        assert kubernetes_command._get_pod_completion_time(pod) == self.T3

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
import sys
import unittest
from unittest import mock
from unittest.mock import MagicMock

import kubernetes.client.models as k8s
import pendulum
import pytest
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException

from airflow.exceptions import AirflowException
from airflow.kubernetes.secret import Secret
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.xcom_sidecar import PodDefaults
from airflow.utils import timezone
from airflow.utils.types import DagRunType
from airflow.version import version as airflow_version

# noinspection DuplicatedCode

HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesHook"


def create_context(task):
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("Europe/Amsterdam")
    execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "run_id": dag_run.run_id,
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
    }


# noinspection DuplicatedCode,PyUnusedLocal
class TestKubernetesPodOperatorSystem(unittest.TestCase):
    def get_current_task_name(self):
        # reverse test name to make pod name unique (it has limited length)
        return "_" + unittest.TestCase.id(self).replace(".", "_")[::-1]

    def setUp(self):
        self.maxDiff = None
        self.api_client = ApiClient()
        self.expected_pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": mock.ANY,
                "annotations": {},
                "labels": {
                    "foo": "bar",
                    "kubernetes_pod_operator": "True",
                    "airflow_version": airflow_version.replace("+", "-"),
                    "airflow_kpo_in_cluster": "False",
                    "run_id": "manual__2016-01-01T0100000100-da4d1ce7b",
                    "dag_id": "dag",
                    "task_id": "task",
                    "try_number": "1",
                },
            },
            "spec": {
                "affinity": {},
                "containers": [
                    {
                        "image": "ubuntu:16.04",
                        "args": ["echo 10"],
                        "command": ["bash", "-cx"],
                        "env": [],
                        "envFrom": [],
                        "name": "base",
                        "ports": [],
                        "volumeMounts": [],
                    }
                ],
                "hostNetwork": False,
                "imagePullSecrets": [],
                "initContainers": [],
                "nodeSelector": {},
                "restartPolicy": "Never",
                "securityContext": {},
                "tolerations": [],
                "volumes": [],
            },
        }

    def tearDown(self):
        hook = KubernetesHook(conn_id=None, in_cluster=False)
        client = hook.core_v1_client
        client.delete_collection_namespaced_pod(namespace="default")

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch(HOOK_CLASS, new=MagicMock)
    def test_image_pull_secrets_correctly_set(self, await_pod_completion_mock, create_mock):
        fake_pull_secrets = "fakeSecret"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            image_pull_secrets=fake_pull_secrets,
            cluster_context="default",
        )
        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        await_pod_completion_mock.return_value = mock_pod
        context = create_context(k)
        k.execute(context=context)
        assert create_mock.call_args[1]["pod"].spec.image_pull_secrets == [
            k8s.V1LocalObjectReference(name=fake_pull_secrets)
        ]

    def test_working_pod(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod["spec"] == actual_pod["spec"]
        assert self.expected_pod["metadata"]["labels"] == actual_pod["metadata"]["labels"]

    def test_pod_node_selector(self):
        node_selector = {"beta.kubernetes.io/os": "linux"}
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            node_selector=node_selector,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["nodeSelector"] = node_selector
        assert self.expected_pod == actual_pod

    def test_pod_affinity(self):
        affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {"key": "beta.kubernetes.io/os", "operator": "In", "values": ["linux"]}
                            ]
                        }
                    ]
                }
            }
        }
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            affinity=affinity,
        )
        context = create_context(k)
        k.execute(context=context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["affinity"] = affinity
        assert self.expected_pod == actual_pod

    def test_run_as_user_root(self):
        security_context = {
            "securityContext": {
                "runAsUser": 0,
            }
        }
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["securityContext"] = security_context
        assert self.expected_pod == actual_pod

    def test_run_as_user_non_root(self):
        security_context = {
            "securityContext": {
                "runAsUser": 1000,
            }
        }

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["securityContext"] = security_context
        assert self.expected_pod == actual_pod

    def test_fs_group(self):
        security_context = {
            "securityContext": {
                "fsGroup": 1000,
            }
        }

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["securityContext"] = security_context
        assert self.expected_pod == actual_pod

    def test_faulty_service_account(self):
        """pod creation should fail when service account does not exist"""
        service_account = "foobar"
        namespace = "default"
        k = KubernetesPodOperator(
            namespace=namespace,
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
            service_account_name=service_account,
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        with pytest.raises(
            ApiException, match=f"error looking up service account {namespace}/{service_account}"
        ):
            k.get_or_create_pod(pod, context)

    def test_pod_failure(self):
        """
        Tests that the task fails when a pod reports a failure
        """
        bad_internal_command = ["foobar 10 "]
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=bad_internal_command,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        with pytest.raises(AirflowException):
            context = create_context(k)
            k.execute(context)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod["spec"]["containers"][0]["args"] = bad_internal_command
            assert self.expected_pod == actual_pod

    def test_xcom_push(self):
        return_value = '{"foo": "bar"\n, "buzz": 2}'
        args = [f"echo '{return_value}' > /airflow/xcom/return.json"]
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=args,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=True,
        )
        context = create_context(k)
        assert k.execute(context) == json.loads(return_value)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        volume = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME)
        volume_mount = self.api_client.sanitize_for_serialization(PodDefaults.VOLUME_MOUNT)
        container = self.api_client.sanitize_for_serialization(PodDefaults.SIDECAR_CONTAINER)
        self.expected_pod["spec"]["containers"][0]["args"] = args
        self.expected_pod["spec"]["containers"][0]["volumeMounts"].insert(0, volume_mount)
        self.expected_pod["spec"]["volumes"].insert(0, volume)
        self.expected_pod["spec"]["containers"].append(container)
        assert self.expected_pod == actual_pod

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch(HOOK_CLASS, new=MagicMock)
    def test_envs_from_configmaps(self, mock_monitor, mock_start):
        # GIVEN
        configmap = "test-configmap"
        # WHEN
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
            configmaps=[configmap],
        )
        # THEN
        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        mock_monitor.return_value = mock_pod
        context = create_context(k)
        k.execute(context)
        assert mock_start.call_args[1]["pod"].spec.containers[0].env_from == [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name=configmap))
        ]

    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
    @mock.patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
    @mock.patch(HOOK_CLASS, new=MagicMock)
    def test_envs_from_secrets(self, await_pod_completion_mock, create_mock):
        # GIVEN
        secret_ref = "secret_name"
        secrets = [Secret("env", None, secret_ref)]
        # WHEN
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            secrets=secrets,
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )
        # THEN

        mock_pod = MagicMock()
        mock_pod.status.phase = "Succeeded"
        await_pod_completion_mock.return_value = mock_pod
        context = create_context(k)
        k.execute(context)
        assert create_mock.call_args[1]["pod"].spec.containers[0].env_from == [
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name=secret_ref))
        ]

    def test_env_vars(self):
        # WHEN
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars={
                "ENV1": "val1",
                "ENV2": "val2",
            },
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            in_cluster=False,
            do_xcom_push=False,
        )

        context = create_context(k)
        k.execute(context)

        # THEN
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["containers"][0]["env"] = [
            {"name": "ENV1", "value": "val1"},
            {"name": "ENV2", "value": "val2"},
        ]
        assert self.expected_pod == actual_pod

    def test_pod_template_file_with_overrides_system(self):
        fixture = sys.path[0] + "/tests/kubernetes/basic_pod.yaml"
        k = KubernetesPodOperator(
            task_id="task" + self.get_current_task_name(),
            labels={"foo": "bar", "fizz": "buzz"},
            env_vars={"env_name": "value"},
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {
            "fizz": "buzz",
            "foo": "bar",
            "airflow_version": mock.ANY,
            "airflow_kpo_in_cluster": "False",
            "dag_id": "dag",
            "run_id": "manual__2016-01-01T0100000100-da4d1ce7b",
            "kubernetes_pod_operator": "True",
            "task_id": mock.ANY,
            "try_number": "1",
        }
        assert k.pod.spec.containers[0].env == [k8s.V1EnvVar(name="env_name", value="value")]
        assert result == {"hello": "world"}

    def test_init_container(self):
        # GIVEN
        volume_mounts = [
            k8s.V1VolumeMount(mount_path="/etc/foo", name="test-volume", sub_path=None, read_only=True)
        ]

        init_environments = [
            k8s.V1EnvVar(name="key1", value="value1"),
            k8s.V1EnvVar(name="key2", value="value2"),
        ]

        init_container = k8s.V1Container(
            name="init-container",
            image="ubuntu:16.04",
            env=init_environments,
            volume_mounts=volume_mounts,
            command=["bash", "-cx"],
            args=["echo 10"],
        )

        expected_init_container = {
            "name": "init-container",
            "image": "ubuntu:16.04",
            "command": ["bash", "-cx"],
            "args": ["echo 10"],
            "env": [{"name": "key1", "value": "value1"}, {"name": "key2", "value": "value2"}],
            "volumeMounts": [{"mountPath": "/etc/foo", "name": "test-volume", "readOnly": True}],
        }

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels={"foo": "bar"},
            name="test",
            task_id="task",
            init_containers=[init_container],
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        actual_pod = self.api_client.sanitize_for_serialization(pod)
        self.expected_pod["spec"]["initContainers"] = [expected_init_container]
        assert actual_pod == self.expected_pod

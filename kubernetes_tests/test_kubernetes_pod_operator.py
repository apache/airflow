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
import logging
import os
import shutil
import sys
from copy import copy
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import ANY, MagicMock
from uuid import uuid4

import pendulum
import pytest
from kubernetes.client import models as k8s
from kubernetes.client.api_client import ApiClient
from kubernetes.client.rest import ApiException
from pytest import param

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.types import DagRunType
from airflow.version import version as airflow_version

HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesHook"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"


def create_context(task) -> Context:
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.tz.timezone("Europe/Amsterdam")
    execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(
        dag_id=dag.dag_id,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
    )
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.dag_id = dag.dag_id
    task_instance.xcom_push = mock.Mock()  # type: ignore
    return Context(
        dag=dag,
        run_id=dag_run.run_id,
        task=task,
        ti=task_instance,
        task_instance=task_instance,
    )


def get_kubeconfig_path():
    kubeconfig_path = os.environ.get("KUBECONFIG")
    return kubeconfig_path if kubeconfig_path else os.path.expanduser("~/.kube/config")


def get_label():
    test = os.environ.get("PYTEST_CURRENT_TEST")
    test = test.split(" ", 1)[0]  # if invoked from setup, will have suffix ' (setup)', e.g.
    label = "".join(filter(str.isalnum, test)).lower()
    return label[-63:]


@pytest.mark.execution_timeout(180)
class TestKubernetesPodOperatorSystem:
    def setup(self):
        self.maxDiff = None
        self.api_client = ApiClient()
        self.labels = {"test_label": get_label()}
        self.expected_pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "namespace": "default",
                "name": ANY,
                "annotations": {},
                "labels": {
                    "test_label": get_label(),
                    "kubernetes_pod_operator": "True",
                    "airflow_version": airflow_version.replace("+", "-"),
                    "airflow_kpo_in_cluster": "False",
                    "run_id": "manual__2016-01-01T0100000100-da4d1ce7b",
                    "dag_id": "dag",
                    "task_id": ANY,
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

    def _get_labels_selector(self) -> str | None:
        if not self.labels:
            return None
        return ",".join([f"{key}={value}" for key, value in enumerate(self.labels)])

    def teardown(self) -> None:
        hook = KubernetesHook(conn_id=None, in_cluster=False)
        client = hook.core_v1_client
        client.delete_collection_namespaced_pod(namespace="default", grace_period_seconds=0)

    def test_do_xcom_push_defaults_false(self):
        with NamedTemporaryFile(prefix="kube_config", suffix=".cfg") as f:
            new_config_path = f.name
        old_config_path = get_kubeconfig_path()
        shutil.copy(old_config_path, new_config_path)
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            config_file=new_config_path,
        )
        assert not k.do_xcom_push

    def test_config_path_move(self):
        with NamedTemporaryFile(prefix="kube_config", suffix=".cfg") as f:
            new_config_path = f.name
        old_config_path = get_kubeconfig_path()
        shutil.copy(old_config_path, new_config_path)

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=False,
            config_file=new_config_path,
        )
        context = create_context(k)
        k.execute(context)
        expected_pod = copy(self.expected_pod)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert actual_pod == expected_pod

    def test_working_pod(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod["spec"] == actual_pod["spec"]
        assert self.expected_pod["metadata"]["labels"] == actual_pod["metadata"]["labels"]

    def test_delete_operator_pod(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=True,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        assert self.expected_pod["spec"] == actual_pod["spec"]
        assert self.expected_pod["metadata"]["labels"] == actual_pod["metadata"]["labels"]

    def test_already_checked_on_success(self):
        """
        When ``is_delete_operator_pod=False``, pod should have 'already_checked'
        label, whether pod is successful or not.
        """
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = k.find_pod("default", context, exclude_checked=False)
        actual_pod = self.api_client.sanitize_for_serialization(actual_pod)
        assert actual_pod["metadata"]["labels"]["already_checked"] == "True"

    def test_already_checked_on_failure(self):
        """
        When ``is_delete_operator_pod=False``, pod should have 'already_checked'
        label, whether pod is successful or not.
        """
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["lalala"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            is_delete_operator_pod=False,
        )
        context = create_context(k)
        with pytest.raises(AirflowException):
            k.execute(context)
        actual_pod = k.find_pod("default", context, exclude_checked=False)
        actual_pod = self.api_client.sanitize_for_serialization(actual_pod)
        status = next(iter(filter(lambda x: x["name"] == "base", actual_pod["status"]["containerStatuses"])))
        assert status["state"]["terminated"]["reason"] == "Error"
        assert actual_pod["metadata"]["labels"]["already_checked"] == "True"

    def test_pod_hostnetwork(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            hostnetwork=True,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["hostNetwork"] = True
        assert self.expected_pod["spec"] == actual_pod["spec"]
        assert self.expected_pod["metadata"]["labels"] == actual_pod["metadata"]["labels"]

    def test_pod_dnspolicy(self):
        dns_policy = "ClusterFirstWithHostNet"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            hostnetwork=True,
            dnspolicy=dns_policy,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["hostNetwork"] = True
        self.expected_pod["spec"]["dnsPolicy"] = dns_policy
        assert self.expected_pod["spec"] == actual_pod["spec"]
        assert self.expected_pod["metadata"]["labels"] == actual_pod["metadata"]["labels"]

    def test_pod_schedulername(self):
        scheduler_name = "default-scheduler"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            schedulername=scheduler_name,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["schedulerName"] = scheduler_name
        assert self.expected_pod == actual_pod

    def test_pod_node_selector(self):
        node_selector = {"beta.kubernetes.io/os": "linux"}
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            node_selector=node_selector,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["nodeSelector"] = node_selector
        assert self.expected_pod == actual_pod

    def test_pod_resources(self):
        resources = k8s.V1ResourceRequirements(
            requests={"memory": "64Mi", "cpu": "250m", "ephemeral-storage": "1Gi"},
            limits={"memory": "64Mi", "cpu": 0.25, "nvidia.com/gpu": None, "ephemeral-storage": "2Gi"},
        )
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            container_resources=resources,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["containers"][0]["resources"] = {
            "requests": {"memory": "64Mi", "cpu": "250m", "ephemeral-storage": "1Gi"},
            "limits": {"memory": "64Mi", "cpu": 0.25, "nvidia.com/gpu": None, "ephemeral-storage": "2Gi"},
        }
        assert self.expected_pod == actual_pod

    @pytest.mark.parametrize(
        "val",
        [
            param(
                k8s.V1Affinity(
                    node_affinity=k8s.V1NodeAffinity(
                        required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                            node_selector_terms=[
                                k8s.V1NodeSelectorTerm(
                                    match_expressions=[
                                        k8s.V1NodeSelectorRequirement(
                                            key="beta.kubernetes.io/os",
                                            operator="In",
                                            values=["linux"],
                                        )
                                    ]
                                )
                            ]
                        )
                    )
                ),
                id="current",
            ),
            param(
                {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {
                                            "key": "beta.kubernetes.io/os",
                                            "operator": "In",
                                            "values": ["linux"],
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                },
                id="backcompat",
            ),
        ],
    )
    def test_pod_affinity(self, val):
        expected = {
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
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            affinity=val,
        )
        context = create_context(k)
        k.execute(context=context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["affinity"] = expected
        assert self.expected_pod == actual_pod

    def test_port(self):
        port = k8s.V1ContainerPort(
            name="http",
            container_port=80,
        )

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            ports=[port],
        )
        context = create_context(k)
        k.execute(context=context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["containers"][0]["ports"] = [{"name": "http", "containerPort": 80}]
        assert self.expected_pod == actual_pod

    def test_volume_mount(self):
        with mock.patch.object(PodManager, "log") as mock_logger:
            volume_mount = k8s.V1VolumeMount(
                name="test-volume", mount_path="/tmp/test_volume", sub_path=None, read_only=False
            )

            volume = k8s.V1Volume(
                name="test-volume",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume"),
            )

            args = [
                'echo "retrieved from mount" > /tmp/test_volume/test.txt && cat /tmp/test_volume/test.txt'
            ]
            k = KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=args,
                labels=self.labels,
                volume_mounts=[volume_mount],
                volumes=[volume],
                task_id=str(uuid4()),
                in_cluster=False,
                do_xcom_push=False,
            )
            context = create_context(k)
            k.execute(context=context)
            mock_logger.info.assert_any_call("retrieved from mount")
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod["spec"]["containers"][0]["args"] = args
            self.expected_pod["spec"]["containers"][0]["volumeMounts"] = [
                {"name": "test-volume", "mountPath": "/tmp/test_volume", "readOnly": False}
            ]
            self.expected_pod["spec"]["volumes"] = [
                {"name": "test-volume", "persistentVolumeClaim": {"claimName": "test-volume"}}
            ]
            assert self.expected_pod == actual_pod

    @pytest.mark.parametrize("uid", [0, 1000])
    def test_run_as_user(self, uid):
        security_context = {"runAsUser": uid}
        name = str(uuid4())
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id=name,
            name=name,
            random_name_suffix=False,
            is_delete_operator_pod=False,
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        pod = k.hook.core_v1_client.read_namespaced_pod(
            name=name,
            namespace="default",
        )
        assert pod.to_dict()["spec"]["security_context"]["run_as_user"] == uid

    @pytest.mark.parametrize("gid", [0, 1000])
    def test_fs_group(self, gid):
        security_context = {"fsGroup": gid}
        name = str(uuid4())
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            task_id=name,
            name=name,
            random_name_suffix=False,
            is_delete_operator_pod=False,
            in_cluster=False,
            do_xcom_push=False,
            security_context=security_context,
        )
        context = create_context(k)
        k.execute(context)
        pod = k.hook.core_v1_client.read_namespaced_pod(
            name=name,
            namespace="default",
        )
        assert pod.to_dict()["spec"]["security_context"]["fs_group"] == gid

    def test_disable_privilege_escalation(self):
        container_security_context = {"allowPrivilegeEscalation": False}

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            container_security_context=container_security_context,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["containers"][0]["securityContext"] = container_security_context
        assert self.expected_pod == actual_pod

    def test_faulty_image(self):
        bad_image_name = "foobar"
        k = KubernetesPodOperator(
            namespace="default",
            image=bad_image_name,
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
        )
        with pytest.raises(AirflowException):
            context = create_context(k)
            k.execute(context)
            actual_pod = self.api_client.sanitize_for_serialization(k.pod)
            self.expected_pod["spec"]["containers"][0]["image"] = bad_image_name
            assert self.expected_pod == actual_pod

    def test_faulty_service_account(self):
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            startup_timeout_seconds=5,
            service_account_name="foobar",
        )
        context = create_context(k)
        pod = k.build_pod_request_obj(context)
        with pytest.raises(ApiException, match="error looking up service account default/foobar"):
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
            labels=self.labels,
            task_id=str(uuid4()),
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
        expected = {"test_label": get_label(), "buzz": 2}
        args = [f"echo '{json.dumps(expected)}' > /airflow/xcom/return.json"]
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=args,
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=True,
        )
        context = create_context(k)
        assert k.execute(context) == expected

    def test_env_vars(self):
        # WHEN
        env_vars = [
            k8s.V1EnvVar(name="ENV1", value="val1"),
            k8s.V1EnvVar(name="ENV2", value="val2"),
            k8s.V1EnvVar(
                name="ENV3",
                value_from=k8s.V1EnvVarSource(field_ref=k8s.V1ObjectFieldSelector(field_path="status.podIP")),
            ),
        ]

        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            env_vars=env_vars,
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
        )
        # THEN
        context = create_context(k)
        actual_pod = self.api_client.sanitize_for_serialization(k.build_pod_request_obj(context))
        self.expected_pod["spec"]["containers"][0]["env"] = [
            {"name": "ENV1", "value": "val1"},
            {"name": "ENV2", "value": "val2"},
            {"name": "ENV3", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
        ]
        assert self.expected_pod == actual_pod

    def test_pod_template_file_system(self):
        """Note: this test requires that you have a namespace ``mem-example`` in your cluster."""
        fixture = sys.path[0] + "/tests/kubernetes/basic_pod.yaml"
        k = KubernetesPodOperator(
            task_id=str(uuid4()),
            in_cluster=False,
            labels=self.labels,
            pod_template_file=fixture,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert result == {"hello": "world"}

    @pytest.mark.parametrize(
        "input",
        [
            param([k8s.V1EnvVar(name="env_name", value="value")], id="current"),
            param({"env_name": "value"}, id="backcompat"),  # todo: remove?
        ],
    )
    def test_pod_template_file_with_overrides_system(self, input):
        fixture = sys.path[0] + "/tests/kubernetes/basic_pod.yaml"
        k = KubernetesPodOperator(
            task_id=str(uuid4()),
            labels=self.labels,
            env_vars=[k8s.V1EnvVar(name="env_name", value="value")],
            in_cluster=False,
            pod_template_file=fixture,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {
            "test_label": get_label(),
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

    def test_pod_template_file_with_full_pod_spec(self):
        fixture = sys.path[0] + "/tests/kubernetes/basic_pod.yaml"
        pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={"test_label": get_label(), "fizz": "buzz"},
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        env=[k8s.V1EnvVar(name="env_name", value="value")],
                    )
                ]
            ),
        )
        k = KubernetesPodOperator(
            task_id=str(uuid4()),
            labels=self.labels,
            in_cluster=False,
            pod_template_file=fixture,
            full_pod_spec=pod_spec,
            do_xcom_push=True,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {
            "fizz": "buzz",
            "test_label": get_label(),
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

    def test_full_pod_spec(self):
        pod_spec = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={"test_label": get_label(), "fizz": "buzz"}, namespace="default", name="test-pod"
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="perl",
                        command=["/bin/bash"],
                        args=["-c", 'echo {\\"hello\\" : \\"world\\"} | cat > /airflow/xcom/return.json'],
                        env=[k8s.V1EnvVar(name="env_name", value="value")],
                    )
                ],
                restart_policy="Never",
            ),
        )
        k = KubernetesPodOperator(
            task_id=str(uuid4()),
            in_cluster=False,
            labels=self.labels,
            full_pod_spec=pod_spec,
            do_xcom_push=True,
            is_delete_operator_pod=False,
            startup_timeout_seconds=30,
        )

        context = create_context(k)
        result = k.execute(context)
        assert result is not None
        assert k.pod.metadata.labels == {
            "fizz": "buzz",
            "test_label": get_label(),
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

        volume = k8s.V1Volume(
            name="test-volume",
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-volume"),
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
            labels=self.labels,
            task_id=str(uuid4()),
            volumes=[volume],
            init_containers=[init_container],
            in_cluster=False,
            do_xcom_push=False,
        )
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["initContainers"] = [expected_init_container]
        self.expected_pod["spec"]["volumes"] = [
            {"name": "test-volume", "persistentVolumeClaim": {"claimName": "test-volume"}}
        ]
        assert self.expected_pod == actual_pod

    @mock.patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start")
    @mock.patch(f"{POD_MANAGER_CLASS}.extract_xcom")
    @mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    @mock.patch(f"{POD_MANAGER_CLASS}.create_pod", new=MagicMock)
    @mock.patch(HOOK_CLASS)
    def test_pod_template_file(
        self,
        hook_mock,
        await_pod_completion_mock,
        extract_xcom_mock,
        await_xcom_sidecar_container_start_mock,
        caplog,
    ):
        # todo: This isn't really a system test
        await_xcom_sidecar_container_start_mock.return_value = None
        hook_mock.return_value.is_in_cluster = False
        hook_mock.return_value.get_xcom_sidecar_container_image.return_value = None
        extract_xcom_mock.return_value = "{}"
        path = sys.path[0] + "/tests/kubernetes/pod.yaml"
        k = KubernetesPodOperator(
            task_id=str(uuid4()),
            labels=self.labels,
            random_name_suffix=False,
            pod_template_file=path,
            do_xcom_push=True,
        )
        pod_mock = MagicMock()
        pod_mock.status.phase = "Succeeded"
        await_pod_completion_mock.return_value = pod_mock
        context = create_context(k)

        # I'm not really sure what the point is of this assert
        with caplog.at_level(logging.DEBUG, logger="airflow.task.operators"):
            k.execute(context)
            expected_lines = [
                "Starting pod:",
                "api_version: v1",
                "kind: Pod",
                "metadata:",
                "  annotations: {}",
                "  cluster_name: null",
                "  creation_timestamp: null",
                "  deletion_grace_period_seconds: null",
            ]
            actual = [x.getMessage() for x in caplog.records if x.msg == "Starting pod:\n%s"][0].splitlines()
            assert actual[: len(expected_lines)] == expected_lines

        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        expected_dict = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "annotations": {},
                "labels": {
                    "test_label": get_label(),
                    "airflow_kpo_in_cluster": "False",
                    "dag_id": "dag",
                    "run_id": "manual__2016-01-01T0100000100-da4d1ce7b",
                    "kubernetes_pod_operator": "True",
                    "task_id": mock.ANY,
                    "try_number": "1",
                },
                "name": "memory-demo",
                "namespace": "mem-example",
            },
            "spec": {
                "affinity": {},
                "containers": [
                    {
                        "args": ["--vm", "1", "--vm-bytes", "150M", "--vm-hang", "1"],
                        "command": ["stress"],
                        "env": [],
                        "envFrom": [],
                        "image": "ghcr.io/apache/airflow-stress:1.0.4-2021.07.04",
                        "name": "base",
                        "ports": [],
                        "resources": {"limits": {"memory": "200Mi"}, "requests": {"memory": "100Mi"}},
                        "volumeMounts": [{"mountPath": "/airflow/xcom", "name": "xcom"}],
                    },
                    {
                        "command": ["sh", "-c", 'trap "exit 0" INT; while true; do sleep 1; done;'],
                        "image": "alpine",
                        "name": "airflow-xcom-sidecar",
                        "resources": {"requests": {"cpu": "1m"}},
                        "volumeMounts": [{"mountPath": "/airflow/xcom", "name": "xcom"}],
                    },
                ],
                "hostNetwork": False,
                "imagePullSecrets": [],
                "initContainers": [],
                "nodeSelector": {},
                "restartPolicy": "Never",
                "securityContext": {},
                "tolerations": [],
                "volumes": [{"emptyDir": {}, "name": "xcom"}],
            },
        }
        version = actual_pod["metadata"]["labels"]["airflow_version"]
        assert version.startswith(airflow_version)
        del actual_pod["metadata"]["labels"]["airflow_version"]
        assert expected_dict == actual_pod

    @mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
    @mock.patch(f"{POD_MANAGER_CLASS}.create_pod", new=MagicMock)
    @mock.patch(HOOK_CLASS)
    def test_pod_priority_class_name(self, hook_mock, await_pod_completion_mock):
        """
        Test ability to assign priorityClassName to pod

        todo: This isn't really a system test
        """
        hook_mock.return_value.is_in_cluster = False

        priority_class_name = "medium-test"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["echo 10"],
            labels=self.labels,
            task_id=str(uuid4()),
            in_cluster=False,
            do_xcom_push=False,
            priority_class_name=priority_class_name,
        )

        pod_mock = MagicMock()
        pod_mock.status.phase = "Succeeded"
        await_pod_completion_mock.return_value = pod_mock
        context = create_context(k)
        k.execute(context)
        actual_pod = self.api_client.sanitize_for_serialization(k.pod)
        self.expected_pod["spec"]["priorityClassName"] = priority_class_name
        assert self.expected_pod == actual_pod

    def test_pod_name(self):
        pod_name_too_long = "a" * 221
        with pytest.raises(AirflowException):
            KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["echo 10"],
                labels=self.labels,
                name=pod_name_too_long,
                task_id=str(uuid4()),
                in_cluster=False,
                do_xcom_push=False,
            )

    @mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion", new=MagicMock)
    def test_on_kill(self):
        hook = KubernetesHook(conn_id=None, in_cluster=False)
        client = hook.core_v1_client
        name = "test"
        namespace = "default"
        k = KubernetesPodOperator(
            namespace="default",
            image="ubuntu:16.04",
            cmds=["bash", "-cx"],
            arguments=["sleep 1000"],
            labels=self.labels,
            name=name,
            task_id=name,
            in_cluster=False,
            do_xcom_push=False,
            get_logs=False,
            termination_grace_period=0,
        )
        context = create_context(k)
        with pytest.raises(AirflowException):
            k.execute(context)
        name = k.pod.metadata.name
        pod = client.read_namespaced_pod(name=name, namespace=namespace)
        assert pod.status.phase == "Running"
        k.on_kill()
        with pytest.raises(ApiException, match=r'pods \\"test.[a-z0-9]+\\" not found'):
            client.read_namespaced_pod(name=name, namespace=namespace)

    def test_reattach_failing_pod_once(self):
        hook = KubernetesHook(conn_id=None, in_cluster=False)
        client = hook.core_v1_client
        name = "test"
        namespace = "default"

        def get_op():
            return KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["exit 1"],
                labels=self.labels,
                name="test",
                task_id=name,
                in_cluster=False,
                do_xcom_push=False,
                is_delete_operator_pod=False,
                termination_grace_period=0,
            )

        k = get_op()

        context = create_context(k)

        # launch pod
        with mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion") as await_pod_completion_mock:
            pod_mock = MagicMock()

            pod_mock.status.phase = "Succeeded"
            await_pod_completion_mock.return_value = pod_mock

            # we want to simulate that there was a worker failure and the airflow operator process
            # was killed without running the cleanup process.  in this case the pod will not be marked as
            # already checked
            k.cleanup = MagicMock()

            k.execute(context)
            name = k.pod.metadata.name
            pod = client.read_namespaced_pod(name=name, namespace=namespace)
            while pod.status.phase != "Failed":
                pod = client.read_namespaced_pod(name=name, namespace=namespace)
            assert "already_checked" not in pod.metadata.labels

        # create a new version of the same operator instance to remove the monkey patching in first
        # part of the test
        k = get_op()

        # `create_pod` should not be called because there's a pod there it should find
        # should use the found pod and patch as "already_checked" (in failure block)
        with mock.patch(f"{POD_MANAGER_CLASS}.create_pod") as create_mock:
            with pytest.raises(AirflowException):
                k.execute(context)
            pod = client.read_namespaced_pod(name=name, namespace=namespace)
            assert pod.metadata.labels["already_checked"] == "True"
            create_mock.assert_not_called()

        # recreate op just to ensure we're not relying on any statefulness
        k = get_op()

        # `create_pod` should be called because though there's still a pod to be found,
        # it will be `already_checked`
        with mock.patch(f"{POD_MANAGER_CLASS}.create_pod") as create_mock:
            with pytest.raises(AirflowException):
                k.execute(context)
            create_mock.assert_called_once()

    def test_using_resources(self):
        exception_message = (
            "Specifying resources for the launched pod with 'resources' is deprecated. "
            "Use 'container_resources' instead."
        )
        with pytest.raises(AirflowException, match=exception_message):
            resources = k8s.V1ResourceRequirements(
                requests={"memory": "64Mi", "cpu": "250m", "ephemeral-storage": "1Gi"},
                limits={"memory": "64Mi", "cpu": 0.25, "nvidia.com/gpu": None, "ephemeral-storage": "2Gi"},
            )
            KubernetesPodOperator(
                namespace="default",
                image="ubuntu:16.04",
                cmds=["bash", "-cx"],
                arguments=["echo 10"],
                labels=self.labels,
                task_id=str(uuid4()),
                in_cluster=False,
                do_xcom_push=False,
                resources=resources,
            )

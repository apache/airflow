#
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

import asyncio
import copy
import json
from datetime import date
from functools import cached_property
from unittest import mock
from unittest.mock import mock_open, patch
from uuid import uuid4

import pendulum
import pytest
import pytest_asyncio
import yaml
from kubernetes.client import models as k8s

from airflow import DAG
from airflow.models import Connection, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.pod_generator import MAX_LABEL_LEN
from airflow.providers.cncf.kubernetes.triggers.pod import KubernetesPodTrigger
from airflow.providers.common.compat.sdk import TaskDeferred
from airflow.utils import timezone
from airflow.utils.types import DagRunType

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"


@pytest_asyncio.fixture(autouse=True, scope="function")
async def patch_pod_manager_methods():
    # Patch watch_pod_events
    patch_watch_pod_events = mock.patch(f"{POD_MANAGER_CLASS}.watch_pod_events", new_callable=mock.AsyncMock)
    mock_watch_pod_events = patch_watch_pod_events.start()
    mock_watch_pod_events.return_value = asyncio.Future()
    mock_watch_pod_events.return_value.set_result(None)

    # Patch await_pod_start
    patch_await_pod_start = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start", new_callable=mock.AsyncMock)
    mock_await_pod_start = patch_await_pod_start.start()
    mock_await_pod_start.return_value = asyncio.Future()
    mock_await_pod_start.return_value.set_result(None)

    yield

    mock.patch.stopall()


def _get_expected_k8s_dict():
    """Create expected K8S dict on-demand."""
    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": "default_yaml_template", "namespace": "default"},
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": "gcr.io/spark-operator/spark:v2.4.5",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/test.py",
            "sparkVersion": "3.0.0",
            "restartPolicy": {"type": "Never"},
            "successfulRunHistoryLimit": 1,
            "pythonVersion": "3",
            "volumes": [],
            "labels": {},
            "imagePullSecrets": "",
            "hadoopConf": {},
            "dynamicAllocation": {
                "enabled": False,
                "initialExecutors": 1,
                "maxExecutors": 1,
                "minExecutors": 1,
            },
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "365m",
                "labels": {},
                "nodeSelector": {},
                "serviceAccount": "default",
                "volumeMounts": [],
                "env": [],
                "envFrom": [],
                "tolerations": [],
                "affinity": {"nodeAffinity": {}, "podAffinity": {}, "podAntiAffinity": {}},
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "365m",
                "labels": {},
                "env": [],
                "envFrom": [],
                "nodeSelector": {},
                "volumeMounts": [],
                "tolerations": [],
                "affinity": {"nodeAffinity": {}, "podAffinity": {}, "podAntiAffinity": {}},
            },
        },
    }


def _get_expected_application_dict_with_labels(task_name="default_yaml"):
    """Create expected application dict with task context labels on-demand."""
    task_context_labels = {
        "dag_id": "dag",
        "task_id": task_name,
        "run_id": "manual__2016-01-01T0100000100-da4d1ce7b",
        "spark_kubernetes_operator": "True",
        "try_number": "0",
        "version": "2.4.5",
    }

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": task_name, "namespace": "default"},
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "image": "gcr.io/spark-operator/spark:v2.4.5",
            "imagePullPolicy": "Always",
            "mainClass": "org.apache.spark.examples.SparkPi",
            "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar",
            "sparkVersion": "2.4.5",
            "restartPolicy": {"type": "Never"},
            "volumes": [{"name": "test-volume", "hostPath": {"path": "/tmp", "type": "Directory"}}],
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "512m",
                "labels": task_context_labels.copy(),
                "serviceAccount": "spark",
                "volumeMounts": [{"name": "test-volume", "mountPath": "/tmp"}],
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "512m",
                "labels": task_context_labels.copy(),
                "volumeMounts": [{"name": "test-volume", "mountPath": "/tmp"}],
            },
        },
    }


def _get_expected_application_dict_without_task_context_labels(task_name="default_yaml"):
    """Create expected application dict without task context labels (only original file labels)."""
    original_file_labels = {
        "version": "2.4.5",
    }

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": task_name, "namespace": "default"},
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "image": "gcr.io/spark-operator/spark:v2.4.5",
            "imagePullPolicy": "Always",
            "mainClass": "org.apache.spark.examples.SparkPi",
            "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar",
            "sparkVersion": "2.4.5",
            "restartPolicy": {"type": "Never"},
            "volumes": [{"name": "test-volume", "hostPath": {"path": "/tmp", "type": "Directory"}}],
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "512m",
                "labels": original_file_labels.copy(),
                "serviceAccount": "spark",
                "volumeMounts": [{"name": "test-volume", "mountPath": "/tmp"}],
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "512m",
                "labels": original_file_labels.copy(),
                "volumeMounts": [{"name": "test-volume", "mountPath": "/tmp"}],
            },
        },
    }


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator(mock_kubernetes_hook, data_file):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file=data_file("spark/application_test.yaml").as_posix(),
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )
    mock_kubernetes_hook.assert_not_called()  # constructor shouldn't call the hook

    assert "hook" not in operator.__dict__  # Cached property has not been accessed as part of construction.


def test_init_spark_kubernetes_operator(data_file):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file=data_file("spark/application_test.yaml").as_posix(),
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
        base_container_name="base",
        get_logs=True,
    )
    assert operator.base_container_name == "spark-kubernetes-driver"
    assert operator.container_logs == ["spark-kubernetes-driver"]


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator_hook(mock_kubernetes_hook, data_file):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file=data_file("spark/application_test.yaml").as_posix(),
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )
    operator.hook
    mock_kubernetes_hook.assert_called_with(
        conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )


def create_context(task):
    dag = DAG(dag_id="dag", schedule=None)
    tzinfo = pendulum.timezone("Europe/Amsterdam")
    logical_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    if AIRFLOW_V_3_0_PLUS:
        dag_run = DagRun(
            dag_id=dag.dag_id,
            logical_date=logical_date,
            run_id=DagRun.generate_run_id(
                run_type=DagRunType.MANUAL, logical_date=logical_date, run_after=logical_date
            ),
        )
    else:
        dag_run = DagRun(
            dag_id=dag.dag_id,
            execution_date=logical_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, logical_date),
        )
    if AIRFLOW_V_3_0_PLUS:
        from uuid6 import uuid7

        task_instance = TaskInstance(task=task, dag_version_id=uuid7())
    else:
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


@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_requested_container_logs")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator.client")
@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.cleanup")
@patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object_status")
@patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
class TestSparkKubernetesOperatorCreateApplication:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(conn_id="kubernetes_default_kube_config", conn_type="kubernetes", extra=json.dumps({}))
        )
        create_connection_without_db(
            Connection(
                conn_id="kubernetes_with_namespace",
                conn_type="kubernetes",
                extra=json.dumps({"extra__kubernetes__namespace": "mock_namespace"}),
            )
        )
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    def execute_operator(
        self,
        task_name,
        *,
        name=None,
        job_spec=None,
        application_file=None,
        random_name_suffix=False,
    ):
        op = SparkKubernetesOperator(
            name=name,
            task_id=task_name,
            random_name_suffix=random_name_suffix,
            application_file=application_file,
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            reattach_on_restart=False,  # Disable reattach for application creation tests
        )
        context = create_context(op)
        op.execute(context)
        return op

    @cached_property
    def call_commons(self):
        return {
            "group": "sparkoperator.k8s.io",
            "namespace": "default",
            "plural": "sparkapplications",
            "version": "v1beta2",
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("task_name", "application_file_path"),
        [
            ("default_yaml", "spark/application_test.yaml"),
            ("default_json", "spark/application_test.json"),
        ],
    )
    @pytest.mark.parametrize("random_name_suffix", [True, False])
    def test_create_application(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
        task_name,
        application_file_path,
        random_name_suffix,
    ):
        done_op = self.execute_operator(
            task_name=task_name,
            application_file=data_file(application_file_path).as_posix(),
            random_name_suffix=random_name_suffix,
        )
        assert done_op.task_id == task_name
        # The name generation is out of scope of this test, so we don't set any
        # expectations on the name. We just check that the name is set.
        assert isinstance(done_op.name, str)
        assert done_op.name != ""

        expected_dict = _get_expected_application_dict_without_task_context_labels(task_name)
        expected_dict["metadata"]["name"] = done_op.name
        mock_create_namespaced_crd.assert_called_with(
            body=expected_dict,
            **self.call_commons,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("task_name", "application_file_path"),
        [
            ("default_yaml", "spark/application_test.yaml"),
            ("default_json", "spark/application_test.json"),
        ],
    )
    @pytest.mark.parametrize("random_name_suffix", [True, False])
    def test_create_application_and_use_name_from_operator_args(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
        task_name,
        application_file_path,
        random_name_suffix,
    ):
        name = f"test-name-{task_name}"
        done_op = self.execute_operator(
            task_name=task_name,
            name=name,
            application_file=data_file(application_file_path).as_posix(),
            random_name_suffix=random_name_suffix,
        )

        name_normalized = name.replace("_", "-")
        assert done_op.task_id == task_name
        assert isinstance(done_op.name, str)
        if random_name_suffix:
            assert done_op.name.startswith(name_normalized)
        else:
            assert done_op.name == name_normalized

        expected_dict = _get_expected_application_dict_without_task_context_labels(task_name)
        expected_dict["metadata"]["name"] = done_op.name
        mock_create_namespaced_crd.assert_called_with(
            body=expected_dict,
            **self.call_commons,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("task_name", "application_file_path"),
        [
            ("task_id_yml", "spark/application_test_with_no_name_from_config.yaml"),
            ("task_id_json", "spark/application_test_with_no_name_from_config.json"),
        ],
    )
    @pytest.mark.parametrize("random_name_suffix", [True, False])
    def test_create_application_and_use_name_task_id(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
        task_name,
        application_file_path,
        random_name_suffix,
    ):
        done_op = self.execute_operator(
            task_name=task_name,
            application_file=data_file(application_file_path).as_posix(),
            random_name_suffix=random_name_suffix,
        )

        name_normalized = task_name.replace("_", "-")
        assert isinstance(done_op.name, str)
        if random_name_suffix:
            assert done_op.name.startswith(name_normalized)
        else:
            assert done_op.name == name_normalized

        expected_dict = _get_expected_application_dict_without_task_context_labels(task_name)
        expected_dict["metadata"]["name"] = done_op.name
        mock_create_namespaced_crd.assert_called_with(
            body=expected_dict,
            **self.call_commons,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("random_name_suffix", [True, False])
    def test_new_template_from_yaml(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
        random_name_suffix,
    ):
        task_name = "default_yaml_template"
        done_op = self.execute_operator(
            task_name=task_name,
            application_file=data_file("spark/application_template.yaml").as_posix(),
            random_name_suffix=random_name_suffix,
        )

        name_normalized = task_name.replace("_", "-")
        assert isinstance(done_op.name, str)
        if random_name_suffix:
            assert done_op.name.startswith(name_normalized)
        else:
            assert done_op.name == name_normalized

        expected_dict = _get_expected_k8s_dict()
        expected_dict["metadata"]["name"] = done_op.name
        mock_create_namespaced_crd.assert_called_with(
            body=expected_dict,
            **self.call_commons,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("random_name_suffix", [True, False])
    def test_template_spec(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
        random_name_suffix,
    ):
        task_name = "default_yaml_template"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())
        done_op = self.execute_operator(
            task_name=task_name,
            job_spec=job_spec,
            random_name_suffix=random_name_suffix,
        )

        name_normalized = task_name.replace("_", "-")
        assert isinstance(done_op.name, str)
        if random_name_suffix:
            assert done_op.name.startswith(name_normalized)
        else:
            assert done_op.name == name_normalized

        expected_dict = _get_expected_k8s_dict()
        expected_dict["metadata"]["name"] = done_op.name
        mock_create_namespaced_crd.assert_called_with(
            body=expected_dict,
            **self.call_commons,
        )


@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.fetch_requested_container_logs")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator.client")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator.create_job_name")
@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.cleanup")
@patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object_status")
@patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
@patch("airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator.execute", return_value=None)
@patch(
    "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.is_in_cluster",
    new_callable=mock.PropertyMock,
    return_value=False,
)
class TestSparkKubernetesOperator:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(conn_id="kubernetes_default_kube_config", conn_type="kubernetes", extra=json.dumps({}))
        )
        create_connection_without_db(
            Connection(
                conn_id="kubernetes_with_namespace",
                conn_type="kubernetes",
                extra=json.dumps({"extra__kubernetes__namespace": "mock_namespace"}),
            )
        )
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    def execute_operator(self, task_name, mock_create_job_name, job_spec, mock_get_kube_client=None):
        mock_create_job_name.return_value = task_name

        if mock_get_kube_client:
            mock_get_kube_client.list_namespaced_pod.return_value.items = []

        op = SparkKubernetesOperator(
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
            get_logs=True,
            reattach_on_restart=False,  # Disable reattach for basic tests
        )
        context = create_context(op)
        op.execute(context)
        return op

    def test_env(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "default_env"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())
        # test env vars
        job_spec["kubernetes"]["env_vars"] = {"TEST_ENV_1": "VALUE1"}

        env_from = [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="env-direct-configmap")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="env-direct-secret")),
        ]
        job_spec["kubernetes"]["env_from"] = copy.deepcopy(env_from)

        job_spec["kubernetes"]["from_env_config_map"] = ["env-from-configmap"]
        job_spec["kubernetes"]["from_env_secret"] = ["env-from-secret"]

        op = self.execute_operator(
            task_name, mock_create_job_name, job_spec=job_spec, mock_get_kube_client=mock_get_kube_client
        )
        assert op.launcher.body["spec"]["driver"]["env"] == [
            k8s.V1EnvVar(name="TEST_ENV_1", value="VALUE1"),
        ]
        assert op.launcher.body["spec"]["executor"]["env"] == [
            k8s.V1EnvVar(name="TEST_ENV_1", value="VALUE1"),
        ]

        env_from = env_from + [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="env-from-configmap")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="env-from-secret")),
        ]
        assert op.launcher.body["spec"]["driver"]["envFrom"] == env_from
        assert op.launcher.body["spec"]["executor"]["envFrom"] == env_from

    @pytest.mark.asyncio
    def test_volume(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "default_volume"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())
        volumes = [
            k8s.V1Volume(
                name="test-pvc",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="test-pvc"),
            ),
            k8s.V1Volume(
                name="test-configmap-mount",
                config_map=k8s.V1ConfigMapVolumeSource(name="test-configmap-mount"),
            ),
        ]
        volume_mounts = [
            k8s.V1VolumeMount(mount_path="/pvc-path", name="test-pvc"),
            k8s.V1VolumeMount(mount_path="/configmap-path", name="test-configmap-mount"),
        ]
        job_spec["kubernetes"]["volumes"] = copy.deepcopy(volumes)
        job_spec["kubernetes"]["volume_mounts"] = copy.deepcopy(volume_mounts)
        job_spec["kubernetes"]["config_map_mounts"] = {"test-configmap-mounts-field": "/cm-path"}
        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        assert op.launcher.body["spec"]["volumes"] == volumes + [
            k8s.V1Volume(
                name="test-configmap-mounts-field",
                config_map=k8s.V1ConfigMapVolumeSource(name="test-configmap-mounts-field"),
            )
        ]
        volume_mounts = volume_mounts + [
            k8s.V1VolumeMount(mount_path="/cm-path", name="test-configmap-mounts-field")
        ]
        assert op.launcher.body["spec"]["driver"]["volumeMounts"] == volume_mounts
        assert op.launcher.body["spec"]["executor"]["volumeMounts"] == volume_mounts

    @pytest.mark.asyncio
    def test_pull_secret(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "test_pull_secret"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())
        job_spec["kubernetes"]["image_pull_secrets"] = "secret1,secret2"
        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        exp_secrets = [k8s.V1LocalObjectReference(name=secret) for secret in ["secret1", "secret2"]]
        assert op.launcher.body["spec"]["imagePullSecrets"] == exp_secrets

    @pytest.mark.asyncio
    def test_affinity(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "test_affinity"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())
        job_spec["kubernetes"]["affinity"] = k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                    node_selector_terms=[
                        k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(
                                    key="beta.kubernetes.io/instance-type",
                                    operator="In",
                                    values=["r5.xlarge"],
                                )
                            ]
                        )
                    ]
                )
            )
        )

        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)
        affinity = k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                    node_selector_terms=[
                        k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(
                                    key="beta.kubernetes.io/instance-type",
                                    operator="In",
                                    values=["r5.xlarge"],
                                )
                            ]
                        )
                    ]
                )
            )
        )
        assert op.launcher.body["spec"]["driver"]["affinity"] == affinity
        assert op.launcher.body["spec"]["executor"]["affinity"] == affinity

    @pytest.mark.asyncio
    def test_toleration(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        toleration = k8s.V1Toleration(
            key="dedicated",
            operator="Equal",
            value="test",
            effect="NoSchedule",
        )
        task_name = "test_tolerations"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())
        job_spec["kubernetes"]["tolerations"] = [toleration]
        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        assert op.launcher.body["spec"]["driver"]["tolerations"] == [toleration]
        assert op.launcher.body["spec"]["executor"]["tolerations"] == [toleration]

    @pytest.mark.asyncio
    def test_get_logs_from_driver(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "test_get_logs_from_driver"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())

        def mock_parent_execute_side_effect(context):
            mock_fetch_requested_container_logs(
                pod=mock_create_pod.return_value,
                containers="spark-kubernetes-driver",
                follow_logs=True,
                container_name_log_prefix_enabled=True,
                log_formatter=None,
            )
            return None

        mock_parent_execute.side_effect = mock_parent_execute_side_effect

        self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        mock_fetch_requested_container_logs.assert_called_once_with(
            pod=mock_create_pod.return_value,
            containers="spark-kubernetes-driver",
            follow_logs=True,
            container_name_log_prefix_enabled=True,
            log_formatter=None,
        )

    @pytest.mark.asyncio
    def test_find_custom_pod_labels(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "test_find_custom_pod_labels"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())

        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
            get_logs=True,
        )
        context = create_context(op)
        op.execute(context)
        label_selector = op._build_find_pod_label_selector(context) + ",spark-role=driver"
        op.find_spark_job(context)
        mock_get_kube_client.list_namespaced_pod.assert_called_with("default", label_selector=label_selector)

    @patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook")
    def test_adds_task_context_labels_to_driver_and_executor(
        self,
        mock_kubernetes_hook,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "test_adds_task_context_labels"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())

        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
            get_logs=True,
            reattach_on_restart=True,
        )
        context = create_context(op)
        op.execute(context)

        task_context_labels = op._get_ti_pod_labels(context)

        # Check that labels were added to the template body structure
        created_body = mock_create_namespaced_crd.call_args[1]["body"]
        for component in ["driver", "executor"]:
            for label_key, label_value in task_context_labels.items():
                assert label_key in created_body["spec"][component]["labels"]
                assert created_body["spec"][component]["labels"][label_key] == label_value

    def test_reattach_on_restart_with_task_context_labels(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
    ):
        task_name = "test_reattach_on_restart"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())

        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
            get_logs=True,
            reattach_on_restart=True,
        )
        context = create_context(op)

        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = f"{task_name}-driver"
        mock_pod.metadata.labels = op._get_ti_pod_labels(context)
        mock_pod.metadata.labels["spark-role"] = "driver"
        mock_pod.metadata.labels["try_number"] = str(context["ti"].try_number)
        mock_get_kube_client.list_namespaced_pod.return_value.items = [mock_pod]

        op.execute(context)

        label_selector = op._build_find_pod_label_selector(context) + ",spark-role=driver"
        mock_get_kube_client.list_namespaced_pod.assert_called_with("default", label_selector=label_selector)

        mock_create_namespaced_crd.assert_not_called()

    @pytest.mark.asyncio
    def test_execute_deferrable(
        self,
        mock_is_in_cluster,
        mock_parent_execute,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_completion,
        mock_fetch_requested_container_logs,
        data_file,
        mocker,
    ):
        task_name = "test_execute_deferrable"
        job_spec = yaml.safe_load(data_file("spark/application_template.yaml").read_text())

        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
            get_logs=True,
            deferrable=True,
        )
        context = create_context(op)

        mock_file = mock_open(read_data='{"a": "b"}')
        mocker.patch("builtins.open", mock_file)

        with pytest.raises(TaskDeferred) as exc:
            op.execute(context)

        assert isinstance(exc.value.trigger, KubernetesPodTrigger)


@pytest.mark.db_test
def test_template_body_templating(create_task_instance_of_operator, session):
    ti = create_task_instance_of_operator(
        SparkKubernetesOperator,
        template_spec={"foo": "{{ ds }}", "bar": "{{ dag_run.dag_id }}"},
        kubernetes_conn_id="kubernetes_default_kube_config",
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
        session=session,
    )
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: SparkKubernetesOperator = ti.task
    assert task.template_body == {"spark": {"foo": "2016-01-01", "bar": "test_template_body_templating_dag"}}


@pytest.mark.db_test
def test_resolve_application_file_template_file(dag_maker, tmp_path, session):
    logical_date = timezone.datetime(2024, 2, 1, tzinfo=timezone.utc)
    filename = "test-application-file.yml"
    (tmp_path / filename).write_text("foo: {{ ds }}\nbar: {{ dag_run.dag_id }}\nspam: egg")

    with dag_maker(
        dag_id="test_resolve_application_file_template_file",
        template_searchpath=tmp_path.as_posix(),
        session=session,
    ):
        SparkKubernetesOperator(
            application_file=filename,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_template_body_templating_task",
        )
    ti = dag_maker.create_dagrun(logical_date=logical_date).task_instances[0]
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: SparkKubernetesOperator = ti.task
    assert task.template_body == {
        "spark": {
            "foo": date(2024, 2, 1),
            "bar": "test_resolve_application_file_template_file",
            "spam": "egg",
        }
    }


@pytest.mark.db_test
@pytest.mark.parametrize(
    "body",
    [
        pytest.param(["a", "b"], id="list"),
        pytest.param(42, id="int"),
        pytest.param("{{ ds }}", id="jinja"),
        pytest.param(None, id="none"),
    ],
)
def test_resolve_application_file_template_non_dictionary(dag_maker, tmp_path, body, session):
    logical_date = timezone.datetime(2024, 2, 1, tzinfo=timezone.utc)
    filename = "test-application-file.yml"
    with open((tmp_path / filename), "w") as fp:
        yaml.safe_dump(body, fp)

    with dag_maker(
        dag_id="test_resolve_application_file_template_nondictionary",
        template_searchpath=tmp_path.as_posix(),
        session=session,
    ):
        SparkKubernetesOperator(
            application_file=filename,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_template_body_templating_task",
        )
    ti = dag_maker.create_dagrun(logical_date=logical_date).task_instances[0]
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: SparkKubernetesOperator = ti.task
    with pytest.raises(TypeError, match="application_file body can't transformed into the dictionary"):
        _ = task.template_body


@pytest.mark.db_test
@pytest.mark.parametrize(
    "use_literal_value", [pytest.param(True, id="literal-value"), pytest.param(False, id="whitespace-compat")]
)
def test_resolve_application_file_real_file(
    create_task_instance_of_operator, tmp_path, use_literal_value, session
):
    application_file = tmp_path / "test-application-file.yml"
    application_file.write_text("foo: bar\nspam: egg")

    application_file = application_file.resolve().as_posix()
    if use_literal_value:
        try:
            from airflow.template.templater import LiteralValue
        except ImportError:
            # Airflow 3.0+
            from airflow.sdk.definitions._internal.templater import LiteralValue

        application_file = LiteralValue(application_file)
    else:
        # Prior Airflow 2.8 workaround was adding whitespace at the end of the filepath
        application_file = f"{application_file} "

    ti = create_task_instance_of_operator(
        SparkKubernetesOperator,
        application_file=application_file,
        kubernetes_conn_id="kubernetes_default_kube_config",
        dag_id="test_resolve_application_file_real_file",
        task_id="test_template_body_templating_task",
        session=session,
    )
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: SparkKubernetesOperator = ti.task

    assert task.template_body == {"spark": {"foo": "bar", "spam": "egg"}}


@pytest.mark.db_test
def test_resolve_application_file_real_file_not_exists(create_task_instance_of_operator, tmp_path, session):
    application_file = (tmp_path / "test-application-file.yml").resolve().as_posix()
    try:
        from airflow.template.templater import LiteralValue
    except ImportError:
        # Airflow 3.0+
        from airflow.sdk.definitions._internal.templater import LiteralValue

    ti = create_task_instance_of_operator(
        SparkKubernetesOperator,
        application_file=LiteralValue(application_file),
        kubernetes_conn_id="kubernetes_default_kube_config",
        dag_id="test_resolve_application_file_real_file_not_exists",
        task_id="test_template_body_templating_task",
        session=session,
    )
    session.add(ti)
    session.commit()
    ti.render_templates()
    task: SparkKubernetesOperator = ti.task
    with pytest.raises(TypeError, match="application_file body can't transformed into the dictionary"):
        _ = task.template_body


@pytest.mark.parametrize(
    "random_name_suffix",
    [pytest.param(True, id="use-random_name_suffix"), pytest.param(False, id="skip-random_name_suffix")],
)
def test_create_job_name(random_name_suffix: bool):
    name = f"x{uuid4()}"
    op = SparkKubernetesOperator(task_id="task_id", name=name, random_name_suffix=random_name_suffix)
    pod_name = op.create_job_name()

    if random_name_suffix:
        assert pod_name.startswith(name)
        assert pod_name != name
    else:
        assert pod_name == name


def test_create_job_name_should_truncate_long_names():
    long_name = f"{uuid4()}" + "x" * MAX_LABEL_LEN
    op = SparkKubernetesOperator(task_id="task_id", name=long_name, random_name_suffix=False)
    pod_name = op.create_job_name()

    assert pod_name == long_name[:MAX_LABEL_LEN]


class TestSparkKubernetesLifecycle:
    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.CustomObjectLauncher")
    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
    def test_launcher_access_without_execute(self, mock_hook, mock_launcher_cls):
        """Test that launcher is accessible even if execute is not called (e.g. after deferral)."""
        op = SparkKubernetesOperator(
            task_id="test_task",
            namespace="default",
            application_file="example.yaml",
            kubernetes_conn_id="kubernetes_default",
        )

        # Mock the template body loading since we don't have a real file
        with mock.patch.object(SparkKubernetesOperator, "manage_template_specs") as mock_manage:
            mock_manage.return_value = {"spark": {"spec": {}}}

            # Access launcher
            launcher = op.launcher

            assert launcher is not None
            assert mock_launcher_cls.called

    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.CustomObjectLauncher")
    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
    def test_on_kill_works_without_execute(self, mock_hook, mock_launcher_cls):
        """Test that on_kill works without execute being called."""
        op = SparkKubernetesOperator(
            task_id="test_task",
            namespace="default",
            application_file="example.yaml",
            name="test-job",
        )

        mock_launcher_instance = mock_launcher_cls.return_value

        with mock.patch.object(SparkKubernetesOperator, "manage_template_specs") as mock_manage:
            mock_manage.return_value = {"spark": {"spec": {}}}

            op.on_kill()

            # Should call delete_spark_job on the launcher
            mock_launcher_instance.delete_spark_job.assert_called_once()

            # Check arguments
            call_args = mock_launcher_instance.delete_spark_job.call_args
            # We expect spark_job_name="test-job"
            assert call_args.kwargs.get("spark_job_name") == "test-job"

    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.CustomObjectLauncher")
    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
    @mock.patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesPodOperator.execute")
    def test_reattach_skips_launcher_creation_in_execute(
        self, mock_super_execute, mock_hook, mock_launcher_cls
    ):
        """Test that reattach logic skips explicit launcher creation but property still works."""
        op = SparkKubernetesOperator(
            task_id="test_task",
            namespace="default",
            application_file="example.yaml",
            reattach_on_restart=True,
        )

        # Mock finding an existing pod
        mock_pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="existing-pod"))

        with (
            mock.patch.object(SparkKubernetesOperator, "find_spark_job", return_value=mock_pod),
            mock.patch.object(
                SparkKubernetesOperator, "manage_template_specs", return_value={"spark": {"spec": {}}}
            ),
            mock.patch.object(SparkKubernetesOperator, "_get_ti_pod_labels", return_value={}),
        ):
            context = {"ti": mock.MagicMock(), "run_id": "test_run"}

            # Run execute
            op.execute(context)

            # Verify super().execute was called
            mock_super_execute.assert_called_once()

            # Verify launcher was NOT instantiated during execute (because we returned early)
            # We can check if the mock_launcher_cls was instantiated.
            # It should NOT be instantiated during execute because _setup_spark_configuration returns early.
            # However, accessing op.launcher later WILL instantiate it.

            # Reset mock to clear any previous calls (though there shouldn't be any)
            mock_launcher_cls.reset_mock()

            # Access launcher now
            assert op.launcher is not None

            # Now it should have been instantiated
            mock_launcher_cls.assert_called_once()

            # And verify delete works
            op.on_kill()
            mock_launcher_cls.return_value.delete_spark_job.assert_called()

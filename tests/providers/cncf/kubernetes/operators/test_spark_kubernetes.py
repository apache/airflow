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

import copy
import json
from os.path import join
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pendulum
import pytest
import yaml
from kubernetes.client import models as k8s

from airflow import DAG
from airflow.models import Connection, DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils import db, timezone
from airflow.utils.types import DagRunType


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator(mock_kubernetes_hook):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file=join(Path(__file__).parent, "spark_application_test.yaml"),
        kubernetes_conn_id="kubernetes_conn_id",
        in_cluster=True,
        cluster_context="cluster_context",
        config_file="config_file",
    )
    mock_kubernetes_hook.assert_not_called()  # constructor shouldn't call the hook

    assert "hook" not in operator.__dict__  # Cached property has not been accessed as part of construction.


@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.KubernetesHook")
def test_spark_kubernetes_operator_hook(mock_kubernetes_hook):
    operator = SparkKubernetesOperator(
        task_id="task_id",
        application_file=join(Path(__file__).parent, "spark_application_test.yaml"),
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


TEST_K8S_DICT = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "default_yaml_template", "namespace": "default"},
    "spec": {
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {},
            "memory": "365m",
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
            "labels": {},
            "env": [],
            "envFrom": [],
            "memory": "365m",
            "nodeSelector": {},
            "volumeMounts": [],
            "tolerations": [],
            "affinity": {"nodeAffinity": {}, "podAffinity": {}, "podAntiAffinity": {}},
        },
        "hadoopConf": {},
        "dynamicAllocation": {"enabled": False, "initialExecutors": 1, "maxExecutors": 1, "minExecutors": 1},
        "image": "gcr.io/spark-operator/spark:v2.4.5",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/test.py",
        "mode": "cluster",
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "3.0.0",
        "successfulRunHistoryLimit": 1,
        "pythonVersion": "3",
        "type": "Python",
        "imagePullSecrets": "",
        "labels": {},
        "volumes": [],
    },
}

TEST_APPLICATION_DICT = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {"name": "default_yaml", "namespace": "default"},
    "spec": {
        "driver": {
            "coreLimit": "1200m",
            "cores": 1,
            "labels": {"version": "2.4.5"},
            "memory": "512m",
            "serviceAccount": "spark",
            "volumeMounts": [{"mountPath": "/tmp", "name": "test-volume"}],
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "labels": {"version": "2.4.5"},
            "memory": "512m",
            "volumeMounts": [{"mountPath": "/tmp", "name": "test-volume"}],
        },
        "image": "gcr.io/spark-operator/spark:v2.4.5",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar",
        "mainClass": "org.apache.spark.examples.SparkPi",
        "mode": "cluster",
        "restartPolicy": {"type": "Never"},
        "sparkVersion": "2.4.5",
        "type": "Scala",
        "volumes": [{"hostPath": {"path": "/tmp", "type": "Directory"}, "name": "test-volume"}],
    },
}


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


@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_start")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
@patch("airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator.client")
@patch(
    "airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator.create_job_name"
)  # , return_value='default')
@patch("airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.cleanup")
@patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object_status")
@patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
class TestSparkKubernetesOperator:
    def setUp(self):
        db.merge_conn(
            Connection(conn_id="kubernetes_default_kube_config", conn_type="kubernetes", extra=json.dumps({}))
        )
        db.merge_conn(
            Connection(
                conn_id="kubernetes_with_namespace",
                conn_type="kubernetes",
                extra=json.dumps({"extra__kubernetes__namespace": "mock_namespace"}),
            )
        )
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    def execute_operator(self, task_name, mock_create_job_name, job_spec):
        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            template_spec=job_spec,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
        )
        context = create_context(op)
        op.execute(context)
        return op

    def test_create_application_from_yaml_json(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "default_yaml"
        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            application_file=join(Path(__file__).parent, "spark_application_test.yaml"),
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
        )
        context = create_context(op)
        op.execute(context)
        TEST_APPLICATION_DICT["metadata"]["name"] = task_name
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group="sparkoperator.k8s.io",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

        task_name = "default_json"
        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            application_file=join(Path(__file__).parent, "spark_application_test.json"),
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
        )
        context = create_context(op)
        op.execute(context)
        TEST_APPLICATION_DICT["metadata"]["name"] = task_name
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group="sparkoperator.k8s.io",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    def test_new_template_from_yaml(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "default_yaml_template"
        mock_create_job_name.return_value = task_name
        op = SparkKubernetesOperator(
            application_file=join(Path(__file__).parent, "spark_application_template.yaml"),
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id=task_name,
        )
        context = create_context(op)
        op.execute(context)
        TEST_K8S_DICT["metadata"]["name"] = task_name
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_K8S_DICT,
            group="sparkoperator.k8s.io",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    def test_template_spec(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "default_yaml_template"
        job_spec = yaml.safe_load(open(join(Path(__file__).parent, "spark_application_template.yaml")))
        self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        TEST_K8S_DICT["metadata"]["name"] = task_name
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_K8S_DICT,
            group="sparkoperator.k8s.io",
            namespace="default",
            plural="sparkapplications",
            version="v1beta2",
        )

    def test_env(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "default_env"
        job_spec = yaml.safe_load(open(join(Path(__file__).parent, "spark_application_template.yaml")))
        # test env vars
        job_spec["kubernetes"]["env_vars"] = {"TEST_ENV_1": "VALUE1"}

        # test env from
        env_from = [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="env-direct-configmap")),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="env-direct-secret")),
        ]
        job_spec["kubernetes"]["env_from"] = copy.deepcopy(env_from)

        # test from_env_config_map
        job_spec["kubernetes"]["from_env_config_map"] = ["env-from-configmap"]
        job_spec["kubernetes"]["from_env_secret"] = ["env-from-secret"]

        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)
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

    def test_volume(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "default_volume"
        job_spec = yaml.safe_load(open(join(Path(__file__).parent, "spark_application_template.yaml")))
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

    def test_pull_secret(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "test_pull_secret"
        job_spec = yaml.safe_load(open(join(Path(__file__).parent, "spark_application_template.yaml")))
        job_spec["kubernetes"]["image_pull_secrets"] = "secret1,secret2"
        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        exp_secrets = [k8s.V1LocalObjectReference(name=secret) for secret in ["secret1", "secret2"]]
        assert op.launcher.body["spec"]["imagePullSecrets"] == exp_secrets

    def test_affinity(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        task_name = "test_affinity"
        job_spec = yaml.safe_load(open(join(Path(__file__).parent, "spark_application_template.yaml")))
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

    def test_toleration(
        self,
        mock_create_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_cleanup,
        mock_create_job_name,
        mock_get_kube_client,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        toleration = k8s.V1Toleration(
            key="dedicated",
            operator="Equal",
            value="test",
            effect="NoSchedule",
        )
        task_name = "test_tolerations"
        job_spec = yaml.safe_load(open(join(Path(__file__).parent, "spark_application_template.yaml")))
        job_spec["kubernetes"]["tolerations"] = [toleration]
        op = self.execute_operator(task_name, mock_create_job_name, job_spec=job_spec)

        assert op.launcher.body["spec"]["driver"]["tolerations"] == [toleration]
        assert op.launcher.body["spec"]["executor"]["tolerations"] == [toleration]


@pytest.mark.db_test
def test_template_body_templating(create_task_instance_of_operator):
    ti = create_task_instance_of_operator(
        SparkKubernetesOperator,
        template_spec={"foo": "{{ ds }}", "bar": "{{ dag_run.dag_id }}"},
        kubernetes_conn_id="kubernetes_default_kube_config",
        dag_id="test_template_body_templating_dag",
        task_id="test_template_body_templating_task",
        execution_date=timezone.datetime(2024, 2, 1, tzinfo=timezone.utc),
    )
    ti.render_templates()
    task: SparkKubernetesOperator = ti.task
    assert task.template_body == {"spark": {"foo": "2024-02-01", "bar": "test_template_body_templating_dag"}}

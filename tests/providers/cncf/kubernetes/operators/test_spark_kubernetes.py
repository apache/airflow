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
#

import json
import unittest
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

TEST_VALID_APPLICATION_YAML = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v2.4.5"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar"
  sparkVersion: "2.4.5"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    labels:
      version: 2.4.5
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    labels:
      version: 2.4.5
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
"""
TEST_VALID_APPLICATION_JSON = """
{
   "apiVersion":"sparkoperator.k8s.io/v1beta2",
   "kind":"SparkApplication",
   "metadata":{
      "name":"spark-pi",
      "namespace":"default"
   },
   "spec":{
      "type":"Scala",
      "mode":"cluster",
      "image":"gcr.io/spark-operator/spark:v2.4.5",
      "imagePullPolicy":"Always",
      "mainClass":"org.apache.spark.examples.SparkPi",
      "mainApplicationFile":"local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar",
      "sparkVersion":"2.4.5",
      "restartPolicy":{
         "type":"Never"
      },
      "volumes":[
         {
            "name":"test-volume",
            "hostPath":{
               "path":"/tmp",
               "type":"Directory"
            }
         }
      ],
      "driver":{
         "cores":1,
         "coreLimit":"1200m",
         "memory":"512m",
         "labels":{
            "version":"2.4.5"
         },
         "serviceAccount":"spark",
         "volumeMounts":[
            {
               "name":"test-volume",
               "mountPath":"/tmp"
            }
         ]
      },
      "executor":{
         "cores":1,
         "instances":1,
         "memory":"512m",
         "labels":{
            "version":"2.4.5"
         },
         "volumeMounts":[
            {
               "name":"test-volume",
               "mountPath":"/tmp"
            }
         ]
      }
   }
}
"""
TEST_APPLICATION_DICT = {
    'apiVersion': 'sparkoperator.k8s.io/v1beta2',
    'kind': 'SparkApplication',
    'metadata': {'name': 'spark-pi', 'namespace': 'default'},
    'spec': {
        'driver': {
            'coreLimit': '1200m',
            'cores': 1,
            'labels': {'version': '2.4.5'},
            'memory': '512m',
            'serviceAccount': 'spark',
            'volumeMounts': [{'mountPath': '/tmp', 'name': 'test-volume'}],
        },
        'executor': {
            'cores': 1,
            'instances': 1,
            'labels': {'version': '2.4.5'},
            'memory': '512m',
            'volumeMounts': [{'mountPath': '/tmp', 'name': 'test-volume'}],
        },
        'image': 'gcr.io/spark-operator/spark:v2.4.5',
        'imagePullPolicy': 'Always',
        'mainApplicationFile': 'local:///opt/spark/examples/jars/spark-examples_2.11-2.4.5.jar',
        'mainClass': 'org.apache.spark.examples.SparkPi',
        'mode': 'cluster',
        'restartPolicy': {'type': 'Never'},
        'sparkVersion': '2.4.5',
        'type': 'Scala',
        'volumes': [{'hostPath': {'path': '/tmp', 'type': 'Directory'}, 'name': 'test-volume'}],
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
@patch("airflow.kubernetes.custom_object_launcher.CustomObjectLauncher._load_body")
@patch("airflow.kubernetes.kube_client.get_kube_client")
@patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object_status')
@patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
@patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
class TestSparkKubernetesOperatorYaml(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(conn_id='kubernetes_default_kube_config', conn_type='kubernetes', extra=json.dumps({}))
        )
        db.merge_conn(
            Connection(
                conn_id='kubernetes_with_namespace',
                conn_type='kubernetes',
                extra=json.dumps({'extra__kubernetes__namespace': 'mock_namespace'}),
            )
        )
        args = {'owner': 'airflow', 'start_date': timezone.datetime(2020, 2, 1)}
        self.dag = DAG('test_dag_id', default_args=args)

    def test_create_application_from_yaml(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        mock_body.return_value = yaml.safe_load(TEST_VALID_APPLICATION_YAML)
        mock_create_namespaced_crd.return_value = yaml.safe_load(TEST_VALID_APPLICATION_YAML)
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_YAML,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
        )
        context = create_context(op)
        op.execute(context)
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
        )
        mock_delete_namespaced_crd.assert_called_once_with(
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
            name=TEST_APPLICATION_DICT["metadata"]["name"],
        )

    def test_create_application_from_json(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        mock_body.return_value = TEST_APPLICATION_DICT
        mock_create_namespaced_crd.return_value = TEST_APPLICATION_DICT
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
        )
        context = create_context(op)
        op.execute(context)
        mock_delete_namespaced_crd.assert_called_once_with(
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
            name=TEST_APPLICATION_DICT["metadata"]["name"],
        )
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
        )

    def test_create_application_from_json_with_api_group_and_version(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        mock_body.return_value = json.loads(TEST_VALID_APPLICATION_JSON)
        mock_create_namespaced_crd.return_value = json.loads(TEST_VALID_APPLICATION_JSON)
        api_group = 'sparkoperator.example.com'
        api_version = 'v1alpha1'
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
            api_group=api_group,
            api_version=api_version,
        )
        context = create_context(op)
        op.execute(context)
        mock_delete_namespaced_crd.assert_called_once_with(
            group=api_group,
            namespace='default',
            plural='sparkapplications',
            version=api_version,
            name=TEST_APPLICATION_DICT["metadata"]["name"],
        )
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group=api_group,
            namespace='default',
            plural='sparkapplications',
            version=api_version,
        )

    def test_namespace_from_operator(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        mock_body.return_value = json.loads(TEST_VALID_APPLICATION_JSON)
        mock_create_namespaced_crd.return_value = json.loads(TEST_VALID_APPLICATION_JSON)
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            namespace='operator_namespace',
            kubernetes_conn_id='kubernetes_with_namespace',
            task_id='test_task_id',
        )
        context = create_context(op)
        op.execute(context)
        mock_delete_namespaced_crd.assert_called_once_with(
            group='sparkoperator.k8s.io',
            namespace='operator_namespace',
            plural='sparkapplications',
            version='v1beta2',
            name=TEST_APPLICATION_DICT["metadata"]["name"],
        )
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='operator_namespace',
            plural='sparkapplications',
            version='v1beta2',
        )

    def test_namespace_from_connection(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        mock_body.return_value = json.loads(TEST_VALID_APPLICATION_JSON)
        mock_create_namespaced_crd.return_value = json.loads(TEST_VALID_APPLICATION_JSON)
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_with_namespace',
            task_id='test_task_id',
        )
        context = create_context(op)
        op.execute(context)
        mock_delete_namespaced_crd.assert_called_once_with(
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
            name=TEST_APPLICATION_DICT["metadata"]["name"],
        )
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='default',
            plural='sparkapplications',
            version='v1beta2',
        )


@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_completion")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.await_pod_start")
@patch("airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager.create_pod")
@patch("airflow.kubernetes.custom_object_launcher.CustomObjectLauncher._load_body")
@patch("airflow.kubernetes.kube_client.get_kube_client")
@patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object_status')
@patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
@patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
class TestSparkKubernetesOperator:
    def setup_class(self):
        db.merge_conn(
            Connection(conn_id='kubernetes_default_kube_config', conn_type='kubernetes', extra=json.dumps({}))
        )
        db.merge_conn(
            Connection(
                conn_id='kubernetes_with_namespace',
                conn_type='kubernetes',
                extra=json.dumps({'extra__kubernetes__namespace': 'mock_namespace'}),
            )
        )
        args = {'owner': 'airflow', 'start_date': timezone.datetime(2020, 2, 1)}
        self.dag = DAG('test_dag_id', default_args=args)

    def test_env_vars(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        env_from = [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='env-direct-configmap')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='env-direct-secret')),
        ]
        from_env_config_map = ['env-from-configmap']
        from_env_secret = ['env-from-secret']
        op = SparkKubernetesOperator(
            task_id='test-env-vars',
            namespace='mock_namespace',
            service_account_name='mock_sa',
            code_path='/code/path',
            image='mock_image_tag',
            env_vars={"TEST_ENV_1": 'VALUE1', "TEST_ENV_2": 'VALUE2'},
            env_from=env_from,
            from_env_config_map=from_env_config_map,
            from_env_secret=from_env_secret,
            dag=self.dag,
        )
        context = create_context(op)
        op.execute(context)
        assert op.launcher.body['spec']['driver']['env'] == [
            k8s.V1EnvVar(name='TEST_ENV_1', value='VALUE1'),
            k8s.V1EnvVar(name='TEST_ENV_2', value='VALUE2'),
        ]
        assert op.launcher.body['spec']['executor']['env'] == [
            k8s.V1EnvVar(name='TEST_ENV_1', value='VALUE1'),
            k8s.V1EnvVar(name='TEST_ENV_2', value='VALUE2'),
        ]
        exp_env_from = [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='env-direct-configmap')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='env-direct-secret')),
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='env-from-configmap')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='env-from-secret')),
        ]
        assert op.launcher.body['spec']['driver']['envFrom'] == exp_env_from
        assert op.launcher.body['spec']['executor']['envFrom'] == exp_env_from

    def test_volume_mount(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        volumes = [
            k8s.V1Volume(
                name='test-pvc',
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='test-pvc'),
            )
        ]
        volume_mount = [k8s.V1VolumeMount(mount_path='/pvc-path', name='test-pvc')]
        op = SparkKubernetesOperator(
            task_id='test_volume_mount',
            namespace='mock_namespace',
            service_account_name='mock_sa',
            code_path='/code/path',
            image='mock_image_tag',
            volumes=volumes,
            volume_mounts=volume_mount,
            config_map_mounts={'test-configmap-mount': '/configmap-path'},
            dag=self.dag,
        )
        context = create_context(op)
        op.execute(context)
        exp_vols = volumes + [
            k8s.V1Volume(
                name='test-configmap-mount',
                config_map=k8s.V1ConfigMapVolumeSource(name='test-configmap-mount'),
            )
        ]
        exp_vol_mounts = [
            k8s.V1VolumeMount(mount_path='/pvc-path', name='test-pvc'),
            k8s.V1VolumeMount(mount_path='/configmap-path', name='test-configmap-mount'),
        ]

        assert op.launcher.body['spec']['volumes'] == exp_vols
        assert op.launcher.body['spec']['driver']['volumeMounts'] == exp_vol_mounts
        assert op.launcher.body['spec']['executor']['volumeMounts'] == exp_vol_mounts

    def test_pull_secret(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        volume_mount = [k8s.V1VolumeMount(mount_path='/pvc-path', name='test-pvc')]
        op = SparkKubernetesOperator(
            task_id='test_pull_secret',
            namespace='mock_namespace',
            service_account_name='mock_sa',
            code_path='/code/path',
            image='mock_image_tag',
            image_pull_secrets='secret1,secret2',
            volume_mounts=volume_mount,
            config_map_mounts={'test-configmap-mount': '/configmap-path'},
            dag=self.dag,
        )
        context = create_context(op)
        op.execute(context)
        exp_secrets = [k8s.V1LocalObjectReference(name=secret) for secret in ['secret1', 'secret2']]
        assert op.launcher.body['spec']['imagePullSecrets'] == exp_secrets

    def test_affinity(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        affinity = k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                    node_selector_terms=[
                        k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(
                                    key='beta.kubernetes.io/instance-type',
                                    operator='In',
                                    values=['test'],
                                )
                            ]
                        )
                    ]
                )
            )
        )
        op = SparkKubernetesOperator(
            task_id='test_affinity',
            code_path='/code/path',
            image='mock_image_tag',
            affinity=affinity,
            dag=self.dag,
        )
        context = create_context(op)
        op.execute(context)
        assert op.launcher.body['spec']['driver']['affinity'] == affinity
        assert op.launcher.body['spec']['executor']['affinity'] == affinity

    def test_toleration(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
    ):
        toleration = k8s.V1Toleration(
            key='dedicated',
            operator='Equal',
            value='test',
            effect='NoSchedule',
        )
        op = SparkKubernetesOperator(
            task_id='test_toleration',
            code_path='/code/path',
            image='mock_image_tag',
            tolerations=[toleration],
            dag=self.dag,
        )
        context = create_context(op)
        op.execute(context)
        assert op.launcher.body['spec']['driver']['tolerations'] == [toleration]
        assert op.launcher.body['spec']['executor']['tolerations'] == [toleration]

    @pytest.mark.parametrize(
        "task_id, resources, dr_exp_cores, dr_exp_cor_limit, dr_exp_mem, exec_exp_cores, "
        "exec_exp_cor_limit, exec_exp_mem, dr_gpu, exec_gpu",
        [
            (
                'test_resources_0',
                {
                    'driver_request_cpu': '1',
                    'driver_limit_cpu': '1',
                    'driver_limit_memory': '1Gi',
                    'executor_request_cpu': '2',
                    'executor_limit_cpu': '1',
                    'executor_limit_memory': '1Gi',
                    'driver_gpu_name': 'nvidia.com/gpu',
                    'driver_gpu_quantity': '1',
                    'executor_gpu_name': 'nvidia.com/gpu',
                    'executor_gpu_quantity': '2',
                },
                1,
                '1',
                '731m',
                2,
                '1',
                '731m',
                {'name': 'nvidia.com/gpu', 'quantity': 1},
                {'name': 'nvidia.com/gpu', 'quantity': 2},
            ),  # 731 + (40%*731) = 1024m(1Gi)
            (
                'test_resources_1',
                {
                    'driver_request_cpu': 1,
                    'driver_limit_cpu': 1,
                    'driver_limit_memory': '1024m',
                },
                1,
                '1',
                '731m',
                None,
                None,
                None,
                None,
                None,
            ),
            ('test_resources_2', {}, None, None, None, None, None, None, None, None),
        ],
    )
    def test_resources(
        self,
        mock_create_namespaced_crd,
        mock_delete_namespaced_crd,
        mock_get_namespaced_custom_object_status,
        mock_get_kube_client,
        mock_body,
        mock_create_pod,
        mock_await_pod_start,
        mock_await_pod_completion,
        task_id,
        resources,
        dr_exp_cores,
        dr_exp_cor_limit,
        dr_exp_mem,
        exec_exp_cores,
        exec_exp_cor_limit,
        exec_exp_mem,
        dr_gpu,
        exec_gpu,
    ):
        op = SparkKubernetesOperator(
            task_id=task_id,
            code_path='/code/path',
            image='mock_image_tag',
            resources=resources,
            dag=self.dag,
        )
        context = create_context(op)
        op.execute(context)
        assert op.launcher.body['spec']['driver'].get('cores') == dr_exp_cores
        assert op.launcher.body['spec']['driver'].get('coreLimit') == dr_exp_cor_limit
        assert op.launcher.body['spec']['driver'].get('memory') == dr_exp_mem
        assert op.launcher.body['spec']['driver'].get('gpu') == dr_gpu

        assert op.launcher.body['spec']['executor'].get('cores') == exec_exp_cores
        assert op.launcher.body['spec']['executor'].get('coreLimit') == exec_exp_cor_limit
        assert op.launcher.body['spec']['executor'].get('memory') == exec_exp_mem
        assert op.launcher.body['spec']['executor'].get('gpu') == exec_gpu

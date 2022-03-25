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

from airflow.models import Connection
from airflow.models.xcom import IN_MEMORY_DAGRUN_ID
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils import db, timezone
from airflow.models import DAG, DagRun, TaskInstance
from airflow.utils.state import State
from kubernetes.client import models as k8s

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


@patch('airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn')
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

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_create_application_from_yaml(
        self, mock_create_namespaced_crd, mock_delete_namespaced_crd, mock_kubernetes_hook
    ):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_YAML,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
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

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_create_application_from_json(
        self, mock_create_namespaced_crd, mock_delete_namespaced_crd, mock_kubernetes_hook
    ):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_default_kube_config',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
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

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_create_application_from_json_with_api_group_and_version(
        self, mock_create_namespaced_crd, mock_delete_namespaced_crd, mock_kubernetes_hook
    ):
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
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
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

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_namespace_from_operator(
        self, mock_create_namespaced_crd, mock_delete_namespaced_crd, mock_kubernetes_hook
    ):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            namespace='operator_namespace',
            kubernetes_conn_id='kubernetes_with_namespace',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
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

    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.delete_namespaced_custom_object')
    @patch('kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object')
    def test_namespace_from_connection(
        self, mock_create_namespaced_crd, mock_delete_namespaced_crd, mock_kubernetes_hook
    ):
        op = SparkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id='kubernetes_with_namespace',
            task_id='test_task_id',
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()
        mock_delete_namespaced_crd.assert_called_once_with(
            group='sparkoperator.k8s.io',
            namespace='mock_namespace',
            plural='sparkapplications',
            version='v1beta2',
            name=TEST_APPLICATION_DICT["metadata"]["name"],
        )
        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group='sparkoperator.k8s.io',
            namespace='mock_namespace',
            plural='sparkapplications',
            version='v1beta2',
        )


POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
DEFAULT_DATE = timezone.datetime(2016, 1, 1, 1, 0, 0)
SPARK_CLASS = "airflow.providers.cncf.kubernetes.operators.spark_kubernetes.SparkKubernetesOperator"


class TestSparkKubernetesOperator:
    def setup_method(self):
        self.create_pod_patch = mock.patch(f"{POD_MANAGER_CLASS}.create_pod")
        self.await_pod_patch = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start")
        self.await_pod_completion_patch = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion")
        self.client_patch = mock.patch("airflow.kubernetes.kube_client.get_kube_client")
        self.client_patch_pod_launcher = mock.patch("airflow.kubernetes.pod_launcher_deprecated.get_kube_client")

        self.delete_spark_job_patch = mock.patch(f"{SPARK_CLASS}.delete_spark_job")
        self.create_custom_obj_patch = mock.patch(f"{SPARK_CLASS}.create_new_custom_obj_for_operator")

        self.delete_spark_job_patch.start()
        self.delete_spark_job_patch = self.create_custom_obj_patch.start()
        self.delete_spark_job_patch.return_value = (State.SUCCESS, None)
        self.create_mock = self.create_pod_patch.start()
        self.await_start_mock = self.await_pod_patch.start()
        self.await_pod_mock = self.await_pod_completion_patch.start()
        self.client_mock = self.client_patch.start()
        self.client_mock = self.client_patch_pod_launcher.start()
        args = {'owner': 'airflow', 'start_date': timezone.datetime(2020, 2, 1)}
        self.dag = DAG('test_dag_id', default_args=args)

    def teardown_method(self):
        self.create_pod_patch.stop()
        self.await_pod_patch.stop()
        self.await_pod_completion_patch.stop()
        self.client_patch.stop()
        self.client_patch_pod_launcher.stop()
        self.delete_spark_job_patch.stop()

    @staticmethod
    def create_context(task):
        dag = DAG(dag_id="dag")
        task_instance = TaskInstance(task=task, run_id=IN_MEMORY_DAGRUN_ID)
        task_instance.dag_run = DagRun(run_id=IN_MEMORY_DAGRUN_ID)
        return {
            "dag": dag,
            "ts": DEFAULT_DATE.isoformat(),
            "task": task,
            "ti": task_instance,
            "task_instance": task_instance,
        }

    def test_env_vars(self):
        env_from = [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='env-direct-configmap')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='env-direct-secret'))
        ]
        from_env_config_map = ['env-from-configmap']
        from_env_secret = ['env-from-secret']
        op = SparkKubernetesOperator(
            task_id='test-spark',
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
        context = self.create_context(op)
        op.execute(context)
        assert op.launcher.body['spec']['driver']['env'] == [k8s.V1EnvVar(name='TEST_ENV_1', value='VALUE1'), k8s.V1EnvVar(name='TEST_ENV_2', value='VALUE2')]
        assert op.launcher.body['spec']['executor']['env'] == [k8s.V1EnvVar(name='TEST_ENV_1', value='VALUE1'), k8s.V1EnvVar(name='TEST_ENV_2', value='VALUE2')]
        exp_env_from = [
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='env-direct-configmap')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='env-direct-secret')),
            k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name='env-from-configmap')),
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name='env-from-secret'))
        ]
        assert op.launcher.body['spec']['driver']['envFrom'] == exp_env_from
        assert op.launcher.body['spec']['executor']['envFrom'] == exp_env_from

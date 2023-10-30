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

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from airflow import DAG
from airflow.models import Connection
from airflow.providers.apache.flink.operators.flink_kubernetes import FlinkKubernetesOperator
from airflow.utils import db, timezone

pytestmark = pytest.mark.db_test


TEST_VALID_APPLICATION_YAML = """
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-sm-ex
  namespace: default
spec:
  image: flink:1.15
  flinkVersion: v1_15
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "2"
    state.savepoints.dir: file:///flink-data/savepoints
    state.checkpoints.dir: file:///flink-data/checkpoints
    high-availability: org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory
    high-availability.storageDir: file:///flink-data/ha
  ingress:
    template: "{{name}}.{{namespace}}.flink.k8s.io"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
    podTemplate:
      apiVersion: v1
      kind: Pod
      metadata:
        name: task-manager-pod-template
      spec:
        initContainers:
          # Sample sidecar container
          - name: busybox
            image: busybox:latest
            command: [ 'sh','-c','echo hello from task manager' ]
  job:
    jarURI: local:///opt/flink/examples/streaming/StateMachineExample.jar
    parallelism: 2
    upgradeMode: stateless
    state: running
    savepointTriggerNonce: 0
"""

TEST_VALID_APPLICATION_JSON = """
{
  "apiVersion": "flink.apache.org/v1beta1",
  "kind": "FlinkDeployment",
  "metadata": {
    "name": "flink-sm-ex",
    "namespace": "default"
  },
  "spec": {
    "image": "flink:1.15",
    "flinkVersion": "v1_15",
    "flinkConfiguration": {
      "taskmanager.numberOfTaskSlots": "2",
      "state.savepoints.dir": "file:///flink-data/savepoints",
      "state.checkpoints.dir": "file:///flink-data/checkpoints",
      "high-availability": "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
      "high-availability.storageDir": "file:///flink-data/ha"
    },
    "ingress": {
      "template": "{{name}}.{{namespace}}.flink.k8s.io"
    },
    "serviceAccount": "flink",
    "jobManager": {
      "resource": {
        "memory": "2048m",
        "cpu": 1
      }
    },
    "taskManager": {
      "resource": {
        "memory": "2048m",
        "cpu": 1
      },
      "podTemplate": {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
          "name": "task-manager-pod-template"
        },
        "spec": {
          "initContainers": [
            {
              "name": "busybox",
              "image": "busybox:latest",
              "command": [
                "sh",
                "-c",
                "echo hello from task manager"
              ]
            }
          ]
        }
      }
    },
    "job": {
      "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
      "parallelism": 2,
      "upgradeMode": "stateless",
      "state": "running",
      "savepointTriggerNonce": 0
    }
  }
}
"""
TEST_APPLICATION_DICT = {
    "apiVersion": "flink.apache.org/v1beta1",
    "kind": "FlinkDeployment",
    "metadata": {"name": "flink-sm-ex", "namespace": "default"},
    "spec": {
        "image": "flink:1.15",
        "flinkVersion": "v1_15",
        "flinkConfiguration": {
            "taskmanager.numberOfTaskSlots": "2",
            "state.savepoints.dir": "file:///flink-data/savepoints",
            "state.checkpoints.dir": "file:///flink-data/checkpoints",
            "high-availability": "org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory",
            "high-availability.storageDir": "file:///flink-data/ha",
        },
        "ingress": {"template": "{{name}}.{{namespace}}.flink.k8s.io"},
        "serviceAccount": "flink",
        "jobManager": {"resource": {"memory": "2048m", "cpu": 1}},
        "taskManager": {
            "resource": {"memory": "2048m", "cpu": 1},
            "podTemplate": {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {"name": "task-manager-pod-template"},
                "spec": {
                    "initContainers": [
                        {
                            "name": "busybox",
                            "image": "busybox:latest",
                            "command": ["sh", "-c", "echo hello from task manager"],
                        }
                    ]
                },
            },
        },
        "job": {
            "jarURI": "local:///opt/flink/examples/streaming/StateMachineExample.jar",
            "parallelism": 2,
            "upgradeMode": "stateless",
            "state": "running",
            "savepointTriggerNonce": 0,
        },
    },
}


@patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn")
class TestFlinkKubernetesOperator:
    def setup_method(self):
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

    @patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
    def test_create_application_from_yaml(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = FlinkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_task_id",
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()

        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group="flink.apache.org",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
    def test_create_application_from_json(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = FlinkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_task_id",
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()

        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group="flink.apache.org",
            namespace="default",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
    def test_create_application_from_json_with_api_group_and_version(
        self, mock_create_namespaced_crd, mock_kubernetes_hook
    ):
        api_group = "flink.apache.org"
        api_version = "v1beta1"
        op = FlinkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_task_id",
            api_group=api_group,
            api_version=api_version,
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()

        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group=api_group,
            namespace="default",
            plural="flinkdeployments",
            version=api_version,
        )

    @patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
    def test_namespace_from_operator(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = FlinkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            namespace="operator_namespace",
            kubernetes_conn_id="kubernetes_with_namespace",
            task_id="test_task_id",
        )
        op.execute(None)
        mock_kubernetes_hook.assert_called_once_with()

        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group="flink.apache.org",
            namespace="operator_namespace",
            plural="flinkdeployments",
            version="v1beta1",
        )

    @patch("kubernetes.client.api.custom_objects_api.CustomObjectsApi.create_namespaced_custom_object")
    def test_namespace_from_connection(self, mock_create_namespaced_crd, mock_kubernetes_hook):
        op = FlinkKubernetesOperator(
            application_file=TEST_VALID_APPLICATION_JSON,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_with_namespace",
            task_id="test_task_id",
        )
        op.execute(None)

        mock_kubernetes_hook.assert_called_once_with()

        mock_create_namespaced_crd.assert_called_with(
            body=TEST_APPLICATION_DICT,
            group="flink.apache.org",
            namespace="mock_namespace",
            plural="flinkdeployments",
            version="v1beta1",
        )

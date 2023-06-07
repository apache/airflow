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

from unittest.mock import patch

import pytest
import yaml

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.utils import timezone

TEST_VALID_RESOURCE_YAML = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: test_pvc
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: standard
  resources:
    requests:
      storage: 5Gi
"""

TEST_VALID_LIST_RESOURCE_YAML = """
apiVersion: v1
kind: List
items:
- apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: test_pvc_1
- apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: test_pvc_2
"""

HOOK_CLASS = "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook"


@patch("airflow.utils.context.Context")
class TestKubernetesXResourceOperator:
    @pytest.fixture(autouse=True)
    def setup_tests(self, dag_maker):
        self._default_client_patch = patch(f"{HOOK_CLASS}._get_default_client")
        self._default_client_mock = self._default_client_patch.start()

        yield

        patch.stopall()

    def setup_method(self):
        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    @patch("kubernetes.client.api.CoreV1Api.create_namespaced_persistent_volume_claim")
    def test_create_application_from_yaml(self, mock_create_namespaced_persistent_volume_claim, context):
        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
        )

        op.execute(context)

        mock_create_namespaced_persistent_volume_claim.assert_called_once_with(
            body=yaml.safe_load(TEST_VALID_RESOURCE_YAML), namespace="default"
        )

    @patch("kubernetes.client.api.CoreV1Api.delete_namespaced_persistent_volume_claim")
    def test_single_delete_application_from_yaml(
        self, mock_delete_namespaced_persistent_volume_claim, context
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
        )

        op.execute(context)

        mock_delete_namespaced_persistent_volume_claim.assert_called()

    @patch("kubernetes.client.api.CoreV1Api.delete_namespaced_persistent_volume_claim")
    def test_multi_delete_application_from_yaml(
        self, mock_delete_namespaced_persistent_volume_claim, context
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_VALID_LIST_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
        )

        op.execute(context)

        mock_delete_namespaced_persistent_volume_claim.assert_called()

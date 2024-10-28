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

from unittest.mock import MagicMock, patch

import pytest
import yaml
from kubernetes.client.rest import ApiException

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

TEST_VALID_CRD_YAML = """
apiVersion: ray.io/v1
kind: RayJob
metadata:
  name: rayjob-sample
spec:
  entrypoint: python /home/ray/program/job.py
  shutdownAfterJobFinishes: true
"""

TEST_NOT_NAMESPACED_CRD_YAML = """
apiVersion: kueue.x-k8s.io/v1beta1
kind: ResourceFlavor
metadata:
  name: default-flavor-test
"""

HOOK_CLASS = "airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook"


@pytest.mark.db_test
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
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.CoreV1Api.create_namespaced_persistent_volume_claim")
    def test_create_application_from_yaml(
        self,
        mock_create_namespaced_persistent_volume_claim,
        mock_load_kube_config,
        context,
    ):
        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            config_file="/foo/bar",
        )

        op.execute(context)

        mock_create_namespaced_persistent_volume_claim.assert_called_once_with(
            body=yaml.safe_load(TEST_VALID_RESOURCE_YAML), namespace="default"
        )

    @patch("kubernetes.client.api.CoreV1Api.create_namespaced_persistent_volume_claim")
    def test_create_application_from_yaml_list(
        self, mock_create_namespaced_persistent_volume_claim, context
    ):
        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_LIST_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
        )

        op.execute(context)

        assert mock_create_namespaced_persistent_volume_claim.call_count == 2

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.CoreV1Api.delete_namespaced_persistent_volume_claim")
    def test_single_delete_application_from_yaml(
        self,
        mock_delete_namespaced_persistent_volume_claim,
        mock_load_kube_config,
        context,
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            config_file="/foo/bar",
        )

        op.execute(context)

        mock_delete_namespaced_persistent_volume_claim.assert_called()

    @patch("kubernetes.config.load_kube_config")
    @patch("kubernetes.client.api.CoreV1Api.delete_namespaced_persistent_volume_claim")
    def test_multi_delete_application_from_yaml(
        self,
        mock_delete_namespaced_persistent_volume_claim,
        mock_load_kube_config,
        context,
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_VALID_LIST_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            config_file="/foo/bar",
        )

        op.execute(context)

        mock_delete_namespaced_persistent_volume_claim.assert_called()

    @patch("kubernetes.client.api.CustomObjectsApi.create_namespaced_custom_object")
    def test_create_custom_application_from_yaml(
        self, mock_create_namespaced_custom_object, context
    ):
        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_CRD_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            custom_resource_definition=True,
        )

        op.execute(context)

        mock_create_namespaced_custom_object.assert_called_once_with(
            "ray.io",
            "v1",
            "default",
            "rayjobs",
            yaml.safe_load(TEST_VALID_CRD_YAML),
        )

    @patch("kubernetes.client.api.CustomObjectsApi.delete_namespaced_custom_object")
    def test_delete_custom_application_from_yaml(
        self, mock_delete_namespaced_custom_object, context
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_VALID_CRD_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            custom_resource_definition=True,
        )

        op.execute(context)

        mock_delete_namespaced_custom_object.assert_called_once_with(
            "ray.io",
            "v1",
            "default",
            "rayjobs",
            "rayjob-sample",
        )

    @patch("kubernetes.client.api.CustomObjectsApi.create_cluster_custom_object")
    def test_create_not_namespaced_custom_app_from_yaml(
        self, mock_create_cluster_custom_object, context
    ):
        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_NOT_NAMESPACED_CRD_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            custom_resource_definition=True,
            namespaced=False,
        )

        op.execute(context)

        mock_create_cluster_custom_object.assert_called_once_with(
            "kueue.x-k8s.io",
            "v1beta1",
            "resourceflavors",
            yaml.safe_load(TEST_NOT_NAMESPACED_CRD_YAML),
        )

    @patch("kubernetes.client.api.CustomObjectsApi.delete_cluster_custom_object")
    def test_delete_not_namespaced_custom_app_from_yaml(
        self, mock_delete_cluster_custom_object, context
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_NOT_NAMESPACED_CRD_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            custom_resource_definition=True,
            namespaced=False,
        )

        op.execute(context)

        mock_delete_cluster_custom_object.assert_called_once_with(
            "kueue.x-k8s.io",
            "v1beta1",
            "resourceflavors",
            "default-flavor-test",
        )

    @patch("kubernetes.config.load_kube_config")
    @patch("airflow.providers.cncf.kubernetes.operators.resource.create_from_yaml")
    def test_create_objects_retries_on_500_error(
        self, mock_create_from_yaml, mock_load_kube_config, context
    ):
        mock_create_from_yaml.side_effect = [
            ApiException(status=500),
            MagicMock(),
        ]

        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            config_file="/foo/bar",
        )
        op.execute(context)

        assert mock_create_from_yaml.call_count == 2

    @patch("kubernetes.config.load_kube_config")
    @patch("airflow.providers.cncf.kubernetes.operators.resource.create_from_yaml")
    def test_create_objects_fails_on_other_exception(
        self, mock_create_from_yaml, mock_load_kube_config, context
    ):
        mock_create_from_yaml.side_effect = [ApiException(status=404)]

        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            config_file="/foo/bar",
        )
        with pytest.raises(ApiException):
            op.execute(context)

    @patch("kubernetes.config.load_kube_config")
    @patch("airflow.providers.cncf.kubernetes.operators.resource.create_from_yaml")
    def test_create_objects_retries_three_times(
        self, mock_create_from_yaml, mock_load_kube_config, context
    ):
        mock_create_from_yaml.side_effect = [
            ApiException(status=500),
            ApiException(status=500),
            ApiException(status=500),
            ApiException(status=500),
        ]

        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default",
            task_id="test_task_id",
            config_file="/foo/bar",
        )
        with pytest.raises(ApiException):
            op.execute(context)

        assert mock_create_from_yaml.call_count == 3

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
from unittest.mock import patch

import yaml

from airflow import DAG
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.resource import (
    KubernetesCreateResourceOperator,
    KubernetesDeleteResourceOperator,
)
from airflow.utils import db, timezone

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


@patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn")
@patch("kubernetes.client.api.CoreV1Api.create_namespaced_persistent_volume_claim")
@patch("kubernetes.client.api.CoreV1Api.delete_namespaced_persistent_volume_claim")
@patch("airflow.utils.context.Context")
class TestKubernetesCreateOperator:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="kubernetes_default_kube_config",
                conn_type="kubernetes",
                extra=json.dumps({}),
            )
        )

        db.merge_conn(
            Connection(
                conn_id="kubernetes_with_namespace",
                conn_type="kubernetes",
                extra=json.dumps({"namespace": "mock_namespace"}),
            )
        )

        args = {"owner": "airflow", "start_date": timezone.datetime(2020, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    def test_create_application_from_yaml(
        self,
        context,
        mock_delete_namespaced_persistent_volume_claim,
        mock_create_namespaced_persistent_volume_claim,
        mock_kubernetes_hook,
    ):
        op = KubernetesCreateResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_task_id",
        )

        op.execute(context)
        mock_kubernetes_hook.assert_called_once_with()

        mock_create_namespaced_persistent_volume_claim.assert_called_once_with(
            body=yaml.safe_load(TEST_VALID_RESOURCE_YAML), namespace="default"
        )

    def test_delete_application_from_yaml(
        self,
        context,
        mock_delete_namespaced_persistent_volume_claim,
        mock_create_namespaced_persistent_volume_claim,
        mock_kubernetes_hook,
    ):
        op = KubernetesDeleteResourceOperator(
            yaml_conf=TEST_VALID_RESOURCE_YAML,
            dag=self.dag,
            kubernetes_conn_id="kubernetes_default_kube_config",
            task_id="test_task_id",
        )

        op.execute(context)
        mock_kubernetes_hook.assert_called_once_with()
        mock_delete_namespaced_persistent_volume_claim.assert_called_once()

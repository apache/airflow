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

import json
import os
import unittest
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.google.cloud.operators.kubernetes_engine import (
    GKECreateClusterOperator,
    GKEDeleteClusterOperator,
    GKEStartPodOperator,
)

TEST_GCP_PROJECT_ID = "test-id"
PROJECT_LOCATION = "test-location"
PROJECT_TASK_ID = "test-task-id"
CLUSTER_NAME = "test-cluster-name"

PROJECT_BODY = {"name": "test-name"}
PROJECT_BODY_CREATE_DICT = {"name": "test-name", "initial_node_count": 1}
PROJECT_BODY_CREATE_DICT_NODE_POOLS = {
    "name": "test-name",
    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
}

PROJECT_BODY_CREATE_CLUSTER = type("Cluster", (object,), {"name": "test-name", "initial_node_count": 1})()
PROJECT_BODY_CREATE_CLUSTER_NODE_POOLS = type(
    "Cluster",
    (object,),
    {"name": "test-name", "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}]},
)()

TASK_NAME = "test-task-name"
NAMESPACE = ("default",)
IMAGE = "bash"

GCLOUD_COMMAND = "gcloud container clusters get-credentials {} --zone {} --project {}"
KUBE_ENV_VAR = "KUBECONFIG"
FILE_NAME = "/tmp/mock_name"
KUB_OP_PATH = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.{}"
GKE_HOOK_MODULE_PATH = "airflow.providers.google.cloud.operators.kubernetes_engine"
GKE_HOOK_PATH = f"{GKE_HOOK_MODULE_PATH}.GKEClusterHook"
KUB_OPERATOR_EXEC = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesPodOperator.execute"
TEMP_FILE = "tempfile.NamedTemporaryFile"
GKE_OP_PATH = "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator"
CLUSTER_URL = "https://test-host"
SSL_CA_CERT = "TEST_SSL_CA_CERT_CONTENT"


class TestGoogleCloudPlatformContainerOperator(unittest.TestCase):
    @parameterized.expand(
        (body,)
        for body in [
            PROJECT_BODY_CREATE_DICT,
            PROJECT_BODY_CREATE_DICT_NODE_POOLS,
            PROJECT_BODY_CREATE_CLUSTER,
            PROJECT_BODY_CREATE_CLUSTER_NODE_POOLS,
        ]
    )
    @mock.patch(GKE_HOOK_PATH)
    @mock.patch(f"{GKE_HOOK_MODULE_PATH}.Cluster.to_dict")
    def test_create_execute(self, body, mock_to_dict, mock_hook):
        operator = GKECreateClusterOperator(
            project_id=TEST_GCP_PROJECT_ID, location=PROJECT_LOCATION, body=body, task_id=PROJECT_TASK_ID
        )

        operator.execute(context=mock.MagicMock())
        mock_hook.return_value.create_cluster.assert_called_once_with(
            cluster=body, project_id=TEST_GCP_PROJECT_ID
        )

    @parameterized.expand(
        (body,)
        for body in [
            None,
            {"missing_name": "test-name", "initial_node_count": 1},
            {
                "name": "test-name",
                "initial_node_count": 1,
                "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            {"missing_name": "test-name", "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}]},
            {
                "name": "test-name",
                "missing_initial_node_count": 1,
                "missing_node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
            },
            type("Cluster", (object,), {"missing_name": "test-name", "initial_node_count": 1})(),
            type(
                "Cluster",
                (object,),
                {
                    "missing_name": "test-name",
                    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "name": "test-name",
                    "missing_initial_node_count": 1,
                    "missing_node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
            type(
                "Cluster",
                (object,),
                {
                    "name": "test-name",
                    "initial_node_count": 1,
                    "node_pools": [{"name": "a_node_pool", "initial_node_count": 1}],
                },
            )(),
        ]
    )
    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute_error_body(self, body, mock_hook):
        with pytest.raises(AirflowException):
            GKECreateClusterOperator(
                project_id=TEST_GCP_PROJECT_ID, location=PROJECT_LOCATION, body=body, task_id=PROJECT_TASK_ID
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute_error_project_id(self, mock_hook):
        with pytest.raises(AirflowException):
            GKECreateClusterOperator(location=PROJECT_LOCATION, body=PROJECT_BODY, task_id=PROJECT_TASK_ID)

    @mock.patch(GKE_HOOK_PATH)
    def test_create_execute_error_location(self, mock_hook):
        with pytest.raises(AirflowException):
            GKECreateClusterOperator(
                project_id=TEST_GCP_PROJECT_ID, body=PROJECT_BODY, task_id=PROJECT_TASK_ID
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute(self, mock_hook):
        operator = GKEDeleteClusterOperator(
            project_id=TEST_GCP_PROJECT_ID,
            name=CLUSTER_NAME,
            location=PROJECT_LOCATION,
            task_id=PROJECT_TASK_ID,
        )

        operator.execute(None)
        mock_hook.return_value.delete_cluster.assert_called_once_with(
            name=CLUSTER_NAME, project_id=TEST_GCP_PROJECT_ID
        )

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute_error_project_id(self, mock_hook):
        with pytest.raises(AirflowException):
            GKEDeleteClusterOperator(location=PROJECT_LOCATION, name=CLUSTER_NAME, task_id=PROJECT_TASK_ID)

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute_error_cluster_name(self, mock_hook):
        with pytest.raises(AirflowException):
            GKEDeleteClusterOperator(
                project_id=TEST_GCP_PROJECT_ID, location=PROJECT_LOCATION, task_id=PROJECT_TASK_ID
            )

    @mock.patch(GKE_HOOK_PATH)
    def test_delete_execute_error_location(self, mock_hook):
        with pytest.raises(AirflowException):
            GKEDeleteClusterOperator(
                project_id=TEST_GCP_PROJECT_ID, name=CLUSTER_NAME, task_id=PROJECT_TASK_ID
            )


class TestGKEPodOperator(unittest.TestCase):
    def setUp(self):
        self.gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
        )
        self.gke_op.pod = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )

    def test_template_fields(self):
        assert set(KubernetesPodOperator.template_fields).issubset(GKEStartPodOperator.template_fields)

    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_execute(self, fetch_cluster_info_mock, file_mock, exec_mock):
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    def test_config_file_throws_error(self):
        with pytest.raises(AirflowException):
            GKEStartPodOperator(
                project_id=TEST_GCP_PROJECT_ID,
                location=PROJECT_LOCATION,
                cluster_name=CLUSTER_NAME,
                task_id=PROJECT_TASK_ID,
                name=TASK_NAME,
                namespace=NAMESPACE,
                image=IMAGE,
                config_file="/path/to/alternative/kubeconfig",
            )

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connections",
        return_value=[Connection(extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'}))],
    )
    @mock.patch(KUB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_execute_with_impersonation_service_account(
        self, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        self.gke_op.impersonation_chain = "test_account@example.com"
        self.gke_op.execute(context=mock.MagicMock())
        fetch_cluster_info_mock.assert_called_once()

    @mock.patch.dict(os.environ, {})
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connections",
        return_value=[Connection(extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'}))],
    )
    @mock.patch(KUB_OPERATOR_EXEC)
    @mock.patch(TEMP_FILE)
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_execute_with_impersonation_service_chain_one_element(
        self, fetch_cluster_info_mock, file_mock, exec_mock, get_con_mock
    ):
        self.gke_op.impersonation_chain = ["test_account@example.com"]
        self.gke_op.execute(context=mock.MagicMock())

        fetch_cluster_info_mock.assert_called_once()


class TestGKEPodOperatorAsync(unittest.TestCase):
    def setUp(self):
        self.gke_op = GKEStartPodOperator(
            project_id=TEST_GCP_PROJECT_ID,
            location=PROJECT_LOCATION,
            cluster_name=CLUSTER_NAME,
            task_id=PROJECT_TASK_ID,
            name=TASK_NAME,
            namespace=NAMESPACE,
            image=IMAGE,
            deferrable=True,
        )
        self.gke_op.pod = mock.MagicMock(
            name=TASK_NAME,
            namespace=NAMESPACE,
        )
        self.gke_op._cluster_url = CLUSTER_URL
        self.gke_op._ssl_ca_cert = SSL_CA_CERT

    @mock.patch(KUB_OP_PATH.format("convert_config_file_to_dict"))
    @mock.patch.dict(os.environ, {})
    @mock.patch(KUB_OP_PATH.format("build_pod_request_obj"))
    @mock.patch(KUB_OP_PATH.format("get_or_create_pod"))
    @mock.patch(
        "airflow.hooks.base.BaseHook.get_connections",
        return_value=[Connection(extra=json.dumps({"keyfile_dict": '{"private_key": "r4nd0m_k3y"}'}))],
    )
    @mock.patch(f"{GKE_OP_PATH}.fetch_cluster_info")
    def test_async_create_pod_should_execute_successfully(
        self, fetch_cluster_info_mock, get_con_mock, mocked_pod, mocked_pod_obj, mocked_config
    ):
        with pytest.raises(TaskDeferred):
            self.gke_op._cluster_url = CLUSTER_URL
            self.gke_op._ssl_ca_cert = SSL_CA_CERT
            self.gke_op.execute(context=mock.MagicMock())
            fetch_cluster_info_mock.assert_called_once()

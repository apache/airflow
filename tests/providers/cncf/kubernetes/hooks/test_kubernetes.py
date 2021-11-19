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
import os
import tempfile
import unittest
from unittest import mock
from unittest.mock import patch

import kubernetes
import pytest
from parameterized import parameterized

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils import db
from tests.test_utils.db import clear_db_connections

KUBE_CONFIG_PATH = os.getenv('KUBECONFIG', '~/.kube/config')


class TestKubernetesHook(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        for conn_id, extra in [
            ('kubernetes_in_cluster', {'extra__kubernetes__in_cluster': True}),
            ('kubernetes_kube_config', {'extra__kubernetes__kube_config': '{"test": "kube"}'}),
            ('kubernetes_kube_config_path', {'extra__kubernetes__kube_config_path': 'path/to/file'}),
            ('kubernetes_in_cluster_empty', {'extra__kubernetes__in_cluster': ''}),
            ('kubernetes_kube_config_empty', {'extra__kubernetes__kube_config': ''}),
            ('kubernetes_kube_config_path_empty', {'extra__kubernetes__kube_config_path': ''}),
            ('kubernetes_with_namespace', {'extra__kubernetes__namespace': 'mock_namespace'}),
            ('kubernetes_default_kube_config', {}),
        ]:
            db.merge_conn(Connection(conn_type='kubernetes', conn_id=conn_id, extra=json.dumps(extra)))

    @classmethod
    def tearDownClass(cls) -> None:
        clear_db_connections()

    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    def test_in_cluster_connection(self, mock_kube_config_loader):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_in_cluster')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_loader.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    def test_in_cluster_connection_empty(self, mock_kube_config_merger, mock_kube_config_loader):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_in_cluster_empty')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_loader.assert_called_once_with(KUBE_CONFIG_PATH)
        mock_kube_config_merger.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_kube_config_path(self, mock_kube_config_loader, mock_kube_config_merger):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_kube_config_path')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_loader.assert_called_once_with("path/to/file")
        mock_kube_config_merger.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_kube_config_path_empty(self, mock_kube_config_loader, mock_kube_config_merger):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_kube_config_path_empty')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_loader.assert_called_once_with(KUBE_CONFIG_PATH)
        mock_kube_config_merger.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch.object(tempfile, 'NamedTemporaryFile')
    def test_kube_config_connection(self, mock_kube_config_loader, mock_kube_config_merger, mock_tempfile):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_kube_config')
        api_conn = kubernetes_hook.get_conn()
        mock_tempfile.is_called_once()
        mock_kube_config_loader.assert_called_once()
        mock_kube_config_merger.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_kube_config_connection_empty(self, mock_kube_config_loader, mock_kube_config_merger):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_kube_config_empty')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_loader.assert_called_once_with(KUBE_CONFIG_PATH)
        mock_kube_config_merger.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION", "/mock/config")
    def test_default_kube_config_connection(
        self,
        mock_kube_config_loader,
        mock_kube_config_merger,
    ):
        kubernetes_hook = KubernetesHook(conn_id='kubernetes_default_kube_config')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_loader.assert_called_once_with("/mock/config")
        mock_kube_config_merger.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    def test_get_namespace(self):
        kubernetes_hook_with_namespace = KubernetesHook(conn_id='kubernetes_with_namespace')
        kubernetes_hook_without_namespace = KubernetesHook(conn_id='kubernetes_default_kube_config')
        assert kubernetes_hook_with_namespace.get_namespace() == 'mock_namespace'
        assert kubernetes_hook_without_namespace.get_namespace() == 'default'


class TestKubernetesHookIncorrectConfiguration(unittest.TestCase):
    @parameterized.expand(
        (
            "kubernetes://?extra__kubernetes__kube_config_path=/tmp/&extra__kubernetes__kube_config=[1,2,3]",
            "kubernetes://?extra__kubernetes__kube_config_path=/tmp/&extra__kubernetes__in_cluster=[1,2,3]",
            "kubernetes://?extra__kubernetes__kube_config=/tmp/&extra__kubernetes__in_cluster=[1,2,3]",
        )
    )
    def test_should_raise_exception_on_invalid_configuration(self, conn_uri):
        with mock.patch.dict("os.environ", AIRFLOW_CONN_KUBERNETES_DEFAULT=conn_uri), pytest.raises(
            AirflowException, match="Invalid connection configuration"
        ):
            kubernetes_hook = KubernetesHook()
            kubernetes_hook.get_conn()

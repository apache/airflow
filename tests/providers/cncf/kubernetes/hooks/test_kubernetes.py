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
from unittest import mock
from unittest.mock import MagicMock, patch

import kubernetes
import pytest
from kubernetes.config import ConfigException

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.utils import db
from tests.test_utils.db import clear_db_connections

KUBE_CONFIG_PATH = os.getenv('KUBECONFIG', '~/.kube/config')
HOOK_MODULE = "airflow.providers.cncf.kubernetes.hooks.kubernetes"


class TestKubernetesHook:
    @classmethod
    def setup_class(cls) -> None:
        for conn_id, extra in [
            ('in_cluster', {'extra__kubernetes__in_cluster': True}),
            ('in_cluster_empty', {'extra__kubernetes__in_cluster': ''}),
            ('kube_config', {'extra__kubernetes__kube_config': '{"test": "kube"}'}),
            ('kube_config_path', {'extra__kubernetes__kube_config_path': 'path/to/file'}),
            ('kube_config_empty', {'extra__kubernetes__kube_config': ''}),
            ('kube_config_path_empty', {'extra__kubernetes__kube_config_path': ''}),
            ('kube_config_empty', {'extra__kubernetes__kube_config': ''}),
            ('kube_config_path_empty', {'extra__kubernetes__kube_config_path': ''}),
            ('context_empty', {'extra__kubernetes__cluster_context': ''}),
            ('context', {'extra__kubernetes__cluster_context': 'my-context'}),
            ('with_namespace', {'extra__kubernetes__namespace': 'mock_namespace'}),
            ('default_kube_config', {}),
            ('disable_verify_ssl', {'extra__kubernetes__disable_verify_ssl': True}),
            ('disable_verify_ssl_empty', {'extra__kubernetes__disable_verify_ssl': ''}),
            ('disable_tcp_keepalive', {'extra__kubernetes__disable_tcp_keepalive': True}),
            ('disable_tcp_keepalive_empty', {'extra__kubernetes__disable_tcp_keepalive': ''}),
        ]:
            db.merge_conn(Connection(conn_type='kubernetes', conn_id=conn_id, extra=json.dumps(extra)))

    @classmethod
    def teardown_class(cls) -> None:
        clear_db_connections()

    @pytest.mark.parametrize(
        'in_cluster_param, conn_id, in_cluster_called',
        (
            (True, None, True),
            (None, None, False),
            (False, None, False),
            (None, 'in_cluster', True),
            (True, 'in_cluster', True),
            (False, 'in_cluster', False),
            (None, 'in_cluster_empty', False),
            (True, 'in_cluster_empty', True),
            (False, 'in_cluster_empty', False),
        ),
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    @patch(f"{HOOK_MODULE}.KubernetesHook._get_default_client")
    def test_in_cluster_connection(
        self,
        mock_get_default_client,
        mock_in_cluster_loader,
        mock_merger,
        mock_loader,
        in_cluster_param,
        conn_id,
        in_cluster_called,
    ):
        """
        Verifies whether in_cluster is called depending on combination of hook param and connection extra.
        Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, in_cluster=in_cluster_param)
        mock_get_default_client.return_value = kubernetes.client.api_client.ApiClient()
        api_conn = kubernetes_hook.get_conn()
        if in_cluster_called:
            mock_in_cluster_loader.assert_called_once()
            mock_merger.assert_not_called()
            mock_loader.assert_not_called()
        else:
            mock_get_default_client.assert_called()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize('in_cluster_fails', [True, False])
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    def test_get_default_client(
        self,
        mock_incluster,
        mock_merger,
        mock_loader,
        in_cluster_fails,
    ):
        """
        Verifies the behavior of the ``_get_default_client`` function.  It should try the "in cluster"
        loader first but if that fails, try to use the default kubeconfig file.
        """
        if in_cluster_fails:
            mock_incluster.side_effect = ConfigException('any')
        kubernetes_hook = KubernetesHook()
        api_conn = kubernetes_hook._get_default_client()
        if in_cluster_fails:
            mock_incluster.assert_called_once()
            mock_merger.assert_called_once_with(KUBE_CONFIG_PATH)
            mock_loader.assert_called_once()
        else:
            mock_incluster.assert_called_once()
            mock_merger.assert_not_called()
            mock_loader.assert_not_called()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        'disable_verify_ssl, conn_id, disable_called',
        (
            (True, None, True),
            (None, None, False),
            (False, None, False),
            (None, 'disable_verify_ssl', True),
            (True, 'disable_verify_ssl', True),
            (False, 'disable_verify_ssl', False),
            (None, 'disable_verify_ssl_empty', False),
            (True, 'disable_verify_ssl_empty', True),
            (False, 'disable_verify_ssl_empty', False),
        ),
    )
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader", new=MagicMock())
    @patch(f"{HOOK_MODULE}._disable_verify_ssl")
    def test_disable_verify_ssl(
        self,
        mock_disable,
        disable_verify_ssl,
        conn_id,
        disable_called,
    ):
        """
        Verifies whether disable verify ssl is called depending on combination of hook param and
        connection extra. Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, disable_verify_ssl=disable_verify_ssl)
        api_conn = kubernetes_hook.get_conn()
        assert mock_disable.called is disable_called
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        'disable_tcp_keepalive, conn_id, expected',
        (
            (True, None, False),
            (None, None, True),
            (False, None, True),
            (None, 'disable_tcp_keepalive', False),
            (True, 'disable_tcp_keepalive', False),
            (False, 'disable_tcp_keepalive', True),
            (None, 'disable_tcp_keepalive_empty', True),
            (True, 'disable_tcp_keepalive_empty', False),
            (False, 'disable_tcp_keepalive_empty', True),
        ),
    )
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader", new=MagicMock())
    @patch(f"{HOOK_MODULE}._enable_tcp_keepalive")
    def test_disable_tcp_keepalive(
        self,
        mock_enable,
        disable_tcp_keepalive,
        conn_id,
        expected,
    ):
        """
        Verifies whether enable tcp keepalive is called depending on combination of hook
        param and connection extra. Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, disable_tcp_keepalive=disable_tcp_keepalive)
        api_conn = kubernetes_hook.get_conn()
        assert mock_enable.called is expected
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        'config_path_param, conn_id, call_path',
        (
            (None, None, KUBE_CONFIG_PATH),
            ('/my/path/override', None, '/my/path/override'),
            (None, 'kube_config_path', 'path/to/file'),
            ('/my/path/override', 'kube_config_path', '/my/path/override'),
            (None, 'kube_config_path_empty', KUBE_CONFIG_PATH),
            ('/my/path/override', 'kube_config_path_empty', '/my/path/override'),
        ),
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_kube_config_path(
        self, mock_kube_config_merger, mock_kube_config_loader, config_path_param, conn_id, call_path
    ):
        """
        Verifies kube config path depending on combination of hook param and connection extra.
        Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, config_file=config_path_param)
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_merger.assert_called_once_with(call_path)
        mock_kube_config_loader.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        'conn_id, has_config',
        (
            (None, False),
            ('kube_config', True),
            ('kube_config_empty', False),
        ),
    )
    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch.object(tempfile, 'NamedTemporaryFile')
    def test_kube_config_connection(
        self, mock_tempfile, mock_kube_config_merger, mock_kube_config_loader, conn_id, has_config
    ):
        """
        Verifies whether temporary kube config file is created.
        """
        mock_tempfile.return_value.__enter__.return_value.name = "fake-temp-file"
        mock_kube_config_merger.return_value.config = {"fake_config": "value"}
        kubernetes_hook = KubernetesHook(conn_id=conn_id)
        api_conn = kubernetes_hook.get_conn()
        if has_config:
            mock_tempfile.is_called_once()
            mock_kube_config_loader.assert_called_once()
            mock_kube_config_merger.assert_called_once_with('fake-temp-file')
        else:
            mock_tempfile.assert_not_called()
            mock_kube_config_loader.assert_called_once()
            mock_kube_config_merger.assert_called_once_with(KUBE_CONFIG_PATH)
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        'context_param, conn_id, expected_context',
        (
            ('param-context', None, 'param-context'),
            (None, None, None),
            ('param-context', 'context', 'param-context'),
            (None, 'context', 'my-context'),
            ('param-context', 'context_empty', 'param-context'),
            (None, 'context_empty', None),
        ),
    )
    @patch("kubernetes.config.load_kube_config")
    def test_cluster_context(self, mock_load_kube_config, context_param, conn_id, expected_context):
        """
        Verifies cluster context depending on combination of hook param and connection extra.
        Hook param should beat extra.
        """
        kubernetes_hook = KubernetesHook(conn_id=conn_id, cluster_context=context_param)
        kubernetes_hook.get_conn()
        mock_load_kube_config.assert_called_with(client_configuration=None, context=expected_context)

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    @patch("kubernetes.config.kube_config.KUBE_CONFIG_DEFAULT_LOCATION", "/mock/config")
    def test_default_kube_config_connection(self, mock_kube_config_merger, mock_kube_config_loader):
        kubernetes_hook = KubernetesHook(conn_id='default_kube_config')
        api_conn = kubernetes_hook.get_conn()
        mock_kube_config_merger.assert_called_once_with("/mock/config")
        mock_kube_config_loader.assert_called_once()
        assert isinstance(api_conn, kubernetes.client.api_client.ApiClient)

    @pytest.mark.parametrize(
        'conn_id, expected',
        (
            pytest.param(None, None, id='no-conn-id'),
            pytest.param('with_namespace', 'mock_namespace', id='conn-with-namespace'),
            pytest.param('default_kube_config', 'default', id='conn-without-namespace'),
        ),
    )
    def test_get_namespace(self, conn_id, expected):
        hook = KubernetesHook(conn_id=conn_id)
        assert hook.get_namespace() == expected

    @patch("kubernetes.config.kube_config.KubeConfigLoader")
    @patch("kubernetes.config.kube_config.KubeConfigMerger")
    def test_client_types(self, mock_kube_config_merger, mock_kube_config_loader):
        hook = KubernetesHook(None)
        assert isinstance(hook.core_v1_client, kubernetes.client.CoreV1Api)
        assert isinstance(hook.api_client, kubernetes.client.ApiClient)
        assert isinstance(hook.get_conn(), kubernetes.client.ApiClient)

    @patch(f"{HOOK_MODULE}._disable_verify_ssl")
    @patch(f"{HOOK_MODULE}.KubernetesHook._get_default_client", new=MagicMock)
    def test_patch_core_settings_verify_ssl(self, mock_disable_verify_ssl):
        hook = KubernetesHook()
        hook.get_conn()
        mock_disable_verify_ssl.assert_not_called()
        mock_disable_verify_ssl.reset_mock()
        hook._deprecated_core_disable_verify_ssl = True
        hook.get_conn()
        mock_disable_verify_ssl.assert_called()

    @patch(f"{HOOK_MODULE}._enable_tcp_keepalive")
    @patch(f"{HOOK_MODULE}.KubernetesHook._get_default_client", new=MagicMock)
    def test_patch_core_settings_tcp_keepalive(self, mock_enable_tcp_keepalive):
        hook = KubernetesHook()
        hook.get_conn()
        mock_enable_tcp_keepalive.assert_called()
        mock_enable_tcp_keepalive.reset_mock()
        hook._deprecated_core_disable_tcp_keepalive = True
        hook.get_conn()
        mock_enable_tcp_keepalive.assert_not_called()

    @patch("kubernetes.config.kube_config.KubeConfigLoader", new=MagicMock())
    @patch("kubernetes.config.kube_config.KubeConfigMerger", new=MagicMock())
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    @patch(f"{HOOK_MODULE}.KubernetesHook._get_default_client")
    def test_patch_core_settings_in_cluster(self, mock_get_default_client, mock_in_cluster_loader):
        hook = KubernetesHook(conn_id=None)
        hook.get_conn()
        mock_in_cluster_loader.assert_not_called()
        mock_in_cluster_loader.reset_mock()
        hook._deprecated_core_in_cluster = False
        hook.get_conn()
        mock_in_cluster_loader.assert_not_called()
        mock_get_default_client.assert_called()

    @pytest.mark.parametrize(
        'key, key_val, attr, attr_val',
        [
            ('in_cluster', False, '_deprecated_core_in_cluster', False),
            ('verify_ssl', False, '_deprecated_core_disable_verify_ssl', True),
            ('cluster_context', 'hi', '_deprecated_core_cluster_context', 'hi'),
            ('config_file', '/path/to/file.txt', '_deprecated_core_config_file', '/path/to/file.txt'),
            ('enable_tcp_keepalive', False, '_deprecated_core_disable_tcp_keepalive', True),
        ],
    )
    @patch("kubernetes.config.incluster_config.InClusterConfigLoader", new=MagicMock())
    @patch("kubernetes.config.kube_config.KubeConfigLoader", new=MagicMock())
    @patch("kubernetes.config.kube_config.KubeConfigMerger", new=MagicMock())
    def test_core_settings_warnings(self, key, key_val, attr, attr_val):
        hook = KubernetesHook(conn_id=None)
        setattr(hook, attr, attr_val)
        with pytest.warns(DeprecationWarning, match=rf'.*Airflow settings.*\n.*{key}={key_val!r}.*'):
            hook.get_conn()


class TestKubernetesHookIncorrectConfiguration:
    @pytest.mark.parametrize(
        'conn_uri',
        (
            "kubernetes://?extra__kubernetes__kube_config_path=/tmp/&extra__kubernetes__kube_config=[1,2,3]",
            "kubernetes://?extra__kubernetes__kube_config_path=/tmp/&extra__kubernetes__in_cluster=[1,2,3]",
            "kubernetes://?extra__kubernetes__kube_config=/tmp/&extra__kubernetes__in_cluster=[1,2,3]",
        ),
    )
    def test_should_raise_exception_on_invalid_configuration(self, conn_uri):
        with mock.patch.dict("os.environ", AIRFLOW_CONN_KUBERNETES_DEFAULT=conn_uri), pytest.raises(
            AirflowException, match="Invalid connection configuration"
        ):
            kubernetes_hook = KubernetesHook()
            kubernetes_hook.get_conn()

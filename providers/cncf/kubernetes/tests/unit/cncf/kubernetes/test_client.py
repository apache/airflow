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

import socket
from unittest import mock

import pytest
from kubernetes.client import Configuration

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.cncf.kubernetes.kube_client import (
    _disable_verify_ssl,
    _enable_tcp_keepalive,
    _TimeoutK8sApiClient,
    enable_tcp_keepalive,
    get_kube_client,
)

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def restore_default_configuration():
    original = Configuration.get_default_copy()
    yield
    Configuration.set_default(original)


class TestClient:
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    def test_load_cluster_config(self, config):
        get_kube_client(in_cluster=True)
        config.load_incluster_config.assert_called()
        config.load_kube_config.assert_not_called()

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    def test_load_file_config(self, config):
        get_kube_client(in_cluster=False)
        config.load_incluster_config.assert_not_called()
        config.load_kube_config.assert_called()

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.conf")
    def test_load_config_disable_ssl(self, conf, config):
        conf.getboolean.return_value = False
        conf.getjson.return_value = {"total": 3, "backoff_factor": 0.5}
        client = get_kube_client(in_cluster=False)
        conf.getboolean.assert_called_with("kubernetes_executor", "verify_ssl")
        assert not client.api_client.configuration.verify_ssl

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.conf")
    def test_load_config_ssl_ca_cert(self, conf, config):
        conf.get.return_value = "/path/to/ca.crt"
        conf.getjson.return_value = {"total": 3, "backoff_factor": 0.5}
        client = get_kube_client(in_cluster=False)
        conf.get.assert_called_with("kubernetes_executor", "ssl_ca_cert")
        assert client.api_client.configuration.ssl_ca_cert == "/path/to/ca.crt"

    @pytest.mark.platform("linux")
    def test_enable_tcp_keepalive(self):
        configuration = Configuration()
        configuration.socket_options = None

        enable_tcp_keepalive(configuration)

        assert configuration.socket_options == [
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
        ]

    @pytest.mark.platform("linux")
    def test_enable_tcp_keepalive_preserves_existing_options(self):
        configuration = Configuration()
        configuration.socket_options = [
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 1234),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5),
        ]

        enable_tcp_keepalive(configuration)

        assert configuration.socket_options == [
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 1234),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
        ]

    @pytest.mark.platform("linux")
    def test_enable_tcp_keepalive_is_idempotent(self):
        configuration = Configuration()
        configuration.socket_options = None

        enable_tcp_keepalive(configuration)
        first_call_options = list(configuration.socket_options)
        enable_tcp_keepalive(configuration)

        assert configuration.socket_options == first_call_options

    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.log", autospec=True)
    def test_enable_tcp_keepalive_unsupported_kubernetes_client(self, mock_log):
        configuration = mock.create_autospec(Configuration, instance=True, spec_set=True)
        del configuration.socket_options

        enable_tcp_keepalive(configuration)

        mock_log.warning.assert_called_once()
        assert not hasattr(configuration, "socket_options")

    @pytest.mark.platform("linux")
    @pytest.mark.parametrize("enabled", [True, False])
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config", autospec=True)
    def test_get_kube_client_tcp_keepalive(self, mock_config, enabled):
        with conf_vars({("kubernetes_executor", "enable_tcp_keepalive"): str(enabled)}):
            client = get_kube_client(in_cluster=False)

        configuration_options = client.api_client.configuration.socket_options
        pool_options = client.api_client.rest_client.pool_manager.connection_pool_kw.get("socket_options")
        keepalive = (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if enabled:
            assert keepalive in configuration_options
            assert keepalive in pool_options
        else:
            assert not configuration_options or keepalive not in configuration_options
            assert not pool_options or keepalive not in pool_options

    @pytest.mark.platform("linux")
    @pytest.mark.parametrize("enabled", [True, False])
    def test_timeout_api_client_tcp_keepalive(self, enabled):
        default_options = Configuration.get_default_copy().socket_options

        api_client = _TimeoutK8sApiClient(should_enable_tcp_keepalive=enabled)

        keepalive = (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        options = api_client.configuration.socket_options or []
        assert (keepalive in options) is enabled
        assert Configuration.get_default_copy().socket_options == default_options

    @pytest.mark.platform("linux")
    def test_deprecated_enable_tcp_keepalive_sets_default_configuration(self, restore_default_configuration):
        with pytest.warns(AirflowProviderDeprecationWarning):
            _enable_tcp_keepalive()

        options = Configuration.get_default_copy().socket_options
        assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options
        assert (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120) in options

    def test_disable_verify_ssl(self):
        configuration = Configuration()
        assert configuration.verify_ssl

        _disable_verify_ssl()

        # Support wide range of kube client libraries
        if hasattr(Configuration, "get_default_copy"):
            configuration = Configuration.get_default_copy()
        else:
            configuration = Configuration()
        assert not configuration.verify_ssl

    @mock.patch("kubernetes.config.incluster_config.InClusterConfigLoader")
    @conf_vars(
        {("kubernetes_executor", "api_client_retry_configuration"): '{"total": 3, "backoff_factor": 0.5}'}
    )
    def test_api_client_retry_configuration_correct_values(self, mock_in_cluster_loader):
        get_kube_client(in_cluster=True)
        client_configuration = mock_in_cluster_loader().load_and_set.call_args.args[0]
        assert client_configuration.retries.total == 3
        assert client_configuration.retries.backoff_factor == 0.5

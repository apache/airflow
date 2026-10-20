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
from urllib3.connection import HTTPConnection

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.cncf.kubernetes.kube_client import (
    _disable_verify_ssl,
    _enable_tcp_keepalive,
    _TimeoutK8sApiClient,
    configure_tcp_keepalive,
    get_kube_client,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

GLOBAL_CONNECTION_SETTINGS = {
    ("kubernetes_executor", "in_cluster"): "False",
    ("kubernetes_executor", "config_file"): "/global/kubeconfig",
    ("kubernetes_executor", "cluster_context"): "global-context",
    ("kubernetes_executor", "ssl_ca_cert"): "/global/ca.crt",
    ("kubernetes_executor", "api_client_retry_configuration"): '{"total": 1}',
}
TEAM_A_CONNECTION_SETTINGS = {
    "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__IN_CLUSTER": "False",
    "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__CONFIG_FILE": "/team-a/kubeconfig",
    "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__CLUSTER_CONTEXT": "team-a-context",
    "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__SSL_CA_CERT": "/team-a/ca.crt",
    "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__API_CLIENT_RETRY_CONFIGURATION": '{"total": 7}',
}


@pytest.fixture
def fresh_default_configuration():
    default = Configuration.get_default_copy()
    Configuration.set_default(Configuration())
    yield
    Configuration.set_default(default)


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
    @pytest.mark.parametrize(
        ("config_overrides", "expected_idle", "expected_intvl", "expected_cnt"),
        [
            pytest.param({}, 120, 30, 6, id="defaults"),
            pytest.param(
                {
                    ("kubernetes_executor", "tcp_keep_idle"): "60",
                    ("kubernetes_executor", "tcp_keep_intvl"): "10",
                    ("kubernetes_executor", "tcp_keep_cnt"): "3",
                },
                60,
                10,
                3,
                id="custom",
            ),
        ],
    )
    def test_configure_tcp_keepalive(self, config_overrides, expected_idle, expected_intvl, expected_cnt):
        configuration = Configuration()
        configuration.socket_options = None

        with conf_vars(config_overrides):
            configure_tcp_keepalive(configuration)

        assert configuration.socket_options == [
            *HTTPConnection.default_socket_options,
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, expected_idle),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, expected_intvl),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, expected_cnt),
        ]

    @pytest.mark.platform("linux")
    def test_configure_tcp_keepalive_preserves_existing_options(self):
        configuration = Configuration()
        configuration.socket_options = [
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 1234),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 0),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5),
        ]

        configure_tcp_keepalive(configuration)

        assert configuration.socket_options == [
            (socket.SOL_SOCKET, socket.SO_RCVBUF, 1234),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
        ]

    @pytest.mark.platform("linux")
    def test_configure_tcp_keepalive_is_idempotent(self):
        configuration = Configuration()
        configuration.socket_options = None

        configure_tcp_keepalive(configuration)
        first_call_options = list(configuration.socket_options)
        configure_tcp_keepalive(configuration)

        assert configuration.socket_options == first_call_options

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
    @pytest.mark.parametrize("supply_configuration", [True, False])
    @pytest.mark.parametrize("disable_verify_ssl", [True, False])
    @pytest.mark.parametrize("enabled", [True, False])
    def test_timeout_api_client_configuration(self, enabled, disable_verify_ssl, supply_configuration):
        default = Configuration.get_default_copy()
        configuration = Configuration() if supply_configuration else None
        expected_verify_ssl = False if disable_verify_ssl else (configuration or default).verify_ssl

        api_client = _TimeoutK8sApiClient(
            configuration,
            disable_verify_ssl=disable_verify_ssl,
            enable_tcp_keepalive=enabled,
        )

        if supply_configuration:
            assert api_client.configuration is configuration
        assert api_client.configuration.verify_ssl is expected_verify_ssl
        keepalive = (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        options = api_client.configuration.socket_options or []
        assert (keepalive in options) is enabled
        unchanged_default = Configuration.get_default_copy()
        assert unchanged_default.socket_options == default.socket_options
        assert unchanged_default.verify_ssl == default.verify_ssl

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

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @conf_vars(GLOBAL_CONNECTION_SETTINGS)
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    def test_team_uses_its_own_connection_settings(self, config, monkeypatch):
        for key, value in TEAM_A_CONNECTION_SETTINGS.items():
            monkeypatch.setenv(key, value)

        client = get_kube_client(team_name="team_a")

        config.load_incluster_config.assert_not_called()
        config.load_kube_config.assert_called_once_with(
            config_file="/team-a/kubeconfig", context="team-a-context", client_configuration=mock.ANY
        )
        assert client.api_client.configuration.ssl_ca_cert == "/team-a/ca.crt"
        assert client.api_client.configuration.retries.total == 7

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @conf_vars(GLOBAL_CONNECTION_SETTINGS)
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    def test_team_does_not_fall_back_to_the_global_connection_settings(self, config):
        client = get_kube_client(team_name="team_b")

        config.load_incluster_config.assert_called_once()
        config.load_kube_config.assert_not_called()
        assert client.api_client.configuration.ssl_ca_cert is None
        assert client.api_client.configuration.retries.total != 1

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @conf_vars({("kubernetes_executor", "verify_ssl"): "False"})
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config")
    def test_team_verify_ssl_does_not_leak_between_clients(
        self, config, monkeypatch, fresh_default_configuration
    ):
        """The process-wide default configuration must not carry one executor's verify_ssl to another."""
        monkeypatch.setenv("AIRFLOW__TEAM_B___KUBERNETES_EXECUTOR__VERIFY_SSL", "False")

        team_b_client = get_kube_client(in_cluster=True, team_name="team_b")
        assert Configuration.get_default_copy().verify_ssl
        global_client = get_kube_client(in_cluster=True)
        team_a_client = get_kube_client(in_cluster=True, team_name="team_a")

        assert not team_b_client.api_client.configuration.verify_ssl
        assert not global_client.api_client.configuration.verify_ssl
        assert team_a_client.api_client.configuration.verify_ssl

    @pytest.mark.platform("linux")
    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="team config requires Airflow 3.2+")
    @conf_vars({("kubernetes_executor", "enable_tcp_keepalive"): "True"})
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.config", autospec=True)
    def test_team_client_gets_tcp_keepalive(self, mock_config):
        client = get_kube_client(in_cluster=False, team_name="team_a")

        pool_options = client.api_client.rest_client.pool_manager.connection_pool_kw["socket_options"]
        assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in pool_options

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
from urllib3.connection import HTTPConnection, HTTPSConnection

from airflow.providers.cncf.kubernetes.kube_client import (
    _disable_verify_ssl,
    _enable_tcp_keepalive,
    get_kube_client,
)

from tests_common.test_utils.config import conf_vars


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
        socket_options = [
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 120),
            (socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 30),
            (socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 6),
        ]
        expected_http_connection_options = HTTPConnection.default_socket_options + socket_options
        expected_https_connection_options = HTTPSConnection.default_socket_options + socket_options

        _enable_tcp_keepalive()

        assert HTTPConnection.default_socket_options == expected_http_connection_options
        assert HTTPSConnection.default_socket_options == expected_https_connection_options

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

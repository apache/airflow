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
import unittest
from unittest import mock

from kubernetes.client import Configuration
from urllib3.connection import HTTPConnection, HTTPSConnection

from airflow.kubernetes.kube_client import _disable_verify_ssl, _enable_tcp_keepalive, get_kube_client


class TestClient(unittest.TestCase):
    @mock.patch('airflow.kubernetes.kube_client.config')
    def test_load_cluster_config(self, config):
        get_kube_client(in_cluster=True)
        assert config.load_incluster_config.called
        assert config.load_kube_config.not_called

    @mock.patch('airflow.kubernetes.kube_client.config')
    def test_load_file_config(self, config):
        get_kube_client(in_cluster=False)
        assert config.load_incluster_config.not_called
        assert config.load_kube_config.called

    @mock.patch('airflow.kubernetes.kube_client.config')
    @mock.patch('airflow.kubernetes.kube_client.conf')
    def test_load_config_disable_ssl(self, conf, config):
        conf.getboolean.return_value = False
        get_kube_client(in_cluster=False)
        conf.getboolean.assert_called_with('kubernetes', 'verify_ssl')
        # Support wide range of kube client libraries
        if hasattr(Configuration, 'get_default_copy'):
            configuration = Configuration.get_default_copy()
        else:
            configuration = Configuration()
        self.assertFalse(configuration.verify_ssl)

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
        self.assertTrue(configuration.verify_ssl)

        _disable_verify_ssl()

        # Support wide range of kube client libraries
        if hasattr(Configuration, 'get_default_copy'):
            configuration = Configuration.get_default_copy()
        else:
            configuration = Configuration()
        self.assertFalse(configuration.verify_ssl)

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

import socket
import unittest
from unittest import mock

from urllib3.connection import HTTPConnection, HTTPSConnection

from airflow.kubernetes.kube_client import RefreshConfiguration, _enable_tcp_keepalive, get_kube_client


class TestClient(unittest.TestCase):

    @mock.patch('airflow.kubernetes.kube_client.config')
    def test_load_cluster_config(self, _):
        client = get_kube_client(in_cluster=True)
        assert not isinstance(client.api_client.configuration, RefreshConfiguration)

    @mock.patch('airflow.kubernetes.kube_client.config')
    @mock.patch('airflow.kubernetes.refresh_config._get_kube_config_loader_for_yaml_file')
    def test_load_file_config(self, _, _2):
        client = get_kube_client(in_cluster=False)
        assert isinstance(client.api_client.configuration, RefreshConfiguration)

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

        self.assertEqual(HTTPConnection.default_socket_options, expected_http_connection_options)
        self.assertEqual(HTTPSConnection.default_socket_options, expected_https_connection_options)

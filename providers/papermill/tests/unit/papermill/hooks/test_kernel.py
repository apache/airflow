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

from unittest.mock import patch

from airflow.models import Connection


class TestKernelHook:
    """
    Tests for Kernel connection
    """

    def test_kernel_connection(self):
        """
        Test that fetches kernelConnection with configured host and ports
        """
        from airflow.providers.papermill.hooks.kernel import KernelHook

        conn = Connection(
            conn_type="jupyter_kernel", host="test_host", extra='{"shell_port": 60000, "session_key": "key"}'
        )
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        assert hook.get_conn().ip == "test_host"
        assert hook.get_conn().shell_port == 60000
        assert hook.get_conn().session_key == "key"

    @patch("airflow.providers.papermill.hooks.kernel.register_remote_gateway_kernel_engine")
    @patch("airflow.providers.papermill.hooks.kernel.register_remote_kernel_engine")
    def test_zmq_connection_does_not_register_gateway_engine(self, mock_zmq_register, mock_gateway_register):
        from airflow.providers.papermill.hooks.kernel import KernelConnection, KernelHook

        conn = Connection(conn_type="jupyter_kernel", host="test_host")
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        assert isinstance(hook.get_conn(), KernelConnection)
        mock_zmq_register.assert_called_once()
        mock_gateway_register.assert_not_called()

    @patch("airflow.providers.papermill.hooks.kernel.register_remote_gateway_kernel_engine")
    def test_gateway_connection_with_url_host(self, mock_gateway_register):
        from airflow.providers.papermill.hooks.kernel import GatewayKernelConnection, KernelHook

        conn = Connection(
            conn_type="jupyter_kernel",
            host="https://gateway.example.com",
            port=8888,
            password="s3cret",
            extra='{"verify_ssl": false, "kernel_id": "abc", "request_timeout": 120}',
        )
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        gateway_conn = hook.get_conn()
        assert isinstance(gateway_conn, GatewayKernelConnection)
        assert gateway_conn.url == "https://gateway.example.com:8888"
        assert gateway_conn.token == "s3cret"
        assert gateway_conn.verify_ssl is False
        assert gateway_conn.kernel_id == "abc"
        assert gateway_conn.request_timeout == 120
        assert gateway_conn.auth_scheme == "token"
        assert gateway_conn.auth_header_key == "Authorization"
        mock_gateway_register.assert_called_once()

    @patch("airflow.providers.papermill.hooks.kernel.register_remote_gateway_kernel_engine")
    def test_gateway_connection_with_use_gateway_extra(self, mock_gateway_register):
        from airflow.providers.papermill.hooks.kernel import GatewayKernelConnection, KernelHook

        conn = Connection(
            conn_type="jupyter_kernel",
            host="gateway.example.com",
            extra='{"use_gateway": true, "token": "tok"}',
        )
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        gateway_conn = hook.get_conn()
        assert isinstance(gateway_conn, GatewayKernelConnection)
        assert gateway_conn.url == "https://gateway.example.com"
        assert gateway_conn.token == "tok"
        assert gateway_conn.verify_ssl is True

    @patch("airflow.providers.papermill.hooks.kernel.register_remote_gateway_kernel_engine")
    def test_gateway_connection_url_with_path_keeps_port_in_netloc(self, mock_gateway_register):
        from airflow.providers.papermill.hooks.kernel import KernelHook

        conn = Connection(
            conn_type="jupyter_kernel",
            host="https://hub.example.com/user/alice",
            port=8443,
            password="jupyterhub-token",
        )
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        assert hook.get_conn().url == "https://hub.example.com:8443/user/alice"

    @patch("airflow.providers.papermill.hooks.kernel.register_remote_gateway_kernel_engine")
    @patch("airflow.providers.papermill.hooks.kernel.register_remote_kernel_engine")
    def test_string_use_gateway_false_stays_zmq(self, mock_zmq_register, mock_gateway_register):
        from airflow.providers.papermill.hooks.kernel import KernelConnection, KernelHook

        conn = Connection(conn_type="jupyter_kernel", host="remote_host", extra='{"use_gateway": "false"}')
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        assert hook.use_gateway is False
        assert isinstance(hook.get_conn(), KernelConnection)
        mock_gateway_register.assert_not_called()

    @patch("airflow.providers.papermill.hooks.kernel.register_remote_gateway_kernel_engine")
    def test_string_verify_ssl_false_is_coerced_to_bool(self, mock_gateway_register):
        from airflow.providers.papermill.hooks.kernel import KernelHook

        conn = Connection(
            conn_type="jupyter_kernel",
            host="https://gateway.example.com",
            extra='{"verify_ssl": "false"}',
        )
        with patch.object(KernelHook, "get_connection", return_value=conn):
            hook = KernelHook()
        assert hook.get_conn().verify_ssl is False

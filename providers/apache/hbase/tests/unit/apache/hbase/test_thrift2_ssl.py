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

from unittest.mock import MagicMock, patch

from airflow.providers.apache.hbase.client import HBaseThrift2Client


class TestThrift2SSL:
    """Test Thrift2 SSL connection."""

    @patch("airflow.providers.apache.hbase.client.thrift2_client.TSSLSocket.TSSLSocket")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TTransport.TBufferedTransport")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TBinaryProtocol.TBinaryProtocol")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.THBaseService.Client")
    def test_ssl_connection_with_options(self, mock_client, mock_protocol, mock_transport, mock_ssl_socket):
        """Test SSL connection is created with ssl_options."""
        mock_socket = MagicMock()
        mock_ssl_socket.return_value = mock_socket
        mock_socket.setTimeout = MagicMock()

        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        ssl_options = {
            "ca_certs": "/path/to/ca.crt",
            "cert_file": "/path/to/client.crt",
            "key_file": "/path/to/client.key",
            "validate": True,
        }

        mock_ssl_ctx = MagicMock()

        client = HBaseThrift2Client(host="localhost", port=9090, ssl_options=ssl_options)
        with patch.object(client, "_create_ssl_context", return_value=mock_ssl_ctx):
            client.open()

        mock_ssl_socket.assert_called_once_with(
            host="localhost",
            port=9090,
            ssl_context=mock_ssl_ctx,
        )
        mock_transport_instance.open.assert_called_once()

    @patch("airflow.providers.apache.hbase.client.thrift2_client.TSocket.TSocket")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TTransport.TBufferedTransport")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TBinaryProtocol.TBinaryProtocol")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.THBaseService.Client")
    def test_connection_without_ssl(self, mock_client, mock_protocol, mock_transport, mock_socket):
        """Test regular connection is created without ssl_options."""
        mock_socket_instance = MagicMock()
        mock_socket.return_value = mock_socket_instance
        mock_socket_instance.setTimeout = MagicMock()

        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        client = HBaseThrift2Client(host="localhost", port=9090, ssl_options=None)
        client.open()

        # Verify regular TSocket was created
        mock_socket.assert_called_once_with("localhost", 9090)

        # Verify TSSLSocket was NOT used
        with patch("airflow.providers.apache.hbase.client.thrift2_client.TSSLSocket.TSSLSocket") as mock_ssl:
            mock_ssl.assert_not_called()

        # Verify transport was opened
        mock_transport_instance.open.assert_called_once()

    @patch("airflow.providers.apache.hbase.client.thrift2_client.TSSLSocket.TSSLSocket")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TTransport.TBufferedTransport")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TBinaryProtocol.TBinaryProtocol")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.THBaseService.Client")
    def test_ssl_with_minimal_options(self, mock_client, mock_protocol, mock_transport, mock_ssl_socket):
        """Test SSL connection with minimal options (only ca_certs)."""
        mock_socket = MagicMock()
        mock_ssl_socket.return_value = mock_socket
        mock_socket.setTimeout = MagicMock()

        mock_transport_instance = MagicMock()
        mock_transport.return_value = mock_transport_instance

        ssl_options = {"ca_certs": "/path/to/ca.crt"}
        mock_ssl_ctx = MagicMock()

        client = HBaseThrift2Client(host="localhost", port=9090, ssl_options=ssl_options)
        with patch.object(client, "_create_ssl_context", return_value=mock_ssl_ctx):
            client.open()

        mock_ssl_socket.assert_called_once_with(
            host="localhost",
            port=9090,
            ssl_context=mock_ssl_ctx,
        )

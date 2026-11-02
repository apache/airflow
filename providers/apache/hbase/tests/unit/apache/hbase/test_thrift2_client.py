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

from airflow.providers.apache.hbase.client.thrift2_client import HBaseThrift2Client


class TestHBaseThrift2Client:
    """Test HBase Thrift2 client."""

    def test_client_initialization(self):
        """Test client initialization with default parameters."""
        client = HBaseThrift2Client(host="localhost", port=9090)

        assert client.config.host == "localhost"
        assert client.config.port == 9090
        assert client.config.timeout == 30000
        assert client.config.ssl_options is None
        assert client.config.retry_max_attempts == 3
        assert client.config.retry_delay == 1.0
        assert client.config.retry_backoff_factor == 2.0

    def test_client_initialization_with_custom_params(self):
        """Test client initialization with custom parameters."""
        ssl_options = {"ca_certs": "/path/to/ca.crt", "validate": True}
        client = HBaseThrift2Client(
            host="hbase.example.com",
            port=9091,
            timeout=60000,
            ssl_options=ssl_options,
            retry_max_attempts=5,
            retry_delay=2.0,
            retry_backoff_factor=3.0,
        )

        assert client.config.host == "hbase.example.com"
        assert client.config.port == 9091
        assert client.config.timeout == 60000
        assert client.config.ssl_options == ssl_options
        assert client.config.retry_max_attempts == 5
        assert client.config.retry_delay == 2.0
        assert client.config.retry_backoff_factor == 3.0

    @patch("airflow.providers.apache.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TTransport")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TSocket")
    def test_open_connection(self, mock_socket, mock_transport, mock_protocol, mock_service):
        """Test opening connection."""
        mock_socket_inst = MagicMock()
        mock_socket.TSocket.return_value = mock_socket_inst

        mock_transport_inst = MagicMock()
        mock_transport.TBufferedTransport.return_value = mock_transport_inst

        mock_protocol_inst = MagicMock()
        mock_protocol.TBinaryProtocol.return_value = mock_protocol_inst

        mock_client = MagicMock()
        mock_service.Client.return_value = mock_client

        client = HBaseThrift2Client(host="localhost", port=9090)
        client.open()

        assert client._client == mock_client
        mock_socket.TSocket.assert_called_once_with("localhost", 9090)
        mock_transport_inst.open.assert_called_once()

    @patch("airflow.providers.apache.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TTransport")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TSocket")
    def test_close_connection(self, mock_socket, mock_transport, mock_protocol, mock_service):
        """Test closing connection."""
        mock_transport_inst = MagicMock()
        mock_transport.TBufferedTransport.return_value = mock_transport_inst

        client = HBaseThrift2Client(host="localhost", port=9090)
        client.open()
        client.close()

        mock_transport_inst.close.assert_called_once()

    def test_close_without_connection(self):
        """Test closing when no connection exists."""
        client = HBaseThrift2Client(host="localhost", port=9090)

        # Should not raise exception
        client.close()

    @patch("airflow.providers.apache.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TTransport")
    @patch("airflow.providers.apache.hbase.client.thrift2_client.TSocket")
    def test_table_exists(self, mock_socket, mock_transport, mock_protocol, mock_service):
        """Test table_exists method."""
        mock_client = MagicMock()
        mock_client.tableExists.return_value = True
        mock_service.Client.return_value = mock_client

        client = HBaseThrift2Client(host="localhost", port=9090)
        client.open()

        result = client.table_exists("test_table")

        assert result is True
        mock_client.tableExists.assert_called_once()


class TestResolveTableName:
    """Test namespace resolution for data-plane operations."""

    def _make_client(self, namespace="default"):
        client = HBaseThrift2Client(host="localhost", port=9090, namespace=namespace)
        client._client = MagicMock()
        return client

    def test_default_namespace_no_prefix(self):
        """Default namespace should not add prefix."""
        client = self._make_client(namespace="default")
        assert client._resolve_table_name("users") == b"users"

    def test_custom_namespace_adds_prefix(self):
        """Non-default namespace should be prepended."""
        client = self._make_client(namespace="production")
        assert client._resolve_table_name("users") == b"production:users"

    def test_fully_qualified_name_unchanged(self):
        """Table name with ':' should not be modified."""
        client = self._make_client(namespace="production")
        assert client._resolve_table_name("staging:users") == b"staging:users"

    def test_put_uses_namespace(self):
        """put() should resolve table name with namespace."""
        client = self._make_client(namespace="production")
        client.put("users", "row1", {"cf:name": "Alice"})
        table_arg = client._client.put.call_args[0][0]
        assert table_arg == b"production:users"

    def test_get_uses_namespace(self):
        """get() should resolve table name with namespace."""
        client = self._make_client(namespace="production")
        client._client.get.return_value = MagicMock(columnValues=[])
        client.get("users", "row1")
        table_arg = client._client.get.call_args[0][0]
        assert table_arg == b"production:users"

    def test_delete_uses_namespace(self):
        """delete() should resolve table name with namespace."""
        client = self._make_client(namespace="production")
        client.delete("users", "row1")
        table_arg = client._client.deleteSingle.call_args[0][0]
        assert table_arg == b"production:users"

    def test_scan_uses_namespace(self):
        """scan() should resolve table name with namespace."""
        client = self._make_client(namespace="production")
        client._client.openScanner.return_value = 1
        client._client.getScannerRows.return_value = []
        client.scan("users")
        table_arg = client._client.openScanner.call_args[0][0]
        assert table_arg == b"production:users"

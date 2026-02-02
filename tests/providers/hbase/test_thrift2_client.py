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

from unittest.mock import MagicMock, patch

from airflow.providers.hbase.client.thrift2_client import HBaseThrift2Client


class TestHBaseThrift2Client:
    """Test HBase Thrift2 client."""

    def test_client_initialization(self):
        """Test client initialization with default parameters."""
        client = HBaseThrift2Client(host="localhost", port=9090)
        
        assert client.host == "localhost"
        assert client.port == 9090
        assert client.timeout == 30000
        assert client.ssl_context is None
        assert client.retry_max_attempts == 3
        assert client.retry_delay == 1.0
        assert client.retry_backoff_factor == 2.0

    def test_client_initialization_with_custom_params(self):
        """Test client initialization with custom parameters."""
        ssl_context = MagicMock()
        client = HBaseThrift2Client(
            host="hbase.example.com",
            port=9091,
            timeout=60000,
            ssl_context=ssl_context,
            retry_max_attempts=5,
            retry_delay=2.0,
            retry_backoff_factor=3.0
        )
        
        assert client.host == "hbase.example.com"
        assert client.port == 9091
        assert client.timeout == 60000
        assert client.ssl_context == ssl_context
        assert client.retry_max_attempts == 5
        assert client.retry_delay == 2.0
        assert client.retry_backoff_factor == 3.0

    @patch("airflow.providers.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.hbase.client.thrift2_client.TTransport")
    @patch("airflow.providers.hbase.client.thrift2_client.TSocket")
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

    @patch("airflow.providers.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.hbase.client.thrift2_client.TTransport")
    @patch("airflow.providers.hbase.client.thrift2_client.TSocket")
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

    @patch("airflow.providers.hbase.client.thrift2_client.THBaseService")
    @patch("airflow.providers.hbase.client.thrift2_client.TBinaryProtocol")
    @patch("airflow.providers.hbase.client.thrift2_client.TTransport")
    @patch("airflow.providers.hbase.client.thrift2_client.TSocket")
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

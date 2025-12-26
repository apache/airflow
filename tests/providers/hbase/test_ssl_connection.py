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

"""Tests for SSLHappyBaseConnection class."""

import ssl
from unittest.mock import Mock, patch

import pytest

from airflow.providers.hbase.ssl_connection import SSLHappyBaseConnection, create_ssl_connection


class TestSSLHappyBaseConnection:
    """Test SSLHappyBaseConnection functionality."""

    @patch('happybase.Connection.__init__')
    def test_ssl_connection_initialization(self, mock_parent_init):
        """Test SSL connection can be initialized."""
        mock_parent_init.return_value = None
        
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        
        assert conn.ssl_context == ssl_context
        assert conn._temp_cert_files == []

    @patch('happybase.Connection.__init__')
    @patch('airflow.providers.hbase.ssl_connection.TSSLSocket')
    @patch('airflow.providers.hbase.ssl_connection.TFramedTransport')
    @patch('airflow.providers.hbase.ssl_connection.TBinaryProtocol')
    @patch('airflow.providers.hbase.ssl_connection.TClient')
    def test_refresh_thrift_client_with_ssl(self, mock_client, mock_protocol, mock_transport, mock_socket, mock_parent_init):
        """Test _refresh_thrift_client creates SSL components correctly."""
        mock_parent_init.return_value = None
        
        # Setup mocks
        mock_socket_instance = Mock()
        mock_transport_instance = Mock()
        mock_protocol_instance = Mock()
        mock_client_instance = Mock()
        
        mock_socket.return_value = mock_socket_instance
        mock_transport.return_value = mock_transport_instance
        mock_protocol.return_value = mock_protocol_instance
        mock_client.return_value = mock_client_instance
        
        # Create SSL context
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Create connection and refresh client
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        # Set attributes that would normally be set by parent __init__
        conn.host = 'localhost'
        conn.port = 9091
        conn._refresh_thrift_client()
        
        # Verify SSL components were created correctly
        mock_socket.assert_called_with(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        mock_transport.assert_called_once_with(mock_socket_instance)
        mock_protocol.assert_called_once_with(mock_transport_instance, decode_response=False)
        mock_client.assert_called_once()
        
        # Verify connection attributes
        assert conn.transport == mock_transport_instance
        assert conn.client == mock_client_instance

    @patch('happybase.Connection.__init__')
    @patch('happybase.Connection._refresh_thrift_client')
    def test_refresh_thrift_client_without_ssl(self, mock_parent_refresh, mock_parent_init):
        """Test _refresh_thrift_client falls back to parent when no SSL."""
        mock_parent_init.return_value = None
        
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9090,
            ssl_context=None  # No SSL
        )
        conn._refresh_thrift_client()
        
        # Verify parent method was called
        mock_parent_refresh.assert_called_once()

    @patch('happybase.Connection.__init__')
    def test_open_connection(self, mock_parent_init):
        """Test opening SSL connection."""
        mock_parent_init.return_value = None
        
        ssl_context = ssl.create_default_context()
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        
        # Mock transport
        mock_transport = Mock()
        mock_transport.is_open.return_value = False
        conn.transport = mock_transport
        
        # Test open
        conn.open()
        mock_transport.open.assert_called_once()

    @patch('happybase.Connection.__init__')
    def test_open_connection_already_open(self, mock_parent_init):
        """Test opening already open connection."""
        mock_parent_init.return_value = None
        
        ssl_context = ssl.create_default_context()
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        
        # Mock transport as already open
        mock_transport = Mock()
        mock_transport.is_open.return_value = True
        conn.transport = mock_transport
        
        # Test open
        conn.open()
        mock_transport.open.assert_not_called()

    @patch('happybase.Connection.__init__')
    @patch('happybase.Connection.close')
    def test_close_connection(self, mock_parent_close, mock_parent_init):
        """Test closing connection and cleanup."""
        mock_parent_init.return_value = None
        
        ssl_context = ssl.create_default_context()
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        
        # Add some temp files
        conn._temp_cert_files = ['/tmp/test1.pem', '/tmp/test2.pem']
        
        with patch.object(conn, '_cleanup_temp_files') as mock_cleanup:
            conn.close()
            
            mock_parent_close.assert_called_once()
            mock_cleanup.assert_called_once()

    @patch('happybase.Connection.__init__')
    @patch('os.path.exists')
    @patch('os.unlink')
    def test_cleanup_temp_files(self, mock_unlink, mock_exists, mock_parent_init):
        """Test temporary file cleanup."""
        mock_parent_init.return_value = None
        
        ssl_context = ssl.create_default_context()
        conn = SSLHappyBaseConnection(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
        
        # Setup temp files
        temp_files = ['/tmp/test1.pem', '/tmp/test2.pem']
        conn._temp_cert_files = temp_files.copy()
        
        # Mock file existence
        mock_exists.return_value = True
        
        # Test cleanup
        conn._cleanup_temp_files()
        
        # Verify files were deleted
        assert mock_unlink.call_count == 2
        mock_unlink.assert_any_call('/tmp/test1.pem')
        mock_unlink.assert_any_call('/tmp/test2.pem')
        
        # Verify list was cleared
        assert conn._temp_cert_files == []


class TestCreateSSLConnection:
    """Test create_ssl_connection function."""

    @patch('airflow.models.Variable.get')
    @patch('tempfile.NamedTemporaryFile')
    @patch('ssl.SSLContext.load_verify_locations')
    @patch('ssl.SSLContext.load_cert_chain')
    @patch('happybase.Connection.__init__')
    def test_create_ssl_connection_with_certificates(self, mock_parent_init, mock_load_cert, mock_load_ca, mock_tempfile, mock_variable_get):
        """Test SSL connection creation with certificates."""
        mock_parent_init.return_value = None
        
        # Mock certificate content
        mock_variable_get.side_effect = lambda key, default: {
            'hbase/ca-cert': 'CA_CERT_CONTENT',
            'hbase/client-cert': 'CLIENT_CERT_CONTENT',
            'hbase/client-key': 'CLIENT_KEY_CONTENT'
        }.get(key, default)
        
        # Mock temp files
        mock_ca_file = Mock()
        mock_ca_file.name = '/tmp/ca.pem'
        mock_cert_file = Mock()
        mock_cert_file.name = '/tmp/cert.pem'
        mock_key_file = Mock()
        mock_key_file.name = '/tmp/key.pem'
        
        mock_tempfile.side_effect = [mock_ca_file, mock_cert_file, mock_key_file]
        
        # Test SSL config
        ssl_config = {
            'use_ssl': True,
            'ssl_verify_mode': 'CERT_NONE',
            'ssl_ca_secret': 'hbase/ca-cert',
            'ssl_cert_secret': 'hbase/client-cert',
            'ssl_key_secret': 'hbase/client-key'
        }
        
        # Create connection
        conn = create_ssl_connection('localhost', 9091, ssl_config)
        
        # Verify it's our SSL connection class
        assert isinstance(conn, SSLHappyBaseConnection)
        assert conn.ssl_context is not None
        assert len(conn._temp_cert_files) == 3
        
        # Verify SSL methods were called
        mock_load_ca.assert_called_once()
        mock_load_cert.assert_called_once()

    @patch('happybase.Connection')
    def test_create_ssl_connection_without_ssl(self, mock_happybase_conn):
        """Test connection creation without SSL."""
        ssl_config = {'use_ssl': False}
        
        create_ssl_connection('localhost', 9090, ssl_config)
        
        # Verify regular HappyBase connection was created
        mock_happybase_conn.assert_called_once_with(host='localhost', port=9090)

    def test_ssl_verify_modes(self):
        """Test different SSL verification modes."""
        test_cases = [
            ('CERT_NONE', ssl.CERT_NONE),
            ('CERT_OPTIONAL', ssl.CERT_OPTIONAL),
            ('CERT_REQUIRED', ssl.CERT_REQUIRED),
            ('INVALID_MODE', ssl.CERT_REQUIRED)  # Default fallback
        ]
        
        with patch('happybase.Connection.__init__', return_value=None):
            for verify_mode, expected_ssl_mode in test_cases:
                ssl_config = {
                    'use_ssl': True,
                    'ssl_verify_mode': verify_mode
                }
                
                conn = create_ssl_connection('localhost', 9091, ssl_config)
                assert conn.ssl_context.verify_mode == expected_ssl_mode


class TestSSLIntegration:
    """Integration tests for SSL functionality."""

    def test_ssl_context_creation(self):
        """Test SSL context can be created and configured."""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        assert isinstance(ssl_context, ssl.SSLContext)
        assert ssl_context.verify_mode == ssl.CERT_NONE
        assert not ssl_context.check_hostname

    def test_thrift_ssl_components_available(self):
        """Test that required Thrift SSL components are available."""
        try:
            from thriftpy2.transport import TSSLSocket, TFramedTransport
            from thriftpy2.protocol import TBinaryProtocol
            from thriftpy2.thrift import TClient
            
            # This should not raise ImportError
            assert True, "All Thrift SSL components imported successfully"
            
        except ImportError as e:
            pytest.fail(f"Required Thrift SSL components not available: {e}")

    @patch('thriftpy2.transport.TSSLSocket')
    def test_ssl_socket_creation(self, mock_ssl_socket):
        """Test TSSLSocket can be created with SSL context."""
        ssl_context = ssl.create_default_context()
        mock_socket_instance = Mock()
        mock_ssl_socket.return_value = mock_socket_instance
        
        # This should work without errors
        from thriftpy2.transport import TSSLSocket
        TSSLSocket(host='localhost', port=9091, ssl_context=ssl_context)
        
        mock_ssl_socket.assert_called_once_with(
            host='localhost',
            port=9091,
            ssl_context=ssl_context
        )
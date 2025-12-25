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

"""Tests for HBase SSL/TLS functionality."""

import ssl
from unittest.mock import patch

import pytest

from airflow.providers.hbase.hooks.hbase import HBaseHook


class TestHBaseSSL:
    """Test SSL/TLS functionality in HBase hook."""

    def test_ssl_disabled_by_default(self):
        """Test that SSL is disabled by default."""
        hook = HBaseHook()
        ssl_args = hook._setup_ssl_connection({})
        
        assert ssl_args == {}

    def test_ssl_enabled_basic(self):
        """Test basic SSL enablement."""
        hook = HBaseHook()
        config = {"use_ssl": True}
        ssl_args = hook._setup_ssl_connection(config)
        
        assert ssl_args["transport"] == "framed"
        assert ssl_args["protocol"] == "compact"

    def test_ssl_custom_port(self):
        """Test SSL with custom port."""
        hook = HBaseHook()
        config = {"use_ssl": True, "ssl_port": 9443}
        ssl_args = hook._setup_ssl_connection(config)
        
        assert ssl_args["port"] == 9443

    def test_ssl_cert_none_verification(self):
        """Test SSL with no certificate verification."""
        hook = HBaseHook()
        config = {"use_ssl": True, "ssl_verify_mode": "CERT_NONE"}
        hook._setup_ssl_connection(config)
        
        ssl_context = hook._ssl_context
        assert ssl_context.verify_mode == ssl.CERT_NONE
        assert not ssl_context.check_hostname

    def test_ssl_cert_optional_verification(self):
        """Test SSL with optional certificate verification."""
        hook = HBaseHook()
        config = {"use_ssl": True, "ssl_verify_mode": "CERT_OPTIONAL"}
        hook._setup_ssl_connection(config)
        
        ssl_context = hook._ssl_context
        assert ssl_context.verify_mode == ssl.CERT_OPTIONAL

    def test_ssl_cert_required_verification(self):
        """Test SSL with required certificate verification (default)."""
        hook = HBaseHook()
        config = {"use_ssl": True, "ssl_verify_mode": "CERT_REQUIRED"}
        hook._setup_ssl_connection(config)
        
        ssl_context = hook._ssl_context
        assert ssl_context.verify_mode == ssl.CERT_REQUIRED

    @patch('airflow.models.Variable.get')
    def test_ssl_ca_secret(self, mock_variable_get):
        """Test SSL with CA certificate content from secrets."""
        mock_variable_get.return_value = "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----"
        
        hook = HBaseHook()
        config = {"use_ssl": True, "ssl_ca_secret": "hbase/ca-cert"}
        
        with patch('ssl.SSLContext.load_verify_locations') as mock_load_ca:
            hook._setup_ssl_connection(config)
            
            mock_variable_get.assert_called_once_with("hbase/ca-cert", None)
            mock_load_ca.assert_called_once()

    @patch('airflow.models.Variable.get')
    def test_ssl_client_certificates_from_secrets(self, mock_variable_get):
        """Test SSL with client certificate content from secrets."""
        mock_variable_get.side_effect = [
            "-----BEGIN CERTIFICATE-----\nMIIC...\n-----END CERTIFICATE-----",
            "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"
        ]
        
        hook = HBaseHook()
        config = {
            "use_ssl": True,
            "ssl_cert_secret": "hbase/client-cert",
            "ssl_key_secret": "hbase/client-key"
        }
        
        with patch('ssl.SSLContext.load_cert_chain') as mock_load_cert:
            hook._setup_ssl_connection(config)
            
            assert mock_variable_get.call_count == 2
            mock_load_cert.assert_called_once()


    def test_ssl_min_version(self):
        """Test SSL minimum version configuration."""
        hook = HBaseHook()
        config = {"use_ssl": True, "ssl_min_version": "TLSv1_2"}
        hook._setup_ssl_connection(config)
        
        ssl_context = hook._ssl_context
        assert ssl_context.minimum_version == ssl.TLSVersion.TLSv1_2

    @patch('airflow.providers.hbase.hooks.hbase.HBaseHook._connect_with_retry')
    @patch('airflow.providers.hbase.hooks.hbase.HBaseHook.get_connection')
    def test_get_conn_with_ssl(self, mock_get_connection, mock_connect_with_retry):
        """Test get_conn method with SSL configuration."""
        # Mock connection
        mock_conn = mock_get_connection.return_value
        mock_conn.host = "hbase-ssl.example.com"
        mock_conn.port = 9091
        mock_conn.extra_dejson = {
            "use_ssl": True,
            "ssl_verify_mode": "CERT_REQUIRED"
        }
        
        # Mock SSL connection
        mock_ssl_conn = mock_connect_with_retry.return_value
        
        hook = HBaseHook()
        result = hook.get_conn()
        
        # Verify SSL connection was created
        mock_connect_with_retry.assert_called_once()
        assert result == mock_ssl_conn
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

import pytest

from airflow.providers.hbase.auth import AuthenticatorFactory, HBaseAuthenticator, SimpleAuthenticator
from airflow.providers.hbase.auth.base import KerberosAuthenticator


class TestAuthenticatorFactory:
    """Test AuthenticatorFactory."""

    def test_create_simple_authenticator(self):
        """Test creating simple authenticator."""
        authenticator = AuthenticatorFactory.create("simple")
        assert isinstance(authenticator, SimpleAuthenticator)

    def test_create_kerberos_authenticator(self):
        """Test creating kerberos authenticator."""
        authenticator = AuthenticatorFactory.create("kerberos")
        assert isinstance(authenticator, KerberosAuthenticator)

    def test_create_unknown_authenticator(self):
        """Test creating unknown authenticator raises error."""
        with pytest.raises(ValueError, match="Unknown authentication method: unknown"):
            AuthenticatorFactory.create("unknown")

    def test_register_custom_authenticator(self):
        """Test registering custom authenticator."""
        class CustomAuthenticator(HBaseAuthenticator):
            def authenticate(self, config):
                return {"custom": True}

        AuthenticatorFactory.register("custom", CustomAuthenticator)
        authenticator = AuthenticatorFactory.create("custom")
        assert isinstance(authenticator, CustomAuthenticator)


class TestSimpleAuthenticator:
    """Test SimpleAuthenticator."""

    def test_authenticate(self):
        """Test simple authentication returns empty dict."""
        authenticator = SimpleAuthenticator()
        result = authenticator.authenticate({})
        assert result == {}


class TestKerberosAuthenticator:
    """Test KerberosAuthenticator."""

    def test_authenticate_missing_principal(self):
        """Test authentication fails when principal missing."""
        authenticator = KerberosAuthenticator()
        with pytest.raises(ValueError, match="Kerberos principal is required"):
            authenticator.authenticate({})

    @patch("airflow.providers.hbase.auth.base.subprocess.run")
    @patch("os.path.exists")
    def test_authenticate_with_keytab_path(self, mock_exists, mock_subprocess):
        """Test authentication with keytab path."""
        mock_exists.return_value = True
        mock_subprocess.return_value = MagicMock()

        authenticator = KerberosAuthenticator()
        config = {
            "principal": "test@EXAMPLE.COM",
            "keytab_path": "/path/to/test.keytab"
        }
        
        result = authenticator.authenticate(config)
        
        assert result == {}
        mock_subprocess.assert_called_once_with(
            ["kinit", "-kt", "/path/to/test.keytab", "test@EXAMPLE.COM"],
            capture_output=True, text=True, check=True
        )

    @patch("airflow.providers.hbase.auth.base.subprocess.run")
    @patch("os.path.exists")
    def test_authenticate_keytab_not_found(self, mock_exists, mock_subprocess):
        """Test authentication fails when keytab not found."""
        mock_exists.return_value = False

        authenticator = KerberosAuthenticator()
        config = {
            "principal": "test@EXAMPLE.COM",
            "keytab_path": "/path/to/missing.keytab"
        }
        
        with pytest.raises(ValueError, match="Keytab file not found"):
            authenticator.authenticate(config)

    @patch("airflow.providers.hbase.auth.base.subprocess.run")
    def test_authenticate_kinit_failure(self, mock_subprocess):
        """Test authentication fails when kinit fails."""
        from subprocess import CalledProcessError
        mock_subprocess.side_effect = CalledProcessError(1, "kinit", stderr="Authentication failed")

        authenticator = KerberosAuthenticator()
        config = {
            "principal": "test@EXAMPLE.COM",
            "keytab_path": "/path/to/test.keytab"
        }
        
        with patch("os.path.exists", return_value=True):
            with pytest.raises(RuntimeError, match="Kerberos authentication failed"):
                authenticator.authenticate(config)
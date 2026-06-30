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
"""Tests for LDAPAuthMechanism."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from airflow.providers.teradata.auth.ldap import LDAPAuthMechanism


class TestLDAPAuthMechanism:
    """Test cases for LDAPAuthMechanism."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mechanism = LDAPAuthMechanism()

    def test_mechanism_properties(self):
        """Verify LDAP mechanism properties."""
        assert self.mechanism.mechanism_name == "LDAP"
        assert "LDAP" in self.mechanism.display_name

    def test_get_connection_config(self):
        """Verify LDAP adds logmech parameter."""
        connection = Mock(spec_set=[])
        base_config = {
            "host": "ldap.example.com",
            "dbs_port": 1025,
            "database": "mydb",
            "user": "john.doe",
            "password": "xxxxx",
        }

        result = self.mechanism.get_connection_config(connection, base_config)

        for key in base_config:
            assert result[key] == base_config[key]
        assert result["logmech"] == "LDAP"

    def test_get_connection_config_immutable(self):
        """Verify LDAP doesn't modify base_config."""
        connection = Mock(spec_set=[])
        base_config = {"host": "ldap", "user": "john", "password": "xxx"}
        original = base_config.copy()

        self.mechanism.get_connection_config(connection, base_config)

        assert base_config == original

    def test_validate_config_success(self):
        """Verify validation passes with logmech, user, password."""
        config = {
            "logmech": "LDAP",
            "user": "john.doe",
            "password": "xxxxx",
        }

        self.mechanism.validate_config(config)

    def test_validate_missing_user(self):
        """Verify error when LDAP user missing."""
        config = {"logmech": "LDAP", "password": "xxxxx"}

        with pytest.raises(ValueError, match="username"):
            self.mechanism.validate_config(config)

    def test_validate_missing_password(self):
        """Verify error when LDAP password missing."""
        config = {"logmech": "LDAP", "user": "john.doe"}

        with pytest.raises(ValueError, match="password"):
            self.mechanism.validate_config(config)

    def test_validate_logdata_allows_missing_user_password(self):
        """Verify logdata-based auth is accepted without user/password."""
        config = {"logmech": "LDAP", "logdata": "authcid=john password=xxx"}

        self.mechanism.validate_config(config)

    def test_validate_invalid_logmech(self):
        """Verify error if logmech not set to LDAP."""
        config = {
            "logmech": "TD2",
            "user": "john",
            "password": "xxx",
        }

        with pytest.raises(ValueError, match="LDAP"):
            self.mechanism.validate_config(config)

    def test_validate_with_ssl_params(self):
        """Verify LDAP validation works with SSL parameters."""
        config = {
            "logmech": "LDAP",
            "user": "john",
            "password": "xxx",
            "sslmode": "verify-ca",
            "sslca": "/path/to/ca.pem",
        }

        self.mechanism.validate_config(config)

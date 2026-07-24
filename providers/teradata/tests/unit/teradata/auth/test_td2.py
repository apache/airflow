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
"""Tests for TD2AuthMechanism."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from airflow.providers.teradata.auth.td2 import TD2AuthMechanism


class TestTD2AuthMechanism:
    """Test cases for TD2AuthMechanism."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mechanism = TD2AuthMechanism()

    def test_mechanism_properties(self):
        """Verify TD2 mechanism properties."""
        assert self.mechanism.mechanism_name == "TD2"
        assert "Native" in self.mechanism.display_name

    def test_get_connection_config(self):
        """Verify TD2 keeps provided credentials and adds no logmech."""
        connection = Mock(spec_set=[])
        base_config = {
            "host": "localhost",
            "dbs_port": 1025,
            "database": "mydb",
            "user": "myuser",
            "password": "mypwd",
        }

        result = self.mechanism.get_connection_config(connection, base_config)

        assert result == base_config
        assert "logmech" not in result

    def test_get_connection_config_applies_dbc_default(self):
        """Verify empty user/password fall back to the historical 'dbc' default."""
        connection = Mock(spec_set=[])
        base_config = {"host": "localhost", "user": None, "password": None}

        result = self.mechanism.get_connection_config(connection, base_config)

        assert result["user"] == "dbc"
        assert result["password"] == "dbc"

    def test_validate_config_success(self):
        """Verify validation passes with user and password."""
        config = {
            "user": "dbc",
            "password": "dbc",
        }

        self.mechanism.validate_config(config)

    def test_validate_missing_user(self):
        """Verify error when user is missing."""
        config = {"password": "dbc"}

        with pytest.raises(ValueError, match="user"):
            self.mechanism.validate_config(config)

    def test_validate_missing_password(self):
        """Verify error when password is missing."""
        config = {"user": "dbc"}

        with pytest.raises(ValueError, match="password"):
            self.mechanism.validate_config(config)

    def test_validate_empty_password_allowed(self):
        """Verify empty string password is allowed (None is not)."""
        config = {"user": "dbc", "password": ""}

        self.mechanism.validate_config(config)

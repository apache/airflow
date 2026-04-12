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
"""Tests for validation utilities."""

from __future__ import annotations

from airflowctl.api.datamodels.auth_generated import LoginBody
from airflowctl.api.datamodels.generated import ConnectionBody, PoolBody, VariableBody
from airflowctl.utils.validation import (
    check_required_fields,
    format_missing_fields_message,
    get_field_description,
    get_missing_required_fields,
    is_required_field,
)


class TestIsRequiredField:
    """Test is_required_field function."""

    def test_login_body_username_required(self):
        """Test that username is a required field in LoginBody."""
        assert is_required_field(LoginBody, "username") is True

    def test_login_body_password_required(self):
        """Test that password is a required field in LoginBody."""
        assert is_required_field(LoginBody, "password") is True

    def test_connection_body_connection_id_required(self):
        """Test that connection_id is required in ConnectionBody."""
        assert is_required_field(ConnectionBody, "connection_id") is True

    def test_connection_body_conn_type_required(self):
        """Test that conn_type is required in ConnectionBody."""
        assert is_required_field(ConnectionBody, "conn_type") is True

    def test_connection_body_optional_fields(self):
        """Test that optional fields return False."""
        # description has a default of None so it is NOT required
        assert is_required_field(ConnectionBody, "description") is False

    def test_pool_body_name_required(self):
        """Test that name is required in PoolBody."""
        assert is_required_field(PoolBody, "name") is True

    def test_pool_body_slots_required(self):
        """Test that slots is required in PoolBody."""
        assert is_required_field(PoolBody, "slots") is True

    def test_variable_body_key_required(self):
        """Test that key is required in VariableBody."""
        assert is_required_field(VariableBody, "key") is True

    def test_variable_body_value_required(self):
        """Test that value is required in VariableBody."""
        assert is_required_field(VariableBody, "value") is True


class TestGetMissingRequiredFields:
    """Test get_missing_required_fields function."""

    def test_login_body_missing_username(self):
        """Test missing username in LoginBody."""
        missing = get_missing_required_fields(LoginBody, {"password": "test"})
        assert "username" in missing

    def test_login_body_missing_password(self):
        """Test missing password in LoginBody."""
        missing = get_missing_required_fields(LoginBody, {"username": "test"})
        assert "password" in missing

    def test_login_body_both_missing(self):
        """Test both username and password missing."""
        missing = get_missing_required_fields(LoginBody, {})
        assert "username" in missing
        assert "password" in missing

    def test_login_body_all_present(self):
        """Test all required fields present."""
        missing = get_missing_required_fields(LoginBody, {"username": "test", "password": "test"})
        assert len(missing) == 0

    def test_connection_body_all_present(self):
        """Test ConnectionBody with all required fields."""
        missing = get_missing_required_fields(
            ConnectionBody, {"connection_id": "test", "conn_type": "sqlite"}
        )
        assert len(missing) == 0


class TestCheckRequiredFields:
    """Test check_required_fields function."""

    def test_connection_body_missing_connection_id(self):
        """Test checking required fields for ConnectionBody."""
        data = {"conn_type": "sqlite"}
        missing = check_required_fields(ConnectionBody, data)
        assert "connection_id" in missing

    def test_pool_body_missing_both(self):
        """Test PoolBody with no required fields provided."""
        data = {}
        missing = check_required_fields(PoolBody, data)
        assert "name" in missing
        assert "slots" in missing


class TestGetFieldDescription:
    """Test get_field_description function."""

    def test_username_has_title(self):
        """Test username field has proper title."""
        desc = get_field_description(LoginBody, "username")
        assert desc == "Username"

    def test_password_has_title(self):
        """Test password field has proper title."""
        desc = get_field_description(LoginBody, "password")
        assert desc == "Password"

    def test_connection_id_title(self):
        """Test connection_id field has proper title."""
        desc = get_field_description(ConnectionBody, "connection_id")
        assert desc == "Connection Id"

    def test_unknown_field_fallback(self):
        """Test fallback for unknown field."""
        desc = get_field_description(LoginBody, "unknown_field")
        assert desc == "Unknown Field"


class TestFormatMissingFieldsMessage:
    """Test format_missing_fields_message function."""

    def test_single_missing_field(self):
        """Test message for single missing field."""
        msg = format_missing_fields_message(LoginBody, ["username"])
        assert "Username" in msg

    def test_multiple_missing_fields(self):
        """Test message for multiple missing fields."""
        msg = format_missing_fields_message(LoginBody, ["username", "password"])
        assert "Username" in msg
        assert "Password" in msg

    def test_empty_list(self):
        """Test empty list returns empty string."""
        msg = format_missing_fields_message(LoginBody, [])
        assert msg == ""

    def test_connection_body_missing_fields(self):
        """Test ConnectionBody missing fields message."""
        msg = format_missing_fields_message(ConnectionBody, ["connection_id", "conn_type"])
        assert "Connection Id" in msg
        assert "Conn Type" in msg

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

import pytest

from airflow.sdk import get_current_context
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasEvent,
    AssetAliasUniqueKey,
    AssetUniqueKey,
)
from airflow.sdk.definitions.connection import Connection
from airflow.sdk.definitions.variable import Variable
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import AssetResult, ConnectionResult, ErrorResponse, VariableResult
from airflow.sdk.execution_time.context import (
    ConnectionAccessor,
    OutletEventAccessor,
    OutletEventAccessors,
    VariableAccessor,
    _convert_connection_result_conn,
    _convert_variable_result_to_variable,
    set_current_context,
)


def test_convert_connection_result_conn():
    """Test that the ConnectionResult is converted to a Connection object."""
    conn = ConnectionResult(
        conn_id="test_conn",
        conn_type="mysql",
        host="mysql",
        schema="airflow",
        login="root",
        password="password",
        port=1234,
        extra='{"extra_key": "extra_value"}',
    )
    conn = _convert_connection_result_conn(conn)
    assert conn == Connection(
        conn_id="test_conn",
        conn_type="mysql",
        host="mysql",
        schema="airflow",
        login="root",
        password="password",
        port=1234,
        extra='{"extra_key": "extra_value"}',
    )


def test_convert_variable_result_to_variable():
    """Test that the VariableResult is converted to a Variable object."""
    var = VariableResult(
        key="test_key",
        value="test_value",
    )
    var = _convert_variable_result_to_variable(var, deserialize_json=False)
    assert var == Variable(
        key="test_key",
        value="test_value",
    )


def test_convert_variable_result_to_variable_with_deserialize_json():
    """Test that the VariableResult is converted to a Variable object with deserialize_json set to True."""
    var = VariableResult(
        key="test_key",
        value='{\r\n  "key1": "value1",\r\n  "key2": "value2",\r\n  "enabled": true,\r\n  "threshold": 42\r\n}',
    )
    var = _convert_variable_result_to_variable(var, deserialize_json=True)
    assert var == Variable(
        key="test_key", value={"key1": "value1", "key2": "value2", "enabled": True, "threshold": 42}
    )


class TestConnectionAccessor:
    def test_getattr_connection(self, mock_supervisor_comms):
        """
        Test that the connection is fetched when accessed via __getattr__.

        The __getattr__ method is used for template rendering. Example: ``{{ conn.mysql_conn.host }}``.
        """
        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

        mock_supervisor_comms.get_message.return_value = conn_result

        # Fetch the connection; triggers __getattr__
        conn = accessor.mysql_conn

        expected_conn = Connection(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)
        assert conn == expected_conn

    def test_get_method_valid_connection(self, mock_supervisor_comms):
        """Test that the get method returns the requested connection using `conn.get`."""
        accessor = ConnectionAccessor()
        conn_result = ConnectionResult(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

        mock_supervisor_comms.get_message.return_value = conn_result

        conn = accessor.get("mysql_conn")
        assert conn == Connection(conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306)

    def test_get_method_with_default(self, mock_supervisor_comms):
        """Test that the get method returns the default connection when the requested connection is not found."""
        accessor = ConnectionAccessor()
        default_conn = {"conn_id": "default_conn", "conn_type": "sqlite"}
        error_response = ErrorResponse(
            error=ErrorType.CONNECTION_NOT_FOUND, detail={"conn_id": "nonexistent_conn"}
        )

        mock_supervisor_comms.get_message.return_value = error_response

        conn = accessor.get("nonexistent_conn", default_conn=default_conn)
        assert conn == default_conn

    def test_getattr_connection_for_extra_dejson(self, mock_supervisor_comms):
        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(
            conn_id="mysql_conn",
            conn_type="mysql",
            host="mysql",
            port=3306,
            extra='{"extra_key": "extra_value"}',
        )

        mock_supervisor_comms.get_message.return_value = conn_result

        # Fetch the connection's dejson; triggers __getattr__
        dejson = accessor.mysql_conn.extra_dejson

        assert dejson == {"extra_key": "extra_value"}

    @patch("airflow.sdk.definitions.connection.log", create=True)
    def test_getattr_connection_for_extra_dejson_decode_error(self, mock_log, mock_supervisor_comms):
        mock_log.return_value = MagicMock()

        accessor = ConnectionAccessor()

        # Conn from the supervisor / API Server
        conn_result = ConnectionResult(
            conn_id="mysql_conn", conn_type="mysql", host="mysql", port=3306, extra="This is not JSON!"
        )

        mock_supervisor_comms.get_message.return_value = conn_result

        # Fetch the connection's dejson; triggers __getattr__
        dejson = accessor.mysql_conn.extra_dejson

        # empty in case of failed deserialising
        assert dejson == {}

        mock_log.exception.assert_called_once_with(
            "Failed to deserialize extra property `extra`, returning empty dictionary"
        )


class TestVariableAccessor:
    def test_getattr_variable(self, mock_supervisor_comms):
        """
        Test that the variable is fetched when accessed via __getattr__.
        """
        accessor = VariableAccessor(deserialize_json=False)

        # Variable from the supervisor / API Server
        var_result = VariableResult(key="test_key", value="test_value")

        mock_supervisor_comms.get_message.return_value = var_result

        # Fetch the variable; triggers __getattr__
        var = accessor.test_key

        expected_var = Variable(key="test_key", value="test_value")
        assert var == expected_var

    def test_get_method_valid_variable(self, mock_supervisor_comms):
        """Test that the get method returns the requested variable using `var.get`."""
        accessor = VariableAccessor(deserialize_json=False)
        var_result = VariableResult(key="test_key", value="test_value")

        mock_supervisor_comms.get_message.return_value = var_result

        var = accessor.get("test_key")
        assert var == Variable(key="test_key", value="test_value")

    def test_get_method_with_default(self, mock_supervisor_comms):
        """Test that the get method returns the default variable when the requested variable is not found."""

        accessor = VariableAccessor(deserialize_json=False)
        default_var = {"default_key": "default_value"}
        error_response = ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"test_key": "test_value"})

        mock_supervisor_comms.get_message.return_value = error_response

        var = accessor.get("nonexistent_var_key", default_var=default_var)
        assert var == default_var


class TestCurrentContext:
    def test_current_context_roundtrip(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            assert get_current_context() == example_context

    def test_context_removed_after_exit(self):
        example_context = {"Hello": "World"}

        with set_current_context(example_context):
            pass
        with pytest.raises(RuntimeError):
            get_current_context()

    def test_nested_context(self):
        """
        Nested execution context should be supported in case the user uses multiple context managers.
        Each time the execute method of an operator is called, we set a new 'current' context.
        This test verifies that no matter how many contexts are entered - order is preserved
        """
        max_stack_depth = 15
        ctx_list = []
        for i in range(max_stack_depth):
            # Create all contexts in ascending order
            new_context = {"ContextId": i}
            # Like 15 nested with statements
            ctx_obj = set_current_context(new_context)
            ctx_obj.__enter__()
            ctx_list.append(ctx_obj)
        for i in reversed(range(max_stack_depth)):
            # Iterate over contexts in reverse order - stack is LIFO
            ctx = get_current_context()
            assert ctx["ContextId"] == i
            # End of with statement
            ctx_list[i].__exit__(None, None, None)


class TestOutletEventAccessor:
    @pytest.mark.parametrize(
        "key, asset_alias_events",
        (
            (AssetUniqueKey.from_asset(Asset("test_uri")), []),
            (
                AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias")),
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(uri="test_uri", name="test_uri"),
                        extra={},
                    )
                ],
            ),
        ),
    )
    def test_add(self, key, asset_alias_events, mock_supervisor_comms):
        asset = Asset("test_uri")
        mock_supervisor_comms.get_message.return_value = asset

        outlet_event_accessor = OutletEventAccessor(key=key, extra={})
        outlet_event_accessor.add(asset)
        assert outlet_event_accessor.asset_alias_events == asset_alias_events

    @pytest.mark.parametrize(
        "key, asset_alias_events",
        (
            (AssetUniqueKey.from_asset(Asset("test_uri")), []),
            (
                AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias")),
                [
                    AssetAliasEvent(
                        source_alias_name="test_alias",
                        dest_asset_key=AssetUniqueKey(name="test-asset", uri="test://asset-uri/"),
                        extra={},
                    )
                ],
            ),
        ),
    )
    def test_add_with_db(self, key, asset_alias_events, mock_supervisor_comms):
        asset = Asset(uri="test://asset-uri", name="test-asset")
        mock_supervisor_comms.get_message.return_value = asset

        outlet_event_accessor = OutletEventAccessor(key=key, extra={"not": ""})
        outlet_event_accessor.add(asset, extra={})
        assert outlet_event_accessor.asset_alias_events == asset_alias_events


class TestOutletEventAccessors:
    @pytest.mark.parametrize(
        "access_key, internal_key",
        (
            (Asset("test"), AssetUniqueKey.from_asset(Asset("test"))),
            (
                Asset(name="test", uri="test://asset"),
                AssetUniqueKey.from_asset(Asset(name="test", uri="test://asset")),
            ),
            (AssetAlias("test_alias"), AssetAliasUniqueKey.from_asset_alias(AssetAlias("test_alias"))),
        ),
    )
    def test__get_item__dict_key_not_exists(self, access_key, internal_key):
        outlet_event_accessors = OutletEventAccessors()
        assert len(outlet_event_accessors) == 0
        outlet_event_accessor = outlet_event_accessors[access_key]
        assert len(outlet_event_accessors) == 1
        assert outlet_event_accessor.key == internal_key
        assert outlet_event_accessor.extra == {}

    @pytest.mark.parametrize(
        ["access_key", "asset"],
        (
            (Asset.ref(name="test"), Asset(name="test")),
            (Asset.ref(name="test1"), Asset(name="test1", uri="test://asset-uri")),
            (Asset.ref(uri="test://asset-uri"), Asset(uri="test://asset-uri")),
        ),
    )
    def test__get_item__asset_ref(self, access_key, asset, mock_supervisor_comms):
        """Test accessing OutletEventAccessors with AssetRef resolves to correct Asset."""
        internal_key = AssetUniqueKey.from_asset(asset)
        outlet_event_accessors = OutletEventAccessors()
        assert len(outlet_event_accessors) == 0

        # Asset from the API Server via the supervisor
        mock_supervisor_comms.get_message.return_value = AssetResult(
            name=asset.name,
            uri=asset.uri,
            group=asset.group,
        )

        outlet_event_accessor = outlet_event_accessors[access_key]
        assert len(outlet_event_accessors) == 1
        assert outlet_event_accessor.key == internal_key
        assert outlet_event_accessor.extra == {}

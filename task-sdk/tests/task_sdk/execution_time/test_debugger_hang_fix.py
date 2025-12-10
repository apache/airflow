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

"""Test that the debugger hang fix (issue #51861) works correctly."""

from __future__ import annotations

import pytest

from airflow.sdk.execution_time.context import ConnectionAccessor, MacrosAccessor, VariableAccessor


class TestDebuggerHangFix:
    """Test that Accessor classes guard against dunder method introspection."""

    def test_connection_accessor_guards_dunder_methods(self):
        """
        Test that ConnectionAccessor guards against dunder method introspection.

        __getattr__ is only called for attributes NOT explicitly defined on the class.
        This test verifies that undefined dunder methods are properly guarded.
        """
        accessor = ConnectionAccessor()

        # __iter__ is explicitly defined to raise TypeError (clear error message)
        with pytest.raises(TypeError, match="not iterable"):
            iter(accessor)

        # These dunder methods are NOT explicitly defined, so __getattr__ guard catches them
        # These are commonly probed by debuggers during introspection
        guarded_methods = ["__len__", "__contains__", "__getitem__"]

        for method in guarded_methods:
            # The guard should prevent these from triggering database lookups
            assert not hasattr(accessor, method), f"{method} should be guarded by __getattr__"

    def test_variable_accessor_guards_dunder_methods(self):
        """
        Test that VariableAccessor guards against dunder method introspection.

        __getattr__ is only called for attributes NOT explicitly defined on the class.
        """
        accessor = VariableAccessor(deserialize_json=False)

        # __iter__ is explicitly defined to raise TypeError
        with pytest.raises(TypeError, match="not iterable"):
            iter(accessor)

        # These dunder methods are NOT explicitly defined, so __getattr__ guard catches them
        guarded_methods = ["__len__", "__contains__", "__getitem__"]

        for method in guarded_methods:
            assert not hasattr(accessor, method), f"{method} should be guarded by __getattr__"

    def test_macros_accessor_guards_dunder_methods(self):
        """
        Test that MacrosAccessor guards against dunder method introspection.

        __getattr__ is only called for attributes NOT explicitly defined on the class.
        """
        accessor = MacrosAccessor()

        # __iter__ is explicitly defined to raise TypeError
        with pytest.raises(TypeError, match="not iterable"):
            iter(accessor)

        # These dunder methods are NOT explicitly defined, so __getattr__ guard catches them
        guarded_methods = ["__len__", "__contains__", "__getitem__"]

        for method in guarded_methods:
            assert not hasattr(accessor, method), f"{method} should be guarded by __getattr__"

    def test_hasattr_guards_prevent_database_lookups(self):
        """Test that __getattr__ guards prevent database lookups for dunder methods."""
        conn = ConnectionAccessor()
        var = VariableAccessor(deserialize_json=False)
        macros = MacrosAccessor()

        # hasattr() calls getattr() and catches AttributeError
        # The guards in __getattr__ should raise AttributeError for dunders,
        # preventing database lookups

        # __iter__ exists explicitly (returns True), but doesn't do DB lookup
        assert hasattr(conn, "__iter__")
        assert hasattr(var, "__iter__")
        assert hasattr(macros, "__iter__")

        # Other dunder methods should be guarded (returns False)
        assert not hasattr(conn, "__len__")
        assert not hasattr(var, "__len__")
        assert not hasattr(macros, "__len__")

        assert not hasattr(conn, "__contains__")
        assert not hasattr(var, "__contains__")
        assert not hasattr(macros, "__contains__")

    def test_normal_attribute_access_still_works(self, mock_supervisor_comms):
        """Test that normal (non-dunder) attribute access still works normally."""
        from airflow.sdk.execution_time.comms import ConnectionResult

        # ConnectionAccessor should still work for normal connection names
        conn = ConnectionAccessor()
        mock_supervisor_comms.send.return_value = ConnectionResult(
            conn_id="test_conn", conn_type="postgres", host="localhost"
        )

        # This should work - it's a normal attribute access
        result = conn.test_conn
        assert result.conn_id == "test_conn"

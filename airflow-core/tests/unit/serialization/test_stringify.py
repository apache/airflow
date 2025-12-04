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

from dataclasses import dataclass
from typing import ClassVar

import pytest

from airflow.sdk.serde import serialize
from airflow.serialization.stringify import CLASSNAME, VERSION, stringify
from airflow.utils.module_loading import qualname


@dataclass
class W:
    __version__: ClassVar[int] = 2
    x: int


@dataclass
class V:
    __version__: ClassVar[int] = 1
    w: W
    s: list
    t: tuple
    c: int


class TestStringify:
    # Existing ported over tests
    def test_stringify(self):
        i = V(W(10), ["l1", "l2"], (1, 2), 10)
        e = serialize(i)
        s = stringify(e)

        assert f"{qualname(V)}@version={V.__version__}" in s
        # asdict from dataclasses removes class information
        assert "w={'x': 10}" in s
        assert "s=['l1', 'l2']" in s
        assert "t=(1,2)" in s
        assert "c=10" in s
        e["__data__"]["t"] = (1, 2)

        s = stringify(e)
        assert "t=(1, 2)" in s

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (123, "dummy@version=1(123)"),
            ([1], "dummy@version=1(1)"),
        ],
    )
    def test_serde_stringify_primitives(self, value, expected):
        e = {CLASSNAME: "dummy", VERSION: 1, "__data__": value}
        assert stringify(e) == expected

    # New tests
    def test_stringify_none(self):
        assert stringify(None) is None

    @pytest.mark.parametrize(
        "value",
        [
            42,
            "hello",
            True,
            False,
            3.14,
        ],
    )
    def test_stringify_primitives(self, value):
        assert stringify(value) == value

    def test_stringify_raw_list(self):
        result = stringify([1, 2, 3])
        assert result == [1, 2, 3]
        assert isinstance(result, list)

    def test_stringify_raw_tuple(self):
        result = stringify((1, 2, 3))
        assert result == (1, 2, 3)
        assert isinstance(result, tuple)

    def test_stringify_raw_set(self):
        result = stringify({1, 2, 3})
        assert result == {1, 2, 3}
        assert isinstance(result, set)

    def test_stringify_raw_frozenset(self):
        result = stringify(frozenset({1, 2, 3}))
        assert result == [1, 2, 3]
        assert isinstance(result, list)

    def test_stringify_plain_dict(self):
        result = stringify({"key": "value", "num": 42})
        assert result == {"key": "value", "num": 42}
        assert isinstance(result, dict)

    def test_stringify_serialized_tuple(self):
        e = {CLASSNAME: "builtins.tuple", VERSION: 1, "__data__": [1, 2, 3]}
        result = stringify(e)
        assert result == "(1,2,3)"
        assert isinstance(result, str)

    def test_stringify_serialized_set(self):
        e = {CLASSNAME: "builtins.set", VERSION: 1, "__data__": [1, 2, 3]}
        result = stringify(e)
        assert result == "(1,2,3)"
        assert isinstance(result, str)

    def test_stringify_serialized_frozenset(self):
        e = {CLASSNAME: "builtins.frozenset", VERSION: 1, "__data__": [1, 2, 3]}
        result = stringify(e)
        assert result == "(1,2,3)"
        assert isinstance(result, str)

    def test_stringify_nested_serialized(self):
        e = {
            CLASSNAME: "test.Outer",
            VERSION: 1,
            "__data__": {
                "inner": {CLASSNAME: "builtins.tuple", VERSION: 1, "__data__": [1, 2, 3]},
            },
        }
        result = stringify(e)
        assert "test.Outer@version=1" in result
        assert "inner=(1,2,3)" in result

    def test_stringify_old_style_tuple(self):
        e = {"__type": "builtins.tuple", "__var": [1, 2, 3]}
        result = stringify(e)
        assert result == "(1,2,3)"

    def test_stringify_old_style_dict(self):
        e = {"__type": "dict", "__var": {"key": "value"}}
        result = stringify(e)
        assert result == {"key": "value"}
        assert isinstance(result, dict)

    def test_stringify_list_with_serialized_tuple(self):
        e = [{CLASSNAME: "builtins.tuple", VERSION: 1, "__data__": [1, 2]}]
        result = stringify(e)
        assert result == ["(1,2)"]
        assert isinstance(result, list)

    def test_stringify_dict_with_serialized_tuple(self):
        e = {"key": {CLASSNAME: "builtins.tuple", VERSION: 1, "__data__": [1, 2]}}
        result = stringify(e)
        assert result == {"key": "(1,2)"}
        assert isinstance(result, dict)

    def test_stringify_empty_list(self):
        assert stringify([]) == []

    def test_stringify_empty_tuple(self):
        assert stringify(()) == ()

    def test_stringify_empty_set(self):
        assert stringify(set()) == set()

    def test_stringify_empty_dict(self):
        assert stringify({}) == {}

    def test_stringify_dict_with_none_value(self):
        result = stringify({"key": None})
        assert result == {"key": None}

    def test_stringify_list_with_none(self):
        result = stringify([None, 1, None])
        assert result == [None, 1, None]

    def test_stringify_custom_object(self):
        e = {
            CLASSNAME: "deltalake.table.DeltaTable",
            VERSION: 1,
            "__data__": {"table_uri": "s3://bucket/path", "version": 0},
        }
        result = stringify(e)
        assert "deltalake.table.DeltaTable@version=1" in result
        assert "table_uri=s3://bucket/path" in result
        assert "version=0" in result

    def test_stringify_empty_classname_error(self):
        e = {CLASSNAME: "", VERSION: 1, "__data__": {}}
        with pytest.raises(TypeError, match="classname cannot be empty"):
            stringify(e)

    def test_stringify_already_deserialized_object(self):
        class CustomObj:
            def __init__(self):
                self.x = 10

        obj = CustomObj()
        result = stringify(obj)
        assert result is obj

    def test_stringify_nested_plain_dict(self):
        e = {"outer": {"inner": "value", "num": 42}}
        result = stringify(e)
        assert result == {"outer": {"inner": "value", "num": 42}}

    def test_stringify_recursive_collection(self):
        e = [[1, 2], [3, 4]]
        result = stringify(e)
        assert result == [[1, 2], [3, 4]]

    def test_stringify_dict_with_nested_serialized(self):
        e = {
            "key1": {CLASSNAME: "builtins.tuple", VERSION: 1, "__data__": [1, 2]},
            "key2": "value",
        }
        result = stringify(e)
        assert result == {"key1": "(1,2)", "key2": "value"}

    def test_error_thrown_for_airflow_classes(self):
        from airflow.sdk import AssetAlias

        e = serialize(AssetAlias("x"))
        with pytest.raises(ValueError, match="Cannot stringify Airflow class"):
            stringify(e)

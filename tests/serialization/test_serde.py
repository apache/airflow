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

import datetime
import enum
from dataclasses import dataclass
from importlib import import_module
from typing import ClassVar

import attr
import pytest
from pydantic import BaseModel

from airflow.datasets import Dataset
from airflow.serialization.serde import (
    CLASSNAME,
    DATA,
    SCHEMA_ID,
    VERSION,
    _get_patterns,
    _match,
    deserialize,
    serialize,
)
from airflow.utils.module_loading import import_string, iter_namespace, qualname
from tests.test_utils.config import conf_vars


@pytest.fixture()
def recalculate_patterns():
    _get_patterns.cache_clear()


class Z:
    __version__: ClassVar[int] = 1

    def __init__(self, x):
        self.x = x

    def serialize(self) -> dict:
        return dict({"x": self.x})

    @staticmethod
    def deserialize(data: dict, version: int):
        if version != 1:
            raise TypeError("version != 1")
        return Z(data["x"])

    def __eq__(self, other):
        return self.x == other.x


@attr.define
class Y:
    x: int
    __version__: ClassVar[int] = 1

    def __init__(self, x):
        self.x = x


class X:
    pass


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


class U(BaseModel):
    __version__: ClassVar[int] = 1
    x: int
    v: V
    u: tuple


class C:
    def __call__(self):
        return None


@pytest.mark.usefixtures("recalculate_patterns")
class TestSerDe:
    def test_ser_primitives(self):
        i = 10
        e = serialize(i)
        assert i == e

        i = 10.1
        e = serialize(i)
        assert i == e

        i = "test"
        e = serialize(i)
        assert i == e

        i = True
        e = serialize(i)
        assert i == e

        Color = enum.IntEnum("Color", ["RED", "GREEN"])
        i = Color.RED
        e = serialize(i)
        assert i == e

    def test_ser_collections(self):
        i = [1, 2]
        e = deserialize(serialize(i))
        assert i == e

        i = ("a", "b", "a", "c")
        e = deserialize(serialize(i))
        assert i == e

        i = {2, 3}
        e = deserialize(serialize(i))
        assert i == e

        i = frozenset({6, 7})
        e = deserialize(serialize(i))
        assert i == e

    def test_der_collections_compat(self):
        i = [1, 2]
        e = deserialize(i)
        assert i == e

        i = ("a", "b", "a", "c")
        e = deserialize(i)
        assert i == e

        i = {2, 3}
        e = deserialize(i)
        assert i == e

    def test_ser_plain_dict(self):
        i = {"a": 1, "b": 2}
        e = serialize(i)
        assert i == e

        with pytest.raises(AttributeError, match="^reserved"):
            i = {CLASSNAME: "cannot"}
            serialize(i)

        with pytest.raises(AttributeError, match="^reserved"):
            i = {SCHEMA_ID: "cannot"}
            serialize(i)

    def test_no_serializer(self):
        with pytest.raises(TypeError, match="^cannot serialize"):
            i = Exception
            serialize(i)

    def test_ser_registered(self):
        i = datetime.datetime(2000, 10, 1)
        e = serialize(i)
        assert e[DATA]

    def test_serder_custom(self):
        i = Z(1)
        e = serialize(i)
        assert Z.__version__ == e[VERSION]
        assert qualname(Z) == e[CLASSNAME]
        assert e[DATA]

        d = deserialize(e)
        assert i.x == getattr(d, "x", None)

    def test_serder_attr(self):
        i = Y(10)
        e = serialize(i)
        assert Y.__version__ == e[VERSION]
        assert qualname(Y) == e[CLASSNAME]
        assert e[DATA]

        d = deserialize(e)
        assert i.x == getattr(d, "x", None)

    def test_serder_dataclass(self):
        i = W(12)
        e = serialize(i)
        assert W.__version__ == e[VERSION]
        assert qualname(W) == e[CLASSNAME]
        assert e[DATA]

        d = deserialize(e)
        assert i.x == getattr(d, "x", None)

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "airflow[.].*",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_for_imports(self):
        i = Z(10)
        e = serialize(i)
        with pytest.raises(ImportError) as ex:
            deserialize(e)

        assert f"{qualname(Z)} was not found in allow list" in str(ex.value)

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "tests.*",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_replace(self):
        assert _match("tests.airflow.deep")
        assert _match("testsfault") is False

    def test_incompatible_version(self):
        data = dict(
            {
                "__classname__": Y.__module__ + "." + Y.__qualname__,
                "__version__": 2,
            }
        )
        with pytest.raises(TypeError, match="newer than"):
            deserialize(data)

    def test_raise_undeserializable(self):
        data = dict(
            {
                "__classname__": X.__module__ + "." + X.__qualname__,
                "__version__": 0,
            }
        )
        with pytest.raises(TypeError, match="No deserializer"):
            deserialize(data)

    def test_backwards_compat(self):
        """
        Verify deserialization of old-style encoded Xcom values including nested ones
        """
        uri = "s3://does_not_exist"
        data = {
            "__type": "airflow.datasets.Dataset",
            "__source": None,
            "__var": {
                "__var": {
                    "uri": uri,
                    "extra": {
                        "__var": {"hi": "bye"},
                        "__type": "dict",
                    },
                },
                "__type": "dict",
            },
        }
        dataset = deserialize(data)
        assert dataset.extra == {"hi": "bye"}
        assert dataset.uri == uri

    def test_backwards_compat_wrapped(self):
        """
        Verify deserialization of old-style wrapped XCom value
        """
        i = {
            "extra": {"__var": {"hi": "bye"}, "__type": "dict"},
        }
        e = deserialize(i)
        assert e["extra"] == {"hi": "bye"}

    def test_encode_dataset(self):
        dataset = Dataset("mytest://dataset")
        obj = deserialize(serialize(dataset))
        assert dataset.uri == obj.uri

    def test_serializers_importable_and_str(self):
        """test if all distributed serializers are lazy loading and can be imported"""
        import airflow.serialization.serializers

        for _, name, _ in iter_namespace(airflow.serialization.serializers):
            mod = import_module(name)
            for s in getattr(mod, "serializers", list()):
                if not isinstance(s, str):
                    raise TypeError(f"{s} is not of type str. This is required for lazy loading")
                try:
                    import_string(s)
                except ImportError:
                    raise AttributeError(f"{s} cannot be imported (located in {name})")

    def test_stringify(self):
        i = V(W(10), ["l1", "l2"], (1, 2), 10)
        e = serialize(i)
        s = deserialize(e, full=False)

        assert f"{qualname(V)}@version={V.__version__}" in s
        # asdict from dataclasses removes class information
        assert "w={'x': 10}" in s
        assert "s=['l1', 'l2']" in s
        assert "t=(1,2)" in s
        assert "c=10" in s
        e["__data__"]["t"] = (1, 2)

        s = deserialize(e, full=False)

    @pytest.mark.parametrize(
        "obj, expected",
        [
            (
                Z(10),
                {
                    "__classname__": "tests.serialization.test_serde.Z",
                    "__version__": 1,
                    "__data__": {"x": 10},
                },
            ),
            (
                W(2),
                {"__classname__": "tests.serialization.test_serde.W", "__version__": 2, "__data__": {"x": 2}},
            ),
        ],
    )
    def test_serialized_data(self, obj, expected):
        assert expected == serialize(obj)

    def test_deserialize_non_serialized_data(self):
        i = Z(10)
        e = deserialize(i)
        assert i == e

    def test_pydantic(self):
        i = U(x=10, v=V(W(10), ["l1", "l2"], (1, 2), 10), u=(1, 2))
        e = serialize(i)
        s = deserialize(e)
        assert i == s

    def test_error_when_serializing_callable_without_name(self):
        i = C()
        with pytest.raises(
            TypeError, match="cannot serialize object of type <class 'tests.serialization.test_serde.C'>"
        ):
            serialize(i)

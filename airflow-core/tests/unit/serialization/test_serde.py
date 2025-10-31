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
import textwrap
from collections import namedtuple
from dataclasses import dataclass
from importlib import import_module, metadata
from typing import ClassVar

import attr
import pytest
from packaging import version
from pydantic import BaseModel

from airflow.sdk.definitions.asset import Asset
from airflow.serialization.serde import (
    CLASSNAME,
    DATA,
    SCHEMA_ID,
    VERSION,
    _get_patterns,
    _get_regexp_patterns,
    _match,
    _match_glob,
    _match_regexp,
    deserialize,
    serialize,
)
from airflow.utils.module_loading import import_string, iter_namespace, qualname

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def recalculate_patterns():
    _get_patterns.cache_clear()
    _get_regexp_patterns.cache_clear()
    _match_glob.cache_clear()
    _match_regexp.cache_clear()
    try:
        yield
    finally:
        _get_patterns.cache_clear()
        _get_regexp_patterns.cache_clear()
        _match_glob.cache_clear()
        _match_regexp.cache_clear()


def generate_serializers_importable_tests():
    """
    Generate test cases for `test_serializers_importable_and_str`.

    The function iterates through all the modules defined under `airflow.serialization.serializers`. It loads
    the import strings defined in the `serializers` from each module, and create a test case to verify that the
    serializer is importable.
    """
    import airflow.serialization.serializers

    NUMPY_VERSION = version.parse(metadata.version("numpy"))

    serializer_tests = []

    for _, name, _ in iter_namespace(airflow.serialization.serializers):
        ############################################################
        # Handle compatibility / optional dependency at module level
        ############################################################
        # https://github.com/apache/airflow/pull/37320
        if name == "airflow.serialization.serializers.iceberg":
            try:
                import pyiceberg  # noqa: F401
            except ImportError:
                continue
        # https://github.com/apache/airflow/pull/38074
        if name == "airflow.serialization.serializers.deltalake":
            try:
                import deltalake  # noqa: F401
            except ImportError:
                continue
        mod = import_module(name)
        for s in getattr(mod, "serializers", list()):
            ############################################################
            # Handle compatibility issue at serializer level
            ############################################################
            if s == "numpy.bool" and NUMPY_VERSION.major < 2:
                reason = textwrap.dedent(f"""\
                    Current NumPy version: {NUMPY_VERSION}

                    In NumPy 1.20, `numpy.bool` was deprecated as an alias for the built-in `bool`.
                    For NumPy versions <= 1.26, attempting to import `numpy.bool` raises an ImportError.
                    Starting with NumPy 2.0, `numpy.bool` is reintroduced as the NumPy scalar type,
                    and `numpy.bool_` becomes an alias for `numpy.bool`.

                    The serializers are loaded lazily at runtime. As a result:
                    - With NumPy <= 1.26, only `numpy.bool_` is loaded.
                    - With NumPy >= 2.0, only `numpy.bool` is loaded.

                    This test case deliberately attempts to import both `numpy.bool` and `numpy.bool_`,
                    regardless of the installed NumPy version. Therefore, when NumPy <= 1.26 is installed,
                    importing `numpy.bool` will raise an ImportError.
                """)
                serializer_tests.append(pytest.param(name, s, marks=pytest.mark.skip(reason=reason)))
            else:
                serializer_tests.append(pytest.param(name, s))
    return serializer_tests


SERIALIZER_TESTS = generate_serializers_importable_tests()


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


@attr.define
class T:
    x: int
    y: Y
    u: tuple
    w: W

    __version__: ClassVar[int] = 1


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

        i = {CLASSNAME: "cannot"}
        with pytest.raises(AttributeError, match="^reserved"):
            serialize(i)

        i = {SCHEMA_ID: "cannot"}
        with pytest.raises(AttributeError, match="^reserved"):
            serialize(i)

    def test_ser_namedtuple(self):
        CustomTuple = namedtuple("CustomTuple", ["id", "value"])
        data = CustomTuple(id=1, value="something")

        i = deserialize(serialize(data))
        e = (1, "something")
        assert i == e

    def test_no_serializer(self):
        i = Exception
        with pytest.raises(TypeError, match="^cannot serialize"):
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
            ("core", "allowed_deserialization_classes"): "airflow.*",
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
            ("core", "allowed_deserialization_classes"): "airflow.*",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_for_deserialize_pydantic_model(self):
        # for Pydantic model to be deserialized, it must be in `allowed_deserialization_classes`
        i = U(x=7, v=V(w=W(x=42), s=["hello", "world"], t=(1, 2, 3), c=99), u=("extra", 123))
        e = serialize(i)
        with pytest.raises(ImportError) as ex:
            deserialize(e)

        assert f"{qualname(U)} was not found in allow list" in str(ex.value)

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "unit.airflow.*",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_match(self):
        assert _match("unit.airflow.deep")
        assert _match("unit.wrongpath") is False

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "unit.airflow.deep",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_match_class(self):
        """
        Test the match function when passing a full classname as
        allowed_deserialization_classes
        """
        assert _match("unit.airflow.deep")
        assert _match("unit.airflow.FALSE") is False

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "",
            ("core", "allowed_deserialization_classes_regexp"): r"unit\.airflow\..",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_match_regexp(self):
        """
        Test the match function when passing a path as
        allowed_deserialization_classes_regexp with no glob pattern defined
        """
        assert _match("unit.airflow.deep")
        assert _match("unit.wrongpath") is False

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "",
            ("core", "allowed_deserialization_classes_regexp"): r"unit\.airflow\.deep",
        }
    )
    @pytest.mark.usefixtures("recalculate_patterns")
    def test_allow_list_match_class_regexp(self):
        """
        Test the match function when passing a full classname as
        allowed_deserialization_classes_regexp with no glob pattern defined
        """
        assert _match("unit.airflow.deep")
        assert _match("unit.airflow.FALSE") is False

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
        uri = "s3://does/not/exist"
        data = {
            "__type": "airflow.sdk.definitions.asset.Asset",
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
        asset = deserialize(data)
        assert asset.extra == {"hi": "bye"}
        assert asset.uri == uri

    def test_backwards_compat_wrapped(self):
        """
        Verify deserialization of old-style wrapped XCom value
        """
        i = {
            "extra": {"__var": {"hi": "bye"}, "__type": "dict"},
        }
        e = deserialize(i)
        assert e["extra"] == {"hi": "bye"}

    def test_encode_asset(self):
        asset = Asset(uri="mytest://asset", name="test")
        obj = deserialize(serialize(asset))
        assert asset.uri == obj.uri

    @pytest.mark.parametrize("name, s", SERIALIZER_TESTS)
    def test_serializers_importable_and_str(self, name, s):
        """Test if all distributed serializers are lazy loading and can be imported"""
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
                    "__classname__": "unit.serialization.test_serde.Z",
                    "__version__": 1,
                    "__data__": {"x": 10},
                },
            ),
            (
                W(2),
                {"__classname__": "unit.serialization.test_serde.W", "__version__": 2, "__data__": {"x": 2}},
            ),
        ],
    )
    def test_serialized_data(self, obj, expected):
        assert expected == serialize(obj)

    def test_deserialize_non_serialized_data(self):
        i = Z(10)
        e = deserialize(i)
        assert i == e

    def test_attr(self):
        i = T(y=Y(10), u=(1, 2), x=10, w=W(11))
        e = serialize(i)
        s = deserialize(e)
        assert i == s

    def test_error_when_serializing_callable_without_name(self):
        i = C()
        with pytest.raises(
            TypeError, match="cannot serialize object of type <class 'unit.serialization.test_serde.C'>"
        ):
            serialize(i)

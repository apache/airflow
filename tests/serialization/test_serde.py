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
from typing import ClassVar

import attr
import pytest

from airflow.datasets import Dataset
from airflow.serialization.serde import (
    CLASSNAME,
    DATA,
    SCHEMA_ID,
    VERSION,
    _compile_patterns,
    _match,
    deserialize,
    serialize,
)
from airflow.utils.module_loading import qualname
from tests.test_utils.config import conf_vars


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


class TestSerDe:
    @pytest.fixture(autouse=True)
    def ensure_clean_allow_list(self):
        _compile_patterns()
        yield

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

    def test_ser_iterables(self):
        i = [1, 2]
        e = serialize(i)
        assert i == e

        i = ("a", "b", "a", "c")
        e = serialize(i)
        assert i == e

        i = {2, 3}
        e = serialize(i)
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
    def test_allow_list_for_imports(self):
        _compile_patterns()
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
    def test_allow_list_replace(self):
        _compile_patterns()
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
        uri = "s3://does_not_exist"
        data = {
            "__type": "airflow.datasets.Dataset",
            "__source": None,
            "__var": {
                "__var": {
                    "uri": uri,
                    "extra": None,
                },
                "__type": "dict",
            },
        }
        dataset = deserialize(data)
        assert dataset.uri == uri

    def test_encode_dataset(self):
        dataset = Dataset("mytest://dataset")
        obj = deserialize(serialize(dataset))
        assert dataset.uri == obj.uri

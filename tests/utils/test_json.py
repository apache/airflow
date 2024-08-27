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

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar

import numpy as np
import pendulum
import pytest

from airflow.assets import Asset
from airflow.utils import json as utils_json


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


@dataclass
class U:
    __version__: ClassVar[int] = 2
    x: int


class TestWebEncoder:
    def test_encode_datetime(self):
        obj = datetime.strptime("2017-05-21 00:00:00", "%Y-%m-%d %H:%M:%S")
        assert json.dumps(obj, cls=utils_json.WebEncoder) == '"2017-05-21T00:00:00+00:00"'

    def test_encode_pendulum(self):
        obj = pendulum.datetime(2017, 5, 21, tz="Asia/Kolkata")
        assert json.dumps(obj, cls=utils_json.WebEncoder) == '"2017-05-21T00:00:00+05:30"'

    def test_encode_date(self):
        assert json.dumps(date(2017, 5, 21), cls=utils_json.WebEncoder) == '"2017-05-21"'

    def test_encode_numpy_int(self):
        assert json.dumps(np.int32(5), cls=utils_json.WebEncoder) == "5"

    def test_encode_numpy_bool(self):
        assert json.dumps(np.bool_(True), cls=utils_json.WebEncoder) == "true"

    def test_encode_numpy_float(self):
        assert json.dumps(np.float16(3.76953125), cls=utils_json.WebEncoder) == "3.76953125"


class TestXComEncoder:
    def test_encode_raises(self):
        with pytest.raises(TypeError, match="^.*is not JSON serializable$"):
            json.dumps(
                Exception,
                cls=utils_json.XComEncoder,
            )

    def test_encode_xcom_asset(self):
        asset = Asset("mytest://asset")
        s = json.dumps(asset, cls=utils_json.XComEncoder)
        obj = json.loads(s, cls=utils_json.XComDecoder)
        assert asset.uri == obj.uri

    @pytest.mark.parametrize(
        "data",
        [
            ({"foo": 1, "bar": 2},),
            ({"foo": 1, "bar": 2, "baz": Z(1)},),
            (
                {"foo": 1, "bar": 2},
                {"foo": 1, "bar": 2, "baz": Z(1)},
            ),
            ({"d1": {"d2": 3}},),
            ({"d1": {"d2": Z(1)}},),
            ({"d1": {"d2": {"d3": 4}}},),
            ({"d1": {"d2": {"d3": Z(1)}}},),
        ],
    )
    def test_encode_xcom_with_nested_dict(self, data):
        i = json.dumps(data, cls=utils_json.XComEncoder)
        e = json.loads(i, cls=utils_json.XComDecoder)
        assert data == e

    def test_orm_deserialize(self):
        x = 14
        u = U(x=x)
        s = json.dumps(u, cls=utils_json.XComEncoder)
        o = json.loads(s, cls=utils_json.XComDecoder, object_hook=utils_json.XComDecoder.orm_object_hook)
        assert o == f"{U.__module__}.{U.__qualname__}@version={U.__version__}(x={x})"

    def test_collections(self):
        i = [1, 2]
        e = json.loads(json.dumps(i, cls=utils_json.XComEncoder), cls=utils_json.XComDecoder)
        assert i == e

        i = ("a", "b", "a", "c")
        e = json.loads(json.dumps(i, cls=utils_json.XComEncoder), cls=utils_json.XComDecoder)
        assert i == e

        i = {2, 3}
        e = json.loads(json.dumps(i, cls=utils_json.XComEncoder), cls=utils_json.XComDecoder)
        assert i == e

        i = frozenset({6, 7})
        e = json.loads(json.dumps(i, cls=utils_json.XComEncoder), cls=utils_json.XComDecoder)
        assert i == e

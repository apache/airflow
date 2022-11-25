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

import decimal
import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar

import attr
import numpy as np
import pendulum
import pytest

from airflow.datasets import Dataset
from airflow.utils import json as utils_json
from tests.test_utils.config import conf_vars


class Z:
    version = 1

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
    version: ClassVar[int] = 1

    def __init__(self, x):
        self.x = x


class X:
    pass


@dataclass
class U:
    version: ClassVar[int] = 2
    x: int


class TestAirflowJsonEncoder:
    def test_encode_datetime(self):
        obj = datetime.strptime("2017-05-21 00:00:00", "%Y-%m-%d %H:%M:%S")
        assert json.dumps(obj, cls=utils_json.AirflowJsonEncoder) == '"2017-05-21T00:00:00+00:00"'

    def test_encode_pendulum(self):
        obj = pendulum.datetime(2017, 5, 21, tz="Asia/Kolkata")
        assert json.dumps(obj, cls=utils_json.AirflowJsonEncoder) == '"2017-05-21T00:00:00+05:30"'

    def test_encode_date(self):
        assert json.dumps(date(2017, 5, 21), cls=utils_json.AirflowJsonEncoder) == '"2017-05-21"'

    @pytest.mark.parametrize(
        "expr, expected",
        [("1", "1"), ("52e4", "520000"), ("2e0", "2"), ("12e-2", "0.12"), ("12.34", "12.34")],
    )
    def test_encode_decimal(self, expr, expected):
        assert json.dumps(decimal.Decimal(expr), cls=utils_json.AirflowJsonEncoder) == expected

    def test_encode_numpy_int(self):
        assert json.dumps(np.int32(5), cls=utils_json.AirflowJsonEncoder) == "5"

    def test_encode_numpy_bool(self):
        assert json.dumps(np.bool_(True), cls=utils_json.AirflowJsonEncoder) == "true"

    def test_encode_numpy_float(self):
        assert json.dumps(np.float16(3.76953125), cls=utils_json.AirflowJsonEncoder) == "3.76953125"

    def test_encode_k8s_v1pod(self):
        from kubernetes.client import models as k8s

        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="foo",
                namespace="bar",
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="foo",
                        image="bar",
                    )
                ]
            ),
        )
        assert json.loads(json.dumps(pod, cls=utils_json.AirflowJsonEncoder)) == {
            "metadata": {"name": "foo", "namespace": "bar"},
            "spec": {"containers": [{"image": "bar", "name": "foo"}]},
        }

    def test_encode_raises(self):
        with pytest.raises(TypeError, match="^.*is not JSON serializable$"):
            json.dumps(
                Exception,
                cls=utils_json.AirflowJsonEncoder,
            )

    def test_encode_xcom_dataset(self):
        dataset = Dataset("mytest://dataset")
        s = json.dumps(dataset, cls=utils_json.XComEncoder)
        obj = json.loads(s, cls=utils_json.XComDecoder)
        assert dataset.uri == obj.uri

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
        decoder = utils_json.XComDecoder()
        dataset = decoder.object_hook(data)
        assert dataset.uri == uri

    def test_raise_on_reserved(self):
        data = {"__classname__": "my.class"}
        with pytest.raises(AttributeError):
            json.dumps(data, cls=utils_json.XComEncoder)

    def test_custom_serialize(self):
        x = 11
        z = Z(x)
        s = json.dumps(z, cls=utils_json.XComEncoder)
        o = json.loads(s, cls=utils_json.XComDecoder)

        assert o.x == x

    def test_raise_undeserializable(self):
        data = dict(
            {
                "__classname__": X.__module__ + "." + X.__qualname__,
                "__version__": 0,
            }
        )
        s = json.dumps(data)
        with pytest.raises(TypeError) as info:
            json.loads(s, cls=utils_json.XComDecoder)

        assert "cannot deserialize" in str(info.value)

    def test_attr_version(self):
        x = 14
        y = Y(x)
        s = json.dumps(y, cls=utils_json.XComEncoder)
        o = json.loads(s, cls=utils_json.XComDecoder)

        assert o.x == x

    def test_incompatible_version(self):
        data = dict(
            {
                "__classname__": Y.__module__ + "." + Y.__qualname__,
                "__version__": 2,
            }
        )
        s = json.dumps(data)
        with pytest.raises(TypeError) as info:
            json.loads(s, cls=utils_json.XComDecoder)

        assert "newer than" in str(info.value)

    def test_dataclass(self):
        x = 12
        u = U(x=x)
        s = json.dumps(u, cls=utils_json.XComEncoder)
        o = json.loads(s, cls=utils_json.XComDecoder)

        assert o.x == x

    def test_orm_deserialize(self):
        x = 14
        u = U(x=x)
        s = json.dumps(u, cls=utils_json.XComEncoder)
        o = json.loads(s, cls=utils_json.XComDecoder, object_hook=utils_json.XComDecoder.orm_object_hook)
        assert o == f"{U.__module__}.{U.__qualname__}@version={U.version}(x={x})"

    def test_orm_custom_deserialize(self):
        x = 14
        z = Z(x=x)
        s = json.dumps(z, cls=utils_json.XComEncoder)
        o = json.loads(s, cls=utils_json.XComDecoder, object_hook=utils_json.XComDecoder.orm_object_hook)
        assert o == f"{Z.__module__}.{Z.__qualname__}@version={Z.version}(x={x})"

    @conf_vars(
        {
            ("core", "allowed_deserialization_classes"): "airflow[.].*",
        }
    )
    def test_allow_list_for_imports(self):
        x = 14
        z = Z(x=x)
        s = json.dumps(z, cls=utils_json.XComEncoder)

        with pytest.raises(ImportError) as e:
            json.loads(s, cls=utils_json.XComDecoder)

        assert f"{Z.__module__}.{Z.__qualname__} was not found in allow list" in str(e.value)

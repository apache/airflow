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
import decimal

import numpy
import pandas as pd
import pendulum.tz
import pytest
from pendulum import DateTime

from airflow.models.param import Param, ParamsDict
from airflow.serialization.serde import DATA, deserialize, serialize


class TestSerializers:
    def test_datetime(self):
        i = datetime.datetime(2022, 7, 10, 22, 10, 43, microsecond=0, tzinfo=pendulum.tz.UTC)
        s = serialize(i)
        d = deserialize(s)
        assert i.timestamp() == d.timestamp()

        i = DateTime(2022, 7, 10, tzinfo=pendulum.tz.UTC)
        s = serialize(i)
        d = deserialize(s)
        assert i.timestamp() == d.timestamp()

        i = datetime.date(2022, 7, 10)
        s = serialize(i)
        d = deserialize(s)
        assert i == d

        i = datetime.timedelta(days=320)
        s = serialize(i)
        d = deserialize(s)
        assert i == d

        i = datetime.datetime(
            2022, 7, 10, 22, 10, 43, microsecond=0, tzinfo=pendulum.timezone("America/New_York")
        )
        s = serialize(i)
        d = deserialize(s)
        assert i.timestamp() == d.timestamp()

        i = DateTime(2022, 7, 10, tzinfo=pendulum.timezone("America/New_York"))
        s = serialize(i)
        d = deserialize(s)
        assert i.timestamp() == d.timestamp()

    def test_deserialize_datetime_v1(self):

        s = {
            "__classname__": "pendulum.datetime.DateTime",
            "__version__": 1,
            "__data__": {"timestamp": 1657505443.0, "tz": "UTC"},
        }
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "UTC"

        s["__data__"]["tz"] = "Europe/Paris"
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "Europe/Paris"

        s["__data__"]["tz"] = "America/New_York"
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "America/New_York"

        s["__data__"]["tz"] = "EDT"
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "-04:00"
        # assert that it's serializable with the new format
        assert deserialize(serialize(d)) == d

        s["__data__"]["tz"] = "CDT"
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "-05:00"
        # assert that it's serializable with the new format
        assert deserialize(serialize(d)) == d

        s["__data__"]["tz"] = "MDT"
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "-06:00"
        # assert that it's serializable with the new format
        assert deserialize(serialize(d)) == d

        s["__data__"]["tz"] = "PDT"
        d = deserialize(s)
        assert d.timestamp() == 1657505443.0
        assert d.tzinfo.name == "-07:00"
        # assert that it's serializable with the new format
        assert deserialize(serialize(d)) == d

    @pytest.mark.parametrize(
        "expr, expected",
        [("1", "1"), ("52e4", "520000"), ("2e0", "2"), ("12e-2", "0.12"), ("12.34", "12.34")],
    )
    def test_encode_decimal(self, expr, expected):
        assert deserialize(serialize(decimal.Decimal(expr))) == decimal.Decimal(expected)

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
        assert serialize(pod)[DATA] == {
            "metadata": {"name": "foo", "namespace": "bar"},
            "spec": {"containers": [{"image": "bar", "name": "foo"}]},
        }

    def test_numpy(self):
        i = numpy.int16(10)
        e = serialize(i)
        d = deserialize(e)
        assert i == d

    def test_params(self):
        i = ParamsDict({"x": Param(default="value", description="there is a value", key="test")})
        e = serialize(i)
        d = deserialize(e)
        assert i["x"] == d["x"]

    def test_pandas(self):
        i = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        e = serialize(i)
        d = deserialize(e)
        assert i.equals(d)

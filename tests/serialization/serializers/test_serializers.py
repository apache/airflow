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
from unittest.mock import patch

import numpy as np
import pendulum.tz
import pytest
from dateutil.tz import tzutc
from deltalake import DeltaTable
from pendulum import DateTime
from pyiceberg.catalog import Catalog
from pyiceberg.io import FileIO
from pyiceberg.table import Table

from airflow import PY39
from airflow.models.param import Param, ParamsDict
from airflow.serialization.serde import DATA, deserialize, serialize

if PY39:
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


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

        i = DateTime(2022, 7, 10, tzinfo=tzutc())
        s = serialize(i)
        d = deserialize(s)
        assert i.timestamp() == d.timestamp()

        i = DateTime(2022, 7, 10, tzinfo=ZoneInfo("Europe/Paris"))
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
        i = np.int16(10)
        e = serialize(i)
        d = deserialize(e)
        assert i == d

    def test_params(self):
        i = ParamsDict({"x": Param(default="value", description="there is a value", key="test")})
        e = serialize(i)
        d = deserialize(e)
        assert i["x"] == d["x"]

    def test_pandas(self):
        import pandas as pd

        i = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        e = serialize(i)
        d = deserialize(e)
        assert i.equals(d)

    @patch.object(Catalog, "__abstractmethods__", set())
    @patch.object(FileIO, "__abstractmethods__", set())
    @patch("pyiceberg.catalog.Catalog.load_table")
    @patch("pyiceberg.catalog.load_catalog")
    def test_iceberg(self, mock_load_catalog, mock_load_table):
        uri = "http://rest.no.where"
        catalog = Catalog("catalog", uri=uri)
        identifier = ("catalog", "schema", "table")
        mock_load_catalog.return_value = catalog

        i = Table(identifier, "bar", catalog=catalog, metadata_location="", io=FileIO())
        mock_load_table.return_value = i

        e = serialize(i)
        d = deserialize(e)
        assert i == d
        mock_load_catalog.assert_called_with("catalog", uri=uri)
        mock_load_table.assert_called_with((identifier[1], identifier[2]))

    @patch("deltalake.table.Metadata")
    @patch("deltalake.table.RawDeltaTable")
    @patch.object(DeltaTable, "version", return_value=0)
    @patch.object(DeltaTable, "table_uri", new_callable=lambda: "/tmp/bucket/path")
    def test_deltalake(self, mock_table_uri, mock_version, mock_deltalake, mock_metadata):
        uri = "/tmp/bucket/path"

        i = DeltaTable(uri, storage_options={"key": "value"})

        e = serialize(i)
        d = deserialize(e)
        assert i.table_uri == d.table_uri
        assert i.version() == d.version()
        assert i._storage_options == d._storage_options

        i = DeltaTable(uri)
        e = serialize(i)
        d = deserialize(e)
        assert i.table_uri == d.table_uri
        assert i.version() == d.version()
        assert i._storage_options == d._storage_options
        assert d._storage_options is None

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
import sys
from importlib import metadata
from unittest.mock import patch

import numpy as np
import pendulum
import pendulum.tz
import pytest
from dateutil.tz import tzutc
from packaging import version
from pendulum import DateTime
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow.models.param import Param, ParamsDict
from airflow.serialization.serde import DATA, deserialize, serialize

if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo

PENDULUM3 = version.parse(metadata.version("pendulum")).major == 3


class TestSerializers:
    def test_datetime(self):
        i = datetime.datetime(
            2022, 7, 10, 22, 10, 43, microsecond=0, tzinfo=pendulum.tz.UTC
        )
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
            2022,
            7,
            10,
            22,
            10,
            43,
            microsecond=0,
            tzinfo=pendulum.timezone("America/New_York"),
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

        i = datetime.datetime.now()
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
        [
            ("1", "1"),
            ("52e4", "520000"),
            ("2e0", "2"),
            ("12e-2", "0.12"),
            ("12.34", "12.34"),
        ],
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
        i = ParamsDict(
            {"x": Param(default="value", description="there is a value", key="test")}
        )
        e = serialize(i)
        d = deserialize(e)
        assert i["x"] == d["x"]

    def test_pandas(self):
        import pandas as pd

        i = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
        e = serialize(i)
        d = deserialize(e)
        assert i.equals(d)

    def test_iceberg(self):
        pytest.importorskip("pyiceberg", minversion="2.0.0")
        from pyiceberg.catalog import Catalog
        from pyiceberg.io import FileIO
        from pyiceberg.table import Table

        with patch.object(Catalog, "__abstractmethods__", set()), patch.object(
            FileIO, "__abstractmethods__", set()
        ), patch("pyiceberg.catalog.Catalog.load_table") as mock_load_table, patch(
            "pyiceberg.catalog.load_catalog"
        ) as mock_load_catalog:
            uri = "http://rest.no.where"
            catalog = Catalog("catalog", uri=uri)
            identifier = ("catalog", "schema", "table")
            mock_load_catalog.return_value = catalog

            i = Table(
                identifier, "bar", catalog=catalog, metadata_location="", io=FileIO()
            )
            mock_load_table.return_value = i

            e = serialize(i)
            d = deserialize(e)
            assert i == d
        mock_load_catalog.assert_called_with("catalog", uri=uri)
        mock_load_table.assert_called_with((identifier[1], identifier[2]))

    def test_deltalake(selfa):
        deltalake = pytest.importorskip("deltalake")

        with patch("deltalake.table.Metadata"), patch(
            "deltalake.table.RawDeltaTable"
        ), patch.object(deltalake.DeltaTable, "version", return_value=0), patch.object(
            deltalake.DeltaTable, "table_uri", new_callable=lambda: "/tmp/bucket/path"
        ):
            uri = "/tmp/bucket/path"

            i = deltalake.DeltaTable(uri, storage_options={"key": "value"})

            e = serialize(i)
            d = deserialize(e)
            assert i.table_uri == d.table_uri
            assert i.version() == d.version()
            assert i._storage_options == d._storage_options

            i = deltalake.DeltaTable(uri)
            e = serialize(i)
            d = deserialize(e)
            assert i.table_uri == d.table_uri
            assert i.version() == d.version()
            assert i._storage_options == d._storage_options
            assert d._storage_options is None

    @pytest.mark.skipif(not PENDULUM3, reason="Test case for pendulum~=3")
    @pytest.mark.parametrize(
        "ser_value, expected",
        [
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680307200.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                "UTC",
                                "pendulum.tz.timezone.FixedTimezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=Timezone("UTC")),
                id="in-utc-timezone",
            ),
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680292800.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                "Asia/Tbilisi",
                                "pendulum.tz.timezone.Timezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=Timezone("Asia/Tbilisi")),
                id="non-dts-timezone",
            ),
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680303600.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                "Europe/London",
                                "pendulum.tz.timezone.Timezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=Timezone("Europe/London")),
                id="dts-timezone",
            ),
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680310800.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                -3600,
                                "pendulum.tz.timezone.FixedTimezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=FixedTimezone(-3600)),
                id="offset-timezone",
            ),
        ],
    )
    def test_pendulum_2_to_3(self, ser_value, expected):
        """Test deserialize objects in pendulum 3 which serialised in pendulum 2."""
        assert deserialize(ser_value) == expected

    @pytest.mark.skipif(PENDULUM3, reason="Test case for pendulum~=2")
    @pytest.mark.parametrize(
        "ser_value, expected",
        [
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680307200.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": ["UTC", "pendulum.tz.timezone.Timezone", 1, True],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=Timezone("UTC")),
                id="in-utc-timezone",
            ),
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680292800.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                "Asia/Tbilisi",
                                "pendulum.tz.timezone.Timezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=Timezone("Asia/Tbilisi")),
                id="non-dts-timezone",
            ),
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680303600.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                "Europe/London",
                                "pendulum.tz.timezone.Timezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=Timezone("Europe/London")),
                id="dts-timezone",
            ),
            pytest.param(
                {
                    "__classname__": "pendulum.datetime.DateTime",
                    "__version__": 2,
                    "__data__": {
                        "timestamp": 1680310800.0,
                        "tz": {
                            "__classname__": "builtins.tuple",
                            "__version__": 1,
                            "__data__": [
                                -3600,
                                "pendulum.tz.timezone.FixedTimezone",
                                1,
                                True,
                            ],
                        },
                    },
                },
                pendulum.datetime(2023, 4, 1, tz=FixedTimezone(-3600)),
                id="offset-timezone",
            ),
        ],
    )
    def test_pendulum_3_to_2(self, ser_value, expected):
        """Test deserialize objects in pendulum 2 which serialised in pendulum 3."""
        assert deserialize(ser_value) == expected

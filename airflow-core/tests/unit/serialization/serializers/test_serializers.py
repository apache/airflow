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
from importlib import metadata
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np
import pendulum
import pendulum.tz
import pytest
from dateutil.tz import tzutc
from kubernetes.client import models as k8s
from packaging import version
from pendulum import DateTime
from pendulum.tz.timezone import FixedTimezone, Timezone

from airflow.sdk.definitions.param import Param, ParamsDict
from airflow.serialization.serde import CLASSNAME, DATA, VERSION, _stringify, decode, deserialize, serialize
from airflow.serialization.serializers import builtin
from airflow.utils.module_loading import qualname

from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

PENDULUM3 = version.parse(metadata.version("pendulum")).major == 3


class CustomTZ(datetime.tzinfo):
    name = "My/Custom"

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        return datetime.timedelta(hours=2)

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return datetime.timedelta(0)

    def tzname(self, dt: datetime.datetime | None) -> str | None:
        return self.name


class NoNameTZ(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(hours=2)


@skip_if_force_lowest_dependencies_marker
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
        [("1", "1"), ("52e4", "520000"), ("2e0", "2"), ("12e-2", "0.12"), ("12.34", "12.34")],
    )
    def test_encode_decimal(self, expr, expected):
        assert deserialize(serialize(decimal.Decimal(expr))) == decimal.Decimal(expected)

    def test_encode_k8s_v1pod(self):
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

    def test_bignum_serialize_non_decimal(self):
        from airflow.serialization.serializers.bignum import serialize

        assert serialize(12345) == ("", "", 0, False)

    @pytest.mark.parametrize(
        ("klass", "version", "payload", "msg"),
        [
            (
                "decimal.Decimal",
                999,
                "0",
                r"serialized 999 of decimal\.Decimal",  # newer version
            ),
            (
                "wrong.ClassName",
                1,
                "0",
                r"wrong\.ClassName != .*Decimal",  # wrong classname
            ),
        ],
    )
    def test_bignum_deserialize_errors(self, klass, version, payload, msg):
        from airflow.serialization.serializers.bignum import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, payload)

    def test_numpy(self):
        i = np.int16(10)
        e = serialize(i)
        d = deserialize(e)
        assert i == d

    def test_numpy_serializers(self):
        from airflow.serialization.serializers.numpy import serialize

        numpy_version = metadata.version("numpy")
        is_numpy_2 = version.parse(numpy_version).major == 2

        assert serialize(np.bool_(False)) == (False, "numpy.bool" if is_numpy_2 else "numpy.bool_", 1, True)
        if is_numpy_2:
            assert serialize(np.float64(3.14)) == (float(np.float64(3.14)), "numpy.float64", 1, True)
        else:
            assert serialize(np.float32(3.14)) == (float(np.float32(3.14)), "numpy.float32", 1, True)
        assert serialize(np.array([1, 2, 3])) == ("", "", 0, False)

    @pytest.mark.parametrize(
        ("klass", "ver", "value", "msg"),
        [
            ("numpy.int32", 999, 123, r"serialized version is newer"),
            ("numpy.float32", 1, 123, r"unsupported numpy\.float32"),
        ],
    )
    def test_numpy_deserialize_errors(self, klass, ver, value, msg):
        from airflow.serialization.serializers.numpy import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, ver, value)

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

    def test_pandas_serializers(self):
        from airflow.serialization.serializers.pandas import serialize

        assert serialize(123) == ("", "", 0, False)

    @pytest.mark.parametrize(
        ("version", "data", "msg"),
        [
            (999, "", r"serialized 999 .* > 1"),  # version too new
            (1, 123, r"wrong data type .*<class 'int'>"),  # bad payload type
        ],
    )
    def test_pandas_deserialize_errors(self, version, data, msg):
        from airflow.serialization.serializers.pandas import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize("pandas.core.frame.DataFrame", version, data)

    def test_iceberg(self):
        pytest.importorskip("pyiceberg", minversion="2.0.0")
        from pyiceberg.catalog import Catalog
        from pyiceberg.io import FileIO
        from pyiceberg.table import Table

        with (
            patch.object(Catalog, "__abstractmethods__", set()),
            patch.object(FileIO, "__abstractmethods__", set()),
            patch("pyiceberg.catalog.Catalog.load_table") as mock_load_table,
            patch("pyiceberg.catalog.load_catalog") as mock_load_catalog,
        ):
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

    def test_deltalake(self):
        deltalake = pytest.importorskip("deltalake")

        with (
            patch("deltalake.table.Metadata"),
            patch("deltalake.table.RawDeltaTable"),
            patch.object(deltalake.DeltaTable, "version", return_value=0),
            patch.object(deltalake.DeltaTable, "table_uri", new_callable=lambda: "/tmp/bucket/path"),
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

    def test_deltalake_serialize_deserialize(self):
        from airflow.serialization.serializers.deltalake import serialize

        assert serialize(object()) == ("", "", 0, False)

    @pytest.mark.parametrize(
        ("klass", "version", "payload", "msg"),
        [
            (
                "deltalake.table.DeltaTable",
                999,
                {},
                r"serialized version is newer than class version",
            ),
            (
                "not_a_real_class",
                1,
                {},
                r"do not know how to deserialize",
            ),
        ],
    )
    def test_deltalake_deserialize_errors(self, klass, version, payload, msg):
        from airflow.serialization.serializers.deltalake import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, payload)

    def test_kubernetes_serializer(self, monkeypatch):
        from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
        from airflow.serialization.serializers.kubernetes import serialize

        pod = k8s.V1Pod(metadata=k8s.V1ObjectMeta(name="foo"))
        monkeypatch.setattr(PodGenerator, "serialize_pod", lambda o: (_ for _ in ()).throw(Exception("fail")))
        assert serialize(pod) == ("", "", 0, False)
        assert serialize(123) == ("", "", 0, False)

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
                            "__data__": ["UTC", "pendulum.tz.timezone.FixedTimezone", 1, True],
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
                            "__data__": ["Asia/Tbilisi", "pendulum.tz.timezone.Timezone", 1, True],
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
                            "__data__": ["Europe/London", "pendulum.tz.timezone.Timezone", 1, True],
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
                            "__data__": [-3600, "pendulum.tz.timezone.FixedTimezone", 1, True],
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
                            "__data__": ["Asia/Tbilisi", "pendulum.tz.timezone.Timezone", 1, True],
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
                            "__data__": ["Europe/London", "pendulum.tz.timezone.Timezone", 1, True],
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
                            "__data__": [-3600, "pendulum.tz.timezone.FixedTimezone", 1, True],
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

    def test_timezone_serialize_fixed(self):
        from airflow.serialization.serializers.timezone import serialize

        assert serialize(FixedTimezone(0)) == ("UTC", "pendulum.tz.timezone.FixedTimezone", 1, True)

    def test_timezone_serialize_no_name(self):
        from airflow.serialization.serializers.timezone import serialize

        assert serialize(NoNameTZ()) == ("", "", 0, False)

    def test_timezone_deserialize_zoneinfo(self):
        from airflow.serialization.serializers.timezone import deserialize

        zi = deserialize("backports.zoneinfo.ZoneInfo", 1, "Asia/Taipei")
        assert isinstance(zi, ZoneInfo)
        assert zi.key == "Asia/Taipei"

    @pytest.mark.parametrize(
        "klass, version, data, msg",
        [
            ("pendulum.tz.timezone.FixedTimezone", 1, 1.23, "is not of type int or str"),
            ("pendulum.tz.timezone.FixedTimezone", 999, "UTC", "serialized 999 .* > 1"),
        ],
    )
    def test_timezone_deserialize_errors(self, klass, version, data, msg):
        from airflow.serialization.serializers.timezone import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, data)

    @pytest.mark.parametrize(
        "tz_obj, expected",
        [
            (None, None),
            (CustomTZ(), "My/Custom"),
            (ZoneInfo("Asia/Taipei"), "Asia/Taipei"),
        ],
    )
    def test_timezone_get_tzinfo_name(self, tz_obj, expected):
        from airflow.serialization.serializers.timezone import _get_tzinfo_name

        assert _get_tzinfo_name(tz_obj) == expected

    def test_json_schema_load_dag_schema_dict(self, monkeypatch):
        from airflow.exceptions import AirflowException
        from airflow.serialization.json_schema import load_dag_schema_dict

        monkeypatch.setattr(
            "airflow.serialization.json_schema.pkgutil.get_data", lambda __name__, fname: None
        )

        with pytest.raises(AirflowException) as ctx:
            load_dag_schema_dict()
        assert "Schema file schema.json does not exists" in str(ctx.value)

    def test_builtin_deserialize_frozenset(self):
        res = builtin.deserialize(qualname(frozenset), 1, [13, 14])
        assert isinstance(res, frozenset)
        assert res == frozenset({13, 14})

    def test_builtin_deserialize_version_too_new(self):
        with pytest.raises(TypeError, match="serialized version is newer than class version"):
            builtin.deserialize(qualname(tuple), 999, [1, 2])

    @pytest.mark.parametrize(
        "func, msg",
        [
            (builtin.deserialize, r"do not know how to deserialize"),
            (builtin.stringify, r"do not know how to stringify"),
        ],
    )
    def test_builtin_unknown_type_errors(self, func, msg):
        with pytest.raises(TypeError, match=msg):
            func("builtins.list", 1, [1, 2])

    def test_serde_decode_type_error(self):
        bad = {CLASSNAME: 123, VERSION: 1, DATA: {}}
        with pytest.raises(ValueError, match="cannot decode"):
            decode(bad)

    def test_serde_deserialize_with_type_hint_stringified(self):
        fake = {"a": 1, "b": 2, "__version__": 1}
        got = deserialize(fake, type_hint=dict, full=False)
        assert got == "builtins.dict@version=0(a=1,b=2,__version__=1)"

    def test_serde_deserialize_empty_classname(self):
        bad = {CLASSNAME: "", VERSION: 1, DATA: {}}
        with pytest.raises(TypeError, match="classname cannot be empty"):
            deserialize(bad)

    @pytest.mark.parametrize(
        "value, expected",
        [
            (123, "dummy@version=1(123)"),
            ([1], "dummy@version=1([,1,])"),
        ],
    )
    def test_serde_stringify_primitives(self, value, expected):
        assert _stringify("dummy", 1, value) == expected

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
from typing import ClassVar
from unittest.mock import patch
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pendulum
import pendulum.tz
import pytest
from dateutil.tz import tzutc
from kubernetes.client import models as k8s
from packaging import version
from pendulum import DateTime
from pendulum.tz.timezone import FixedTimezone, Timezone
from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass as pydantic_dataclass

from airflow.sdk.definitions.param import Param, ParamsDict
from airflow.sdk.serialization.serde import CLASSNAME, DATA, VERSION, decode, deserialize, serialize
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


class FooBarModel(BaseModel):
    """Pydantic BaseModel for testing Pydantic Serialization/Deserialization."""

    banana: float = 1.1
    foo: str = Field()


@pydantic_dataclass
class PydanticDataclass:
    __version__: ClassVar[int] = 1
    a: int
    b: str


@skip_if_force_lowest_dependencies_marker
class TestSerializers:
    @pytest.mark.parametrize(
        "input_obj",
        [
            pytest.param(
                datetime.datetime(2022, 7, 10, 22, 10, 43, microsecond=0, tzinfo=pendulum.tz.UTC),
                id="datetime_utc",
            ),
            pytest.param(DateTime(2022, 7, 10, tzinfo=pendulum.tz.UTC), id="pendulum_datetime_utc"),
            pytest.param(datetime.date(2022, 7, 10), id="date"),
            pytest.param(datetime.timedelta(days=320), id="timedelta"),
            pytest.param(
                datetime.datetime(
                    2022, 7, 10, 22, 10, 43, microsecond=0, tzinfo=pendulum.timezone("America/New_York")
                ),
                id="datetime_ny_tz",
            ),
            pytest.param(
                DateTime(2022, 7, 10, tzinfo=pendulum.timezone("America/New_York")),
                id="pendulum_datetime_ny_tz",
            ),
            pytest.param(DateTime(2022, 7, 10, tzinfo=tzutc()), id="datetime_tzutc"),
            pytest.param(DateTime(2022, 7, 10, tzinfo=ZoneInfo("Europe/Paris")), id="datetime_zoneinfo"),
            pytest.param(datetime.datetime.now(), id="datetime_now"),
        ],
    )
    def test_datetime(self, input_obj):
        """Test serialization and deserialization of various datetime-related objects."""
        serialized_obj = serialize(input_obj)
        deserialized_obj = deserialize(serialized_obj)
        if isinstance(input_obj, (datetime.date, datetime.timedelta)):
            assert input_obj == deserialized_obj
        else:
            assert input_obj.timestamp() == deserialized_obj.timestamp()

    @pytest.mark.parametrize(
        ("tz_input", "expected_tz_name"),
        [
            pytest.param("UTC", "UTC", id="utc"),
            pytest.param("Europe/Paris", "Europe/Paris", id="europe_paris"),
            pytest.param("America/New_York", "America/New_York", id="america_new_york"),
            pytest.param("EDT", "-04:00", id="edt_ambiguous"),
            pytest.param("CDT", "-05:00", id="cdt_ambiguous"),
            pytest.param("MDT", "-06:00", id="mdt_ambiguous"),
            pytest.param("PDT", "-07:00", id="pdt_ambiguous"),
        ],
    )
    def test_deserialize_datetime_v1(self, tz_input, expected_tz_name):
        """Test deserialization of datetime objects from version 1 format."""
        serialized_data = {
            "__classname__": "pendulum.datetime.DateTime",
            "__version__": 1,
            "__data__": {"timestamp": 1657505443.0, "tz": "UTC"},
        }

        serialized_data["__data__"]["tz"] = tz_input
        deserialized_dt = deserialize(serialized_data)
        assert deserialized_dt.timestamp() == 1657505443.0
        assert deserialized_dt.tzinfo.name == expected_tz_name
        # Assert that it's serializable with the new format for ambiguous timezones
        if tz_input in ["EDT", "CDT", "MDT", "PDT"]:
            assert deserialize(serialize(deserialized_dt)) == deserialized_dt

    @pytest.mark.parametrize(
        ("expr", "expected"),
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

    def test_bignum_deserialize_decimal(self):
        from airflow.serialization.serializers.bignum import deserialize

        res = deserialize(decimal.Decimal, 1, decimal.Decimal(12345))
        assert res == decimal.Decimal(12345)

    @pytest.mark.parametrize(
        ("klass", "version", "payload", "msg"),
        [
            (
                decimal.Decimal,
                999,
                "0",
                r"serialized 999 of decimal\.Decimal > 1",  # newer version
            ),
            (
                str,
                1,
                "0",
                r"do not know how to deserialize builtins\.str",  # wrong classname
            ),
        ],
    )
    def test_bignum_deserialize_errors(self, klass, version, payload, msg):
        from airflow.serialization.serializers.bignum import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, payload)

    @pytest.mark.parametrize(
        "value",
        [
            np.int8(1),
            np.int16(2),
            np.int64(4),
            np.float16(4.5),
            np.float64(123.11241231351),
        ],
    )
    def test_numpy(self, value):
        e = serialize(value)
        assert isinstance(e, dict)
        d = deserialize(e)
        assert value == d
        assert type(value) is type(d)

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
            (np.int32, 999, 123, r"serialized version is newer"),
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
        ("klass", "version", "data", "msg"),
        [
            (pd.DataFrame, 999, "", r"serialized 999 of pandas.core.frame.DataFrame > 1"),  # version too new
            (
                pd.DataFrame,
                1,
                123,
                r"serialized pandas.core.frame.DataFrame has wrong data type .*<class 'int'>",
            ),  # bad payload type
            (str, 1, "", r"do not know how to deserialize builtins.str"),  # bad class
        ],
    )
    def test_pandas_deserialize_errors(self, klass, version, data, msg):
        from airflow.serialization.serializers.pandas import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, data)

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

    def test_pydantic(self):
        m = FooBarModel(banana=3.14, foo="hello")
        e = serialize(m)
        d = deserialize(e)

        assert m.banana == d.banana
        assert m.foo == d.foo

    @pytest.mark.parametrize(
        ("klass", "version", "data", "msg"),
        [
            (
                FooBarModel,
                999,
                FooBarModel(banana=3.14, foo="hello"),
                "Serialized version 999 is newer than the supported version 1",
            ),
            (str, 1, "", r"No deserializer found for builtins\.str"),
        ],
    )
    def test_pydantic_deserialize_errors(self, klass, version, data, msg):
        from airflow.serialization.serializers.pydantic import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, data)

    def test_pydantic_dataclass(self):
        orig = PydanticDataclass(a=5, b="SerDe Pydantic Dataclass Test")
        serialized = serialize(orig)
        assert orig.__version__ == serialized[VERSION]
        assert qualname(orig) == serialized[CLASSNAME]
        assert serialized[DATA]

        decoded = deserialize(serialized)
        assert decoded.a == orig.a
        assert decoded.b == orig.b
        assert type(decoded) is type(orig)

    @pytest.mark.skipif(not PENDULUM3, reason="Test case for pendulum~=3")
    @pytest.mark.parametrize(
        ("ser_value", "expected"),
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
        ("ser_value", "expected"),
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

        zi = deserialize(ZoneInfo, 1, "Asia/Taipei")
        assert isinstance(zi, ZoneInfo)
        assert zi.key == "Asia/Taipei"

    @pytest.mark.parametrize(
        ("klass", "version", "data", "msg"),
        [
            (FixedTimezone, 1, 1.23, "is not of type int or str"),
            (FixedTimezone, 999, "UTC", "serialized 999 .* > 1"),
        ],
    )
    def test_timezone_deserialize_errors(self, klass, version, data, msg):
        from airflow.serialization.serializers.timezone import deserialize

        with pytest.raises(TypeError, match=msg):
            deserialize(klass, version, data)

    @pytest.mark.parametrize(
        ("tz_obj", "expected"),
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

    @pytest.mark.parametrize(
        ("klass", "version", "data"),
        [(tuple, 1, [11, 12]), (set, 1, [11, 12]), (frozenset, 1, [11, 12])],
    )
    def test_builtin_deserialize(self, klass, version, data):
        res = builtin.deserialize(klass, version, klass(data))
        assert isinstance(res, klass)
        assert res == klass(data)

    @pytest.mark.parametrize(
        ("klass", "version", "data", "msg"),
        [
            (tuple, 999, [11, 12], r"serialized version 999 is newer than class version 1"),
            (set, 2, [11, 12], r"serialized version 2 is newer than class version 1"),
            (frozenset, 13, [11, 12], r"serialized version 13 is newer than class version 1"),
        ],
    )
    def test_builtin_deserialize_version_too_new(self, klass, version, data, msg):
        with pytest.raises(TypeError, match=msg):
            builtin.deserialize(klass, version, data)

    @pytest.mark.parametrize(
        ("klass", "version", "data", "msg"),
        [
            (str, 1, "11, 12", r"do not know how to deserialize builtins\.str"),
            (int, 1, 11, r"do not know how to deserialize builtins\.int"),
            (bool, 1, True, r"do not know how to deserialize builtins\.bool"),
            (float, 1, 0.999, r"do not know how to deserialize builtins\.float"),
        ],
    )
    def test_builtin_deserialize_wrong_types(self, klass, version, data, msg):
        with pytest.raises(TypeError, match=msg):
            builtin.deserialize(klass, version, data)

    @pytest.mark.parametrize(
        ("func", "msg"),
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

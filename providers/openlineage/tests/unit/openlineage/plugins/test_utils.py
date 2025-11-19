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
import json
import uuid
from json import JSONEncoder
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from attrs import define
from openlineage.client.utils import RedactMixin
from pkg_resources import parse_version

from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.sdk import timezone
from airflow.providers.openlineage.plugins.facets import AirflowDebugRunFacet
from airflow.providers.openlineage.utils.utils import (
    DagInfo,
    InfoJsonEncodable,
    OpenLineageRedactor,
    _get_all_packages_installed,
    _is_name_redactable,
    get_airflow_debug_facet,
    get_airflow_run_facet,
    get_fully_qualified_class_name,
    get_processing_engine_facet,
    is_operator_disabled,
)
from airflow.serialization.enums import DagAttributeTypes, Encoding
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import (
    BashOperator,
)
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.models.dag import get_next_data_interval

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk._shared.secrets_masker import DEFAULT_SENSITIVE_FIELDS, SecretsMasker
elif AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.secrets_masker import (  # type: ignore[no-redef]
        DEFAULT_SENSITIVE_FIELDS,
        SecretsMasker,
    )
else:
    from airflow.utils.log.secrets_masker import (  # type: ignore[attr-defined,no-redef]
        DEFAULT_SENSITIVE_FIELDS,
        SecretsMasker,
    )

from airflow.providers.common.compat.sdk import DAG

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType


class SafeStrDict(dict):
    def __str__(self):
        castable = []
        for key, val in self.items():
            try:
                str(key), str(val)
                castable.append((key, val))
            except (TypeError, NotImplementedError):
                continue
        return str(dict(castable))


@patch("airflow.providers.openlineage.utils.utils.metadata.distributions")
def test_get_all_packages_installed(mock_distributions):
    mock_distributions.return_value = [MagicMock(metadata={"Name": "package1"}, version="1.0.0")]
    assert _get_all_packages_installed() == {"package1": "1.0.0"}


@patch("airflow.providers.openlineage.utils.utils.conf.debug_mode", return_value=False)
def test_get_airflow_debug_facet_not_in_debug_mode(mock_debug_mode):
    assert get_airflow_debug_facet() == {}


@patch("airflow.providers.openlineage.utils.utils._get_all_packages_installed")
@patch("airflow.providers.openlineage.utils.utils.conf.debug_mode")
def test_get_airflow_debug_facet_logging_set_to_debug(mock_debug_mode, mock_get_packages):
    mock_debug_mode.return_value = True
    mock_get_packages.return_value = {"package1": "1.0.0"}
    result = get_airflow_debug_facet()
    expected_result = {"debug": AirflowDebugRunFacet(packages={"package1": "1.0.0"})}
    assert result == expected_result


@pytest.mark.db_test
@pytest.mark.need_serialized_dag
def test_get_dagrun_start_end(dag_maker):
    start_date = datetime.datetime(2022, 1, 1)
    end_date = datetime.datetime(2022, 1, 1, hour=2)
    with dag_maker("test", start_date=start_date, end_date=end_date, schedule="@once") as dag:
        pass
    dag_maker.sync_dagbag_to_db()

    run_id = str(uuid.uuid1())
    if AIRFLOW_V_3_1_PLUS:
        data_interval = get_next_data_interval(dag.timetable, dag_maker.dag_model)
    else:
        data_interval = dag.get_next_data_interval(dag_maker.dag_model)
    if AIRFLOW_V_3_0_PLUS:
        dagrun_kwargs = {
            "logical_date": data_interval.start,
            "run_after": data_interval.end,
            "triggered_by": DagRunTriggeredByType.TEST,
        }
    else:
        dagrun_kwargs = {"execution_date": data_interval.start}
    dagrun = dag.create_dagrun(
        state=State.NONE,
        run_id=run_id,
        run_type=DagRunType.MANUAL,
        data_interval=data_interval,
        **dagrun_kwargs,
    )
    assert dagrun.data_interval_start is not None
    start_date_tz = datetime.datetime(2022, 1, 1, tzinfo=timezone.utc)
    end_date_tz = datetime.datetime(2022, 1, 1, hour=2, tzinfo=timezone.utc)
    assert dagrun.data_interval_start, dagrun.data_interval_end == (start_date_tz, end_date_tz)


def test_parse_version():
    assert parse_version("2.3.0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.0.dev0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.0.beta1") >= parse_version("2.3.0.dev0")
    assert parse_version("2.3.1") >= parse_version("2.3.0.dev0")
    assert parse_version("2.4.0") >= parse_version("2.3.0.dev0")
    assert parse_version("3.0.0") >= parse_version("2.3.0.dev0")
    assert parse_version("2.2.0") < parse_version("2.3.0.dev0")
    assert parse_version("2.1.3") < parse_version("2.3.0.dev0")
    assert parse_version("2.2.4") < parse_version("2.3.0.dev0")
    assert parse_version("1.10.15") < parse_version("2.3.0.dev0")
    assert parse_version("2.2.4.dev0") < parse_version("2.3.0.dev0")


def test_safe_dict():
    assert str(SafeStrDict({"a": 1})) == str({"a": 1})

    class NotImplemented:
        def __str__(self):
            raise NotImplementedError

    assert str(SafeStrDict({"a": NotImplemented()})) == str({})


def test_info_json_encodable():
    class TestInfo(InfoJsonEncodable):
        excludes = ["exclude_1", "exclude_2", "imastring"]
        casts = {"iwanttobeint": lambda x: int(x.imastring)}
        renames = {"_faulty_name": "goody_name"}

    @define
    class Test:
        exclude_1: str
        imastring: str
        _faulty_name: str
        donotcare: str

    obj = Test("val", "123", "not_funny", "abc")

    assert json.loads(json.dumps(TestInfo(obj))) == {
        "iwanttobeint": 123,
        "goody_name": "not_funny",
        "donotcare": "abc",
    }


def test_info_json_encodable_without_slots():
    class TestInfo(InfoJsonEncodable):
        excludes = ["exclude_1", "exclude_2", "imastring"]
        casts = {"iwanttobeint": lambda x: int(x.imastring)}
        renames = {"_faulty_name": "goody_name"}

    @define(slots=False)
    class Test:
        exclude_1: str
        imastring: str
        _faulty_name: str
        donotcare: str

    obj = Test("val", "123", "not_funny", "abc")

    assert json.loads(json.dumps(TestInfo(obj))) == {
        "iwanttobeint": 123,
        "goody_name": "not_funny",
        "donotcare": "abc",
    }


def test_info_json_encodable_list_does_flatten():
    class TestInfo(InfoJsonEncodable):
        includes = ["alist"]

    @define
    class Test:
        alist: list[str]

    obj = Test(["a", "b", "c"])

    assert json.loads(json.dumps(TestInfo(obj))) == {"alist": "['a', 'b', 'c']"}


def test_info_json_encodable_list_does_include_nonexisting():
    class TestInfo(InfoJsonEncodable):
        includes = ["exists", "doesnotexist"]

    @define
    class Test:
        exists: str

    obj = Test("something")

    assert json.loads(json.dumps(TestInfo(obj))) == {"exists": "something"}


def test_is_name_redactable():
    class NotMixin:
        def __init__(self):
            self.password = "passwd"

    class Mixined(RedactMixin):
        _skip_redact = ["password"]

        def __init__(self):
            self.password = "passwd"
            self.transparent = "123"

    assert _is_name_redactable("password", NotMixin())
    assert not _is_name_redactable("password", Mixined())
    assert _is_name_redactable("transparent", Mixined())


@pytest.mark.enable_redact
def test_redact_with_exclusions(monkeypatch):
    sm = SecretsMasker()
    if AIRFLOW_V_3_1_PLUS:
        sm.sensitive_variables_fields = list(DEFAULT_SENSITIVE_FIELDS)
    redactor = OpenLineageRedactor.from_masker(sm)

    class NotMixin:
        def __init__(self):
            self.password = "passwd"

    class Proxy:
        pass

    def default(self, o):
        if isinstance(o, NotMixin):
            return o.__dict__
        raise TypeError

    assert redactor.redact(NotMixin()).password == "passwd"
    monkeypatch.setattr(JSONEncoder, "default", default)
    assert redactor.redact(NotMixin()).password == "***"

    assert redactor.redact(Proxy()) == "<<non-redactable: Proxy>>"
    assert redactor.redact({"a": "a", "b": Proxy()}) == {"a": "a", "b": "<<non-redactable: Proxy>>"}

    class Mixined(RedactMixin):
        _skip_redact = ["password"]

        def __init__(self):
            self.password = "passwd"
            self.transparent = "123"

    @define
    class NestedMixined(RedactMixin):
        _skip_redact = ["nested_field"]
        password: str
        nested_field: Any

    assert redactor.redact(Mixined()).password == "passwd"
    assert redactor.redact(Mixined()).transparent == "123"
    assert redactor.redact({"password": "passwd"}) == {"password": "***"}
    redacted_nested = redactor.redact(NestedMixined("passwd", NestedMixined("passwd", None)))
    assert redacted_nested == NestedMixined("***", NestedMixined("passwd", None))


def test_get_fully_qualified_class_name():
    from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter

    result = get_fully_qualified_class_name(BashOperator(task_id="test", bash_command="exit 0;"))
    assert result == "airflow.providers.standard.operators.bash.BashOperator"

    result = get_fully_qualified_class_name(OpenLineageAdapter())
    assert result == "airflow.providers.openlineage.plugins.adapter.OpenLineageAdapter"


@patch("airflow.providers.openlineage.conf.disabled_operators")
def test_is_operator_disabled(mock_disabled_operators):
    mock_disabled_operators.return_value = {}
    op = BashOperator(task_id="test", bash_command="exit 0;")
    assert is_operator_disabled(op) is False

    mock_disabled_operators.return_value = {"random_string"}
    assert is_operator_disabled(op) is False

    mock_disabled_operators.return_value = {
        "airflow.providers.standard.operators.bash.BashOperator",
        "airflow.providers.standard.operators.python.PythonOperator",
    }
    assert is_operator_disabled(op) is True


@patch("airflow.providers.openlineage.conf.include_full_task_info")
def test_includes_full_task_info(mock_include_full_task_info):
    mock_include_full_task_info.return_value = True
    # There should be no 'bash_command' in excludes and it's not in includes - so
    # it's a good choice for checking TaskInfo vs TaskInfoComplete
    assert (
        "bash_command"
        in get_airflow_run_facet(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            BashOperator(task_id="bash_op", bash_command="sleep 1"),
            MagicMock(),
        )["airflow"].task
    )


@patch("airflow.providers.openlineage.conf.include_full_task_info")
def test_does_not_include_full_task_info(mock_include_full_task_info):
    mock_include_full_task_info.return_value = False
    # There should be no 'bash_command' in excludes and it's not in includes - so
    # it's a good choice for checking TaskInfo vs TaskInfoComplete
    assert (
        "bash_command"
        not in get_airflow_run_facet(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            BashOperator(task_id="bash_op", bash_command="sleep 1"),
            MagicMock(),
        )["airflow"].task
    )


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This test checks serialization only in 3.0 conditions")
def test_serialize_timetable_complex_with_alias():
    from airflow.providers.common.compat.assets import AssetAlias, AssetAll, AssetAny
    from airflow.timetables.simple import AssetTriggeredTimetable

    asset = AssetAny(
        Asset(name="2", uri="test://2", group="test-group"),
        AssetAlias(name="example-alias", group="test-group"),
        Asset(name="3", uri="test://3", group="test-group"),
        AssetAll(AssetAlias("another"), Asset("4")),
    )
    dag = MagicMock()
    dag.timetable = AssetTriggeredTimetable(asset)
    dag_info = DagInfo(dag)

    assert dag_info.timetable == {
        "asset_condition": {
            "__type": DagAttributeTypes.ASSET_ANY,
            "objects": [
                {
                    "__type": DagAttributeTypes.ASSET,
                    "extra": {},
                    "uri": "test://2/",
                    "name": "2",
                    "group": "test-group",
                },
                {
                    "__type": DagAttributeTypes.ASSET_ALIAS,
                    "name": "example-alias",
                    "group": "test-group",
                },
                {
                    "__type": DagAttributeTypes.ASSET,
                    "extra": {},
                    "uri": "test://3/",
                    "name": "3",
                    "group": "test-group",
                },
                {
                    "__type": DagAttributeTypes.ASSET_ALL,
                    "objects": [
                        {
                            "__type": DagAttributeTypes.ASSET_ALIAS,
                            "name": "another",
                            "group": "asset",
                        },
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "extra": {},
                            "uri": "4",
                            "name": "4",
                            "group": "asset",
                        },
                    ],
                },
            ],
        }
    }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This test checks serialization only in 3.0 conditions")
def test_serialize_timetable_single_asset():
    dag = DAG(dag_id="test", start_date=datetime.datetime(2025, 1, 1), schedule=Asset("a"))
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "asset_condition": {
            "__type": DagAttributeTypes.ASSET,
            "uri": "a",
            "name": "a",
            "group": "asset",
            "extra": {},
        }
    }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This test checks serialization only in 3.0 conditions")
def test_serialize_timetable_list_of_assets():
    dag = DAG(dag_id="test", start_date=datetime.datetime(2025, 1, 1), schedule=[Asset("a"), Asset("b")])
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "asset_condition": {
            "__type": DagAttributeTypes.ASSET_ALL,
            "objects": [
                {"__type": DagAttributeTypes.ASSET, "uri": "a", "name": "a", "group": "asset", "extra": {}},
                {"__type": DagAttributeTypes.ASSET, "uri": "b", "name": "b", "group": "asset", "extra": {}},
            ],
        }
    }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This test checks serialization only in 3.0 conditions")
def test_serialize_timetable_with_complex_logical_condition():
    dag = DAG(
        dag_id="test",
        start_date=datetime.datetime(2025, 1, 1),
        schedule=(Asset("ds1", extra={"some_extra": 1}) | Asset("ds2"))
        & (Asset("ds3") | Asset("ds4", extra={"another_extra": 345})),
    )
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "asset_condition": {
            "__type": DagAttributeTypes.ASSET_ALL,
            "objects": [
                {
                    "__type": DagAttributeTypes.ASSET_ANY,
                    "objects": [
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds1",
                            "extra": {"some_extra": 1},
                            "name": "ds1",
                            "group": "asset",
                        },
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds2",
                            "extra": {},
                            "name": "ds2",
                            "group": "asset",
                        },
                    ],
                },
                {
                    "__type": DagAttributeTypes.ASSET_ANY,
                    "objects": [
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds3",
                            "extra": {},
                            "name": "ds3",
                            "group": "asset",
                        },
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds4",
                            "extra": {"another_extra": 345},
                            "name": "ds4",
                            "group": "asset",
                        },
                    ],
                },
            ],
        }
    }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This test checks serialization only in 3.0 conditions")
def test_serialize_timetable_with_dataset_or_time_schedule():
    from airflow.timetables.assets import AssetOrTimeSchedule
    from airflow.timetables.trigger import CronTriggerTimetable

    dag = DAG(
        dag_id="test",
        start_date=datetime.datetime(2025, 1, 1),
        schedule=AssetOrTimeSchedule(
            timetable=CronTriggerTimetable("0 0 * 3 *", timezone="UTC"),
            assets=(Asset("ds1", extra={"some_extra": 1}) | Asset("ds2"))
            & (Asset("ds3") | Asset("ds4", extra={"another_extra": 345})),
        ),
    )
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "timetable": {
            Encoding.TYPE: "airflow.timetables.trigger.CronTriggerTimetable",
            Encoding.VAR: {
                "expression": "0 0 * 3 *",
                "timezone": "UTC",
                "interval": 0.0,
                "run_immediately": False,
            },
        },
        "asset_condition": {
            "__type": DagAttributeTypes.ASSET_ALL,
            "objects": [
                {
                    "__type": DagAttributeTypes.ASSET_ANY,
                    "objects": [
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds1",
                            "extra": {"some_extra": 1},
                            "name": "ds1",
                            "group": "asset",
                        },
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds2",
                            "extra": {},
                            "name": "ds2",
                            "group": "asset",
                        },
                    ],
                },
                {
                    "__type": DagAttributeTypes.ASSET_ANY,
                    "objects": [
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds3",
                            "extra": {},
                            "name": "ds3",
                            "group": "asset",
                        },
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "uri": "ds4",
                            "extra": {"another_extra": 345},
                            "name": "ds4",
                            "group": "asset",
                        },
                    ],
                },
            ],
        },
    }


@pytest.mark.skipif(
    AIRFLOW_V_3_0_PLUS,
    reason="This test checks serialization only in 2.10 conditions",
)
def test_serialize_timetable_2_10_complex_with_alias():
    from airflow.providers.common.compat.assets import AssetAlias, AssetAll, AssetAny
    from airflow.timetables.simple import DatasetTriggeredTimetable

    asset = AssetAny(
        Asset("2"),
        AssetAlias("example-alias"),
        Asset("3"),
        AssetAll(AssetAlias("this-should-not-be-seen"), Asset("4")),
    )

    dag = MagicMock()
    dag.timetable = DatasetTriggeredTimetable(asset)
    dag_info = DagInfo(dag)

    assert dag_info.timetable == {
        "dataset_condition": {
            "__type": DagAttributeTypes.DATASET_ANY,
            "objects": [
                {"__type": DagAttributeTypes.DATASET, "extra": None, "uri": "2"},
                {"__type": DagAttributeTypes.DATASET_ANY, "objects": []},
                {"__type": DagAttributeTypes.DATASET, "extra": None, "uri": "3"},
                {
                    "__type": DagAttributeTypes.DATASET_ALL,
                    "objects": [
                        {"__type": DagAttributeTypes.DATASET_ANY, "objects": []},
                        {"__type": DagAttributeTypes.DATASET, "extra": None, "uri": "4"},
                    ],
                },
            ],
        }
    }


@pytest.mark.skipif(
    AIRFLOW_V_3_0_PLUS,
    reason="This test checks serialization only in 2.10 conditions",
)
def test_serialize_timetable_2_10_single_asset():
    dag = DAG(dag_id="test", start_date=datetime.datetime(2025, 1, 1), schedule=Asset("a"))
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "dataset_condition": {"__type": DagAttributeTypes.DATASET, "uri": "a", "extra": None}
    }


@pytest.mark.skipif(
    AIRFLOW_V_3_0_PLUS,
    reason="This test checks serialization only in 2.10 conditions",
)
def test_serialize_timetable_2_10_list_of_assets():
    dag = DAG(dag_id="test", start_date=datetime.datetime(2025, 1, 1), schedule=[Asset("a"), Asset("b")])
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "dataset_condition": {
            "__type": DagAttributeTypes.DATASET_ALL,
            "objects": [
                {"__type": DagAttributeTypes.DATASET, "extra": None, "uri": "a"},
                {"__type": DagAttributeTypes.DATASET, "extra": None, "uri": "b"},
            ],
        }
    }


@pytest.mark.skipif(
    AIRFLOW_V_3_0_PLUS,
    reason="This test checks serialization only in 2.10 conditions",
)
def test_serialize_timetable_2_10_with_complex_logical_condition():
    dag = DAG(
        dag_id="test",
        start_date=datetime.datetime(2025, 1, 1),
        schedule=(Asset("ds1", extra={"some_extra": 1}) | Asset("ds2"))
        & (Asset("ds3") | Asset("ds4", extra={"another_extra": 345})),
    )
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "dataset_condition": {
            "__type": DagAttributeTypes.DATASET_ALL,
            "objects": [
                {
                    "__type": DagAttributeTypes.DATASET_ANY,
                    "objects": [
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds1", "extra": {"some_extra": 1}},
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds2", "extra": None},
                    ],
                },
                {
                    "__type": DagAttributeTypes.DATASET_ANY,
                    "objects": [
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds3", "extra": None},
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds4", "extra": {"another_extra": 345}},
                    ],
                },
            ],
        }
    }


@pytest.mark.skipif(
    AIRFLOW_V_3_0_PLUS,
    reason="This test checks serialization only in 2.10 conditions",
)
def test_serialize_timetable_2_10_with_dataset_or_time_schedule():
    from airflow.timetables.datasets import DatasetOrTimeSchedule
    from airflow.timetables.trigger import CronTriggerTimetable

    dag = DAG(
        dag_id="test",
        start_date=datetime.datetime(2025, 1, 1),
        schedule=DatasetOrTimeSchedule(
            timetable=CronTriggerTimetable("0 0 * 3 *", timezone="UTC"),
            datasets=(Asset("ds1", extra={"some_extra": 1}) | Asset("ds2"))
            & (Asset("ds3") | Asset("ds4", extra={"another_extra": 345})),
        ),
    )
    dag_info = DagInfo(dag)
    assert dag_info.timetable == {
        "timetable": {
            "__type": "airflow.timetables.trigger.CronTriggerTimetable",
            "__var": {"expression": "0 0 * 3 *", "timezone": "UTC", "interval": 0.0},
        },
        "dataset_condition": {
            "__type": DagAttributeTypes.DATASET_ALL,
            "objects": [
                {
                    "__type": DagAttributeTypes.DATASET_ANY,
                    "objects": [
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds1", "extra": {"some_extra": 1}},
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds2", "extra": None},
                    ],
                },
                {
                    "__type": DagAttributeTypes.DATASET_ANY,
                    "objects": [
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds3", "extra": None},
                        {"__type": DagAttributeTypes.DATASET, "uri": "ds4", "extra": {"another_extra": 345}},
                    ],
                },
            ],
        },
    }


@pytest.mark.parametrize(
    ("airflow_version", "ol_version"),
    [
        ("2.9.3", "1.12.2"),
        ("2.10.1", "1.13.0"),
        ("3.0.0", "1.14.0"),
    ],
)
def test_get_processing_engine_facet(airflow_version, ol_version):
    with patch("airflow.providers.openlineage.utils.utils.AIRFLOW_VERSION", airflow_version):
        with patch("airflow.providers.openlineage.utils.utils.OPENLINEAGE_PROVIDER_VERSION", ol_version):
            result = get_processing_engine_facet()
            assert result["processing_engine"].version == airflow_version
            assert result["processing_engine"].openlineageAdapterVersion == ol_version

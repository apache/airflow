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

import datetime
import pathlib
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import pendulum
import pytest
from uuid6 import uuid7

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.providers.common.compat.assets import Asset
from airflow.providers.openlineage.plugins.facets import AirflowDagRunFacet, AirflowJobFacet
from airflow.providers.openlineage.utils.utils import (
    _MAX_DOC_BYTES,
    DagInfo,
    DagRunInfo,
    TaskGroupInfo,
    TaskInfo,
    TaskInfoComplete,
    TaskInstanceInfo,
    _get_task_groups_details,
    _get_tasks_details,
    _truncate_string_to_byte_size,
    get_airflow_dag_run_facet,
    get_airflow_job_facet,
    get_dag_documentation,
    get_fully_qualified_class_name,
    get_job_name,
    get_operator_class,
    get_operator_provider_version,
    get_task_documentation,
    get_user_provided_run_facets,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.timetables.events import EventsTimetable
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import BashOperator, PythonOperator
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_3_PLUS, AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import BaseOperator, TaskGroup, task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
    from airflow.models.baseoperator import BaseOperator  # type: ignore[no-redef]
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]

BASH_OPERATOR_PATH = "airflow.providers.standard.operators.bash"
PYTHON_OPERATOR_PATH = "airflow.providers.standard.operators.python"


class CustomOperatorForTest(BashOperator):
    pass


class CustomOperatorFromEmpty(EmptyOperator):
    pass


@pytest.mark.db_test
def test_get_airflow_job_facet():
    with DAG(dag_id="dag", schedule=None, start_date=datetime.datetime(2024, 6, 1)) as dag:
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")

        with TaskGroup("section_1", prefix_group_id=True):
            task_10 = PythonOperator(task_id="task_3", python_callable=lambda: 1)

        task_0 >> task_10

    dagrun_mock = MagicMock(DagRun)
    dagrun_mock.dag = dag

    result = get_airflow_job_facet(dagrun_mock)
    assert result == {
        "airflow": AirflowJobFacet(
            taskTree={},
            taskGroups={
                "section_1": {
                    "parent_group": None,
                    "ui_color": "CornflowerBlue",
                    "ui_fgcolor": "#000",
                    "ui_label": "section_1",
                }
            },
            tasks={
                "task_0": {
                    "operator": f"{BASH_OPERATOR_PATH}.BashOperator",
                    "task_group": None,
                    "emits_ol_events": True,
                    "ui_color": "#f0ede4",
                    "ui_fgcolor": "#000",
                    "ui_label": "task_0",
                    "is_setup": False,
                    "is_teardown": False,
                    "downstream_task_ids": ["section_1.task_3"],
                },
                "section_1.task_3": {
                    "operator": f"{PYTHON_OPERATOR_PATH}.PythonOperator",
                    "task_group": "section_1",
                    "emits_ol_events": True,
                    "ui_color": "#ffefeb",
                    "ui_fgcolor": "#000",
                    "ui_label": "task_3",
                    "is_setup": False,
                    "is_teardown": False,
                    "downstream_task_ids": [],
                },
            },
        )
    }


@pytest.mark.db_test
def test_get_airflow_dag_run_facet():
    with DAG(
        dag_id="dag",
        schedule="@once",
        start_date=datetime.datetime(2024, 6, 1),
        tags=["test"],
    ) as dag:
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")

        with TaskGroup("section_1", prefix_group_id=True):
            task_10 = PythonOperator(task_id="task_3", python_callable=lambda: 1)

        task_0 >> task_10

    dagrun_mock = MagicMock(DagRun)
    dagrun_mock.dag = dag
    dagrun_mock.conf = {}
    dagrun_mock.clear_number = 0
    dagrun_mock.dag_id = dag.dag_id
    dagrun_mock.data_interval_start = datetime.datetime(2024, 6, 1, 1, 2, 3, tzinfo=datetime.timezone.utc)
    dagrun_mock.data_interval_end = datetime.datetime(2024, 6, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.external_trigger = True
    dagrun_mock.run_id = "manual_2024-06-01T00:00:00+00:00"
    dagrun_mock.run_type = DagRunType.MANUAL
    dagrun_mock.logical_date = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.run_after = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.start_date = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.end_date = datetime.datetime(2024, 6, 1, 1, 2, 14, 34172, tzinfo=datetime.timezone.utc)
    dagrun_mock.dag_versions = [
        MagicMock(
            bundle_name="bundle_name",
            bundle_version="bundle_version",
            id="version_id",
            version_number="version_number",
        )
    ]

    result = get_airflow_dag_run_facet(dagrun_mock)

    expected_dag_info = {
        "dag_id": "dag",
        "description": None,
        "fileloc": pathlib.Path(__file__).resolve().as_posix(),
        "owner": "airflow",
        "timetable": {},
        "timetable_summary": "@once",
        "start_date": "2024-06-01T00:00:00+00:00",
        "tags": "['test']",
        "owner_links": {},
    }
    if hasattr(dag, "schedule_interval"):  # Airflow 2 compat.
        expected_dag_info["schedule_interval"] = "@once"
    assert result == {
        "airflowDagRun": AirflowDagRunFacet(
            dag=expected_dag_info,
            dagRun={
                "conf": {},
                "dag_id": "dag",
                "data_interval_start": "2024-06-01T01:02:03+00:00",
                "data_interval_end": "2024-06-01T02:03:04+00:00",
                "external_trigger": True,
                "run_id": "manual_2024-06-01T00:00:00+00:00",
                "run_type": DagRunType.MANUAL,
                "start_date": "2024-06-01T01:02:04+00:00",
                "end_date": "2024-06-01T01:02:14.034172+00:00",
                "duration": 10.034172,
                "logical_date": "2024-06-01T01:02:04+00:00",
                "run_after": "2024-06-01T01:02:04+00:00",
                "dag_bundle_name": "bundle_name",
                "dag_bundle_version": "bundle_version",
                "dag_version_id": "version_id",
                "dag_version_number": "version_number",
            },
        )
    }


@pytest.mark.parametrize(
    ("dag_run_attrs", "expected_duration"),
    (
        ({"start_date": None, "end_date": None}, None),
        ({"start_date": datetime.datetime(2025, 1, 1), "end_date": None}, None),
        ({"start_date": None, "end_date": datetime.datetime(2025, 1, 1)}, None),
        ({"start_date": "2024-06-01T01:02:04+00:00", "end_date": "2024-06-01T01:02:14.034172+00:00"}, None),
        (
            {
                "start_date": datetime.datetime(2025, 1, 1, 6, 1, 1, tzinfo=datetime.timezone.utc),
                "end_date": datetime.datetime(2025, 1, 1, 6, 1, 12, 3456, tzinfo=datetime.timezone.utc),
            },
            11.003456,
        ),
    ),
)
def test_dag_run_duration(dag_run_attrs, expected_duration):
    dag_run = MagicMock(**dag_run_attrs)
    result = DagRunInfo.duration(dag_run)
    assert result == expected_duration


def test_dag_run_version_no_versions():
    dag_run = MagicMock()
    del dag_run.dag_versions
    result = DagRunInfo.dag_version_info(dag_run, "somekey")
    assert result is None


@pytest.mark.parametrize("key", ["bundle_name", "bundle_version", "version_id", "version_number"])
@pytest.mark.db_test
def test_dag_run_version(key):
    dagrun_mock = MagicMock(DagRun)
    dagrun_mock.dag_versions = [
        MagicMock(
            bundle_name="bundle_name",
            bundle_version="bundle_version",
            id="version_id",
            version_number="version_number",
        )
    ]
    result = DagRunInfo.dag_version_info(dagrun_mock, key)
    assert result == key


def test_get_fully_qualified_class_name_serialized_operator():
    op_module_path = BASH_OPERATOR_PATH
    op_name = "BashOperator"

    op = BashOperator(task_id="test", bash_command="echo 1")
    op_path_before_serialization = get_fully_qualified_class_name(op)
    assert op_path_before_serialization == f"{op_module_path}.{op_name}"

    serialized = SerializedBaseOperator.serialize_operator(op)
    deserialized = SerializedBaseOperator.deserialize_operator(serialized)

    op_path_after_deserialization = get_fully_qualified_class_name(deserialized)
    assert op_path_after_deserialization == f"{op_module_path}.{op_name}"
    assert deserialized._task_module == op_module_path
    assert deserialized.task_type == op_name


def test_get_fully_qualified_class_name_mapped_operator():
    mapped = MockOperator.partial(task_id="task_2").expand(arg2=["a", "b", "c"])
    mapped_op_path = get_fully_qualified_class_name(mapped)
    assert mapped_op_path == "tests_common.test_utils.mock_operators.MockOperator"


def test_get_fully_qualified_class_name_bash_operator():
    result = get_fully_qualified_class_name(BashOperator(task_id="test", bash_command="echo 0;"))
    expected_result = f"{BASH_OPERATOR_PATH}.BashOperator"
    assert result == expected_result


def test_truncate_string_to_byte_size_ascii_below_limit():
    s = "A" * (_MAX_DOC_BYTES - 500)
    result = _truncate_string_to_byte_size(s)
    assert result == s
    assert len(result.encode("utf-8")) == _MAX_DOC_BYTES - 500


def test_truncate_string_to_byte_size_ascii_exact_limit():
    s = "A" * _MAX_DOC_BYTES
    result = _truncate_string_to_byte_size(s)
    assert result == s
    assert len(result.encode("utf-8")) == _MAX_DOC_BYTES


def test_truncate_string_to_byte_size_ascii_over_limit():
    s = "A" * (_MAX_DOC_BYTES + 10)
    result = _truncate_string_to_byte_size(s)
    assert len(result.encode("utf-8")) == _MAX_DOC_BYTES
    assert result == s[:_MAX_DOC_BYTES]  # Each ASCII char = 1 byte


def test_truncate_string_to_byte_size_utf8_multibyte_under_limit():
    emoji = "🧠"
    s = emoji * 1000  # Each emoji is 4 bytes, total 4000 bytes
    result = _truncate_string_to_byte_size(s)
    assert result == s
    assert len(result.encode("utf-8")) <= _MAX_DOC_BYTES


def test_truncate_string_to_byte_size_utf8_multibyte_truncation():
    emoji = "🧠"
    full = emoji * (_MAX_DOC_BYTES // 4 + 10)
    result = _truncate_string_to_byte_size(full)
    result_bytes = result.encode("utf-8")
    assert len(result_bytes) <= _MAX_DOC_BYTES
    assert result_bytes.decode("utf-8") == result  # still valid UTF-8
    # Ensure we didn't include partial emoji
    assert result.endswith(emoji)


def test_truncate_string_to_byte_size_split_multibyte_character():
    s = "A" * 10 + "🧠"
    encoded = s.encode("utf-8")
    # Chop in the middle of the emoji (🧠 = 4 bytes)
    partial = encoded[:-2]
    result = _truncate_string_to_byte_size(s, max_size=len(partial))
    assert "🧠" not in result
    assert result == "A" * 10  # emoji should be dropped


def test_truncate_string_to_byte_size_empty_string():
    result = _truncate_string_to_byte_size("")
    assert result == ""


def test_truncate_string_to_byte_size_exact_multibyte_fit():
    emoji = "🚀"
    num = _MAX_DOC_BYTES // len(emoji.encode("utf-8"))
    s = emoji * num
    result = _truncate_string_to_byte_size(s)
    assert result == s
    assert len(result.encode("utf-8")) <= _MAX_DOC_BYTES


def test_truncate_string_to_byte_size_null_characters():
    s = "\x00" * (_MAX_DOC_BYTES + 10)
    result = _truncate_string_to_byte_size(s)
    assert len(result.encode("utf-8")) == _MAX_DOC_BYTES
    assert all(c == "\x00" for c in result)


def test_truncate_string_to_byte_size_non_bmp_characters():
    # Characters like '𝄞' (U+1D11E) are >2 bytes in UTF-8
    s = "𝄞" * 1000
    result = _truncate_string_to_byte_size(s)
    assert len(result.encode("utf-8")) <= _MAX_DOC_BYTES
    assert result.encode("utf-8").decode("utf-8") == result


@pytest.mark.parametrize(
    "operator, expected_doc, expected_mime_type",
    [
        (None, None, None),
        (MagicMock(doc=None, doc_md=None, doc_json=None, doc_yaml=None, doc_rst=None), None, None),
        (MagicMock(doc="Test doc"), "Test doc", "text/plain"),
        (MagicMock(doc_md="test.md", doc=None), "test.md", "text/markdown"),
        (
            MagicMock(doc_json='{"key": "value"}', doc=None, doc_md=None),
            '{"key": "value"}',
            "application/json",
        ),
        (
            MagicMock(doc_yaml="key: value", doc_json=None, doc=None, doc_md=None),
            "key: value",
            "application/x-yaml",
        ),
        (
            MagicMock(doc_rst="Test RST", doc_yaml=None, doc_json=None, doc=None, doc_md=None),
            "Test RST",
            "text/x-rst",
        ),
    ],
)
def test_get_task_documentation(operator, expected_doc, expected_mime_type):
    result_doc, result_mime_type = get_task_documentation(operator)
    assert result_doc == expected_doc
    assert result_mime_type == expected_mime_type


def test_get_task_documentation_serialized_operator():
    op = BashOperator(task_id="test", bash_command="echo 1", doc="some_doc")
    op_doc_before_serialization = get_task_documentation(op)
    assert op_doc_before_serialization == ("some_doc", "text/plain")

    serialized = SerializedBaseOperator.serialize_operator(op)
    deserialized = SerializedBaseOperator.deserialize_operator(serialized)

    op_doc_after_deserialization = get_task_documentation(deserialized)
    assert op_doc_after_deserialization == ("some_doc", "text/plain")


def test_get_task_documentation_mapped_operator():
    mapped = MockOperator.partial(task_id="task_2", doc_md="some_doc").expand(arg2=["a", "b", "c"])
    mapped_op_doc = get_task_documentation(mapped)
    assert mapped_op_doc == ("some_doc", "text/markdown")


def test_get_task_documentation_longer_than_allowed():
    doc = "A" * (_MAX_DOC_BYTES + 10)
    operator = MagicMock(doc=doc)
    result_doc, result_mime_type = get_task_documentation(operator)
    assert result_doc == "A" * _MAX_DOC_BYTES
    assert result_mime_type == "text/plain"


@pytest.mark.parametrize(
    "dag, expected_doc, expected_mime_type",
    [
        (None, None, None),
        (MagicMock(doc_md=None, description=None), None, None),
        (MagicMock(doc_md="test.md", description=None), "test.md", "text/markdown"),
        (MagicMock(doc_md="test.md", description="Description text"), "test.md", "text/markdown"),
        (MagicMock(description="Description text", doc_md=None), "Description text", "text/plain"),
    ],
)
def test_get_dag_documentation(dag, expected_doc, expected_mime_type):
    result_doc, result_mime_type = get_dag_documentation(dag)
    assert result_doc == expected_doc
    assert result_mime_type == expected_mime_type


def test_get_dag_documentation_longer_than_allowed():
    doc = "A" * (_MAX_DOC_BYTES + 10)
    dag = MagicMock(doc_md=doc, description=None)
    result_doc, result_mime_type = get_dag_documentation(dag)
    assert result_doc == "A" * _MAX_DOC_BYTES
    assert result_mime_type == "text/markdown"


def test_get_job_name():
    task_instance = MagicMock(dag_id="example_dag", task_id="example_task")
    expected_result = "example_dag.example_task"
    assert get_job_name(task_instance) == expected_result


def test_get_job_name_empty_ids():
    task_instance = MagicMock(dag_id="", task_id="")
    expected_result = "."
    assert get_job_name(task_instance) == expected_result


def test_get_operator_class():
    op_class = get_operator_class(BashOperator(task_id="test", bash_command="echo 0;"))
    assert op_class == BashOperator


def test_get_operator_class_mapped_operator():
    mapped = MockOperator.partial(task_id="task").expand(arg2=["a", "b", "c"])
    op_class = get_operator_class(mapped)
    assert op_class == MockOperator


def test_get_tasks_details():
    class TestMappedOperator(BaseOperator):
        def __init__(self, value, **kwargs):
            super().__init__(**kwargs)
            self.value = value

        def execute(self, context):
            return self.value + 1

    @task
    def generate_list() -> list:
        return [1, 2, 3]

    @task
    def process_item(item: int) -> int:
        return item * 2

    @task
    def sum_values(values: list[int]) -> int:
        return sum(values)

    with DAG(dag_id="dag", schedule=None, start_date=datetime.datetime(2024, 6, 1)) as dag:
        task_ = CustomOperatorForTest(task_id="task", bash_command="exit 0;")
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")
        task_1 = CustomOperatorFromEmpty(task_id="task_1")
        task_2 = PythonOperator(task_id="task_2", python_callable=lambda: 1)
        task_3 = BashOperator(task_id="task_3", bash_command="exit 0;")
        task_4 = EmptyOperator(task_id="task_4.test.dot")
        task_5 = BashOperator(task_id="task_5", bash_command="exit 0;")
        task_6 = TestMappedOperator.partial(task_id="task_6").expand(value=[1, 2])

        list_result = generate_list()
        processed_results = process_item.expand(item=list_result)
        result_sum = sum_values(processed_results)  # noqa: F841

        with TaskGroup("section_1", prefix_group_id=True) as tg:
            task_10 = PythonOperator(task_id="task_3", python_callable=lambda: 1)
            with TaskGroup("section_2", parent_group=tg) as tg2:
                task_11 = EmptyOperator(task_id="task_11")  # noqa: F841
                with TaskGroup("section_3", parent_group=tg2):
                    task_12 = PythonOperator(task_id="task_12", python_callable=lambda: 1)

        task_ >> [task_2, task_6]
        task_0 >> [task_2, task_1] >> task_3 >> [task_4, task_5]
        task_1 >> task_6 >> task_3 >> task_4 >> task_5
        task_3 >> task_10 >> task_12

    py_decorator_path = (
        "airflow.providers.standard.decorators.python._PythonDecoratedOperator"
        if AIRFLOW_V_3_0_PLUS
        else "airflow.decorators.python._PythonDecoratedOperator"
    )

    expected = {
        "generate_list": {
            "emits_ol_events": True,
            "is_setup": False,
            "is_teardown": False,
            "operator": py_decorator_path,
            "task_group": None,
            "ui_color": "#ffefeb",
            "ui_fgcolor": "#000",
            "ui_label": "generate_list",
            "downstream_task_ids": [
                "process_item",
            ],
        },
        "process_item": {
            "emits_ol_events": True,
            "is_setup": False,
            "is_teardown": False,
            "operator": py_decorator_path,
            "task_group": None,
            "ui_color": "#ffefeb",
            "ui_fgcolor": "#000",
            "ui_label": "process_item",
            "downstream_task_ids": [
                "sum_values",
            ],
        },
        "sum_values": {
            "emits_ol_events": True,
            "is_setup": False,
            "is_teardown": False,
            "operator": py_decorator_path,
            "task_group": None,
            "ui_color": "#ffefeb",
            "ui_fgcolor": "#000",
            "ui_label": "sum_values",
            "downstream_task_ids": [],
        },
        "task": {
            "operator": "unit.openlineage.utils.test_utils.CustomOperatorForTest",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": CustomOperatorForTest.ui_color,
            "ui_fgcolor": CustomOperatorForTest.ui_fgcolor,
            "ui_label": "task",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "task_2",
                "task_6",
            ],
        },
        "task_0": {
            "operator": f"{BASH_OPERATOR_PATH}.BashOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": BashOperator.ui_color,
            "ui_fgcolor": BashOperator.ui_fgcolor,
            "ui_label": "task_0",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "task_1",
                "task_2",
            ],
        },
        "task_1": {
            "operator": "unit.openlineage.utils.test_utils.CustomOperatorFromEmpty",
            "task_group": None,
            "emits_ol_events": False,
            "ui_color": CustomOperatorFromEmpty.ui_color,
            "ui_fgcolor": CustomOperatorFromEmpty.ui_fgcolor,
            "ui_label": "task_1",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "task_3",
                "task_6",
            ],
        },
        "task_2": {
            "operator": f"{PYTHON_OPERATOR_PATH}.PythonOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": PythonOperator.ui_color,
            "ui_fgcolor": PythonOperator.ui_fgcolor,
            "ui_label": "task_2",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "task_3",
            ],
        },
        "task_3": {
            "operator": f"{BASH_OPERATOR_PATH}.BashOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": BashOperator.ui_color,
            "ui_fgcolor": BashOperator.ui_fgcolor,
            "ui_label": "task_3",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "section_1.task_3",
                "task_4.test.dot",
                "task_5",
            ],
        },
        "task_4.test.dot": {
            "operator": "airflow.providers.standard.operators.empty.EmptyOperator",
            "task_group": None,
            "emits_ol_events": False,
            "ui_color": EmptyOperator.ui_color,
            "ui_fgcolor": EmptyOperator.ui_fgcolor,
            "ui_label": "task_4.test.dot",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "task_5",
            ],
        },
        "task_5": {
            "operator": f"{BASH_OPERATOR_PATH}.BashOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": BashOperator.ui_color,
            "ui_fgcolor": BashOperator.ui_fgcolor,
            "ui_label": "task_5",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [],
        },
        "task_6": {
            "emits_ol_events": True,
            "is_setup": False,
            "is_teardown": False,
            "operator": "unit.openlineage.utils.test_utils.TestMappedOperator",
            "task_group": None,
            "ui_color": "#fff",
            "ui_fgcolor": "#000",
            "ui_label": "task_6",
            "downstream_task_ids": [
                "task_3",
            ],
        },
        "section_1.task_3": {
            "operator": f"{PYTHON_OPERATOR_PATH}.PythonOperator",
            "task_group": "section_1",
            "emits_ol_events": True,
            "ui_color": PythonOperator.ui_color,
            "ui_fgcolor": PythonOperator.ui_fgcolor,
            "ui_label": "task_3",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [
                "section_1.section_2.section_3.task_12",
            ],
        },
        "section_1.section_2.task_11": {
            "operator": "airflow.providers.standard.operators.empty.EmptyOperator",
            "task_group": "section_1.section_2",
            "emits_ol_events": False,
            "ui_color": EmptyOperator.ui_color,
            "ui_fgcolor": EmptyOperator.ui_fgcolor,
            "ui_label": "task_11",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [],
        },
        "section_1.section_2.section_3.task_12": {
            "operator": f"{PYTHON_OPERATOR_PATH}.PythonOperator",
            "task_group": "section_1.section_2.section_3",
            "emits_ol_events": True,
            "ui_color": PythonOperator.ui_color,
            "ui_fgcolor": PythonOperator.ui_fgcolor,
            "ui_label": "task_12",
            "is_setup": False,
            "is_teardown": False,
            "downstream_task_ids": [],
        },
    }

    result = _get_tasks_details(dag)
    assert result == expected


def test_get_tasks_details_empty_dag():
    assert _get_tasks_details(DAG("test_dag", schedule=None, start_date=datetime.datetime(2024, 6, 1))) == {}


def test_get_tasks_large_dag():
    """Test how get_tasks behaves for a large dag with many dependent tasks."""
    with DAG("test", schedule=None) as dag:
        start = EmptyOperator(task_id="start")

        a = [
            start >> EmptyOperator(task_id=f"a_1_{i}") >> EmptyOperator(task_id=f"a_2_{i}")
            for i in range(200)
        ]

        middle = EmptyOperator(task_id="middle")

        b = [
            middle >> EmptyOperator(task_id=f"b_1_{i}") >> EmptyOperator(task_id=f"b_2_{i}")
            for i in range(200)
        ]

        middle2 = EmptyOperator(task_id="middle2")

        c = [
            middle2 >> EmptyOperator(task_id=f"c_1_{i}") >> EmptyOperator(task_id=f"c_2_{i}")
            for i in range(200)
        ]

        end = EmptyOperator(task_id="end")

        start >> a >> middle >> b >> middle2 >> c >> end

    result = _get_tasks_details(dag)

    expected_dependencies = {
        "start": 400,
        "middle": 400,
        "middle2": 400,
        "end": 0,
    }

    assert len(result) == 1204
    for task_id, task_info in result.items():
        assert len(task_info["downstream_task_ids"]) == expected_dependencies.get(task_id, 1)


def test_get_task_groups_details():
    with DAG("test_dag", schedule=None, start_date=datetime.datetime(2024, 6, 1)) as dag:
        with TaskGroup("tg1", prefix_group_id=True):
            task_1 = EmptyOperator(task_id="task_1")  # noqa: F841
        with TaskGroup("tg2", prefix_group_id=False):
            task = EmptyOperator(task_id="task_1")  # noqa: F841
        with TaskGroup("tg3"):
            task_2 = EmptyOperator(task_id="task_2")  # noqa: F841

    result = _get_task_groups_details(dag)
    expected = {
        "tg1": {
            "parent_group": None,
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg1",
        },
        "tg2": {
            "parent_group": None,
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg2",
        },
        "tg3": {
            "parent_group": None,
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg3",
        },
    }

    assert result == expected


def test_get_task_groups_details_nested():
    with DAG("test_dag", schedule=None, start_date=datetime.datetime(2024, 6, 1)) as dag:
        with TaskGroup("tg1", prefix_group_id=True) as tg:
            with TaskGroup("tg2", parent_group=tg) as tg2:
                with TaskGroup("tg3", parent_group=tg2):
                    pass

    result = _get_task_groups_details(dag)
    expected = {
        "tg1": {
            "parent_group": None,
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg1",
        },
        "tg1.tg2": {
            "parent_group": "tg1",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg2",
        },
        "tg1.tg2.tg3": {
            "parent_group": "tg1.tg2",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg3",
        },
    }

    assert result == expected


def test_get_task_groups_details_no_task_groups():
    assert (
        _get_task_groups_details(
            DAG("test_dag", schedule=None, start_date=datetime.datetime(2024, 6, 1)),
        )
        == {}
    )


@patch("airflow.providers.openlineage.conf.custom_run_facets", return_value=set())
def test_get_user_provided_run_facets_with_no_function_definition(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"unit.openlineage.utils.custom_facet_fixture.get_additional_test_facet"},
)
def test_get_user_provided_run_facets_with_function_definition(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert len(result) == 1
    assert result["additional_run_facet"].name == f"test-lineage-namespace-{TaskInstanceState.RUNNING}"
    assert result["additional_run_facet"].cluster == "TEST_test-dag.test-task"


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={
        "unit.openlineage.utils.custom_facet_fixture.get_additional_test_facet",
    },
)
def test_get_user_provided_run_facets_with_return_value_as_none(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=BashOperator(
                task_id="test-task",
                bash_command="exit 0;",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=BashOperator(
                task_id="test-task",
                bash_command="exit 0;",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={
        "invalid_function",
        "unit.openlineage.utils.custom_facet_fixture.get_additional_test_facet",
        "unit.openlineage.utils.custom_facet_fixture.return_type_is_not_dict",
        "unit.openlineage.utils.custom_facet_fixture.get_another_test_facet",
    },
)
def test_get_user_provided_run_facets_with_multiple_function_definition(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert len(result) == 2
    assert result["additional_run_facet"].name == f"test-lineage-namespace-{TaskInstanceState.RUNNING}"
    assert result["additional_run_facet"].cluster == "TEST_test-dag.test-task"
    assert result["another_run_facet"] == {"name": "another-lineage-namespace"}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={
        "unit.openlineage.utils.custom_facet_fixture.get_additional_test_facet",
        "unit.openlineage.utils.custom_facet_fixture.get_duplicate_test_facet_key",
    },
)
def test_get_user_provided_run_facets_with_duplicate_facet_keys(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert len(result) == 1
    assert result["additional_run_facet"].name == f"test-lineage-namespace-{TaskInstanceState.RUNNING}"
    assert result["additional_run_facet"].cluster == "TEST_test-dag.test-task"


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"invalid_function"},
)
def test_get_user_provided_run_facets_with_invalid_function_definition(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"providers.unit.openlineage.utils.custom_facet_fixture.return_type_is_not_dict"},
)
def test_get_user_provided_run_facets_with_wrong_return_type_function(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"providers.unit.openlineage.utils.custom_facet_fixture.get_custom_facet_throws_exception"},
)
def test_get_user_provided_run_facets_with_exception(mock_custom_facet_funcs):
    if AIRFLOW_V_3_0_PLUS:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
            dag_version_id=mock.MagicMock(),
        )
    else:
        sample_ti = TaskInstance(
            task=EmptyOperator(
                task_id="test-task",
                dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1)),
            ),
            state="running",
        )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


def test_daginfo_timetable_summary():
    from airflow.timetables.simple import NullTimetable

    dag = MagicMock()
    # timetable is enough to get summary
    dag.timetable = NullTimetable()
    dag.timetable_summary = None
    assert DagInfo(dag).timetable_summary == "None"

    # but if summary is present, it's preferred
    dag.timetable_summary = "explicit_summary"
    assert DagInfo(dag).timetable_summary == "explicit_summary"


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 tests")
class TestDagInfoAirflow2:
    def test_dag_info(self):
        with DAG(
            dag_id="dag_id",
            schedule="@once",
            start_date=datetime.datetime(2024, 6, 1),
            tags=["test"],
            description="test desc",
            owner_links={"some_owner": "https://airflow.apache.org"},
        ) as dag:
            task_0 = BashOperator(task_id="task_0", bash_command="exit 0;", owner="first")  # noqa: F841
            task_1 = BashOperator(task_id="task_1", bash_command="exit 1;", owner="second")  # noqa: F841

        result = dict(DagInfo(dag))
        assert sorted(result["owner"].split(", ")) == ["first", "second"]
        result.pop("owner")
        assert result == {
            "dag_id": "dag_id",
            "description": "test desc",
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "schedule_interval": "@once",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "['test']",
            "timetable": {},
            "timetable_summary": "@once",
            "owner_links": {"some_owner": "https://airflow.apache.org"},
        }

    def test_dag_info_schedule_cron(self):
        dag = DAG(
            dag_id="dag_id",
            schedule="*/4 3 * * *",
            start_date=datetime.datetime(2024, 6, 1),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "schedule_interval": "*/4 3 * * *",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "timetable": {"expression": "*/4 3 * * *", "timezone": "UTC"},
            "timetable_summary": "*/4 3 * * *",
            "owner_links": {},
        }

    def test_dag_info_schedule_events_timetable(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=EventsTimetable(
                event_dates=[
                    pendulum.datetime(2025, 3, 3, 8, 27, tz="America/Chicago"),
                    pendulum.datetime(2025, 3, 17, 8, 27, tz="America/Chicago"),
                    pendulum.datetime(2025, 3, 22, 20, 50, tz="America/Chicago"),
                ],
                description="My Team's Baseball Games",
            ),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "schedule_interval": "My Team's Baseball Games",
            "timetable_summary": "My Team's Baseball Games",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "event_dates": [
                    "2025-03-03 08:27:00-06:00",
                    "2025-03-17 08:27:00-05:00",
                    "2025-03-22 20:50:00-05:00",
                ],
                "restrict_to_events": False,
            },
        }

    def test_dag_info_schedule_list_single_dataset(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=[Asset(uri="uri1", extra={"a": 1})],
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "schedule_interval": "Dataset",
            "timetable_summary": "Dataset",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "dataset_condition": {
                    "__type": "dataset_all",
                    "objects": [{"__type": "dataset", "uri": "uri1", "extra": {"a": 1}}],
                }
            },
        }

    def test_dag_info_schedule_list_two_datasets(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=[Asset(uri="uri1", extra={"a": 1}), Asset(uri="uri2")],
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "schedule_interval": "Dataset",
            "timetable_summary": "Dataset",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "dataset_condition": {
                    "__type": "dataset_all",
                    "objects": [
                        {"__type": "dataset", "uri": "uri1", "extra": {"a": 1}},
                        {"__type": "dataset", "uri": "uri2", "extra": None},
                    ],
                }
            },
        }

    def test_dag_info_schedule_datasets_logical_condition(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=((Asset("uri1", extra={"a": 1}) | Asset("uri2")) & (Asset("uri3") | Asset("uri4"))),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "schedule_interval": "Dataset",
            "timetable_summary": "Dataset",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "dataset_condition": {
                    "__type": "dataset_all",
                    "objects": [
                        {
                            "__type": "dataset_any",
                            "objects": [
                                {"__type": "dataset", "uri": "uri1", "extra": {"a": 1}},
                                {"__type": "dataset", "uri": "uri2", "extra": None},
                            ],
                        },
                        {
                            "__type": "dataset_any",
                            "objects": [
                                {"__type": "dataset", "uri": "uri3", "extra": None},
                                {"__type": "dataset", "uri": "uri4", "extra": None},
                            ],
                        },
                    ],
                }
            },
        }

    def test_dag_info_schedule_dataset_or_time_schedule(self):
        # Airflow 2 import, this test is only run on Airflow 2
        from airflow.timetables.datasets import DatasetOrTimeSchedule

        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=DatasetOrTimeSchedule(
                timetable=CronTriggerTimetable("*/4 3 * * *", timezone="UTC"),
                datasets=((Asset("uri1", extra={"a": 1}) | Asset("uri2")) & (Asset("uri3") | Asset("uri4"))),
            ),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "schedule_interval": "Dataset or */4 3 * * *",
            "timetable_summary": "Dataset or */4 3 * * *",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "dataset_condition": {
                    "__type": "dataset_all",
                    "objects": [
                        {
                            "__type": "dataset_any",
                            "objects": [
                                {"__type": "dataset", "uri": "uri1", "extra": {"a": 1}},
                                {"__type": "dataset", "uri": "uri2", "extra": None},
                            ],
                        },
                        {
                            "__type": "dataset_any",
                            "objects": [
                                {"__type": "dataset", "uri": "uri3", "extra": None},
                                {"__type": "dataset", "uri": "uri4", "extra": None},
                            ],
                        },
                    ],
                },
                "timetable": {
                    "__type": "airflow.timetables.trigger.CronTriggerTimetable",
                    "__var": {"expression": "*/4 3 * * *", "timezone": "UTC", "interval": 0.0},
                },
            },
        }


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow < 3.0 tests")
class TestDagInfoAirflow210:
    def test_dag_info_schedule_single_dataset_directly(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=Asset(uri="uri1", extra={"a": 1}),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {"dataset_condition": {"__type": "dataset", "uri": "uri1", "extra": {"a": 1}}},
            "schedule_interval": "Dataset",
            "timetable_summary": "Dataset",
        }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 tests")
class TestDagInfoAirflow3:
    def test_dag_info(self):
        with DAG(
            dag_id="dag_id",
            schedule="@once",
            start_date=datetime.datetime(2024, 6, 1),
            tags={"test"},
            description="test desc",
            owner_links={"some_owner": "https://airflow.apache.org"},
        ) as dag:
            task_0 = BashOperator(task_id="task_0", bash_command="exit 0;", owner="first")  # noqa: F841
            task_1 = BashOperator(task_id="task_1", bash_command="exit 1;", owner="second")  # noqa: F841

        result = dict(DagInfo(dag))
        assert sorted(result["owner"].split(", ")) == ["first", "second"]
        result.pop("owner")
        assert result == {
            "dag_id": "dag_id",
            "description": "test desc",
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "['test']",
            "timetable": {},
            "timetable_summary": "@once",
            "owner_links": {"some_owner": "https://airflow.apache.org"},
        }

    def test_dag_info_schedule_cron(self):
        dag = DAG(
            dag_id="dag_id",
            schedule="*/4 3 * * *",
            start_date=datetime.datetime(2024, 6, 1),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {"expression": "*/4 3 * * *", "timezone": "UTC"},
            "timetable_summary": "*/4 3 * * *",
        }

    def test_dag_info_schedule_events_timetable(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=EventsTimetable(
                event_dates=[
                    pendulum.datetime(2025, 3, 3, 8, 27, tz="America/Chicago"),
                    pendulum.datetime(2025, 3, 17, 8, 27, tz="America/Chicago"),
                    pendulum.datetime(2025, 3, 22, 20, 50, tz="America/Chicago"),
                ],
                description="My Team's Baseball Games",
            ),
        )

        timetable = {
            "event_dates": [
                "2025-03-03T08:27:00-06:00",
                "2025-03-17T08:27:00-05:00",
                "2025-03-22T20:50:00-05:00",
            ],
            "restrict_to_events": False,
        }
        if AIRFLOW_V_3_0_3_PLUS:
            timetable.update(
                {
                    "_summary": "My Team's Baseball Games",
                    "description": "My Team's Baseball Games",
                }
            )
            timetable["description"] = "My Team's Baseball Games"
        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": timetable,
            "timetable_summary": "My Team's Baseball Games",
        }

    def test_dag_info_schedule_single_asset_directly(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=Asset(uri="uri1", extra={"a": 1}),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "asset_condition": {
                    "__type": "asset",
                    "uri": "uri1",
                    "name": "uri1",
                    "group": "asset",
                    "extra": {"a": 1},
                }
            },
            "timetable_summary": "Asset",
        }

    def test_dag_info_schedule_list_single_assets(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=[Asset(uri="uri1", extra={"a": 1})],
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "asset_condition": {
                    "__type": "asset_all",
                    "objects": [
                        {
                            "__type": "asset",
                            "uri": "uri1",
                            "name": "uri1",
                            "group": "asset",
                            "extra": {"a": 1},
                        }
                    ],
                }
            },
            "timetable_summary": "Asset",
        }

    def test_dag_info_schedule_list_two_assets(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=[Asset(uri="uri1", extra={"a": 1}), Asset(uri="uri2")],
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "asset_condition": {
                    "__type": "asset_all",
                    "objects": [
                        {
                            "__type": "asset",
                            "uri": "uri1",
                            "name": "uri1",
                            "group": "asset",
                            "extra": {"a": 1},
                        },
                        {"__type": "asset", "uri": "uri2", "name": "uri2", "group": "asset", "extra": {}},
                    ],
                }
            },
            "timetable_summary": "Asset",
        }

    def test_dag_info_schedule_assets_logical_condition(self):
        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=((Asset("uri1", extra={"a": 1}) | Asset("uri2")) & (Asset("uri3") | Asset("uri4"))),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "asset_condition": {
                    "__type": "asset_all",
                    "objects": [
                        {
                            "__type": "asset_any",
                            "objects": [
                                {
                                    "__type": "asset",
                                    "uri": "uri1",
                                    "name": "uri1",
                                    "group": "asset",
                                    "extra": {"a": 1},
                                },
                                {
                                    "__type": "asset",
                                    "uri": "uri2",
                                    "name": "uri2",
                                    "group": "asset",
                                    "extra": {},
                                },
                            ],
                        },
                        {
                            "__type": "asset_any",
                            "objects": [
                                {
                                    "__type": "asset",
                                    "uri": "uri3",
                                    "name": "uri3",
                                    "group": "asset",
                                    "extra": {},
                                },
                                {
                                    "__type": "asset",
                                    "uri": "uri4",
                                    "name": "uri4",
                                    "group": "asset",
                                    "extra": {},
                                },
                            ],
                        },
                    ],
                }
            },
            "timetable_summary": "Asset",
        }

    def test_dag_info_schedule_asset_or_time_schedule(self):
        from airflow.timetables.assets import AssetOrTimeSchedule

        dag = DAG(
            dag_id="dag_id",
            start_date=datetime.datetime(2024, 6, 1),
            schedule=AssetOrTimeSchedule(
                timetable=CronTriggerTimetable("*/4 3 * * *", timezone="UTC"),
                assets=((Asset("uri1", extra={"a": 1}) | Asset("uri2")) & (Asset("uri3") | Asset("uri4"))),
            ),
        )

        result = DagInfo(dag)
        assert dict(result) == {
            "dag_id": "dag_id",
            "description": None,
            "fileloc": pathlib.Path(__file__).resolve().as_posix(),
            "owner": "",
            "start_date": "2024-06-01T00:00:00+00:00",
            "tags": "[]",
            "owner_links": {},
            "timetable": {
                "asset_condition": {
                    "__type": "asset_all",
                    "objects": [
                        {
                            "__type": "asset_any",
                            "objects": [
                                {
                                    "__type": "asset",
                                    "uri": "uri1",
                                    "name": "uri1",
                                    "group": "asset",
                                    "extra": {"a": 1},
                                },
                                {
                                    "__type": "asset",
                                    "uri": "uri2",
                                    "name": "uri2",
                                    "group": "asset",
                                    "extra": {},
                                },
                            ],
                        },
                        {
                            "__type": "asset_any",
                            "objects": [
                                {
                                    "__type": "asset",
                                    "uri": "uri3",
                                    "name": "uri3",
                                    "group": "asset",
                                    "extra": {},
                                },
                                {
                                    "__type": "asset",
                                    "uri": "uri4",
                                    "name": "uri4",
                                    "group": "asset",
                                    "extra": {},
                                },
                            ],
                        },
                    ],
                },
                "timetable": {
                    "__type": "airflow.timetables.trigger.CronTriggerTimetable",
                    "__var": {
                        "expression": "*/4 3 * * *",
                        "timezone": "UTC",
                        "interval": 0.0,
                        "run_immediately": False,
                    },
                },
            },
            "timetable_summary": "Asset or */4 3 * * *",
        }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 test")
@patch.object(DagRun, "dag_versions", new_callable=PropertyMock)
def test_dagrun_info_af3(mocked_dag_versions):
    from airflow.models.dag_version import DagVersion
    from airflow.utils.types import DagRunTriggeredByType

    date = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
    dv1 = DagVersion()
    dv2 = DagVersion()
    dv2.id = "version_id"
    dv2.version_number = "version_number"
    dv2.bundle_name = "bundle_name"
    dv2.bundle_version = "bundle_version"

    mocked_dag_versions.return_value = [dv1, dv2]
    dagrun = DagRun(
        dag_id="dag_id",
        run_id="dag_run__run_id",
        queued_at=date,
        logical_date=date,
        run_after=date,
        start_date=date,
        conf={"a": 1},
        state=DagRunState.RUNNING,
        run_type=DagRunType.MANUAL,
        creating_job_id=123,
        data_interval=(date, date),
        triggered_by=DagRunTriggeredByType.UI,
        backfill_id=999,
        bundle_version="bundle_version",
    )
    assert dagrun.dag_versions == [dv1, dv2]
    dagrun.end_date = date + datetime.timedelta(seconds=74, microseconds=546)

    result = DagRunInfo(dagrun)
    assert dict(result) == {
        "conf": {"a": 1},
        "dag_id": "dag_id",
        "data_interval_end": "2024-06-01T00:00:00+00:00",
        "data_interval_start": "2024-06-01T00:00:00+00:00",
        "duration": 74.000546,
        "end_date": "2024-06-01T00:01:14.000546+00:00",
        "run_id": "dag_run__run_id",
        "run_type": DagRunType.MANUAL,
        "start_date": "2024-06-01T00:00:00+00:00",
        "logical_date": "2024-06-01T00:00:00+00:00",
        "run_after": "2024-06-01T00:00:00+00:00",
        "dag_bundle_name": "bundle_name",
        "dag_bundle_version": "bundle_version",
        "dag_version_id": "version_id",
        "dag_version_number": "version_number",
    }


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 test")
def test_dagrun_info_af2():
    date = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
    dag = DAG(
        "dag_id",
        schedule=None,
        start_date=date,
    )

    dagrun = dag.create_dagrun(
        run_id="dag_run__run_id",
        data_interval=(date, date),
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        execution_date=date,
        conf={"a": 1},
    )
    dagrun.start_date = date
    dagrun.end_date = date + datetime.timedelta(seconds=74, microseconds=546)

    result = DagRunInfo(dagrun)
    assert dict(result) == {
        "conf": {"a": 1},
        "dag_id": "dag_id",
        "data_interval_end": "2024-06-01T00:00:00+00:00",
        "data_interval_start": "2024-06-01T00:00:00+00:00",
        "duration": 74.000546,
        "end_date": "2024-06-01T00:01:14.000546+00:00",
        "run_id": "dag_run__run_id",
        "run_type": DagRunType.MANUAL,
        "external_trigger": False,
        "start_date": "2024-06-01T00:00:00+00:00",
        "logical_date": "2024-06-01T00:00:00+00:00",
        "dag_bundle_name": None,
        "dag_bundle_version": None,
        "dag_version_id": None,
        "dag_version_number": None,
    }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 test")
def test_taskinstance_info_af3():
    from airflow.sdk.api.datamodels._generated import TaskInstance
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

    task = BaseOperator(task_id="hello")
    task._is_mapped = True
    dag_id = "basic_task"

    dag = DAG(dag_id=dag_id, start_date=timezone.datetime(2024, 12, 3))
    task.dag = dag

    ti_id = uuid7()
    ti = TaskInstance(
        id=ti_id,
        task_id=task.task_id,
        dag_id=dag_id,
        run_id="test_run",
        try_number=1,
        map_index=2,
        dag_version_id=ti_id,
    )
    start_date = timezone.datetime(2025, 1, 1)

    runtime_ti = RuntimeTaskInstance.model_construct(
        **ti.model_dump(exclude_unset=True),
        task=task,
        _ti_context_from_server=None,
        start_date=start_date,
    )
    runtime_ti.end_date = start_date + datetime.timedelta(seconds=12, milliseconds=345)
    bundle_instance = MagicMock(version="bundle_version")
    bundle_instance.name = "bundle_name"
    runtime_ti.bundle_instance = bundle_instance

    assert dict(TaskInstanceInfo(runtime_ti)) == {
        "log_url": runtime_ti.log_url,
        "map_index": 2,
        "try_number": 1,
        "dag_bundle_version": "bundle_version",
        "dag_bundle_name": "bundle_name",
    }


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 test")
@patch.object(TaskInstance, "log_url", "some_log_url")  # Depends on the host, hard to test exact value
def test_taskinstance_info_af2():
    some_date = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
    task_obj = PythonOperator(task_id="task_id", python_callable=lambda x: x)
    ti = TaskInstance(
        task=task_obj, run_id="task_instance_run_id", state=TaskInstanceState.RUNNING, map_index=2
    )
    ti.duration = 12.345
    ti.queued_dttm = some_date

    assert dict(TaskInstanceInfo(ti)) == {
        "duration": 12.345,
        "map_index": 2,
        "pool": "default_pool",
        "try_number": 0,
        "queued_dttm": "2024-06-01T00:00:00+00:00",
        "log_url": "some_log_url",
        "dag_bundle_name": None,
        "dag_bundle_version": None,
    }


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 test")
def test_task_info_af3():
    class CustomOperator(PythonOperator):
        def __init__(self, *args, **kwargs):
            self.deferrable = True
            self.trigger_dag_id = "trigger_dag_id"
            self.external_dag_id = "external_dag_id"
            self.external_task_id = "external_task_id"
            super().__init__(*args, **kwargs)

    with DAG(
        dag_id="dag",
        schedule="@once",
        start_date=datetime.datetime(2024, 6, 1),
    ) as dag:
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;", dag=dag)
        task_1 = BashOperator(task_id="task_1", bash_command="exit 0;", dag=dag)

        with TaskGroup("section_1", prefix_group_id=True) as tg:
            task_10 = CustomOperator(
                task_id="task_3",
                python_callable=lambda: 1,
                inlets=[Asset(uri="uri1", extra={"a": 1})],
                outlets=[Asset(uri="uri2", extra={"b": 2}), Asset(uri="uri3", extra={"c": 3})],
            )

        task_0 >> task_10
        tg >> task_1

    result = TaskInfo(task_10)
    tg_info = TaskGroupInfo(tg)
    assert dict(tg_info) == {
        "downstream_group_ids": "[]",
        "downstream_task_ids": "['task_1']",
        "group_id": "section_1",
        "prefix_group_id": True,
        "tooltip": "",
        "upstream_group_ids": "[]",
        "upstream_task_ids": "[]",
    }
    assert dict(result) == {
        "deferrable": True,
        "depends_on_past": False,
        "downstream_task_ids": "['task_1']",
        "execution_timeout": None,
        "executor_config": {},
        "external_dag_id": "external_dag_id",
        "external_task_id": "external_task_id",
        "ignore_first_depends_on_past": False,
        "inlets": "[{'uri': 'uri1', 'extra': {'a': 1}}]",
        "mapped": False,
        "max_active_tis_per_dag": None,
        "max_active_tis_per_dagrun": None,
        "max_retry_delay": None,
        "multiple_outputs": False,
        "operator_class": "CustomOperator",
        "operator_class_path": get_fully_qualified_class_name(task_10),
        "operator_provider_version": None,  # Custom operator doesn't have provider version
        "outlets": "[{'uri': 'uri2', 'extra': {'b': 2}}, {'uri': 'uri3', 'extra': {'c': 3}}]",
        "owner": "airflow",
        "priority_weight": 1,
        "queue": "default",
        "retries": 0,
        "retry_exponential_backoff": False,
        "run_as_user": None,
        "task_group": tg_info,
        "task_id": "section_1.task_3",
        "trigger_dag_id": "trigger_dag_id",
        "trigger_rule": "all_success",
        "upstream_task_ids": "['task_0']",
        "wait_for_downstream": False,
        "wait_for_past_depends_before_skipping": False,
    }


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 test")
def test_task_info_af2():
    class CustomOperator(PythonOperator):
        def __init__(self, *args, **kwargs):
            self.deferrable = True
            self.trigger_dag_id = "trigger_dag_id"
            self.external_dag_id = "external_dag_id"
            self.external_task_id = "external_task_id"
            super().__init__(*args, **kwargs)

    with DAG(
        dag_id="dag",
        schedule="@once",
        start_date=datetime.datetime(2024, 6, 1),
    ) as dag:
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;", dag=dag)
        task_1 = BashOperator(task_id="task_1", bash_command="exit 0;", dag=dag)

        with TaskGroup("section_1", prefix_group_id=True) as tg:
            task_10 = CustomOperator(
                task_id="task_3",
                python_callable=lambda: 1,
                inlets=[Asset(uri="uri1", extra={"a": 1})],
                outlets=[Asset(uri="uri2", extra={"b": 2}), Asset(uri="uri3", extra={"c": 3})],
            )

        task_0 >> task_10
        tg >> task_1

    result = TaskInfo(task_10)
    tg_info = TaskGroupInfo(tg)
    assert dict(tg_info) == {
        "downstream_group_ids": "[]",
        "downstream_task_ids": "['task_1']",
        "group_id": "section_1",
        "prefix_group_id": True,
        "tooltip": "",
        "upstream_group_ids": "[]",
        "upstream_task_ids": "[]",
    }
    assert dict(result) == {
        "deferrable": True,
        "depends_on_past": False,
        "downstream_task_ids": "['task_1']",
        "execution_timeout": None,
        "executor_config": {},
        "external_dag_id": "external_dag_id",
        "external_task_id": "external_task_id",
        "ignore_first_depends_on_past": True,
        "is_setup": False,
        "is_teardown": False,
        "sla": None,
        "inlets": "[{'uri': 'uri1', 'extra': {'a': 1}}]",
        "mapped": False,
        "max_active_tis_per_dag": None,
        "max_active_tis_per_dagrun": None,
        "max_retry_delay": None,
        "multiple_outputs": False,
        "operator_class": "CustomOperator",
        "operator_class_path": get_fully_qualified_class_name(task_10),
        "operator_provider_version": None,  # Custom operator doesn't have provider version
        "outlets": "[{'uri': 'uri2', 'extra': {'b': 2}}, {'uri': 'uri3', 'extra': {'c': 3}}]",
        "owner": "airflow",
        "priority_weight": 1,
        "queue": "default",
        "retries": 0,
        "retry_exponential_backoff": False,
        "run_as_user": None,
        "task_group": tg_info,
        "task_id": "section_1.task_3",
        "trigger_dag_id": "trigger_dag_id",
        "trigger_rule": "all_success",
        "upstream_task_ids": "['task_0']",
        "wait_for_downstream": False,
        "wait_for_past_depends_before_skipping": False,
    }


def test_task_info_complete():
    task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")
    result = TaskInfoComplete(task_0)
    assert "'bash_command': 'exit 0;'" in str(result)


@patch("airflow.providers.openlineage.utils.utils.get_fully_qualified_class_name")
def test_get_operator_provider_version_exception_handling(mock_class_name):
    mock_class_name.side_effect = Exception("Test exception")
    operator = MagicMock()
    assert get_operator_provider_version(operator) is None


def test_get_operator_provider_version_for_core_operator():
    """Test that get_operator_provider_version returns None for core operators."""
    operator = BaseOperator(task_id="test_task")
    result = get_operator_provider_version(operator)
    assert result is None


@patch("airflow.providers_manager.ProvidersManager")
def test_get_operator_provider_version_for_provider_operator(mock_providers_manager):
    """Test that get_operator_provider_version returns version for provider operators."""
    # Mock ProvidersManager
    mock_manager_instance = MagicMock()
    mock_providers_manager.return_value = mock_manager_instance

    # Mock providers data
    mock_manager_instance.providers = {
        "apache-airflow-providers-standard": MagicMock(version="1.2.0"),
        "apache-airflow-providers-amazon": MagicMock(version="8.12.0"),
        "apache-airflow-providers-google": MagicMock(version="10.5.0"),
    }

    # Test with BashOperator (standard provider)
    operator = BashOperator(task_id="test_task", bash_command="echo test")
    result = get_operator_provider_version(operator)
    assert result == "1.2.0"


@patch("airflow.providers_manager.ProvidersManager")
def test_get_operator_provider_version_provider_not_found(mock_providers_manager):
    """Test that get_operator_provider_version returns None when provider is not found."""
    # Mock ProvidersManager with no matching provider
    mock_manager_instance = MagicMock()
    mock_providers_manager.return_value = mock_manager_instance
    mock_manager_instance.providers = {
        "apache-airflow-providers-amazon": MagicMock(version="8.12.0"),
        "apache-airflow-providers-google": MagicMock(version="10.5.0"),
    }

    operator = BashOperator(task_id="test_task", bash_command="echo test")
    result = get_operator_provider_version(operator)
    assert result is None


def test_get_operator_provider_version_for_custom_operator():
    """Test that get_operator_provider_version returns None for custom operators."""

    # Create a custom operator that doesn't belong to any provider
    class CustomOperator(BaseOperator):
        def execute(self, context):
            pass

    operator = CustomOperator(task_id="test_task")
    result = get_operator_provider_version(operator)
    assert result is None


@patch("airflow.providers_manager.ProvidersManager")
def test_get_operator_provider_version_for_mapped_operator(mock_providers_manager):
    """Test that get_operator_provider_version works with mapped operators."""
    # Mock ProvidersManager
    mock_manager_instance = MagicMock()
    mock_providers_manager.return_value = mock_manager_instance

    # Mock providers data
    mock_manager_instance.providers = {
        "apache-airflow-providers-standard": MagicMock(version="1.2.0"),
        "apache-airflow-providers-amazon": MagicMock(version="8.12.0"),
    }

    # Test with mapped BashOperator (standard provider)
    mapped_operator = BashOperator.partial(task_id="test_task").expand(bash_command=["echo 1", "echo 2"])
    result = get_operator_provider_version(mapped_operator)
    assert result == "1.2.0"

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
from openlineage.client.facet_v2 import parent_run
from uuid6 import uuid7

from airflow import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.providers.common.compat.assets import Asset
from airflow.providers.common.compat.sdk import BaseOperator, TaskGroup, task, timezone
from airflow.providers.openlineage.conf import namespace
from airflow.providers.openlineage.plugins.facets import AirflowDagRunFacet, AirflowJobFacet
from airflow.providers.openlineage.utils.utils import (
    _MAX_DOC_BYTES,
    DagInfo,
    DagRunInfo,
    TaskGroupInfo,
    TaskInfo,
    TaskInfoComplete,
    TaskInstanceInfo,
    _extract_ol_info_from_asset_event,
    _get_ol_job_dependencies_from_asset_events,
    _get_openlineage_data_from_dagrun_conf,
    _get_task_groups_details,
    _get_tasks_details,
    _truncate_string_to_byte_size,
    build_dag_run_ol_run_id,
    build_task_instance_ol_run_id,
    get_airflow_dag_run_facet,
    get_airflow_job_facet,
    get_airflow_state_run_facet,
    get_dag_documentation,
    get_dag_job_dependency_facet,
    get_dag_parent_run_facet,
    get_fully_qualified_class_name,
    get_job_name,
    get_operator_class,
    get_operator_provider_version,
    get_parent_information_from_dagrun_conf,
    get_root_information_from_dagrun_conf,
    get_task_documentation,
    get_task_parent_run_facet,
    get_user_provided_run_facets,
    is_dag_run_asset_triggered,
    is_valid_uuid,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.events import EventsTimetable
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import BashOperator, OperatorSerialization, PythonOperator
from tests_common.test_utils.mock_operators import MockOperator
from tests_common.test_utils.taskinstance import create_task_instance
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_3_PLUS, AIRFLOW_V_3_0_PLUS

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
    dagrun_mock.execution_date = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.logical_date = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.run_after = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.start_date = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.end_date = datetime.datetime(2024, 6, 1, 1, 2, 14, 34172, tzinfo=datetime.timezone.utc)
    dagrun_mock.triggering_user_name = "user1"
    dagrun_mock.triggered_by = "something"
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
                "clear_number": 0,
                "dag_id": "dag",
                "data_interval_start": "2024-06-01T01:02:03+00:00",
                "data_interval_end": "2024-06-01T02:03:04+00:00",
                "external_trigger": True,
                "run_id": "manual_2024-06-01T00:00:00+00:00",
                "run_type": DagRunType.MANUAL,
                "start_date": "2024-06-01T01:02:04+00:00",
                "end_date": "2024-06-01T01:02:14.034172+00:00",
                "duration": 10.034172,
                "execution_date": "2024-06-01T01:02:04+00:00",
                "logical_date": "2024-06-01T01:02:04+00:00",
                "run_after": "2024-06-01T01:02:04+00:00",
                "dag_bundle_name": "bundle_name",
                "dag_bundle_version": "bundle_version",
                "dag_version_id": "version_id",
                "dag_version_number": "version_number",
                "triggering_user_name": "user1",
                "triggered_by": "something",
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

    serialized = OperatorSerialization.serialize_operator(op)
    deserialized = OperatorSerialization.deserialize_operator(serialized)

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
    ("operator", "expected_doc", "expected_mime_type"),
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

    serialized = OperatorSerialization.serialize_operator(op)
    deserialized = OperatorSerialization.deserialize_operator(serialized)

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
    ("dag", "expected_doc", "expected_mime_type"),
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


@pytest.mark.parametrize("dr_conf", (None, {}))
def test_get_openlineage_data_from_dagrun_conf_none_conf(dr_conf):
    _dr_conf = None if dr_conf is None else {}
    assert _get_openlineage_data_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == _dr_conf  # Assert conf is not changed


def test_get_openlineage_data_from_dagrun_conf_no_openlineage_key():
    dr_conf = {"something_else": {"a": 1}}
    assert _get_openlineage_data_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"something_else": {"a": 1}}  # Assert conf is not changed


def test_get_openlineage_data_from_dagrun_conf_invalid_type():
    dr_conf = {"openlineage": "not_a_dict"}
    assert _get_openlineage_data_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"openlineage": "not_a_dict"}  # Assert conf is not changed


def test_get_openlineage_data_from_dagrun_conf_valid_dict():
    dr_conf = {"openlineage": {"key": "value"}}
    assert _get_openlineage_data_from_dagrun_conf(dr_conf) == {"key": "value"}
    assert dr_conf == {"openlineage": {"key": "value"}}  # Assert conf is not changed


@pytest.mark.parametrize("dr_conf", (None, {}))
def test_get_parent_information_from_dagrun_conf_no_conf(dr_conf):
    _dr_conf = None if dr_conf is None else {}
    assert get_parent_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == _dr_conf  # Assert conf is not changed


def test_get_parent_information_from_dagrun_conf_no_openlineage():
    dr_conf = {"something": "else"}
    assert get_parent_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"something": "else"}  # Assert conf is not changed


def test_get_parent_information_from_dagrun_conf_openlineage_not_dict():
    dr_conf = {"openlineage": "my_value"}
    assert get_parent_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"openlineage": "my_value"}  # Assert conf is not changed


def test_get_parent_information_from_dagrun_conf_missing_keys():
    dr_conf = {"openlineage": {"parentRunId": "id_only"}}
    assert get_parent_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"openlineage": {"parentRunId": "id_only"}}  # Assert conf is not changed


def test_get_parent_information_from_dagrun_conf_invalid_run_id():
    dr_conf = {
        "openlineage": {
            "parentRunId": "not_uuid",
            "parentJobNamespace": "ns",
            "parentJobName": "jobX",
        }
    }
    assert get_parent_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "parentRunId": "not_uuid",
            "parentJobNamespace": "ns",
            "parentJobName": "jobX",
        }
    }


def test_get_parent_information_from_dagrun_conf_valid_data():
    dr_conf = {
        "openlineage": {
            "parentRunId": "11111111-1111-1111-1111-111111111111",
            "parentJobNamespace": "ns",
            "parentJobName": "jobX",
        }
    }
    expected = {
        "parent_run_id": "11111111-1111-1111-1111-111111111111",
        "parent_job_namespace": "ns",
        "parent_job_name": "jobX",
    }
    assert get_parent_information_from_dagrun_conf(dr_conf) == expected
    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "parentRunId": "11111111-1111-1111-1111-111111111111",
            "parentJobNamespace": "ns",
            "parentJobName": "jobX",
        }
    }


@pytest.mark.parametrize("dr_conf", (None, {}))
def test_get_root_information_from_dagrun_conf_no_conf(dr_conf):
    _dr_conf = None if dr_conf is None else {}
    assert get_root_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == _dr_conf  # Assert conf is not changed


def test_get_root_information_from_dagrun_conf_no_openlineage():
    dr_conf = {"something": "else"}
    assert get_root_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"something": "else"}  # Assert conf is not changed


def test_get_root_information_from_dagrun_conf_openlineage_not_dict():
    dr_conf = {"openlineage": "my_value"}
    assert get_root_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"openlineage": "my_value"}  # Assert conf is not changed


def test_get_root_information_from_dagrun_conf_missing_keys():
    dr_conf = {"openlineage": {"rootParentRunId": "id_only"}}
    assert get_root_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {"openlineage": {"rootParentRunId": "id_only"}}  # Assert conf is not changed


def test_get_root_information_from_dagrun_conf_invalid_run_id():
    dr_conf = {
        "openlineage": {
            "rootParentRunId": "not_uuid",
            "rootParentJobNamespace": "ns",
            "rootParentJobName": "jobX",
        }
    }
    assert get_root_information_from_dagrun_conf(dr_conf) == {}
    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "rootParentRunId": "not_uuid",
            "rootParentJobNamespace": "ns",
            "rootParentJobName": "jobX",
        }
    }


def test_get_root_information_from_dagrun_conf_valid_data():
    dr_conf = {
        "openlineage": {
            "rootParentRunId": "11111111-1111-1111-1111-111111111111",
            "rootParentJobNamespace": "ns",
            "rootParentJobName": "jobX",
        }
    }
    expected = {
        "root_parent_run_id": "11111111-1111-1111-1111-111111111111",
        "root_parent_job_namespace": "ns",
        "root_parent_job_name": "jobX",
    }
    assert get_root_information_from_dagrun_conf(dr_conf) == expected
    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "rootParentRunId": "11111111-1111-1111-1111-111111111111",
            "rootParentJobNamespace": "ns",
            "rootParentJobName": "jobX",
        }
    }


@pytest.mark.parametrize("dr_conf", (None, {}))
def test_get_dag_parent_run_facet_no_conf(dr_conf):
    _dr_conf = None if dr_conf is None else {}
    assert get_dag_parent_run_facet(dr_conf) == {}
    assert dr_conf == _dr_conf  # Assert conf is not changed


def test_get_dag_parent_run_facet_missing_keys():
    dr_conf = {"openlineage": {"parentRunId": "11111111-1111-1111-1111-111111111111"}}
    assert get_dag_parent_run_facet(dr_conf) == {}
    # Assert conf is not changed
    assert dr_conf == {"openlineage": {"parentRunId": "11111111-1111-1111-1111-111111111111"}}


def test_get_dag_parent_run_facet_valid_no_root():
    dr_conf = {
        "openlineage": {
            "parentRunId": "11111111-1111-1111-1111-111111111111",
            "parentJobNamespace": "ns",
            "parentJobName": "jobA",
        }
    }

    result = get_dag_parent_run_facet(dr_conf)
    parent_facet = result.get("parent")

    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    assert parent_facet.root is not None  # parent is used as root, since root is missing
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == "ns"
    assert parent_facet.root.job.name == "jobA"

    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "parentRunId": "11111111-1111-1111-1111-111111111111",
            "parentJobNamespace": "ns",
            "parentJobName": "jobA",
        }
    }


def test_get_dag_parent_run_facet_invalid_uuid():
    dr_conf = {
        "openlineage": {
            "parentRunId": "not_uuid",
            "parentJobNamespace": "ns",
            "parentJobName": "jobA",
        }
    }

    result = get_dag_parent_run_facet(dr_conf)
    assert result == {}
    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "parentRunId": "not_uuid",
            "parentJobNamespace": "ns",
            "parentJobName": "jobA",
        }
    }


def test_get_dag_parent_run_facet_valid_with_root():
    dr_conf = {
        "openlineage": {
            "parentRunId": "11111111-1111-1111-1111-111111111111",
            "parentJobNamespace": "ns",
            "parentJobName": "jobA",
            "rootParentRunId": "22222222-2222-2222-2222-222222222222",
            "rootParentJobNamespace": "rootns",
            "rootParentJobName": "rootjob",
        }
    }

    result = get_dag_parent_run_facet(dr_conf)
    parent_facet = result.get("parent")

    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    assert parent_facet.root is not None
    assert parent_facet.root.run.runId == "22222222-2222-2222-2222-222222222222"
    assert parent_facet.root.job.namespace == "rootns"
    assert parent_facet.root.job.name == "rootjob"

    assert dr_conf == {  # Assert conf is not changed
        "openlineage": {
            "parentRunId": "11111111-1111-1111-1111-111111111111",
            "parentJobNamespace": "ns",
            "parentJobName": "jobA",
            "rootParentRunId": "22222222-2222-2222-2222-222222222222",
            "rootParentJobNamespace": "rootns",
            "rootParentJobName": "rootjob",
        }
    }


def test_get_task_parent_run_facet_defaults():
    """Test default behavior with minimal parameters - parent is used as root with default namespace."""
    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
    )
    parent_facet = result.get("parent")

    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == namespace()
    assert parent_facet.job.name == "jobA"
    # Root should default to parent values when no root info is provided
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == namespace()
    assert parent_facet.root.job.name == "jobA"


def test_get_task_parent_run_facet_custom_root_values():
    """Test with all explicit root parameters provided - root should use the provided values."""
    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        root_parent_run_id="22222222-2222-2222-2222-222222222222",
        root_parent_job_name="rjob",
        root_parent_job_namespace="rns",
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    assert parent_facet.root.run.runId == "22222222-2222-2222-2222-222222222222"
    assert parent_facet.root.job.namespace == "rns"
    assert parent_facet.root.job.name == "rjob"


def test_get_task_parent_run_facet_partial_root_info_ignored():
    """Test that incomplete explicit root identifiers are ignored - root defaults to parent."""
    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        root_parent_run_id="22222222-2222-2222-2222-222222222222",  # Only run_id provided
        # Missing root_parent_job_name and root_parent_job_namespace
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should default to parent since incomplete root info was ignored
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == "ns"
    assert parent_facet.root.job.name == "jobA"


def test_get_task_parent_run_facet_with_empty_dr_conf():
    """Test with empty dr_conf - root should default to function parent parameters."""
    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf={},
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should default to parent
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == "ns"
    assert parent_facet.root.job.name == "jobA"


def test_get_task_parent_run_facet_with_dr_conf_root_info():
    """Test with dr_conf containing root information - root should use values from dr_conf."""
    dr_conf = {
        "openlineage": {
            "rootParentRunId": "22222222-2222-2222-2222-222222222222",
            "rootParentJobNamespace": "rootns",
            "rootParentJobName": "rootjob",
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should use values from dr_conf
    assert parent_facet.root.run.runId == "22222222-2222-2222-2222-222222222222"
    assert parent_facet.root.job.namespace == "rootns"
    assert parent_facet.root.job.name == "rootjob"


def test_get_task_parent_run_facet_with_dr_conf_parent_info_only():
    """Test with dr_conf containing only parent information - parent info is used as root fallback."""
    dr_conf = {
        "openlineage": {
            "parentRunId": "33333333-3333-3333-3333-333333333333",
            "parentJobNamespace": "conf_parent_ns",
            "parentJobName": "conf_parent_job",
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should use parent info from dr_conf as fallback
    assert parent_facet.root.run.runId == "33333333-3333-3333-3333-333333333333"
    assert parent_facet.root.job.namespace == "conf_parent_ns"
    assert parent_facet.root.job.name == "conf_parent_job"


def test_get_task_parent_run_facet_with_dr_conf_both_parent_and_root():
    """Test with dr_conf containing both root and parent information - root info takes precedence."""
    dr_conf = {
        "openlineage": {
            "parentRunId": "33333333-3333-3333-3333-333333333333",
            "parentJobNamespace": "conf_parent_ns",
            "parentJobName": "conf_parent_job",
            "rootParentRunId": "44444444-4444-4444-4444-444444444444",
            "rootParentJobNamespace": "conf_root_ns",
            "rootParentJobName": "conf_root_job",
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should use explicit root info from dr_conf
    assert parent_facet.root.run.runId == "44444444-4444-4444-4444-444444444444"
    assert parent_facet.root.job.namespace == "conf_root_ns"
    assert parent_facet.root.job.name == "conf_root_job"


def test_get_task_parent_run_facet_with_dr_conf_incomplete_root():
    """Test with dr_conf containing incomplete root information - root defaults to function parent."""
    dr_conf = {
        "openlineage": {
            "rootParentRunId": "22222222-2222-2222-2222-222222222222",
            # Missing rootParentJobNamespace and rootParentJobName
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should default to parent since dr_conf root info is incomplete
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == "ns"
    assert parent_facet.root.job.name == "jobA"


def test_get_task_parent_run_facet_with_dr_conf_invalid_root_uuid():
    """Test with dr_conf containing invalid root UUID - validation fails, root defaults to parent."""
    dr_conf = {
        "openlineage": {
            "rootParentRunId": "not_a_valid_uuid",
            "rootParentJobNamespace": "rootns",
            "rootParentJobName": "rootjob",
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should default to parent since dr_conf root UUID is invalid
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == "ns"
    assert parent_facet.root.job.name == "jobA"


def test_get_task_parent_run_facet_explicit_root_overrides_dr_conf():
    """Test that explicitly provided root parameters take precedence over dr_conf values."""
    dr_conf = {
        "openlineage": {
            "rootParentRunId": "99999999-9999-9999-9999-999999999999",
            "rootParentJobNamespace": "conf_rootns",
            "rootParentJobName": "conf_rootjob",
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        root_parent_run_id="22222222-2222-2222-2222-222222222222",
        root_parent_job_name="explicit_rjob",
        root_parent_job_namespace="explicit_rns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should use explicitly provided values, not dr_conf
    assert parent_facet.root.run.runId == "22222222-2222-2222-2222-222222222222"
    assert parent_facet.root.job.namespace == "explicit_rns"
    assert parent_facet.root.job.name == "explicit_rjob"


def test_get_task_parent_run_facet_partial_root_in_dr_conf_with_full_parent():
    """Test partial root + full parent in dr_conf - parent info is used as root fallback."""
    dr_conf = {
        "openlineage": {
            "parentRunId": "33333333-3333-3333-3333-333333333333",
            "parentJobNamespace": "conf_parent_ns",
            "parentJobName": "conf_parent_job",
            "rootParentRunId": "44444444-4444-4444-4444-444444444444",
            # Missing rootParentJobNamespace and rootParentJobName
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should use parent info from dr_conf since root info is incomplete
    assert parent_facet.root is not None
    assert parent_facet.root.run.runId == "33333333-3333-3333-3333-333333333333"
    assert parent_facet.root.job.namespace == "conf_parent_ns"
    assert parent_facet.root.job.name == "conf_parent_job"


def test_get_task_parent_run_facet_partial_root_and_partial_parent_in_dr_conf():
    """Test both root and parent incomplete in dr_conf - root defaults to function parent."""
    dr_conf = {
        "openlineage": {
            "parentRunId": "33333333-3333-3333-3333-333333333333",
            # Missing parentJobNamespace and parentJobName
            "rootParentRunId": "44444444-4444-4444-4444-444444444444",
            # Missing rootParentJobNamespace and rootParentJobName
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should default to function parent since both dr_conf root and parent are incomplete
    assert parent_facet.root is not None
    assert parent_facet.root.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.root.job.namespace == "ns"
    assert parent_facet.root.job.name == "jobA"


def test_get_task_parent_run_facet_invalid_root_uuid_with_valid_parent_in_dr_conf():
    """Test invalid root UUID with valid parent in dr_conf - parent info used as root fallback."""
    dr_conf = {
        "openlineage": {
            "parentRunId": "33333333-3333-3333-3333-333333333333",
            "parentJobNamespace": "conf_parent_ns",
            "parentJobName": "conf_parent_job",
            "rootParentRunId": "not_a_valid_uuid",
            "rootParentJobNamespace": "conf_root_ns",
            "rootParentJobName": "conf_root_job",
        }
    }

    result = get_task_parent_run_facet(
        parent_run_id="11111111-1111-1111-1111-111111111111",
        parent_job_name="jobA",
        parent_job_namespace="ns",
        dr_conf=dr_conf,
    )

    parent_facet = result.get("parent")
    assert isinstance(parent_facet, parent_run.ParentRunFacet)
    assert parent_facet.run.runId == "11111111-1111-1111-1111-111111111111"
    assert parent_facet.job.namespace == "ns"
    assert parent_facet.job.name == "jobA"
    # Root should use parent info from dr_conf since root UUID is invalid
    assert parent_facet.root is not None
    assert parent_facet.root.run.runId == "33333333-3333-3333-3333-333333333333"
    assert parent_facet.root.job.namespace == "conf_parent_ns"
    assert parent_facet.root.job.name == "conf_parent_job"


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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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
        sample_ti = create_task_instance(
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


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 tests")
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
    dagrun.triggering_user_name = "my_user"

    result = DagRunInfo(dagrun)
    assert dict(result) == {
        "conf": {"a": 1},
        "clear_number": 0,
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
        "triggered_by": DagRunTriggeredByType.UI,
        "triggering_user_name": "my_user",
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
        "clear_number": 0,
        "dag_id": "dag_id",
        "data_interval_end": "2024-06-01T00:00:00+00:00",
        "data_interval_start": "2024-06-01T00:00:00+00:00",
        "duration": 74.000546,
        "end_date": "2024-06-01T00:01:14.000546+00:00",
        "run_id": "dag_run__run_id",
        "run_type": DagRunType.MANUAL,
        "external_trigger": False,
        "start_date": "2024-06-01T00:00:00+00:00",
        "execution_date": "2024-06-01T00:00:00+00:00",
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
            # Mock some specific attributes from different operators
            self.deferrable = True  # Deferrable operators
            self.column_mapping = "column_mapping"  # SQLColumnCheckOperator
            self.column_names = "column_names"  # SQLInsertRowsOperator
            self.database = "database"  # BaseSQlOperator
            self.execution_date = "execution_date"  # AF 2 ExternalTaskMarker (if run, as it's EmptyOperator)
            self.external_dag_id = (
                "external_dag_id"  # ExternalTaskSensor and ExternalTaskMarker (if run, as it's EmptyOperator)
            )
            self.external_dates_filter = "external_dates_filter"  # ExternalTaskSensor
            self.external_task_group_id = "external_task_group_id"  # ExternalTaskSensor
            self.external_task_id = "external_task_id"  # ExternalTaskSensor and ExternalTaskMarker (if run, as it's EmptyOperator)
            self.external_task_ids = "external_task_ids"  # ExternalTaskSensor
            self.follow_branch = "follow_branch"  # BranchSQLOperator
            self.follow_task_ids_if_false = "follow_task_ids_if_false"  # BranchSQLOperator
            self.follow_task_ids_if_true = "follow_task_ids_if_true"  # BranchSQLOperator
            self.ignore_zero = "ignore_zero"  # SQLIntervalCheckOperator
            self.logical_date = "logical_date"  # AF 3 ExternalTaskMarker (if run, as it's EmptyOperator)
            self.max_threshold = "max_threshold"  # SQLThresholdCheckOperator
            self.metrics_thresholds = "metrics_thresholds"  # SQLIntervalCheckOperator
            self.min_threshold = "min_threshold"  # SQLThresholdCheckOperator
            self.parameters = "parameters"  # SQLCheckOperator, SQLValueCheckOperator and BranchSQLOperator
            self.pass_value = "pass_value"  # SQLValueCheckOperator
            self.postoperator = "postoperator"  # SQLInsertRowsOperator
            self.preoperator = "preoperator"  # SQLInsertRowsOperator
            self.ratio_formula = "ratio_formula"  # SQLIntervalCheckOperator
            self.table_name_with_schema = "table_name_with_schema"  # SQLInsertRowsOperator
            self.tol = "tol"  # SQLValueCheckOperator
            self.trigger_dag_id = "trigger_dag_id"  # TriggerDagRunOperator
            self.trigger_run_id = "trigger_run_id"  # TriggerDagRunOperator
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
        "trigger_rule": "all_success",
        "upstream_task_ids": "['task_0']",
        "wait_for_downstream": False,
        "wait_for_past_depends_before_skipping": False,
        # Operator-specific useful attributes
        "column_mapping": "column_mapping",
        "column_names": "column_names",
        "database": "database",
        "execution_date": "execution_date",
        "external_dag_id": "external_dag_id",
        "external_dates_filter": "external_dates_filter",
        "external_task_group_id": "external_task_group_id",
        "external_task_id": "external_task_id",
        "external_task_ids": "external_task_ids",
        "follow_branch": "follow_branch",
        "follow_task_ids_if_false": "follow_task_ids_if_false",
        "follow_task_ids_if_true": "follow_task_ids_if_true",
        "ignore_zero": "ignore_zero",
        "logical_date": "logical_date",
        "max_threshold": "max_threshold",
        "metrics_thresholds": "metrics_thresholds",
        "min_threshold": "min_threshold",
        "parameters": "parameters",
        "pass_value": "pass_value",
        "postoperator": "postoperator",
        "preoperator": "preoperator",
        "ratio_formula": "ratio_formula",
        "table_name_with_schema": "table_name_with_schema",
        "tol": "tol",
        "trigger_dag_id": "trigger_dag_id",
        "trigger_run_id": "trigger_run_id",
    }


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 test")
def test_task_info_af2():
    class CustomOperator(PythonOperator):
        def __init__(self, *args, **kwargs):
            # Mock some specific attributes from different operators
            self.deferrable = True  # Deferrable operators
            self.column_mapping = "column_mapping"  # SQLColumnCheckOperator
            self.column_names = "column_names"  # SQLInsertRowsOperator
            self.database = "database"  # BaseSQlOperator
            self.execution_date = "execution_date"  # AF 2 ExternalTaskMarker (if run, as it's EmptyOperator)
            self.external_dag_id = (
                "external_dag_id"  # ExternalTaskSensor and ExternalTaskMarker (if run, as it's EmptyOperator)
            )
            self.external_dates_filter = "external_dates_filter"  # ExternalTaskSensor
            self.external_task_group_id = "external_task_group_id"  # ExternalTaskSensor
            self.external_task_id = "external_task_id"  # ExternalTaskSensor and ExternalTaskMarker (if run, as it's EmptyOperator)
            self.external_task_ids = "external_task_ids"  # ExternalTaskSensor
            self.follow_branch = "follow_branch"  # BranchSQLOperator
            self.follow_task_ids_if_false = "follow_task_ids_if_false"  # BranchSQLOperator
            self.follow_task_ids_if_true = "follow_task_ids_if_true"  # BranchSQLOperator
            self.ignore_zero = "ignore_zero"  # SQLIntervalCheckOperator
            self.logical_date = "logical_date"  # AF 3 ExternalTaskMarker (if run, as it's EmptyOperator)
            self.max_threshold = "max_threshold"  # SQLThresholdCheckOperator
            self.metrics_thresholds = "metrics_thresholds"  # SQLIntervalCheckOperator
            self.min_threshold = "min_threshold"  # SQLThresholdCheckOperator
            self.parameters = "parameters"  # SQLCheckOperator, SQLValueCheckOperator and BranchSQLOperator
            self.pass_value = "pass_value"  # SQLValueCheckOperator
            self.postoperator = "postoperator"  # SQLInsertRowsOperator
            self.preoperator = "preoperator"  # SQLInsertRowsOperator
            self.ratio_formula = "ratio_formula"  # SQLIntervalCheckOperator
            self.table_name_with_schema = "table_name_with_schema"  # SQLInsertRowsOperator
            self.tol = "tol"  # SQLValueCheckOperator
            self.trigger_dag_id = "trigger_dag_id"  # TriggerDagRunOperator
            self.trigger_run_id = "trigger_run_id"  # TriggerDagRunOperator
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
        "trigger_rule": "all_success",
        "upstream_task_ids": "['task_0']",
        "wait_for_downstream": False,
        "wait_for_past_depends_before_skipping": False,
        # Operator-specific useful attributes
        "column_mapping": "column_mapping",
        "column_names": "column_names",
        "database": "database",
        "execution_date": "execution_date",
        "external_dag_id": "external_dag_id",
        "external_dates_filter": "external_dates_filter",
        "external_task_group_id": "external_task_group_id",
        "external_task_id": "external_task_id",
        "external_task_ids": "external_task_ids",
        "follow_branch": "follow_branch",
        "follow_task_ids_if_false": "follow_task_ids_if_false",
        "follow_task_ids_if_true": "follow_task_ids_if_true",
        "ignore_zero": "ignore_zero",
        "logical_date": "logical_date",
        "max_threshold": "max_threshold",
        "metrics_thresholds": "metrics_thresholds",
        "min_threshold": "min_threshold",
        "parameters": "parameters",
        "pass_value": "pass_value",
        "postoperator": "postoperator",
        "preoperator": "preoperator",
        "ratio_formula": "ratio_formula",
        "table_name_with_schema": "table_name_with_schema",
        "tol": "tol",
        "trigger_dag_id": "trigger_dag_id",
        "trigger_run_id": "trigger_run_id",
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


class TestGetAirflowStateRunFacet:
    @pytest.mark.db_test
    def test_task_with_timestamps_defined(self, dag_maker):
        """Test task instance with defined start_date and end_date."""
        with dag_maker(dag_id="test_dag"):
            BaseOperator(task_id="test_task")

        dag_run = dag_maker.create_dagrun()
        ti = dag_run.get_task_instance(task_id="test_task")

        # Set valid timestamps
        start_time = pendulum.parse("2024-01-01T10:00:00Z")
        end_time = pendulum.parse("2024-01-01T10:02:30Z")  # 150 seconds difference
        ti.start_date = start_time
        ti.end_date = end_time
        ti.state = TaskInstanceState.SUCCESS
        ti.duration = None

        # Persist changes to database
        with create_session() as session:
            session.merge(ti)
            session.commit()

        result = get_airflow_state_run_facet(
            dag_id="test_dag",
            run_id=dag_run.run_id,
            task_ids=["test_task"],
            dag_run_state=DagRunState.SUCCESS,
        )

        assert result["airflowState"].tasksDuration["test_task"] == 150.0

    @pytest.mark.db_test
    def test_task_with_none_timestamps_fallback_to_zero(self, dag_maker):
        """Test task with None timestamps falls back to 0.0."""
        with dag_maker(dag_id="test_dag"):
            BaseOperator(task_id="terminated_task")

        dag_run = dag_maker.create_dagrun()
        ti = dag_run.get_task_instance(task_id="terminated_task")

        # Set None timestamps (signal-terminated case)
        ti.start_date = None
        ti.end_date = None
        ti.state = TaskInstanceState.SKIPPED
        ti.duration = None

        # Persist changes to database
        with create_session() as session:
            session.merge(ti)
            session.commit()

        result = get_airflow_state_run_facet(
            dag_id="test_dag",
            run_id=dag_run.run_id,
            task_ids=["terminated_task"],
            dag_run_state=DagRunState.FAILED,
        )

        assert result["airflowState"].tasksDuration["terminated_task"] == 0.0


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 specific test")
def test_is_dag_run_asset_triggered_af3():
    """Test is_dag_run_asset_triggered for Airflow 3."""
    from airflow.models.dagrun import DagRunTriggeredByType

    dag_run = MagicMock(triggered_by=DagRunTriggeredByType.ASSET)

    assert is_dag_run_asset_triggered(dag_run) is True

    dag_run.triggered_by = DagRunTriggeredByType.TIMETABLE
    assert is_dag_run_asset_triggered(dag_run) is False


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Airflow 2 specific test")
def test_is_dag_run_asset_triggered_af2():
    """Test is_dag_run_asset_triggered for Airflow 2."""
    from airflow.models.dagrun import DagRunType

    dag_run = MagicMock(run_type=DagRunType.DATASET_TRIGGERED)

    assert is_dag_run_asset_triggered(dag_run) is True

    dag_run.run_type = DagRunType.MANUAL
    assert is_dag_run_asset_triggered(dag_run) is False


def test_build_task_instance_ol_run_id():
    """Test deterministic UUID generation for task instance."""
    logical_date = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = build_task_instance_ol_run_id(
        dag_id="test_dag",
        task_id="test_task",
        try_number=1,
        logical_date=logical_date,
        map_index=0,
    )

    assert run_id == "018cc4e5-2200-7b27-b511-a7a14aa0662a"

    # Should be deterministic - same inputs produce same output
    run_id2 = build_task_instance_ol_run_id(
        dag_id="test_dag",
        task_id="test_task",
        try_number=1,
        logical_date=logical_date,
        map_index=0,
    )
    assert run_id == run_id2

    # Different inputs should produce different outputs
    run_id3 = build_task_instance_ol_run_id(
        dag_id="test_dag",
        task_id="test_task",
        try_number=2,  # Different try_number
        logical_date=logical_date,
        map_index=0,
    )
    assert run_id != run_id3


def test_build_dag_run_ol_run_id():
    """Test deterministic UUID generation for DAG run."""
    logical_date = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = build_dag_run_ol_run_id(
        dag_id="test_dag",
        logical_date=logical_date,
        clear_number=0,
    )
    assert run_id == "018cc4e5-2200-725f-8091-596ad71712b2"

    # Should be deterministic - same inputs produce same output
    run_id2 = build_dag_run_ol_run_id(
        dag_id="test_dag",
        logical_date=logical_date,
        clear_number=0,
    )
    assert run_id == run_id2

    # Different inputs should produce different outputs
    run_id3 = build_dag_run_ol_run_id(
        dag_id="test_dag",
        logical_date=logical_date,
        clear_number=1,  # Different clear_number
    )
    assert run_id != run_id3


def test_validate_uuid_valid():
    """Test validation of valid UUID strings."""
    valid_uuids = [
        "550e8400-e29b-41d4-a716-446655440000",
        "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "00000000-0000-0000-0000-000000000000",
    ]
    for uuid_str in valid_uuids:
        assert is_valid_uuid(uuid_str) is True


def test_validate_uuid_invalid():
    """Test validation of invalid UUID strings."""
    invalid_uuids = [
        "not-a-uuid",
        "550e8400-e29b-41d4-a716",  # Too short
        "550e8400-e29b-41d4-a716-446655440000-extra",  # Too long
        "550e8400-e29b-41d4-a716-44665544000g",  # Invalid character
        "",
        "123",
        None,
    ]
    for uuid_str in invalid_uuids:
        assert is_valid_uuid(uuid_str) is False


class TestExtractOlInfoFromAssetEvent:
    """Tests for _extract_ol_info_from_asset_event function."""

    def test_extract_ol_info_from_task_instance(self):
        """Test extraction from TaskInstance (priority 1)."""
        logical_date = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Mock TaskInstance - using MagicMock without spec to avoid SQLAlchemy mapper inspection
        ti = MagicMock()
        ti.dag_id = "source_dag"
        ti.task_id = "source_task"
        ti.try_number = 1
        ti.map_index = 0

        # Mock DagRun
        source_dr = MagicMock()
        source_dr.logical_date = logical_date
        source_dr.run_after = None

        # Mock AssetEvent
        asset_event = MagicMock()
        asset_event.source_task_instance = ti
        asset_event.source_dag_run = source_dr
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {}

        result = _extract_ol_info_from_asset_event(asset_event)

        expected_run_id = build_task_instance_ol_run_id(
            dag_id="source_dag",
            task_id="source_task",
            try_number=1,
            logical_date=logical_date,
            map_index=0,
        )
        assert result == {
            "job_name": "source_dag.source_task",
            "job_namespace": namespace(),
            "run_id": expected_run_id,
        }

    def test_extract_ol_info_from_task_instance_no_logical_date(self):
        """Test extraction from TaskInstance without logical_date."""
        # Mock TaskInstance
        ti = MagicMock()
        ti.dag_id = "source_dag"
        ti.task_id = "source_task"
        ti.try_number = 1
        ti.map_index = 0

        # Mock DagRun with None logical_date
        source_dr = MagicMock()
        source_dr.logical_date = None
        source_dr.run_after = None

        # Mock AssetEvent
        asset_event = MagicMock()
        asset_event.source_task_instance = ti
        asset_event.source_dag_run = source_dr
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {}

        result = _extract_ol_info_from_asset_event(asset_event)

        # run_id should not be included if logical_date is None
        assert result == {
            "job_name": "source_dag.source_task",
            "job_namespace": namespace(),
        }

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Airflow 3 specific test")
    def test_extract_ol_info_from_task_instance_run_after_fallback(self):
        """Test extraction from TaskInstance with run_after fallback (AF3)."""
        run_after = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Mock TaskInstance
        ti = MagicMock()
        ti.dag_id = "source_dag"
        ti.task_id = "source_task"
        ti.try_number = 1
        ti.map_index = 0

        # Mock DagRun with None logical_date but run_after set
        source_dr = MagicMock()
        source_dr.logical_date = None
        source_dr.run_after = run_after

        # Mock AssetEvent
        asset_event = MagicMock()
        asset_event.source_task_instance = ti
        asset_event.source_dag_run = source_dr
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {}

        result = _extract_ol_info_from_asset_event(asset_event)

        # Should use run_after as fallback for logical_date
        expected_run_id = build_task_instance_ol_run_id(
            dag_id="source_dag",
            task_id="source_task",
            try_number=1,
            logical_date=run_after,
            map_index=0,
        )
        assert result == {
            "job_name": "source_dag.source_task",
            "job_namespace": namespace(),
            "run_id": expected_run_id,
        }

    def test_extract_ol_info_from_source_fields(self):
        """Test extraction from AssetEvent source fields (priority 2)."""
        # Mock AssetEvent without TaskInstance
        asset_event = MagicMock()
        asset_event.source_task_instance = None
        asset_event.source_dag_run = None
        asset_event.source_dag_id = "source_dag"
        asset_event.source_task_id = "source_task"
        asset_event.extra = {}

        result = _extract_ol_info_from_asset_event(asset_event)

        # run_id cannot be constructed from source fields alone
        assert result == {
            "job_name": "source_dag.source_task",
            "job_namespace": namespace(),
        }

    def test_extract_ol_info_from_extra(self):
        """Test extraction from asset_event.extra (priority 3)."""
        # Mock AssetEvent
        asset_event = MagicMock()
        asset_event.source_task_instance = None
        asset_event.source_dag_run = None
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {
            "openlineage": {
                "parentJobName": "extra_job",
                "parentJobNamespace": "extra_namespace",
                "parentRunId": "550e8400-e29b-41d4-a716-446655440000",
            }
        }

        result = _extract_ol_info_from_asset_event(asset_event)

        assert result == {
            "job_name": "extra_job",
            "job_namespace": "extra_namespace",
            "run_id": "550e8400-e29b-41d4-a716-446655440000",
        }

    def test_extract_ol_info_from_extra_no_run_id(self):
        """Test extraction from asset_event.extra without run_id."""
        # Mock AssetEvent
        asset_event = MagicMock()
        asset_event.source_task_instance = None
        asset_event.source_dag_run = None
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {
            "openlineage": {
                "parentJobName": "extra_job",
                "parentJobNamespace": "extra_namespace",
            }
        }

        result = _extract_ol_info_from_asset_event(asset_event)

        assert result == {
            "job_name": "extra_job",
            "job_namespace": "extra_namespace",
        }

    def test_extract_ol_info_from_extra_no_job_name(self):
        """Test extraction from asset_event.extra without job_name."""
        # Mock AssetEvent
        asset_event = MagicMock()
        asset_event.source_task_instance = None
        asset_event.source_dag_run = None
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {
            "openlineage": {
                "parentRunId": "550e8400-e29b-41d4-a716-446655440000",
                "parentJobNamespace": "extra_namespace",
            }
        }

        result = _extract_ol_info_from_asset_event(asset_event)

        assert result is None

    def test_extract_ol_info_insufficient_info(self):
        """Test extraction when no information is available."""
        # Mock AssetEvent with no information
        asset_event = MagicMock()
        asset_event.source_task_instance = None
        asset_event.source_dag_run = None
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {}

        result = _extract_ol_info_from_asset_event(asset_event)

        assert result is None


class TestGetOlJobDependenciesFromAssetEvents:
    """Tests for _get_ol_job_dependencies_from_asset_events function."""

    def test_get_ol_job_dependencies_no_events(self):
        """Test when no events are provided."""
        result = _get_ol_job_dependencies_from_asset_events([])

        assert result == []

    @patch("airflow.providers.openlineage.utils.utils._extract_ol_info_from_asset_event")
    def test_get_ol_job_dependencies_with_events(self, mock_extract):
        """Test extraction and deduplication of asset events."""
        # Mock asset events
        asset_event1 = MagicMock()
        asset_event1.id = 1
        asset_event1.source_run_id = "run1"
        asset_event1.asset_id = 101
        asset_event1.dataset_id = 101
        asset_event1.uri = "s3://bucket/file1"
        asset_event1.extra = {}
        asset_event1.partition_key = None

        asset_event2 = MagicMock()
        asset_event2.id = 2
        asset_event2.source_run_id = "run2"
        asset_event2.asset_id = 102
        asset_event2.dataset_id = 102
        asset_event2.uri = "s3://bucket/file2"
        asset_event2.extra = {}
        asset_event2.partition_key = None

        # Mock extraction results
        mock_extract.side_effect = [
            {
                "job_name": "dag1.task1",
                "job_namespace": "namespace",
                "run_id": "550e8400-e29b-41d4-a716-446655440000",
            },
            {
                "job_name": "dag2.task2",
                "job_namespace": "namespace",
                "run_id": "550e8400-e29b-41d4-a716-446655440001",
            },
        ]

        result = _get_ol_job_dependencies_from_asset_events([asset_event1, asset_event2])

        assert result == [
            {
                "job_name": "dag1.task1",
                "job_namespace": "namespace",
                "run_id": "550e8400-e29b-41d4-a716-446655440000",
                "asset_events": [
                    {
                        "dag_run_id": "run1",
                        "asset_event_id": 1,
                        "asset_event_extra": None,
                        "asset_id": 101,
                        "asset_uri": "s3://bucket/file1",
                        "partition_key": None,
                    }
                ],
            },
            {
                "job_name": "dag2.task2",
                "job_namespace": "namespace",
                "run_id": "550e8400-e29b-41d4-a716-446655440001",
                "asset_events": [
                    {
                        "dag_run_id": "run2",
                        "asset_event_id": 2,
                        "asset_event_extra": None,
                        "asset_id": 102,
                        "asset_uri": "s3://bucket/file2",
                        "partition_key": None,
                    }
                ],
            },
        ]

    @patch("airflow.providers.openlineage.utils.utils._extract_ol_info_from_asset_event")
    def test_get_ol_job_dependencies_deduplication(self, mock_extract):
        """Test deduplication of duplicate asset events."""
        # Mock asset events
        asset_event1 = MagicMock()
        asset_event1.id = 1
        asset_event1.source_run_id = "run1"
        asset_event1.asset_id = 101
        asset_event1.dataset_id = 101
        asset_event1.uri = "s3://bucket/file1"
        asset_event1.extra = {}
        asset_event1.partition_key = None

        asset_event2 = MagicMock()
        asset_event2.id = 2
        asset_event2.source_run_id = "run2"
        asset_event2.asset_id = 102
        asset_event2.dataset_id = 102
        asset_event2.uri = "s3://bucket/file2"
        asset_event2.extra = {}
        asset_event2.partition_key = None

        # Mock extraction results - same job/run (should be deduplicated)
        same_info = {
            "job_name": "dag1.task1",
            "job_namespace": "namespace",
        }
        mock_extract.side_effect = [same_info, same_info]

        result = _get_ol_job_dependencies_from_asset_events([asset_event1, asset_event2])

        # Should be deduplicated to one entry with both events aggregated
        assert result == [
            {
                "job_name": "dag1.task1",
                "job_namespace": "namespace",
                "asset_events": [
                    {
                        "dag_run_id": "run1",
                        "asset_event_id": 1,
                        "asset_event_extra": None,
                        "asset_id": 101,
                        "asset_uri": "s3://bucket/file1",
                        "partition_key": None,
                    },
                    {
                        "dag_run_id": "run2",
                        "asset_event_id": 2,
                        "asset_event_extra": None,
                        "asset_id": 102,
                        "asset_uri": "s3://bucket/file2",
                        "partition_key": None,
                    },
                ],
            }
        ]

    @patch("airflow.providers.openlineage.utils.utils._extract_ol_info_from_asset_event")
    def test_get_ol_job_dependencies_insufficient_info(self, mock_extract):
        """Test handling when extraction returns None."""
        # Mock asset event
        asset_event = MagicMock()
        asset_event.id = 1

        # Mock extraction returning None
        mock_extract.return_value = None

        result = _get_ol_job_dependencies_from_asset_events([asset_event])

        assert result == []


class TestGetDagJobDependencyFacet:
    """Tests for get_dag_job_dependency_facet function.

    These tests mock only the DB-accessing function (_get_eagerly_loaded_dagrun_consumed_asset_events)
    to test the full flow of facet generation including event processing and facet building.
    """

    @patch("airflow.providers.openlineage.utils.utils._get_eagerly_loaded_dagrun_consumed_asset_events")
    def test_get_dag_job_dependency_facet_no_events(self, mock_get_events):
        """Test when no asset events are found."""
        mock_get_events.return_value = []

        result = get_dag_job_dependency_facet("test_dag", "test_run_id")

        assert result == {}
        mock_get_events.assert_called_once_with("test_dag", "test_run_id")

    @patch("airflow.providers.openlineage.utils.utils._get_eagerly_loaded_dagrun_consumed_asset_events")
    def test_get_dag_job_dependency_facet_exception_handling(self, mock_get_events):
        """Test exception handling in get_dag_job_dependency_facet."""
        mock_get_events.side_effect = Exception("Database error")

        result = get_dag_job_dependency_facet("test_dag", "test_run_id")

        assert result == {}

    @patch("airflow.providers.openlineage.utils.utils._get_eagerly_loaded_dagrun_consumed_asset_events")
    def test_get_dag_job_dependency_facet_insufficient_info_skipped(self, mock_get_events):
        """Test that events with insufficient info are skipped."""
        # Create an event with no usable information
        asset_event = MagicMock()
        asset_event.source_task_instance = None
        asset_event.source_dag_run = None
        asset_event.source_dag_id = None
        asset_event.source_task_id = None
        asset_event.extra = {}
        asset_event.id = 1
        asset_event.source_run_id = None
        asset_event.asset_id = 101
        asset_event.dataset_id = 101
        asset_event.uri = "s3://bucket/file"
        asset_event.partition_key = None

        mock_get_events.return_value = [asset_event]

        result = get_dag_job_dependency_facet("test_dag", "test_run_id")

        assert result == {}

    @patch("airflow.providers.openlineage.utils.utils._get_eagerly_loaded_dagrun_consumed_asset_events")
    def test_get_dag_job_dependency_facet_with_events(self, mock_get_events):
        """Test facet generation with asset events - tests full flow."""
        logical_date = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Create mock asset events with source TaskInstance (priority 1 source)
        ti1 = MagicMock()
        ti1.dag_id = "source_dag1"
        ti1.task_id = "source_task1"
        ti1.try_number = 1
        ti1.map_index = 0

        source_dr1 = MagicMock()
        source_dr1.logical_date = logical_date
        source_dr1.run_after = None

        asset_event1 = MagicMock()
        asset_event1.source_task_instance = ti1
        asset_event1.source_dag_run = source_dr1
        asset_event1.source_dag_id = None
        asset_event1.source_task_id = None
        asset_event1.extra = {}
        asset_event1.id = 1
        asset_event1.source_run_id = "run1"
        asset_event1.asset_id = 101
        asset_event1.dataset_id = 101
        asset_event1.uri = "s3://bucket/file1"
        asset_event1.partition_key = None

        # Second event with source fields (priority 2 source, no run_id)
        asset_event2 = MagicMock()
        asset_event2.source_task_instance = None
        asset_event2.source_dag_run = None
        asset_event2.source_dag_id = "source_dag2"
        asset_event2.source_task_id = "source_task2"
        asset_event2.extra = {}
        asset_event2.id = 2
        asset_event2.source_run_id = "run2"
        asset_event2.asset_id = 102
        asset_event2.dataset_id = 102
        asset_event2.uri = "s3://bucket/file2"
        asset_event2.partition_key = None

        mock_get_events.return_value = [asset_event1, asset_event2]

        result = get_dag_job_dependency_facet("test_dag", "test_run_id")

        # Verify result structure
        assert len(result) == 1
        facet = result["jobDependencies"]
        assert len(facet.upstream) == 2
        assert len(facet.downstream) == 0

        # Verify first dependency (from TaskInstance source, has run_id)
        dep1 = facet.upstream[0]
        assert dep1.job.namespace == namespace()
        assert dep1.job.name == "source_dag1.source_task1"
        expected_run_id = build_task_instance_ol_run_id(
            dag_id="source_dag1",
            task_id="source_task1",
            try_number=1,
            logical_date=logical_date,
            map_index=0,
        )
        assert dep1.run.runId == expected_run_id
        assert dep1.dependency_type == "IMPLICIT_ASSET_DEPENDENCY"
        assert dep1.airflow["asset_events"] == [
            {
                "dag_run_id": "run1",
                "asset_event_id": 1,
                "asset_event_extra": None,
                "asset_id": 101,
                "asset_uri": "s3://bucket/file1",
                "partition_key": None,
            }
        ]

        # Verify second dependency (from source fields, no run_id)
        dep2 = facet.upstream[1]
        assert dep2.job.namespace == namespace()
        assert dep2.job.name == "source_dag2.source_task2"
        assert dep2.run is None
        assert dep2.dependency_type == "IMPLICIT_ASSET_DEPENDENCY"
        assert dep2.airflow["asset_events"] == [
            {
                "dag_run_id": "run2",
                "asset_event_id": 2,
                "asset_event_extra": None,
                "asset_id": 102,
                "asset_uri": "s3://bucket/file2",
                "partition_key": None,
            }
        ]

    @patch("airflow.providers.openlineage.utils.utils._get_eagerly_loaded_dagrun_consumed_asset_events")
    def test_get_dag_job_dependency_facet_deduplication(self, mock_get_events):
        """Test that duplicate asset events from same job/run are deduplicated."""
        logical_date = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

        # Create two events from the same source TI (should be deduplicated)
        ti = MagicMock()
        ti.dag_id = "source_dag"
        ti.task_id = "source_task"
        ti.try_number = 1
        ti.map_index = 0

        source_dr = MagicMock()
        source_dr.logical_date = logical_date
        source_dr.run_after = None

        asset_event1 = MagicMock()
        asset_event1.source_task_instance = ti
        asset_event1.source_dag_run = source_dr
        asset_event1.source_dag_id = None
        asset_event1.source_task_id = None
        asset_event1.extra = {}
        asset_event1.id = 1
        asset_event1.source_run_id = "run1"
        asset_event1.asset_id = 101
        asset_event1.dataset_id = 101
        asset_event1.uri = "s3://bucket/file1"
        asset_event1.partition_key = None

        asset_event2 = MagicMock()
        asset_event2.source_task_instance = ti  # Same TI
        asset_event2.source_dag_run = source_dr  # Same DR
        asset_event2.source_dag_id = None
        asset_event2.source_task_id = None
        asset_event2.extra = {}
        asset_event2.id = 2
        asset_event2.source_run_id = "run1"
        asset_event2.asset_id = 102  # Different asset
        asset_event2.dataset_id = 102  # Different asset
        asset_event2.uri = "s3://bucket/file2"
        asset_event2.partition_key = None

        mock_get_events.return_value = [asset_event1, asset_event2]

        result = get_dag_job_dependency_facet("test_dag", "test_run_id")

        assert len(result) == 1
        facet = result["jobDependencies"]
        assert len(facet.upstream) == 1
        assert len(facet.downstream) == 0

        # Verify the single deduplicated dependency
        dep = facet.upstream[0]
        assert dep.job.namespace == namespace()
        assert dep.job.name == "source_dag.source_task"
        expected_run_id = build_task_instance_ol_run_id(
            dag_id="source_dag",
            task_id="source_task",
            try_number=1,
            logical_date=logical_date,
            map_index=0,
        )
        assert dep.run.runId == expected_run_id
        assert dep.dependency_type == "IMPLICIT_ASSET_DEPENDENCY"

        # Both asset events should be aggregated into single dependency
        assert dep.airflow["asset_events"] == [
            {
                "dag_run_id": "run1",
                "asset_event_id": 1,
                "asset_event_extra": None,
                "asset_id": 101,
                "asset_uri": "s3://bucket/file1",
                "partition_key": None,
            },
            {
                "dag_run_id": "run1",
                "asset_event_id": 2,
                "asset_event_extra": None,
                "asset_id": 102,
                "asset_uri": "s3://bucket/file2",
                "partition_key": None,
            },
        ]

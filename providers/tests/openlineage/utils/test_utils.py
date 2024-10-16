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
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.mappedoperator import MappedOperator
from airflow.models.taskinstance import TaskInstance, TaskInstanceState
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.plugins.facets import AirflowDagRunFacet, AirflowJobFacet
from airflow.providers.openlineage.utils.utils import (
    _get_task_groups_details,
    _get_tasks_details,
    get_airflow_dag_run_facet,
    get_airflow_job_facet,
    get_fully_qualified_class_name,
    get_job_name,
    get_operator_class,
    get_user_provided_run_facets,
)
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import AIRFLOW_V_2_10_PLUS, BashOperator
from tests_common.test_utils.mock_operators import MockOperator

BASH_OPERATOR_PATH = "airflow.providers.standard.operators.bash"
if not AIRFLOW_V_2_10_PLUS:
    BASH_OPERATOR_PATH = "airflow.operators.bash"


class CustomOperatorForTest(BashOperator):
    pass


class CustomOperatorFromEmpty(EmptyOperator):
    pass


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
                    "tooltip": "",
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
                    "operator": "airflow.operators.python.PythonOperator",
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
    dagrun_mock.dag_id = dag.dag_id
    dagrun_mock.data_interval_start = datetime.datetime(2024, 6, 1, 1, 2, 3, tzinfo=datetime.timezone.utc)
    dagrun_mock.data_interval_end = datetime.datetime(2024, 6, 1, 2, 3, 4, tzinfo=datetime.timezone.utc)
    dagrun_mock.external_trigger = True
    dagrun_mock.run_id = "manual_2024-06-01T00:00:00+00:00"
    dagrun_mock.run_type = DagRunType.MANUAL
    dagrun_mock.start_date = datetime.datetime(2024, 6, 1, 1, 2, 4, tzinfo=datetime.timezone.utc)

    result = get_airflow_dag_run_facet(dagrun_mock)

    expected_dag_info = {
        "dag_id": "dag",
        "description": None,
        "fileloc": pathlib.Path(__file__).resolve().as_posix(),
        "owner": "airflow",
        "timetable": {},
        "start_date": "2024-06-01T00:00:00+00:00",
        "tags": "['test']",
    }
    if hasattr(dag, "schedule_interval"):  # Airflow 2 compat.
        expected_dag_info["schedule_interval"] = "@once"
    else:  # Airflow 3 and up.
        expected_dag_info["timetable_summary"] = "@once"
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
                "run_type": "manual",
                "start_date": "2024-06-01T01:02:04+00:00",
            },
        )
    }


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
    assert deserialized._task_type == op_name


def test_get_fully_qualified_class_name_mapped_operator():
    mapped = MockOperator.partial(task_id="task_2").expand(arg2=["a", "b", "c"])
    assert isinstance(mapped, MappedOperator)
    mapped_op_path = get_fully_qualified_class_name(mapped)
    assert mapped_op_path == "tests_common.test_utils.mock_operators.MockOperator"


def test_get_fully_qualified_class_name_bash_operator():
    result = get_fully_qualified_class_name(BashOperator(task_id="test", bash_command="echo 0;"))
    expected_result = f"{BASH_OPERATOR_PATH}.BashOperator"
    assert result == expected_result


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
    assert isinstance(mapped, MappedOperator)
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

    expected = {
        "generate_list": {
            "emits_ol_events": True,
            "is_setup": False,
            "is_teardown": False,
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
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
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
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
            "operator": "airflow.decorators.python._PythonDecoratedOperator",
            "task_group": None,
            "ui_color": "#ffefeb",
            "ui_fgcolor": "#000",
            "ui_label": "sum_values",
            "downstream_task_ids": [],
        },
        "task": {
            "operator": "providers.tests.openlineage.utils.test_utils.CustomOperatorForTest",
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
            "operator": "providers.tests.openlineage.utils.test_utils.CustomOperatorFromEmpty",
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
            "operator": "airflow.operators.python.PythonOperator",
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
            "operator": "airflow.operators.empty.EmptyOperator",
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
            "operator": "providers.tests.openlineage.utils.test_utils.TestMappedOperator",
            "task_group": None,
            "ui_color": "#fff",
            "ui_fgcolor": "#000",
            "ui_label": "task_6",
            "downstream_task_ids": [
                "task_3",
            ],
        },
        "section_1.task_3": {
            "operator": "airflow.operators.python.PythonOperator",
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
            "operator": "airflow.operators.empty.EmptyOperator",
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
            "operator": "airflow.operators.python.PythonOperator",
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
            "tooltip": "",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg1",
        },
        "tg2": {
            "parent_group": None,
            "tooltip": "",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg2",
        },
        "tg3": {
            "parent_group": None,
            "tooltip": "",
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
            "tooltip": "",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg1",
        },
        "tg1.tg2": {
            "parent_group": "tg1",
            "tooltip": "",
            "ui_color": "CornflowerBlue",
            "ui_fgcolor": "#000",
            "ui_label": "tg2",
        },
        "tg1.tg2.tg3": {
            "parent_group": "tg1.tg2",
            "tooltip": "",
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
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
        ),
        state="running",
    )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"providers.tests.openlineage.utils.custom_facet_fixture.get_additional_test_facet"},
)
def test_get_user_provided_run_facets_with_function_definition(mock_custom_facet_funcs):
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
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
        "providers.tests.openlineage.utils.custom_facet_fixture.get_additional_test_facet",
    },
)
def test_get_user_provided_run_facets_with_return_value_as_none(mock_custom_facet_funcs):
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
        "providers.tests.openlineage.utils.custom_facet_fixture.get_additional_test_facet",
        "providers.tests.openlineage.utils.custom_facet_fixture.return_type_is_not_dict",
        "providers.tests.openlineage.utils.custom_facet_fixture.get_another_test_facet",
    },
)
def test_get_user_provided_run_facets_with_multiple_function_definition(mock_custom_facet_funcs):
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
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
        "providers.tests.openlineage.utils.custom_facet_fixture.get_additional_test_facet",
        "providers.tests.openlineage.utils.custom_facet_fixture.get_duplicate_test_facet_key",
    },
)
def test_get_user_provided_run_facets_with_duplicate_facet_keys(mock_custom_facet_funcs):
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
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
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
        ),
        state="running",
    )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"providers.tests.openlineage.utils.custom_facet_fixture.return_type_is_not_dict"},
)
def test_get_user_provided_run_facets_with_wrong_return_type_function(mock_custom_facet_funcs):
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
        ),
        state="running",
    )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}


@patch(
    "airflow.providers.openlineage.conf.custom_run_facets",
    return_value={"providers.tests.openlineage.utils.custom_facet_fixture.get_custom_facet_throws_exception"},
)
def test_get_user_provided_run_facets_with_exception(mock_custom_facet_funcs):
    sample_ti = TaskInstance(
        task=EmptyOperator(
            task_id="test-task", dag=DAG("test-dag", schedule=None, start_date=datetime.datetime(2024, 7, 1))
        ),
        state="running",
    )
    result = get_user_provided_run_facets(sample_ti, TaskInstanceState.RUNNING)
    assert result == {}

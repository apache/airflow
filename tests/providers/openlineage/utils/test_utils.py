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
from unittest.mock import MagicMock

from airflow import DAG
from airflow.models.mappedoperator import MappedOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.openlineage.plugins.facets import AirflowJobFacet
from airflow.providers.openlineage.utils.utils import (
    _get_parsed_dag_tree,
    _get_task_groups_details,
    _get_tasks_details,
    _safe_get_dag_tree_view,
    get_airflow_job_facet,
    get_fully_qualified_class_name,
    get_job_name,
    get_operator_class,
)
from airflow.serialization.serialized_objects import SerializedBaseOperator
from airflow.utils.task_group import TaskGroup
from tests.test_utils.mock_operators import MockOperator


class CustomOperatorForTest(BashOperator):
    pass


class CustomOperatorFromEmpty(EmptyOperator):
    pass


def test_get_airflow_job_facet():
    with DAG(dag_id="dag", start_date=datetime.datetime(2024, 6, 1)) as dag:
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")

        with TaskGroup("section_1", prefix_group_id=True):
            task_10 = PythonOperator(task_id="task_3", python_callable=lambda: 1)

        task_0 >> task_10

    dagrun_mock = MagicMock()
    dagrun_mock.dag = dag

    result = get_airflow_job_facet(dagrun_mock)
    assert result == {
        "airflow": AirflowJobFacet(
            taskTree={"task_0": {"section_1.task_3": {}}},
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
                    "operator": "airflow.operators.bash.BashOperator",
                    "task_group": None,
                    "emits_ol_events": True,
                    "ui_color": "#f0ede4",
                    "ui_fgcolor": "#000",
                    "ui_label": "task_0",
                    "is_setup": False,
                    "is_teardown": False,
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
                },
            },
        )
    }


def test_get_fully_qualified_class_name_serialized_operator():
    op_module_path = "airflow.operators.bash"
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
    assert mapped_op_path == "tests.test_utils.mock_operators.MockOperator"


def test_get_fully_qualified_class_name_bash_operator():
    result = get_fully_qualified_class_name(BashOperator(task_id="test", bash_command="echo 0;"))
    expected_result = "airflow.operators.bash.BashOperator"
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
    with DAG(dag_id="dag", start_date=datetime.datetime(2024, 6, 1)) as dag:
        task = CustomOperatorForTest(task_id="task", bash_command="exit 0;")  # noqa: F841
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")  # noqa: F841
        task_1 = CustomOperatorFromEmpty(task_id="task_1")  # noqa: F841
        task_2 = PythonOperator(task_id="task_2", python_callable=lambda: 1)  # noqa: F841
        task_3 = BashOperator(task_id="task_3", bash_command="exit 0;")  # noqa: F841
        task_4 = EmptyOperator(task_id="task_4.test.dot")  # noqa: F841
        task_5 = BashOperator(task_id="task_5", bash_command="exit 0;")  # noqa: F841

        with TaskGroup("section_1", prefix_group_id=True) as tg:
            task_10 = PythonOperator(task_id="task_3", python_callable=lambda: 1)  # noqa: F841
            with TaskGroup("section_2", parent_group=tg) as tg2:
                task_11 = EmptyOperator(task_id="task_11")  # noqa: F841
                with TaskGroup("section_3", parent_group=tg2):
                    task_12 = PythonOperator(task_id="task_12", python_callable=lambda: 1)  # noqa: F841

    expected = {
        "task": {
            "operator": "tests.providers.openlineage.utils.test_utils.CustomOperatorForTest",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": CustomOperatorForTest.ui_color,
            "ui_fgcolor": CustomOperatorForTest.ui_fgcolor,
            "ui_label": "task",
            "is_setup": False,
            "is_teardown": False,
        },
        "task_0": {
            "operator": "airflow.operators.bash.BashOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": BashOperator.ui_color,
            "ui_fgcolor": BashOperator.ui_fgcolor,
            "ui_label": "task_0",
            "is_setup": False,
            "is_teardown": False,
        },
        "task_1": {
            "operator": "tests.providers.openlineage.utils.test_utils.CustomOperatorFromEmpty",
            "task_group": None,
            "emits_ol_events": False,
            "ui_color": CustomOperatorFromEmpty.ui_color,
            "ui_fgcolor": CustomOperatorFromEmpty.ui_fgcolor,
            "ui_label": "task_1",
            "is_setup": False,
            "is_teardown": False,
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
        },
        "task_3": {
            "operator": "airflow.operators.bash.BashOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": BashOperator.ui_color,
            "ui_fgcolor": BashOperator.ui_fgcolor,
            "ui_label": "task_3",
            "is_setup": False,
            "is_teardown": False,
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
        },
        "task_5": {
            "operator": "airflow.operators.bash.BashOperator",
            "task_group": None,
            "emits_ol_events": True,
            "ui_color": BashOperator.ui_color,
            "ui_fgcolor": BashOperator.ui_fgcolor,
            "ui_label": "task_5",
            "is_setup": False,
            "is_teardown": False,
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
        },
    }

    result = _get_tasks_details(dag)
    assert result == expected


def test_get_tasks_details_empty_dag():
    assert _get_tasks_details(DAG("test_dag", start_date=datetime.datetime(2024, 6, 1))) == {}


def test_dag_tree_level_indent():
    """Tests the correct indentation of tasks in a DAG tree view.

    Test verifies that the tree view of the DAG correctly represents the hierarchical structure
    of the tasks with proper indentation. The expected indentation increases by 4 spaces for each
    subsequent level in the DAG. The test asserts that the generated tree view matches the expected
    lines with correct indentation.
    """
    with DAG(dag_id="dag", start_date=datetime.datetime(2024, 6, 1)) as dag:
        task_0 = EmptyOperator(task_id="task_0")
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = EmptyOperator(task_id="task_2")
        task_3 = EmptyOperator(task_id="task_3")

    task_0 >> task_1 >> task_2
    task_3 >> task_2

    indent = 4 * " "
    expected_lines = [
        "<Task(EmptyOperator): task_0>",
        indent + "<Task(EmptyOperator): task_1>",
        2 * indent + "<Task(EmptyOperator): task_2>",
        "<Task(EmptyOperator): task_3>",
        indent + "<Task(EmptyOperator): task_2>",
    ]
    assert _safe_get_dag_tree_view(dag) == expected_lines


def test_get_dag_tree():
    with DAG(dag_id="dag", start_date=datetime.datetime(2024, 6, 1)) as dag:
        task = CustomOperatorForTest(task_id="task", bash_command="exit 0;")
        task_0 = BashOperator(task_id="task_0", bash_command="exit 0;")
        task_1 = BashOperator(task_id="task_1", bash_command="exit 1;")
        task_2 = PythonOperator(task_id="task_2", python_callable=lambda: 1)
        task_3 = BashOperator(task_id="task_3", bash_command="exit 0;")
        task_4 = EmptyOperator(
            task_id="task_4",
        )
        task_5 = BashOperator(task_id="task_5", bash_command="exit 0;")
        task_6 = EmptyOperator(task_id="task_6.test5")
        task_7 = BashOperator(task_id="task_7", bash_command="exit 0;")
        task_8 = PythonOperator(task_id="task_8", python_callable=lambda: 1)  # noqa: F841
        task_9 = BashOperator(task_id="task_9", bash_command="exit 0;")

        with TaskGroup("section_1", prefix_group_id=True) as tg:
            task_10 = PythonOperator(task_id="task_3", python_callable=lambda: 1)
            with TaskGroup("section_2", parent_group=tg) as tg2:
                task_11 = EmptyOperator(task_id="task_11")  # noqa: F841
                with TaskGroup("section_3", parent_group=tg2):
                    task_12 = PythonOperator(task_id="task_12", python_callable=lambda: 1)

        task >> [task_2, task_7]
        task_0 >> [task_2, task_1] >> task_3 >> [task_4, task_5] >> task_6
        task_1 >> task_9 >> task_3 >> task_4 >> task_5 >> task_6
        task_3 >> task_10 >> task_12

        expected = {
            "section_1.section_2.task_11": {},
            "task": {
                "task_2": {
                    "task_3": {
                        "section_1.task_3": {"section_1.section_2.section_3.task_12": {}},
                        "task_4": {"task_5": {"task_6.test5": {}}, "task_6.test5": {}},
                        "task_5": {"task_6.test5": {}},
                    }
                },
                "task_7": {},
            },
            "task_0": {
                "task_1": {
                    "task_3": {
                        "section_1.task_3": {"section_1.section_2.section_3.task_12": {}},
                        "task_4": {"task_5": {"task_6.test5": {}}, "task_6.test5": {}},
                        "task_5": {"task_6.test5": {}},
                    },
                    "task_9": {
                        "task_3": {
                            "section_1.task_3": {"section_1.section_2.section_3.task_12": {}},
                            "task_4": {"task_5": {"task_6.test5": {}}, "task_6.test5": {}},
                            "task_5": {"task_6.test5": {}},
                        }
                    },
                },
                "task_2": {
                    "task_3": {
                        "section_1.task_3": {"section_1.section_2.section_3.task_12": {}},
                        "task_4": {"task_5": {"task_6.test5": {}}, "task_6.test5": {}},
                        "task_5": {"task_6.test5": {}},
                    }
                },
            },
            "task_8": {},
        }
        result = _get_parsed_dag_tree(dag)
        assert result == expected


def test_get_dag_tree_empty_dag():
    assert _get_parsed_dag_tree(DAG("test_dag", start_date=datetime.datetime(2024, 6, 1))) == {}


def test_get_task_groups_details():
    with DAG("test_dag", start_date=datetime.datetime(2024, 6, 1)) as dag:
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
    with DAG("test_dag", start_date=datetime.datetime(2024, 6, 1)) as dag:
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
    assert _get_task_groups_details(DAG("test_dag", start_date=datetime.datetime(2024, 6, 1))) == {}

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

import pendulum
import pytest

from airflow.exceptions import TaskAlreadyInTaskGroup
from airflow.sdk import (
    DAG,
    XComArg,
    dag,
    setup,
    task as task_decorator,
    task_group as task_group_decorator,
    teardown,
    timezone,
)
from airflow.sdk.definitions.taskgroup import TaskGroup

from tests_common.test_utils.compat import BashOperator, EmptyOperator, PythonOperator

DEFAULT_DATE = timezone.datetime(2025, 1, 1)


class TestTaskGroup:
    @pytest.mark.parametrize(
        ("group_id", "exc_type", "exc_value"),
        [
            pytest.param(
                123,
                TypeError,
                "The key has to be a string and is <class 'int'>:123",
                id="type",
            ),
            pytest.param(
                "a" * 1000,
                ValueError,
                "The key has to be less than 200 characters, not 1000",
                id="long",
            ),
            pytest.param(
                "something*invalid",
                ValueError,
                "The key 'something*invalid' has to be made of alphanumeric characters, dashes, "
                "and underscores exclusively",
                id="illegal",
            ),
        ],
    )
    def test_dag_id_validation(self, group_id, exc_type, exc_value):
        with pytest.raises(exc_type) as ctx:
            TaskGroup(group_id)
        assert str(ctx.value) == exc_value


def test_task_group_dependencies_between_tasks_if_task_group_is_empty_1():
    """
    Test that if a task group is empty, the dependencies between tasks are still maintained.
    """
    with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.parse("20200101")):
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group1") as tg1:
            pass
        with TaskGroup("group2") as tg2:
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")
            task2 >> task3

        task1 >> tg1 >> tg2

    assert task1.downstream_task_ids == {"group2.task2"}


def test_task_group_dependencies_between_tasks_if_task_group_is_empty_2():
    """
    Test that if a task group is empty, the dependencies between tasks are still maintained.
    """
    with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.parse("20200101")):
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group1") as tg1:
            pass
        with TaskGroup("group2") as tg2:
            pass
        with TaskGroup("group3") as tg3:
            pass
        with TaskGroup("group4") as tg4:
            pass
        with TaskGroup("group5") as tg5:
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")
            task2 >> task3
        task1 >> tg1 >> tg2 >> tg3 >> tg4 >> tg5

    assert task1.downstream_task_ids == {"group5.task2"}


def test_task_group_dependencies_between_tasks_if_task_group_is_empty_3():
    """
    Test that if a task group is empty, the dependencies between tasks are still maintained.
    """
    with DAG(dag_id="test_dag", schedule=None, start_date=pendulum.parse("20200101")):
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group1") as tg1:
            pass
        with TaskGroup("group2") as tg2:
            pass
        task2 = EmptyOperator(task_id="task2")
        with TaskGroup("group3") as tg3:
            pass
        with TaskGroup("group4") as tg4:
            pass
        with TaskGroup("group5") as tg5:
            task3 = EmptyOperator(task_id="task3")
            task4 = EmptyOperator(task_id="task4")
            task3 >> task4
        task1 >> tg1 >> tg2 >> task2 >> tg3 >> tg4 >> tg5

    assert task1.downstream_task_ids == {"task2"}
    assert task2.downstream_task_ids == {"group5.task3"}


def test_build_task_group_context_manager():
    """Test basic TaskGroup functionality using context manager."""
    logical_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_context_manager", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            _ = EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                _ = EmptyOperator(task_id="task3")
                _ = EmptyOperator(task_id="task4")

        task5 = EmptyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    assert task1.get_direct_relative_ids(upstream=False) == {
        "group234.group34.task4",
        "group234.group34.task3",
        "group234.task2",
    }
    assert task5.get_direct_relative_ids(upstream=True) == {
        "group234.group34.task4",
        "group234.group34.task3",
    }

    assert dag.task_group.group_id is None
    assert dag.task_group.is_root
    assert set(dag.task_group.children.keys()) == {"task1", "group234", "task5"}
    assert group34.group_id == "group234.group34"


def test_build_task_group():
    """
    Test alternative syntax to use TaskGroup. It should result in the same TaskGroup
    as using context manager.
    """
    logical_date = pendulum.parse("20200101")
    dag = DAG("test_build_task_group", schedule=None, start_date=logical_date)
    task1 = EmptyOperator(task_id="task1", dag=dag)
    group234 = TaskGroup("group234", dag=dag)
    _ = EmptyOperator(task_id="task2", dag=dag, task_group=group234)
    group34 = TaskGroup("group34", dag=dag, parent_group=group234)
    _ = EmptyOperator(task_id="task3", dag=dag, task_group=group34)
    _ = EmptyOperator(task_id="task4", dag=dag, task_group=group34)
    task5 = EmptyOperator(task_id="task5", dag=dag)

    task1 >> group234
    group34 >> task5

    # Test basic TaskGroup structure
    assert dag.task_group.group_id is None
    assert dag.task_group.is_root
    assert set(dag.task_group.children.keys()) == {"task1", "group234", "task5"}
    assert group34.group_id == "group234.group34"


def test_build_task_group_with_prefix():
    """
    Tests that prefix_group_id turns on/off prefixing of task_id with group_id.
    """
    logical_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_with_prefix", start_date=logical_date):
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234", prefix_group_id=False) as group234:
            task2 = EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                task3 = EmptyOperator(task_id="task3")

                with TaskGroup("group4", prefix_group_id=False) as group4:
                    task4 = EmptyOperator(task_id="task4")

        task5 = EmptyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    assert task2.task_id == "task2"
    assert group34.group_id == "group34"
    assert task3.task_id == "group34.task3"
    assert group4.group_id == "group34.group4"
    assert task4.task_id == "task4"
    assert task5.task_id == "task5"
    assert group234.get_child_by_label("task2") == task2
    assert group234.get_child_by_label("group34") == group34
    assert group4.get_child_by_label("task4") == task4


def test_build_task_group_with_prefix_functionality():
    """
    Tests TaskGroup prefix_group_id functionality - additional test for comprehensive coverage.
    """
    logical_date = pendulum.parse("20200101")
    with DAG("test_prefix_functionality", start_date=logical_date):
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234", prefix_group_id=False) as group234:
            task2 = EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                task3 = EmptyOperator(task_id="task3")

                with TaskGroup("group4", prefix_group_id=False) as group4:
                    task4 = EmptyOperator(task_id="task4")

        task5 = EmptyOperator(task_id="task5")
        task1 >> group234
        group34 >> task5

    # Test prefix_group_id behavior
    assert task2.task_id == "task2"  # prefix_group_id=False, so no prefix
    assert group34.group_id == "group34"  # nested group gets prefixed
    assert task3.task_id == "group34.task3"  # task in nested group gets full prefix
    assert group4.group_id == "group34.group4"  # nested group gets parent prefix
    assert task4.task_id == "task4"  # prefix_group_id=False, so no prefix
    assert task5.task_id == "task5"  # root level task, no prefix

    # Test group hierarchy and child access
    assert group234.get_child_by_label("task2") == task2
    assert group234.get_child_by_label("group34") == group34
    assert group4.get_child_by_label("task4") == task4


def test_build_task_group_with_task_decorator():
    """
    Test that TaskGroup can be used with the @task decorator.
    """
    from airflow.sdk import task

    @task
    def task_1():
        print("task_1")

    @task
    def task_2():
        return "task_2"

    @task
    def task_3():
        return "task_3"

    @task
    def task_4(task_2_output, task_3_output):
        print(task_2_output, task_3_output)

    @task
    def task_5():
        print("task_5")

    logical_date = pendulum.parse("20200101")
    with DAG("test_build_task_group_with_task_decorator", start_date=logical_date):
        tsk_1 = task_1()

        with TaskGroup("group234") as group234:
            tsk_2 = task_2()
            tsk_3 = task_3()
            tsk_4 = task_4(tsk_2, tsk_3)

        tsk_5 = task_5()

        tsk_1 >> group234 >> tsk_5

    # Test TaskGroup functionality with @task decorator
    assert tsk_1.operator in tsk_2.operator.upstream_list
    assert tsk_1.operator in tsk_3.operator.upstream_list
    assert tsk_5.operator in tsk_4.operator.downstream_list

    # Test TaskGroup structure
    assert group234.group_id == "group234"
    assert len(group234.children) == 3  # task_2, task_3, task_4
    assert "group234.task_2" in group234.children
    assert "group234.task_3" in group234.children
    assert "group234.task_4" in group234.children


def test_sub_dag_task_group():
    """
    Tests dag.partial_subset() updates task_group correctly.
    """
    logical_date = pendulum.parse("20200101")
    with DAG("test_test_task_group_sub_dag", schedule=None, start_date=logical_date) as dag:
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group234") as group234:
            _ = EmptyOperator(task_id="task2")

            with TaskGroup("group34") as group34:
                _ = EmptyOperator(task_id="task3")
                _ = EmptyOperator(task_id="task4")

        with TaskGroup("group6") as group6:
            _ = EmptyOperator(task_id="task6")

        task7 = EmptyOperator(task_id="task7")
        task5 = EmptyOperator(task_id="task5")

        task1 >> group234
        group34 >> task5
        group234 >> group6
        group234 >> task7

    subset = dag.partial_subset(task_ids="task5", include_upstream=True, include_downstream=False)

    # Test that partial_subset correctly updates task_group structure
    groups = subset.task_group.get_task_group_dict()
    assert groups.keys() == {None, "group234", "group234.group34"}

    included_group_ids = {"group234", "group234.group34"}
    included_task_ids = {"group234.group34.task3", "group234.group34.task4", "task1", "task5"}

    # Test that subset maintains correct group relationships
    for task_group in groups.values():
        assert task_group.upstream_group_ids.issubset(included_group_ids)
        assert task_group.downstream_group_ids.issubset(included_group_ids)
        assert task_group.upstream_task_ids.issubset(included_task_ids)
        assert task_group.downstream_task_ids.issubset(included_task_ids)

    # Test that subset maintains correct task relationships
    for task in subset.task_group:
        assert task.upstream_task_ids.issubset(included_task_ids)
        assert task.downstream_task_ids.issubset(included_task_ids)

    # Test basic subset properties
    assert len(subset.tasks) == 4  # task1, task3, task4, task5
    assert subset.task_dict["task1"].task_id == "task1"
    assert subset.task_dict["group234.group34.task3"].task_id == "group234.group34.task3"
    assert subset.task_dict["group234.group34.task4"].task_id == "group234.group34.task4"
    assert subset.task_dict["task5"].task_id == "task5"


def test_dag_edges_task_group_structure():
    logical_date = pendulum.parse("20200101")
    with DAG("test_dag_edges", schedule=None, start_date=logical_date):
        task1 = EmptyOperator(task_id="task1")
        with TaskGroup("group_a") as group_a:
            with TaskGroup("group_b") as group_b:
                task2 = EmptyOperator(task_id="task2")
                task3 = EmptyOperator(task_id="task3")
                task4 = EmptyOperator(task_id="task4")
                task2 >> [task3, task4]

            task5 = EmptyOperator(task_id="task5")

            task5 << group_b

        task1 >> group_a

        with TaskGroup("group_c") as group_c:
            task6 = EmptyOperator(task_id="task6")
            task7 = EmptyOperator(task_id="task7")
            task8 = EmptyOperator(task_id="task8")
            [task6, task7] >> task8
            group_a >> group_c

        task5 >> task8

        task9 = EmptyOperator(task_id="task9")
        task10 = EmptyOperator(task_id="task10")

        group_c >> [task9, task10]

        with TaskGroup("group_d") as group_d:
            task11 = EmptyOperator(task_id="task11")
            task12 = EmptyOperator(task_id="task12")
            task11 >> task12

        group_d << group_c

    # Test TaskGroup structure and relationships
    assert group_a.group_id == "group_a"
    assert group_b.group_id == "group_a.group_b"
    assert group_c.group_id == "group_c"
    assert group_d.group_id == "group_d"

    # Test task relationships within groups
    assert task2.downstream_task_ids == {"group_a.group_b.task3", "group_a.group_b.task4"}
    assert task3.upstream_task_ids == {"group_a.group_b.task2"}
    assert task4.upstream_task_ids == {"group_a.group_b.task2"}
    assert task5.upstream_task_ids == {"group_a.group_b.task3", "group_a.group_b.task4"}

    # Test cross-group relationships
    assert task1.downstream_task_ids == {"group_a.group_b.task2"}
    assert task6.upstream_task_ids == {"group_a.task5"}
    assert task7.upstream_task_ids == {"group_a.task5"}
    assert task8.upstream_task_ids == {"group_c.task6", "group_c.task7", "group_a.task5"}

    # Test group-to-task relationships
    assert task9.upstream_task_ids == {"group_c.task8"}
    assert task10.upstream_task_ids == {"group_c.task8"}
    assert task11.upstream_task_ids == {"group_c.task8"}


def test_duplicate_group_id():
    from airflow.exceptions import DuplicateTaskIdFound

    logical_date = pendulum.parse("20200101")

    with DAG("test_duplicate_group_id", schedule=None, start_date=logical_date):
        _ = EmptyOperator(task_id="task1")
        with pytest.raises(DuplicateTaskIdFound, match=r".* 'task1' .*"), TaskGroup("task1"):
            pass

    with DAG("test_duplicate_group_id", schedule=None, start_date=logical_date):
        _ = EmptyOperator(task_id="task1")
        with TaskGroup("group1", prefix_group_id=False):
            with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1' .*"), TaskGroup("group1"):
                pass

    with DAG("test_duplicate_group_id", schedule=None, start_date=logical_date):
        with TaskGroup("group1", prefix_group_id=False):
            with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1' .*"):
                _ = EmptyOperator(task_id="group1")

    with DAG("test_duplicate_group_id", schedule=None, start_date=logical_date):
        _ = EmptyOperator(task_id="task1")
        with TaskGroup("group1"):
            with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1.downstream_join_id' .*"):
                _ = EmptyOperator(task_id="downstream_join_id")

    with DAG("test_duplicate_group_id", schedule=None, start_date=logical_date):
        _ = EmptyOperator(task_id="task1")
        with TaskGroup("group1"):
            with pytest.raises(DuplicateTaskIdFound, match=r".* 'group1.upstream_join_id' .*"):
                _ = EmptyOperator(task_id="upstream_join_id")


def test_task_without_dag():
    """
    Test that if a task doesn't have a DAG when it's being set as the relative of another task which
    has a DAG, the task should be added to the root TaskGroup of the other task's DAG.
    """
    dag = DAG(dag_id="test_task_without_dag", schedule=None, start_date=pendulum.parse("20200101"))
    op1 = EmptyOperator(task_id="op1", dag=dag)
    op2 = EmptyOperator(task_id="op2")
    op3 = EmptyOperator(task_id="op3")
    op1 >> op2
    op3 >> op2

    assert op1.dag == op2.dag == op3.dag
    assert dag.task_group.children.keys() == {"op1", "op2", "op3"}
    assert dag.task_group.children.keys() == dag.task_dict.keys()


def test_default_args():
    """Testing TaskGroup with default_args"""
    logical_date = pendulum.parse("20201109")
    with DAG(
        dag_id="example_task_group_default_args",
        schedule=None,
        start_date=logical_date,
        default_args={"owner": "dag"},
    ):
        with TaskGroup("group1", default_args={"owner": "group"}):
            task_1 = EmptyOperator(task_id="task_1")
            task_2 = EmptyOperator(task_id="task_2", owner="task")
            task_3 = EmptyOperator(task_id="task_3", default_args={"owner": "task"})

            assert task_1.owner == "group"
            assert task_2.owner == "task"
            assert task_3.owner == "task"


def test_iter_tasks():
    with DAG("test_dag", schedule=None, start_date=pendulum.parse("20200101")) as dag:
        with TaskGroup("section_1") as tg1:
            EmptyOperator(task_id="task1")

        with TaskGroup("section_2") as tg2:
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")
            mapped_bash_operator = BashOperator.partial(task_id="bash_task").expand(
                bash_command=[
                    "echo hello 1",
                    "echo hello 2",
                    "echo hello 3",
                ]
            )
            task2 >> task3 >> mapped_bash_operator

    tg1 >> tg2
    root_group = dag.task_group
    assert [t.task_id for t in root_group.iter_tasks()] == [
        "section_1.task1",
        "section_2.task2",
        "section_2.task3",
        "section_2.bash_task",
    ]
    assert [t.task_id for t in tg1.iter_tasks()] == [
        "section_1.task1",
    ]
    assert [t.task_id for t in tg2.iter_tasks()] == [
        "section_2.task2",
        "section_2.task3",
        "section_2.bash_task",
    ]


def test_override_dag_default_args():
    logical_date = pendulum.parse("20201109")
    with DAG(
        dag_id="example_task_group_default_args",
        schedule=None,
        start_date=logical_date,
        default_args={"owner": "dag"},
    ):
        with TaskGroup("group1", default_args={"owner": "group"}):
            task_1 = EmptyOperator(task_id="task_1")
            task_2 = EmptyOperator(task_id="task_2", owner="task")
            task_3 = EmptyOperator(task_id="task_3", default_args={"owner": "task"})

            assert task_1.owner == "group"
            assert task_2.owner == "task"
            assert task_3.owner == "task"


def test_override_dag_default_args_in_nested_tg():
    logical_date = pendulum.parse("20201109")
    with DAG(
        dag_id="example_task_group_default_args",
        schedule=None,
        start_date=logical_date,
        default_args={"owner": "dag"},
    ):
        with TaskGroup("group1", default_args={"owner": "group1"}):
            task_1 = EmptyOperator(task_id="task_1")
            with TaskGroup("group2", default_args={"owner": "group2"}):
                task_2 = EmptyOperator(task_id="task_2")
                task_3 = EmptyOperator(task_id="task_3", owner="task")

            assert task_1.owner == "group1"
            assert task_2.owner == "group2"
            assert task_3.owner == "task"


def test_override_dag_default_args_in_multi_level_nested_tg():
    logical_date = pendulum.parse("20201109")
    with DAG(
        dag_id="example_task_group_default_args",
        schedule=None,
        start_date=logical_date,
        default_args={"owner": "dag"},
    ):
        with TaskGroup("group1", default_args={"owner": "group1"}):
            task_1 = EmptyOperator(task_id="task_1")
            with TaskGroup("group2"):
                task_2 = EmptyOperator(task_id="task_2")
                with TaskGroup("group3", default_args={"owner": "group3"}):
                    task_3 = EmptyOperator(task_id="task_3")
                    task_4 = EmptyOperator(task_id="task_4", owner="task")

            assert task_1.owner == "group1"
            assert task_2.owner == "group1"  # inherits from group1
            assert task_3.owner == "group3"
            assert task_4.owner == "task"


def test_task_group_arrow_with_setups_teardowns():
    with DAG(dag_id="hi", schedule=None, start_date=pendulum.datetime(2022, 1, 1)):
        with TaskGroup(group_id="tg1") as tg1:
            s1 = EmptyOperator(task_id="s1")
            w1 = EmptyOperator(task_id="w1")
            t1 = EmptyOperator(task_id="t1")
            s1 >> w1 >> t1.as_teardown(setups=s1)
        w2 = EmptyOperator(task_id="w2")
        tg1 >> w2

    assert t1.downstream_task_ids == set()
    assert w1.downstream_task_ids == {"tg1.t1", "w2"}
    assert s1.downstream_task_ids == {"tg1.t1", "tg1.w1"}
    assert t1.upstream_task_ids == {"tg1.s1", "tg1.w1"}


def test_task_group_arrow_basic():
    with DAG(dag_id="basic_group_test"):
        with TaskGroup("group_1") as g1:
            task_1 = EmptyOperator(task_id="task_1")

        with TaskGroup("group_2") as g2:
            task_2 = EmptyOperator(task_id="task_2")

        g1 >> g2

    # Test basic TaskGroup relationships
    assert task_1.downstream_task_ids == {"group_2.task_2"}
    assert task_2.upstream_task_ids == {"group_1.task_1"}


def test_task_group_nested_structure():
    with DAG(dag_id="nested_group_test"):
        with TaskGroup("group_1") as g1:
            with TaskGroup("group_1_1") as g1_1:
                task_1_1 = EmptyOperator(task_id="task_1_1")

        with TaskGroup("group_2") as g2:
            task_2 = EmptyOperator(task_id="task_2")

        g1 >> g2

    # Test nested TaskGroup structure
    assert g1_1.group_id == "group_1.group_1_1"
    assert task_1_1.task_id == "group_1.group_1_1.task_1_1"
    assert task_1_1.downstream_task_ids == {"group_2.task_2"}
    assert task_2.upstream_task_ids == {"group_1.group_1_1.task_1_1"}


def test_task_group_with_invalid_arg_type_raises_error():
    error_msg = r"'ui_color' must be <class 'str'> \(got 123 that is a <class 'int'>\)\."
    with DAG(dag_id="dag_with_tg_invalid_arg_type", schedule=None):
        with pytest.raises(TypeError, match=error_msg):
            _ = TaskGroup("group_1", ui_color=123)


def test_task_group_arrow_with_setup_group_deeper_setup():
    """
    When recursing upstream for a non-teardown leaf, we should ignore setups that
    are direct upstream of a teardown.
    """
    with DAG(dag_id="setup_group_teardown_group_2", schedule=None, start_date=pendulum.now()):
        with TaskGroup("group_1") as g1:

            @setup
            def setup_1(): ...

            @setup
            def setup_2(): ...

            @teardown
            def teardown_0(): ...

            s1 = setup_1()
            s2 = setup_2()
            t0 = teardown_0()
            s2 >> t0

        with TaskGroup("group_2") as g2:

            @teardown
            def teardown_1(): ...

            @teardown
            def teardown_2(): ...

            t1 = teardown_1()
            t2 = teardown_2()

        @task_decorator
        def work(): ...

        w1 = work()
        g1 >> w1 >> g2
        t1.as_teardown(setups=s1)
        t2.as_teardown(setups=s2)
    assert set(s1.operator.downstream_task_ids) == {"work", "group_2.teardown_1"}
    assert set(s2.operator.downstream_task_ids) == {"group_1.teardown_0", "group_2.teardown_2"}
    assert set(w1.operator.downstream_task_ids) == {"group_2.teardown_1", "group_2.teardown_2"}
    assert set(t1.operator.downstream_task_ids) == set()
    assert set(t2.operator.downstream_task_ids) == set()


def test_add_to_sub_group():
    with DAG("test_dag", schedule=None, start_date=pendulum.parse("20200101")):
        tg = TaskGroup("section")
        task = EmptyOperator(task_id="task")
        with pytest.raises(TaskAlreadyInTaskGroup) as ctx:
            tg.add(task)

    assert str(ctx.value) == "cannot add 'task' to 'section' (already in the DAG's root group)"


def test_add_to_another_group():
    with DAG("test_dag", schedule=None, start_date=pendulum.parse("20200101")):
        tg = TaskGroup("section_1")
        with TaskGroup("section_2"):
            task = EmptyOperator(task_id="task")
        with pytest.raises(TaskAlreadyInTaskGroup) as ctx:
            tg.add(task)

    assert str(ctx.value) == "cannot add 'section_2.task' to 'section_1' (already in group 'section_2')"


def test_task_group_edge_modifier_chain():
    from airflow.sdk import Label, chain

    with DAG(dag_id="test", schedule=None, start_date=pendulum.DateTime(2022, 5, 20)) as dag:
        start = EmptyOperator(task_id="sleep_3_seconds")

        with TaskGroup(group_id="group1") as tg:
            t1 = EmptyOperator(task_id="dummy1")
            t2 = EmptyOperator(task_id="dummy2")

        t3 = EmptyOperator(task_id="echo_done")

    # The case we are testing for is when a Label is inside a list -- meaning that we do tg.set_upstream
    # instead of label.set_downstream
    chain(start, [Label("branch three")], tg, t3)

    assert start.downstream_task_ids == {t1.node_id, t2.node_id}
    assert t3.upstream_task_ids == {t1.node_id, t2.node_id}
    assert tg.upstream_task_ids == set()
    assert tg.downstream_task_ids == {t3.node_id}
    # Check that we can perform a topological_sort
    dag.topological_sort()


def test_mapped_task_group_id_prefix_task_id():
    from tests_common.test_utils.mock_operators import MockOperator

    with DAG(dag_id="d", schedule=None, start_date=DEFAULT_DATE) as dag:
        t1 = MockOperator.partial(task_id="t1").expand(arg1=[])
        with TaskGroup("g"):
            t2 = MockOperator.partial(task_id="t2").expand(arg1=[])

    assert t1.task_id == "t1"
    assert t2.task_id == "g.t2"

    dag.get_task("t1") == t1
    dag.get_task("g.t2") == t2


def test_pass_taskgroup_output_to_task():
    """Test that the output of a task group can be passed to a task."""
    from airflow.sdk import task

    @task
    def one():
        return 1

    @task_group_decorator
    def addition_task_group(num):
        @task
        def add_one(i):
            return i + 1

        return add_one(num)

    @task
    def increment(num):
        return num + 1

    @dag(schedule=None, start_date=pendulum.DateTime(2022, 1, 1), default_args={"owner": "airflow"})
    def wrap():
        total_1 = one()
        assert isinstance(total_1, XComArg)
        total_2 = addition_task_group(total_1)
        assert isinstance(total_2, XComArg)
        total_3 = increment(total_2)
        assert isinstance(total_3, XComArg)

    wrap()


def test_decorator_unknown_args():
    """Test that unknown args passed to the decorator cause an error at parse time"""
    with pytest.raises(TypeError):

        @task_group_decorator(b=2)
        def tg(): ...


def test_decorator_multiple_use_task():
    from airflow.sdk import task

    @dag("test-dag", schedule=None, start_date=DEFAULT_DATE)
    def _test_dag():
        @task
        def t():
            pass

        @task_group_decorator
        def tg():
            for _ in range(3):
                t()

        t() >> tg() >> t()

    test_dag = _test_dag()
    assert test_dag.task_ids == [
        "t",  # Start end.
        "tg.t",
        "tg.t__1",
        "tg.t__2",
        "t__1",  # End node.
    ]


def test_build_task_group_depended_by_task():
    """A decorator-based task group should be able to be used as a relative to operators."""
    from airflow.sdk import dag as dag_decorator, task

    @dag_decorator(schedule=None, start_date=pendulum.now())
    def build_task_group_depended_by_task():
        @task
        def task_start():
            return "[Task_start]"

        @task
        def task_end():
            return "[Task_end]"

        @task
        def task_thing(value):
            return f"[Task_thing {value}]"

        @task_group_decorator
        def section_1():
            task_thing(1)
            task_thing(2)

        task_start() >> section_1() >> task_end()

    dag = build_task_group_depended_by_task()
    task_thing_1 = dag.task_dict["section_1.task_thing"]
    task_thing_2 = dag.task_dict["section_1.task_thing__1"]

    # Tasks in the task group don't depend on each other; they both become
    # downstreams to task_start, and upstreams to task_end.
    assert task_thing_1.upstream_task_ids == task_thing_2.upstream_task_ids == {"task_start"}
    assert task_thing_1.downstream_task_ids == task_thing_2.downstream_task_ids == {"task_end"}


def test_build_task_group_with_operators():
    """Tests Dag with Tasks created with *Operators and TaskGroup created with taskgroup decorator"""
    from airflow.sdk import task

    def task_start():
        return "[Task_start]"

    def task_end():
        print("[ Task_End  ]")

    # Creating Tasks
    @task
    def task_1(value):
        return f"[ Task1 {value} ]"

    @task
    def task_2(value):
        return f"[ Task2 {value} ]"

    @task
    def task_3(value):
        print(f"[ Task3 {value} ]")

    # Creating TaskGroups
    @task_group_decorator(group_id="section_1")
    def section_a(value):
        """TaskGroup for grouping related Tasks"""
        return task_3(task_2(task_1(value)))

    logical_date = pendulum.parse("20201109")
    with DAG(
        dag_id="example_task_group_decorator_mix",
        schedule=None,
        start_date=logical_date,
        tags=["example"],
    ) as dag:
        t_start = PythonOperator(task_id="task_start", python_callable=task_start, dag=dag)
        sec_1 = section_a(t_start.output)
        t_end = PythonOperator(task_id="task_end", python_callable=task_end, dag=dag)
        sec_1.set_downstream(t_end)

    # Testing Tasks in Dag
    assert set(dag.task_group.children.keys()) == {"section_1", "task_start", "task_end"}
    assert set(dag.task_group.children["section_1"].children.keys()) == {
        "section_1.task_2",
        "section_1.task_3",
        "section_1.task_1",
    }

    # Testing Tasks downstream
    assert dag.task_dict["task_start"].downstream_task_ids == {"section_1.task_1"}
    assert dag.task_dict["section_1.task_3"].downstream_task_ids == {"task_end"}

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

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.taskgroup import TaskGroup


class TestTaskGroup:
    @pytest.mark.parametrize(
        "group_id, exc_type, exc_value",
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

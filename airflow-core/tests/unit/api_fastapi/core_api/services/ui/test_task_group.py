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

import pytest

from airflow.api_fastapi.core_api.services.ui.task_group import resolve_task_group_pattern_to_task_ids
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"


@pytest.fixture(autouse=True)
def _clear_db():
    clear_db_runs()
    yield
    clear_db_runs()


class TestResolveTaskGroupPatternToTaskIds:
    """Test resolve_task_group_pattern_to_task_ids function."""

    @pytest.mark.parametrize(
        "dag, pattern, expected_result",
        [
            pytest.param(None, "any_pattern", None, id="no_dag"),
            pytest.param(object(), "any_pattern", None, id="dag_without_task_group"),
        ],
    )
    def test_invalid_inputs(self, dag, pattern, expected_result):
        """Test function with invalid DAG inputs."""
        result = resolve_task_group_pattern_to_task_ids(dag, pattern)
        assert result == expected_result

    @pytest.mark.parametrize(
        "group_id, tasks_in_group, expected_task_ids",
        [
            pytest.param(
                "simple_group",
                ["task_1", "task_2"],
                {"simple_group.task_1", "simple_group.task_2"},
                id="simple_group",
            ),
            pytest.param(
                "empty_group",
                [],
                set(),
                id="empty_group",
            ),
        ],
    )
    def test_simple_task_groups(self, dag_maker, session, group_id, tasks_in_group, expected_task_ids):
        """Test resolving patterns for simple task groups."""
        with dag_maker(dag_id=DAG_ID, session=session) as dag:
            with TaskGroup(group_id):
                for task_id in tasks_in_group:
                    BashOperator(task_id=task_id, bash_command=f"echo {task_id}")

        result = resolve_task_group_pattern_to_task_ids(dag, group_id)

        if expected_task_ids:
            assert result is not None
            assert set(result) == expected_task_ids
        else:
            assert result == []

    def test_nested_task_groups(self, dag_maker, session):
        """Test resolving patterns for nested task groups."""
        with dag_maker(dag_id=DAG_ID, session=session) as dag:
            with TaskGroup("parent_group"):
                BashOperator(task_id="parent_task", bash_command="echo parent")
                with TaskGroup("child_group"):
                    BashOperator(task_id="child_task_1", bash_command="echo child1")
                    BashOperator(task_id="child_task_2", bash_command="echo child2")

        # Test parent group includes all nested tasks
        result = resolve_task_group_pattern_to_task_ids(dag, "parent_group")
        assert result is not None
        assert len(result) == 3
        expected_parent = {
            "parent_group.parent_task",
            "parent_group.child_group.child_task_1",
            "parent_group.child_group.child_task_2",
        }
        assert set(result) == expected_parent

        # Test child group only includes child tasks
        result = resolve_task_group_pattern_to_task_ids(dag, "parent_group.child_group")
        assert result is not None
        assert len(result) == 2
        expected_child = {
            "parent_group.child_group.child_task_1",
            "parent_group.child_group.child_task_2",
        }
        assert set(result) == expected_child

    def test_nonexistent_group(self, dag_maker, session):
        """Test resolving pattern for non-existent task group."""
        with dag_maker(dag_id=DAG_ID, session=session) as dag:
            BashOperator(task_id="standalone_task", bash_command="echo standalone")

        result = resolve_task_group_pattern_to_task_ids(dag, "nonexistent_group")
        assert result is None

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

from airflow.api_fastapi.core_api.services.ui.grid import _merge_node_dicts


def test_merge_node_dicts_with_none_new_list():
    """Test merging with None new list doesn't crash.

    Regression test for https://github.com/apache/airflow/issues/61208
    When a TaskGroup is converted to a task, new can be None for some runs.
    """
    current = [{"id": "task1", "label": "Task 1"}]
    new = None

    _merge_node_dicts(current, new)

    assert len(current) == 1
    assert current[0]["id"] == "task1"


def test_merge_node_dicts_preserves_taskgroup_structure():
    """Test TaskGroup structure is preserved when converting to task."""
    current = [
        {
            "id": "task_a",
            "label": "Task A",
            "children": [
                {"id": "task_a.subtask1", "label": "Subtask 1"},
            ],
        }
    ]
    new = [{"id": "task_a", "label": "Task A", "children": None}]

    _merge_node_dicts(current, new)

    # Current structure (TaskGroup) is preserved
    assert len(current) == 1
    assert current[0]["id"] == "task_a"
    assert current[0]["children"] is not None
    assert len(current[0]["children"]) == 1


def test_merge_node_dicts_merges_nested_children():
    """Test merging nodes with nested children."""
    current = [
        {
            "id": "group1",
            "label": "Group 1",
            "children": [
                {"id": "group1.task1", "label": "Task 1"},
            ],
        }
    ]
    new = [
        {
            "id": "group1",
            "label": "Group 1",
            "children": [
                {"id": "group1.task1", "label": "Task 1"},
                {"id": "group1.task2", "label": "Task 2"},
            ],
        }
    ]

    _merge_node_dicts(current, new)

    assert len(current) == 1
    assert current[0]["id"] == "group1"
    assert len(current[0]["children"]) == 2


def test_merge_node_dicts_merges_children_and_appends_new_nodes():
    current = [
        {
            "id": "group",
            "label": "group",
            "children": [{"id": "group.task_a", "label": "task_a"}],
        },
        {"id": "task", "label": "task"},
    ]
    new = [
        {
            "id": "group",
            "label": "group",
            "children": [{"id": "group.task_b", "label": "task_b"}],
        },
        {"id": "new_task", "label": "new_task"},
    ]

    _merge_node_dicts(current, new)

    assert [node["id"] for node in current] == ["group", "task", "new_task"]
    group_children = {child["id"] for child in current[0]["children"]}
    assert group_children == {"group.task_a", "group.task_b"}


def test_merge_node_dicts_preserves_existing_non_group_node_shape():
    current = [{"id": "task", "label": "task"}]
    new = [{"id": "task", "label": "task", "children": [{"id": "task.subtask", "label": "subtask"}]}]

    _merge_node_dicts(current, new)

    assert current == [{"id": "task", "label": "task"}]


def test_merge_node_dicts_large_merge_keeps_unique_nodes():
    current = [{"id": f"group_{i}", "children": [{"id": f"group_{i}.old_task"}]} for i in range(400)]
    new = [{"id": f"group_{i}", "children": [{"id": f"group_{i}.new_task"}]} for i in range(400)]
    new.extend({"id": f"new_task_{i}"} for i in range(400))

    _merge_node_dicts(current, new)

    assert len(current) == 800
    assert {child["id"] for child in current[0]["children"]} == {
        "group_0.old_task",
        "group_0.new_task",
    }
    assert {child["id"] for child in current[-401]["children"]} == {
        "group_399.old_task",
        "group_399.new_task",
    }

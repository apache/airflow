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
"""Task group utilities for UI API services."""

from __future__ import annotations

from collections.abc import Callable
from functools import cache
from operator import methodcaller

from airflow.configuration import conf
from airflow.models.mappedoperator import MappedOperator, is_mapped
from airflow.serialization.serialized_objects import SerializedBaseOperator


@cache
def get_task_group_children_getter() -> Callable:
    """Get the Task Group Children Getter for the DAG."""
    sort_order = conf.get("api", "grid_view_sorting_order")
    if sort_order == "topological":
        return methodcaller("topological_sort")
    return methodcaller("hierarchical_alphabetical_sort")


def task_group_to_dict(task_item_or_group, parent_group_is_mapped=False):
    """Create a nested dict representation of this TaskGroup and its children used to construct the Graph."""
    if isinstance(task := task_item_or_group, (SerializedBaseOperator, MappedOperator)):
        # we explicitly want the short task ID here, not the full doted notation if in a group
        task_display_name = task.task_display_name if task.task_display_name != task.task_id else task.label
        node_operator = {
            "id": task.task_id,
            "label": task_display_name,
            "operator": task.operator_name,
            "type": "task",
        }
        if task.is_setup:
            node_operator["setup_teardown_type"] = "setup"
        elif task.is_teardown:
            node_operator["setup_teardown_type"] = "teardown"
        if is_mapped(task) or parent_group_is_mapped:
            node_operator["is_mapped"] = True
        return node_operator

    task_group = task_item_or_group
    mapped = is_mapped(task_group)
    children = [
        task_group_to_dict(child, parent_group_is_mapped=parent_group_is_mapped or mapped)
        for child in get_task_group_children_getter()(task_group)
    ]

    if task_group.upstream_group_ids or task_group.upstream_task_ids:
        # This is the join node used to reduce the number of edges between two TaskGroup.
        children.append({"id": task_group.upstream_join_id, "label": "", "type": "join"})

    if task_group.downstream_group_ids or task_group.downstream_task_ids:
        # This is the join node used to reduce the number of edges between two TaskGroup.
        children.append({"id": task_group.downstream_join_id, "label": "", "type": "join"})

    return {
        "id": task_group.group_id,
        "label": task_group.group_display_name or task_group.label,
        "tooltip": task_group.tooltip,
        "is_mapped": mapped,
        "children": children,
        "type": "task",
    }


def task_group_to_dict_grid(task_item_or_group, parent_group_is_mapped=False):
    """Create a nested dict representation of this TaskGroup and its children used to construct the Grid."""
    if isinstance(task := task_item_or_group, (MappedOperator, SerializedBaseOperator)):
        mapped = None
        if parent_group_is_mapped or is_mapped(task):
            mapped = True
        setup_teardown_type = None
        if task.is_setup is True:
            setup_teardown_type = "setup"
        elif task.is_teardown is True:
            setup_teardown_type = "teardown"
        # we explicitly want the short task ID here, not the full doted notation if in a group
        task_display_name = task.task_display_name if task.task_display_name != task.task_id else task.label
        return {
            "id": task.task_id,
            "label": task_display_name,
            "is_mapped": mapped,
            "children": None,
            "setup_teardown_type": setup_teardown_type,
        }

    task_group = task_item_or_group
    task_group_sort = get_task_group_children_getter()
    mapped = is_mapped(task_group)
    children = [
        task_group_to_dict_grid(x, parent_group_is_mapped=parent_group_is_mapped or mapped)
        for x in task_group_sort(task_group)
    ]

    return {
        "id": task_group.group_id,
        "label": task_group.group_display_name or task_group.label,
        "is_mapped": mapped or None,
        "children": children or None,
    }

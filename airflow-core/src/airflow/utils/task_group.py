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
"""A collection of closely related tasks on the same DAG that should be grouped together visually."""

from __future__ import annotations

from typing import TYPE_CHECKING

import airflow.sdk.definitions.taskgroup

if TYPE_CHECKING:
    from airflow.typing_compat import TypeAlias

TaskGroup: TypeAlias = airflow.sdk.definitions.taskgroup.TaskGroup
MappedTaskGroup: TypeAlias = airflow.sdk.definitions.taskgroup.MappedTaskGroup


def task_group_to_dict(task_item_or_group, parent_group_is_mapped=False):
    """Create a nested dict representation of this TaskGroup and its children used to construct the Graph."""
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    if isinstance(task := task_item_or_group, AbstractOperator):
        setup_teardown_type = {}
        is_mapped = {}
        node_type = {"type": "task"}
        node_operator = {}
        if task.is_setup is True:
            setup_teardown_type["setup_teardown_type"] = "setup"
        elif task.is_teardown is True:
            setup_teardown_type["setup_teardown_type"] = "teardown"
        if isinstance(task, MappedOperator) or parent_group_is_mapped:
            is_mapped["is_mapped"] = True
        if isinstance(task, BaseOperator) or isinstance(task, MappedOperator):
            node_operator["operator"] = task.operator_name

        return {
            "id": task.task_id,
            "label": task.label,
            **is_mapped,
            **setup_teardown_type,
            **node_type,
            **node_operator,
        }

    task_group = task_item_or_group
    is_mapped = isinstance(task_group, MappedTaskGroup)
    children = [
        task_group_to_dict(child, parent_group_is_mapped=parent_group_is_mapped or is_mapped)
        for child in sorted(task_group.children.values(), key=lambda t: t.label)
    ]

    if task_group.upstream_group_ids or task_group.upstream_task_ids:
        # This is the join node used to reduce the number of edges between two TaskGroup.
        children.append({"id": task_group.upstream_join_id, "label": "", "type": "join"})

    if task_group.downstream_group_ids or task_group.downstream_task_ids:
        # This is the join node used to reduce the number of edges between two TaskGroup.
        children.append({"id": task_group.downstream_join_id, "label": "", "type": "join"})

    return {
        "id": task_group.group_id,
        "label": task_group.label,
        "tooltip": task_group.tooltip,
        "is_mapped": is_mapped,
        "children": children,
        "type": "task",
    }

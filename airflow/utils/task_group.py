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

import functools
import operator
from typing import TYPE_CHECKING, Any, Iterator

import methodtools

import airflow.sdk.definitions.contextmanager
import airflow.sdk.definitions.taskgroup

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dag import DAG
    from airflow.models.expandinput import ExpandInput
    from airflow.models.operator import Operator
    from airflow.typing_compat import TypeAlias

TaskGroup: TypeAlias = airflow.sdk.definitions.taskgroup.TaskGroup


class MappedTaskGroup(airflow.sdk.definitions.taskgroup.MappedTaskGroup):
    """
    A mapped task group.

    This doesn't really do anything special, just holds some additional metadata
    for expansion later.

    Don't instantiate this class directly; call *expand* or *expand_kwargs* on
    a ``@task_group`` function instead.
    """

    def iter_mapped_dependencies(self) -> Iterator[Operator]:
        """Upstream dependencies that provide XComs used by this mapped task group."""
        from airflow.models.xcom_arg import XComArg

        for op, _ in XComArg.iter_xcom_references(self._expand_input):
            yield op

    def get_mapped_ti_count(self, run_id: str, *, session: Session) -> int:
        """
        Return the number of instances a task in this group should be mapped to at run time.

        This considers both literal and non-literal mapped arguments, and the
        result is therefore available when all depended tasks have finished. The
        return value should be identical to ``parse_time_mapped_ti_count`` if
        all mapped arguments are literal.

        If this group is inside mapped task groups, all the nested counts are
        multiplied and accounted.

        :meta private:

        :raise NotFullyPopulated: If upstream tasks are not all complete yet.
        :return: Total number of mapped TIs this task should have.
        """
        groups = self.iter_mapped_task_groups()
        return functools.reduce(
            operator.mul,
            (g._expand_input.get_total_map_length(run_id, session=session) for g in groups),
        )


class TaskGroupContext(airflow.sdk.definitions.contextmanager.TaskGroupContext, share_parent_context=True):
    """TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager."""

    @classmethod
    def push_context_managed_task_group(cls, task_group: TaskGroup):
        """Push a TaskGroup into the list of managed TaskGroups."""
        return cls.push(task_group)

    @classmethod
    def pop_context_managed_task_group(cls) -> TaskGroup | None:
        """Pops the last TaskGroup from the list of managed TaskGroups and update the current TaskGroup."""
        return cls.pop()

    @classmethod
    def get_current_task_group(cls, dag: DAG | None) -> TaskGroup | None:
        """Get the current TaskGroup."""
        return cls.get_current(dag)


def task_group_to_dict(task_item_or_group):
    """Create a nested dict representation of this TaskGroup and its children used to construct the Graph."""
    from airflow.models.abstractoperator import AbstractOperator
    from airflow.models.mappedoperator import MappedOperator

    if isinstance(task := task_item_or_group, AbstractOperator):
        setup_teardown_type = {}
        is_mapped = {}
        if task.is_setup is True:
            setup_teardown_type["setupTeardownType"] = "setup"
        elif task.is_teardown is True:
            setup_teardown_type["setupTeardownType"] = "teardown"
        if isinstance(task, MappedOperator):
            is_mapped["isMapped"] = True
        return {
            "id": task.task_id,
            "value": {
                "label": task.label,
                "labelStyle": f"fill:{task.ui_fgcolor};",
                "style": f"fill:{task.ui_color};",
                "rx": 5,
                "ry": 5,
                **is_mapped,
                **setup_teardown_type,
            },
        }
    task_group = task_item_or_group
    is_mapped = isinstance(task_group, MappedTaskGroup)
    children = [
        task_group_to_dict(child) for child in sorted(task_group.children.values(), key=lambda t: t.label)
    ]

    if task_group.upstream_group_ids or task_group.upstream_task_ids:
        children.append(
            {
                "id": task_group.upstream_join_id,
                "value": {
                    "label": "",
                    "labelStyle": f"fill:{task_group.ui_fgcolor};",
                    "style": f"fill:{task_group.ui_color};",
                    "shape": "circle",
                },
            }
        )

    if task_group.downstream_group_ids or task_group.downstream_task_ids:
        # This is the join node used to reduce the number of edges between two TaskGroup.
        children.append(
            {
                "id": task_group.downstream_join_id,
                "value": {
                    "label": "",
                    "labelStyle": f"fill:{task_group.ui_fgcolor};",
                    "style": f"fill:{task_group.ui_color};",
                    "shape": "circle",
                },
            }
        )

    return {
        "id": task_group.group_id,
        "value": {
            "label": task_group.label,
            "labelStyle": f"fill:{task_group.ui_fgcolor};",
            "style": f"fill:{task_group.ui_color}",
            "rx": 5,
            "ry": 5,
            "clusterLabelPos": "top",
            "tooltip": task_group.tooltip,
            "isMapped": is_mapped,
        },
        "children": children,
    }

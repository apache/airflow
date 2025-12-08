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

import re
from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

from airflow.sdk._shared.definitions.node import GenericDAGNode
from airflow.sdk.definitions._internal.mixins import DependencyMixin

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.edges import EdgeModifier
    from airflow.sdk.definitions.taskgroup import TaskGroup  # noqa: F401
    from airflow.sdk.types import Logger, Operator  # noqa: F401
    from airflow.serialization.enums import DagAttributeTypes


KEY_REGEX = re.compile(r"^[\w.-]+$")
GROUP_KEY_REGEX = re.compile(r"^[\w-]+$")
CAMELCASE_TO_SNAKE_CASE_REGEX = re.compile(r"(?!^)([A-Z]+)")


def validate_key(k: str, max_length: int = 250):
    """Validate value used as a key."""
    if not isinstance(k, str):
        raise TypeError(f"The key has to be a string and is {type(k)}:{k}")
    if (length := len(k)) > max_length:
        raise ValueError(f"The key has to be less than {max_length} characters, not {length}")
    if not KEY_REGEX.match(k):
        raise ValueError(
            f"The key {k!r} has to be made of alphanumeric characters, dashes, "
            f"dots, and underscores exclusively"
        )


def validate_group_key(k: str, max_length: int = 200):
    """Validate value used as a group key."""
    if not isinstance(k, str):
        raise TypeError(f"The key has to be a string and is {type(k)}:{k}")
    if (length := len(k)) > max_length:
        raise ValueError(f"The key has to be less than {max_length} characters, not {length}")
    if not GROUP_KEY_REGEX.match(k):
        raise ValueError(
            f"The key {k!r} has to be made of alphanumeric characters, dashes, and underscores exclusively"
        )


class DAGNode(GenericDAGNode["DAG", "Operator", "TaskGroup", "Logger"], DependencyMixin, metaclass=ABCMeta):
    """
    A base class for a node in the graph of a workflow.

    A node may be an Operator or a Task Group, either mapped or unmapped.
    """

    start_date: datetime | None
    end_date: datetime | None

    @property
    @abstractmethod
    def roots(self) -> Sequence[DAGNode]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def leaves(self) -> Sequence[DAGNode]:
        raise NotImplementedError()

    def _set_relatives(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        upstream: bool = False,
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Set relatives for the task or task list."""
        from airflow.sdk.bases.operator import BaseOperator
        from airflow.sdk.definitions.mappedoperator import MappedOperator

        if not isinstance(task_or_task_list, Sequence):
            task_or_task_list = [task_or_task_list]

        task_list: list[BaseOperator | MappedOperator] = []
        for task_object in task_or_task_list:
            task_object.update_relative(self, not upstream, edge_modifier=edge_modifier)
            relatives = task_object.leaves if upstream else task_object.roots
            for task in relatives:
                if not isinstance(task, (BaseOperator, MappedOperator)):
                    raise TypeError(
                        f"Relationships can only be set between Operators; received {task.__class__.__name__}"
                    )
                task_list.append(task)

        # relationships can only be set if the tasks share a single Dag. Tasks
        # without a Dag are assigned to that Dag.
        dags: set[DAG] = {task.dag for task in [*self.roots, *task_list] if task.has_dag() and task.dag}

        if len(dags) > 1:
            raise RuntimeError(f"Tried to set relationships between tasks in more than one Dag: {dags}")
        if len(dags) == 1:
            dag = dags.pop()
        else:
            raise ValueError(
                "Tried to create relationships between tasks that don't have Dags yet. "
                f"Set the Dag for at least one task and try again: {[self, *task_list]}"
            )

        if not self.has_dag():
            # If this task does not yet have a Dag, add it to the same Dag as the other task.
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                # If the other task does not yet have a Dag, add it to the same Dag as this task and
                dag.add_task(task)  # type: ignore[arg-type]
            if upstream:
                task.downstream_task_ids.add(self.node_id)
                self.upstream_task_ids.add(task.node_id)
                if edge_modifier:
                    edge_modifier.add_edge_info(dag, task.node_id, self.node_id)
            else:
                self.downstream_task_ids.add(task.node_id)
                task.upstream_task_ids.add(self.node_id)
                if edge_modifier:
                    edge_modifier.add_edge_info(dag, self.node_id, task.node_id)

    def set_downstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Set a node (or nodes) to be directly downstream from the current node."""
        self._set_relatives(task_or_task_list, upstream=False, edge_modifier=edge_modifier)

    def set_upstream(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """Set a node (or nodes) to be directly upstream from the current node."""
        self._set_relatives(task_or_task_list, upstream=True, edge_modifier=edge_modifier)

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize a task group's content; used by TaskGroupSerialization."""
        raise NotImplementedError()

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

import logging
import re
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

import methodtools
import re2

from airflow.exceptions import AirflowException
from airflow.sdk.definitions.mixins import DependencyMixin

if TYPE_CHECKING:
    from airflow.models.operator import Operator
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.edges import EdgeModifier
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.types import Logger
    from airflow.serialization.enums import DagAttributeTypes


KEY_REGEX = re2.compile(r"^[\w.-]+$")
GROUP_KEY_REGEX = re2.compile(r"^[\w-]+$")
CAMELCASE_TO_SNAKE_CASE_REGEX = re.compile(r"(?!^)([A-Z]+)")


def validate_key(k: str, max_length: int = 250):
    """Validate value used as a key."""
    if not isinstance(k, str):
        raise TypeError(f"The key has to be a string and is {type(k)}:{k}")
    if len(k) > max_length:
        raise ValueError(f"The key has to be less than {max_length} characters")
    if not KEY_REGEX.match(k):
        raise ValueError(
            f"The key {k!r} has to be made of alphanumeric characters, dashes, "
            + "dots and underscores exclusively"
        )


class DAGNode(DependencyMixin, metaclass=ABCMeta):
    """
    A base class for a node in the graph of a workflow.

    A node may be an Operator or a Task Group, either mapped or unmapped.
    """

    dag: DAG | None
    task_group: TaskGroup | None
    """The task_group that contains this node"""
    start_date: datetime | None
    end_date: datetime | None
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]

    def __init__(self):
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        super().__init__()

    @property
    @abstractmethod
    def node_id(self) -> str:
        raise NotImplementedError()

    @property
    def label(self) -> str | None:
        tg = self.task_group
        if tg and tg.node_id and tg.prefix_group_id:
            # "task_group_id.task_id" -> "task_id"
            return self.node_id[len(tg.node_id) + 1 :]
        return self.node_id

    def has_dag(self) -> bool:
        return self.dag is not None

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc/meaningless ID."""
        if self.dag:
            return self.dag.dag_id
        return "_in_memory_dag_"

    @property
    @methodtools.lru_cache()
    def log(self) -> Logger:
        typ = type(self)
        name = f"{typ.__module__}.{typ.__qualname__}"
        return logging.getLogger(name)

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
        from airflow.models.mappedoperator import MappedOperator
        from airflow.sdk.definitions.baseoperator import BaseOperator

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

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        dags: set[DAG] = {task.dag for task in [*self.roots, *task_list] if task.has_dag() and task.dag}

        if len(dags) > 1:
            raise AirflowException(f"Tried to set relationships between tasks in more than one DAG: {dags}")
        elif len(dags) == 1:
            dag = dags.pop()
        else:
            raise ValueError(
                "Tried to create relationships between tasks that don't have DAGs yet. "
                + f"Set the DAG for at least one task and try again: {[self, *task_list]}"
            )

        if not self.has_dag():
            # If this task does not yet have a dag, add it to the same dag as the other task.
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                # If the other task does not yet have a dag, add it to the same dag as this task and
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

    @property
    def downstream_list(self) -> Iterable[Operator]:
        """List of nodes directly downstream."""
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a DAG yet")
        return [self.dag.get_task(tid) for tid in self.downstream_task_ids]

    @property
    def upstream_list(self) -> Iterable[Operator]:
        """List of nodes directly upstream."""
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a DAG yet")
        return [self.dag.get_task(tid) for tid in self.upstream_task_ids]

    def get_direct_relative_ids(self, upstream: bool = False) -> set[str]:
        """Get set of the direct relative ids to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        else:
            return self.downstream_task_ids

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize a task group's content; used by TaskGroupSerialization."""
        raise NotImplementedError()

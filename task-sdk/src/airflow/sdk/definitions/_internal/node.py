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
from collections.abc import Collection, Iterable, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

import structlog

from airflow.sdk.definitions._internal.mixins import DependencyMixin

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.edges import EdgeModifier
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.types import Logger, Operator
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

    _log_config_logger_name: str | None = None
    _logger_name: str | None = None
    _cached_logger: Logger | None = None

    def __init__(self):
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        super().__init__()

    def get_dag(self) -> DAG | None:
        return self.dag

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
    def log(self) -> Logger:
        """
        Get a logger for this node.

        The logger name is determined by:
        1. Using _logger_name if provided
        2. Otherwise, using the class's module and qualified name
        3. Prefixing with _log_config_logger_name if set
        """
        if self._cached_logger is not None:
            return self._cached_logger

        typ = type(self)

        logger_name: str = (
            self._logger_name if self._logger_name is not None else f"{typ.__module__}.{typ.__qualname__}"
        )

        if self._log_config_logger_name:
            logger_name = (
                f"{self._log_config_logger_name}.{logger_name}"
                if logger_name
                else self._log_config_logger_name
            )

        self._cached_logger = structlog.get_logger(logger_name)
        return self._cached_logger

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

    @property
    def downstream_list(self) -> Iterable[Operator]:
        """List of nodes directly downstream."""
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a Dag yet")
        return [self.dag.get_task(tid) for tid in self.downstream_task_ids]

    @property
    def upstream_list(self) -> Iterable[Operator]:
        """List of nodes directly upstream."""
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a Dag yet")
        return [self.dag.get_task(tid) for tid in self.upstream_task_ids]

    def get_direct_relative_ids(self, upstream: bool = False) -> set[str]:
        """Get set of the direct relative ids to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        return self.downstream_task_ids

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[Operator]:
        """Get list of the direct relatives to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_list
        return self.downstream_list

    def get_flat_relative_ids(self, *, upstream: bool = False) -> set[str]:
        """
        Get a flat set of relative IDs, upstream or downstream.

        Will recurse each relative found in the direction specified.

        :param upstream: Whether to look for upstream or downstream relatives.
        """
        dag = self.get_dag()
        if not dag:
            return set()

        relatives: set[str] = set()

        # This is intentionally implemented as a loop, instead of calling
        # get_direct_relative_ids() recursively, since Python has significant
        # limitation on stack level, and a recursive implementation can blow up
        # if a DAG contains very long routes.
        task_ids_to_trace = self.get_direct_relative_ids(upstream)
        while task_ids_to_trace:
            task_ids_to_trace_next: set[str] = set()
            for task_id in task_ids_to_trace:
                if task_id in relatives:
                    continue
                task_ids_to_trace_next.update(dag.task_dict[task_id].get_direct_relative_ids(upstream))
                relatives.add(task_id)
            task_ids_to_trace = task_ids_to_trace_next

        return relatives

    def get_flat_relatives(self, upstream: bool = False) -> Collection[Operator]:
        """Get a flat list of relatives, either upstream or downstream."""
        dag = self.get_dag()
        if not dag:
            return set()
        return [dag.task_dict[task_id] for task_id in self.get_flat_relative_ids(upstream=upstream)]

    def get_upstreams_follow_setups(self) -> Iterable[Operator]:
        """All upstreams and, for each upstream setup, its respective teardowns."""
        for task in self.get_flat_relatives(upstream=True):
            yield task
            if task.is_setup:
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups_and_teardowns(self) -> Iterable[Operator]:
        """
        Only *relevant* upstream setups and their teardowns.

        This method is meant to be used when we are clearing the task (non-upstream) and we need
        to add in the *relevant* setups and their teardowns.

        Relevant in this case means, the setup has a teardown that is downstream of ``self``,
        or the setup has no teardowns.
        """
        downstream_teardown_ids = {
            x.task_id for x in self.get_flat_relatives(upstream=False) if x.is_teardown
        }
        for task in self.get_flat_relatives(upstream=True):
            if not task.is_setup:
                continue
            has_no_teardowns = not any(x.is_teardown for x in task.downstream_list)
            # if task has no teardowns or has teardowns downstream of self
            if has_no_teardowns or task.downstream_task_ids.intersection(downstream_teardown_ids):
                yield task
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups(self) -> Iterable[Operator]:
        """
        Return relevant upstream setups.

        This method is meant to be used when we are checking task dependencies where we need
        to wait for all the upstream setups to complete before we can run the task.
        """
        for task in self.get_upstreams_only_setups_and_teardowns():
            if task.is_setup:
                yield task

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize a task group's content; used by TaskGroupSerialization."""
        raise NotImplementedError()

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

from typing import TYPE_CHECKING, Generic, TypeVar

import structlog

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from ..logging.types import Logger

Dag = TypeVar("Dag")
Task = TypeVar("Task")
TaskGroup = TypeVar("TaskGroup")


class GenericDAGNode(Generic[Dag, Task, TaskGroup]):
    """
    Generic class for a node in the graph of a workflow.

    A node may be an operator or task group, either mapped or unmapped.
    """

    dag: Dag | None
    task_group: TaskGroup | None
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]

    _log_config_logger_name: str | None = None
    _logger_name: str | None = None
    _cached_logger: Logger | None = None

    def __init__(self):
        super().__init__()
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()

    @property
    def log(self) -> Logger:
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
    def dag_id(self) -> str:
        if self.dag:
            return self.dag.dag_id
        return "_in_memory_dag_"

    @property
    def node_id(self) -> str:
        raise NotImplementedError()

    @property
    def label(self) -> str | None:
        tg = self.task_group
        if tg and tg.node_id and tg.prefix_group_id:
            # "task_group_id.task_id" -> "task_id"
            return self.node_id[len(tg.node_id) + 1 :]
        return self.node_id

    @property
    def upstream_list(self) -> Iterable[Task]:
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a Dag yet")
        return [self.dag.get_task(tid) for tid in self.upstream_task_ids]

    @property
    def downstream_list(self) -> Iterable[Task]:
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a Dag yet")
        return [self.dag.get_task(tid) for tid in self.downstream_task_ids]

    def has_dag(self) -> bool:
        return self.dag is not None

    def get_dag(self) -> Dag | None:
        return self.dag

    def get_direct_relative_ids(self, upstream: bool = False) -> set[str]:
        """Get set of the direct relative ids to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        return self.downstream_task_ids

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[Task]:
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

    def get_flat_relatives(self, upstream: bool = False) -> Collection[Task]:
        """Get a flat list of relatives, either upstream or downstream."""
        dag = self.get_dag()
        if not dag:
            return set()
        return [dag.task_dict[task_id] for task_id in self.get_flat_relative_ids(upstream=upstream)]

    def get_upstreams_follow_setups(self) -> Iterable[Task]:
        """All upstreams and, for each upstream setup, its respective teardowns."""
        for task in self.get_flat_relatives(upstream=True):
            yield task
            if task.is_setup:
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups_and_teardowns(self) -> Iterable[Task]:
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

    def get_upstreams_only_setups(self) -> Iterable[Task]:
        """
        Return relevant upstream setups.

        This method is meant to be used when we are checking task dependencies where we need
        to wait for all the upstream setups to complete before we can run the task.
        """
        for task in self.get_upstreams_only_setups_and_teardowns():
            if task.is_setup:
                yield task

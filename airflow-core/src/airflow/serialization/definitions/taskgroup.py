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

from __future__ import annotations

import copy
import functools
import operator
import weakref
from typing import TYPE_CHECKING

import attrs
import methodtools

from airflow.serialization.definitions.node import DAGNode

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator
    from typing import Any, ClassVar

    from airflow.models.expandinput import SchedulerExpandInput
    from airflow.serialization.serialized_objects import SerializedDAG, SerializedOperator


@attrs.define(eq=False, hash=False, kw_only=True)
class SerializedTaskGroup(DAGNode):
    """Serialized representation of a TaskGroup used in protected processes."""

    _group_id: str | None = attrs.field(alias="group_id")
    group_display_name: str | None = attrs.field()
    prefix_group_id: bool = attrs.field()
    parent_group: SerializedTaskGroup | None = attrs.field()
    dag: SerializedDAG = attrs.field()
    tooltip: str = attrs.field()
    default_args: dict[str, Any] = attrs.field(factory=dict)

    # TODO: Are these actually useful?
    ui_color: str = attrs.field(default="CornflowerBlue")
    ui_fgcolor: str = attrs.field(default="#000")

    children: dict[str, DAGNode] = attrs.field(factory=dict, init=False)
    upstream_group_ids: set[str | None] = attrs.field(factory=set, init=False)
    downstream_group_ids: set[str | None] = attrs.field(factory=set, init=False)
    upstream_task_ids: set[str] = attrs.field(factory=set, init=False)
    downstream_task_ids: set[str] = attrs.field(factory=set, init=False)

    is_mapped: ClassVar[bool] = False

    def __repr__(self) -> str:
        return f"<SerializedTaskGroup: {self.group_id}>"

    @staticmethod
    def _iter_child(child):
        """Iterate over the children of this TaskGroup."""
        if isinstance(child, SerializedTaskGroup):
            yield from child
        else:
            yield child

    def __iter__(self):
        for child in self.children.values():
            yield from self._iter_child(child)

    @property
    def group_id(self) -> str | None:
        if (
            self._group_id
            and self.parent_group
            and self.parent_group.prefix_group_id
            and self.parent_group._group_id
        ):
            return self.parent_group.child_id(self._group_id)
        return self._group_id

    @property
    def label(self) -> str:
        """group_id excluding parent's group_id used as the node label in UI."""
        return self.group_display_name or self._group_id or ""

    @property
    def node_id(self) -> str:
        return self.group_id or ""

    @property
    def is_root(self) -> bool:
        return not self._group_id

    # TODO (GH-52141): This shouldn't need to be writable after serialization,
    # but DAGNode defines the property as writable.
    @property
    def task_group(self) -> SerializedTaskGroup | None:  # type: ignore[override]
        return self.parent_group

    def child_id(self, label: str) -> str:
        if self.prefix_group_id and (group_id := self.group_id):
            return f"{group_id}.{label}"
        return label

    @property
    def upstream_join_id(self) -> str:
        return f"{self.group_id}.upstream_join_id"

    @property
    def downstream_join_id(self) -> str:
        return f"{self.group_id}.downstream_join_id"

    @property
    def roots(self) -> list[DAGNode]:
        return list(self.get_roots())

    @property
    def leaves(self) -> list[DAGNode]:
        return list(self.get_leaves())

    def get_roots(self) -> Generator[SerializedOperator, None, None]:
        """Return a generator of tasks with no upstream dependencies within the TaskGroup."""
        tasks = list(self)
        ids = {x.task_id for x in tasks}
        for task in tasks:
            if task.upstream_task_ids.isdisjoint(ids):
                yield task

    def get_leaves(self) -> Generator[SerializedOperator, None, None]:
        """Return a generator of tasks with no downstream dependencies within the TaskGroup."""
        tasks = list(self)
        ids = {x.task_id for x in tasks}

        def has_non_teardown_downstream(task, exclude: str):
            for down_task in task.downstream_list:
                if down_task.task_id == exclude:
                    continue
                if down_task.task_id not in ids:
                    continue
                if not down_task.is_teardown:
                    return True
            return False

        def recurse_for_first_non_teardown(task):
            for upstream_task in task.upstream_list:
                if upstream_task.task_id not in ids:
                    # upstream task is not in task group
                    continue
                elif upstream_task.is_teardown:
                    yield from recurse_for_first_non_teardown(upstream_task)
                elif task.is_teardown and upstream_task.is_setup:
                    # don't go through the teardown-to-setup path
                    continue
                # return unless upstream task already has non-teardown downstream in group
                elif not has_non_teardown_downstream(upstream_task, exclude=task.task_id):
                    yield upstream_task

        for task in tasks:
            if task.downstream_task_ids.isdisjoint(ids):
                if not task.is_teardown:
                    yield task
                else:
                    yield from recurse_for_first_non_teardown(task)

    def get_task_group_dict(self) -> dict[str | None, SerializedTaskGroup]:
        """Create a flat dict of group_id: TaskGroup."""

        def build_map(node: DAGNode) -> Generator[tuple[str | None, SerializedTaskGroup]]:
            if not isinstance(node, SerializedTaskGroup):
                return
            yield node.group_id, node
            for child in node.children.values():
                yield from build_map(child)

        return dict(build_map(self))

    def iter_tasks(self) -> Iterator[SerializedOperator]:
        """Return an iterator of the child tasks."""
        from airflow.models.mappedoperator import MappedOperator
        from airflow.serialization.serialized_objects import SerializedBaseOperator

        groups_to_visit = [self]
        while groups_to_visit:
            for child in groups_to_visit.pop(0).children.values():
                if isinstance(child, (MappedOperator, SerializedBaseOperator)):
                    yield child
                elif isinstance(child, SerializedTaskGroup):
                    groups_to_visit.append(child)
                else:
                    raise ValueError(
                        f"Encountered a DAGNode that is not a task or task "
                        f"group: {type(child).__module__}.{type(child)}"
                    )

    def iter_mapped_task_groups(self) -> Iterator[SerializedMappedTaskGroup]:
        """
        Find mapped task groups in the hierarchy.

        Groups are returned from the closest to the outmost. If *self* is a
        mapped task group, it is returned first.
        """
        group: SerializedTaskGroup | None = self
        while group is not None:
            if isinstance(group, SerializedMappedTaskGroup):
                yield group
            group = group.parent_group

    def topological_sort(self) -> list[DAGNode]:
        """
        Sorts children in topographical order.

        A task in the result would come after any of its upstream dependencies.
        """
        # This uses a modified version of Kahn's Topological Sort algorithm to
        # not have to pre-compute the "in-degree" of the nodes.
        graph_unsorted = copy.copy(self.children)
        graph_sorted: list[DAGNode] = []
        if not self.children:
            return graph_sorted
        while graph_unsorted:
            for node in list(graph_unsorted.values()):
                for edge in node.upstream_list:
                    if edge.node_id in graph_unsorted:
                        break
                    # Check for task's group is a child (or grand child) of this TG,
                    tg = edge.task_group
                    while tg:
                        if tg.node_id in graph_unsorted:
                            break
                        tg = tg.parent_group

                    if tg:
                        # We are already going to visit that TG
                        break
                else:
                    del graph_unsorted[node.node_id]
                    graph_sorted.append(node)
        return graph_sorted

    def add(self, node: DAGNode) -> DAGNode:
        # Set the TG first, as setting it might change the return value of node_id!
        node.task_group = weakref.proxy(self)
        if isinstance(node, SerializedTaskGroup):
            if self.dag:
                node.dag = self.dag
        self.children[node.node_id] = node
        return node


@attrs.define(kw_only=True, repr=False)
class SerializedMappedTaskGroup(SerializedTaskGroup):
    """Serialized representation of a MappedTaskGroup used in protected processes."""

    _expand_input: SchedulerExpandInput = attrs.field(alias="expand_input")

    is_mapped: ClassVar[bool] = True

    def __repr__(self) -> str:
        return f"<SerializedMappedTaskGroup: {self.group_id}>"

    @methodtools.lru_cache(maxsize=None)
    def get_parse_time_mapped_ti_count(self) -> int:
        """
        Return the number of instances a task in this group should be mapped to.

        This only considers literal mapped arguments, and would return *None*
        when any non-literal values are used for mapping.

        If this group is inside mapped task groups, all the nested counts are
        multiplied and accounted.

        :raise NotFullyPopulated: If any non-literal mapped arguments are encountered.
        :return: The total number of mapped instances each task should have.
        """
        return functools.reduce(
            operator.mul,
            (g._expand_input.get_parse_time_mapped_ti_count() for g in self.iter_mapped_task_groups()),
        )

    def iter_mapped_dependencies(self) -> Iterator[SerializedOperator]:
        """Upstream dependencies that provide XComs used by this mapped task group."""
        from airflow.models.xcom_arg import SchedulerXComArg

        for op, _ in SchedulerXComArg.iter_xcom_references(self._expand_input):
            yield op

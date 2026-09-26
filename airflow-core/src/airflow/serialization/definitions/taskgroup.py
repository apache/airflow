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

import functools
import operator
import weakref
from collections import deque
from typing import TYPE_CHECKING

import attrs
import methodtools

from airflow._shared.dagnode.node import TaskGroupMixin
from airflow.serialization.definitions.node import DAGNode

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator
    from typing import Any, ClassVar

    from airflow.models.expandinput import SchedulerExpandInput
    from airflow.serialization.definitions.dag import SerializedDAG, SerializedOperator


@attrs.define(eq=False, hash=False, kw_only=True)
class SerializedTaskGroup(TaskGroupMixin, DAGNode):
    """Serialized representation of a TaskGroup used in protected processes."""

    _group_id: str | None = attrs.field(alias="group_id")
    group_display_name: str | None = attrs.field()
    prefix_group_id: bool = attrs.field()
    parent_group: SerializedTaskGroup | None = attrs.field()
    dag: SerializedDAG = attrs.field()
    tooltip: str = attrs.field()
    doc_md: str | None = attrs.field(default=None)
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
        from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
        from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator

        groups_to_visit = deque([self])
        while groups_to_visit:
            for child in groups_to_visit.popleft().children.values():
                if isinstance(child, (SerializedMappedOperator, SerializedBaseOperator)):
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

    def hierarchical_alphabetical_sort(self) -> list[DAGNode]:
        """
        Sort children in hierarchical alphabetical order: groups first, then tasks, each alphabetical.

        Mirrors ``TaskGroup.hierarchical_alphabetical_sort`` in task-sdk. This orders one group's
        direct children; the server-side graph/grid builder in
        ``api_fastapi.core_api.services.ui.task_group`` walks the tree and re-applies it at every
        level, so the API response is fully ordered at all nesting levels and the UI renders it as-is.
        """
        return sorted(
            self.children.values(),
            key=lambda node: (not isinstance(node, SerializedTaskGroup), node.node_id),
        )

    def topological_sort(
        self, *, group_dict: dict[str | None, SerializedTaskGroup] | None = None
    ) -> list[DAGNode]:
        """
        Sort children topologically — a task always comes after its upstream dependencies.

        See ``TaskGroup.topological_sort`` in task-sdk for the algorithm. Unlike the task-sdk
        variant, a cycle between siblings does not raise. ``DAG.check_cycle`` rejects task-level
        cycles, but a task with no upstream inside its own group counts as a root of that group,
        so edges routed through tasks outside the group can still make siblings depend on each
        other. ``partial_subset`` can also create such a cycle by dropping a task's in-group
        upstream. Grid and Graph must render these Dags, so the siblings on a cycle are ordered
        as one unit instead (see ``_sort_cyclic_projection``).
        """
        children = self.children
        if not children:
            return []

        nodes = list(children.values())
        n = len(nodes)
        id_to_idx = {nid: i for i, nid in enumerate(children)}
        if group_dict is None:
            group_dict = self.dag.task_group.get_task_group_dict()

        projected: list[tuple[int, ...]] = [()] * n
        nodes_with_back_edge = 0
        for i, child in enumerate(nodes):
            deps = self._project_child_deps(i, child, id_to_idx, group_dict)
            if deps:
                projected[i] = deps
                if any(d > i for d in deps):
                    nodes_with_back_edge += 1

        # The ratio catches dense back-heavy groups; a 32-node absolute cutoff keeps
        # padded reverse-declared runs on the fast path once sweep rescans overtake pass-numbering.
        if nodes_with_back_edge >= 32 or nodes_with_back_edge * 2 > n:
            return self._sort_via_pass_numbering(nodes, projected)
        return self._sweep_projection(nodes, projected)

    def _project_child_deps(
        self,
        child_idx: int,
        child: DAGNode,
        id_to_idx: dict[str, int],
        group_dict: dict[str | None, SerializedTaskGroup],
    ) -> tuple[int, ...]:
        upstream_ids = child._topological_upstream_ids
        if not upstream_ids:
            return ()
        sib_deps: set[int] = set()
        for edge_id in upstream_ids:
            j = id_to_idx.get(edge_id)
            if j is not None:
                if j != child_idx:
                    sib_deps.add(j)
                continue
            tg = group_dict.get(edge_id)
            if tg is None:
                edge = self.dag.get_task(edge_id)
                tg = edge.task_group
            while tg is not None:
                anc_idx = id_to_idx.get(tg.node_id)
                if anc_idx is not None:
                    if anc_idx != child_idx:
                        sib_deps.add(anc_idx)
                    break
                tg = tg.parent_group
        return tuple(sib_deps)

    def _sweep_projection(self, nodes: list[DAGNode], projected: list[tuple[int, ...]]) -> list[DAGNode]:
        n = len(nodes)
        emitted = bytearray(n)
        order: list[DAGNode] = []
        order_append = order.append
        pending: list[int] = []
        pending_append = pending.append
        for i in range(n):
            blocked = False
            for d in projected[i]:
                if not emitted[d]:
                    blocked = True
                    break
            if blocked:
                pending_append(i)
                continue
            emitted[i] = 1
            order_append(nodes[i])
        while pending:
            next_pending: list[int] = []
            next_pending_append = next_pending.append
            for i in pending:
                blocked = False
                for d in projected[i]:
                    if not emitted[d]:
                        blocked = True
                        break
                if blocked:
                    next_pending_append(i)
                    continue
                emitted[i] = 1
                order_append(nodes[i])
            if len(next_pending) == len(pending):
                return self._sort_cyclic_projection(nodes, projected)
            pending = next_pending
        return order

    def _sort_via_pass_numbering(
        self, nodes: list[DAGNode], projected: list[tuple[int, ...]]
    ) -> list[DAGNode]:
        sorted_indices = self._compute_pass_order(projected)
        if len(sorted_indices) != len(nodes):
            return self._sort_cyclic_projection(nodes, projected)
        return [nodes[i] for i in sorted_indices]

    @staticmethod
    def _compute_pass_order(projected: list[tuple[int, ...]]) -> list[int]:
        n = len(projected)
        in_degree = [len(deps) for deps in projected]
        successors: list[list[int]] = [[] for _ in range(n)]
        for i, deps in enumerate(projected):
            for d in deps:
                successors[d].append(i)

        pass_of = [0] * n
        queue: deque[int] = deque(i for i in range(n) if in_degree[i] == 0)
        processed: list[int] = []
        while queue:
            i = queue.popleft()
            my_pass = 1
            for d in projected[i]:
                d_pass = pass_of[d]
                if d < i:
                    if d_pass > my_pass:
                        my_pass = d_pass
                elif d_pass + 1 > my_pass:
                    my_pass = d_pass + 1
            pass_of[i] = my_pass
            processed.append(i)
            for s in successors[i]:
                in_degree[s] -= 1
                if in_degree[s] == 0:
                    queue.append(s)

        # Children on or downstream of a cycle never reach in-degree 0 and are left out.
        return sorted(processed, key=lambda i: (pass_of[i], i))

    def _sort_cyclic_projection(
        self, nodes: list[DAGNode], projected: list[tuple[int, ...]]
    ) -> list[DAGNode]:
        # Each component is one unit placed by its first child; units follow the same pass
        # ordering as the acyclic path, and a unit's children keep insertion order.
        component_of = self._find_projection_components(projected)
        count = max(component_of) + 1
        members: list[list[int]] = [[] for _ in range(count)]
        component_deps: list[set[int]] = [set() for _ in range(count)]
        for i, deps in enumerate(projected):
            c = component_of[i]
            members[c].append(i)
            component_deps[c].update(component_of[d] for d in deps if component_of[d] != c)
        component_order = self._compute_pass_order([tuple(deps) for deps in component_deps])
        return [nodes[i] for c in component_order for i in members[c]]

    @staticmethod
    def _find_projection_components(projected: list[tuple[int, ...]]) -> list[int]:
        """Return each child's strongly connected component, numbered in order of its first child."""
        n = len(projected)
        successors: list[list[int]] = [[] for _ in range(n)]
        for i, deps in enumerate(projected):
            for d in deps:
                successors[d].append(i)

        # Kosaraju: finish order along successors, then collect components along dependencies.
        visited = bytearray(n)
        finish_order: list[int] = []
        for start in range(n):
            if visited[start]:
                continue
            visited[start] = 1
            stack: list[tuple[int, Iterator[int]]] = [(start, iter(successors[start]))]
            while stack:
                node, remaining = stack[-1]
                for s in remaining:
                    if not visited[s]:
                        visited[s] = 1
                        stack.append((s, iter(successors[s])))
                        break
                else:
                    stack.pop()
                    finish_order.append(node)

        root_of = [-1] * n
        for start in reversed(finish_order):
            if root_of[start] != -1:
                continue
            root_of[start] = start
            to_visit = [start]
            while to_visit:
                node = to_visit.pop()
                for d in projected[node]:
                    if root_of[d] == -1:
                        root_of[d] = start
                        to_visit.append(d)

        numbering: dict[int, int] = {}
        return [numbering.setdefault(root, len(numbering)) for root in root_of]

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
        from airflow.serialization.definitions.xcom_arg import SchedulerXComArg

        for op, _ in SchedulerXComArg.iter_xcom_references(self._expand_input):
            yield op

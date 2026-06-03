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
"""A collection of closely related tasks on the same Dag that should be grouped together visually."""

from __future__ import annotations

import copy
import re
import weakref
from collections import deque
from collections.abc import Generator, Iterator, Sequence
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk import TriggerRule
from airflow.sdk.definitions._internal.node import DAGNode, validate_group_key
from airflow.sdk.exceptions import (
    AirflowDagCycleException,
    DuplicateTaskIdFound,
    NodeNotFound,
    TaskAlreadyInTaskGroup,
)

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import DagAttributeTypes
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
    from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput, ListOfDictsExpandInput
    from airflow.sdk.definitions._internal.mixins import DependencyMixin
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.edges import EdgeModifier
    from airflow.sdk.types import Operator


def _default_parent_group() -> TaskGroup | None:
    from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext

    return TaskGroupContext.get_current()


def _parent_used_group_ids(tg: TaskGroup) -> set:
    if tg.parent_group:
        return tg.parent_group.used_group_ids
    return set()


# This could be achieved with `@dag.default` and make this a method, but for some unknown reason when we do
# that it makes Mypy (1.9.0 and 1.13.0 tested) seem to entirely loose track that this is an Attrs class. So
# we've gone with this and moved on with our lives, mypy is to much of a dark beast to battle over this.
def _default_dag(instance: TaskGroup):
    from airflow.sdk.definitions._internal.contextmanager import DagContext

    if (pg := instance.parent_group) is not None:
        return pg.dag
    return DagContext.get_current()


# Mypy does not like a lambda for some reason. An explicit annotated function makes it happy.
def _validate_group_id(instance, attribute, value: str) -> None:
    validate_group_key(value)


def _convert_doc_md(doc_md: str | None) -> str | None:
    """Convert markdown file paths to file contents."""
    if doc_md is None:
        return doc_md

    if doc_md.endswith(".md"):
        try:
            with open(doc_md) as fh:
                return fh.read()
        except FileNotFoundError:
            return doc_md

    return doc_md


@attrs.define(repr=False)
class TaskGroup(DAGNode):
    """
    A collection of tasks.

    When set_downstream() or set_upstream() are called on the TaskGroup, it is applied across
    all tasks within the group if necessary.

    :param group_id: a unique, meaningful id for the TaskGroup. group_id must not conflict
        with group_id of TaskGroup or task_id of tasks in the Dag. Root TaskGroup has group_id
        set to None.
    :param prefix_group_id: If set to True, child task_id and group_id will be prefixed with
        this TaskGroup's group_id. If set to False, child task_id and group_id are not prefixed.
        Default is True.
    :param parent_group: The parent TaskGroup of this TaskGroup. parent_group is set to None
        for the root TaskGroup.
    :param dag: The Dag that this TaskGroup belongs to.
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators,
        will override default_args defined in the Dag level.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :param tooltip: The tooltip of the TaskGroup node when displayed in the UI
    :param doc_md: Markdown documentation for the TaskGroup displayed in the UI
    :param ui_color: The fill color of the TaskGroup node when displayed in the UI
    :param ui_fgcolor: The label color of the TaskGroup node when displayed in the UI
    :param add_suffix_on_collision: If this task group name already exists,
        automatically add `__1` etc suffixes
    :param group_display_name: If set, this will be the display name for the TaskGroup node in the UI.
    """

    _group_id: str | None = attrs.field(
        validator=attrs.validators.optional(_validate_group_id),
        # This is the default behaviour for attrs, but by specifying this it makes IDEs happier
        alias="group_id",
    )
    group_display_name: str = attrs.field(default="", validator=attrs.validators.instance_of(str))
    prefix_group_id: bool = attrs.field(default=True)
    parent_group: TaskGroup | None = attrs.field(factory=_default_parent_group)
    dag: DAG = attrs.field(default=attrs.Factory(_default_dag, takes_self=True))
    default_args: dict[str, Any] = attrs.field(factory=dict, converter=copy.deepcopy)
    tooltip: str = attrs.field(default="", validator=attrs.validators.instance_of(str))
    doc_md: str | None = attrs.field(default=None, converter=_convert_doc_md)
    children: dict[str, DAGNode] = attrs.field(factory=dict, init=False)

    upstream_group_ids: set[str | None] = attrs.field(factory=set, init=False)
    downstream_group_ids: set[str | None] = attrs.field(factory=set, init=False)
    upstream_task_ids: set[str] = attrs.field(factory=set, init=False)
    downstream_task_ids: set[str] = attrs.field(factory=set, init=False)

    used_group_ids: set[str] = attrs.field(
        default=attrs.Factory(_parent_used_group_ids, takes_self=True),
        init=False,
        on_setattr=attrs.setters.frozen,
    )

    ui_color: str = attrs.field(default="CornflowerBlue", validator=attrs.validators.instance_of(str))
    ui_fgcolor: str = attrs.field(default="#000", validator=attrs.validators.instance_of(str))

    add_suffix_on_collision: bool = False

    @dag.validator
    def _validate_dag(self, _attr, dag):
        if not dag:
            raise RuntimeError("TaskGroup can only be used inside a dag")

    def __attrs_post_init__(self):
        # TODO: If attrs supported init only args we could use that here
        # https://github.com/python-attrs/attrs/issues/342
        self._check_for_group_id_collisions(self.add_suffix_on_collision)

        if self._group_id and not self.parent_group and self.dag:
            # Support `tg = TaskGroup(x, dag=dag)`
            self.parent_group = self.dag.task_group

        if self.parent_group:
            self.parent_group.add(self)
            if self.parent_group.default_args:
                self.default_args = {
                    **self.parent_group.default_args,
                    **self.default_args,
                }

        if self._group_id:
            self.used_group_ids.add(self.group_id)
            self.used_group_ids.add(self.downstream_join_id)
            self.used_group_ids.add(self.upstream_join_id)

    def _check_for_group_id_collisions(self, add_suffix_on_collision: bool):
        if self._group_id is None:
            return
        # if given group_id already used assign suffix by incrementing largest used suffix integer
        # Example : task_group ==> task_group__1 -> task_group__2 -> task_group__3
        if self.group_id in self.used_group_ids:
            if not add_suffix_on_collision:
                raise DuplicateTaskIdFound(f"group_id '{self._group_id}' has already been added to the DAG")
            base = re.split(r"__\d+$", self._group_id)[0]
            suffixes = sorted(
                int(re.split(r"^.+__", used_group_id)[1])
                for used_group_id in self.used_group_ids
                if used_group_id is not None and re.match(rf"^{base}__\d+$", used_group_id)
            )
            if not suffixes:
                self._group_id += "__1"
            else:
                self._group_id = f"{base}__{suffixes[-1] + 1}"

    @classmethod
    def create_root(cls, dag: DAG) -> TaskGroup:
        """Create a root TaskGroup with no group_id or parent."""
        return cls(group_id=None, dag=dag, parent_group=None)

    @property
    def node_id(self):
        return self.group_id

    @property
    def is_root(self) -> bool:
        """Returns True if this TaskGroup is the root TaskGroup. Otherwise False."""
        return not self._group_id

    @property
    def task_group(self) -> TaskGroup | None:
        return self.parent_group

    @task_group.setter
    def task_group(self, value: TaskGroup | None):
        self.parent_group = value

    def __iter__(self):
        for child in self.children.values():
            yield from self._iter_child(child)

    @staticmethod
    def _iter_child(child):
        """Iterate over the children of this TaskGroup."""
        if isinstance(child, TaskGroup):
            yield from child
        else:
            yield child

    def add(self, task: DAGNode) -> DAGNode:
        """
        Add a task or TaskGroup to this TaskGroup.

        :meta private:
        """
        from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator
        from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext

        if TaskGroupContext.active:
            if task.task_group and task.task_group != self:
                task.task_group.children.pop(task.node_id, None)
                task.task_group = self
        existing_tg = task.task_group
        if isinstance(task, AbstractOperator) and existing_tg is not None and existing_tg != self:
            raise TaskAlreadyInTaskGroup(task.node_id, existing_tg.node_id, self.node_id)

        # Set the TG first, as setting it might change the return value of node_id!
        task.task_group = weakref.proxy(self)
        key = task.node_id

        if key in self.children:
            node_type = "Task" if hasattr(task, "task_id") else "Task Group"
            raise DuplicateTaskIdFound(f"{node_type} id '{key}' has already been added to the Dag")

        if isinstance(task, TaskGroup):
            if self.dag:
                if task.dag is not None and self.dag is not task.dag:
                    raise ValueError(
                        "Cannot mix TaskGroups from different Dags: %s and %s",
                        self.dag,
                        task.dag,
                    )
                task.dag = self.dag
            if task.children:
                raise ValueError("Cannot add a non-empty TaskGroup")

        self.children[key] = task
        return task

    def _remove(self, task: DAGNode) -> None:
        key = task.node_id

        if key not in self.children:
            raise KeyError(f"Node id {key!r} not part of this task group")

        self.used_group_ids.remove(key)
        del self.children[key]

    @property
    def group_id(self) -> str | None:
        """group_id of this TaskGroup."""
        if (
            self._group_id
            and self.parent_group
            and self.parent_group.prefix_group_id
            and self.parent_group._group_id
        ):
            # defer to parent whether it adds a prefix
            return self.parent_group.child_id(self._group_id)
        return self._group_id

    @property
    def label(self) -> str | None:
        """group_id excluding parent's group_id used as the node label in UI."""
        return self.group_display_name or self._group_id

    def update_relative(
        self,
        other: DependencyMixin,
        upstream: bool = True,
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """
        Override TaskMixin.update_relative.

        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids
        accordingly so that we can reduce the number of edges when displaying Graph view.
        """
        if isinstance(other, TaskGroup):
            # Handles setting relationship between a TaskGroup and another TaskGroup
            if upstream:
                parent, child = (self, other)
                if edge_modifier:
                    edge_modifier.add_edge_info(self.dag, other.downstream_join_id, self.upstream_join_id)
            else:
                parent, child = (other, self)
                if edge_modifier:
                    edge_modifier.add_edge_info(self.dag, self.downstream_join_id, other.upstream_join_id)

            parent.upstream_group_ids.add(child.group_id)
            child.downstream_group_ids.add(parent.group_id)
        else:
            # Handles setting relationship between a TaskGroup and a task
            for task in other.roots:
                if not isinstance(task, DAGNode):
                    raise RuntimeError(
                        "Relationships can only be set between TaskGroup "
                        f"or operators; received {task.__class__.__name__}"
                    )

                # Do not set a relationship between a TaskGroup and a Label's roots
                if self == task:
                    continue

                if upstream:
                    self.upstream_task_ids.add(task.node_id)
                    if edge_modifier:
                        edge_modifier.add_edge_info(self.dag, task.node_id, self.upstream_join_id)
                else:
                    self.downstream_task_ids.add(task.node_id)
                    if edge_modifier:
                        edge_modifier.add_edge_info(self.dag, self.downstream_join_id, task.node_id)

    def _set_relatives(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        upstream: bool = False,
        edge_modifier: EdgeModifier | None = None,
    ) -> None:
        """
        Call set_upstream/set_downstream for all root/leaf tasks within this TaskGroup.

        Update upstream_group_ids/downstream_group_ids/upstream_task_ids/downstream_task_ids.
        """
        if not isinstance(task_or_task_list, Sequence):
            task_or_task_list = [task_or_task_list]

        # Helper function to find leaves from a task list or task group
        def find_leaves(group_or_task) -> list[Any]:
            while group_or_task:
                group_or_task_leaves = list(group_or_task.get_leaves())
                if group_or_task_leaves:
                    return group_or_task_leaves
                if group_or_task.upstream_task_ids:
                    upstream_task_ids_list = list(group_or_task.upstream_task_ids)
                    return [self.dag.get_task(task_id) for task_id in upstream_task_ids_list]
                group_or_task = group_or_task.parent_group
            return []

        # Check if the current TaskGroup is empty
        leaves = find_leaves(self)

        for task_like in task_or_task_list:
            self.update_relative(task_like, upstream, edge_modifier=edge_modifier)

        if upstream:
            for task in self.get_roots():
                task.set_upstream(task_or_task_list)
        else:
            for task in leaves:  # Use the fetched leaves
                task.set_downstream(task_or_task_list)

    def __enter__(self) -> TaskGroup:
        from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext

        TaskGroupContext.push(self)
        return self

    def __exit__(self, _type, _value, _tb):
        from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext

        TaskGroupContext.pop()

    def has_task(self, task: BaseOperator) -> bool:
        """Return True if this TaskGroup or its children TaskGroups contains the given task."""
        if task.task_id in self.children:
            return True

        return any(child.has_task(task) for child in self.children.values() if isinstance(child, TaskGroup))

    @property
    def roots(self) -> list[BaseOperator]:
        """Required by DependencyMixin."""
        return list(self.get_roots())

    @property
    def leaves(self) -> list[BaseOperator]:
        """Required by DependencyMixin."""
        return list(self.get_leaves())

    def get_roots(self) -> Generator[BaseOperator, None, None]:
        """Return a generator of tasks with no upstream dependencies within the TaskGroup."""
        tasks = list(self)
        ids = {x.task_id for x in tasks}
        for task in tasks:
            if task.upstream_task_ids.isdisjoint(ids):
                yield task

    def get_leaves(self) -> Generator[BaseOperator, None, None]:
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

    def child_id(self, label):
        """Prefix label with group_id if prefix_group_id is True. Otherwise return the label as-is."""
        if self.prefix_group_id:
            group_id = self.group_id
            if group_id:
                return f"{group_id}.{label}"

        return label

    @property
    def upstream_join_id(self) -> str:
        """
        Creates a unique ID for upstream dependencies of this TaskGroup.

        If this TaskGroup has immediate upstream TaskGroups or tasks, a proxy node called
        upstream_join_id will be created in Graph view to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}.upstream_join_id"

    @property
    def downstream_join_id(self) -> str:
        """
        Creates a unique ID for downstream dependencies of this TaskGroup.

        If this TaskGroup has immediate downstream TaskGroups or tasks, a proxy node called
        downstream_join_id will be created in Graph view to join the outgoing edges from this
        TaskGroup to reduce the total number of edges needed to be displayed.
        """
        return f"{self.group_id}.downstream_join_id"

    def get_task_group_dict(self) -> dict[str, TaskGroup]:
        """Return a flat dictionary of group_id: TaskGroup."""
        task_group_map = {}

        def build_map(task_group):
            if not isinstance(task_group, TaskGroup):
                return

            task_group_map[task_group.group_id] = task_group

            for child in task_group.children.values():
                build_map(child)

        build_map(self)
        return task_group_map

    def get_child_by_label(self, label: str) -> DAGNode:
        """Get a child task/TaskGroup by its label (i.e. task_id/group_id without the group_id prefix)."""
        return self.children[self.child_id(label)]

    def __getitem__(self, label: str) -> DAGNode:
        try:
            return self.get_child_by_label(label)
        except KeyError:
            raise NodeNotFound(f"Task {label!r} not found")

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize task group; required by DagNode."""
        from airflow.sdk.api.datamodels._generated import DagAttributeTypes
        from airflow.serialization.serialized_objects import TaskGroupSerialization

        return (
            DagAttributeTypes.TASK_GROUP,
            TaskGroupSerialization.serialize_task_group(self),
        )

    def hierarchical_alphabetical_sort(self):
        """
        Sort children in hierarchical alphabetical order.

        - groups in alphabetical order first
        - tasks in alphabetical order after them.

        :return: list of tasks in hierarchical alphabetical order
        """
        return sorted(
            self.children.values(),
            key=lambda node: (not isinstance(node, TaskGroup), node.node_id),
        )

    def topological_sort(self) -> list[DAGNode]:
        """
        Sort children topologically — a task always comes after its upstream dependencies.

        Projects each child's per-task upstream IDs onto sibling-level integer indices once,
        then runs a greedy multi-pass sweep using a bytearray-backed emission flag. Equivalent
        in emission order to the previous modified-Kahn implementation, but moves the per-edge
        ``upstream_list`` materialization and ``parent_group`` walks out of the sweep's inner
        loop so they happen once per call instead of once per outer-loop pass.
        """
        children = self.children
        if not children:
            return []
        nodes = list(children.values())
        id_to_idx = {nid: i for i, nid in enumerate(children)}
        projected = [self._project_child_deps(i, c, id_to_idx) for i, c in enumerate(nodes)]
        return self._sweep_projection(nodes, projected)

    def _project_child_deps(
        self, child_idx: int, child: DAGNode, id_to_idx: dict[str, int]
    ) -> tuple[int, ...]:
        # Project one child's per-task upstream IDs onto sibling-level integer indices.
        # Self-deps are filtered once at the end via ``discard`` so the inner loop stays tight.
        upstream_ids = child.upstream_task_ids
        if not upstream_ids:
            return ()
        sib_deps: set[int] = set()
        for edge_id in upstream_ids:
            j = id_to_idx.get(edge_id)
            if j is not None:
                sib_deps.add(j)
                continue
            tg = self.dag.get_task(edge_id).task_group
            while tg is not None:
                j = id_to_idx.get(tg.node_id)
                if j is not None:
                    sib_deps.add(j)
                    break
                tg = tg.parent_group
        sib_deps.discard(child_idx)
        return tuple(sib_deps)

    def _sweep_projection(self, nodes: list[DAGNode], projected: list[tuple[int, ...]]) -> list[DAGNode]:
        # Greedy multi-pass sweep. emitted[i] == 1 iff nodes[i] has been emitted.
        # Pass 1 iterates range(n) directly; only blocked nodes are recorded into
        # ``pending`` and re-checked in subsequent passes. Avoids paying for a
        # ``list(range(n))`` allocation on single-pass shapes (the common case) while
        # still skipping already-emitted nodes on multi-pass shapes (e.g. a diamond's
        # single trailing sink).
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
                raise AirflowDagCycleException(f"A cyclic dependency occurred in dag: {self.dag_id}")
            pending = next_pending
        return order

    def iter_mapped_task_groups(self) -> Iterator[MappedTaskGroup]:
        """
        Return mapped task groups in the hierarchy.

        Groups are returned from the closest to the outmost. If *self* is a
        mapped task group, it is returned first.

        :meta private:
        """
        group: TaskGroup | None = self
        while group is not None:
            if isinstance(group, MappedTaskGroup):
                yield group
            group = group.parent_group

    def iter_tasks(self) -> Iterator[AbstractOperator]:
        """Return an iterator of the child tasks."""
        from airflow.sdk.definitions._internal.abstractoperator import AbstractOperator

        groups_to_visit = deque([self])

        while groups_to_visit:
            visiting = groups_to_visit.popleft()

            for child in visiting.children.values():
                if isinstance(child, AbstractOperator):
                    yield child
                elif isinstance(child, TaskGroup):
                    groups_to_visit.append(child)
                else:
                    raise ValueError(
                        f"Encountered a DAGNode that is not a TaskGroup or an "
                        f"AbstractOperator: {type(child).__module__}.{type(child)}"
                    )


@attrs.define(kw_only=True, repr=False)
class MappedTaskGroup(TaskGroup):
    """
    A mapped task group.

    This doesn't really do anything special, just holds some additional metadata
    for expansion later.

    Don't instantiate this class directly; call *expand* or *expand_kwargs* on
    a ``@task_group`` function instead.
    """

    _expand_input: DictOfListsExpandInput | ListOfDictsExpandInput = attrs.field(alias="expand_input")

    def __iter__(self):
        for child in self.children.values():
            if getattr(child, "trigger_rule", None) == TriggerRule.ALWAYS:
                raise ValueError(
                    "Task-generated mapping within a mapped task group is not "
                    "allowed with trigger rule 'always'"
                )
            yield from self._iter_child(child)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for op, _ in self._expand_input.iter_references():
            self.set_upstream(op)
        super().__exit__(exc_type, exc_val, exc_tb)

    def iter_mapped_dependencies(self) -> Iterator[Operator]:
        """Upstream dependencies that provide XComs used by this mapped task group."""
        from airflow.sdk.definitions.xcom_arg import XComArg

        for op, _ in XComArg.iter_xcom_references(self._expand_input):
            yield op

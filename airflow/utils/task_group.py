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

import copy
import functools
import operator
import weakref
from typing import TYPE_CHECKING, Any, Generator, Iterator, Sequence

import methodtools
import re2

from airflow.exceptions import (
    AirflowDagCycleException,
    AirflowException,
    DuplicateTaskIdFound,
    TaskAlreadyInTaskGroup,
)
from airflow.models.taskmixin import DAGNode
from airflow.serialization.enums import DagAttributeTypes
from airflow.utils.helpers import validate_group_key, validate_instance_args
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.abstractoperator import AbstractOperator
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.models.expandinput import ExpandInput
    from airflow.models.operator import Operator
    from airflow.models.taskmixin import DependencyMixin
    from airflow.utils.edgemodifier import EdgeModifier

# TODO: The following mapping is used to validate that the arguments passed to the TaskGroup are of the
#  correct type. This is a temporary solution until we find a more sophisticated method for argument
#  validation. One potential method is to use get_type_hints from the typing module. However, this is not
#  fully compatible with future annotations for Python versions below 3.10. Once we require a minimum Python
#  version that supports `get_type_hints` effectively or find a better approach, we can replace this
#  manual type-checking method.
TASKGROUP_ARGS_EXPECTED_TYPES = {
    "group_id": str,
    "prefix_group_id": bool,
    "tooltip": str,
    "ui_color": str,
    "ui_fgcolor": str,
    "add_suffix_on_collision": bool,
}


class TaskGroup(DAGNode):
    """
    A collection of tasks.

    When set_downstream() or set_upstream() are called on the TaskGroup, it is applied across
    all tasks within the group if necessary.

    :param group_id: a unique, meaningful id for the TaskGroup. group_id must not conflict
        with group_id of TaskGroup or task_id of tasks in the DAG. Root TaskGroup has group_id
        set to None.
    :param prefix_group_id: If set to True, child task_id and group_id will be prefixed with
        this TaskGroup's group_id. If set to False, child task_id and group_id are not prefixed.
        Default is True.
    :param parent_group: The parent TaskGroup of this TaskGroup. parent_group is set to None
        for the root TaskGroup.
    :param dag: The DAG that this TaskGroup belongs to.
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators,
        will override default_args defined in the DAG level.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :param tooltip: The tooltip of the TaskGroup node when displayed in the UI
    :param ui_color: The fill color of the TaskGroup node when displayed in the UI
    :param ui_fgcolor: The label color of the TaskGroup node when displayed in the UI
    :param add_suffix_on_collision: If this task group name already exists,
        automatically add `__1` etc suffixes
    """

    used_group_ids: set[str | None]

    def __init__(
        self,
        group_id: str | None,
        prefix_group_id: bool = True,
        parent_group: TaskGroup | None = None,
        dag: DAG | None = None,
        default_args: dict[str, Any] | None = None,
        tooltip: str = "",
        ui_color: str = "CornflowerBlue",
        ui_fgcolor: str = "#000",
        add_suffix_on_collision: bool = False,
    ):
        from airflow.models.dag import DagContext

        self.prefix_group_id = prefix_group_id
        self.default_args = copy.deepcopy(default_args or {})

        dag = dag or DagContext.get_current_dag()

        if group_id is None:
            # This creates a root TaskGroup.
            if parent_group:
                raise AirflowException("Root TaskGroup cannot have parent_group")
            # used_group_ids is shared across all TaskGroups in the same DAG to keep track
            # of used group_id to avoid duplication.
            self.used_group_ids = set()
            self.dag = dag
        else:
            if prefix_group_id:
                # If group id is used as prefix, it should not contain spaces nor dots
                # because it is used as prefix in the task_id
                validate_group_key(group_id)
            else:
                if not isinstance(group_id, str):
                    raise ValueError("group_id must be str")
                if not group_id:
                    raise ValueError("group_id must not be empty")

            if not parent_group and not dag:
                raise AirflowException("TaskGroup can only be used inside a dag")

            parent_group = parent_group or TaskGroupContext.get_current_task_group(dag)
            if not parent_group:
                raise AirflowException("TaskGroup must have a parent_group except for the root TaskGroup")
            if dag is not parent_group.dag:
                raise RuntimeError(
                    "Cannot mix TaskGroups from different DAGs: %s and %s", dag, parent_group.dag
                )

            self.used_group_ids = parent_group.used_group_ids

        # if given group_id already used assign suffix by incrementing largest used suffix integer
        # Example : task_group ==> task_group__1 -> task_group__2 -> task_group__3
        self._group_id = group_id
        self._check_for_group_id_collisions(add_suffix_on_collision)

        self.children: dict[str, DAGNode] = {}

        if parent_group:
            parent_group.add(self)
            self._update_default_args(parent_group)

        self.used_group_ids.add(self.group_id)
        if self.group_id:
            self.used_group_ids.add(self.downstream_join_id)
            self.used_group_ids.add(self.upstream_join_id)

        self.tooltip = tooltip
        self.ui_color = ui_color
        self.ui_fgcolor = ui_fgcolor

        # Keep track of TaskGroups or tasks that depend on this entire TaskGroup separately
        # so that we can optimize the number of edges when entire TaskGroups depend on each other.
        self.upstream_group_ids: set[str | None] = set()
        self.downstream_group_ids: set[str | None] = set()
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()

        validate_instance_args(self, TASKGROUP_ARGS_EXPECTED_TYPES)

    def _check_for_group_id_collisions(self, add_suffix_on_collision: bool):
        if self._group_id is None:
            return
        # if given group_id already used assign suffix by incrementing largest used suffix integer
        # Example : task_group ==> task_group__1 -> task_group__2 -> task_group__3
        if self._group_id in self.used_group_ids:
            if not add_suffix_on_collision:
                raise DuplicateTaskIdFound(f"group_id '{self._group_id}' has already been added to the DAG")
            base = re2.split(r"__\d+$", self._group_id)[0]
            suffixes = sorted(
                int(re2.split(r"^.+__", used_group_id)[1])
                for used_group_id in self.used_group_ids
                if used_group_id is not None and re2.match(rf"^{base}__\d+$", used_group_id)
            )
            if not suffixes:
                self._group_id += "__1"
            else:
                self._group_id = f"{base}__{suffixes[-1] + 1}"

    def _update_default_args(self, parent_group: TaskGroup):
        if parent_group.default_args:
            self.default_args = {**parent_group.default_args, **self.default_args}

    @classmethod
    def create_root(cls, dag: DAG) -> TaskGroup:
        """Create a root TaskGroup with no group_id or parent."""
        return cls(group_id=None, dag=dag)

    @property
    def node_id(self):
        return self.group_id

    @property
    def is_root(self) -> bool:
        """Returns True if this TaskGroup is the root TaskGroup. Otherwise False."""
        return not self.group_id

    @property
    def parent_group(self) -> TaskGroup | None:
        return self.task_group

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
        Add a task to this TaskGroup.

        :meta private:
        """
        from airflow.models.abstractoperator import AbstractOperator

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
            raise DuplicateTaskIdFound(f"{node_type} id '{key}' has already been added to the DAG")

        if isinstance(task, TaskGroup):
            if self.dag:
                if task.dag is not None and self.dag is not task.dag:
                    raise RuntimeError(
                        "Cannot mix TaskGroups from different DAGs: %s and %s", self.dag, task.dag
                    )
                task.dag = self.dag
            if task.children:
                raise AirflowException("Cannot add a non-empty TaskGroup")

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
        if self.task_group and self.task_group.prefix_group_id and self.task_group._group_id:
            # defer to parent whether it adds a prefix
            return self.task_group.child_id(self._group_id)

        return self._group_id

    @property
    def label(self) -> str | None:
        """group_id excluding parent's group_id used as the node label in UI."""
        return self._group_id

    def update_relative(
        self, other: DependencyMixin, upstream: bool = True, edge_modifier: EdgeModifier | None = None
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
                    raise AirflowException(
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

        for task_like in task_or_task_list:
            self.update_relative(task_like, upstream, edge_modifier=edge_modifier)

        if upstream:
            for task in self.get_roots():
                task.set_upstream(task_or_task_list)
        else:
            for task in self.get_leaves():
                task.set_downstream(task_or_task_list)

    def __enter__(self) -> TaskGroup:
        TaskGroupContext.push_context_managed_task_group(self)
        return self

    def __exit__(self, _type, _value, _tb):
        TaskGroupContext.pop_context_managed_task_group()

    def has_task(self, task: BaseOperator) -> bool:
        """Return True if this TaskGroup or its children TaskGroups contains the given task."""
        if task.task_id in self.children:
            return True

        return any(child.has_task(task) for child in self.children.values() if isinstance(child, TaskGroup))

    @property
    def roots(self) -> list[BaseOperator]:
        """Required by TaskMixin."""
        return list(self.get_roots())

    @property
    def leaves(self) -> list[BaseOperator]:
        """Required by TaskMixin."""
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
                elif down_task.task_id not in ids:
                    continue
                elif not down_task.is_teardown:
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

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize task group; required by DAGNode."""
        from airflow.serialization.serialized_objects import TaskGroupSerialization

        return DagAttributeTypes.TASK_GROUP, TaskGroupSerialization.serialize_task_group(self)

    def hierarchical_alphabetical_sort(self):
        """
        Sort children in hierarchical alphabetical order.

        - groups in alphabetical order first
        - tasks in alphabetical order after them.

        :return: list of tasks in hierarchical alphabetical order
        """
        return sorted(
            self.children.values(), key=lambda node: (not isinstance(node, TaskGroup), node.node_id)
        )

    def topological_sort(self, _include_subdag_tasks: bool = False):
        """
        Sorts children in topographical order, such that a task comes after any of its upstream dependencies.

        :return: list of tasks in topological order
        """
        # This uses a modified version of Kahn's Topological Sort algorithm to
        # not have to pre-compute the "in-degree" of the nodes.
        from airflow.operators.subdag import SubDagOperator  # Avoid circular import

        graph_unsorted = copy.copy(self.children)

        graph_sorted: list[DAGNode] = []

        # special case
        if not self.children:
            return graph_sorted

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted graph. If a set of edges doesn't contain
            # any nodes that haven't been resolved, that is, that are still in the unsorted graph, remove the
            # pair from the unsorted graph, and append it to the sorted graph. Note here that by using
            # the values() method for iterating, a copy of the unsorted graph is used, allowing us to modify
            # the unsorted graph as we move through it.
            #
            # We also keep a flag for checking that graph is acyclic, which is true if any nodes are resolved
            # during each pass through the graph. If not, we need to exit as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted.values()):
                for edge in node.upstream_list:
                    if edge.node_id in graph_unsorted:
                        break
                    # Check for task's group is a child (or grand child) of this TG,
                    tg = edge.task_group
                    while tg:
                        if tg.node_id in graph_unsorted:
                            break
                        tg = tg.task_group

                    if tg:
                        # We are already going to visit that TG
                        break
                else:
                    acyclic = True
                    del graph_unsorted[node.node_id]
                    graph_sorted.append(node)
                    if _include_subdag_tasks and isinstance(node, SubDagOperator):
                        graph_sorted.extend(
                            node.subdag.task_group.topological_sort(_include_subdag_tasks=True)
                        )

            if not acyclic:
                raise AirflowDagCycleException(f"A cyclic dependency occurred in dag: {self.dag_id}")

        return graph_sorted

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
            group = group.task_group

    def iter_tasks(self) -> Iterator[AbstractOperator]:
        """Return an iterator of the child tasks."""
        from airflow.models.abstractoperator import AbstractOperator

        groups_to_visit = [self]

        while groups_to_visit:
            visiting = groups_to_visit.pop(0)

            for child in visiting.children.values():
                if isinstance(child, AbstractOperator):
                    yield child
                elif isinstance(child, TaskGroup):
                    groups_to_visit.append(child)
                else:
                    raise ValueError(
                        f"Encountered a DAGNode that is not a TaskGroup or an AbstractOperator: {type(child)}"
                    )


class MappedTaskGroup(TaskGroup):
    """
    A mapped task group.

    This doesn't really do anything special, just holds some additional metadata
    for expansion later.

    Don't instantiate this class directly; call *expand* or *expand_kwargs* on
    a ``@task_group`` function instead.
    """

    def __init__(self, *, expand_input: ExpandInput, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._expand_input = expand_input

    def __iter__(self):
        from airflow.models.abstractoperator import AbstractOperator

        for child in self.children.values():
            if isinstance(child, AbstractOperator) and child.trigger_rule == TriggerRule.ALWAYS:
                raise ValueError(
                    "Task-generated mapping within a mapped task group is not allowed with trigger rule 'always'"
                )
            yield from self._iter_child(child)

    def iter_mapped_dependencies(self) -> Iterator[Operator]:
        """Upstream dependencies that provide XComs used by this mapped task group."""
        from airflow.models.xcom_arg import XComArg

        for op, _ in XComArg.iter_xcom_references(self._expand_input):
            yield op

    @methodtools.lru_cache(maxsize=None)
    def get_parse_time_mapped_ti_count(self) -> int:
        """
        Return the Number of instances a task in this group should be mapped to, when a DAG run is created.

        This only considers literal mapped arguments, and would return *None*
        when any non-literal values are used for mapping.

        If this group is inside mapped task groups, all the nested counts are
        multiplied and accounted.

        :meta private:

        :raise NotFullyPopulated: If any non-literal mapped arguments are encountered.
        :return: The total number of mapped instances each task should have.
        """
        return functools.reduce(
            operator.mul,
            (g._expand_input.get_parse_time_mapped_ti_count() for g in self.iter_mapped_task_groups()),
        )

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

    def __exit__(self, exc_type, exc_val, exc_tb):
        for op, _ in self._expand_input.iter_references():
            self.set_upstream(op)
        super().__exit__(exc_type, exc_val, exc_tb)


class TaskGroupContext:
    """TaskGroup context is used to keep the current TaskGroup when TaskGroup is used as ContextManager."""

    active: bool = False
    _context_managed_task_group: TaskGroup | None = None
    _previous_context_managed_task_groups: list[TaskGroup] = []

    @classmethod
    def push_context_managed_task_group(cls, task_group: TaskGroup):
        """Push a TaskGroup into the list of managed TaskGroups."""
        if cls._context_managed_task_group:
            cls._previous_context_managed_task_groups.append(cls._context_managed_task_group)
        cls._context_managed_task_group = task_group
        cls.active = True

    @classmethod
    def pop_context_managed_task_group(cls) -> TaskGroup | None:
        """Pops the last TaskGroup from the list of managed TaskGroups and update the current TaskGroup."""
        old_task_group = cls._context_managed_task_group
        if cls._previous_context_managed_task_groups:
            cls._context_managed_task_group = cls._previous_context_managed_task_groups.pop()
        else:
            cls._context_managed_task_group = None
        cls.active = False
        return old_task_group

    @classmethod
    def get_current_task_group(cls, dag: DAG | None) -> TaskGroup | None:
        """Get the current TaskGroup."""
        from airflow.models.dag import DagContext

        if not cls._context_managed_task_group:
            dag = dag or DagContext.get_current_dag()
            if dag:
                # If there's currently a DAG but no TaskGroup, return the root TaskGroup of the dag.
                return dag.task_group

        return cls._context_managed_task_group


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
